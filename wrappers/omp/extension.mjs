// SPDX-License-Identifier: MIT

import { randomBytes } from "node:crypto";
import { lstatSync, realpathSync } from "node:fs";
import path from "node:path";

import {
  BridgeCallError,
  BridgeClosedError,
  connectBridge,
} from "../pifamily/extension/bridge.mjs";

export const launchEnvironmentName = "SESSIONBUS_OMP_LAUNCH";
export const toolName = "sessionbus";
export const deliveryMessageType = "sessionbus-delivery";

const processExtensionKey = Symbol.for("sessionbus.omp.managed-extension.v1");
const topologyMode = Object.freeze({ lane: "rpc", interactive: "tui" });
const switchReasons = Object.freeze(["new", "resume", "fork"]);
const actions = Object.freeze([
  "list", "send", "spawn", "describe", "trace", "run", "start", "wait", "status",
  "interrupt", "close", "forget", "ack",
]);

const maxBindings = 256;
const maxQueuedDeliveries = 256;
const maxReportWork = 256;
const maxTextBytes = 1 << 20;
const maxRetainedBytes = 32 << 20;
const maxBatchItems = 64;
const maxBatchBytes = 1 << 20;

const toolDescription = `Call Sessionbus using this exact OMP session identity.
Use list to discover peers and self_info, send to message peers, describe before
spawning a product lane, trace a direct child's Sessionbus messages, and
run/start/status/wait/ack/interrupt/close/forget to
own a lane through its full lifecycle. Arguments must have the exact public
shape for the selected action. A successful send confirms only the recipient's
published delivery disposition; queued_for_next_turn is not model consumption.
A lane queued_for_next_turn receipt means the daemon retained the definitely
unsubmitted message in bounded memory and scheduled its next managed run.
A completion pointer is an ordinary peer message, not the lane result. Tracing
defaults off; events copies message and settled-delivery metadata, content also
includes message bodies, and neither mode includes history or lane lifecycle.
Copies arrive as daemon-generated JSON trace envelopes in ordinary messages.`;

function object(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function exactKeys(value, keys, what) {
  if (!object(value)) throw new Error(`OMP ${what} is not an object`);
  const actual = Object.keys(value);
  if (actual.length !== keys.length || keys.some((key) => !Object.hasOwn(value, key))) {
    throw new Error(`OMP ${what} fields are invalid`);
  }
  return value;
}

function boundedText(value, limit, what, { empty = false, whitespace = true } = {}) {
  if (typeof value !== "string" || (!empty && value.length === 0) ||
      (!whitespace && /\s/u.test(value)) || value.includes("\0") || Buffer.byteLength(value) > limit) {
    throw new Error(`OMP ${what} is invalid`);
  }
  return value;
}

function nativeID(value) {
  return boundedText(value, 256, "native session identity", { whitespace: false });
}

function ownerToken(value) {
  return boundedText(value, 256, "owner token", { whitespace: false });
}

function messageID(value) {
  return boundedText(value, 256, "message identity");
}

function toolCallID(value) {
  return boundedText(value, 256, "native tool call identity", { whitespace: false });
}

function abortReason(signal) {
  if (signal?.reason instanceof Error) return signal.reason;
  const error = new Error("The operation was aborted");
  error.name = "AbortError";
  return error;
}

function throwIfAborted(signal) {
  if (signal?.aborted) throw abortReason(signal);
}

function combinedSignal(...signals) {
  const present = signals.filter(Boolean);
  if (present.length === 0) return undefined;
  if (present.length === 1) return present[0];
  return AbortSignal.any(present);
}

function validatePhysicalLaunch(launch, fs = { lstatSync, realpathSync }) {
  const directory = fs.lstatSync(launch.directory);
  if (!directory.isDirectory() || directory.isSymbolicLink() ||
      (directory.mode & 0o777) !== 0o700 || directory.uid !== process.getuid() ||
      fs.realpathSync(launch.directory) !== launch.directory) {
    throw new Error("OMP managed launch directory is not private, physical, and owned");
  }
  const socket = fs.lstatSync(launch.socket);
  if (!socket.isSocket() || socket.isSymbolicLink() || socket.uid !== process.getuid() ||
      path.dirname(launch.socket) !== launch.directory) {
    throw new Error("OMP managed bridge socket is not physical and owned");
  }
}

export function captureLaunch(environment = process.env, parent = process.ppid, fs = { lstatSync, realpathSync }) {
  const raw = environment[launchEnvironmentName];
  delete environment[launchEnvironmentName];
  if (raw === undefined) return null;
  if (typeof raw !== "string" || Buffer.byteLength(raw) > 64 << 10) {
    throw new Error("OMP managed launch metadata is invalid");
  }
  let launch;
  try {
    launch = JSON.parse(raw);
  } catch {
    throw new Error("OMP managed launch metadata is invalid");
  }
  exactKeys(launch, ["directory", "owner_pid", "socket", "topology"], "managed launch metadata");
  if (!Number.isSafeInteger(launch.owner_pid) || launch.owner_pid <= 1 || launch.owner_pid !== parent ||
      !Object.hasOwn(topologyMode, launch.topology) ||
      !path.isAbsolute(boundedText(launch.directory, 4096, "managed launch directory")) ||
      !path.isAbsolute(boundedText(launch.socket, 4096, "managed bridge socket"))) {
    throw new Error("OMP managed launch metadata is invalid");
  }
  validatePhysicalLaunch(launch, fs);
  return Object.freeze({ ...launch });
}

// Same closed field union as the shared MCP declaration. Action-specific
// required fields and combinations remain enforced by the public kit.
function argumentSchema() {
  const properties = {};
  for (const field of ["session_id", "host", "message", "target", "group", "product", "name", "resume_session_id", "notify_target", "input", "run_id"]) {
    properties[field] = { type: "string" };
  }
  for (const field of ["targets", "extra_groups"]) {
    properties[field] = { type: "array", items: { type: "string" } };
  }
  for (const field of ["persistent", "notify", "forget"]) properties[field] = { type: "boolean" };
  for (const field of ["auto_close_ms", "timeout_ms"]) properties[field] = { type: "integer" };
  properties.trace = { type: "string", enum: ["off", "events", "content"] };
  properties.mode = { type: "string", enum: ["off", "events", "content"] };
  const open = {};
  for (const field of ["cwd", "permission_mode", "model", "reasoning_effort"]) open[field] = { type: "string" };
  open.arguments = { type: "array", items: { type: "string" } };
  properties.open = { type: "object", additionalProperties: false, properties: open };
  return { type: "object", additionalProperties: false, properties,
    description: "Use only the fields listed for the selected action in the tool description. send has no summary field; put the complete content in message." };
}

function toolParameters() {
  return {
    type: "object",
    additionalProperties: false,
    required: ["action", "arguments"],
    properties: {
      action: { type: "string", enum: [...actions] },
      arguments: argumentSchema(),
    },
  };
}

function defaultToken() {
  return randomBytes(16).toString("hex");
}

function retainedStringBytes(...values) {
  return values.reduce((total, value) => total + Buffer.byteLength(value), 0);
}

function exactEcho(result, state, extra) {
  const keys = ["owner_token", "session_id", ...Object.keys(extra)];
  exactKeys(result, keys, "bridge response");
  if (result.owner_token !== state.ownerToken || result.session_id !== state.sessionID) {
    throw new Error("OMP bridge changed native owner identity");
  }
  for (const [key, expected] of Object.entries(extra)) {
    const actual = result[key];
    if (Array.isArray(expected) ? !Array.isArray(actual) || actual.length !== expected.length || actual.some((value, index) => value !== expected[index]) : actual !== expected) {
      throw new Error(`OMP bridge changed ${key}`);
    }
  }
  return result;
}

function deliveryEntryBytes(entry) {
  return retainedStringBytes(entry.messageID, entry.body);
}

function customDeliveryMessage(state, batch) {
  return {
    customType: deliveryMessageType,
    content: batch.content,
    display: true,
    attribution: "user",
    details: {
      owner_token: state.ownerToken,
      session_id: state.sessionID,
      batch_token: batch.batchToken,
      message_ids: [...batch.messageIDs],
    },
  };
}

// A lane delivery steered into the running native run. It has no batch: the
// lane's receipt is the hand-off itself, and nothing later is correlated.
function laneDeliveryMessage(state, messageID, body) {
  return {
    customType: deliveryMessageType,
    content: body,
    display: true,
    attribution: "user",
    details: { owner_token: state.ownerToken, session_id: state.sessionID, message_ids: [messageID] },
  };
}

function matchingDelivery(message, state, batch) {
  if (!object(message) || message.role !== "custom" || message.customType !== deliveryMessageType ||
      message.content !== batch.content || message.display !== true || message.attribution !== "user") return false;
  if (!object(message.details)) return false;
  try {
    exactKeys(message.details, ["owner_token", "session_id", "batch_token", "message_ids"], "delivery details");
  } catch {
    return false;
  }
  const ids = message.details.message_ids;
  return message.details.owner_token === state.ownerToken && message.details.session_id === state.sessionID &&
    message.details.batch_token === batch.batchToken && Array.isArray(ids) && ids.length === batch.messageIDs.length &&
    ids.every((value, index) => value === batch.messageIDs[index]);
}

function referencesDeliveryBatch(message, state, batch) {
  return object(message) && message.role === "custom" && message.customType === deliveryMessageType && object(message.details) &&
    message.details.owner_token === state.ownerToken && message.details.session_id === state.sessionID &&
    message.details.batch_token === batch.batchToken;
}

export function createOMPExtension({ launch, connect = connectBridge, createToken = defaultToken, limits = {} } = {}) {
  const limitKeys = ["bindings", "queuedDeliveries", "reportWork", "retainedBytes", "batchItems", "batchBytes"];
  if (!object(limits) || Object.keys(limits).some((key) => !limitKeys.includes(key))) {
    throw new Error("OMP extension limit overrides are invalid");
  }
  const limit = {
    bindings: limits.bindings ?? maxBindings,
    queuedDeliveries: limits.queuedDeliveries ?? maxQueuedDeliveries,
    reportWork: limits.reportWork ?? maxReportWork,
    retainedBytes: limits.retainedBytes ?? maxRetainedBytes,
    batchItems: limits.batchItems ?? maxBatchItems,
    batchBytes: limits.batchBytes ?? maxBatchBytes,
  };
  for (const [key, maximum] of Object.entries({
    bindings: maxBindings, queuedDeliveries: maxQueuedDeliveries, reportWork: maxReportWork,
    retainedBytes: maxRetainedBytes, batchItems: maxBatchItems, batchBytes: maxBatchBytes,
  })) {
    if (!Number.isSafeInteger(limit[key]) || limit[key] < 1 || limit[key] > maximum) {
      throw new Error(`OMP ${key} limit is invalid`);
    }
  }
  const lifetime = new AbortController();
  const bindings = new Map();
  const factories = new Set();
  let bridgePromise;
  let bridge;
  let bridgeClosing = false;
  let failure;
  let primary;
  let retainedBytes = 0;
  let reportItems = 0;
  let tokenCounter = 0;

  function reserve(bytes) {
    if (!Number.isSafeInteger(bytes) || bytes < 0 || retainedBytes > limit.retainedBytes - bytes) return false;
    retainedBytes += bytes;
    return true;
  }

  function release(bytes) {
    if (!Number.isSafeInteger(bytes) || bytes < 0 || bytes > retainedBytes) {
      throw new Error("OMP retained-payload accounting is invalid");
    }
    retainedBytes -= bytes;
  }

  function freshToken(kind) {
    tokenCounter += 1;
    return ownerToken(`${createToken()}-${kind}-${tokenCounter}`);
  }

  function failState(state, error) {
    if (!state || state.failure) return;
    state.failure = error instanceof Error ? error : new Error(String(error));
    state.factory.failed = state.failure;
    state.ending = true;
    discardDeliveries(state);
    if (state.previous) {
      removeState(state.previous);
      state.previous = undefined;
    }
    if (!state.controller.signal.aborted) state.controller.abort(state.failure);
    try { state.ctx?.abort?.(); } catch {}
    if (state.scope === "primary") {
      try { state.ctx?.shutdown?.(); } catch {}
    }
  }

  function failStartup(factory, ctx, error) {
    factory.failed = error instanceof Error ? error : new Error(String(error));
    factories.delete(factory);
    try { ctx?.abort?.(); } catch {}
    if (ctx?.mode !== "print") {
      try { ctx?.shutdown?.(); } catch {}
    }
  }

  function failProcess(error) {
    if (failure) return;
    failure = error instanceof Error ? error : new Error(String(error));
    if (!lifetime.signal.aborted) lifetime.abort(failure);
    for (const factory of factories) failState(factory.current, failure);
    if (bridge && !bridgeClosing) void bridge.close().catch(() => {});
  }

  function describeState(state) {
    const currentID = nativeID(state.ctx.sessionManager.getSessionId());
    if (currentID !== state.sessionID || state.factory.current !== state || bindings.get(state.ownerToken) !== state) {
      const error = new Error("OMP native session identity changed without a lifecycle event");
      failState(state, error);
      throw error;
    }
    const name = boundedText(state.ctx.sessionManager.getSessionName() ?? "", 4096, "native name", { empty: true });
    const cwd = boundedText(state.ctx.cwd, 32 << 10, "native working directory");
    if (!path.isAbsolute(cwd)) throw new Error("OMP native working directory is not absolute");
    return { owner_token: state.ownerToken, session_id: state.sessionID, name, cwd };
  }

  function resetBoundarySnapshot(ctx) {
    const entries = ctx.sessionManager.getEntries();
    if (!Array.isArray(entries)) throw new Error("OMP native session entries are unavailable");
    let count = 0;
    let lastID = "";
    for (const entry of entries) {
      if (entry?.type !== "reset_boundary") continue;
      count += 1;
      lastID = boundedText(entry.id, 4096, "native reset boundary id");
    }
    return { count, lastID };
  }

  function rejectNewResetBoundary(state, ctx) {
    const current = resetBoundarySnapshot(ctx);
    if (state.batches.size === 0) {
      state.resetBoundary = current;
      return;
    }
    if (current.count === state.resetBoundary.count && current.lastID === state.resetBoundary.lastID) return;
    // A user's /clear while a delivery is pending is ordinary native use: the
    // pending attempts are uncertain and end; the session stays.
    state.resetBoundary = current;
    abandonBatches(state.factory, state, [...state.batches.values()]);
  }

  function nativeState(params, keys) {
    exactKeys(params, keys, "native request parameters");
    const token = ownerToken(params.owner_token);
    const sessionID = nativeID(params.session_id);
    const state = bindings.get(token);
    if (!state || state.sessionID !== sessionID || state.ended) {
      throw new BridgeCallError("stale_owner", "OMP native request does not own a current factory");
    }
    describeState(state);
    rejectNewResetBoundary(state, state.ctx);
    return state;
  }

  async function nativeRequest({ method, params, signal }) {
    throwIfAborted(signal);
    switch (method) {
      case "native.describe": {
        const state = nativeState(params, ["owner_token", "session_id"]);
        state.described = true;
        return describeState(state);
      }
      case "native.stage": {
        const state = nativeState(params, ["owner_token", "session_id", "message_id", "body"]);
        if (!state.described || state.ending || (state.scope === "primary" && launch.topology === "lane")) {
          throw new BridgeCallError("stale_owner", "OMP native stage does not own a deliverable factory");
        }
        const id = messageID(params.message_id);
        const body = boundedText(params.body, maxTextBytes, "staged delivery body", { empty: true });
        if (state.queued.some((entry) => entry.messageID === id) ||
            [...state.batches.values()].some((batch) => batch.messageIDs.includes(id))) {
          throw new BridgeCallError("duplicate_message", "OMP staged delivery identity is already owned");
        }
        const entry = { messageID: id, body };
        const bytes = deliveryEntryBytes(entry);
        const retainedDeliveries = state.queued.length +
          [...state.batches.values()].reduce((count, batch) => count + batch.entries.length, 0);
        if (retainedDeliveries >= limit.queuedDeliveries || !reserve(bytes)) {
          return { owner_token: state.ownerToken, session_id: state.sessionID, message_id: id, queued: false, reason: "queue_full" };
        }
        state.queued.push(entry);
        state.deliveryBytes += bytes;
        // OMP chooses between steering into its running task and starting a
        // turn only after an await (agent-session.ts:8814-8840), so the
        // interactive primary's receipt is decided here, from OMP's busy
        // state before the send, with no native confirmation. Task children
        // keep their queued receipt.
        const reportsBusy = state.scope === "primary" && launch.topology === "interactive";
        const busy = reportsBusy && !state.ctx.isIdle();
        scheduleDeliveryTurn(state.factory, state);
        return {
          owner_token: state.ownerToken, session_id: state.sessionID, message_id: id, queued: true,
          ...(reportsBusy ? { busy } : {}),
        };
      }
      case "native.steer": {
        const state = nativeState(params, ["owner_token", "session_id", "message_id", "body", "run_token"]);
        if (state.scope !== "primary" || launch.topology !== "lane") {
          throw new BridgeCallError("method_not_found", "OMP native steer is only for the lane's primary session");
        }
        if (!state.described || state.ending) {
          throw new BridgeCallError("stale_owner", "OMP native steer does not own a deliverable factory");
        }
        const id = messageID(params.message_id);
        const body = boundedText(params.body, maxTextBytes, "steered delivery body", { empty: true });
        const runToken = ownerToken(params.run_token);
        throwIfAborted(signal);
        // The Run's token, OMP's busy state and the send are one synchronous
        // step, so a steer made for a Run never enters a later one. OMP places
        // the message only after an await of its own (agent-session.ts:8814-
        // 8840): still streaming, it joins the steering queue; idle by then,
        // with no triggerTurn, it is appended to history without a turn.
        if (state.runToken !== runToken || state.ctx.isIdle()) {
          return { owner_token: state.ownerToken, session_id: state.sessionID, message_id: id, accepted: false, reason: "idle" };
        }
        state.pi.sendMessage(laneDeliveryMessage(state, id, body), { deliverAs: "steer" });
        return { owner_token: state.ownerToken, session_id: state.sessionID, message_id: id, accepted: true };
      }
      case "native.shutdown": {
        const state = nativeState(params, ["owner_token", "session_id"]);
        if (state.scope !== "primary") {
          throw new BridgeCallError("method_not_found", "OMP native shutdown is unavailable for Task children");
        }
        state.ctx.shutdown();
        return { owner_token: state.ownerToken, session_id: state.sessionID, requested: true };
      }
      default:
        throw new BridgeCallError("method_not_found", "OMP native bridge method is unavailable");
    }
  }

  async function connection() {
    if (!launch) throw new Error("OMP managed launch metadata is missing");
    if (failure) throw failure;
    if (!bridgePromise) {
      bridgePromise = Promise.resolve(connect(launch.socket, {
        role: "native",
        handler: nativeRequest,
        signal: lifetime.signal,
      })).then((value) => {
        bridge = value;
        void value.done.then(() => {
          if (!bridgeClosing && !lifetime.signal.aborted) {
            failProcess(new BridgeClosedError("OMP managed owner connection ended"));
          }
        });
        return value;
      });
      bridgePromise.catch(failProcess);
    }
    return bridgePromise;
  }

  async function callHost(state, method, params, signal = undefined, { ending = false } = {}) {
    throwIfAborted(signal);
    const owner = await connection();
    return owner.call(method, params, {
      signal: combinedSignal(lifetime.signal, ending ? undefined : state?.controller.signal, signal),
    });
  }

  function reportBytes(method, params) {
    const encoded = JSON.stringify({ method, params });
    if (typeof encoded !== "string" || Buffer.byteLength(encoded) > maxTextBytes) {
      throw new Error("OMP owner report exceeds its native bound");
    }
    return Buffer.byteLength(encoded);
  }

  function enqueueReport(factory, state, method, params, validate, { ending = false } = {}) {
    if (!ending && (factory.failed || state.ending || state.failure)) return false;
    let bytes;
    try {
      bytes = reportBytes(method, params);
    } catch (error) {
      failState(state, error);
      return false;
    }
    if (factory.reports.length >= limit.reportWork || reportItems >= limit.reportWork || !reserve(bytes)) {
      failState(state, new Error("OMP owner report capacity is exhausted"));
      return false;
    }
    factory.reports.push({ state, method, params, validate, bytes, ending });
    reportItems += 1;
    startReportWorker(factory);
    return true;
  }

  function startReportWorker(factory) {
    if (factory.worker) return;
    const worker = drainReports(factory).catch((error) => { factory.workerError ??= error; });
    factory.worker = worker;
    void worker.finally(() => {
      if (factory.worker !== worker) return;
      factory.worker = undefined;
      if (factory.reports.length > 0 && !factory.failed) startReportWorker(factory);
    });
  }

  async function drainReports(factory) {
    while (factory.reports.length > 0) {
      const work = factory.reports.shift();
      try {
        const result = await callHost(work.state, work.method, work.params, undefined, { ending: work.ending });
        work.validate(result);
      } catch (error) {
        failState(work.state, error);
        if (factory.current !== work.state) failState(factory.current, error);
        const endingReports = [];
        while (factory.reports.length > 0) {
          const abandoned = factory.reports.shift();
          if (abandoned.ending) {
            endingReports.push(abandoned);
            continue;
          }
          reportItems -= 1;
          release(abandoned.bytes);
        }
        factory.reports.unshift(...endingReports);
        throw error;
      } finally {
        reportItems -= 1;
        release(work.bytes);
      }
    }
  }

  async function joinReports(factory) {
    while (factory.worker || factory.reports.length > 0) {
      if (!factory.worker) startReportWorker(factory);
      await factory.worker;
    }
    if (factory.workerError) throw factory.workerError;
  }

  function discardDeliveries(state) {
    let bytes = 0;
    for (const entry of state.queued) bytes += deliveryEntryBytes(entry);
    for (const batch of state.batches.values()) {
      for (const entry of batch.entries) bytes += deliveryEntryBytes(entry);
      bytes += batch.contentBytes;
    }
    state.queued = [];
    state.batches.clear();
    if (bytes > 0) {
      state.deliveryBytes -= bytes;
      release(bytes);
    }
  }

  function removeState(state) {
    if (bindings.get(state.ownerToken) !== state) return;
    discardDeliveries(state);
    bindings.delete(state.ownerToken);
    if (primary === state) primary = undefined;
    state.ended = true;
    if (!state.controller.signal.aborted) state.controller.abort(new BridgeClosedError("OMP native factory ended"));
    release(state.bindingBytes);
  }

  function makeState(factory, pi, ctx, scope, mode) {
    if (bindings.size >= limit.bindings) throw new Error("OMP native factory capacity is exhausted");
    const sessionID = nativeID(ctx.sessionManager.getSessionId());
    const token = freshToken(scope);
    const name = boundedText(ctx.sessionManager.getSessionName() ?? "", 4096, "native name", { empty: true });
    const cwd = boundedText(ctx.cwd, 32 << 10, "native working directory");
    if (!path.isAbsolute(cwd)) throw new Error("OMP native working directory is not absolute");
    const bindingBytes = retainedStringBytes(token, sessionID, name, cwd, scope, mode);
    if (!reserve(bindingBytes)) throw new Error("OMP native factory retained-payload capacity is exhausted");
    const state = {
      factory, pi, ctx, scope, mode, ownerToken: token, sessionID, controller: new AbortController(),
      described: false, admitted: false, ending: false, ended: false, failure: undefined,
      queued: [], batches: new Map(), runToken: undefined,
      deliveryBytes: 0, bindingBytes, reportSequence: 0, runSequence: 0, batchSequence: 0,
      resetBoundary: resetBoundarySnapshot(ctx),
    };
    bindings.set(token, state);
    return state;
  }

  function classification(ctx) {
    const expected = topologyMode[launch.topology];
    if (ctx.mode === expected) return { scope: "primary", mode: expected };
    if (ctx.mode === "print") return { scope: "child", mode: "print" };
    throw new Error("OMP native mode does not match a managed owner scope");
  }

  function readyFactory(factory, pi, ctx) {
    if (factory.current) throw new Error("OMP native factory started twice");
    const { scope, mode } = classification(ctx);
    if (scope === "primary" && primary) throw new Error("OMP primary factory is already bound");
    const state = makeState(factory, pi, ctx, scope, mode);
    factory.current = state;
    if (scope === "primary") primary = state;
    const description = describeState(state);
    const params = {
      topology: launch.topology, directory: launch.directory, scope, mode,
      owner_token: state.ownerToken, session_id: state.sessionID, name: description.name,
    };
    const queued = enqueueReport(factory, state, "owner.ready", params, (result) => {
      exactEcho(result, state, {});
      if (state.failure || state.ending) throw state.failure ?? new Error("OMP owner.ready crossed factory shutdown");
      state.admitted = true;
    });
    if (!queued) throw state.failure ?? new Error("OMP owner.ready was not queued");
  }

  function switchFactory(factory, event, ctx) {
    const previous = factory.current;
    if (!previous || previous.scope !== "primary" || ctx.mode !== previous.mode || !switchReasons.includes(event.reason)) {
      throw new Error("OMP native session switch does not own the primary factory");
    }
    previous.ctx = ctx;
    previous.ending = true;
    discardDeliveries(previous);
    const next = makeState(factory, previous.pi, ctx, "primary", previous.mode);
    next.previous = previous;
    if (next.sessionID === previous.sessionID && event.reason !== "resume") {
      removeState(next);
      throw new Error("OMP native session switch retained its identity");
    }
    factory.current = next;
    primary = next;
    const params = {
      topology: launch.topology, scope: "primary", mode: next.mode,
      previous_owner_token: previous.ownerToken, owner_token: next.ownerToken,
      previous_session_id: previous.sessionID, session_id: next.sessionID,
      name: describeState(next).name, reason: event.reason,
    };
    const queued = enqueueReport(factory, next, "owner.switch", params, (result) => {
      exactEcho(result, next, {});
      if (next.failure || next.ending) throw next.failure ?? new Error("OMP owner.switch crossed factory shutdown");
      next.admitted = true;
      removeState(previous);
      next.previous = undefined;
    });
    if (!queued) throw next.failure ?? new Error("OMP owner.switch was not queued");
  }

  function localState(factory, ctx) {
    const state = factory.current;
    if (!state || ctx.mode !== state.mode || state.failure || state.ending) {
      throw state?.failure ?? new Error("OMP native factory is not admitted");
    }
    state.ctx = ctx;
    describeState(state);
    rejectNewResetBoundary(state, ctx);
    return state;
  }

  function currentState(factory, ctx) {
    const state = localState(factory, ctx);
    if (!state.admitted) throw new Error("OMP native factory is not admitted");
    return state;
  }

  function nextReport(state) {
    state.reportSequence += 1;
    if (!Number.isSafeInteger(state.reportSequence)) throw new Error("OMP owner report sequence is exhausted");
    return state.reportSequence;
  }

  function schedulePreflight(factory, state, prompt) {
    state.runSequence += 1;
    const runToken = ownerToken(`${state.ownerToken}-run-${state.runSequence}`);
    // A lane steer must carry this token until the run yields.
    state.runToken = runToken;
    const sequence = nextReport(state);
    const params = {
      owner_token: state.ownerToken, session_id: state.sessionID, report_sequence: sequence,
      run_token: runToken, prompt,
    };
    if (!enqueueReport(factory, state, "run.preflight", params, (result) => {
      exactEcho(result, state, { report_sequence: sequence, run_token: runToken });
    })) throw state.failure ?? new Error("OMP run.preflight was not queued");
  }

  function scheduleDelivery(factory, state, batch, phase) {
    const sequence = nextReport(state);
    const params = {
      owner_token: state.ownerToken, session_id: state.sessionID, report_sequence: sequence,
      batch_token: batch.batchToken, phase, message_ids: [...batch.messageIDs],
    };
    if (!enqueueReport(factory, state, "delivery.observe", params, (result) => {
      exactEcho(result, state, {
        report_sequence: sequence, batch_token: batch.batchToken, phase, message_ids: batch.messageIDs,
      });
    })) throw state.failure ?? new Error("OMP delivery observation was not queued");
  }

  function claimDelivery(factory, state) {
    if (state.queued.length === 0) return undefined;
    const entries = [];
    let contentBytes = 0;
    for (const entry of state.queued) {
      const separator = entries.length === 0 ? 0 : 2;
      const next = Buffer.byteLength(entry.body) + separator;
      if (entries.length >= limit.batchItems || contentBytes + next > limit.batchBytes) break;
      entries.push(entry);
      contentBytes += next;
    }
    if (entries.length === 0) throw new Error("OMP staged delivery cannot fit a native batch");
    if (!reserve(contentBytes)) throw new Error("OMP native batch retained-payload capacity is exhausted");
    state.deliveryBytes += contentBytes;
    state.queued = state.queued.slice(entries.length);
    state.batchSequence += 1;
    const batch = {
      entries,
      messageIDs: entries.map((entry) => entry.messageID),
      content: entries.map((entry) => entry.body).join("\n\n"),
      contentBytes,
      batchToken: ownerToken(`${state.ownerToken}-batch-${state.batchSequence}`),
      phase: 0,
    };
    state.batches.set(batch.batchToken, batch);
    scheduleDelivery(factory, state, batch, "claimed");
    batch.phase = 1;
    return customDeliveryMessage(state, batch);
  }

  function scheduleDeliveryTurn(factory, state) {
    while (state.queued.length > 0) {
      try {
        const message = claimDelivery(factory, state);
        state.pi.sendMessage(message, { deliverAs: "steer", triggerTurn: true });
      } catch (error) {
        failState(state, error);
        throw error;
      }
    }
  }

  function observeMessage(factory, ctx, message, phase) {
    const state = localState(factory, ctx);
    const matches = [...state.batches.values()].filter((batch) => matchingDelivery(message, state, batch));
    if (matches.length === 0) {
      // A changed copy of a claimed batch: that attempt is uncertain. Only
      // it ends; the session, the peer and later deliveries stay.
      abandonBatches(factory, state,
        [...state.batches.values()].filter((batch) => referencesDeliveryBatch(message, state, batch)));
      return;
    }
    if (matches.length !== 1) {
      abandonBatches(factory, state, matches);
      return;
    }
    const batch = matches[0];
    // OMP hands a message's start and end to extensions on concurrent paths,
    // each through every earlier extension's handlers for that event first
    // (agent.ts 18.13.2:2233-2245; agent-session.ts:3895-3900; runner.ts
    // :1740-1750). An extension bound after startup (runner.ts:1111-1142)
    // runs after OMP's own message_start handlers, so its message_end can
    // arrive first. Hold that end and report both in their order.
    if (phase === "message_end" && batch.phase === 1 && !batch.endFirst) {
      batch.endFirst = true;
      return;
    }
    const expected = phase === "message_start" ? 1 : 2;
    if (batch.phase !== expected) {
      abandonBatches(factory, state, [batch]);
      return;
    }
    scheduleDelivery(factory, state, batch, phase);
    batch.phase = expected + 1;
    if (batch.phase === 2 && batch.endFirst) {
      scheduleDelivery(factory, state, batch, "message_end");
      batch.phase = 3;
    }
    // The provider context already held this batch (see observeContext), so
    // its message_end completes it.
    if (batch.phase === 3 && batch.contextSeen) {
      scheduleDelivery(factory, state, batch, "context");
      batch.phase = 4;
      completeBatch(state, batch);
      scheduleDeliveryTurn(factory, state);
    }
  }

  function completeBatch(state, batch) {
    for (const entry of batch.entries) {
      const bytes = deliveryEntryBytes(entry);
      state.deliveryBytes -= bytes;
      release(bytes);
    }
    state.deliveryBytes -= batch.contentBytes;
    release(batch.contentBytes);
    state.batches.delete(batch.batchToken);
  }

  // abandonBatches ends claimed batches whose native events no longer match
  // what we sent. They were handed to OMP, so their receipts stand and they
  // are never replayed; the owner stops tracking them, and unclaimed
  // deliveries stay eligible for a later turn.
  function abandonBatches(factory, state, batches) {
    if (batches.length === 0) return;
    for (const batch of batches) {
      scheduleDelivery(factory, state, batch, "abandoned");
      completeBatch(state, batch);
    }
    scheduleDeliveryTurn(factory, state);
  }

  function observeContext(factory, ctx, messages) {
    const state = localState(factory, ctx);
    const list = Array.isArray(messages) ? messages : [];
    const completed = [];
    const abandoned = [];
    for (const batch of state.batches.values()) {
      const matches = list.filter((message) => matchingDelivery(message, state, batch));
      const referenced = list.some((message) => referencesDeliveryBatch(message, state, batch));
      if (batch.phase < 3) {
        // A different active or preparing native turn may emit context while
        // this independently submitted batch has not emitted its own message
        // events. Ignore only context that does not reference this batch.
        if (matches.length === 0 && !referenced) continue;
        // OMP builds the provider context, and runs this hook, before it emits
        // the turn's input message events (agent-core 18.11.0
        // agent-loop.ts:1312-1328, 1360, 1412-1416; the same in 18.1.17), for
        // an idle prompt and for a steer drained at a turn boundary alike. One
        // exact copy is that order: note it, and complete at message_end. Any
        // other reference changed the delivery's identity.
        const references = list.filter((message) => referencesDeliveryBatch(message, state, batch)).length;
        if (matches.length !== 1 || references !== 1) {
          abandoned.push(batch);
          continue;
        }
        batch.contextSeen = true;
        continue;
      }
      if (matches.length !== 1 || batch.phase !== 3) {
        abandoned.push(batch);
        continue;
      }
      scheduleDelivery(factory, state, batch, "context");
      batch.phase = 4;
      completed.push(batch);
    }
    for (const batch of completed) completeBatch(state, batch);
    if (abandoned.length > 0) abandonBatches(factory, state, abandoned);
    else scheduleDeliveryTurn(factory, state);
  }

  async function endFactory(factory, ctx) {
    const state = factory.current;
    if (!state || state.ended) return;
    if (ctx.mode !== state.mode || nativeID(ctx.sessionManager.getSessionId()) !== state.sessionID) {
      throw new Error("OMP native shutdown does not own the current factory");
    }
    state.ctx = ctx;
    state.ending = true;
    discardDeliveries(state);
    const params = {
      topology: launch.topology, scope: state.scope, mode: state.mode,
      owner_token: state.ownerToken, session_id: state.sessionID, reason: "shutdown",
    };
    let endError = state.failure ?? factory.failed;
    if (!enqueueReport(factory, state, "session_end", params, (result) => {
      exactEcho(result, state, {});
      state.endAcknowledged = true;
    }, { ending: true })) {
      endError = state.failure ?? new Error("OMP session_end was not queued");
    }
    try {
      await joinReports(factory);
      if (!state.endAcknowledged) throw endError ?? new Error("OMP session_end was not acknowledged");
      if (endError) throw endError;
      if (state.scope === "primary") bridgeClosing = true;
    } catch (error) {
      endError ??= error;
      failState(state, error);
      throw error;
    } finally {
      removeState(state);
      factory.current = undefined;
      factory.failed = endError;
    }
  }

  function bindFactory(pi) {
    if (!launch) throw new Error("OMP managed launch metadata is missing");
    const factory = { pi, current: undefined, reports: [], worker: undefined, workerError: undefined, failed: undefined };
    factories.add(factory);

    pi.registerTool({
      name: toolName,
      label: "Sessionbus",
      description: toolDescription,
      parameters: toolParameters(),
      // The public action envelope has optional fields. Native providers must
      // receive explicit non-strict mode rather than synthesize absent fields.
      strict: false,
      // Sessionbus includes mutating actions and delegation; keep the honest
      // exec tier while granting only this tool in every managed factory.
      loadMode: "essential",
      approval: { tier: "exec", policy: "allow" },
      async execute(callID, params, signal, _onUpdate, ctx) {
        throwIfAborted(signal);
        const state = localState(factory, ctx);
        await joinReports(factory);
        throwIfAborted(signal);
        currentState(factory, ctx);
        toolCallID(callID);
        exactKeys(params, ["action", "arguments"], "Sessionbus tool arguments");
        if (!actions.includes(params.action) || !object(params.arguments)) {
          throw new Error("OMP Sessionbus tool arguments are invalid");
        }
        let response;
        try {
          response = await callHost(state, "tool.call", {
            owner_token: state.ownerToken, session_id: state.sessionID, call_id: callID,
            action: params.action, arguments: params.arguments,
          }, signal);
        } catch (error) {
          if (!(error instanceof BridgeCallError) || error.code !== "tool_error") failState(state, error);
          throw error;
        }
        currentState(factory, ctx);
        exactKeys(response, ["owner_token", "session_id", "call_id", "result"], "tool.call response");
        if (response.owner_token !== state.ownerToken || response.session_id !== state.sessionID ||
            response.call_id !== callID || !object(response.result)) {
          throw new Error("OMP tool.call response is invalid");
        }
        const text = JSON.stringify(response.result);
        if (typeof text !== "string" || Buffer.byteLength(text) > maxTextBytes) {
          throw new Error("OMP Sessionbus result exceeds its native bound");
        }
        return { content: [{ type: "text", text }], details: response };
      },
    });

    pi.on("session_start", (_event, ctx) => {
      try { readyFactory(factory, pi, ctx); } catch (error) {
        if (factory.current) failState(factory.current, error);
        else failStartup(factory, ctx, error);
      }
    });

    pi.on("session_switch", (event, ctx) => {
      try { switchFactory(factory, event, ctx); } catch (error) { failState(factory.current, error); }
    });

    pi.on("before_agent_start", (event, ctx) => {
      try {
        const state = localState(factory, ctx);
        const prompt = boundedText(event.prompt, maxTextBytes, "native preflight prompt", { empty: true });
        if (state.scope === "primary" && launch.topology === "lane") schedulePreflight(factory, state, prompt);
        return undefined;
      } catch (error) {
        failState(factory.current, error);
        return undefined;
      }
    });

    // A yielded end, by OMP's own rule (agent-session.ts:5063-5067), ends the
    // lane run that a steer may join; an end that continues does not.
    pi.on("agent_end", (event, ctx) => {
      const state = factory.current;
      if (launch.topology === "lane" && state?.scope === "primary" &&
          (event?.willContinue !== true || event?.awaitingAsyncWork === true)) state.runToken = undefined;
      // A message_end held for a message_start that never came (see
      // observeMessage) ends with the run that produced it.
      const held = (batch) => batch.endFirst && batch.phase === 1;
      if (!state || state.failure || state.ending || ![...state.batches.values()].some(held)) return;
      try {
        // localState can itself abandon every pending batch on a reset, so
        // the held ones are read after it.
        const live = localState(factory, ctx);
        abandonBatches(factory, live, [...live.batches.values()].filter(held));
      } catch (error) {
        failState(factory.current, error);
      }
    });

    pi.on("message_start", (event, ctx) => {
      try { observeMessage(factory, ctx, event.message, "message_start"); } catch (error) { failState(factory.current, error); }
    });

    pi.on("message_end", (event, ctx) => {
      try { observeMessage(factory, ctx, event.message, "message_end"); } catch (error) { failState(factory.current, error); }
    });

    pi.on("context", (event, ctx) => {
      try { observeContext(factory, ctx, event.messages); } catch (error) { failState(factory.current, error); }
    });

    pi.on("session_shutdown", async (_event, ctx) => {
      try { await endFactory(factory, ctx); } catch (error) { failState(factory.current, error); throw error; }
      finally { factories.delete(factory); }
    });
  }

  Object.defineProperty(bindFactory, "stats", {
    enumerable: false,
    value: () => Object.freeze({ bindings: bindings.size, reports: reportItems, retainedBytes }),
  });
  return bindFactory;
}

let processExtension = globalThis[processExtensionKey];
if (processExtension === undefined) {
  const capturedLaunch = captureLaunch();
  processExtension = createOMPExtension({ launch: capturedLaunch });
  if (capturedLaunch !== null) {
    Object.defineProperty(globalThis, processExtensionKey, {
      configurable: false,
      enumerable: false,
      writable: false,
      value: processExtension,
    });
  }
}
export default processExtension;
