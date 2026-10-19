// SPDX-License-Identifier: MIT
import assert from "node:assert/strict";
import { once } from "node:events";
import fs from "node:fs";
import { mkdtemp, rm } from "node:fs/promises";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import test from "node:test";

import kit from "../../pi/node_modules/@sessionbus/kit/sdk/js/index.js";
import { BridgeCallError } from "../pifamily/extension/bridge.mjs";
import {
  captureLaunch,
  createPiExtension,
  launchEnvironmentName,
  toolName,
} from "./extension.mjs";
import { connectPiInteractivePeer } from "./peer.mjs";

function deferred() {
  let resolve;
  let reject;
  const promise = new Promise((res, rej) => { resolve = res; reject = rej; });
  promise.catch(() => {});
  return { promise, resolve, reject };
}

function immediate() {
  return new Promise((resolve) => setImmediate(resolve));
}

function nativeFixture(id = "native-1", mode = "rpc") {
  const handlers = new Map();
  let name = "Pi title";
  let leaf;
  let pending;
  let idle = true;
  let aborted = 0;
  let abortHook;
  let shutdown = 0;
  const notices = [];
  const shutdownSignal = deferred();
  const createContext = () => ({
    mode,
    cwd: "/project",
    isIdle: () => idle,
    hasUI: true,
    ui: { notify: (message, type) => notices.push({ message, type }) },
    abort: () => { aborted++; abortHook?.(); },
    shutdown: () => { shutdown++; shutdownSignal.resolve(); },
    sessionManager: {
      getSessionId: () => id,
      getLeafId: () => leaf?.id ?? null,
      getLeafEntry: () => leaf,
    },
  });
  const ctx = createContext();
  const pi = {
    tool: undefined,
    registerTool(value) { this.tool = value; },
    on(event, handler) {
      const list = handlers.get(event) ?? [];
      list.push(handler);
      handlers.set(event, list);
    },
    getSessionName: () => name,
    sendMessage(message, options) {
      assert.deepEqual(options, { triggerTurn: true });
      pending = { role: "custom", ...message };
      idle = false;
    },
  };
  const emit = async (type, event = { type }) => {
    // Pi's real runner creates a distinct ExtensionContext for every event.
    const eventContext = createContext();
    let result;
    for (const handler of handlers.get(type) ?? []) result = await handler(event, eventContext);
    return result;
  };
  return {
    pi,
    ctx,
    setID(value) { id = value; },
    setName(value) { name = value; },
    setIdle(value) { idle = value; },
    onAbort(hook) { abortHook = hook; },
    setLeaf(value) { leaf = value === null ? undefined : { id: value }; },
    pending: () => pending,
    async completeMessage() {
      assert.ok(pending);
      const message = pending;
      await emit("message_start", { type: "message_start", message });
      await emit("message_end", { type: "message_end", message });
      leaf = { id: `entry-${id}-${leaf ? 2 : 1}`, parentId: leaf?.id ?? null, type: "custom_message", ...message };
      pending = undefined;
      idle = true;
    },
    leaf: () => leaf,
    aborted: () => aborted,
    shutdown: () => shutdown,
    notices: () => notices,
    waitShutdown: () => shutdownSignal.promise,
    emit,
  };
}

function launch(topology) {
  if (topology === "lane") return { bridge_fd: 3, topology };
  return { socket: "/public/bus.sock", name: "managed", groups: ["team"], topology };
}

// A run token as Go mints it: 32 hex digits.
function runToken(index) {
  return index.toString(16).padStart(32, "0");
}

function fakeConnection({ topology, queue = [], preflightRuns } = {}) {
  const calls = [];
  let preflights = 0;
  const done = deferred();
  let nativeHandler;
  let closes = 0;
  const bridge = {
    done: done.promise,
    async call(method, params, { signal } = {}) {
      if (signal?.aborted) throw signal.reason;
      calls.push({ method, params });
      switch (method) {
        case "run.preflight":
          preflights++;
          return { session_id: params.session_id, run: preflightRuns ? preflightRuns.shift() : runToken(preflights) };
        case "owner.ready":
        case "session_end":
        case "run.input":
        case "run.start":
        case "run.settling":
          return { session_id: params.session_id };
        case "tool.call":
          return { session_id: params.session_id, call_id: params.call_id, result: { action: params.action, arguments: params.arguments } };
        case "owner.drain": {
          let drained = 0;
          while (queue.length) {
            const delivery = queue[0];
            const result = await nativeHandler({ method: "native.append", params: delivery, signal });
            if (!result.accepted) break;
            queue.shift();
            drained++;
          }
          return { session_id: params.session_id, drained };
        }
    case "owner.before_tree":
      return { session_id: params.session_id, pending: queue.length !== 0 };
        default:
          throw new Error(`unexpected host method ${method}`);
      }
    },
    async close() {
      closes++;
      done.resolve();
    },
  };
  return {
    bridge,
    calls,
    closes: () => closes,
    end: () => done.resolve(),
    connect: async (_socket, options) => {
      assert.equal(options.role, "native");
      nativeHandler = options.handler;
      assert.equal(options.signal.aborted, false);
      return bridge;
    },
    native: (method, params, signal) => nativeHandler({ method, params, signal }),
  };
}

test("launch capture scrubs and validates inherited lane descriptors", () => {
  const value = { bridge_fd: 3, topology: "lane" };
  const environment = { [launchEnvironmentName]: JSON.stringify(value) };
  assert.deepEqual(captureLaunch(environment), value);
  assert.equal(Object.hasOwn(environment, launchEnvironmentName), false);

  for (const invalid of [
    { ...value, bridge_fd: 2 },
    { ...value, bridge_fd: 3.5 },
    { ...value, topology: "other" },
    { ...value, extra: true },
  ]) {
    const candidate = { [launchEnvironmentName]: JSON.stringify(invalid) };
    assert.throws(() => captureLaunch(candidate), /metadata/);
    assert.equal(Object.hasOwn(candidate, launchEnvironmentName), false);
  }
});

test("interactive launch capture scrubs immutable public bootstrap", () => {
  const value = { socket: "/public/bus.sock", name: "managed", groups: ["team"], topology: "interactive" };
  const environment = { [launchEnvironmentName]: JSON.stringify(value) };
  assert.deepEqual(captureLaunch(environment), value);
  assert.equal(Object.hasOwn(environment, launchEnvironmentName), false);

  for (const invalid of [
    { ...value, socket: "relative" },
    { ...value, groups: [""] },
    { ...value, topology: "other" },
    { ...value, extra: true },
  ]) {
    const candidate = { [launchEnvironmentName]: JSON.stringify(invalid) };
    assert.throws(() => captureLaunch(candidate), /metadata|socket|groups/);
    assert.equal(Object.hasOwn(candidate, launchEnvironmentName), false);
  }
});

test("lane handshake and ordered witnesses retain native ownership", async () => {
  const native = nativeFixture();
  const owner = fakeConnection({ topology: "lane" });
  createPiExtension({ launch: launch("lane"), connect: owner.connect })(native.pi);
  assert.equal(native.pi.tool.name, toolName);
  assert.deepEqual(native.pi.tool.parameters.properties.action.enum, [
    "list", "send", "spawn", "describe", "trace", "run", "start", "wait", "status", "interrupt", "close", "forget", "ack",
  ]);

  await native.emit("session_start", { type: "session_start", reason: "startup" });
  assert.deepEqual(owner.calls.shift(), {
    method: "owner.ready",
    params: { topology: "lane", session_id: "native-1", name: "Pi title" },
  });
  assert.deepEqual(await owner.native("native.describe", { session_id: "native-1" }), {
    session_id: "native-1", name: "Pi title", cwd: "/project",
  });
  await assert.rejects(owner.native("native.append", {
    session_id: "native-1", message_id: "message", body: "body",
  }), /unavailable in lane/);

  assert.deepEqual(await native.emit("input", { type: "input", source: "rpc", text: "original" }), { action: "continue" });
  await native.emit("before_agent_start", { type: "before_agent_start", prompt: "expanded" });
  await native.emit("agent_start");
  await native.emit("agent_settled");
  // Work that starts after the settle is Pi's own and is not reported.
  await native.emit("agent_start");
  assert.deepEqual(owner.calls.splice(0), [
    { method: "run.input", params: { session_id: "native-1", source: "rpc", text: "original", settling: false } },
    { method: "run.preflight", params: { session_id: "native-1", prompt: "expanded", settling: false, leaf: null, settled: 0 } },
    { method: "run.start", params: { session_id: "native-1", run: runToken(1), settling: false } },
    { method: "run.settling", params: { session_id: "native-1", run: runToken(1), leaf: null } },
  ]);

  const result = await native.pi.tool.execute("call-1", { action: "list", arguments: {} }, undefined, undefined, native.ctx);
  assert.deepEqual(result, {
    content: [{ type: "text", text: '{"action":"list","arguments":{}}' }],
    details: { session_id: "native-1", call_id: "call-1", result: { action: "list", arguments: {} } },
  });

  native.setName("");
  await native.emit("session_info_changed", { type: "session_info_changed", name: undefined });
  assert.deepEqual(owner.calls.splice(0), [
    { method: "tool.call", params: { session_id: "native-1", call_id: "call-1", action: "list", arguments: {} } },
    { method: "owner.ready", params: { topology: "lane", session_id: "native-1", name: "" } },
  ]);
  await native.emit("session_shutdown", { type: "session_shutdown", reason: "reload" });
  assert.deepEqual(owner.calls.shift(), {
    method: "session_end", params: { topology: "lane", session_id: "native-1", reason: "reload" },
  });
  assert.equal(owner.closes(), 0);
});

// A lane puts a peer message in Pi's steering queue only while its own run is
// active (before_agent_start to agent_settled) and only for that run's token,
// checked in the same step as the send; the steered message's events match
// its admitted delivery. Pi reads that queue at the running task's next
// boundary (agent-core agent-loop.js:183-195).
test("lane native.steer steers only for the running Run's token", async () => {
  const native = nativeFixture();
  const sends = [];
  native.pi.sendMessage = (message, options) => { sends.push({ message, options }); };
  const owner = fakeConnection({ topology: "lane" });
  createPiExtension({ launch: launch("lane"), connect: owner.connect })(native.pi);
  await native.emit("session_start", { type: "session_start", reason: "startup" });
  const steer = (id, run) => owner.native("native.steer", { session_id: "native-1", message_id: id, body: "peer", run });
  const idle = (id) => ({ session_id: "native-1", message_id: id, accepted: false, reason: "idle" });
  assert.deepEqual(await steer("early", runToken(1)), idle("early"));

  await native.emit("input", { type: "input", source: "rpc", text: "first" });
  await native.emit("before_agent_start", { type: "before_agent_start", prompt: "first" });
  native.setIdle(false);
  assert.deepEqual(await steer("steer-1", runToken(1)), { session_id: "native-1", message_id: "steer-1", accepted: true });
  assert.deepEqual(sends, [{
    message: { customType: "sessionbus.message", content: "peer", display: true, details: { message_id: "steer-1" } },
    options: { triggerTurn: true, deliverAs: "steer" },
  }]);
  const message = { role: "custom", customType: "sessionbus.message", content: "peer", details: { message_id: "steer-1" } };
  await native.emit("message_start", { type: "message_start", message });
  await native.emit("message_end", { type: "message_end", message });
  assert.equal(native.shutdown(), 0);
  assert.equal(native.aborted(), 0);

  native.setIdle(true);
  await native.emit("agent_settled");
  assert.deepEqual(await steer("late", runToken(1)), idle("late"));

  // A steer made for the first Run that arrives during the next Run is
  // refused; the next Run's own steer is accepted.
  await native.emit("input", { type: "input", source: "rpc", text: "second" });
  await native.emit("before_agent_start", { type: "before_agent_start", prompt: "second" });
  native.setIdle(false);
  assert.deepEqual(await steer("stale", runToken(1)), idle("stale"));
  assert.deepEqual(await steer("steer-2", runToken(2)), { session_id: "native-1", message_id: "steer-2", accepted: true });
  assert.deepEqual(sends.map(({ message: sent }) => sent.details.message_id), ["steer-1", "steer-2"]);
  await assert.rejects(owner.native("native.steer", { session_id: "native-1", message_id: "no-run", body: "peer" }), /fields are invalid/);
});

// Only the lane's own rpc prompt starts its Run (owner decision: the Run ends
// when Pi settles). Input from elsewhere, a run a custom message starts, and
// any run after a settle until the next rpc prompt are Pi's own work: none is
// reported or gets a token, and a run Go answers null for is not reported
// further. The preflight carries the leaf and the count of settled Pi runs;
// the settle carries the leaf where the owned run ended.
test("lane reports only its own rpc run, with the settle count and leaves", async () => {
  const native = nativeFixture();
  const owner = fakeConnection({ topology: "lane", preflightRuns: [runToken(1), null] });
  createPiExtension({ launch: launch("lane"), connect: owner.connect })(native.pi);
  await native.emit("session_start", { type: "session_start", reason: "startup" });
  owner.calls.splice(0);

  native.setLeaf("before-run");
  await native.emit("input", { type: "input", source: "rpc", text: "owned" });
  assert.deepEqual(await native.emit("input", { type: "input", source: "extension", text: "mid-run" }), { action: "continue" });
  await native.emit("before_agent_start", { type: "before_agent_start", messageType: "user", prompt: "owned" });
  await native.emit("agent_start");
  native.setLeaf("owned-answer");
  await native.emit("agent_settled");

  assert.deepEqual(await native.emit("input", { type: "input", source: "extension", text: "own" }), { action: "continue" });
  await native.emit("before_agent_start", { type: "before_agent_start", messageType: "user", prompt: "own" });
  await native.emit("agent_start");
  await native.emit("agent_settled");
  await native.emit("before_agent_start", {
    type: "before_agent_start", messageType: "custom", message: { role: "custom", customType: "other", content: "own" },
  });
  await native.emit("agent_start");
  native.setLeaf("own-answer");
  await native.emit("agent_settled");
  assert.deepEqual(owner.calls.splice(0), [
    { method: "run.input", params: { session_id: "native-1", source: "rpc", text: "owned", settling: false } },
    { method: "run.preflight", params: { session_id: "native-1", prompt: "owned", settling: false, leaf: "before-run", settled: 0 } },
    { method: "run.start", params: { session_id: "native-1", run: runToken(1), settling: false } },
    { method: "run.settling", params: { session_id: "native-1", run: runToken(1), leaf: "owned-answer" } },
  ]);

  // The next preflight counts every settled run, the lane's and Pi's own.
  await native.emit("input", { type: "input", source: "rpc", text: "next" });
  await native.emit("before_agent_start", { type: "before_agent_start", messageType: "user", prompt: "next" });
  await native.emit("agent_start");
  await native.emit("agent_settled");
  assert.deepEqual(owner.calls.splice(0), [
    { method: "run.input", params: { session_id: "native-1", source: "rpc", text: "next", settling: false } },
    { method: "run.preflight", params: { session_id: "native-1", prompt: "next", settling: false, leaf: "own-answer", settled: 3 } },
  ]);
  assert.equal(native.shutdown(), 0);
  assert.equal(native.aborted(), 0);
});

// An interactive session has no Run: a peer message goes into Pi's steering
// queue while a native run is active (before_agent_start to agent_settled),
// and run must be null.
test("interactive native.steer steers into the running task and refuses an idle session", async () => {
  const native = nativeFixture("interactive-1", "tui");
  const sends = [];
  native.pi.sendMessage = (message, options) => { sends.push({ message, options }); };
  const owner = fakeConnection({ topology: "interactive" });
  createPiExtension({ launch: launch("interactive"), connect: owner.connect })(native.pi);
  await native.emit("session_start", { type: "session_start", reason: "startup" });
  const steer = (id, run = null) => owner.native("native.steer", { session_id: "interactive-1", message_id: id, body: "peer", run });
  const idle = (id) => ({ session_id: "interactive-1", message_id: id, accepted: false, reason: "idle" });
  assert.deepEqual(await steer("early"), idle("early"));

  await native.emit("before_agent_start", { type: "before_agent_start", prompt: "typed" });
  native.setIdle(false);
  assert.deepEqual(await steer("steer-1"), { session_id: "interactive-1", message_id: "steer-1", accepted: true });
  assert.deepEqual(sends, [{
    message: { customType: "sessionbus.message", content: "peer", display: true, details: { message_id: "steer-1" } },
    options: { triggerTurn: true, deliverAs: "steer" },
  }]);
  await assert.rejects(steer("token", runToken(1)), /run must be null/);

  native.setIdle(true);
  await native.emit("agent_settled");
  assert.deepEqual(await steer("late"), idle("late"));
  assert.equal(sends.length, 1);
  assert.equal(native.shutdown(), 0);
});

// DEF-04 (owner 2026-10-19, "Clear the queue first"): at an aborted settle a
// steered message whose message_start never fired is written to history with a
// plain send, after one ctx.abort() has cleared any queued copy. A started one
// is left as it is, an appended one is not a steer, and a settle with nothing
// to keep or without an abort changes nothing. Before this change every such
// message was dropped.
test("interactive aborted settle clears Pi's queue once and writes never-started steers to history", async () => {
  const native = nativeFixture("interactive-1", "tui");
  const sends = [];
  native.pi.sendMessage = (message, options) => { sends.push({ message, options, aborts: native.aborted() }); };
  const owner = fakeConnection({ topology: "interactive" });
  createPiExtension({ launch: launch("interactive"), connect: owner.connect })(native.pi);
  await native.emit("session_start", { type: "session_start", reason: "startup" });
  const peer = (id) => ({ customType: "sessionbus.message", content: `body ${id}`, display: true, details: { message_id: id } });
  const accepted = (id) => ({ session_id: "interactive-1", message_id: id, accepted: true });
  const steer = (id) => owner.native("native.steer", { session_id: "interactive-1", message_id: id, body: `body ${id}`, run: null });

  assert.deepEqual(await owner.native("native.append", {
    session_id: "interactive-1", message_id: "appended", body: "body appended",
  }), accepted("appended"));
  await native.emit("before_agent_start", { type: "before_agent_start", prompt: "typed" });
  native.setIdle(false);
  for (const id of ["started", "completed", "never-1", "never-2"]) assert.deepEqual(await steer(id), accepted(id));
  await native.emit("message_start", { type: "message_start", message: { role: "custom", ...peer("started") } });
  await native.emit("message_start", { type: "message_start", message: { role: "custom", ...peer("completed") } });
  await native.emit("message_end", { type: "message_end", message: { role: "custom", ...peer("completed") } });
  assert.equal(sends.length, 5);

  native.setIdle(true);
  await native.emit("agent_settled", { type: "agent_settled", aborted: true });
  assert.equal(native.aborted(), 1);
  assert.deepEqual(sends.slice(5), [
    { message: peer("never-1"), options: undefined, aborts: 1 },
    { message: peer("never-2"), options: undefined, aborts: 1 },
  ]);

  // Nothing is left to keep, so a second aborted settle clears nothing.
  await native.emit("before_agent_start", { type: "before_agent_start", prompt: "again" });
  await native.emit("agent_settled", { type: "agent_settled", aborted: true });
  assert.equal(native.aborted(), 1);
  assert.equal(sends.length, 7);

  // A settle without an abort keeps the earlier behaviour; it is listed as
  // not covered.
  await native.emit("before_agent_start", { type: "before_agent_start", prompt: "third" });
  native.setIdle(false);
  assert.deepEqual(await steer("unaborted"), accepted("unaborted"));
  native.setIdle(true);
  await native.emit("agent_settled", { type: "agent_settled", aborted: false });
  assert.equal(native.aborted(), 1);
  assert.equal(sends.length, 8);
  assert.equal(native.shutdown(), 0);
});

// Pi's queue at an aborted settle, modelled: a mid-run /compact aborts without
// clearing it, so one steer is still queued; in the P3 case the loop had
// dequeued the other and lost it to a throw before its message_start. In both
// the message ends up once in history and not in the queue, so no later run
// delivers it again. Before this change the first stayed queued and the
// second was lost.
test("interactive aborted settle leaves one history copy and no queued copy of a never-started steer", async () => {
  const native = nativeFixture("interactive-1", "tui");
  const queue = [];
  const history = [];
  native.pi.sendMessage = (message, options) => {
    if (options?.deliverAs === "steer") queue.push(message.details.message_id);
    else if (options === undefined) history.push(message.details.message_id);
    else assert.fail(`unexpected send options ${JSON.stringify(options)}`);
  };
  // The TUI's ctx.abort() runs restoreQueuedMessagesToEditor({abort:true}),
  // which clears Pi's queues (Pi 1.2.0 interactive-mode.js:1732-1734, 3891-3910).
  native.onAbort(() => { queue.length = 0; });
  const owner = fakeConnection({ topology: "interactive" });
  createPiExtension({ launch: launch("interactive"), connect: owner.connect })(native.pi);
  await native.emit("session_start", { type: "session_start", reason: "startup" });
  await native.emit("before_agent_start", { type: "before_agent_start", prompt: "typed" });
  native.setIdle(false);
  for (const id of ["compacted", "dequeued"]) {
    assert.equal((await owner.native("native.steer", {
      session_id: "interactive-1", message_id: id, body: `body ${id}`, run: null,
    })).accepted, true);
  }
  queue.splice(queue.indexOf("dequeued"), 1);
  assert.deepEqual(queue, ["compacted"]);

  native.setIdle(true);
  await native.emit("agent_settled", { type: "agent_settled", aborted: true });
  assert.deepEqual(queue, []);
  assert.deepEqual(history, ["compacted", "dequeued"]);
  assert.equal(native.shutdown(), 0);
});

// A lane's rpc abort leaves Pi's queue in place and the next run drains it
// (Pi 1.2.0 rpc-mode.js:327-329), so a lane's aborted settle neither clears
// nor writes anything.
test("lane aborted settle leaves a never-started steer to Pi", async () => {
  const native = nativeFixture();
  const sends = [];
  native.pi.sendMessage = (message, options) => { sends.push({ message, options }); };
  const owner = fakeConnection({ topology: "lane" });
  createPiExtension({ launch: launch("lane"), connect: owner.connect })(native.pi);
  await native.emit("session_start", { type: "session_start", reason: "startup" });
  await native.emit("input", { type: "input", source: "rpc", text: "first" });
  await native.emit("before_agent_start", { type: "before_agent_start", prompt: "first" });
  native.setIdle(false);
  assert.equal((await owner.native("native.steer", {
    session_id: "native-1", message_id: "steer-1", body: "peer", run: runToken(1),
  })).accepted, true);
  native.setIdle(true);
  await native.emit("agent_settled", { type: "agent_settled", aborted: true });
  assert.equal(native.aborted(), 0);
  assert.equal(sends.length, 1);
  assert.equal(native.shutdown(), 0);
});

test("interactive drains before a prompt and after a settled turn through nested native appends", async () => {
  const native = nativeFixture("interactive-1", "tui");
  const queue = [
    { session_id: "interactive-1", message_id: "before", body: "before prompt" },
  ];
  const owner = fakeConnection({ topology: "interactive", queue });
  createPiExtension({ launch: launch("interactive"), connect: owner.connect })(native.pi);
  await native.emit("session_start", { type: "session_start", reason: "startup" });
  owner.calls.length = 0;

  await native.emit("before_agent_start", { type: "before_agent_start", prompt: "native prompt" });
  assert.equal(queue.length, 0);
  assert.equal(native.pending().details.message_id, "before");
  assert.equal(native.leaf(), undefined);
  await native.completeMessage();
  assert.equal(native.leaf().details.message_id, "before");
  queue.push({ session_id: "interactive-1", message_id: "settled", body: "after turn" });
  await native.emit("agent_settled");
  assert.equal(queue.length, 0);
  assert.equal(native.pending().details.message_id, "settled");
  await native.completeMessage();
  assert.equal(native.leaf().details.message_id, "settled");
  assert.deepEqual(owner.calls, [
    { method: "owner.drain", params: { session_id: "interactive-1", witness: "before_agent_start" } },
    { method: "owner.drain", params: { session_id: "interactive-1", witness: "agent_settled" } },
  ]);

  native.setIdle(false);
  assert.deepEqual(await owner.native("native.append", {
    session_id: "interactive-1", message_id: "held", body: "still busy",
  }), { session_id: "interactive-1", message_id: "held", accepted: false, reason: "busy" });
  assert.deepEqual(await native.emit("input", { type: "input", source: "interactive", text: "ordinary" }), { action: "continue" });
  await native.emit("agent_start");
  assert.equal(owner.calls.length, 2);

  await native.emit("session_shutdown", { type: "session_shutdown", reason: "quit" });
  assert.equal(owner.closes(), 1);
});

// Pi 1.2.0 defers a triggerTurn send made during agent_settled handlers and
// stays idle until they finish (agent-session.js:1836-1838). The settled drain
// accepts one such send and keeps the rest queued, as Pi 0.85.1 did; outside
// that drain the busy check still applies.
test("interactive settled drain accepts one deferred Pi 1.2.0 send and keeps the rest queued", async () => {
  const native = nativeFixture("interactive-defer", "tui");
  const send = native.pi.sendMessage;
  native.pi.sendMessage = (message, options) => { send(message, options); native.setIdle(true); };
  const queue = [
    { session_id: "interactive-defer", message_id: "first", body: "one" },
    { session_id: "interactive-defer", message_id: "second", body: "two" },
  ];
  const owner = fakeConnection({ topology: "interactive", queue });
  createPiExtension({ launch: launch("interactive"), connect: owner.connect })(native.pi);
  await native.emit("session_start", { type: "session_start", reason: "startup" });
  await native.emit("agent_settled");
  assert.equal(native.shutdown(), 0);
  assert.equal(native.aborted(), 0);
  assert.equal(native.pending().details.message_id, "first");
  assert.deepEqual(queue.map(({ message_id }) => message_id), ["second"]);
  await native.emit("session_shutdown", { type: "session_shutdown", reason: "quit" });
});

// An idle delivery that lands while Pi still runs later settled handlers is
// deferred by Pi too (agent-session.js:1836-1838). It is accepted and does not
// end the session.
test("interactive idle delivery that Pi defers outside the settled drain is accepted", async () => {
  const native = nativeFixture("interactive-window", "tui");
  const send = native.pi.sendMessage;
  native.pi.sendMessage = (message, options) => { send(message, options); native.setIdle(true); };
  const owner = fakeConnection({ topology: "interactive" });
  createPiExtension({ launch: launch("interactive"), connect: owner.connect })(native.pi);
  await native.emit("session_start", { type: "session_start", reason: "startup" });
  assert.deepEqual(await owner.native("native.append", {
    session_id: "interactive-window", message_id: "window", body: "during later handlers",
  }), { session_id: "interactive-window", message_id: "window", accepted: true });
  assert.equal(native.pending().details.message_id, "window");
  assert.equal(native.shutdown(), 0);
  await native.emit("session_shutdown", { type: "session_shutdown", reason: "quit" });
});

test("interactive wakes after manual compaction and brackets unobservable branch summaries", async () => {
  const native = nativeFixture("interactive-compact", "tui");
  const queue = [];
  const owner = fakeConnection({ topology: "interactive", queue });
  createPiExtension({ launch: launch("interactive"), connect: owner.connect })(native.pi);
  await native.emit("session_start", { type: "session_start", reason: "startup" });
  owner.calls.length = 0;

  queue.push({ session_id: "interactive-compact", message_id: "compact-ok", body: "after compact" });
  await native.emit("session_compact", { type: "session_compact" });
  await immediate();
  assert.equal(queue.length, 0);
  assert.equal(native.pending().details.message_id, "compact-ok");
  assert.deepEqual(await native.emit("session_before_tree", { type: "session_before_tree" }), { cancel: true });
  await immediate();
  await native.completeMessage();

  queue.push({ session_id: "interactive-compact", message_id: "compact-failed", body: "after failure" });
  await native.emit("session_compact_failed", { type: "session_compact_failed", reason: "manual", aborted: true });
  assert.equal(queue.length, 0);
  assert.equal(native.pending().details.message_id, "compact-failed");
  await native.completeMessage();

  queue.push({ session_id: "interactive-compact", message_id: "tree-cancel", body: "drain first" });
  native.setIdle(false);
  assert.deepEqual(await native.emit("session_before_tree", { type: "session_before_tree" }), { cancel: true });
  native.setIdle(true);
  await immediate();
  assert.equal(queue.length, 0);
  assert.equal(native.pending().details.message_id, "tree-cancel");
  assert.equal(native.notices().length, 2);
  await native.completeMessage();

  native.setIdle(false);
  assert.equal(await native.emit("session_before_tree", { type: "session_before_tree" }), undefined);
  assert.deepEqual(await owner.native("native.append", {
    session_id: "interactive-compact", message_id: "during-tree", body: "must reject",
  }), {
    session_id: "interactive-compact", message_id: "during-tree",
    accepted: false, reason: "branch_summary_busy",
  });

  queue.push({ session_id: "interactive-compact", message_id: "tree-ok", body: "after tree" });
  await native.emit("session_tree", { type: "session_tree" });
  native.setIdle(true);
  await immediate();
  assert.equal(queue.length, 0);
  assert.equal(native.pending().details.message_id, "tree-ok");
  await native.completeMessage();

  assert.deepEqual(owner.calls.map(({ method, params }) => [method, params.witness]), [
    ["owner.drain", "session_compact"],
    ["owner.before_tree", undefined],
    ["owner.drain", "session_before_tree_cancelled"],
    ["owner.drain", "session_compact_failed"],
    ["owner.before_tree", undefined],
    ["owner.drain", "session_before_tree_cancelled"],
    ["owner.before_tree", undefined],
    ["owner.drain", "session_tree"],
  ]);
});

test("reload reuses one bridge and replaces the stale native context", async () => {
  const first = nativeFixture("old");
  const second = nativeFixture("new");
  const owner = fakeConnection({ topology: "lane" });
  const extension = createPiExtension({ launch: launch("lane"), connect: owner.connect });
  extension(first.pi);
  await first.emit("session_start", { type: "session_start", reason: "startup" });
  await first.emit("session_shutdown", { type: "session_shutdown", reason: "reload" });
  extension(second.pi);
  await second.emit("session_start", { type: "session_start", reason: "reload" });
  assert.deepEqual(await owner.native("native.describe", { session_id: "new" }), {
    session_id: "new", name: "Pi title", cwd: "/project",
  });
  await assert.rejects(owner.native("native.describe", { session_id: "old" }), /does not match/);
  assert.equal(owner.closes(), 0);
});

test("deferred drain accepts fresh event contexts and rejects a replaced generation", async () => {
  const first = nativeFixture("old", "tui");
  const second = nativeFixture("new", "tui");
  const queue = [{ session_id: "old", message_id: "old-delivery", body: "old body" }];
  const owner = fakeConnection({ topology: "interactive", queue });
  const extension = createPiExtension({ launch: launch("interactive"), connect: owner.connect });
  extension(first.pi);
  await first.emit("session_start", { type: "session_start", reason: "startup" });
  owner.calls.length = 0;

  // This event uses a fresh context just like the real runner. Replace the
  // owner record before its setImmediate callback runs.
  await first.emit("session_compact", { type: "session_compact" });
  await first.emit("session_shutdown", { type: "session_shutdown", reason: "reload" });
  extension(second.pi);
  await second.emit("session_start", { type: "session_start", reason: "reload" });
  await immediate();
  assert.equal(queue.length, 1);
  assert.equal(owner.calls.some(({ method }) => method === "owner.drain"), false);

  // The successor's own fresh event context still drains normally.
  queue[0] = { session_id: "new", message_id: "new-delivery", body: "new body" };
  await second.emit("session_compact", { type: "session_compact" });
  await immediate();
  assert.equal(queue.length, 0);
  assert.equal(second.pending().details.message_id, "new-delivery");
});

test("mode mismatch and bridge failure fail closed", async () => {
  const mismatched = nativeFixture("native", "tui");
  createPiExtension({ launch: launch("lane"), connect: async () => { throw new Error("must not connect"); } })(mismatched.pi);
  await mismatched.emit("session_start", { type: "session_start", reason: "startup" });
  assert.equal(mismatched.shutdown(), 1);

  const failed = nativeFixture();
  createPiExtension({ launch: launch("lane"), connect: async () => { throw new Error("connect failed"); } })(failed.pi);
  await failed.emit("session_start", { type: "session_start", reason: "startup" });
  assert.equal(failed.shutdown(), 1);
  assert.ok(failed.aborted() >= 1);
});

test("an idle owner connection ending retires the native session", async () => {
  const native = nativeFixture();
  const owner = fakeConnection({ topology: "lane" });
  createPiExtension({ launch: launch("lane"), connect: owner.connect })(native.pi);
  await native.emit("session_start", { type: "session_start", reason: "startup" });
  owner.end();
  await new Promise(setImmediate);
  assert.equal(native.aborted(), 1);
  assert.equal(native.shutdown(), 1);
});

test("default managed factory survives real module reevaluation after launch scrub", async () => {
  process.env[launchEnvironmentName] = JSON.stringify(launch("interactive"));
  const first = await import("./extension.mjs?managed-reload=first");
  assert.equal(Object.hasOwn(process.env, launchEnvironmentName), false);
  const second = await import("./extension.mjs?managed-reload=second");
  assert.strictEqual(second.default, first.default);
});

// One real kit Peer, the real direct owner, and a daemon speaking the real
// kit wire protocol over one Unix socket.
async function actualInteractive(t, prefix, handle) {
  const directory = await mkdtemp(path.join(os.tmpdir(), prefix));
  const socket = path.join(directory, "bus.sock");
  const connections = [];
  const methods = [];
  const server = net.createServer((stream) => {
    const connection = new kit.Connection(stream, false, (request) => {
      methods.push(request.method);
      if (request.method === "session.hello") return connection.result(request, {});
      return handle(connection, request);
    });
    connections.push(connection);
  });
  server.listen(socket);
  await once(server, "listening");
  t.after(async () => {
    for (const connection of connections) connection.close();
    await new Promise((resolve) => server.close(resolve));
    await rm(directory, { recursive: true, force: true });
  });
  const native = nativeFixture(`${prefix}native`, "tui");
  createPiExtension({
    launch: { socket, name: "managed", groups: ["team"], topology: "interactive" },
    // No kit reconnect: a failed assertion must not leave a retry loop behind.
    connect: (endpoint, options) => connectPiInteractivePeer(endpoint, {
      ...options, connectPeer: kit.connectPeer, peerOptions: { schedule: () => () => {} },
    }),
  })(native.pi);
  await native.emit("session_start", { type: "session_start", reason: "startup" });
  const call = (callID, action, args) => native.pi.tool.execute(callID, { action, arguments: args }, undefined, undefined, native.ctx);
  return { native, connections, methods, call };
}

test("interactive daemon action rejection is a native tool error on the same admitted Peer", { timeout: 5000 }, async (t) => {
  const f = await actualInteractive(t, "pi-tool-error-", (connection, request) => {
    if (request.method === "message.send") return connection.error(request, -32001);
    if (request.method === "session.list") return connection.result(request, { sessions: [] });
    return connection.error(request, -32600);
  });
  await assert.rejects(f.call("send-call", "send", { target: "missing", message: "hello" }), (error) =>
    error instanceof BridgeCallError && error.code === "tool_error" && error.message === "unknown_session");
  assert.equal(f.native.aborted(), 0);
  assert.equal(f.native.shutdown(), 0);
  const result = await f.call("list-call", "list", {});
  assert.deepEqual(result.details, { session_id: "pi-tool-error-native", call_id: "list-call", result: { sessions: [] } });
  assert.equal(f.connections.length, 1);
  assert.deepEqual(f.methods, ["session.hello", "message.send", "session.list"]);
  await f.native.emit("session_shutdown", { type: "session_shutdown", reason: "quit" });
});

// Preserves main's Go owner, interactive_owner.go:354-357: every Caller.Action
// failure, a lost connection included, is a tool_error and the session stays.
test("interactive transport loss during an action is a native tool error and keeps the session", { timeout: 5000 }, async (t) => {
  const held = deferred();
  const f = await actualInteractive(t, "pi-tool-loss-", (connection, request) => {
    if (request.method === "session.list") { held.resolve(); return; }
    return connection.error(request, -32600);
  });
  const action = f.call("held-call", "list", {});
  await held.promise;
  f.connections[0].close();
  await assert.rejects(action, (error) => error instanceof BridgeCallError && error.code === "tool_error");
  assert.equal(f.native.aborted(), 0);
  assert.equal(f.native.shutdown(), 0);
  await f.native.emit("session_shutdown", { type: "session_shutdown", reason: "quit" });
});

test("native tool arguments match the shared closed MCP field declaration", () => {
  const native = nativeFixture();
  createPiExtension({ launch: launch("lane") })(native.pi);
  const declaration = JSON.parse(fs.readFileSync(new URL("../pifamily/extension/testdata/sessionbus-tool.json", import.meta.url), "utf8"));
  assert.deepEqual(native.pi.tool.parameters.properties.action, declaration.inputSchema.properties.action);
  assert.deepEqual(native.pi.tool.parameters.properties.arguments, declaration.inputSchema.properties.arguments);
  assert.equal(native.pi.tool.parameters.properties.arguments.additionalProperties, false);
  assert.equal(Object.hasOwn(native.pi.tool.parameters.properties.arguments.properties, "summary"), false);
});
