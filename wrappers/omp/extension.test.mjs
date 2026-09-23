// SPDX-License-Identifier: MIT

import assert from "node:assert/strict";
import fs from "node:fs";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import test from "node:test";

import { BridgeCallError, PrivateBridge } from "../pifamily/extension/bridge.mjs";
import {
  captureLaunch,
  createOMPExtension,
  deliveryMessageType,
  launchEnvironmentName,
  toolName,
} from "./extension.mjs";

function deferred() {
  let resolve;
  let reject;
  const promise = new Promise((yes, no) => { resolve = yes; reject = no; });
  return { promise, resolve, reject };
}

async function flush() {
  await Promise.resolve();
  await Promise.resolve();
  await Promise.resolve();
}

function launch(topology = "lane") {
  return { directory: "/private/omp-owner", owner_pid: process.ppid, socket: "/private/omp-owner/bridge.sock", topology };
}

function nativeFixture(sessionID = "native-main", mode = "rpc", cwd = "/work/main", name = "OMP title") {
  const handlers = new Map();
  let currentID = sessionID;
  let currentName = name;
  let aborts = 0;
  let shutdowns = 0;
  let sendError;
  let resetSequence = 0;
  const entries = [];
  const scheduled = [];
  const ctx = {
    mode,
    cwd,
    sessionManager: {
      getSessionId: () => currentID,
      getSessionName: () => currentName,
      getEntries: () => structuredClone(entries),
    },
    abort: () => { aborts += 1; },
    shutdown: () => { shutdowns += 1; },
  };
  const pi = {
    tool: undefined,
    on(event, handler) {
      const values = handlers.get(event) ?? [];
      values.push(handler);
      handlers.set(event, values);
    },
    registerTool(tool) { this.tool = tool; },
    sendMessage(message, options) {
      assert.deepEqual(options, { deliverAs: "steer", triggerTurn: true });
      if (sendError) throw sendError;
      scheduled.push(structuredClone(message));
    },
  };
  return {
    pi,
    ctx,
    context() {
      return {
        mode: ctx.mode,
        cwd: ctx.cwd,
        sessionManager: ctx.sessionManager,
        abort: ctx.abort,
        shutdown: ctx.shutdown,
      };
    },
    setSessionID(value) { currentID = value; },
    setName(value) { currentName = value; },
    addResetBoundary(id = `reset-${++resetSequence}`) { entries.push({ type: "reset_boundary", id }); },
    throwOnSend(error) { sendError = error; },
    aborts: () => aborts,
    shutdowns: () => shutdowns,
    scheduled: () => structuredClone(scheduled),
    async emit(event, value = { type: event }, eventContext = ctx) {
      // OMP clears native hidden-next-turn state when replacing sessions.
      if (event === "session_switch") scheduled.length = 0;
      let result;
      for (const handler of handlers.get(event) ?? []) {
        const candidate = await handler(value, eventContext);
        if (candidate !== undefined) result = candidate;
      }
      if (event === "before_agent_start" && result === undefined && scheduled.length) {
        result = { message: scheduled.shift() };
      }
      return result;
    },
  };
}

class FakeOwner {
  constructor() {
    this.calls = [];
    this.handler = undefined;
    this.holds = new Map();
    this.afterHolds = new Map();
    this.failures = new Map();
    this.waiters = [];
    this.closed = 0;
    this.doneGate = deferred();
    this.done = this.doneGate.promise;
  }

  connect = async (_socket, options) => {
    this.handler = options.handler;
    return this;
  };

  hold(method) {
    const gate = deferred();
    const entered = deferred();
    this.holds.set(method, { gate, entered });
    return { entered: entered.promise, release: gate.resolve };
  }

  holdAfterNative(method) {
    const gate = deferred();
    const entered = deferred();
    this.afterHolds.set(method, { gate, entered });
    return { entered: entered.promise, release: gate.resolve };
  }

  reject(method, error) {
    this.failures.set(method, error);
  }

  async call(method, params, { signal } = {}) {
    if (signal?.aborted) throw signal.reason;
    const record = { method, params: structuredClone(params) };
    this.calls.push(record);
    for (const waiter of this.waiters.splice(0)) waiter();
    const hold = this.holds.get(method);
    if (hold) {
      this.holds.delete(method);
      hold.entered.resolve(record);
      await hold.gate.promise;
      if (signal?.aborted) throw signal.reason;
    }
    const failure = this.failures.get(method);
    if (failure) {
      this.failures.delete(method);
      throw failure;
    }
    if (method === "owner.ready" || method === "owner.switch") {
      const description = await this.native("native.describe", {
        owner_token: params.owner_token,
        session_id: params.session_id,
      });
      assert.equal(description.owner_token, params.owner_token);
      assert.equal(description.session_id, params.session_id);
      const after = this.afterHolds.get(method);
      if (after) {
        this.afterHolds.delete(method);
        after.entered.resolve(record);
        await after.gate.promise;
      }
      return { owner_token: params.owner_token, session_id: params.session_id };
    }
    if (method === "session_end") return { owner_token: params.owner_token, session_id: params.session_id };
    if (method === "run.preflight") {
      return {
        owner_token: params.owner_token,
        session_id: params.session_id,
        report_sequence: params.report_sequence,
        run_token: params.run_token,
      };
    }
    if (method === "delivery.observe") return structuredClone(params);
    if (method === "tool.call") {
      return {
        owner_token: params.owner_token,
        session_id: params.session_id,
        call_id: params.call_id,
        result: { action: params.action, arguments: params.arguments },
      };
    }
    throw new Error(`unexpected host method ${method}`);
  }

  async native(method, params, signal = undefined) {
    if (!this.handler) throw new Error("native bridge handler is unavailable");
    return this.handler({ method, params, signal });
  }

  async waitFor(predicate) {
    while (!predicate(this.calls)) await new Promise((resolve) => this.waiters.push(resolve));
    await flush();
  }

  async close() {
    this.closed += 1;
    this.doneGate.resolve();
  }
}

function deterministicTokens() {
  let value = 0;
  return () => `token${++value}`;
}

function nativeCustom(message) {
  return { role: "custom", ...structuredClone(message), timestamp: 1 };
}

async function start(extension, native, owner) {
  extension(native.pi);
  assert.equal(native.pi.tool.name, toolName);
  await native.emit("session_start");
  await owner.waitFor((calls) => calls.some((call) => call.method === "owner.ready"));
}

test("captureLaunch validates and scrubs private physical metadata", async (t) => {
  const directory = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "omp-extension-")));
  fs.chmodSync(directory, 0o700);
  const socket = path.join(directory, "bridge.sock");
  const server = net.createServer();
  await new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(socket, resolve);
  });
  t.after(async () => {
    await new Promise((resolve) => server.close(resolve));
    fs.rmSync(directory, { recursive: true, force: true });
  });
  const value = { directory, owner_pid: process.ppid, socket, topology: "interactive" };
  const environment = { [launchEnvironmentName]: JSON.stringify(value) };
  assert.deepEqual(captureLaunch(environment), value);
  assert.equal(Object.hasOwn(environment, launchEnvironmentName), false);
  for (const invalid of [
    { ...value, owner_pid: process.ppid + 1 },
    { ...value, topology: "print" },
    { ...value, extra: true },
  ]) {
    const candidate = { [launchEnvironmentName]: JSON.stringify(invalid) };
    assert.throws(() => captureLaunch(candidate), /metadata/);
    assert.equal(Object.hasOwn(candidate, launchEnvironmentName), false);
  }
});

test("lane primary reports ready and preflight before routing its exact tool identity", async () => {
  const owner = new FakeOwner();
  const native = nativeFixture();
  const extension = createOMPExtension({ launch: launch("lane"), connect: owner.connect, createToken: deterministicTokens() });
  await start(extension, native, owner);
  assert.deepEqual(native.pi.tool.approval, { tier: "exec", policy: "allow" });
  assert.equal(native.pi.tool.loadMode, "essential");
  assert.equal(native.pi.tool.strict, false);

  const preflight = await native.emit("before_agent_start", { type: "before_agent_start", prompt: "expanded prompt" }, native.context());
  assert.equal(preflight, undefined);
  await owner.waitFor((calls) => calls.some((call) => call.method === "run.preflight"));
  assert.deepEqual(owner.calls.map((call) => call.method), ["owner.ready", "run.preflight"]);
  const ready = owner.calls[0].params;
  assert.deepEqual(ready, {
    topology: "lane", directory: "/private/omp-owner", scope: "primary", mode: "rpc",
    owner_token: "token1-primary-1", session_id: "native-main", name: "OMP title",
  });
  assert.equal(owner.calls[1].params.report_sequence, 1);
  assert.match(owner.calls[1].params.run_token, /^token1-primary-1-run-1$/u);

  const result = await native.pi.tool.execute(
    "call-one",
    { action: "list", arguments: {} },
    undefined,
    undefined,
    native.context(),
  );
  assert.deepEqual(result, {
    content: [{ type: "text", text: '{"action":"list","arguments":{}}' }],
    details: {
      owner_token: "token1-primary-1", session_id: "native-main", call_id: "call-one",
      result: { action: "list", arguments: {} },
    },
  });
  assert.equal(await owner.native("native.shutdown", {
    owner_token: "token1-primary-1", session_id: "native-main",
  }).then((value) => value.requested), true);
  assert.equal(native.shutdowns(), 1);

  await native.emit("session_shutdown", { type: "session_shutdown" }, native.context());
  assert.deepEqual(owner.calls.at(-1), {
    method: "session_end",
    params: {
      topology: "lane", scope: "primary", mode: "rpc", owner_token: "token1-primary-1",
      session_id: "native-main", reason: "shutdown",
    },
  });
  assert.deepEqual(extension.stats(), { bindings: 0, reports: 0, retainedBytes: 0 });
});

test("native stage can follow published hello before local owner.ready continuation", async () => {
  const owner = new FakeOwner();
  const held = owner.holdAfterNative("owner.ready");
  const native = nativeFixture("interactive-main", "tui");
  const extension = createOMPExtension({ launch: launch("interactive"), connect: owner.connect, createToken: deterministicTokens() });
  extension(native.pi);
  await native.emit("session_start");
  await held.entered;
  const ready = owner.calls[0].params;
  assert.deepEqual(await owner.native("native.stage", {
    owner_token: ready.owner_token,
    session_id: ready.session_id,
    message_id: "immediate",
    body: "after public hello",
  }), {
    owner_token: ready.owner_token,
    session_id: ready.session_id,
    message_id: "immediate",
    queued: true,
  });
  held.release();
  await owner.waitFor((calls) => calls.length >= 1);
  await flush();
});

test("invalid managed factory mode fails closed before retaining a binding", async () => {
  const owner = new FakeOwner();
  const native = nativeFixture("invalid-main", "json");
  const extension = createOMPExtension({
    launch: launch("lane"), connect: owner.connect, createToken: deterministicTokens(),
  });
  extension(native.pi);
  await native.emit("session_start");
  assert.equal(native.aborts(), 1);
  assert.equal(native.shutdowns(), 1);
  assert.deepEqual(extension.stats(), { bindings: 0, reports: 0, retainedBytes: 0 });
  assert.deepEqual(owner.calls, []);
});

test("interactive delivery tracks independent batches through one coalesced native context", async () => {
  const owner = new FakeOwner();
  const native = nativeFixture("interactive-main", "tui");
  const extension = createOMPExtension({
    launch: launch("interactive"), connect: owner.connect, createToken: deterministicTokens(),
    limits: { queuedDeliveries: 2, batchItems: 1 },
  });
  await start(extension, native, owner);
  const token = owner.calls[0].params.owner_token;
  assert.deepEqual(await owner.native("native.stage", {
    owner_token: token, session_id: "interactive-main", message_id: " message one ", body: "first body",
  }), { owner_token: token, session_id: "interactive-main", message_id: " message one ", queued: true });
  assert.deepEqual(await owner.native("native.stage", {
    owner_token: token, session_id: "interactive-main", message_id: "message-two", body: "second body",
  }), { owner_token: token, session_id: "interactive-main", message_id: "message-two", queued: true });
  assert.deepEqual(await owner.native("native.stage", {
    owner_token: token, session_id: "interactive-main", message_id: "message-three", body: "third body",
  }), { owner_token: token, session_id: "interactive-main", message_id: "message-three", queued: false, reason: "queue_full" });
  assert.equal(native.scheduled().length, 2);
  // Both batches are claimed and submitted independently. The original turn
  // can still emit context that does not contain either one.
  await native.emit("context", { type: "context", messages: [{ role: "user", content: "original turn" }] }, native.context());

  const first = await native.emit("before_agent_start", { type: "before_agent_start", prompt: "ordinary user prompt" }, native.context());
  const second = await native.emit("before_agent_start", { type: "before_agent_start", prompt: "next native prompt" }, native.context());
  assert.equal(first.message.customType, deliveryMessageType);
  assert.equal(first.message.content, "first body");
  assert.deepEqual(first.message.details.message_ids, [" message one "]);
  assert.equal(second.message.content, "second body");
  assert.deepEqual(second.message.details.message_ids, ["message-two"]);
  await owner.waitFor((calls) => calls.filter((call) => call.method === "delivery.observe").length === 2);
  const firstMessage = nativeCustom(first.message);
  const secondMessage = nativeCustom(second.message);
  await native.emit("message_start", { type: "message_start", message: firstMessage }, native.context());
  await native.emit("message_end", { type: "message_end", message: firstMessage }, native.context());
  await native.emit("message_start", { type: "message_start", message: secondMessage }, native.context());
  await native.emit("message_end", { type: "message_end", message: secondMessage }, native.context());
  await native.emit("context", {
    type: "context", messages: [{ role: "user", content: "ordinary" }, firstMessage, secondMessage],
  }, native.context());
  await owner.waitFor((calls) => calls.filter((call) => call.method === "delivery.observe").length === 8);
  const reports = owner.calls.filter((call) => call.method === "delivery.observe").map((call) => call.params);
  for (const messageID of [" message one ", "message-two"]) {
    assert.deepEqual(
      reports.filter((report) => report.message_ids.includes(messageID)).map((report) => report.phase),
      ["claimed", "message_start", "message_end", "context"],
    );
  }

  assert.equal((await owner.native("native.stage", {
    owner_token: token, session_id: "interactive-main", message_id: "message-three", body: "third body",
  })).queued, true);
  // Later native context can repeat already-completed history without
  // resurrecting either removed batch.
  await native.emit("context", { type: "context", messages: [firstMessage, secondMessage] }, native.context());
  assert.equal(owner.calls.some((call) => call.method === "run.preflight"), false);
});

test("interactive later batch can complete before an older claimed batch", async () => {
  const owner = new FakeOwner();
  const native = nativeFixture("interactive-main", "tui");
  const extension = createOMPExtension({
    launch: launch("interactive"), connect: owner.connect, createToken: deterministicTokens(),
  });
  await start(extension, native, owner);
  const token = owner.calls[0].params.owner_token;
  for (const [messageID, body] of [["older", "older body"], ["later", "later body"]]) {
    assert.equal((await owner.native("native.stage", {
      owner_token: token, session_id: "interactive-main", message_id: messageID, body,
    })).queued, true);
  }
  const older = nativeCustom((await native.emit("before_agent_start", { type: "before_agent_start", prompt: "older" })).message);
  const later = nativeCustom((await native.emit("before_agent_start", { type: "before_agent_start", prompt: "later" })).message);
  await native.emit("message_start", { type: "message_start", message: later });
  await native.emit("message_end", { type: "message_end", message: later });
  await native.emit("context", { type: "context", messages: [later] });
  await owner.waitFor((calls) => calls.some((call) =>
    call.method === "delivery.observe" && call.params.phase === "context" && call.params.message_ids.includes("later")));
  // A repeated snapshot for the removed later batch is harmless while the
  // older batch has not emitted its own message events.
  await native.emit("context", { type: "context", messages: [later] });
  await native.emit("message_start", { type: "message_start", message: older });
  await native.emit("message_end", { type: "message_end", message: older });
  await native.emit("context", { type: "context", messages: [older, later] });
  await owner.waitFor((calls) => calls.some((call) =>
    call.method === "delivery.observe" && call.params.phase === "context" && call.params.message_ids.includes("older")));
  assert.equal(native.aborts(), 0);
});

test("interactive scheduling failure retires the owner and releases the claimed batch", async () => {
  const owner = new FakeOwner();
  const native = nativeFixture("interactive-main", "tui");
  const extension = createOMPExtension({
    launch: launch("interactive"), connect: owner.connect, createToken: deterministicTokens(),
  });
  await start(extension, native, owner);
  const token = owner.calls[0].params.owner_token;
  native.throwOnSend(new Error("native send binding rejected the message"));
  await assert.rejects(owner.native("native.stage", {
    owner_token: token, session_id: "interactive-main", message_id: "message-one", body: "first body",
  }), /native send binding rejected/);
  assert.equal(native.scheduled().length, 0);
  assert.equal(native.aborts(), 1);
  assert.equal(native.shutdowns(), 1);
  await assert.rejects(native.emit("session_shutdown", { type: "session_shutdown" }, native.context()),
    /native send binding rejected/);
  assert.deepEqual(extension.stats(), { bindings: 0, reports: 0, retainedBytes: 0 });
});

test("interactive batch reservation failure retires without later native submission", async () => {
  const owner = new FakeOwner();
  const native = nativeFixture("interactive-main", "tui");
  const extension = createOMPExtension({
    launch: launch("interactive"), connect: owner.connect, createToken: deterministicTokens(),
    // The entry fits after owner.ready settles, but the duplicate retained
    // batch content cannot fit. This forces claimDelivery's second reserve.
    limits: { retainedBytes: 512 },
  });
  await start(extension, native, owner);
  const token = owner.calls[0].params.owner_token;
  await assert.rejects(owner.native("native.stage", {
    owner_token: token, session_id: "interactive-main", message_id: "reserve-failure", body: "x".repeat(300),
  }), /native batch retained-payload capacity is exhausted/);
  assert.deepEqual(native.scheduled(), []);
  assert.equal(native.aborts(), 1);
  assert.equal(native.shutdowns(), 1);
  await native.emit("context", { type: "context", messages: [] }, native.context());
  assert.deepEqual(native.scheduled(), []);
  await assert.rejects(owner.native("native.stage", {
    owner_token: token, session_id: "interactive-main", message_id: "must-not-submit", body: "later",
  }), /deliverable factory/);
  assert.deepEqual(native.scheduled(), []);
  await assert.rejects(native.emit("session_shutdown", { type: "session_shutdown" }, native.context()),
    /native batch retained-payload capacity is exhausted/);
  assert.deepEqual(extension.stats(), { bindings: 0, reports: 0, retainedBytes: 0 });
});

test("interactive claimed batch retires on a new durable reset boundary", async () => {
  const owner = new FakeOwner();
  const native = nativeFixture("interactive-main", "tui");
  native.addResetBoundary("old-reset");
  const extension = createOMPExtension({
    launch: launch("interactive"), connect: owner.connect, createToken: deterministicTokens(),
  });
  await start(extension, native, owner);
  const token = owner.calls[0].params.owner_token;
  assert.equal((await owner.native("native.stage", {
    owner_token: token, session_id: "interactive-main", message_id: "message-one", body: "first body",
  })).queued, true);
  // A reset already present when the batch was claimed is not a new loss
  // witness, and unrelated context emitted during prompt preparation is valid.
  await native.emit("context", { type: "context", messages: [{ role: "user", content: "original turn" }] }, native.context());
  assert.equal(native.aborts(), 0);
  native.addResetBoundary("new-reset");
  await native.emit("context", { type: "context", messages: [] }, native.context());
  assert.equal(native.aborts(), 1);
  assert.equal(native.shutdowns(), 1);
  await assert.rejects(owner.native("native.stage", {
    owner_token: token, session_id: "interactive-main", message_id: "after-reset", body: "must not queue",
  }), /deliverable factory/);
});

test("interactive FIFO enforces and releases its process retained-payload bound", async () => {
  const owner = new FakeOwner();
  const native = nativeFixture("interactive-main", "tui");
  const extension = createOMPExtension({
    launch: launch("interactive"), connect: owner.connect, createToken: deterministicTokens(),
    limits: { retainedBytes: 1536 },
  });
  await start(extension, native, owner);
  const token = owner.calls[0].params.owner_token;
  assert.equal((await owner.native("native.stage", {
    owner_token: token, session_id: "interactive-main", message_id: "small", body: "a".repeat(400),
  })).queued, true);
  assert.deepEqual(await owner.native("native.stage", {
    owner_token: token, session_id: "interactive-main", message_id: "too-large", body: "b".repeat(1100),
  }), {
    owner_token: token, session_id: "interactive-main", message_id: "too-large", queued: false, reason: "queue_full",
  });
  const injected = await native.emit("before_agent_start", { type: "before_agent_start", prompt: "prompt" });
  const message = nativeCustom(injected.message);
  await native.emit("message_start", { type: "message_start", message });
  await native.emit("message_end", { type: "message_end", message });
  await native.emit("context", { type: "context", messages: [message] });
  await owner.waitFor((calls) => calls.filter((call) => call.method === "delivery.observe").length === 4);
  assert.ok(extension.stats().retainedBytes < 256);
  assert.equal((await owner.native("native.stage", {
    owner_token: token, session_id: "interactive-main", message_id: "after-release", body: "c".repeat(500),
  })).queued, true);
});

test("session replacement rotates owner token and resets report sequence without transferring queued input", async () => {
  const owner = new FakeOwner();
  const native = nativeFixture("session-old", "tui", "/work/old", "old");
  const extension = createOMPExtension({ launch: launch("interactive"), connect: owner.connect, createToken: deterministicTokens() });
  await start(extension, native, owner);
  const oldToken = owner.calls[0].params.owner_token;
  assert.equal((await owner.native("native.stage", {
    owner_token: oldToken, session_id: "session-old", message_id: "old-message", body: "must not transfer",
  })).queued, true);

  native.setSessionID("session-new");
  native.setName("new");
  await native.emit("session_switch", { type: "session_switch", reason: "resume", previousSessionFile: "/old.jsonl" }, native.context());
  await owner.waitFor((calls) => calls.some((call) => call.method === "owner.switch"));
  const switched = owner.calls.find((call) => call.method === "owner.switch").params;
  assert.equal(switched.previous_owner_token, oldToken);
  assert.equal(switched.previous_session_id, "session-old");
  assert.equal(switched.session_id, "session-new");
  await assert.rejects(owner.native("native.describe", {
    owner_token: oldToken, session_id: "session-old",
  }), (error) => error instanceof BridgeCallError && error.code === "stale_owner");

  assert.equal((await owner.native("native.stage", {
    owner_token: switched.owner_token, session_id: "session-new", message_id: "new-message", body: "new body",
  })).queued, true);
  const injected = await native.emit("before_agent_start", { type: "before_agent_start", prompt: "new prompt" });
  assert.equal(injected.message.content, "new body");
  await owner.waitFor((calls) => calls.some((call) => call.method === "delivery.observe" && call.params.session_id === "session-new"));
  const report = owner.calls.find((call) => call.method === "delivery.observe" && call.params.session_id === "session-new").params;
  assert.equal(report.report_sequence, 1);
  assert.deepEqual(report.message_ids, ["new-message"]);
});

test("failed session replacement releases the superseded binding", async () => {
  const owner = new FakeOwner();
  const native = nativeFixture("session-old", "tui", "/work/old", "old");
  const extension = createOMPExtension({
    launch: launch("interactive"), connect: owner.connect, createToken: deterministicTokens(),
  });
  await start(extension, native, owner);
  const held = owner.hold("owner.switch");
  owner.reject("owner.switch", new Error("owner rejected replacement"));
  native.setSessionID("session-new");
  native.setName("new");
  await native.emit("session_switch", { type: "session_switch", reason: "resume" }, native.context());
  await held.entered;
  assert.equal(extension.stats().bindings, 2);
  held.release();
  await flush();
  await flush();
  assert.equal(native.aborts(), 1);
  assert.equal(native.shutdowns(), 1);
  assert.equal(extension.stats().bindings, 1);
  await assert.rejects(native.emit("session_shutdown", { type: "session_shutdown" }, native.context()), /owner rejected replacement/);
  assert.deepEqual(extension.stats(), { bindings: 0, reports: 0, retainedBytes: 0 });
});

test("Task child receives a separate owner and cannot request primary shutdown", async () => {
  const owner = new FakeOwner();
  const extension = createOMPExtension({ launch: launch("interactive"), connect: owner.connect, createToken: deterministicTokens() });
  const primary = nativeFixture("main-session", "tui", "/work/main", "main");
  const child = nativeFixture("child-session", "print", "/work/child", "child");
  await start(extension, primary, owner);
  extension(child.pi);
  assert.deepEqual(primary.pi.tool.approval, { tier: "exec", policy: "allow" });
  assert.deepEqual(child.pi.tool.approval, { tier: "exec", policy: "allow" });
  assert.equal(primary.pi.tool.loadMode, "essential");
  assert.equal(child.pi.tool.loadMode, "essential");
  assert.equal(primary.pi.tool.strict, false);
  assert.equal(child.pi.tool.strict, false);
  await child.emit("session_start");
  await owner.waitFor((calls) => calls.filter((call) => call.method === "owner.ready").length === 2);
  const childReady = owner.calls.filter((call) => call.method === "owner.ready")[1].params;
  assert.equal(childReady.scope, "child");
  assert.equal(childReady.mode, "print");
  assert.notEqual(childReady.owner_token, owner.calls[0].params.owner_token);

  const result = await child.pi.tool.execute("child-call", { action: "list", arguments: {} }, undefined, undefined, child.context());
  assert.equal(result.details.owner_token, childReady.owner_token);
  assert.equal(result.details.session_id, "child-session");
  await assert.rejects(owner.native("native.shutdown", {
    owner_token: childReady.owner_token, session_id: "child-session",
  }), (error) => error instanceof BridgeCallError && error.code === "method_not_found");
  assert.equal(child.shutdowns(), 0);
  await child.emit("session_shutdown", { type: "session_shutdown" }, child.context());
  assert.equal(owner.calls.at(-1).params.scope, "child");
});

test("failed Task child reports its end without retiring the healthy primary", async () => {
  const owner = new FakeOwner();
  const extension = createOMPExtension({
    launch: launch("interactive"), connect: owner.connect, createToken: deterministicTokens(),
  });
  const primary = nativeFixture("main-session", "tui", "/work/main", "main");
  const child = nativeFixture("child-session", "print", "/work/child", "child");
  await start(extension, primary, owner);
  extension(child.pi);
  await child.emit("session_start");
  await owner.waitFor((calls) => calls.filter((call) => call.method === "owner.ready").length === 2);
  const childReady = owner.calls.filter((call) => call.method === "owner.ready")[1].params;
  assert.equal((await owner.native("native.stage", {
    owner_token: childReady.owner_token,
    session_id: childReady.session_id,
    message_id: "child-delivery",
    body: "child body",
  })).queued, true);
  const heldReport = owner.hold("delivery.observe");
  const injected = await child.emit("before_agent_start", { type: "before_agent_start", prompt: "child prompt" }, child.context());
  await heldReport.entered;
  const altered = nativeCustom({
    ...injected.message,
    content: "changed body",
  });
  await child.emit("message_start", { type: "message_start", message: altered }, child.context());
  assert.equal(child.aborts(), 1);
  assert.equal(child.shutdowns(), 0);

  const ending = child.emit("session_shutdown", { type: "session_shutdown" }, child.context());
  await flush();
  assert.equal(owner.calls.some((call) => call.method === "session_end"), false);
  heldReport.release();
  await assert.rejects(
    ending,
    /changed Sessionbus delivery identity/,
  );
  const end = owner.calls.find((call) => call.method === "session_end" && call.params.scope === "child");
  assert.equal(end.params.owner_token, childReady.owner_token);
  assert.equal(end.params.session_id, "child-session");
  assert.equal(primary.aborts(), 0);
  assert.equal(primary.shutdowns(), 0);

  const primaryResult = await primary.pi.tool.execute(
    "primary-after-child",
    { action: "list", arguments: {} },
    undefined,
    undefined,
    primary.context(),
  );
  assert.equal(primaryResult.details.session_id, "main-session");
  assert.equal(extension.stats().bindings, 1);
});

test("actual Unix bridge reports a failed Task child end while the primary remains healthy", async (t) => {
  const directory = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "omp-extension-child-end-")));
  fs.chmodSync(directory, 0o700);
  const socketPath = path.join(directory, "bridge.sock");
  const server = net.createServer();
  const accepted = deferred();
  let acceptedSocket;
  let host;
  let releaseDelivery;
  server.on("connection", (socket) => {
    acceptedSocket = socket;
    accepted.resolve(socket);
  });
  await new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(socketPath, resolve);
  });
  t.after(async () => {
    releaseDelivery?.resolve();
    if (host) {
      try { await host.close(); } catch {}
    } else {
      acceptedSocket?.destroy();
    }
    await new Promise((resolve) => server.close(resolve));
    fs.rmSync(directory, { recursive: true, force: true });
  });

  const extension = createOMPExtension({
    launch: { directory, owner_pid: process.ppid, socket: socketPath, topology: "interactive" },
    createToken: deterministicTokens(),
  });
  const primary = nativeFixture("unix-main", "tui", "/work/main", "main");
  const child = nativeFixture("unix-child", "print", "/work/child", "child");
  extension(primary.pi);
  extension(child.pi);
  const primaryStart = primary.emit("session_start");
  const socket = await accepted.promise;
  const deliveryEntered = deferred();
  releaseDelivery = deferred();
  const childEnd = deferred();
  const childReady = deferred();
  host = new PrivateBridge(socket, {
    role: "host",
    handler: async ({ method, params }) => {
      if (method === "owner.ready") {
        const description = await host.call("native.describe", {
          owner_token: params.owner_token,
          session_id: params.session_id,
        });
        assert.equal(description.session_id, params.session_id);
        if (params.session_id === "unix-child") childReady.resolve(params);
        return { owner_token: params.owner_token, session_id: params.session_id };
      }
      if (method === "delivery.observe") {
        if (params.session_id === "unix-child" && params.phase === "claimed") {
          deliveryEntered.resolve();
          await releaseDelivery.promise;
        }
        return params;
      }
      if (method === "session_end") {
        if (params.session_id === "unix-child") childEnd.resolve(params);
        return { owner_token: params.owner_token, session_id: params.session_id };
      }
      if (method === "tool.call") {
        return {
          owner_token: params.owner_token,
          session_id: params.session_id,
          call_id: params.call_id,
          result: { action: params.action },
        };
      }
      throw new BridgeCallError("method_not_found", `unexpected method ${method}`);
    },
  });
  await host.ready();
  await primaryStart;
  await child.emit("session_start");
  await childReady.promise;

  assert.equal((await host.call("native.stage", {
    owner_token: "token2-child-2",
    session_id: "unix-child",
    message_id: "unix-child-delivery",
    body: "child body",
  })).queued, true);
  const injected = await child.emit("before_agent_start", {
    type: "before_agent_start",
    prompt: "child prompt",
  }, child.context());
  await deliveryEntered.promise;
  await child.emit("message_start", {
    type: "message_start",
    message: nativeCustom({ ...injected.message, content: "changed body" }),
  }, child.context());
  const ending = child.emit("session_shutdown", { type: "session_shutdown" }, child.context());
  const ended = await childEnd.promise;
  assert.equal(ended.owner_token, "token2-child-2");
  releaseDelivery.resolve();
  await assert.rejects(ending, /changed Sessionbus delivery identity/);

  const primaryResult = await primary.pi.tool.execute(
    "unix-primary-after-child",
    { action: "list", arguments: {} },
    undefined,
    undefined,
    primary.context(),
  );
  assert.equal(primaryResult.details.session_id, "unix-main");
  await primary.emit("session_shutdown", { type: "session_shutdown" }, primary.context());
  await host.close();
  assert.deepEqual(extension.stats(), { bindings: 0, reports: 0, retainedBytes: 0 });
});

test("bounded report worker aborts a factory without spawning per-report work", async () => {
  const owner = new FakeOwner();
  const held = owner.hold("owner.ready");
  const native = nativeFixture();
  const extension = createOMPExtension({
    launch: launch("lane"), connect: owner.connect, createToken: deterministicTokens(), limits: { reportWork: 2 },
  });
  extension(native.pi);
  await native.emit("session_start");
  await held.entered;
  await native.emit("before_agent_start", { type: "before_agent_start", prompt: "one" });
  await native.emit("before_agent_start", { type: "before_agent_start", prompt: "two" });
  assert.equal(native.aborts(), 1);
  assert.equal(native.shutdowns(), 1);
  assert.ok(extension.stats().reports <= 2);
  held.release();
  await flush();
});

test("session shutdown joins already-owned report work before its end acknowledgement", async () => {
  const owner = new FakeOwner();
  const native = nativeFixture("interactive-main", "tui");
  const extension = createOMPExtension({ launch: launch("interactive"), connect: owner.connect, createToken: deterministicTokens() });
  await start(extension, native, owner);
  const token = owner.calls[0].params.owner_token;
  assert.equal((await owner.native("native.stage", {
    owner_token: token, session_id: "interactive-main", message_id: "held", body: "held body",
  })).queued, true);
  const hold = owner.hold("delivery.observe");
  const injected = await native.emit("before_agent_start", { type: "before_agent_start", prompt: "prompt" });
  await hold.entered;
  let ended = false;
  const ending = native.emit("session_shutdown", { type: "session_shutdown" }, native.context()).then(() => { ended = true; });
  await flush();
  assert.equal(ended, false);
  assert.equal(owner.calls.some((call) => call.method === "session_end"), false);
  hold.release();
  await ending;
  assert.equal(injected.message.content, "held body");
  assert.equal(owner.calls.at(-1).method, "session_end");
  assert.deepEqual(extension.stats(), { bindings: 0, reports: 0, retainedBytes: 0 });
});

test("unexpected passive bridge end aborts and shuts down the primary", async () => {
  const owner = new FakeOwner();
  const native = nativeFixture();
  const extension = createOMPExtension({ launch: launch("lane"), connect: owner.connect, createToken: deterministicTokens() });
  await start(extension, native, owner);
  owner.doneGate.resolve();
  await flush();
  assert.equal(native.aborts(), 1);
  assert.equal(native.shutdowns(), 1);
});

test("actual Unix bridge supports owner.ready callback into native.describe", async (t) => {
  const directory = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "omp-extension-wire-")));
  fs.chmodSync(directory, 0o700);
  const socketPath = path.join(directory, "bridge.sock");
  const server = net.createServer();
  const accepted = deferred();
  server.on("connection", (socket) => accepted.resolve(socket));
  await new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(socketPath, resolve);
  });
  t.after(async () => {
    await new Promise((resolve) => server.close(resolve));
    fs.rmSync(directory, { recursive: true, force: true });
  });

  const native = nativeFixture();
  const extension = createOMPExtension({
    launch: { directory, owner_pid: process.ppid, socket: socketPath, topology: "lane" },
    createToken: deterministicTokens(),
  });
  extension(native.pi);
  const started = native.emit("session_start");
  const socket = await accepted.promise;
  const host = new PrivateBridge(socket, {
    role: "host",
    handler: async ({ method, params }) => {
      if (method !== "owner.ready") throw new BridgeCallError("method_not_found", "unexpected method");
      const description = await host.call("native.describe", {
        owner_token: params.owner_token,
        session_id: params.session_id,
      });
      assert.equal(description.cwd, "/work/main");
      return { owner_token: params.owner_token, session_id: params.session_id };
    },
  });
  await host.ready();
  await started;
  await host.call("native.shutdown", { owner_token: "token1-primary-1", session_id: "native-main" });
  assert.equal(native.shutdowns(), 1);
  await host.close();
});


test("native tool arguments match the shared closed MCP field declaration", () => {
  const native = nativeFixture();
  createOMPExtension({ launch: launch("lane") })(native.pi);
  const declaration = JSON.parse(fs.readFileSync(new URL("../pifamily/extension/testdata/sessionbus-tool.json", import.meta.url), "utf8"));
  assert.deepEqual(native.pi.tool.parameters.properties.action, declaration.inputSchema.properties.action);
  assert.deepEqual(native.pi.tool.parameters.properties.arguments, declaration.inputSchema.properties.arguments);
  assert.equal(native.pi.tool.parameters.properties.arguments.additionalProperties, false);
  assert.equal(Object.hasOwn(native.pi.tool.parameters.properties.arguments.properties, "summary"), false);
});
