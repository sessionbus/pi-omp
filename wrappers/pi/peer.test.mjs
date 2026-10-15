// SPDX-License-Identifier: MIT
import assert from "node:assert/strict";
import { once } from "node:events";
import { mkdtemp, rm } from "node:fs/promises";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import test from "node:test";

import kit from "../../pi/node_modules/@sessionbus/kit/sdk/js/index.js";
import { BridgeCallError } from "../pifamily/extension/bridge.mjs";
import { PiInteractiveOwner, renderPiDelivery } from "./peer.mjs";

function deferred() {
  let resolve;
  const promise = new Promise((yes) => { resolve = yes; });
  return { promise, resolve };
}

function fakeKit() {
  const records = [];
  let latest;
  const connectPeer = (identity, deliver) => {
    const closed = deferred();
    let identityController = new AbortController();
    const connectionController = new AbortController();
    const peer = {
      identity,
      admitted: structuredClone(identity),
      identityController,
      connection: { signal: connectionController.signal },
      ready: Promise.resolve(),
      closed: closed.promise,
      terminal: false,
      error: null,
      caller: {
        action: async (action, args, signal) => {
          if (signal?.aborted) throw signal.reason;
          records.push({ type: "action", action, args });
          return { action, args };
        },
        disconnected() {},
      },
      async rehello(_signal, name, info) {
        records.push({ type: "rehello", name, info });
        this.identity = { ...this.identity, ...(name === undefined ? {} : { name }), info };
        this.admitted = structuredClone(this.identity);
      },
      async replace(next) {
        records.push({ type: "replace", identity: structuredClone(next) });
        identityController.abort(new Error("replaced"));
        identityController = new AbortController();
        this.identityController = identityController;
        this.identity = structuredClone(next);
        this.admitted = structuredClone(next);
      },
      shutdown() {
        if (this.terminal) return;
        this.terminal = true;
        this.connection = null;
        identityController.abort(new Error("closed"));
        connectionController.abort(new Error("closed"));
        closed.resolve();
      },
      end(error) {
        this.error = error;
        this.shutdown();
      },
      deliver(request, signal = identityController.signal) { return deliver(signal, request, this.admitted); },
    };
    records.push({ type: "connect", identity: structuredClone(identity) });
    latest = peer;
    return peer;
  };
  return { connectPeer, records, latest: () => latest };
}

const launch = { socket: "/bus.sock", name: "fallback", groups: ["team"], topology: "interactive" };
const inbound = (id, body = "hello") => ({
  message_id: id, body,
  from: { session_id: "sender", product: "fixture", name: "Sender", groups: ["team"] },
});

// Pi is not running a task: the steer is refused and the owner appends.
function idleSteer(sessionID, params) {
  return { session_id: sessionID, message_id: params.message_id, accepted: false, reason: "idle" };
}

function ownerFixture({ busy = false, running = false } = {}) {
  const fake = fakeKit();
  let id = "native-one", name = "Native", appended = [], steered = [];
  const native = async ({ method, params }) => {
    if (method === "native.describe") return { session_id: id, name, cwd: "/work" };
    if (method === "native.steer") {
      assert.equal(params.run, null);
      if (!running) return idleSteer(id, params);
      steered.push(params);
      return { session_id: id, message_id: params.message_id, accepted: true };
    }
    if (method === "native.append") {
      if (busy) return { session_id: id, message_id: params.message_id, accepted: false, reason: "busy" };
      appended.push(params);
      return { session_id: id, message_id: params.message_id, accepted: true };
    }
    throw new Error("unexpected native method");
  };
  const owner = new PiInteractiveOwner(launch, native, fake.connectPeer);
  return {
    owner, fake, appended: () => appended, steered: () => steered,
    setBusy: (value) => { busy = value; }, setRunning: (value) => { running = value; },
    setIdentity: (next, title = "") => { id = next; name = title; },
  };
}

async function ready(owner, id = "native-one", name = "Native") {
  return owner.call("owner.ready", { topology: "interactive", session_id: id, name });
}

test("direct owner publishes, rehellos, replaces, and routes exact tool identity", async () => {
  const f = ownerFixture();
  assert.deepEqual(await ready(f.owner), { session_id: "native-one" });
  assert.deepEqual(f.fake.records[0], {
    type: "connect",
    identity: { session_id: "native-one", product: "pi", name: "Native", groups: ["team"], info: { cwd: "/work" } },
  });
  assert.deepEqual(await f.owner.call("tool.call", {
    session_id: "native-one", call_id: "call-one", action: "list", arguments: {},
  }), { session_id: "native-one", call_id: "call-one", result: { action: "list", args: {} } });

  await ready(f.owner, "native-one", "Renamed");
  assert.deepEqual(f.fake.records.at(-1), { type: "rehello", name: "Native", info: { cwd: "/work" } });
  await f.owner.call("session_end", { topology: "interactive", session_id: "native-one", reason: "new" });
  f.setIdentity("native-two", "");
  await ready(f.owner, "native-two", "");
  assert.deepEqual(f.fake.records.at(-1), {
    type: "replace",
    identity: { session_id: "native-two", product: "pi", name: "fallback", groups: ["team"], info: { cwd: "/work" } },
  });
  await f.owner.close();
});

const toolCall = (owner, session = "native-one", id = "call") =>
  owner.call("tool.call", { session_id: session, call_id: id, action: "list", arguments: {} });
const bridgeError = (code, message) => (error) =>
  error instanceof BridgeCallError && error.code === code && (message === undefined || error.message === message);

// Preserves main's Go owner, interactive_owner.go:830-832: without an admitted
// connection a tool call fails at once with not_connected; it does not wait.
test("tool call while the Peer is not admitted fails at once with not_connected", async () => {
  const f = ownerFixture();
  await ready(f.owner);
  f.fake.latest().connection = { signal: AbortSignal.abort(new Error("lost")) };
  const outcome = await Promise.race([
    toolCall(f.owner).then(() => "resolved", (error) => error),
    new Promise((resolve) => setTimeout(() => resolve("waited"), 1000)),
  ]);
  assert.ok(bridgeError("tool_error", "not_connected")(outcome), `outcome: ${outcome}`);
  await f.owner.close();
});

// Preserves main's Go owner, interactive_owner.go:358-362: a result that
// returns after the owner generation changed is stale_session, not a failure
// of the session.
// A same-ID reload retires the old Peer and joins its work before the new Peer
// is installed, so the held result returns while no Peer is current.
test("tool result after a same-ID Peer rotation is stale_session", async () => {
  const f = ownerFixture();
  await ready(f.owner);
  const old = f.fake.latest();
  const held = deferred(), entered = deferred(), retiring = deferred();
  old.caller.action = async () => { entered.resolve(); return held.promise; };
  const shutdown = old.shutdown.bind(old);
  old.shutdown = () => { retiring.resolve(); shutdown(); };
  const call = toolCall(f.owner);
  await entered.promise;
  await f.owner.call("session_end", { topology: "interactive", session_id: "native-one", reason: "reload" });
  const rotated = ready(f.owner);
  await retiring.promise;
  held.resolve({ sessions: [] });
  await assert.rejects(call, bridgeError("stale_session", "Pi tool call crossed a native owner generation"));
  await rotated;
  assert.notStrictEqual(f.fake.latest(), old);
  await f.owner.close();
});

// A malformed session id is a protocol error, as on the successor base; only
// a well-formed id of another generation is stale_session.
test("tool call with a malformed session id stays a protocol error", async () => {
  const f = ownerFixture();
  await ready(f.owner);
  for (const session of [null, 7, {}, [], "", "bad\0id", "x".repeat(257)]) {
    await assert.rejects(toolCall(f.owner, session),
      (error) => !(error instanceof BridgeCallError) && /invalid Pi interactive tool call/.test(error.message));
  }
  assert.deepEqual(f.fake.records.filter(({ type }) => type === "action"), []);
  await f.owner.close();
});

// Preserves main's Go owner, interactive_owner.go:344-349: a call for another
// session is stale_session, not a failure of the session.
test("tool call for another session is stale_session", async () => {
  const f = ownerFixture();
  await ready(f.owner);
  await assert.rejects(toolCall(f.owner, "native-other"),
    bridgeError("stale_session", "Pi tool call does not belong to the current session"));
  assert.deepEqual(f.fake.records.filter(({ type }) => type === "action"), []);
  await f.owner.close();
});

// A session that ends while the drain's native append is in flight ends the
// drain; it is not a delivery fault and must not end the native session.
test("a session end during a drain append ends the drain without a fault", async () => {
  const fake = fakeKit();
  const gate = deferred(), entered = deferred();
  let busy = true;
  const native = async ({ method, params }) => {
    if (method === "native.describe") return { session_id: "native-one", name: "Native", cwd: "/work" };
    if (method === "native.steer") return idleSteer("native-one", params);
    if (method === "native.append") {
      if (busy) return { session_id: "native-one", message_id: params.message_id, accepted: false, reason: "busy" };
      entered.resolve();
      await gate.promise;
      return { session_id: "native-one", message_id: params.message_id, accepted: true };
    }
    throw new Error("unexpected native method");
  };
  const owner = new PiInteractiveOwner(launch, native, fake.connectPeer);
  await ready(owner);
  assert.deepEqual(await fake.latest().deliver(inbound("held")), { disposition: "queued_for_next_turn" });
  busy = false;
  const drained = owner.call("owner.drain", { session_id: "native-one", witness: "agent_settled" });
  await entered.promise;
  await owner.call("session_end", { topology: "interactive", session_id: "native-one", reason: "new" });
  gate.resolve();
  assert.deepEqual(await drained, { session_id: "native-one", drained: 0 });
  await owner.close();
});

// A same-ID reload makes a fresh owner generation current before the old
// drain's append returns; the old drain still ends quietly and does not fault
// the fresh generation.
test("a same-ID reload during a drain append ends the old drain without a fault", async () => {
  const fake = fakeKit();
  const gate = deferred(), entered = deferred();
  let busy = true;
  const native = async ({ method, params }) => {
    if (method === "native.describe") return { session_id: "native-one", name: "Native", cwd: "/work" };
    if (method === "native.steer") return idleSteer("native-one", params);
    if (method === "native.append") {
      if (busy) return { session_id: "native-one", message_id: params.message_id, accepted: false, reason: "busy" };
      entered.resolve();
      await gate.promise;
      return { session_id: "native-one", message_id: params.message_id, accepted: true };
    }
    throw new Error("unexpected native method");
  };
  const owner = new PiInteractiveOwner(launch, native, fake.connectPeer);
  await ready(owner);
  const old = fake.latest();
  assert.deepEqual(await old.deliver(inbound("held")), { disposition: "queued_for_next_turn" });
  busy = false;
  const drained = owner.call("owner.drain", { session_id: "native-one", witness: "agent_settled" });
  await entered.promise;
  await owner.call("session_end", { topology: "interactive", session_id: "native-one", reason: "reload" });
  await ready(owner);
  assert.notStrictEqual(fake.latest(), old);
  gate.resolve();
  assert.deepEqual(await drained, { session_id: "native-one", drained: 0 });
  assert.deepEqual(await fake.latest().deliver(inbound("fresh")), { disposition: "written" });
  await owner.close();
});

test("direct owner preserves FIFO busy wake, capacity, and tree gate", async () => {
  const f = ownerFixture({ busy: true });
  await ready(f.owner);
  assert.deepEqual(await f.fake.latest().deliver(inbound("one")), { disposition: "queued_for_next_turn" });
  assert.deepEqual(await f.fake.latest().deliver(inbound("two")), { disposition: "queued_for_next_turn" });
  assert.deepEqual(await f.owner.call("owner.before_tree", { session_id: "native-one" }), { session_id: "native-one", pending: true });
  assert.deepEqual(await f.fake.latest().deliver(inbound("blocked")), { disposition: "rejected", reason: "native_branch_summary_busy" });

  f.setBusy(false);
  assert.deepEqual(await f.owner.call("owner.drain", {
    session_id: "native-one", witness: "session_before_tree_cancelled",
  }), { session_id: "native-one", drained: 2 });
  assert.deepEqual(f.appended().map((item) => item.message_id), ["one", "two"]);

  f.setBusy(true);
  for (let index = 0; index < 256; index++) {
    assert.equal((await f.fake.latest().deliver(inbound(`capacity-${index}`))).disposition, "queued_for_next_turn");
  }
  assert.deepEqual(await f.fake.latest().deliver(inbound("overflow")), {
    disposition: "rejected", reason: "Pi delivery queue is full",
  });
  await f.owner.close();
});

// While Pi runs a task a delivery goes into Pi's steering queue and is
// answered injected; an idle session still starts a turn with it.
test("a delivery while Pi runs a task is steered into it and answered injected", async () => {
  const f = ownerFixture({ busy: true, running: true });
  await ready(f.owner);
  assert.deepEqual(await f.fake.latest().deliver(inbound("steer")), { disposition: "injected" });
  assert.deepEqual(f.steered().map((item) => item.message_id), ["steer"]);
  assert.deepEqual(f.appended(), []);
  f.setRunning(false);
  f.setBusy(false);
  assert.deepEqual(await f.fake.latest().deliver(inbound("idle")), { disposition: "written" });
  assert.deepEqual(f.appended().map((item) => item.message_id), ["idle"]);
  await f.owner.close();
});

// Messages that waited while Pi was busy without a task (compaction) go into
// the next task at its start, in order; a delivery that arrives while they
// still wait keeps its place behind them.
test("messages held while Pi was busy without a task are steered into the next task in order", async () => {
  const f = ownerFixture({ busy: true });
  await ready(f.owner);
  assert.deepEqual(await f.fake.latest().deliver(inbound("one")), { disposition: "queued_for_next_turn" });
  assert.deepEqual(await f.fake.latest().deliver(inbound("two")), { disposition: "queued_for_next_turn" });
  f.setRunning(true);
  assert.deepEqual(await f.fake.latest().deliver(inbound("three")), { disposition: "queued_for_next_turn" });
  assert.deepEqual(await f.owner.call("owner.drain", {
    session_id: "native-one", witness: "before_agent_start",
  }), { session_id: "native-one", drained: 3 });
  assert.deepEqual(f.steered().map((item) => item.message_id), ["one", "two", "three"]);
  assert.deepEqual(f.appended(), []);
  assert.deepEqual(await f.fake.latest().deliver(inbound("four")), { disposition: "injected" });
  await f.owner.close();
});

test("canceled and poisoned delivery cannot corrupt retained FIFO", async () => {
  const f = ownerFixture({ busy: true });
  await ready(f.owner);
  assert.equal((await f.fake.latest().deliver(inbound("retained"))).disposition, "queued_for_next_turn");
  await assert.rejects(f.fake.latest().deliver({ ...inbound("bad"), from: {} }), /invalid/);
  const controller = new AbortController();
  controller.abort(new Error("canceled"));
  assert.deepEqual(await f.fake.latest().deliver(inbound("canceled"), controller.signal), {
    disposition: "rejected", reason: "native owner closed",
  });
  f.setBusy(false);
  assert.equal((await f.owner.call("owner.drain", { session_id: "native-one", witness: "agent_settled" })).drained, 1);
  assert.deepEqual(f.appended().map((item) => item.message_id), ["retained"]);
  await f.owner.close();
});

test("a Peer that ends for another reason than a takeover still ends the owner", async () => {
  const f = ownerFixture();
  await ready(f.owner);
  const refused = Object.assign(new Error("invalid_hello"), { code: -32602 });
  f.fake.latest().end(refused);
  assert.equal(await f.owner.done, refused);
  await assert.rejects(toolCall(f.owner), /Pi interactive owner is closed/);
  await f.owner.close();
});

// Owner decision 2026-10-15: as with Kilo, a Pi session that another launch
// took over keeps running without Sessionbus, and its tool answers the takeover.
test("a superseded owner keeps Pi running without Sessionbus", { timeout: 5000 }, async (t) => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "pi-superseded-peer-"));
  const socket = path.join(directory, "bus.sock");
  const helloHeld = deferred();
  const listHeld = deferred();
  let daemon;
  let streamClosed;
  let hellos = 0;
  const methods = [];
  const server = net.createServer((stream) => {
    streamClosed = once(stream, "close");
    daemon = new kit.Connection(stream, false, async (request) => {
      methods.push(request.method);
      // The rename's hello and the list call stay unanswered across the takeover.
      if (request.method === "session.hello") return ++hellos === 1 ? daemon.result(request, {}) : helloHeld.resolve();
      if (request.method === "session.list") return listHeld.resolve();
      return daemon.error(request, -32600);
    });
  });
  server.listen(socket);
  await once(server, "listening");
  t.after(async () => {
    daemon?.close();
    await new Promise((resolve) => server.close(resolve));
    await rm(directory, { recursive: true, force: true });
  });

  let busy = true;
  let nativeID = "taken-native";
  let nativeName = "Taken";
  const appended = [];
  const native = async ({ method, params }) => {
    if (method === "native.describe") return { session_id: nativeID, name: nativeName, cwd: "/work" };
    if (method === "native.steer") return idleSteer(nativeID, params);
    if (method === "native.append") {
      if (busy) return { session_id: nativeID, message_id: params.message_id, accepted: false, reason: "busy" };
      appended.push(params);
      return { session_id: nativeID, message_id: params.message_id, accepted: true };
    }
    throw new Error("unexpected native method");
  };
  const owner = new PiInteractiveOwner({ ...launch, socket }, native, kit.connectPeer);
  t.after(() => owner.close());
  await ready(owner, "taken-native", "Taken");
  assert.deepEqual(await daemon.call("message.deliver", inbound("waiting")), { disposition: "queued_for_next_turn" });

  const call = toolCall(owner, "taken-native", "in-flight").then(() => "resolved", (error) => error);
  nativeName = "Renamed";
  const renamed = ready(owner, "taken-native", "Renamed").then((value) => value, (error) => error);
  await listHeld.promise;
  await helloHeld.promise;
  await daemon.call("session.superseded", {}).catch(() => {});
  await streamClosed;

  const takeover = bridgeError("tool_error",
    "Sessionbus: another pi-peer launch took over this session (superseded); relaunch to use Sessionbus here");
  const inFlight = await call;
  assert.ok(takeover(inFlight), `in-flight call: ${inFlight}`);
  assert.deepEqual(await renamed, { session_id: "taken-native" });
  await assert.rejects(toolCall(owner, "taken-native", "later"), takeover);
  const state = await Promise.race([
    owner.done.then(() => "ended"),
    new Promise((resolve) => setImmediate(() => resolve("running"))),
  ]);
  assert.equal(state, "running");

  // The waiting message went with the Peer; Pi itself stays usable.
  busy = false;
  assert.deepEqual(await owner.call("owner.drain", { session_id: "taken-native", witness: "agent_settled" }),
    { session_id: "taken-native", drained: 0 });
  assert.deepEqual(appended, []);
  assert.deepEqual(await owner.call("owner.before_tree", { session_id: "taken-native" }),
    { session_id: "taken-native", pending: false });
  await owner.call("session_end", { topology: "interactive", session_id: "taken-native", reason: "new" });
  nativeID = "later-native";
  assert.deepEqual(await ready(owner, "later-native", "Renamed"), { session_id: "later-native" });
  await assert.rejects(toolCall(owner, "later-native", "after-new"), takeover);
  assert.deepEqual([...methods].sort(), ["session.hello", "session.hello", "session.list"]);

  await owner.call("session_end", { topology: "interactive", session_id: "later-native", reason: "quit" });
  assert.equal(await owner.done, undefined);
});

// From DEV1's read of 7e61f51: the Caller's reply succeeds, and the takeover
// lands before the owner's continuation runs.
test("a successful Caller reply that crosses a takeover answers the takeover text", async () => {
  const f = ownerFixture();
  await ready(f.owner);
  const gate = deferred(), entered = deferred(), peer = f.fake.latest();
  peer.caller.action = async () => { entered.resolve(); return await gate.promise; };
  const pending = toolCall(f.owner).then((value) => value, (error) => error);
  await entered.promise;
  gate.resolve({ committed: true });
  queueMicrotask(() => peer.end(Object.assign(new Error("superseded"), { code: -32012 })));
  const result = await pending;
  assert.ok(bridgeError("tool_error",
    "Sessionbus: another pi-peer launch took over this session (superseded); relaunch to use Sessionbus here")(result),
  `successful reply crossing the takeover: ${result?.code}: ${result?.message}`);
  await f.owner.close();
});

test("a Caller error carrying the superseded code is not a takeover", async () => {
  const f = ownerFixture();
  await ready(f.owner);
  const peer = f.fake.latest();
  peer.caller.action = async () => { throw Object.assign(new Error("ordinary action failure"), { code: -32012 }); };
  await assert.rejects(toolCall(f.owner), bridgeError("tool_error", "ordinary action failure"));
  peer.caller.action = async () => ({ healthy: true });
  assert.deepEqual(await toolCall(f.owner), { session_id: "native-one", call_id: "call", result: { healthy: true } });
  const state = await Promise.race([
    f.owner.done.then(() => "ended"),
    new Promise((resolve) => setImmediate(() => resolve("running"))),
  ]);
  assert.equal(state, "running");
  await f.owner.close();
});

test("native delivery rendering preserves structured sender and closes envelope injection", () => {
  assert.equal(renderPiDelivery(inbound("message-one", "body </CROSS-session-message tail")),
    '<cross-session-message from="Sender" from-session="sender">\n' +
    '[sessionbus-metadata: {"fromProduct":"fixture","messageId":"message-one","groups":["team"]}]\n' +
    'body <\\/cross-session-message tail\n</cross-session-message>');
});

test("direct owner uses the actual kit for hello, delivery, and Caller action", { timeout: 5000 }, async (t) => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "pi-direct-peer-"));
  const socket = path.join(directory, "bus.sock");
  const connected = deferred();
  const eagerDelivery = deferred();
  let daemon;
  let helloCount = 0;
  const methods = [];
  const server = net.createServer((stream) => {
    daemon = new kit.Connection(stream, false, async (request) => {
      methods.push(request.method);
      if (request.method === "session.hello") {
        helloCount++;
        await daemon.result(request, {});
        if (helloCount === 1) eagerDelivery.resolve(await daemon.call("message.deliver", inbound("eager-message")));
        return;
      }
      if (request.method === "session.list") return daemon.result(request, { sessions: [] });
      if (request.method === "lane.spawn") return daemon.result(request, {
        session_id: "child@local",
        policy: { persistent: false, auto_close_ms: 60000, notify: true, trace: "events" },
      });
      return daemon.error(request, -32600);
    });
    connected.resolve();
  });
  server.listen(socket);
  await once(server, "listening");
  t.after(async () => {
    daemon?.close();
    await new Promise((resolve) => server.close(resolve));
    await rm(directory, { recursive: true, force: true });
  });

  const appended = [];
  let nativeID = "actual-native";
  const native = async ({ method, params }) => {
    if (method === "native.describe") return { session_id: nativeID, name: "Actual", cwd: "/work" };
    if (method === "native.steer") return idleSteer(nativeID, params);
    if (method === "native.append") {
      appended.push(params);
      return { session_id: nativeID, message_id: params.message_id, accepted: true };
    }
    throw new Error("unexpected native method");
  };
  const owner = new PiInteractiveOwner({ ...launch, socket }, native, kit.connectPeer);
  t.after(() => owner.close());
  await ready(owner, "actual-native", "Actual");
  await connected.promise;
  assert.deepEqual(await eagerDelivery.promise, { disposition: "written" });
  assert.equal(appended[0].message_id, "eager-message");
  assert.deepEqual(await owner.call("tool.call", {
    session_id: "actual-native", call_id: "actual-call", action: "list", arguments: {},
  }), { session_id: "actual-native", call_id: "actual-call", result: { sessions: [] } });
  assert.deepEqual((await owner.call("tool.call", {
    session_id: "actual-native", call_id: "spawn-call", action: "spawn",
    arguments: { name: "child", product: "fixture-worker", open: {} },
  })).result, {
    session_id: "child@local",
    policy: { persistent: false, auto_close_ms: 60000, notify: true, trace: "events" },
  });
  assert.deepEqual(await daemon.call("message.deliver", inbound("actual-message")), { disposition: "written" });
  assert.equal(appended[1].message_id, "actual-message");
  await owner.call("session_end", { topology: "interactive", session_id: "actual-native", reason: "new" });
  nativeID = "actual-replacement";
  await ready(owner, "actual-replacement", "Actual");
  assert.deepEqual(methods, ["session.hello", "session.list", "lane.spawn", "session.hello"]);
  await owner.close();
});

test("replacement rejects an in-flight busy result from the prior native generation", async () => {
  const fake = fakeKit();
  const appendEntered = deferred();
  const releaseAppend = deferred();
  let id = "native-one";
  const native = async ({ method, params }) => {
    if (method === "native.describe") return { session_id: id, name: "Native", cwd: "/work" };
    if (method === "native.steer") return idleSteer(params.session_id, params);
    if (method === "native.append") {
      appendEntered.resolve();
      await releaseAppend.promise;
      return { session_id: params.session_id, message_id: params.message_id, accepted: false, reason: "busy" };
    }
    throw new Error("unexpected native method");
  };
  const owner = new PiInteractiveOwner(launch, native, fake.connectPeer);
  await ready(owner);
  const delivery = fake.latest().deliver(inbound("prior-generation"));
  await appendEntered.promise;
  await owner.call("session_end", { topology: "interactive", session_id: "native-one", reason: "new" });
  id = "native-two";
  await ready(owner, "native-two", "Native");
  releaseAppend.resolve();
  assert.deepEqual(await delivery, { disposition: "rejected", reason: "native owner closed" });
  assert.deepEqual(await owner.call("owner.drain", {
    session_id: "native-two", witness: "agent_settled",
  }), { session_id: "native-two", drained: 0 });
  await owner.close();
});

test("same-ID native replacement rotates the Peer and rejects its old generation", async () => {
  const f = ownerFixture();
  await ready(f.owner);
  const prior = f.fake.latest();
  await f.owner.call("session_end", { topology: "interactive", session_id: "native-one", reason: "reload" });
  await ready(f.owner);
  const current = f.fake.latest();
  assert.notStrictEqual(current, prior);
  assert.equal(f.fake.records.filter(({ type }) => type === "connect").length, 2);
  assert.deepEqual(await prior.deliver(inbound("stale-same-id")), {
    disposition: "rejected", reason: "native owner closed",
  });
  assert.deepEqual(await current.deliver(inbound("current-same-id")), { disposition: "written" });
  assert.deepEqual(f.appended().map(({ message_id }) => message_id), ["current-same-id"]);
  await f.owner.close();
});

test("actual kit reconnect gates Caller work and never replays retained delivery", { timeout: 5000 }, async (t) => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "pi-direct-reconnect-"));
  const socket = path.join(directory, "bus.sock");
  const firstHello = deferred(), retry = deferred(), secondHello = deferred(), releaseSecond = deferred();
  let firstConnection, scheduled, helloCount = 0, listCount = 0;
  const connections = new Set();
  const server = net.createServer((stream) => {
    let connection;
    connection = new kit.Connection(stream, false, async (request) => {
      if (request.method === "session.hello") {
        helloCount++;
        if (helloCount === 1) { firstConnection = connection; firstHello.resolve(); }
        else { secondHello.resolve(); await releaseSecond.promise; }
        return connection.result(request, {});
      }
      if (request.method === "session.list") { listCount++; return connection.result(request, { sessions: [] }); }
      return connection.error(request, -32600);
    });
    connections.add(connection);
    void connection.done.then(() => connections.delete(connection));
  });
  server.listen(socket);
  await once(server, "listening");
  t.after(async () => {
    for (const connection of connections) connection.close();
    await new Promise((resolve) => server.close(resolve));
    await rm(directory, { recursive: true, force: true });
  });

  let busy = true;
  const appended = [];
  const native = async ({ method, params }) => {
    if (method === "native.describe") return { session_id: "reconnect-native", name: "Reconnect", cwd: "/work" };
    if (method === "native.steer") return idleSteer("reconnect-native", params);
    if (method === "native.append") {
      if (busy) return { session_id: "reconnect-native", message_id: params.message_id, accepted: false, reason: "busy" };
      appended.push(params.message_id);
      return { session_id: "reconnect-native", message_id: params.message_id, accepted: true };
    }
    throw new Error("unexpected native method");
  };
  const owner = new PiInteractiveOwner({ ...launch, socket }, native, kit.connectPeer, {
    schedule(callback, delay) {
      assert.equal(delay, 2000);
      scheduled = callback;
      retry.resolve();
      return () => {};
    },
  });
  t.after(() => owner.close());
  await ready(owner, "reconnect-native", "Reconnect");
  await firstHello.promise;
  assert.deepEqual(await firstConnection.call("message.deliver", inbound("retained-reconnect")), { disposition: "queued_for_next_turn" });
  firstConnection.close();
  await retry.promise;
  // As main's Go owner (interactive_owner.go:830-832), a call while the Peer is
  // reconnecting fails at once with not_connected; it does not wait.
  await assert.rejects(owner.call("tool.call", {
    session_id: "reconnect-native", call_id: "while-disconnected", action: "list", arguments: {},
  }), (error) => error instanceof BridgeCallError && error.code === "tool_error" && error.message === "not_connected");
  scheduled();
  await secondHello.promise;
  assert.equal(listCount, 0);
  assert.deepEqual(appended, []);
  releaseSecond.resolve();
  // Fresh calls until the Peer is admitted again: earlier ones fail with
  // not_connected without reaching the daemon, so exactly one list arrives.
  let fresh;
  for (let attempt = 0; attempt < 200 && fresh === undefined; attempt++) {
    try {
      fresh = await owner.call("tool.call", {
        session_id: "reconnect-native", call_id: `after-reconnect-${attempt}`, action: "list", arguments: {},
      });
    } catch (error) {
      assert.ok(error instanceof BridgeCallError && error.code === "tool_error" && error.message === "not_connected", String(error));
      await new Promise((resolve) => setTimeout(resolve, 5));
    }
  }
  assert.deepEqual(fresh?.result, { sessions: [] });
  assert.equal(listCount, 1);
  busy = false;
  assert.equal((await owner.call("owner.drain", { session_id: "reconnect-native", witness: "agent_settled" })).drained, 1);
  assert.deepEqual(appended, ["retained-reconnect"]);
  await owner.close();
});

test("session end while disconnected cancels the ended generation reconnect", { timeout: 5000 }, async (t) => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "pi-direct-ended-"));
  const socket = path.join(directory, "bus.sock");
  const retry = deferred();
  let connection, scheduled, canceled = false, helloCount = 0;
  const server = net.createServer((stream) => {
    connection = new kit.Connection(stream, false, (request) => {
      if (request.method === "session.hello") {
        helloCount++;
        return connection.result(request, {});
      }
      return connection.error(request, -32600);
    });
  });
  server.listen(socket);
  await once(server, "listening");
  t.after(async () => {
    connection?.close();
    await new Promise((resolve) => server.close(resolve));
    await rm(directory, { recursive: true, force: true });
  });
  const native = async ({ method }) => {
    if (method === "native.describe") return { session_id: "ended-native", name: "Ended", cwd: "/work" };
    throw new Error("unexpected native method");
  };
  const owner = new PiInteractiveOwner({ ...launch, socket }, native, kit.connectPeer, {
    schedule(callback) {
      scheduled = callback;
      retry.resolve();
      return () => { canceled = true; };
    },
  });
  await ready(owner, "ended-native", "Ended");
  connection.close();
  await retry.promise;
  await owner.call("session_end", { topology: "interactive", session_id: "ended-native", reason: "reload" });
  assert.equal(canceled, true);
  scheduled();
  await new Promise(setImmediate);
  assert.equal(helloCount, 1);
  await owner.close();
});

test("close cancels an absent-daemon retry and joins pending readiness", { timeout: 5000 }, async (t) => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "pi-direct-absent-"));
  const socket = path.join(directory, "absent.sock");
  const retry = deferred();
  let scheduled, canceled = false, attempts = 0;
  const native = async ({ method }) => {
    if (method === "native.describe") return { session_id: "absent-native", name: "Absent", cwd: "/work" };
    throw new Error("unexpected native method");
  };
  const owner = new PiInteractiveOwner({ ...launch, socket }, native, kit.connectPeer, {
    connect(path) {
      attempts++;
      return net.createConnection(path);
    },
    schedule(callback) {
      scheduled = callback;
      retry.resolve();
      return () => { canceled = true; };
    },
  });
  const readiness = ready(owner, "absent-native", "Absent");
  await retry.promise;
  await owner.close();
  await assert.rejects(readiness, /closed/);
  assert.equal(canceled, true);
  scheduled();
  await new Promise(setImmediate);
  assert.equal(attempts, 1);
  await rm(directory, { recursive: true, force: true });
});

test("close joins an unadmitted actual-kit hello", { timeout: 5000 }, async (t) => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "pi-direct-hello-"));
  const socket = path.join(directory, "bus.sock");
  const hello = deferred();
  let daemon;
  const server = net.createServer((stream) => {
    daemon = new kit.Connection(stream, false, (request) => {
      if (request.method === "session.hello") { hello.resolve(); return; }
      return daemon.error(request, -32600);
    });
  });
  server.listen(socket);
  await once(server, "listening");
  t.after(async () => {
    daemon?.close();
    await new Promise((resolve) => server.close(resolve));
    await rm(directory, { recursive: true, force: true });
  });
  const native = async ({ method }) => {
    if (method === "native.describe") return { session_id: "hello-native", name: "Hello", cwd: "/work" };
    throw new Error("unexpected native method");
  };
  const owner = new PiInteractiveOwner({ ...launch, socket }, native, kit.connectPeer);
  const readiness = ready(owner, "hello-native", "Hello");
  await hello.promise;
  await owner.close();
  await assert.rejects(readiness, /closed|not connected/);
  await daemon.done;
});

test("close cancels and joins an admitted actual-kit Caller", { timeout: 5000 }, async (t) => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "pi-direct-close-"));
  const socket = path.join(directory, "bus.sock");
  const held = deferred();
  let daemon;
  const server = net.createServer((stream) => {
    daemon = new kit.Connection(stream, false, (request) => {
      if (request.method === "session.hello") return daemon.result(request, {});
      if (request.method === "session.list") { held.resolve(); return; }
      return daemon.error(request, -32600);
    });
  });
  server.listen(socket);
  await once(server, "listening");
  t.after(async () => {
    daemon?.close();
    await new Promise((resolve) => server.close(resolve));
    await rm(directory, { recursive: true, force: true });
  });
  const native = async ({ method }) => {
    if (method === "native.describe") return { session_id: "close-native", name: "Close", cwd: "/work" };
    throw new Error("unexpected native method");
  };
  const owner = new PiInteractiveOwner({ ...launch, socket }, native, kit.connectPeer);
  await ready(owner, "close-native", "Close");
  const action = assert.rejects(owner.call("tool.call", {
    session_id: "close-native", call_id: "held-call", action: "list", arguments: {},
  }), /closed|owner/);
  await held.promise;
  await owner.close();
  await action;
});
