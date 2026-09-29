// SPDX-License-Identifier: MIT
import assert from "node:assert/strict";
import { once } from "node:events";
import { mkdtemp, rm } from "node:fs/promises";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import test from "node:test";

import kit from "../../pi/node_modules/@sessionbus/kit/sdk/js/index.js";
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
      supersede() {
        this.error = new Error("superseded");
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

function ownerFixture({ busy = false } = {}) {
  const fake = fakeKit();
  let id = "native-one", name = "Native", appended = [];
  const native = async ({ method, params }) => {
    if (method === "native.describe") return { session_id: id, name, cwd: "/work" };
    if (method === "native.append") {
      if (busy) return { session_id: id, message_id: params.message_id, accepted: false, reason: "busy" };
      appended.push(params);
      return { session_id: id, message_id: params.message_id, accepted: true };
    }
    throw new Error("unexpected native method");
  };
  const owner = new PiInteractiveOwner(launch, native, fake.connectPeer);
  return { owner, fake, appended: () => appended, setBusy: (value) => { busy = value; }, setIdentity: (next, title = "") => { id = next; name = title; } };
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

test("supersession and close join the direct owner lifetime", async () => {
  const f = ownerFixture();
  await ready(f.owner);
  f.fake.latest().supersede();
  assert.match(String(await f.owner.done), /superseded/);
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
  const action = owner.call("tool.call", {
    session_id: "reconnect-native", call_id: "after-reconnect", action: "list", arguments: {},
  });
  scheduled();
  await secondHello.promise;
  assert.equal(listCount, 0);
  assert.deepEqual(appended, []);
  releaseSecond.resolve();
  assert.deepEqual((await action).result, { sessions: [] });
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
