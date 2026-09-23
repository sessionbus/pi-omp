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
  createPiExtension,
  launchEnvironmentName,
  toolName,
} from "./extension.mjs";

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
  let shutdown = 0;
  const notices = [];
  const shutdownSignal = deferred();
  const createContext = () => ({
    mode,
    cwd: "/project",
    isIdle: () => idle,
    hasUI: true,
    ui: { notify: (message, type) => notices.push({ message, type }) },
    abort: () => { aborted++; },
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
  return { directory: "/private/owner", owner_pid: process.ppid, socket: "/private/owner/bridge.sock", topology };
}

function fakeConnection({ topology, queue = [] } = {}) {
  const calls = [];
  const done = deferred();
  let nativeHandler;
  let closes = 0;
  const bridge = {
    done: done.promise,
    async call(method, params, { signal } = {}) {
      if (signal?.aborted) throw signal.reason;
      calls.push({ method, params });
      switch (method) {
        case "owner.ready":
        case "session_end":
        case "run.input":
        case "run.preflight":
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

test("launch capture scrubs and binds a private physical socket", async (t) => {
  const directory = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "pi-extension-")));
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
  const value = { directory, owner_pid: process.ppid, socket, topology: "lane" };
  const environment = { [launchEnvironmentName]: JSON.stringify(value) };
  assert.deepEqual(captureLaunch(environment), value);
  assert.equal(Object.hasOwn(environment, launchEnvironmentName), false);

  for (const invalid of [
    { ...value, owner_pid: process.ppid + 1 },
    { ...value, topology: "other" },
    { ...value, extra: true },
  ]) {
    const candidate = { [launchEnvironmentName]: JSON.stringify(invalid) };
    assert.throws(() => captureLaunch(candidate), /metadata/);
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
    params: { topology: "lane", directory: "/private/owner", session_id: "native-1", name: "Pi title" },
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
  await native.emit("agent_start");
  assert.deepEqual(owner.calls.splice(0), [
    { method: "run.input", params: { session_id: "native-1", source: "rpc", text: "original", settling: false } },
    { method: "run.preflight", params: { session_id: "native-1", prompt: "expanded", settling: false } },
    { method: "run.start", params: { session_id: "native-1", settling: false } },
    { method: "run.settling", params: { session_id: "native-1" } },
    { method: "run.start", params: { session_id: "native-1", settling: true } },
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
    { method: "owner.ready", params: { topology: "lane", directory: "/private/owner", session_id: "native-1", name: "" } },
  ]);
  await native.emit("session_shutdown", { type: "session_shutdown", reason: "reload" });
  assert.deepEqual(owner.calls.shift(), {
    method: "session_end", params: { topology: "lane", session_id: "native-1", reason: "reload" },
  });
  assert.equal(owner.closes(), 0);
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

test("actual Unix bridge supports handshake, nested drain, tool call, and joined quit", async (t) => {
  const directory = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "pi-extension-wire-")));
  fs.chmodSync(directory, 0o700);
  const socket = path.join(directory, "bridge.sock");
  const queue = [{ session_id: "wire-native", message_id: "wire-message", body: "wire body" }];
  let host;
  const hostReady = deferred();
  const server = net.createServer((connection) => {
    host = new PrivateBridge(connection, {
      role: "host",
      handler: async ({ method, params, bridge, signal }) => {
        switch (method) {
          case "owner.ready": return { session_id: params.session_id };
          case "session_end": return { session_id: params.session_id };
          case "tool.call": return { session_id: params.session_id, call_id: params.call_id, result: { peers: [] } };
          case "owner.drain": {
            let drained = 0;
            while (queue.length) {
              const result = await bridge.call("native.append", queue[0], { signal });
              if (!result.accepted) break;
              queue.shift();
              drained++;
            }
            return { session_id: params.session_id, drained };
          }
          default: throw new BridgeCallError("method_not_found", "unexpected test method");
        }
      },
    });
    host.ready().then(hostReady.resolve, hostReady.reject);
  });
  await new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(socket, resolve);
  });
  t.after(async () => {
    await host?.close().catch(() => {});
    await new Promise((resolve) => server.close(resolve));
    fs.rmSync(directory, { recursive: true, force: true });
  });

  const native = nativeFixture("wire-native", "tui");
  createPiExtension({ launch: { directory, owner_pid: process.ppid, socket, topology: "interactive" } })(native.pi);
  await native.emit("session_start", { type: "session_start", reason: "startup" });
  await hostReady.promise;
  await native.emit("before_agent_start", { type: "before_agent_start", prompt: "wire prompt" });
  assert.equal(queue.length, 0);
  assert.equal(native.pending().details.message_id, "wire-message");
  await native.completeMessage();
  assert.equal(native.leaf().details.message_id, "wire-message");
  assert.deepEqual(await native.pi.tool.execute("wire-call", { action: "list", arguments: {} }, undefined, undefined, native.ctx), {
    content: [{ type: "text", text: '{"peers":[]}' }],
    details: { session_id: "wire-native", call_id: "wire-call", result: { peers: [] } },
  });
  await native.emit("session_shutdown", { type: "session_shutdown", reason: "quit" });
  await host.done;
});

test("actual idle Unix EOF retires native ownership", { timeout: 1000 }, async (t) => {
  const directory = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "pi-extension-eof-")));
  fs.chmodSync(directory, 0o700);
  const socket = path.join(directory, "bridge.sock");
  let host;
  const hostReady = deferred();
  const server = net.createServer((connection) => {
    host = new PrivateBridge(connection, {
      role: "host",
      handler: async ({ method, params }) => {
        if (method !== "owner.ready") throw new BridgeCallError("method_not_found", "unexpected test method");
        return { session_id: params.session_id };
      },
    });
    host.ready().then(hostReady.resolve, hostReady.reject);
  });
  await new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(socket, resolve);
  });
  t.after(async () => {
    await host?.close().catch(() => {});
    await new Promise((resolve) => server.close(resolve));
    fs.rmSync(directory, { recursive: true, force: true });
  });

  const native = nativeFixture("eof-native");
  createPiExtension({ launch: { directory, owner_pid: process.ppid, socket, topology: "lane" } })(native.pi);
  await native.emit("session_start", { type: "session_start", reason: "startup" });
  await hostReady.promise;
  await host.close();
  await native.waitShutdown();
  assert.equal(native.aborted(), 1);
  assert.equal(native.shutdown(), 1);
});

test("default managed factory survives real module reevaluation after launch scrub", async (t) => {
  const directory = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "pi-extension-reload-")));
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

  process.env[launchEnvironmentName] = JSON.stringify({
    directory, owner_pid: process.ppid, socket, topology: "lane",
  });
  const first = await import("./extension.mjs?managed-reload=first");
  assert.equal(Object.hasOwn(process.env, launchEnvironmentName), false);
  const second = await import("./extension.mjs?managed-reload=second");
  assert.strictEqual(second.default, first.default);
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
