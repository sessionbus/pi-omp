// SPDX-License-Identifier: MIT

const maxQueueItems = 256;
const maxQueueBytes = 8 << 20;
const maxTextBytes = 1 << 20;

function deferred() {
  let resolve;
  const promise = new Promise((yes) => { resolve = yes; });
  return { promise, resolve };
}

function clean(error) { return String(error?.message || error || "Pi interactive owner failed"); }
function validText(value, limit, empty = false) {
  return typeof value === "string" && (empty || value.length > 0) && Buffer.byteLength(value) <= limit && !/\0/u.test(value);
}
function exact(value, keys) {
  return value !== null && typeof value === "object" && !Array.isArray(value) &&
    Object.keys(value).length === keys.length && keys.every((key) => Object.hasOwn(value, key));
}
function validateAppend(result, sessionID, messageID) {
  if (!result || result.session_id !== sessionID || result.message_id !== messageID || typeof result.accepted !== "boolean") {
    throw new Error("Pi native append response changed identity");
  }
  if (result.accepted) {
    if (!exact(result, ["session_id", "message_id", "accepted"])) throw new Error("Pi native append acknowledgement is invalid");
  } else if (!exact(result, ["session_id", "message_id", "accepted", "reason"]) ||
             !["busy", "branch_summary_busy"].includes(result.reason)) {
    throw new Error("Pi native append rejection is invalid");
  }
  return result;
}
function combinedSignal(...signals) {
  const present = signals.filter(Boolean);
  if (present.length === 0) return undefined;
  if (present.length === 1) return present[0];
  return AbortSignal.any(present);
}

// Preserve peer-common's structured native envelope byte-for-byte.
export function renderPiDelivery(request) {
  if (!request || !validText(request.message_id, 256) || !validText(request.body, maxTextBytes, true) ||
      !validText(request.from?.session_id, 256) || !validText(request.from?.product, 256)) {
    throw new Error("Pi delivery is invalid");
  }
  const cleanAttribute = (value) => String(value).replace(/["<>\r\n]/gu, "");
  const escaped = { "<": "\\u003c", ">": "\\u003e", "&": "\\u0026", "\u2028": "\\u2028", "\u2029": "\\u2029" };
  const metadata = JSON.stringify({
    fromProduct: request.from.product,
    messageId: request.message_id,
    groups: request.from.groups || [],
  }).replace(/[<>&\u2028\u2029]/gu, (character) => escaped[character]);
  const body = request.body.replace(/<\/cross-session-message/giu, "<\\/cross-session-message");
  const from = cleanAttribute(request.from.name || request.from.session_id);
  const session = cleanAttribute(request.from.session_id);
  return `<cross-session-message from="${from}" from-session="${session}">\n[sessionbus-metadata: ${metadata}]\n${body}\n</cross-session-message>`;
}

class ReadyGate {
  #promise;
  #resolve;
  #reject;
  constructor() {
    this.#promise = new Promise((yes, no) => { this.#resolve = yes; this.#reject = no; });
    this.#promise.catch(() => {});
  }
  settle(error, value) { error ? this.#reject(error) : this.#resolve(value); }
  wait(signal) {
    if (!signal) return this.#promise;
    if (signal.aborted) return Promise.reject(signal.reason || new Error("aborted"));
    return Promise.race([this.#promise, new Promise((_, reject) => {
      signal.addEventListener("abort", () => reject(signal.reason || new Error("aborted")), { once: true });
    })]);
  }
}

class OwnedPeer {
  #peer;
  #id;
  #controller = new AbortController();
  #gate = new ReadyGate();
  #admittedGate;
  #observed = new WeakSet();
  #identities = new WeakSet();
  #tasks = new Set();
  #timers = new Set();
  #dispose;

  constructor(connectPeer, identity, deliver, socket, options = {}) {
    this.#id = identity.session_id;
    const schedule = options.schedule || ((callback, delay) => {
      const timer = setTimeout(callback, delay);
      return () => clearTimeout(timer);
    });
    this.#peer = connectPeer(identity, (signal, message, admitted) => {
      if (this.signal.aborted) return { disposition: "rejected", reason: "native owner closed" };
      return this.#track(() => deliver(AbortSignal.any([signal, this.signal]), message, admitted));
    }, { SESSIONBUS_SOCKET: socket }, {
      ...options,
      schedule: (callback, delay) => {
        if (this.signal.aborted) return;
        const entry = { cancel: undefined };
        this.#timers.add(entry);
        entry.cancel = schedule(() => {
          this.#timers.delete(entry);
          if (this.signal.aborted) return;
          callback();
          this.#observe();
        }, delay);
      },
    });
    this.#observe();
    void this.#peer.closed.then(() => this.dispose(this.#peer.error || new Error("Sessionbus peer closed")));
  }

  get signal() { return this.#controller.signal; }
  get id() { return this.#id; }
  get connected() { return this.#live(); }
  get closed() { return this.#peer.closed; }
  get error() { return this.#peer.error; }

  #live() {
    return !this.signal.aborted && !this.#peer.terminal && this.#peer.connection &&
      !this.#peer.connection.signal.aborted && this.#peer.admitted?.session_id === this.#id &&
      this.#peer.identityController && !this.#peer.identityController.signal.aborted;
  }
  #reset() {
    if (!this.signal.aborted && this.#admittedGate === this.#gate) this.#gate = new ReadyGate();
  }
  #observe() {
    const ready = this.#peer.ready;
    if (!ready || this.#observed.has(ready)) return;
    this.#observed.add(ready);
    void ready.then(() => {
      if (!this.#live()) return;
      this.#admit();
    }, (error) => this.dispose(error));
  }
  #admit() {
    const identity = this.#peer.identityController;
    if (!this.#identities.has(identity)) {
      this.#identities.add(identity);
      identity.signal.addEventListener("abort", () => this.#reset(), { once: true });
    }
    this.#admittedGate = this.#gate;
    this.#gate.settle(undefined, this.#peer);
  }
  async ready(signal) {
    const cancel = signal ? AbortSignal.any([signal, this.signal]) : this.signal;
    while (!this.#live()) {
      if (cancel.aborted) throw cancel.reason;
      this.#reset();
      await this.#gate.wait(cancel);
    }
  }
  action(action, args, signal) {
    return this.#track(async () => {
      const cancel = combinedSignal(signal, this.signal);
      await this.ready(cancel);
      return this.#peer.caller.action(action, args, cancel);
    });
  }
  rehello(name, info, signal) {
    return this.#track(async () => {
      const cancel = combinedSignal(signal, this.signal);
      await this.ready(cancel);
      return this.#peer.rehello(cancel, name || undefined, info);
    });
  }
  replace(identity, signal) {
    return this.#track(async () => {
      const cancel = combinedSignal(signal, this.signal);
      await this.ready(cancel);
      const result = await this.#peer.replace(identity);
      if (cancel?.aborted) throw cancel.reason;
      this.#id = identity.session_id;
      if (!this.#live()) throw new Error("not connected");
      this.#admit();
      return result;
    });
  }
  #track(operation) {
    if (this.signal.aborted) return Promise.reject(this.signal.reason);
    if (this.#tasks.size >= 256) return Promise.reject(new Error("Sessionbus owner work limit reached"));
    const task = Promise.resolve().then(() => {
      if (this.signal.aborted) throw this.signal.reason;
      return operation();
    });
    this.#tasks.add(task);
    void task.then(() => this.#tasks.delete(task), () => this.#tasks.delete(task));
    return task;
  }
  dispose(reason = new Error("native owner closed")) {
    if (this.#dispose) return this.#dispose;
    this.#controller.abort(reason);
    this.#gate.settle(reason);
    for (const timer of this.#timers) timer.cancel?.();
    this.#timers.clear();
    this.#peer.shutdown();
    this.#dispose = Promise.allSettled([...this.#tasks]);
    return this.#dispose;
  }
}

export class PiInteractiveOwner {
  #launch;
  #native;
  #connectPeer;
  #peerOptions;
  #peer;
  #peerToken;
  #session;
  #queue = [];
  #queueBytes = 0;
  #treeBusy = false;
  #closed = false;
  #done = deferred();
  #closing;

  constructor(launch, native, connectPeer, peerOptions = {}) {
    this.#launch = launch;
    this.#native = native;
    this.#connectPeer = connectPeer;
    this.#peerOptions = peerOptions;
  }

  get done() { return this.#done.promise; }

  async call(method, params, { signal } = {}) {
    if (this.#closed) throw new Error("Pi interactive owner is closed");
    switch (method) {
      case "owner.ready": return this.#ready(params, signal);
      case "session_end": return this.#sessionEnd(params);
      case "owner.drain": return this.#drain(params, signal);
      case "owner.before_tree": return this.#beforeTree(params);
      case "tool.call": return this.#toolCall(params, signal);
      default: throw new Error("Pi interactive owner method is unavailable");
    }
  }

  async #ready(params, signal) {
    if (!exact(params, ["topology", "session_id", "name"]) || params.topology !== "interactive" ||
        !validText(params.session_id, 256) || !validText(params.name, 4096, true)) {
      throw new Error("invalid Pi interactive owner readiness");
    }
    const native = await this.#native({ method: "native.describe", params: { session_id: params.session_id }, signal });
    if (!exact(native, ["session_id", "name", "cwd"]) || native.session_id !== params.session_id ||
        !validText(native.name, 4096, true) || !validText(native.cwd, 32 << 10)) {
      throw new Error("Pi native description contradicted owner readiness");
    }
    const identity = {
      session_id: native.session_id,
      product: "pi",
      name: native.name || this.#launch.name || undefined,
      groups: [...this.#launch.groups],
      info: { cwd: native.cwd },
    };
    if (identity.name === undefined) delete identity.name;
    // A native reload/resume can begin a new owner generation with the same
    // durable session ID. Kit replace deliberately rejects that shape, so
    // retire the ended generation and publish a fresh Peer for the successor.
    if (this.#peer && this.#session === undefined && this.#peer.id === identity.session_id) {
      const ended = this.#peer;
      this.#peer = undefined;
      this.#peerToken = undefined;
      await ended.dispose(new Error("Pi native session replaced"));
    }
    // Install the native generation before hello acknowledgement can admit a
    // daemon delivery. Peer admission and its first delivery may share a turn.
    this.#session = identity.session_id;
    if (!this.#peer) {
      const token = {};
      this.#peerToken = token;
      const peer = new OwnedPeer(this.#connectPeer, identity, (cancel, request, admitted) => this.#deliver(token, cancel, request, admitted), this.#launch.socket, this.#peerOptions);
      this.#peer = peer;
      void peer.closed.then(() => {
        if (this.#peer === peer) this.#finish(peer.error || new Error("Sessionbus peer closed"));
      });
      await peer.ready(signal);
    } else if (this.#peer.id === identity.session_id) {
      await this.#peer.rehello(identity.name, identity.info, signal);
    } else {
      await this.#peer.replace(identity, signal);
    }
    return { session_id: identity.session_id };
  }

  async #sessionEnd(params) {
    if (!exact(params, ["topology", "session_id", "reason"]) || params.topology !== "interactive" ||
        params.session_id !== this.#session || !["quit", "reload", "new", "resume", "fork"].includes(params.reason)) {
      throw new Error("invalid Pi interactive session end");
    }
    this.#session = undefined;
    this.#queue = [];
    this.#queueBytes = 0;
    this.#treeBusy = false;
    if (params.reason === "quit") await this.close();
    else if (this.#peer && !this.#peer.connected) {
      const ended = this.#peer;
      this.#peer = undefined;
      this.#peerToken = undefined;
      await ended.dispose(new Error("Pi native session ended"));
    }
    return { session_id: params.session_id };
  }

  async #toolCall(params, signal) {
    if (!exact(params, ["session_id", "call_id", "action", "arguments"]) || params.session_id !== this.#session ||
        !validText(params.call_id, 256) || !validText(params.action, 64) || !params.arguments || typeof params.arguments !== "object") {
      throw new Error("invalid Pi interactive tool call");
    }
    const result = await this.#peer.action(params.action, params.arguments, signal);
    if (params.session_id !== this.#session) throw new Error("Pi tool call crossed a native session replacement");
    return { session_id: params.session_id, call_id: params.call_id, result };
  }

  #beforeTree(params) {
    if (!exact(params, ["session_id"]) || params.session_id !== this.#session) throw new Error("invalid Pi tree gate");
    this.#treeBusy = true;
    return { session_id: params.session_id, pending: this.#queue.length !== 0 };
  }

  async #drain(params, signal) {
    if (!exact(params, ["session_id", "witness"]) || params.session_id !== this.#session) throw new Error("invalid Pi drain witness");
    if (["session_before_tree_cancelled", "session_tree"].includes(params.witness)) this.#treeBusy = false;
    let drained = 0;
    while (this.#queue.length) {
      if (signal?.aborted) throw signal.reason;
      const item = this.#queue[0];
      const result = validateAppend(await this.#native({
        method: "native.append",
        params: { session_id: this.#session, message_id: item.message_id, body: item.body },
        signal,
      }), this.#session, item.message_id);
      if (result.accepted !== true) {
        if (result.reason !== "busy" && result.reason !== "branch_summary_busy") throw new Error("Pi queued delivery rejection is invalid");
        break;
      }
      if (this.#queue[0] !== item) throw new Error("Pi delivery queue changed during drain");
      this.#queue.shift();
      this.#queueBytes -= item.bytes;
      drained++;
    }
    return { session_id: params.session_id, drained };
  }

  async #deliver(token, signal, request, admitted) {
    if (this.#closed || token !== this.#peerToken || signal.aborted || admitted?.session_id !== this.#session) {
      return { disposition: "rejected", reason: "native owner closed" };
    }
    const sessionID = this.#session;
    const body = renderPiDelivery(request);
    if (this.#treeBusy && this.#queue.length) return { disposition: "rejected", reason: "native_branch_summary_busy" };
    if (this.#queue.length) return this.#enqueue(request.message_id, body);
    const result = validateAppend(await this.#native({
      method: "native.append",
      params: { session_id: sessionID, message_id: request.message_id, body },
      signal,
    }), sessionID, request.message_id);
    if (result.accepted === true) {
      if (this.#session === sessionID && this.#treeBusy) this.#treeBusy = false;
      return { disposition: "written" };
    }
    if (token !== this.#peerToken || signal.aborted || this.#session !== sessionID) {
      return { disposition: "rejected", reason: "native owner closed" };
    }
    if (result.reason === "branch_summary_busy") return { disposition: "rejected", reason: "native_branch_summary_busy" };
    if (result.reason !== "busy") throw new Error("Pi native append rejection is invalid");
    return this.#enqueue(request.message_id, body);
  }

  #enqueue(messageID, body) {
    const item = { message_id: messageID, body, bytes: Buffer.byteLength(messageID) + Buffer.byteLength(body) };
    if (this.#queue.length >= maxQueueItems || item.bytes > maxQueueBytes - this.#queueBytes) {
      return { disposition: "rejected", reason: "Pi delivery queue is full" };
    }
    this.#queue.push(item);
    this.#queueBytes += item.bytes;
    return { disposition: "queued_for_next_turn" };
  }

  #finish(error) {
    if (this.#closed) return;
    this.#closed = true;
    this.#done.resolve(error);
  }

  close() {
    if (this.#closing) return this.#closing;
    this.#closed = true;
    this.#queue = [];
    this.#queueBytes = 0;
    this.#treeBusy = false;
    this.#closing = Promise.resolve(this.#peer?.dispose()).finally(() => this.#done.resolve());
    return this.#closing;
  }
}

export async function connectPiInteractivePeer(launch, options = {}) {
  const implementation = options.connectPeer || (await import("@sessionbus/kit")).connectPeer;
  if (typeof implementation !== "function") throw new Error("Sessionbus kit connectPeer is unavailable");
  const owner = new PiInteractiveOwner(launch, options.handler, implementation, options.peerOptions);
  options.signal?.addEventListener("abort", () => void owner.close(), { once: true });
  return owner;
}
