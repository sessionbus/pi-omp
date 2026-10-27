// SPDX-License-Identifier: MIT
import test from "node:test";
import assert from "node:assert/strict";
import { appendNative, describeNative, historyNative } from "./native.mjs";

// Pi d981de1 sets the native active bit synchronously, but persists the custom
// entry later on message_end. Keep the leaf stale here to pin that distinction.
function fixture({ idle = true, starts = true } = {}) {
  let leaf = { type: "message", id: "old-leaf" };
  let sends = 0;
  let valid = true;
  let active = !idle;
  const read = () => { if (!valid) throw new Error("stale native context"); };
  const ctx = {
    cwd: "/native/project", isIdle: () => { read(); return !active; },
    sessionManager: {
      getSessionId: () => { read(); return "native-session"; },
      getLeafId: () => { read(); return leaf?.id ?? null; },
      getLeafEntry: () => { read(); return leaf; },
    },
  };
  const pi = {
    getSessionName: () => "native title",
    sendMessage(_message, options) {
      sends++;
      assert.deepEqual(options, { triggerTurn: true });
      if (starts) active = true;
    },
  };
  return { pi, ctx, sends: () => sends, leaf: () => leaf, settle: () => { active = false; }, invalidate: () => { valid = false; } };
}
const delivery = { session_id: "native-session", message_id: "message-1", body: "exact native envelope\n" };

test("native active transition acknowledges ownership while persisted leaf remains stale", () => {
  const f = fixture();
  assert.deepEqual(describeNative(f.pi, f.ctx), { session_id: "native-session", name: "native title", cwd: "/native/project" });
  assert.deepEqual(appendNative(f.pi, f.ctx, delivery), { accepted: true });
  assert.deepEqual(f.leaf(), { type: "message", id: "old-leaf" });
  assert.equal(f.sends(), 1);
});

test("busy context retains wrapper ownership and makes no native submission", () => {
  const f = fixture({ idle: false });
  assert.deepEqual(appendNative(f.pi, f.ctx, delivery), { accepted: false, reason: "busy" });
  assert.equal(f.sends(), 0);
});

// Pi 1.2.0 leaves the session idle after a triggerTurn send only when it
// defers the send during settled dispatch (agent-session.js:1836-1838); the
// send then runs after the settled handlers, so it is accepted.
test("a send that Pi leaves idle is deferred by Pi and accepted", () => {
  const f = fixture({ starts: false });
  assert.deepEqual(appendNative(f.pi, f.ctx, delivery), { accepted: true });
  assert.equal(f.sends(), 1);
});

test("wrong or stale session cannot receive the delivery", () => {
  const f = fixture();
  assert.throws(() => appendNative(f.pi, f.ctx, { ...delivery, session_id: "other" }), /different native session/);
  f.invalidate();
  assert.throws(() => appendNative(f.pi, f.ctx, delivery), /stale native context/);
  assert.equal(f.sends(), 0);
});

test("delivery identity and body bounds reject before native submission", () => {
  for (const change of [
    { message_id: "x".repeat(257) },
    { body: "x".repeat((1 << 20) + 1) },
  ]) {
    const f = fixture();
    assert.throws(() => appendNative(f.pi, f.ctx, { ...delivery, ...change }), /invalid Pi native delivery/);
    assert.equal(f.sends(), 0);
  }
  const f = fixture();
  assert.deepEqual(appendNative(f.pi, f.ctx, { ...delivery, message_id: "opaque message id" }), { accepted: true });
});

// DEF-04: a history write is a plain send with no options, which an idle Pi
// appends to history with no turn (Pi 1.2.0 agent-session.js:1848-1857).
test("history write is a plain send for the same native session", () => {
  const sends = [];
  const f = fixture();
  f.pi.sendMessage = (message, options) => { sends.push({ message, options }); };
  historyNative(f.pi, f.ctx, delivery);
  assert.deepEqual(sends, [{
    message: { customType: "sessionbus.message", content: delivery.body, display: true, details: { message_id: "message-1" } },
    options: undefined,
  }]);
  assert.throws(() => historyNative(f.pi, f.ctx, { ...delivery, session_id: "other" }), /different native session/);
  assert.equal(sends.length, 1);
});
