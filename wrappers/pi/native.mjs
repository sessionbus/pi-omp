// SPDX-License-Identifier: MIT

export const customMessageType = "sessionbus.message";

function nativeIdentity(ctx) {
  const id = ctx.sessionManager.getSessionId();
  if (typeof id !== "string" || !id.length || Buffer.byteLength(id) > 256 || /[\s\0]/u.test(id)) {
    throw new Error("Pi omitted its native session identity");
  }
  return id;
}

export function describeNative(pi, ctx) {
  const session_id = nativeIdentity(ctx);
  const name = pi.getSessionName() ?? "";
  if (typeof name !== "string" || typeof ctx.cwd !== "string" || !ctx.cwd.length) {
    throw new Error("Pi native session metadata is invalid");
  }
  return { session_id, name, cwd: ctx.cwd };
}

// Pi d981de1 sets _isAgentRunActive synchronously at the start of
// _runAgentPrompt. Reading ctx.isIdle() before and after sendMessage therefore
// distinguishes a definite pre-submission refusal from native ownership. The
// custom message is persisted later, after its asynchronous message_end event;
// an old session leaf is not an admission signal.
//
// Pi 1.2.0 defers a triggerTurn send made during agent_settled handlers until
// every settled handler has finished and stays idle until then
// (agent-session.js:1836-1838, run at :696-703). Such a send is accepted by
// that contract (deferred). If it then fails to start, the error leaves
// _emitAgentSettled; no extension event reports it.
export function appendNative(pi, ctx, delivery, { deferred = false } = {}) {
  if (!delivery || delivery.session_id !== nativeIdentity(ctx)) {
    throw new Error("Pi delivery belongs to a different native session");
  }
  if (typeof delivery.message_id !== "string" || !delivery.message_id.length ||
      Buffer.byteLength(delivery.message_id) > 256 ||
      typeof delivery.body !== "string" || Buffer.byteLength(delivery.body) > (1 << 20)) {
    throw new Error("invalid Pi native delivery");
  }
  if (!ctx.isIdle()) return { accepted: false, reason: "busy" };
  pi.sendMessage({
    customType: customMessageType,
    content: delivery.body,
    display: true,
    details: { message_id: delivery.message_id },
  }, { triggerTurn: true });
  if (!deferred && ctx.isIdle()) {
    throw new Error("Pi did not start the native message turn");
  }
  return { accepted: true };
}
