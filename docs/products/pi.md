# Pi product facts

> Historical source note: citations to pre-split Sessionbus paths resolve in
> the Forgejo `ai/sessionbus` repository through its `legacy-*` branches.
> Citations to product source resolve in the external repository and full
> commit recorded by the split archive manifest. Host evidence paths are
> immutable external artifacts, not repository paths.

Current integration: see [Pi/OMP design](../designs/pi-omp-0.5.0/DESIGN.md) and
[acceptance status](../designs/pi-omp-0.5.0/ACCEPTANCE.md) for native 0.85.1.
The facts and `UNVERIFIED` questions below describe the older source snapshot;
they are preserved as historical inputs, not the current release checklist.

## A lane prompt refused before it reaches Pi (2026-10-28)

A blank lane prompt, and one whose JSON encoding exceeds the 1 MiB native
input frame (Go escapes `<`, `>` and `&` as six bytes each), were refused
before anything was written but ended the lane. Decision (2026-10-28): both
now end only the Run, with the reason, as Pi's own refusal does; nothing is
written or replayed, and the lane stays.

## Tool loadout on resume (Pi 1.3.0, 2026-10-19)

Pi 1.3.0 restores a resumed session's recorded tool loadout whenever
`--tools` names no tools. The restore replaces the active set with the
transcript's `toolsAdded` (`agent-session.js` 1.3.0:214-215, 1374-1383). In
1.2.0 the same restore never ran. A later refresh activates only tools
registered after the previous build (`:2914-2918`). So a resumed session
whose transcript never declared `sessionbus`, for example one started by
plain `pi`, would run without its comms tool.

The managed extension therefore checks the active tools at every
`session_start` (startup, reload, new, resume and fork). If `sessionbus` is
missing it calls `setActiveTools` with our tool added after the others. If it
is present it does nothing. No other tool is removed or reordered.

## Tool modifiers in managed launches (2026-10-19)

Pi's `--tools` takes either plain names and patterns, or only `+name`/`-name`
modifiers. Modifiers change the default selection, in order
(`settings-manager.js:37-73`, `sdk.js:149-156`). With `--no-tools`, the
modifiers start from an empty selection that also bounds the allowed tools.
A list that mixes the two forms is Pi's own error.

The managed launch guard used to compare names with the `+`/`-` still
attached. It therefore rejected valid modifier lists such as
`--tools +sessionbus`. The guard now follows Pi's rule:
- a plain list must name `sessionbus`;
- a modifier list is refused only if it removes `sessionbus` last, or if
  `--no-tools` is set and the list does not add `+sessionbus`;
- a mixed list passes through, and Pi rejects it.

Arguments always pass through unchanged.

## Mandatory-wake source boundary (2026-09-21)

Pi 0.85.1 is bound to upstream `d981de1229ef899957bbe968bc8dcda02a21f477`.
For interactive delivery, an idle `sendMessage(...,{triggerTurn:true})` owns the
message when the synchronous `ctx.isIdle()` transition becomes false; matching
custom `message_start` and `message_end` events confirm its identity later.
Busy delivery enters a bounded wrapper queue and returns
`queued_for_next_turn` immediately, then source-backed lifecycle events drain
it without blocking the sender. Manual compaction publishes success or failure
witnesses. Branch navigation is canceled when accepted Sessionbus work exists;
new work is rejected while an unobservable branch summary is genuinely busy,
and a later native idle check clears stale busy state after failure or cancel.
The lane has no independent active append, so every inbound delivery returns
NotRunning before native submission and daemon v0.5.7 starts or schedules the
managed run. See
[the all-product boundary](../designs/mandatory-message-wake-20260921/NATIVE-BOUNDARIES.md).

- Pi was pinned and exercised as version `0.84.4`; its RPC child runs on Node. [Pi 0.84.4; source: `ff81565:internal/products/pifamily/quirks.go:12-20`, `ff81565:internal/products/pifamily/quirks.go:63-70`]
- Pi's managed process spelling uses separate argv values: `pi --extension <managed-plugin> --mode rpc ...`. [Pi 0.84.4; source: `ff81565:internal/products/pifamily/quirks.go:34-40`, `ff81565:internal/products/pifamily/quirks.go:63-69`, `ff81565:internal/products/pifamily/quirks.go:102-115`]
- Fresh identity is supplied as `--session-id <id>` and fresh title as `--name <title>`; exact resume is `--session <resume-id>`. [Pi 0.84.4; source: `ff81565:internal/products/pifamily/quirks.go:63-69`, `ff81565:internal/products/pifamily/lane.go:119-133`]
- The exact tested resume argv is `--extension /managed/sessionbus.mjs --mode rpc --session pi-native --tools read`. The product's returned `get_state.sessionId` must still equal `pi-native`. [Pi 0.84.4; source: `ff81565:internal/products/pifamily/rpc_lane_test.go:194-223`]
- Pi emits no readiness frame. Readiness is a correlated `get_state` call, and adoption requires a nonempty session ID with `isStreaming:false` and `isCompacting:false`. [Pi 0.84.4; source: `ff81565:internal/products/pifamily/quirks.go:24-31`, `ff81565:internal/products/pifamily/rpc.go:111-131`]
- Every RPC input is one newline-delimited JSON object with generated ID and command in `type`. The first state request has shape `{"id":"as-1","type":"get_state"}`. [Pi 0.84.4; source: `ff81565:internal/products/pifamily/rpc.go:163-201`]
- A successful correlated state response has exact shape `{"type":"response","id":"as-1","command":"get_state","success":true,"data":{"sessionId":"pi-native","sessionName":"live name","isStreaming":false,"isCompacting":false}}`. [Pi 0.84.4; source: `ff81565:internal/products/pifamily/rpc_lane_test.go:50-99`]
- A native error response has shape `{"type":"response","id":"as-1","command":"get_state","success":false,"error":"<native text>"}`; the command must match the pending request and unknown IDs are protocol errors. [Pi 0.84.4; source: `ff81565:internal/products/pifamily/rpc_lane_test.go:83-85`, `ff81565:internal/products/pifamily/rpc.go:329-351`]
- A run writes `{"id":"as-<n>","type":"prompt","message":"<text>"}` only after `get_state` confirms the session is idle. [Pi 0.84.4; source: `ff81565:internal/products/pifamily/lane.go:182-220`, `ff81565:internal/products/pifamily/rpc_lane_test.go:50-99`]
- Pi's terminal event is `agent_settled`. The exact successful fixture is `{"type":"agent_settled","messages":[{"stopReason":"stop"}]}`; the same `agent_end` frame must not terminate a Pi run. [Pi 0.84.4; source: `ff81565:internal/products/pifamily/quirks.go:63-69`, `ff81565:internal/products/pifamily/rpc_lane_test.go:194-242`]
- Final text is read after the terminal via `{"id":"as-<n>","type":"get_last_assistant_text"}`; its response data is `{"text":"<answer>"}`. A missing response is not converted into an empty successful result. [Pi 0.84.4; source: `ff81565:internal/products/pifamily/rpc.go:146-160`, `ff81565:internal/products/pifamily/rpc_lane_test.go:330-363`]
- Active steer first requires `get_state.isStreaming:true` and `isCompacting:false`, then sends `{"id":"as-<n>","type":"steer","message":"<text>"}`. Pi's row says the product does not add OMP-style native steer framing. [Pi 0.84.4; source: `ff81565:internal/products/pifamily/lane.go:301-329`, `ff81565:internal/products/pifamily/quirks.go:63-69`]
- Interrupt first rechecks state, marks the exact turn, and sends correlated `{"id":"as-<n>","type":"abort"}`. A failed abort write rolls back the interrupted mark. [Pi 0.84.4; source: `ff81565:internal/products/pifamily/lane.go:331-353`, `ff81565:internal/products/pifamily/rpc_lane_test.go:448-575`]
- After an accepted interrupt, only Pi's `agent_settled` event closes the run; the resulting native stop reason can be `aborted`. [Pi 0.84.4; source: `ff81565:internal/products/pifamily/rpc_lane_test.go:448-523`]
- Unknown additive streaming events are ignored at this pinned version. Correlated responses, terminal events, and RPC UI requests remain closed typed control frames. [Pi 0.84.4; source: `ff81565:internal/products/pifamily/rpc.go:225-310`]
- Pi's default unattended permission profile is argv `--tools read,grep,find,ls`; bypass uses the product's full default tool set with no extra argv. Other shared permission values are unsupported. [Pi 0.84.4; source: `ff81565:internal/products/pi/permission.go:12-23`, `ff81565:internal/products/pi/permission_test.go:11-24`]
- Model flags were forwarded to the native process; the exact tested addition was `--model deepseek/deepseek-v4-flash`. Lifecycle-owned flags such as `--mode`, `--extension`, `--session`, `--session-id`, `--name`, and tool/approval controls were not allowed through generic arguments. [Pi 0.84.4; source: `ff81565:internal/products/pifamily/rpc_lane_test.go:729-790`, `ff81565:internal/products/pifamily/quirks.go:121-133`]
- The 0.4.0 sources do not contain a typed Pi `--thinking` translation; they only demonstrate arbitrary nonreserved argv forwarding. Treat reasoning-effort spelling as requiring fresh product-help or host evidence before implementation. [Pi 0.84.4; source: `ff81565:internal/products/pifamily/lane.go:90-133`, `ff81565:internal/products/pifamily/rpc_lane_test.go:729-790`]
- Interactive extension identity comes from `ctx.sessionManager.getSessionId()`. On `session_start`, the extension reports that exact ID, `pi.getSessionName()`, and `ctx.cwd`; on `session_info_changed`, it reports the observed title, including a genuine cleared empty title. [Pi extension on 0.84.4; source: `ff81565:integrations/pi/pifamily.mjs:43-51`, `ff81565:integrations/pi/pifamily.mjs:133-197`]
- If a requested name exists and the product has no title, the extension calls `pi.setSessionName(requestedName)` before its initial report. It does not overwrite an existing product title on reconnect. [Pi extension on 0.84.4; source: `ff81565:integrations/pi/pifamily.mjs:133-176`, `ff81565:integrations/pi/pifamily.test.mjs:74-115`]
- Interactive inbound delivery calls `pi.sendUserMessage(<rendered-text>)` when `ctx.isIdle()` and `pi.sendUserMessage(<rendered-text>,{"deliverAs":"steer"})` otherwise. [Pi extension on 0.84.4; source: `ff81565:integrations/pi/pifamily.mjs:157-174`, `ff81565:integrations/pi/pifamily.test.mjs:117-131`]
- The ordinary idle `sendUserMessage` call starts product work; it is not evidence of an append-without-run primitive. A lane wrapper therefore needs its own bounded idle FIFO unless a separate native primitive is proved. [Pi extension on 0.84.4; source: `ff81565:integrations/pi/pifamily.mjs:157-174`]
- Tool ingress is Pi's in-process extension API: `pi.registerTool({name:"sessionbus", ...})`. Exact session context is checked before forwarding, and abort-before-dispatch is rejected. No MCP transport was used by this integration. [Pi extension on 0.84.4; source: `ff81565:integrations/pi/pifamily.mjs:84-115`]
- `session_shutdown` removes the current native identity; reason `quit` also stops the shared live client and releases its process-global extension runtime. [Pi extension on 0.84.4; source: `ff81565:integrations/pi/pifamily.mjs:199-209`]
- UNVERIFIED before a new wrapper: installed Pi help and runtime evidence for `--thinking`, plus the complete fresh/resume/title/model/thinking argv. The signed design requires `--thinking`, while the pinned implementation only proves generic nonreserved forwarding. [Pi version pending host probe; source: `ff81565:internal/products/pifamily/lane.go:90-133`, `ff81565:internal/products/pifamily/rpc_lane_test.go:729-790`, `91fdcb3:docs/designs/UNIVERSAL-SESSION-PROTOCOL.md:1594-1600`, `91fdcb3:docs/designs/UNIVERSAL-SESSION-PROTOCOL.md:1753`]
- UNVERIFIED before a new wrapper: native append-without-run behavior for idle lane delivery. The product's active `steer` is proven, but the captured interactive idle call starts work; lane mode therefore needs the wrapper FIFO specified by the design. [Pi version pending host probe; source: `ff81565:internal/products/pifamily/lane.go:301-329`, `ff81565:integrations/pi/pifamily.mjs:157-174`, `91fdcb3:docs/designs/UNIVERSAL-SESSION-PROTOCOL.md:1598`]
- UNVERIFIED before a new wrapper: native MCP transport/configuration support. The verified ingress is the in-process extension API; the signed design uses that extension in peer/direct or lane/local mode rather than asserting MCP support in Pi. [Pi version pending host probe; source: `ff81565:integrations/pi/pifamily.mjs:84-115`, `91fdcb3:docs/designs/UNIVERSAL-SESSION-PROTOCOL.md:1597`]
