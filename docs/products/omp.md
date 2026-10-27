# Oh My Pi product facts

> Historical source note: citations to pre-split Sessionbus paths resolve in
> the Forgejo `ai/sessionbus` repository through its `legacy-*` branches.
> Citations to product source resolve in the external repository and full
> commit recorded by the split archive manifest. Host evidence paths are
> immutable external artifacts, not repository paths.

## A lane prompt that fails before OMP's run (2026-10-27)

OMP admits a prompt and answers `success`, and can then still fail it before
it hands the message to the agent: no model, no API key, a failed `@path`
read, pre-prompt compaction or a `before_agent_start` handler
(`agent-session.ts` 18.12.2:8196-8301). RPC then writes a second,
`success:false` response for the same id, followed by `prompt_result` with
`agentInvoked:false` and `status:"error"` (`rpc-mode.ts:1621-1622`,
`rpc-prompt-results.ts:109-111, 149-166, 292-296`). An error as the first
response is OMP's refusal and was already handled.

Decision (2026-10-27): a Run must end with its reason and the lane must
stay. Such a failure, with no run start, yield or assistant message for our
prompt, now ends only the Run: it is `unavailable` with OMP's text, a seed
message is reported `rejected`/`not_submitted`, and the failed prompt's
trailing `prompt_result` is absorbed once. Before, the failure ended the lane,
and the Run's record went with it. A prompt our own validation refuses
(empty, or over the native bound of 1 MiB less 1024 bytes) likewise ends only
the Run. A late failure after OMP's run started, and transport or protocol
faults, still end the lane. The Run terminal has no field for a failure
reason, so a Run that OMP's provider error ends after its start stays `done`,
outcome `failed`, native stop reason `error`, without OMP's text.

## Peer messages that start a lane Run on OMP 18.12.2 (2026-10-27)

OMP's prompt path reads the files that each `@path` in the prompt names and
adds their contents for the model (`agent-session.ts` `#promptWithMessage`,
`utils/file-mentions.ts`). No prompt option or RPC field turns it off. An `@`
starts a mention at the start of the text or after whitespace or one of
`([{<"'`; the mention is a quoted path or the following run of characters
that are neither whitespace nor `@`.

An idle lane starts a Run for a peer message by sending the rendered message
as its RPC `prompt`, so OMP would read files that the peer names. Owner
decision (2026-10-27): the lane inserts U+2060 WORD JOINER right before each
`@` at a mention boundary in that message. U+2060 is neither whitespace nor
one of those characters, so the `@` starts no mention: OMP extracts none there
and resolves no path. The model sees the same visible text. A joiner right
after the `@`, the first version, did not suffice: OMP still extracted the
mention, and path resolution cancels the joiner-carrying first segment against
a following `..`, so `@x/../notes.txt` still read `notes.txt`. Only that path
changes. The owner's Run input is sent as given;
a steered message (OMP's steering queue) and the interactive primary's
deliveries (the steering queue or OMP's agent-initiated prompt) do not pass
through `#promptWithMessage` and are unchanged.

## Lane Run boundary on OMP 18.11.0 (2026-10-19)

Owner decision (2026-10-15): an OMP lane Run ends at OMP's own done report for
our prompt, and later work by OMP or other extensions is OMP's own and no
longer ends the lane. The floor is OMP 18.11.0, stated here and in the README
with no runtime version check. Source: OMP 18.11.0 npm `src`
(`modes/rpc/rpc-prompt-results.ts`, `modes/rpc/rpc-mode.ts`,
`session/agent-session.ts`, `cli-commands.ts`).

- **End of a Run.** OMP writes one `prompt_result` with our request id after
  all work of the prompt settled (`rpc-types.ts:229-246`). The Run takes its
  result from the last assistant message before its run's first yielded
  `agent_end` (`yielded ?? isTerminal !== false`, the rule OMP uses at
  `rpc-prompt-results.ts:136`), and ends at our agent-invoked `prompt_result`.
- **OMP's own work.** A run event before our run starts or after it yielded,
  or with no Run open, is ignored. A dialog from that work is cancelled with no
  Run to join, as for Pi.
- **Refusal.** A prompt OMP refuses before admission (for example its native
  busy rejection while its own work runs, `agent-session.ts:7356-7364`) gets an
  error response, then a success response for the same id, then
  `prompt_result` with `agentInvoked:false`. Our RPC reader absorbs exactly one of each for that
  id. The Run fails with OMP's answer and is recorded unavailable, its
  delivery is rejected `not_submitted`, and the lane stays, as for Pi. The slot
  is cleared by the next admitted prompt, a new refusal or transport loss.
- **Preflights.** OMP also preflights its own runs. Each Run opens a window
  before it sends its prompt and closes it at turn end. Every preflight is
  consumed when it arrives. In the window the first one with our prompt is
  ours; one before it is recorded as foreign and fails an admitted Run; one
  after it is ignored. Outside a window a preflight is only recorded. If our
  agent-invoked `prompt_result` arrives without our preflight, the Run fails
  as "owned preflight missing or rewritten", where it used to wait.
- **Subcommands.** OMP drops recognised launch flags, including our
  `--extension` and `--mode`, in front of a hoisted subcommand other than
  `launch` and `acp` (`cli-commands.ts:313`, `402-428`, `462`). The wrapper
  therefore lists all 53 commands and 6 aliases and runs them directly.

Not covered, listed:

- Own work that starts after our Run opens and before OMP reads our prompt is
  counted as ours. Our prompt is then refused as busy and that Run is dropped.
  If the own work also ends before OMP reads our prompt, our prompt is admitted
  and its foreign preflight fails the Run.
- There is no wait for idle before a Run, unlike Pi. While OMP's own work
  runs, each Run is refused.
- The extension sends its preflight on a separate socket without waiting. If
  it reaches the wrapper after our `prompt_result`, the Run fails as missing.
- A session switch during our Run makes OMP report our prompt aborted without
  our run's yielded `agent_end`; the Run fails.
- An `agentInvoked:false` after agent work, or an error after the success
  response, is not a refusal and still fails the Run.
- OMP 18.1.17 never sends an agent-invoked `prompt_result`; a lane Run there
  waits until it is cancelled.

## Lane busy steering on OMP 18.11.0 (2026-10-19)

Owner selection (2026-10-19): OMP keeps its Go helper for this release, and
lanes get busy steering. Source: OMP 18.11.0 npm `src`
(`session/agent-session.ts`, `modes/runtime-init.ts`, `modes/rpc/rpc-mode.ts`)
and `@oh-my-pi/pi-agent-core` 18.11.0 `src/agent-loop.ts`.

- **When.** `Deliver` steers only while the Worker's Run is the lane's current
  Run, its own preflight was taken, and its run started and has not yielded.
  Otherwise the lane is not running, as before.
- **How.** Go calls `native.steer` for the lane's primary session over the
  private bridge, with the token of the Run's own preflight. The extension
  keeps that token from its preflight until the run yields
  (`!willContinue || awaitingAsyncWork`, OMP's own rule at
  `agent-session.ts:5063-5067`) or a later preflight replaces it. In one
  synchronous step it checks the token, checks `!ctx.isIdle()` and calls
  `pi.sendMessage(message, {deliverAs:"steer"})`. Otherwise it answers idle
  and makes no native call.
- **Receipt.** `injected` claims only that hand-off. OMP's `sendMessage`
  returns nothing, and OMP chooses between its steering queue and history only
  after an await (`agent-session.ts:8814-8840`). No native confirmation is
  claimed.
- **No `triggerTurn`.** If OMP went idle during that await, the message is
  appended to history without a turn (`agent-session.ts:8904-8911`) and the
  next Run reads it. With `triggerTurn` it would start OMP's own run instead,
  and a rejection there is written as an error response with no `id`
  (`runtime-init.ts:103-105`, `rpc-mode.ts:1647-1649`), which the RPC reader
  treats as a protocol failure.
- **Failures.** A failed or invalid steer answer leaves the outcome
  uncertain. The delivery fails with that reason and is never retried. That
  failure by itself retires neither the Run nor the lane. Losing the bridge
  or the owner still ends the lane, as before (`native_owner.go:264-271`).
  Unlike interactive `native.stage`, no FIFO order is reserved.

Residual, documented and not a defect: OMP's loop polls steering at each stop
boundary and once more just before it yields (`agent-loop.ts:1752-1778`,
`1788-1799`). A steer queued after that last poll makes OMP re-tag our
yielded `agent_end` `isTerminal:false` (`agent-session.ts:1551-1583`), report
our `prompt_result` there, and then run its queued-message drain as its own
continuation. The message reaches the model and its answer is in the native
history, but not in the Run's result. A steer queued before an interrupt is
delivered the same way (`agent-loop.ts:1744-1751`). There is no earlier gate:
OMP places the message after an await, so a gate would not close the gap and
would refuse steers the run could still read.

## Interactive receipts on OMP 18.11.0 (2026-10-19)

Owner selection (2026-10-19): fix the receipt wording. This replaces the
`queued_for_next_turn` receipt of the section below for the interactive
primary session only.

- `native.stage` still hands each accepted message to OMP at once with
  `sendMessage(...,{deliverAs:"steer",triggerTurn:true})`. Its answer now
  also carries `busy`, read from `!ctx.isIdle()` before that send.
- A busy OMP puts the message in its running task's steering queue, so the
  receipt is `injected`. An idle OMP starts a turn with it, so the receipt is
  `written`. OMP makes that choice only after an await
  (`agent-session.ts:8814-8840`), so both receipts are decided at the call
  and neither is a native confirmation.
- A native Task child's answer carries no busy state, and its receipt stays
  `queued_for_next_turn`. A queue that is full is still rejected
  `queue_full`.

## Interactive Escape on OMP 18.11.0 (2026-10-19)

Owner selection (2026-10-19): release with today's behaviour. DEF-20261015-05
moves into the OMP rework item as an open defect. It is not fixed and does not
block this release, and no code handles it. Source: OMP 18.11.0 npm `src`
(`modes/controllers/input-controller.ts`, `session/queued-messages.ts`,
`session/agent-session.ts`, `modes/controllers/extension-ui-controller.ts`).

- **Escape while the working animation shows.** OMP takes the queued
  user-authored messages back into the editor, then aborts
  (`input-controller.ts:564-569, 1960-1982`). Our interactive delivery is a
  custom message with `attribution:"user"` and `display:true`, which OMP
  counts as user-queued (`queued-messages.ts:61-72`). A steered peer message
  the task had not read yet therefore comes back into the user's editor as if
  typed. It is neither sent nor written to history unless the user submits
  it, and its receipt stays `injected`.
- **Other stops.** An abort that does not clear the queue leaves our message
  queued. Examples are Escape while no animation shows
  (`input-controller.ts:581-582`) and an extension's `ctx.abort()`
  (`extension-ui-controller.ts:245`). The abort's cleanup then drains the
  queue into a new turn (`agent-session.ts:9794-9797`), so the message is
  delivered in that next turn.

## Interactive delivery and OMP's context order (2026-10-19)

The first live idle delivery ended the session. Our extension saw the
delivery in OMP's `context` event before the message's own `message_start`
and `message_end`, and treated that as a protocol violation. It aborted the
task ("Interrupted by user") and shut the session down. The order is OMP's
by design, in 18.1.17 and 18.11.0 alike. The agent loop builds the
provider context, and runs the `context` hook, before it emits the turn's
input message events (`@oh-my-pi/pi-agent-core` `agent-loop.ts`
18.11.0:1312-1328, 1360, 1412-1416). That covers both an idle prompt and a
steer taken at a turn boundary.

- One exact copy of a claimed delivery in such a context is now accepted.
  The delivery completes at its `message_end`, and the wrapper still gets
  the claimed, `message_start`, `message_end` and `context` reports in that
  order.
- A changed copy, or more than one copy, still ends the session.
- Native Task children use the same check and are covered.

## Mandatory-wake source boundary (2026-09-21)

OMP 18.1.17 is bound to upstream
`3b3a6dc9bbd85102ce19d0b1c11bf6870915f6ec`. Interactive `native.stage`
accepts a bounded delivery into the product owner queue and returns
`queued_for_next_turn`; the caller must not resend that accepted message. The
owner schedules it with
`sendMessage(...,{deliverAs:"steer",triggerTurn:true})`, and correlates the
claimed custom message, `message_start`, `message_end`, and context events.
Image normalization happens before native scheduling, so the immediate product-owner
acceptance receipt and later native identity proof remain distinct. The later
events are collection evidence, not a second sender receipt. After normalization,
a streaming native session puts the exact custom message in the agent's steer
queue and schedules its idle drain; an idle session calls
`promptAgentInitiatedMessage`. This one carrier therefore admits active input
without waiting for the current tool call and starts idle work without a
wrapper-side idle/active race. Native usage denial, abort or disposal during an
idle prompt can still decline dispatch behind the API's void return, and an
explicit native interrupt can discard an agent-core custom steer. The wrapper
does not claim model success from its queued receipt or replay either class: an
unconfirmed batch keeps its bounded reservation until a native reset or owner
lifecycle retires it, and the process cap rejects further input rather than
silently freeing an unknown batch. A durable
`reset_boundary` created after the wrapper claims a batch retires the owner
instead of leaving retained capacity wedged, and the wrapper never replays a
possibly submitted batch. Context emitted by the original running turn, or repeated by the prior
batch, is ignored while the newly scheduled batch has not reached its own
message events. Any early reference to that claimed batch remains an ordering
failure; after `message_end`, its context must contain exactly one matching
message. Further bounded batches are submitted independently and correlated
by their unique batch tokens. The lane has no
independent active append, so every inbound delivery returns NotRunning before
native submission and daemon v0.5.7 starts or schedules the managed run. See
[the all-product boundary](../designs/mandatory-message-wake-20260921/NATIVE-BOUNDARIES.md).

## OMP 18.1.17 tool approval source audit (2026-09-19)

The following facts come from native commit
`3b3a6dc9bbd85102ce19d0b1c11bf6870915f6ec`; they do not claim a new installed
acceptance run.

- `ToolDefinition.approval` accepts an approval decision. The extension loader
  stores the definition intact, and both registered-tool and approval wrappers
  proxy its properties. Thus `{tier: "exec", policy: "allow"}` reaches native
  resolution without an SDK conversion dropping it. Sources:
  `packages/coding-agent/src/extensibility/extensions/types.ts:642`,
  `extensions/loader.ts:180`, `extensions/wrapper.ts:47,166`, and
  `extensibility/tool-proxy.ts:6` under the same source tree.
- `resolveApproval` honors tool-owned `policy: "allow"` before the mode tier
  comparison, including under `always-ask`. An explicit native per-tool deny
  takes precedence. Source: `packages/coding-agent/src/tools/approval.ts:132-214`.
- OMP's ordinary CLI `--tools` and `--no-tools` set `options.toolNames`, but
  non-hidden extension tools are still included in unrestricted sessions.
  Do not infer Pi's exclusion behavior for OMP or reject these flags merely
  because their names match. Sources: `packages/coding-agent/src/main.ts:1343-1348`
  and `packages/coding-agent/src/sdk.ts:3293-3305`. The managed lane already
  reserves its tool-selection arguments.
- The installed results below verify primary peer, native Task child and lane
  communication under ordinary policy and preserve an unrelated mutating
  tool's approval. Source inspection alone does not establish those results.

### Native device transport and the first installed grant check

The first ordinary interactive check of `5beacab` on UMKA failed under
`always-ask`: the model invoked `write` on `xd://sessionbus`, and native Write
approval prevented the Sessionbus call. No list/send result or receiver marker
was observed. The denial and cleanup remain part of that check's evidence;
the lane default and explicit bypass message checks passed separately.

This route is native behavior. On the same 18.1.17 source,
`tools/write.ts:518-554` uses the mounted tool's tier and a device policy key,
but `tools/approval.ts:96-98` returns only the tier, dropping its declared
`policy: "allow"` at the outer Write gate. An inner tool grant therefore does
not establish unattended use through this device transport under always-ask.

Register Sessionbus with the native `loadMode: "essential"` so it is exposed
directly. `extensibility/extensions/types.ts:634-636` documents this choice,
`extensions/wrapper.ts:47-48` preserves it, and `tools/xdev.ts:84-87` plus
`sdk.ts:3350-3362` keep essential tools top-level. Other tools retain their
native presentation and policy. The exec-tier allow remains unchanged. The fresh installed checks below verify the correction;
the previous failed attempt is not relabeled or replayed.

The first direct-tool lane check of `817879c` exposed a separate schema issue:
native `openai-codex/gpt-5.5` supplied defaults for all optional union argument
fields. The public validator rejected those foreign fields; no message was
delivered. Preserve that failure separately from the earlier approval failure.

Declare `strict: false` on this tool. Native
`extensibility/extensions/types.ts:643-645` documents explicit false as distinct
from omission, and the wrappers forward the property. In the same source,
`packages/ai/src/providers/openai-codex-responses.ts:4744-4752` emits explicit
`strict: false` only when the definition supplies it (unless the native global
strict-field suppression is selected). `packages/ai/src/utils/schema/CONSTRAINTS.md:56`
records optional-field overfill on backends when the flag is omitted. This
tool-local setting preserves the public optional-field schema and validation;
it does not remove unexpected arguments or change any other tool. Registration
tests cover lane, primary and Task-child definitions.

## Installed communication checks (2026-09-19)

Candidate `d84c4e1c27a89b14a94882a29d054b869ca9f396` was installed through
its ordinary archive installer into UMKA's permanent installation, with native
OMP 18.1.17 and `openai-codex/gpt-5.5`. Ordinary lane, primary peer and native
Task-child calls listed their identities and sent unique markers with settled
`injected` receipts and direct receiver observation. Calls used top-level
`sessionbus` with sparse action arguments. An unrelated native write still
prompted under `always-ask`; declining it returned `Tool call denied by user:
write`, and the file was absent. Explicit bypass lane and primary `--yolo`
checks also delivered their markers. This does not claim a Task-child bypass
check on this final candidate.

The earlier Write-transport approval failure, optional-field overfill failure,
and an earlier bypass primary/Task check with no shared receiver group remain
recorded separately. No successful delivery is attributed to those failed
checks. All owned sessions and processes were cleaned up; the daemon stayed
healthy. Evidence is sealed at `omp-comms-grant-installed-20260919`:
`SHA256SUMS` SHA256
`79909c0a7661bcb06463ade2a7f6f74017edfa80eac8735f16e609577172ed93`;
`OBSERVATIONS.json` SHA256
`d4816f982810ddaa3105d5eddd5e4de6e23f701967bb8b08136ed14224e17d47`.

## Historical product facts

### Explicit bypass compatibility and earlier working implementation

The earlier `ff81565:internal/products/omp/permission.go:15-23` mapped explicit
`bypassPermissions` to `--approval-mode=yolo`. Its shared extension registered
its then-current public tool without a tool-owned approval declaration
(`ff81565:integrations/pi/pifamily.mjs:100-115`). Retain the explicit bypass
behavior; do not infer that this legacy path proved a narrow default grant.

On native `3b3a6dc9bbd85102ce19d0b1c11bf6870915f6ec`,
`packages/coding-agent/src/cli/args.ts:279-280` maps `--yolo` and
`--auto-approve` to the same option. `cli/flag-tables.ts:226-234` accepts
`--approval-mode=yolo`, and `main.ts:1524-1531` applies it only to the running
session's settings. `tools/approval.ts:157-175` preserves an explicit tool-owned
allow in yolo mode. The managed extension keeps its exec-tier allow in both
ordinary and bypass launches. A lane's `permission_mode=bypassPermissions`
selects `--approval-mode=yolo`; empty/default still injects no global mode.

The UMKA help-only capture on 2026-09-19 confirms OMP 18.1.17 and
`--auto-approve` / `--approval-mode` at lines 57-58 of `omp-help.stdout` in
`umka-native-version-preflight-20260919` (evidence manifest SHA256
`708fa0f5b37f28098c8d3fa08a75ad349a280e92c7d0b80a087768d931d5da78`).
This capture is surface evidence; the installed communication checks above are separate.

Current integration: see [Pi/OMP design](../designs/pi-omp-0.5.0/DESIGN.md) and
[acceptance status](../designs/pi-omp-0.5.0/ACCEPTANCE.md) for native 18.1.17.
The facts and `UNVERIFIED` questions below describe the older source snapshot;
they are preserved as historical inputs, not the current release checklist.

- Oh My Pi (OMP) was pinned and exercised as version `18.0.11`; its RPC child runs on Bun. [OMP 18.0.11; source: `ff81565:internal/products/pifamily/quirks.go:12-20`, `ff81565:internal/products/pifamily/quirks.go:71-77`]
- OMP requires equals-style launch controls: `omp --extension=<managed-plugin> --mode=rpc ...`; resume remains the separate pair `--session <id>`. [OMP 18.0.11; source: `ff81565:internal/products/pifamily/quirks.go:34-40`, `ff81565:internal/products/pifamily/quirks.go:71-77`, `ff81565:internal/products/pifamily/quirks.go:102-119`]
- The exact tested fresh argv is `--extension=/managed/sessionbus.mjs --mode=rpc --model deepseek/deepseek-v4-flash --tools read`; the exact tested resume argv is `--extension=/managed/sessionbus.mjs --mode=rpc --session omp-native --tools read`. [OMP 18.0.11; source: `ff81565:internal/products/pifamily/rpc_lane_test.go:419-445`, `ff81565:internal/products/pifamily/rpc_lane_test.go:729-770`]
- OMP does not accept a caller-supplied fresh session ID in the pinned quirk row. The wrapper learns the native ID from `get_state`; fresh title is then applied by RPC `{"id":"as-<n>","type":"set_session_name","name":"<title>"}`. [OMP 18.0.11; source: `ff81565:internal/products/pifamily/quirks.go:71-77`, `ff81565:internal/products/pifamily/lane.go:119-161`, `ff81565:internal/products/pifamily/rpc_lane_test.go:366-417`]
- OMP must emit a ready frame before any response. The exact compatible shape is `{"type":"ready","protocolVersion":1,"supportedProtocolVersions":[1],"maxFrameBytes":1048576}`. [OMP 18.0.11; source: `ff81565:internal/products/pifamily/rpc.go:225-260`, `ff81565:internal/products/pifamily/rpc.go:312-327`, `ff81565:internal/products/pifamily/rpc_lane_test.go:419-423`]
- A response before ready, a repeated ready frame, an omitted protocol 1, or a `maxFrameBytes` other than 1048576 is incompatible/protocol failure. [OMP 18.0.11; source: `ff81565:internal/products/pifamily/rpc.go:249-265`, `ff81565:internal/products/pifamily/rpc.go:312-327`, `ff81565:internal/products/pifamily/rpc_lane_test.go:578-597`]
- Every RPC input is one newline-delimited JSON object. A state request is `{"id":"as-1","type":"get_state"}` and a successful response is `{"type":"response","id":"as-1","command":"get_state","success":true,"data":{"sessionId":"omp-native","sessionName":"live name","isStreaming":false,"isCompacting":false}}`. [OMP 18.0.11; source: `ff81565:internal/products/pifamily/rpc.go:163-201`, `ff81565:internal/products/pifamily/rpc_lane_test.go:50-99`]
- Responses must carry the exact pending ID and command. A native rejection has shape `{"type":"response","id":"as-1","command":"<command>","success":false,"error":"<native text>"}`; an unknown ID or command mismatch closes the protocol. [OMP 18.0.11; source: `ff81565:internal/products/pifamily/rpc.go:329-351`]
- A run first confirms idle with `get_state`, then sends `{"id":"as-<n>","type":"prompt","message":"<text>"}`. [OMP 18.0.11; source: `ff81565:internal/products/pifamily/lane.go:182-220`]
- OMP's terminal event is `agent_end`. The exact continuing frame `{"type":"agent_end","willContinue":true,"messages":[]}` must be ignored; the exact terminating fixture is `{"type":"agent_end","messages":[{"stopReason":"stop"}]}`. [OMP 18.0.11; source: `ff81565:internal/products/pifamily/quirks.go:71-77`, `ff81565:internal/products/pifamily/rpc_lane_test.go:366-417`]
- Pi's `agent_settled` event does not terminate OMP. An accepted interrupt followed by `agent_end` with `{"stopReason":"aborted"}` yields an interrupted outcome. [OMP 18.0.11; source: `ff81565:internal/products/pifamily/rpc_lane_test.go:448-523`]
- Final text is fetched after terminal with `{"id":"as-<n>","type":"get_last_assistant_text"}` and response data `{"text":"<answer>"}`. Missing collection cannot be invented as empty success. [OMP 18.0.11; source: `ff81565:internal/products/pifamily/rpc.go:146-160`, `ff81565:internal/products/pifamily/rpc_lane_test.go:330-363`]
- Active steer requires `get_state.isStreaming:true` and sends `{"id":"as-<n>","type":"steer","message":"<raw-priority>"}`. OMP performs its own system-notice framing, so callers pass the original content exactly once. [OMP 18.0.11; source: `ff81565:internal/products/pifamily/lane.go:301-329`, `ff81565:internal/products/pifamily/rpc_lane_test.go:394-404`]
- Interrupt rechecks state and sends correlated `{"id":"as-<n>","type":"abort"}`. A failed abort write rolls the interrupted mark back. [OMP 18.0.11; source: `ff81565:internal/products/pifamily/lane.go:331-353`, `ff81565:internal/products/pifamily/rpc_lane_test.go:526-575`]
- Fire-and-forget `extension_ui_request` methods are exactly `cancel`, `notify`, `setStatus`, `setWidget`, `setTitle`, and `set_editor_text`; they are ignored. The exact frame skeleton is `{"type":"extension_ui_request","id":"ui-1","method":"<method>"}`. [OMP 18.0.11; source: `ff81565:internal/products/pifamily/rpc.go:286-304`, `ff81565:internal/products/pifamily/rpc_lane_test.go:692-710`]
- Approval-bearing UI methods `select`, `confirm`, `input`, `editor`, and `open_url` require mediation and fail closed. The exact tested approval frame is `{"type":"extension_ui_request","id":"approval-1","method":"confirm","title":"allow tool?"}`. [OMP 18.0.11; source: `ff81565:internal/products/pifamily/rpc.go:294-303`, `ff81565:internal/products/pifamily/rpc_lane_test.go:673-690`]
- Default permission was not runnable unattended because RPC approval mediation was absent. Explicit bypass maps to `--approval-mode=yolo`; other shared permission values are unsupported. [OMP 18.0.11; source: `ff81565:internal/products/omp/permission.go:12-23`, `ff81565:internal/products/omp/permission_test.go:11-28`]
- The 0.4.0 sources demonstrate native `--model <provider/model>` forwarding but do not contain a typed `--thinking` translation. Any reasoning-effort spelling needs fresh product-help or host evidence before implementation. [OMP 18.0.11; source: `ff81565:internal/products/pifamily/rpc_lane_test.go:729-770`, `ff81565:internal/products/pifamily/lane.go:90-133`]
- Lifecycle-owned argv includes mode, extension, session/resume/session-id, session directory/name, tools/exclusions, and approval/yolo flags; generic arguments must not override them. [OMP 18.0.11; source: `ff81565:internal/products/pifamily/quirks.go:121-133`, `ff81565:internal/products/pifamily/rpc_lane_test.go:776-790`]
- OMP's interactive integration is the same Pi-family in-process extension, selected by the three-line entrypoint `createPiFamilyExtension("omp")`. [OMP extension on 18.0.11; source: `ff81565:integrations/omp/agent-sessions.mjs:1-3`, `ff81565:integrations/omp/entrypoint.test.mjs:1-8`]
- Interactive identity comes from `ctx.sessionManager.getSessionId()`. On `session_start`, the extension reports the exact ID, native `getSessionName()`, and cwd; `session_info_changed` retitles it. [OMP extension on 18.0.11; source: `ff81565:integrations/pi/pifamily.mjs:43-51`, `ff81565:integrations/pi/pifamily.mjs:133-197`]
- If a requested name exists and the product has no current title, the extension calls `setSessionName(requestedName)` before reporting. An existing product-owned title wins on reconnect. [OMP extension on 18.0.11; source: `ff81565:integrations/pi/pifamily.mjs:133-176`, `ff81565:integrations/pi/pifamily.test.mjs:74-115`]
- Interactive inbound delivery calls `sendUserMessage(<rendered-text>)` while idle and `sendUserMessage(<rendered-text>,{"deliverAs":"steer"})` while busy. The busy shape is verified as native steer. [OMP extension on 18.0.11; source: `ff81565:integrations/pi/pifamily.mjs:157-174`, `ff81565:integrations/pi/pifamily.test.mjs:117-131`]
- Historical observation, superseded by the current `sendMessage` steer carrier documented in the mandatory-wake source boundary above: the old idle call has no `deliverAs:"nextTurn"` option and therefore does not verify a native next-turn queue. A wrapper must obtain product evidence before reporting `queued_for_next_turn` in peer mode. [OMP extension on 18.0.11; source: `ff81565:integrations/pi/pifamily.mjs:157-174`]
- Tool ingress is OMP's in-process extension API through the shared `registerTool({name:"sessionbus",...})`; exact session context and abort-before-dispatch are checked. No MCP transport was used by this integration. [OMP extension on 18.0.11; source: `ff81565:integrations/pi/pifamily.mjs:84-115`, `ff81565:integrations/omp/agent-sessions.mjs:1-3`]
- `session_shutdown` removes the exact identity; reason `quit` also stops the shared live client and clears the process-global extension runtime. [OMP extension on 18.0.11; source: `ff81565:integrations/pi/pifamily.mjs:199-209`]
- CONTRADICTION requiring a host probe before implementation: the pinned 18.0.11 test uses separate `--session <id>` for resume, while the signed design requires equals-style `--resume=<id>`. Neither spelling may be selected from design alone. [OMP 18.0.11 versus pending installed version; source: `ff81565:internal/products/pifamily/rpc_lane_test.go:419-445`, `91fdcb3:docs/designs/UNIVERSAL-SESSION-PROTOCOL.md:1608-1609`, `91fdcb3:docs/designs/UNIVERSAL-SESSION-PROTOCOL.md:1754`]
- UNVERIFIED before a new wrapper: installed OMP help/runtime evidence for `--cwd=`, `--thinking`, and `--exclude-tools`, and the complete fresh/resume/title/model/thinking argv. The pinned tests prove equals-style extension/mode and native model/tools only. [OMP version pending host probe; source: `ff81565:internal/products/pifamily/rpc_lane_test.go:419-445`, `ff81565:internal/products/pifamily/rpc_lane_test.go:729-790`, `91fdcb3:docs/designs/UNIVERSAL-SESSION-PROTOCOL.md:1608-1609`, `91fdcb3:docs/designs/UNIVERSAL-SESSION-PROTOCOL.md:1754`]
- Historical pre-wrapper verification gap, superseded by the current steer carrier documented in the mandatory-wake source boundary above: the claimed interactive native `nextTurn` queue was unverified. The pinned extension uses ordinary idle `sendUserMessage` with no `deliverAs:"nextTurn"`; only active steer is source-proven. [OMP version pending host probe; source: `ff81565:integrations/pi/pifamily.mjs:157-174`, `91fdcb3:docs/designs/UNIVERSAL-SESSION-PROTOCOL.md:1612`, `91fdcb3:docs/designs/UNIVERSAL-SESSION-PROTOCOL.md:1754`]
- UNVERIFIED before a new wrapper: native MCP transport/configuration support. The verified ingress is the shared in-process Pi-family extension; the signed design retains that extension in peer/direct or lane/local mode. [OMP version pending host probe; source: `ff81565:integrations/pi/pifamily.mjs:84-115`, `ff81565:integrations/omp/agent-sessions.mjs:1-3`, `91fdcb3:docs/designs/UNIVERSAL-SESSION-PROTOCOL.md:1611`]
