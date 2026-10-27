# Sessionbus OMP peer

`omp-peer` integrates Oh My Pi with Sessionbus. Lanes need Oh My Pi 18.11.0 or
newer: a lane Run ends at OMP's own `prompt_result` report for the lane's
prompt, which older versions do not send for agent work, so on them a lane Run
waits until it is cancelled. There is no runtime version check. Earlier builds
were tested with Oh My Pi 18.1.17 and Bun 1.4.0. Install Oh My Pi and make its
Bun runtime available on the login `PATH`, then run this archive's `install`
script. A lane started by the Sessionbus service uses the service's `PATH`. If
`bun` is not on it, the lane uses Bun's standard install location instead:
`$BUN_INSTALL/bin/bun` when `BUN_INSTALL` is set, otherwise `~/.bun/bin/bun`.

Interactive terminal launches retain native OMP arguments and add one managed,
per-process extension. Wrapper-owned `-g`/`--group` values select Sessionbus
groups and `-n`/`--peer-name` supplies an initial name; the live native session
ID, title, and working directory remain authoritative. Native maintenance,
help, version, print/export commands, and non-terminal stdin or stdout run
directly without managed ownership.

In a managed interactive terminal, a peer message is handed to OMP as a steer
when it arrives. If OMP is busy at that moment, the running task reads it at
its next boundary and the receipt is `injected`; if OMP is idle, it starts a
turn with it and the receipt is `written`. Both are decided when the message
is handed over, and OMP confirms neither. A native Task child's receipt stays
`queued_for_next_turn`. If OMP's events for a handed-over message no longer
match what was sent, or you clear the session (`/clear`) while it is pending,
only that message's attempt ends: its receipt stays, it is not sent again,
and the session stays connected for later messages.

Pressing Escape while OMP shows its working animation returns a steered peer
message that OMP had not read yet to your editor, as if you had typed it. It
is neither sent nor written to the session history unless you submit it, and
its receipt stays `injected`. Other stops of a task, such as Escape while no
animation shows, leave the message in OMP's queue, and OMP delivers it in a
new turn after the stop. This is an open defect, to be fixed with the planned
rework of the OMP integration.

Daemon-owned lanes are selected by the Sessionbus launch token and accept no
command-line arguments. The lane exposes the standard Sessionbus tool from the
real native OMP session through the same per-launch extension. Installation
does not register a global OMP extension, create a native session, or make a
model request.

A peer message that arrives while a lane's Run is active, before OMP's run for
it yields, is handed to OMP as a steer, which the running task reads at its
next boundary. Its receipt is `injected`: handed to OMP while it was busy with
that Run, not yet seen by the model. OMP places the message only after an
await of its own; if the task ended in between, the message is appended to
the session history without starting a turn, and the next Run reads it. A
message OMP queues after the task's last check for steering, or before an
interrupt, is answered by OMP's own continuation after the Run: it reaches the
model and its answer is in the native history, but not in the Run's result.
Without an active Run the lane is not running and the daemon starts a later
Run. If OMP fails the lane's prompt before starting work on it, for example
with no model or no API key, or ends it before any run, for example on a
usage-limit denial, the Run is `unavailable` with OMP's message when it gives
one, and the lane stays.

OMP reads the files that each `@path` in a prompt names, with no option to
turn that off. In a peer message that starts a lane Run, the lane therefore
puts an invisible word joiner (U+2060) right before each `@` that OMP would
read as the start of a file mention. The `@` then starts no mention, so OMP
reads no file for it, and the model sees the same visible text. The owner's own Run input is sent unchanged, and
OMP does not read file mentions in a steered message.

Every managed factory exposes the one `sessionbus` tool directly with OMP's
`loadMode: "essential"` and `{tier: "exec", policy: "allow"}` approval.
This covers the lane, interactive
primary and native Task-child factories without changing the approval mode or
any other tool's policy. The exec tier reflects that Sessionbus includes
messaging, delegation and lane controls. An ambient native per-tool denial
remains a native refusal; it is not bypassed or retried.

Explicit interactive `--yolo`, `--auto-approve` and `--approval-mode=yolo`
retain their native meaning and argument order alongside that tool grant.
For a lane, set `open.permission_mode` to `bypassPermissions` to select native
`--approval-mode=yolo`; raw approval flags remain owned by this typed field.
Omitting the field or choosing `default` preserves native configuration.
