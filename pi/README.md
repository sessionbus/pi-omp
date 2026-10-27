# Sessionbus for Pi

This archive installs `pi-peer` and its fixed managed extension beside the
binary. Release v0.5.13 was tested with native `@earendil-works/pi-coding-agent`
1.3.0; 1.4.0 was checked against its source.
Install that package with its `pi` executable available on the login `PATH`.

For a source build:

```sh
scripts/package-product pi ./dist
mkdir /tmp/sessionbus-pi
tar -xzf ./dist/pi-peer-$(go env GOOS)-$(go env GOARCH).tar.gz -C /tmp/sessionbus-pi
sh /tmp/sessionbus-pi/install
```

The installer writes the permanent package to
`~/.local/libexec/sessionbus/pi`, links `~/.local/bin/pi-peer`, and validates
the managed extension payload and bundled Sessionbus kit through the installed
Go command. It does
not add Pi extensions to user or project configuration. `pi-peer` supplies the
extension only for its own interactive and lane launches, so ordinary `pi`
invocations remain ordinary.

Interactive terminals retain Pi's native CLI, configuration, extensions,
permissions, sessions, and model selection. Wrapper `-g`/`--group` values add
Sessionbus groups. Wrapper `-n`/`--peer-name` selects the initial Peer name;
Pi's native `--name` option remains available for the native session title.
Native maintenance, help, version, print/export commands, and non-terminal
stdin or stdout run directly without managed ownership.

For a managed interactive terminal, `pi-peer` validates the native and fixed
payload and then execs Pi. The in-process extension owns the Sessionbus Peer,
tool caller, reconnects, session replacement, and bounded busy-delivery FIFO.
This adds no sidecar process, private socket, launch directory, or interactive
history lock. Managed lanes retain the Go Worker, native RPC pipes, history
lock, and direct native child; their extension bridge is an inherited anonymous
socketpair rather than a filesystem endpoint.

If another `pi-peer` launch takes over the same Pi session, the first Pi keeps
running without Sessionbus: it has no presence, receives no deliveries, drops
messages still waiting in its FIFO, and its Sessionbus tool answers
`Sessionbus: another pi-peer launch took over this session (superseded); relaunch to use Sessionbus here`.
Both windows can write to the session, as with two plain Pi windows. A daemon
refusal of the Peer's hello still ends Pi.

In a managed interactive terminal, a peer message that arrives while Pi is
running a task goes into Pi's own steering queue, which the task reads at its
next boundary; its receipt is `injected`. An idle session starts a turn with
it (`written`). While Pi is busy without a task, for example compacting, the
message waits in the extension's FIFO (`queued_for_next_turn`) and goes into
the next task, in order. Pressing Escape in Pi's terminal aborts the task and
clears Pi's queues (Pi 1.2.0 `interactive-mode.js:3891-3908`,
`agent-session.js:1914-1922`). Typed messages return to the editor. A steered
peer message the task had not read yet is written to that session's history
when the aborted task settles, with no new turn, so the model sees it in the
session's next task. Its receipt stays `injected`, and it is never delivered
twice. Other aborts of a task are handled the same way, for example a
mid-task `/compact`, or a session switch, which writes the message to the
session being left. Before it writes, the extension clears Pi's queues as
Escape does, so text you had queued returns to the editor. A steered message
the task had already read stays in history as it is.

A task that ends without an abort is handled the same way when it leaves a
steered peer message unread: when Pi restored its queue to the editor
(Alt+Up) or a flush of its compaction queue failed, and when Pi keeps the
message for a later task because the task could not continue. Before writing,
the extension clears Pi's queues as Escape does, so text you had queued
returns to the editor. Quitting Pi while it is idle also writes such a message
to history before the session ends, then the peer messages still waiting for
Pi's next task, in arrival order; no new task starts while Pi quits.

Not covered: quitting Pi while a task runs.

A managed interactive launch requires the exact `sessionbus` extension tool.
The wrapper therefore rejects an effective tool selection that removes it:
- `--no-tools` without an overriding `--tools` list that keeps `sessionbus`;
- an explicit `--tools` list of names and `*` patterns none of which matches
  it;
- a `--tools` list of Pi's `+name`/`-name` modifiers that removes it last, or
  that does not add `+sessionbus` after `--no-tools`;
- an `--exclude-tools` name or `*` pattern that matches it, such as
  `session*`.

Modifier lists that keep the tool, such as `--tools +read` or `-bash`, pass
through unchanged.
Other tool selections and project-trust arguments remain native-owned and
unchanged. These checks do not apply to the direct native paths above.
Pi 1.3.0 restores a resumed session's recorded tool set, which can leave
`sessionbus` out. The extension adds it back at every session start, after
the other tools, and changes nothing when it is already active.
For managed interactive launches, the wrapper alias `--yolo` selects Pi's
native `--approve`; an explicit native `--approve` remains unchanged.

Daemon-owned lanes are selected by the Sessionbus launch token and accept no
command-line arguments. The lane exposes the standard Sessionbus tool from the
real native Pi session through the per-launch extension. Installation alone
does not create a native session or make a model request.

A peer message that arrives while a lane's Run is active goes into Pi's own
steering queue, which the running task reads at its next boundary (after the
current tool calls, before its next model request). Its receipt is
`injected`: handed to the running task, not yet seen by the model. Without
an active Run the lane is not running and the daemon starts a later Run.

A lane's Run ends when Pi settles its run. Pi can then run work of its own,
for example a run another extension starts; that work is not part of the
caller's Run, is not in its result and does not end the lane. A later Run
waits for such work to settle. If Pi still refuses the Run's prompt, that
Run is unavailable with Pi's answer and the lane stays open. Dialogs that
Pi's own work raises in a lane are cancelled, as the lane's own are.
