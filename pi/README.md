# Sessionbus for Pi

This archive installs `pi-peer` and its fixed managed extension beside the
binary. The final code of release v0.5.13 (commit `5794951`; later commits
change only documentation) passed one interactive idle wake and one lane Run
with native `@earendil-works/pi-coding-agent` 1.5.1 and Node 25.3.0 on Linux.
The interactive check ended with an ordinary quit; quitting with peer messages
waiting, session switch and busy steering were not run on that code. Earlier
builds of this release had live checks with Pi 1.3.0 (the owner's matrix),
1.4.0 and 1.5.0.
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
it (`written`). Pi holds at most 256 such steered messages (8 MiB) that it has
not read yet; further messages wait in the FIFO for its next task. While Pi is busy without a task, for example compacting, the
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
returns to the editor. Quitting Pi, or switching to another session (new,
resume or fork), also writes such a message to the history of the session
being left before it ends. If a task (or a compaction) is still running then
while such a message or a peer message queued for Pi's next task waits, the
extension aborts it, as Escape does, and waits up to 2 seconds for Pi to
settle; if Pi has not settled by then, nothing is written and Pi quits. Peer messages still
waiting for Pi's next task are written once to the history of the session
being left, on quit or on a session switch, after those, in arrival order; no
new task starts then. A peer message that arrives while Pi quits or switches
is written to that history if Pi is idle and no message waits before it, and
is otherwise refused. Pi 1.5.1 aborts a running task before it quits or
switches; if another extension's handler for that settled task waits on real
I/O, a waiting peer message can start one more task first (a recorded limit).
A peer delivery accepted while another extension's settled handler runs can
be lost if an earlier deferred Pi action fails (a Pi defect, reported as
[earendil-works/pi#11955](https://github.com/earendil-works/pi/issues/11955)).

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
`injected`: handed to the running task, not yet seen by the model. If that
Run ends, for example interrupted, before Pi reads the message, Pi keeps it
queued and the next Run reads it; it is not lost. Pi holds at most 256 such
messages (8 MiB) that it has not read yet; a further one is refused with
`Pi delivery queue is full`. Without an active Run the lane is not running
and the daemon starts a later Run.

Interrupting a lane Run before Pi has started work on its prompt ends the
lane. Pi gives a prompt no identity, so the work it may still do for that
prompt could not be told apart from a later Run's.

A lane's Run ends when Pi settles its run. Pi can then run work of its own,
for example a run another extension starts; that work is not part of the
caller's Run, is not in its result and does not end the lane. A later Run
waits for such work to settle. If Pi still refuses the Run's prompt, that
Run is unavailable with Pi's answer and the lane stays open. When an input
extension forwards the Run's prompt and Pi refuses the original as busy, the
Run follows the forwarded run; if that forwarded prompt fails before its run
starts, the Run is unavailable with both answers, and the lane stays open.
When Pi answers that an extension handled the Run's prompt, the Run follows
the extension's forwarded run if its preflight came first; otherwise only
that Run is unavailable (`Pi native prompt: handled`), and the lane stays
open. When the Run is unavailable this way, or because the forwarded prompt
failed before its run started, the extension received the input: it is not
replayed, and a message that started the Run gets an uncertain receipt and
is not resent. The extension may still run that prompt later, as Pi's own
work outside any Run, so a caller that retries can repeat its effects. A Run
that follows a forwarded run reports its own receipts as usual. If an
extension handles a prompt and forwards it so late that its preflight comes
after the next Run recorded its input, the next Run can take it: Pi's
preflight carries no prompt identity. Dialogs that
Pi's own work raises in a lane are cancelled, as the lane's own are.
