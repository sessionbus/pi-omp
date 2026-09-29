# Sessionbus for Pi

This archive installs `pi-peer` and its fixed managed extension beside the
binary. It was tested with native `@earendil-works/pi-coding-agent` 0.85.1.
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
the three managed extension files through the installed Go command. It does
not add Pi extensions to user or project configuration. `pi-peer` supplies the
extension only for its own interactive and lane launches, so ordinary `pi`
invocations remain ordinary.

Interactive terminals retain Pi's native CLI, configuration, extensions,
permissions, sessions, and model selection. Wrapper `-g`/`--group` values add
Sessionbus groups. Wrapper `-n`/`--peer-name` selects the initial Peer name;
Pi's native `--name` option remains available for the native session title.
Native maintenance, help, version, print/export commands, and non-terminal
stdin or stdout run directly without managed ownership.

A managed interactive launch requires the exact `sessionbus` extension tool.
The wrapper therefore rejects an effective tool selection that removes it:
`--no-tools` without an overriding `--tools` list containing `sessionbus`, an
explicit `--tools` list that omits it, or `--exclude-tools` containing it.
Other tool selections and project-trust arguments remain native-owned and
unchanged. These checks do not apply to the direct native paths above.
For managed interactive launches, the wrapper alias `--yolo` selects Pi's
native `--approve`; an explicit native `--approve` remains unchanged.

Daemon-owned lanes are selected by the Sessionbus launch token and accept no
command-line arguments. The lane exposes the standard Sessionbus tool from the
real native Pi session through the per-launch extension. Installation alone
does not create a native session or make a model request.
