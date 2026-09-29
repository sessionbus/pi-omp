# Sessionbus OMP peer

`omp-peer` integrates Oh My Pi with Sessionbus. It was tested with Oh My Pi
18.1.17 and Bun 1.4.0. Install Oh My Pi and make its Bun runtime available on
the login `PATH`, then run this archive's `install` script.

Interactive terminal launches retain native OMP arguments and add one managed,
per-process extension. Wrapper-owned `-g`/`--group` values select Sessionbus
groups and `-n`/`--peer-name` supplies an initial name; the live native session
ID, title, and working directory remain authoritative. Native maintenance,
help, version, print/export commands, and non-terminal stdin or stdout run
directly without managed ownership.

Daemon-owned lanes are selected by the Sessionbus launch token and accept no
command-line arguments. The lane exposes the standard Sessionbus tool from the
real native OMP session through the same per-launch extension. Installation
does not register a global OMP extension, create a native session, or make a
model request.

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
