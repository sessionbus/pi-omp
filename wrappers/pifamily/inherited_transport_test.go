// SPDX-License-Identifier: MIT

package pifamily

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const inheritedBridgeChild = `
import assert from "node:assert/strict";
import { once } from "node:events";
import { pathToFileURL } from "node:url";

const transport = await import(pathToFileURL(process.env.BRIDGE_MODULE).href);
const kind = process.env.BRIDGE_KIND;
const mode = process.env.BRIDGE_MODE;
const readFD = Number(process.env.BRIDGE_READ_FD);
const writeFD = Number(process.env.BRIDGE_WRITE_FD);
const openStream = () => kind === "pi"
  ? transport.openPiInheritedStream(readFD)
  : transport.openOMPInheritedStream(readFD, writeFD);
const connect = (options) => kind === "pi"
  ? transport.connectPiInheritedBridge(readFD, options)
  : transport.connectOMPInheritedBridge(readFD, writeFD, options);

if (mode === "framing") {
  const bridge = await connect({
    role: "native",
    handler: async ({ method, params }) => {
      assert.equal(method, "native.echo");
      return { value: params.value + ":native" };
    },
  });
  const result = await bridge.call("host.echo", { value: "child" });
  assert.deepEqual(result, { value: "child:host" });
  await bridge.call("host.done", {});
  await bridge.done;
} else if (mode === "peer-eof") {
  const stream = openStream();
  const ended = once(stream, "end");
  const closed = once(stream, "close");
  await new Promise((resolve, reject) => stream.write(Buffer.from("READY\n"), (error) => error ? reject(error) : resolve()));
  await ended;
  await closed;
  process.stdout.write("EOF_OK\n");
} else if (mode === "write-cancel") {
  const stream = openStream();
  const closed = once(stream, "close");
  let completed = 0;
  const chunk = Buffer.alloc(65536, 0x78);
  for (let index = 0; index < 512; index += 1) stream.write(chunk, () => { completed += 1; });
  await new Promise((resolve) => setTimeout(resolve, 30));
  if (completed === 512) throw new Error("inherited write did not remain blocked");
  stream.destroy();
  await closed;
  process.stdout.write("WRITE_CANCEL_OK\n");
} else if (mode === "read-cancel") {
  const stream = openStream();
  const closed = once(stream, "close");
  stream.resume?.();
  setTimeout(() => stream.destroy(), 30);
  await closed;
  process.stdout.write("READ_CANCEL_OK\n");
} else {
  throw new Error("unknown inherited bridge mode " + mode);
}
`

type inheritedRuntime struct {
	kind       string
	executable string
	attach     func(*exec.Cmd) (*InheritedBridgeTransport, error)
}

func inheritedRuntimes(t *testing.T) []inheritedRuntime {
	t.Helper()
	var result []inheritedRuntime
	for _, candidate := range []inheritedRuntime{
		{kind: "pi", executable: "node", attach: AttachPiInheritedBridge},
		{kind: "omp", executable: "bun", attach: AttachOMPInheritedBridge},
	} {
		path, err := exec.LookPath(candidate.executable)
		if err != nil {
			t.Logf("%s unavailable; skipping its inherited transport", candidate.executable)
			continue
		}
		candidate.executable = path
		result = append(result, candidate)
	}
	return result
}

func TestInheritedBridgeFramingAcrossNativeRuntimes(t *testing.T) {
	for _, runtime := range inheritedRuntimes(t) {
		t.Run(runtime.kind, func(t *testing.T) {
			command, transport, output := inheritedCommand(t, runtime, "framing")
			if err := command.Start(); err != nil {
				t.Fatal(err)
			}
			if err := transport.CloseChildCopies(); err != nil {
				t.Fatal(err)
			}
			defer transport.Close()

			doneCalled := make(chan struct{})
			host, err := NewBridge(transport.Parent, BridgeHost, func(_ context.Context, method string, params json.RawMessage) (json.RawMessage, error) {
				switch method {
				case "host.echo":
					var request struct {
						Value string `json:"value"`
					}
					if err := json.Unmarshal(params, &request); err != nil {
						return nil, err
					}
					return json.Marshal(map[string]string{"value": request.Value + ":host"})
				case "host.done":
					close(doneCalled)
					return json.RawMessage("null"), nil
				default:
					return nil, NewBridgeCallError("method_not_found", "unexpected method")
				}
			}, BridgeLimits{})
			if err != nil {
				t.Fatal(err)
			}
			if err := host.Ready(inheritedTestContext(t)); err != nil {
				t.Fatal(err)
			}
			var response struct {
				Value string `json:"value"`
			}
			if err := host.Call(inheritedTestContext(t), "native.echo", map[string]string{"value": "parent"}, &response); err != nil {
				t.Fatal(err)
			}
			if response.Value != "parent:native" {
				t.Fatalf("native response = %q", response.Value)
			}
			select {
			case <-doneCalled:
			case <-inheritedTestContext(t).Done():
				t.Fatal("native framing child did not finish its host call")
			}
			waitBridgeStats(t, host, BridgeStats{})
			if err := host.Close(); err != nil {
				t.Fatal(err)
			}
			if err := waitInheritedCommand(t, command, output); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestInheritedBridgeEOFAndCancellationAcrossNativeRuntimes(t *testing.T) {
	for _, runtime := range inheritedRuntimes(t) {
		for _, mode := range []string{"peer-eof", "write-cancel", "read-cancel"} {
			t.Run(runtime.kind+"/"+mode, func(t *testing.T) {
				command, transport, output := inheritedCommand(t, runtime, mode)
				if err := command.Start(); err != nil {
					t.Fatal(err)
				}
				if err := transport.CloseChildCopies(); err != nil {
					t.Fatal(err)
				}
				if mode == "peer-eof" {
					line, err := bufio.NewReader(transport.Parent).ReadString('\n')
					if err != nil || line != "READY\n" {
						t.Fatalf("ready = %q, %v", line, err)
					}
				}
				// Closing both parent directions supplies peer EOF, or releases
				// the child's blocked read. Write cancellation keeps the unread
				// parent endpoint open until the child cancels its blocked write.
				if mode != "write-cancel" {
					_ = transport.Parent.Close()
				}
				if err := waitInheritedCommand(t, command, output); err != nil {
					t.Fatal(err)
				}
				_ = transport.Close()
				want := strings.ToUpper(strings.ReplaceAll(mode, "-", "_")) + "_OK\n"
				if mode == "peer-eof" {
					want = "EOF_OK\n"
				}
				if output.String() != want {
					t.Fatalf("output = %q, want %q", output.String(), want)
				}
			})
		}
	}
}

func TestInheritedBridgeRejectsNilCommand(t *testing.T) {
	if _, err := AttachPiInheritedBridge(nil); err == nil {
		t.Fatal("Pi accepted nil command")
	}
	if _, err := AttachOMPInheritedBridge(nil); err == nil {
		t.Fatal("OMP accepted nil command")
	}
}

func inheritedCommand(t *testing.T, runtime inheritedRuntime, mode string) (*exec.Cmd, *InheritedBridgeTransport, *bytes.Buffer) {
	t.Helper()
	module, err := filepath.Abs("extension/inherited.mjs")
	if err != nil {
		t.Fatal(err)
	}
	script := filepath.Join(t.TempDir(), "child.mjs")
	if err := os.WriteFile(script, []byte(inheritedBridgeChild), 0o600); err != nil {
		t.Fatal(err)
	}
	command := exec.Command(runtime.executable, script)
	transport, err := runtime.attach(command)
	if err != nil {
		t.Fatal(err)
	}
	output := &bytes.Buffer{}
	command.Stdout = output
	command.Stderr = output
	command.Env = append(os.Environ(),
		"BRIDGE_MODULE="+module,
		"BRIDGE_KIND="+runtime.kind,
		"BRIDGE_MODE="+mode,
		fmt.Sprintf("BRIDGE_READ_FD=%d", transport.ChildReadFD()),
		fmt.Sprintf("BRIDGE_WRITE_FD=%d", transport.ChildWriteFD()),
	)
	return command, transport, output
}

func waitInheritedCommand(t *testing.T, command *exec.Cmd, output *bytes.Buffer) error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- command.Wait() }()
	select {
	case err := <-done:
		if err != nil {
			return fmt.Errorf("native transport child: %w\n%s", err, output.String())
		}
		return nil
	case <-inheritedTestContext(t).Done():
		if command.Process != nil {
			_ = command.Process.Kill()
		}
		<-done
		return fmt.Errorf("native transport child timed out\n%s", output.String())
	}
}

func inheritedTestContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}
