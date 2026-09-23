// SPDX-License-Identifier: MIT

package pifamily

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sessionbus/peer-common/testsocket"
)

func bridgeTestContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func bridgeUnixPair(t *testing.T) (net.Conn, net.Conn) {
	t.Helper()
	path := filepath.Join(testsocket.Directory(t), "bridge.sock")
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	accepted := make(chan net.Conn, 1)
	acceptErr := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			acceptErr <- err
			return
		}
		accepted <- conn
	}()
	client, err := net.Dial("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case server := <-accepted:
		return server, client
	case err := <-acceptErr:
		_ = client.Close()
		t.Fatal(err)
	case <-bridgeTestContext(t).Done():
		_ = client.Close()
		t.Fatal("accepting Pi-family bridge connection timed out")
	}
	return nil, nil
}

func newBridgePair(t *testing.T, hostHandler, nativeHandler BridgeHandler, limits BridgeLimits) (*Bridge, *Bridge) {
	t.Helper()
	hostConn, nativeConn := bridgeUnixPair(t)
	host, err := NewBridge(hostConn, BridgeHost, hostHandler, limits)
	if err != nil {
		t.Fatal(err)
	}
	native, err := NewBridge(nativeConn, BridgeNative, nativeHandler, limits)
	if err != nil {
		_ = host.Close()
		t.Fatal(err)
	}
	ctx := bridgeTestContext(t)
	if err := host.Ready(ctx); err != nil {
		t.Fatal(err)
	}
	if err := native.Ready(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		var wait sync.WaitGroup
		wait.Add(2)
		go func() { defer wait.Done(); _ = host.Close() }()
		go func() { defer wait.Done(); _ = native.Close() }()
		wait.Wait()
	})
	return host, native
}

func TestBridgeFullDuplexNestedCall(t *testing.T) {
	var host *Bridge
	hostHandler := func(ctx context.Context, method string, params json.RawMessage) (json.RawMessage, error) {
		if method != "native.outer" || string(params) != `{"value":"outer"}` {
			return nil, NewBridgeCallError("bad_request", "unexpected outer call")
		}
		var inner struct {
			Value string `json:"value"`
		}
		if err := host.Call(ctx, "host.inner", map[string]string{"value": "inner"}, &inner); err != nil {
			return nil, err
		}
		return json.Marshal(map[string]string{"value": "outer:" + inner.Value})
	}
	nativeHandler := func(_ context.Context, method string, params json.RawMessage) (json.RawMessage, error) {
		if method != "host.inner" || string(params) != `{"value":"inner"}` {
			return nil, NewBridgeCallError("bad_request", "unexpected inner call")
		}
		return json.RawMessage(`{"value":"native"}`), nil
	}
	host, native := newBridgePair(t, hostHandler, nativeHandler, BridgeLimits{})

	var result struct {
		Value string `json:"value"`
	}
	if err := native.Call(bridgeTestContext(t), "native.outer", map[string]string{"value": "outer"}, &result); err != nil {
		t.Fatal(err)
	}
	if result.Value != "outer:native" {
		t.Fatalf("result = %#v", result)
	}
	waitBridgeStats(t, host, BridgeStats{})
	waitBridgeStats(t, native, BridgeStats{})
}

// The first Done evaluation waits for the request write. The second is the
// correlated response wait, where cancellation can preserve the connection.
type bridgeResponseWaitContext struct {
	context.Context
	doneCalls    atomic.Int32
	responseWait chan struct{}
}

func (ctx *bridgeResponseWaitContext) Done() <-chan struct{} {
	if ctx.doneCalls.Add(1) == 2 {
		close(ctx.responseWait)
	}
	return ctx.Context.Done()
}

func TestBridgeCancellationDrainsResponseAndKeepsConnectionHealthy(t *testing.T) {
	started := make(chan struct{})
	finished := make(chan struct{})
	hostHandler := func(ctx context.Context, method string, _ json.RawMessage) (json.RawMessage, error) {
		switch method {
		case "hold":
			close(started)
			<-ctx.Done()
			close(finished)
			return nil, ctx.Err()
		case "ping":
			return json.RawMessage(`{"ok":true}`), nil
		default:
			return nil, NewBridgeCallError("method_not_found", "unknown method")
		}
	}
	_, native := newBridgePair(t, hostHandler, nil, BridgeLimits{})
	baseCtx, cancel := context.WithCancel(bridgeTestContext(t))
	defer cancel()
	callCtx := &bridgeResponseWaitContext{Context: baseCtx, responseWait: make(chan struct{})}
	callErr := make(chan error, 1)
	go func() { callErr <- native.Call(callCtx, "hold", map[string]any{}, nil) }()
	<-started
	select {
	case <-callCtx.responseWait:
	case <-baseCtx.Done():
		t.Fatal("bridge never reached the response wait")
	}
	cancel()
	if err := <-callErr; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled call error = %v", err)
	}
	<-finished
	var result struct {
		OK bool `json:"ok"`
	}
	if err := native.Call(bridgeTestContext(t), "ping", map[string]any{}, &result); err != nil {
		t.Fatal(err)
	}
	if !result.OK {
		t.Fatalf("ping result = %#v", result)
	}
	waitBridgeStats(t, native, BridgeStats{})
}

func TestBridgePendingCapacityRejectsBeforeWrite(t *testing.T) {
	started := make(chan struct{})
	hostHandler := func(ctx context.Context, method string, _ json.RawMessage) (json.RawMessage, error) {
		if method != "hold" {
			return json.RawMessage(`null`), nil
		}
		close(started)
		<-ctx.Done()
		return nil, ctx.Err()
	}
	limits := DefaultBridgeLimits
	limits.MaxPendingCalls = 1
	_, native := newBridgePair(t, hostHandler, nil, limits)
	firstCtx, cancel := context.WithCancel(bridgeTestContext(t))
	first := make(chan error, 1)
	go func() { first <- native.Call(firstCtx, "hold", map[string]any{}, nil) }()
	<-started
	if err := native.Call(bridgeTestContext(t), "second", map[string]any{}, nil); !errors.Is(err, ErrBridgeBusy) {
		t.Fatalf("second call error = %v", err)
	}
	cancel()
	if err := <-first; !errors.Is(err, context.Canceled) {
		t.Fatalf("first call error = %v", err)
	}
}

func TestBridgeRejectsDuplicateRequestAndUnknownResponse(t *testing.T) {
	t.Run("duplicate request", func(t *testing.T) {
		bridge, raw, reader := newRawBridge(t, func(_ context.Context, _ string, _ json.RawMessage) (json.RawMessage, error) {
			return json.RawMessage(`{"ok":true}`), nil
		})
		writeRawFrame(t, raw, `{"version":1,"type":"request","id":"n:1","method":"ping","params":{}}`)
		if line, err := reader.ReadString('\n'); err != nil || !strings.Contains(line, `"id":"n:1"`) {
			t.Fatalf("response = %q, %v", line, err)
		}
		writeRawFrame(t, raw, `{"version":1,"type":"request","id":"n:1","method":"ping","params":{}}`)
		waitBridgeDone(t, bridge)
		if !errors.Is(bridge.Err(), ErrBridgeProtocol) {
			t.Fatalf("bridge error = %v", bridge.Err())
		}
	})

	t.Run("unknown response", func(t *testing.T) {
		bridge, raw, _ := newRawBridge(t, nil)
		writeRawFrame(t, raw, `{"version":1,"type":"response","id":"h:1","result":null}`)
		waitBridgeDone(t, bridge)
		if !errors.Is(bridge.Err(), ErrBridgeProtocol) {
			t.Fatalf("bridge error = %v", bridge.Err())
		}
	})
}

func TestBridgeRejectsOversizedFrame(t *testing.T) {
	limits := DefaultBridgeLimits
	limits.MaxFrameBytes = 256
	bridge, raw, _ := newRawBridgeWithLimits(t, nil, limits)
	body := append([]byte(`{"version":1,"type":"request","id":"n:1","method":"ping","params":{"text":"`),
		append([]byte(strings.Repeat("x", 300)), []byte(`"}}`)...)...)
	body = append(body, '\n')
	_, _ = raw.Write(body)
	waitBridgeDone(t, bridge)
	if !errors.Is(bridge.Err(), ErrBridgeProtocol) {
		t.Fatalf("bridge error = %v", bridge.Err())
	}
}

func TestBridgeRejectedFrameDoesNotConsumeRequestID(t *testing.T) {
	limits := DefaultBridgeLimits
	limits.MaxFrameBytes = 256
	bridge, raw, reader := newRawBridgeWithLimits(t, nil, limits)
	if err := bridge.Call(bridgeTestContext(t), "oversized", map[string]string{"body": strings.Repeat("x", 300)}, nil); !errors.Is(err, ErrBridgeProtocol) {
		t.Fatalf("oversized call error = %v", err)
	}
	callResult := make(chan error, 1)
	go func() { callResult <- bridge.Call(bridgeTestContext(t), "ping", map[string]any{}, nil) }()
	line, err := reader.ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	var request struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal([]byte(line), &request); err != nil {
		t.Fatal(err)
	}
	if request.ID != "h:1" {
		t.Fatalf("request ID after rejected frame = %q", request.ID)
	}
	writeRawFrame(t, raw, `{"version":1,"type":"response","id":"h:1","result":null}`)
	if err := <-callResult; err != nil {
		t.Fatal(err)
	}
}

func TestBridgeHelloPrecedesResponseToEagerPeer(t *testing.T) {
	hostConn, raw := bridgeUnixPair(t)
	writeRawFrame(t, raw, `{"version":1,"type":"hello","role":"native"}`)
	writeRawFrame(t, raw, `{"version":1,"type":"request","id":"n:1","method":"ping","params":{}}`)
	bridge, err := NewBridge(hostConn, BridgeHost, func(_ context.Context, _ string, _ json.RawMessage) (json.RawMessage, error) {
		return json.RawMessage(`{"ok":true}`), nil
	}, BridgeLimits{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = raw.Close(); _ = bridge.Close() })
	reader := bufio.NewReader(raw)
	first, err := reader.ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(first, `"type":"hello"`) {
		t.Fatalf("first host frame = %s", first)
	}
	second, err := reader.ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(second, `"type":"response"`) || !strings.Contains(second, `"id":"n:1"`) {
		t.Fatalf("second host frame = %s", second)
	}
}

func TestBridgeRetainedResponseFailureStillWakesCaller(t *testing.T) {
	started := make(chan struct{})
	limits := DefaultBridgeLimits
	limits.MaxFrameBytes = 512
	limits.MaxRetainedBytes = 512
	bridge, raw, reader := newRawBridgeWithLimits(t, func(ctx context.Context, method string, _ json.RawMessage) (json.RawMessage, error) {
		if method != "hold" {
			return nil, NewBridgeCallError("bad_request", "unexpected method")
		}
		close(started)
		<-ctx.Done()
		return nil, ctx.Err()
	}, limits)
	callResult := make(chan error, 1)
	go func() { callResult <- bridge.Call(bridgeTestContext(t), "outbound", map[string]any{}, nil) }()
	if _, err := reader.ReadString('\n'); err != nil {
		t.Fatal(err)
	}
	writeRawFrame(t, raw, `{"version":1,"type":"request","id":"n:1","method":"hold","params":{}}`)
	<-started
	response, err := json.Marshal(map[string]any{
		"version": 1, "type": "response", "id": "h:1", "result": strings.Repeat("x", 430),
	})
	if err != nil {
		t.Fatal(err)
	}
	writeRawFrame(t, raw, string(response))
	waitBridgeDone(t, bridge)
	select {
	case err := <-callResult:
		if !errors.Is(err, ErrBridgeBusy) {
			t.Fatalf("call error = %v", err)
		}
	case <-bridgeTestContext(t).Done():
		t.Fatal("caller was not released after retained response failure")
	}
}

func TestBridgeConcurrentCallsStayInWireIDOrder(t *testing.T) {
	bridge, raw, reader := newRawBridge(t, nil)
	const calls = 48
	start := make(chan struct{})
	errorsByCall := make(chan error, calls)
	for index := 0; index < calls; index++ {
		index := index
		go func() {
			<-start
			errorsByCall <- bridge.Call(bridgeTestContext(t), "ordered", map[string]any{
				"index": index,
				"body":  strings.Repeat("x", (index%7)*20_000),
			}, nil)
		}()
	}
	close(start)
	for sequence := 1; sequence <= calls; sequence++ {
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		var request struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal([]byte(line), &request); err != nil {
			t.Fatal(err)
		}
		want := "h:" + strconv.Itoa(sequence)
		if request.ID != want {
			t.Fatalf("wire request %d ID = %q, want %q", sequence, request.ID, want)
		}
		writeRawFrame(t, raw, `{"version":1,"type":"response","id":"`+request.ID+`","result":null}`)
	}
	for range calls {
		if err := <-errorsByCall; err != nil {
			t.Fatal(err)
		}
	}
}

func TestBridgeInteroperatesWithNativeModule(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node is unavailable")
	}
	module, err := filepath.Abs("extension/bridge.mjs")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(testsocket.Directory(t), "interop.sock")
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	script := `
import { pathToFileURL } from "node:url";
const { connectBridge } = await import(pathToFileURL(process.argv[1]).href);
const bridge = await connectBridge(process.argv[2], {
  role: "native",
  handler: async ({ method, params }) => {
    if (method !== "native.inner") throw new Error("unexpected method");
    return { value: params.value + ":js" };
  },
});
const result = await bridge.call("host.outer", { value: "start" });
if (result.value !== "start:js:go") throw new Error(JSON.stringify(result));
await bridge.call("host.done", {});
await bridge.done;
if (bridge.stats().pendingCalls !== 0) throw new Error("pending calls remain");
`
	command := exec.Command(node, "--input-type=module", "-e", script, module, path)
	var output bytes.Buffer
	command.Stdout = &output
	command.Stderr = &output
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	processDone := make(chan error, 1)
	go func() { processDone <- command.Wait() }()
	t.Cleanup(func() {
		if command.Process != nil {
			_ = command.Process.Kill()
			<-processDone
		}
	})
	conn, err := listener.Accept()
	if err != nil {
		t.Fatal(err)
	}
	var host *Bridge
	hostAssigned := make(chan struct{})
	doneCalled := make(chan struct{})
	host, err = NewBridge(conn, BridgeHost, func(ctx context.Context, method string, params json.RawMessage) (json.RawMessage, error) {
		<-hostAssigned
		switch method {
		case "host.outer":
			var request struct {
				Value string `json:"value"`
			}
			if err := json.Unmarshal(params, &request); err != nil {
				return nil, err
			}
			var inner struct {
				Value string `json:"value"`
			}
			if err := host.Call(ctx, "native.inner", request, &inner); err != nil {
				return nil, err
			}
			return json.Marshal(map[string]string{"value": inner.Value + ":go"})
		case "host.done":
			close(doneCalled)
			return json.RawMessage(`null`), nil
		default:
			return nil, NewBridgeCallError("method_not_found", "unexpected method")
		}
	}, BridgeLimits{})
	if err != nil {
		t.Fatal(err)
	}
	close(hostAssigned)
	if err := host.Ready(bridgeTestContext(t)); err != nil {
		t.Fatal(err)
	}
	<-doneCalled
	waitBridgeStats(t, host, BridgeStats{})
	if err := host.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-processDone; err != nil {
		t.Fatalf("native module process: %v\n%s", err, output.String())
	}
	command.Process = nil
}

func TestBridgeCloseCancelsAndJoinsHandler(t *testing.T) {
	started := make(chan struct{})
	finished := make(chan struct{})
	hostHandler := func(ctx context.Context, _ string, _ json.RawMessage) (json.RawMessage, error) {
		close(started)
		<-ctx.Done()
		close(finished)
		return nil, ctx.Err()
	}
	host, native := newBridgePair(t, hostHandler, nil, BridgeLimits{})
	callResult := make(chan error, 1)
	go func() { callResult <- native.Call(bridgeTestContext(t), "hold", map[string]any{}, nil) }()
	<-started
	if err := host.Close(); err != nil {
		t.Fatal(err)
	}
	<-finished
	if err := <-callResult; err == nil {
		t.Fatal("call unexpectedly succeeded")
	}
	if got := host.Stats(); got != (BridgeStats{}) {
		t.Fatalf("host stats after close = %#v", got)
	}
}

func TestBridgeCancellationDuringPartialWriteRetiresAndJoins(t *testing.T) {
	hostConn, raw := bridgeUnixPair(t)
	if unix, ok := hostConn.(*net.UnixConn); ok {
		if err := unix.SetWriteBuffer(1024); err != nil {
			t.Fatal(err)
		}
	}
	bridge, err := NewBridge(hostConn, BridgeHost, nil, BridgeLimits{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = raw.Close(); _ = bridge.Close() })
	reader := bufio.NewReader(raw)
	if line, err := reader.ReadString('\n'); err != nil || !strings.Contains(line, `"type":"hello"`) {
		t.Fatalf("hello = %q, %v", line, err)
	}
	writeRawFrame(t, raw, `{"version":1,"type":"hello","role":"native"}`)
	if err := bridge.Ready(bridgeTestContext(t)); err != nil {
		t.Fatal(err)
	}

	callCtx, cancel := context.WithCancel(bridgeTestContext(t))
	callResult := make(chan error, 1)
	go func() {
		callResult <- bridge.Call(callCtx, "large", map[string]string{"body": strings.Repeat("x", 900_000)}, nil)
	}()
	started := make(chan struct{})
	go func() {
		buffer := make([]byte, 128)
		_, _ = reader.Read(buffer)
		close(started)
	}()
	<-started
	cancel()
	if err := <-callResult; !errors.Is(err, context.Canceled) {
		t.Fatalf("call error = %v", err)
	}
	waitBridgeDone(t, bridge)
	if got := bridge.Stats(); got != (BridgeStats{}) {
		t.Fatalf("stats after partial write cancellation = %#v", got)
	}
}

type bridgeHeldCompletionConn struct {
	net.Conn
	enabled atomic.Bool
	held    atomic.Bool
	wrote   chan struct{}
	release chan struct{}
}

func (conn *bridgeHeldCompletionConn) Write(body []byte) (int, error) {
	n, err := conn.Conn.Write(body)
	if conn.enabled.Load() && conn.held.CompareAndSwap(false, true) {
		close(conn.wrote)
		<-conn.release
	}
	return n, err
}

func TestBridgeAcceptedResponseWinsCancellationBeforeWriteCompletion(t *testing.T) {
	hostConn, raw := bridgeUnixPair(t)
	heldConn := &bridgeHeldCompletionConn{
		Conn: hostConn, wrote: make(chan struct{}), release: make(chan struct{}),
	}
	bridge, err := NewBridge(heldConn, BridgeHost, nil, BridgeLimits{})
	if err != nil {
		t.Fatal(err)
	}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(heldConn.release) }) }
	t.Cleanup(func() { release(); _ = raw.Close(); _ = bridge.Close() })
	reader := bufio.NewReader(raw)
	if _, err := reader.ReadString('\n'); err != nil {
		t.Fatal(err)
	}
	writeRawFrame(t, raw, `{"version":1,"type":"hello","role":"native"}`)
	if err := bridge.Ready(bridgeTestContext(t)); err != nil {
		t.Fatal(err)
	}
	heldConn.enabled.Store(true)

	callCtx, cancel := context.WithCancel(bridgeTestContext(t))
	type response struct {
		Value string `json:"value"`
	}
	result := make(chan struct {
		value response
		err   error
	}, 1)
	go func() {
		var value response
		err := bridge.Call(callCtx, "accepted", map[string]any{}, &value)
		result <- struct {
			value response
			err   error
		}{value, err}
	}()
	<-heldConn.wrote
	request, err := reader.ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(request, `"id":"h:1"`) {
		t.Fatalf("request = %s", request)
	}
	writeRawFrame(t, raw, `{"version":1,"type":"response","id":"h:1","result":{"value":"accepted"}}`)
	waitBridgeAcceptedResponse(t, bridge)
	cancel()
	got := <-result
	if got.err != nil || got.value.Value != "accepted" {
		t.Fatalf("call result = %#v, %v", got.value, got.err)
	}
	select {
	case <-bridge.Done():
		t.Fatalf("accepted response cancellation retired bridge: %v", bridge.Err())
	default:
	}
	release()
	waitBridgeStats(t, bridge, BridgeStats{})
}

func newRawBridge(t *testing.T, handler BridgeHandler) (*Bridge, net.Conn, *bufio.Reader) {
	t.Helper()
	return newRawBridgeWithLimits(t, handler, BridgeLimits{})
}

func newRawBridgeWithLimits(t *testing.T, handler BridgeHandler, limits BridgeLimits) (*Bridge, net.Conn, *bufio.Reader) {
	t.Helper()
	hostConn, raw := bridgeUnixPair(t)
	bridge, err := NewBridge(hostConn, BridgeHost, handler, limits)
	if err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(raw)
	if line, err := reader.ReadString('\n'); err != nil || !strings.Contains(line, `"type":"hello"`) {
		t.Fatalf("hello = %q, %v", line, err)
	}
	writeRawFrame(t, raw, `{"version":1,"type":"hello","role":"native"}`)
	if err := bridge.Ready(bridgeTestContext(t)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = raw.Close()
		_ = bridge.Close()
	})
	return bridge, raw, reader
}

func writeRawFrame(t *testing.T, conn net.Conn, frame string) {
	t.Helper()
	if _, err := conn.Write([]byte(frame + "\n")); err != nil {
		t.Fatal(err)
	}
}

func waitBridgeDone(t *testing.T, bridge *Bridge) {
	t.Helper()
	select {
	case <-bridge.Done():
	case <-bridgeTestContext(t).Done():
		t.Fatal("waiting for Pi-family bridge shutdown timed out")
	}
}

func waitBridgeStats(t *testing.T, bridge *Bridge, want BridgeStats) {
	t.Helper()
	ctx := bridgeTestContext(t)
	for bridge.Stats() != want {
		select {
		case <-ctx.Done():
			t.Fatalf("bridge stats = %#v, want %#v", bridge.Stats(), want)
		default:
			runtime.Gosched()
		}
	}
}

func waitBridgeAcceptedResponse(t *testing.T, bridge *Bridge) {
	t.Helper()
	ctx := bridgeTestContext(t)
	for {
		bridge.mu.Lock()
		accepted := len(bridge.pending) == 0 && bridge.retainedBytes > 0
		bridge.mu.Unlock()
		if accepted {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("bridge did not retain the accepted response")
		default:
			runtime.Gosched()
		}
	}
}
