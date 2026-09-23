// SPDX-License-Identifier: MIT

package omp

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	kit "github.com/antst/sessionbus/bus/sdk/go"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
	"github.com/sessionbus/peer-common/testsocket"
	"github.com/sessionbus/pi-omp/wrappers/pifamily"
)

type ownerNativeFixture struct {
	mu           sync.Mutex
	descriptions map[string]ownerDescribeResult
	describeHook func(ownerDescribeRequest)
	stages       []ownerStageRequest
	shutdowns    []ownerDescribeRequest
	stageHook    func(ownerStageRequest)
	stageResult  func(ownerStageRequest) ownerStageResult
	stageRaw     func(ownerStageRequest) json.RawMessage
}

type ownerHeldWriteConn struct {
	net.Conn
	entered chan struct{}
	once    sync.Once
}

func (conn *ownerHeldWriteConn) Write(body []byte) (int, error) {
	conn.once.Do(func() { close(conn.entered) })
	return conn.Conn.Write(body)
}

func (fixture *ownerNativeFixture) handle(_ context.Context, method string, raw json.RawMessage) (json.RawMessage, error) {
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	switch method {
	case "native.describe":
		var request ownerDescribeRequest
		if decodeOwnerJSON(raw, &request) != nil {
			return nil, pifamily.NewBridgeCallError("bad_request", "invalid native description")
		}
		if fixture.describeHook != nil {
			fixture.describeHook(request)
		}
		result, ok := fixture.descriptions[request.OwnerToken]
		if !ok || result.SessionID != request.SessionID {
			return nil, pifamily.NewBridgeCallError("bad_request", "unknown native description")
		}
		return json.Marshal(result)
	case "native.stage":
		var request ownerStageRequest
		if decodeOwnerJSON(raw, &request) != nil {
			return nil, pifamily.NewBridgeCallError("bad_request", "invalid native stage")
		}
		fixture.stages = append(fixture.stages, request)
		if fixture.stageHook != nil {
			fixture.stageHook(request)
		}
		if fixture.stageRaw != nil {
			return fixture.stageRaw(request), nil
		}
		if fixture.stageResult != nil {
			return json.Marshal(fixture.stageResult(request))
		}
		queued := true
		return json.Marshal(ownerStageResult{
			OwnerToken: request.OwnerToken, SessionID: request.SessionID,
			MessageID: request.MessageID, Queued: &queued,
		})
	case "native.shutdown":
		var request ownerDescribeRequest
		if decodeOwnerJSON(raw, &request) != nil {
			return nil, pifamily.NewBridgeCallError("bad_request", "invalid native shutdown")
		}
		fixture.shutdowns = append(fixture.shutdowns, request)
		return json.Marshal(ownerShutdownResult{OwnerToken: request.OwnerToken, SessionID: request.SessionID, Requested: true})
	default:
		return nil, pifamily.NewBridgeCallError("method_not_found", "unexpected native method")
	}
}

func ownerTestContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func ownerBusListener(t *testing.T) net.Listener {
	t.Helper()
	listener, err := net.Listen("unix", filepath.Join(testsocket.Directory(t), "bus.sock"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	return listener
}

func ownerAccept(t *testing.T, listener net.Listener) (net.Conn, *bufio.Scanner) {
	t.Helper()
	if unix, ok := listener.(*net.UnixListener); ok {
		_ = unix.SetDeadline(time.Now().Add(5 * time.Second))
	}
	conn, err := listener.Accept()
	if err != nil {
		t.Fatal(err)
	}
	if unix, ok := listener.(*net.UnixListener); ok {
		_ = unix.SetDeadline(time.Time{})
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn, bufio.NewScanner(conn)
}

func ownerFrame(t *testing.T, scanner *bufio.Scanner) protocol.Frame {
	t.Helper()
	if !scanner.Scan() {
		t.Fatalf("missing public frame: %v", scanner.Err())
	}
	frame, err := protocol.DecodeFrame(scanner.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	return frame
}

func ownerWrite(t *testing.T, conn net.Conn, body []byte) {
	t.Helper()
	if _, err := conn.Write(body); err != nil {
		t.Fatal(err)
	}
}

func ownerHello(t *testing.T, conn net.Conn, scanner *bufio.Scanner) kit.PeerIdentity {
	t.Helper()
	frame := ownerFrame(t, scanner)
	if !frame.Request || frame.Method != "session.hello" {
		t.Fatalf("public hello = %+v", frame)
	}
	params, err := protocol.DecodeParams(frame.Method, frame.Params)
	if err != nil {
		t.Fatal(err)
	}
	hello, ok := params.(*protocol.PeerHello)
	if !ok {
		t.Fatalf("public hello params = %T", params)
	}
	body, err := protocol.ResultBytes(frame.ID, frame.Method, struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	ownerWrite(t, conn, body)
	return *hello
}

func ownerRegistryPair(t *testing.T, options OwnerRegistryOptions, fixture *ownerNativeFixture) (*OwnerRegistry, *pifamily.Bridge) {
	t.Helper()
	registry, err := NewOwnerRegistry(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	left, right := net.Pipe()
	hostBridge, err := pifamily.NewBridge(left, pifamily.BridgeHost, registry.HandleBridge, pifamily.BridgeLimits{})
	if err != nil {
		t.Fatal(err)
	}
	if err = registry.AssignBridge(hostBridge); err != nil {
		t.Fatal(err)
	}
	nativeBridge, err := pifamily.NewBridge(right, pifamily.BridgeNative, fixture.handle, pifamily.BridgeLimits{})
	if err != nil {
		t.Fatal(err)
	}
	if err = hostBridge.Ready(ownerTestContext(t)); err != nil {
		t.Fatal(err)
	}
	if err = nativeBridge.Ready(ownerTestContext(t)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = registry.Close()
		_ = nativeBridge.Close()
	})
	return registry, nativeBridge
}

func ownerReady(t *testing.T, registry *OwnerRegistry, native *pifamily.Bridge, listener net.Listener, request ownerReadyRequest) (kit.PeerIdentity, net.Conn, *bufio.Scanner) {
	t.Helper()
	result := make(chan error, 1)
	go func() { result <- native.Call(ownerTestContext(t), "owner.ready", request, nil) }()
	conn, scanner := ownerAccept(t, listener)
	if !scanner.Scan() {
		t.Fatalf("missing public hello: %v; owner.ready: %v; registry: %v", scanner.Err(), <-result, registry.Err())
	}
	frame, err := protocol.DecodeFrame(scanner.Bytes())
	if err != nil || !frame.Request || frame.Method != "session.hello" {
		t.Fatalf("public hello frame = %+v, %v", frame, err)
	}
	params, err := protocol.DecodeParams(frame.Method, frame.Params)
	if err != nil {
		t.Fatal(err)
	}
	hello, ok := params.(*protocol.PeerHello)
	if !ok {
		t.Fatalf("public hello params = %T", params)
	}
	body, err := protocol.ResultBytes(frame.ID, frame.Method, struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	ownerWrite(t, conn, body)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	ownerWaitPublished(t, registry, request.OwnerToken)
	return *hello, conn, scanner
}

func ownerWaitPublished(t *testing.T, registry *OwnerRegistry, token string) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		registry.mu.Lock()
		state := registry.bindings[token]
		published := state != nil && state.published
		registry.mu.Unlock()
		if published {
			break
		}
		select {
		case <-deadline:
			t.Fatal("public hello acknowledgement was not admitted")
		default:
			time.Sleep(time.Millisecond)
		}
	}
}

func TestOwnerRegistryRejectsInvalidConstruction(t *testing.T) {
	directory := t.TempDir()
	caller := kit.NewCaller(func(context.Context, string, any) (json.RawMessage, error) { return nil, nil })
	for name, options := range map[string]OwnerRegistryOptions{
		"nil context":         {Topology: ownerTopologyLane, Socket: filepath.Join(directory, "bus"), Directory: directory, PrimaryCaller: caller},
		"wrong topology":      {Topology: "print", Socket: filepath.Join(directory, "bus"), Directory: directory},
		"relative socket":     {Topology: ownerTopologyInteractive, Socket: "bus", Directory: directory},
		"lane without caller": {Topology: ownerTopologyLane, Socket: filepath.Join(directory, "bus"), Directory: directory},
		"interactive caller":  {Topology: ownerTopologyInteractive, Socket: filepath.Join(directory, "bus"), Directory: directory, PrimaryCaller: caller},
	} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			if name == "nil context" {
				ctx = nil
			}
			if registry, err := NewOwnerRegistry(ctx, options); err == nil || registry != nil {
				t.Fatalf("invalid registry = %#v, %v", registry, err)
			}
		})
	}
}

func TestOwnerRegistryWaitsForBridgeAssignmentBeforeDispatch(t *testing.T) {
	listener := ownerBusListener(t)
	directory := t.TempDir()
	registry, err := NewOwnerRegistry(context.Background(), OwnerRegistryOptions{
		Topology: ownerTopologyInteractive, Socket: listener.Addr().String(), Directory: directory,
		InitialName: "requested", Groups: []string{"shared"},
	})
	if err != nil {
		t.Fatal(err)
	}
	hostConn, nativeConn := net.Pipe()
	entered := make(chan struct{})
	handled := make(chan error, 1)
	var enterOnce sync.Once
	hostBridge, err := pifamily.NewBridge(hostConn, pifamily.BridgeHost, func(ctx context.Context, method string, raw json.RawMessage) (json.RawMessage, error) {
		enterOnce.Do(func() { close(entered) })
		body, err := registry.HandleBridge(ctx, method, raw)
		handled <- err
		return body, err
	}, pifamily.BridgeLimits{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = registry.Close()
		_ = hostBridge.Close()
		_ = nativeConn.Close()
	})
	if err := nativeConn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(nativeConn)
	hello, err := reader.ReadString('\n')
	if err != nil || !strings.Contains(hello, `"type":"hello"`) || !strings.Contains(hello, `"role":"host"`) {
		t.Fatalf("host hello = %q, %v", hello, err)
	}
	readyParams, err := json.Marshal(ownerReadyRequest{
		Topology: ownerTopologyInteractive, Directory: directory, Scope: ownerScopePrimary,
		Mode: ownerModeTUI, OwnerToken: "owner-token", SessionID: "owner-session", Name: "event-name",
	})
	if err != nil {
		t.Fatal(err)
	}
	eagerFrames := fmt.Sprintf("{\"version\":1,\"type\":\"hello\",\"role\":\"native\"}\n{\"version\":1,\"type\":\"request\",\"id\":\"n:1\",\"method\":\"owner.ready\",\"params\":%s}\n", readyParams)
	if _, err = nativeConn.Write([]byte(eagerFrames)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-ownerTestContext(t).Done():
		t.Fatal("owner.ready was not dispatched before assignment")
	}
	select {
	case err := <-handled:
		t.Fatalf("owner.ready crossed the assignment gate: %v", err)
	default:
	}
	if err = registry.AssignBridge(hostBridge); err != nil {
		t.Fatal(err)
	}
	describeLine, err := reader.ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	var describeFrame struct {
		ID     string               `json:"id"`
		Method string               `json:"method"`
		Params ownerDescribeRequest `json:"params"`
	}
	if err = json.Unmarshal([]byte(describeLine), &describeFrame); err != nil {
		t.Fatal(err)
	}
	if describeFrame.ID != "h:1" || describeFrame.Method != "native.describe" ||
		describeFrame.Params.OwnerToken != "owner-token" || describeFrame.Params.SessionID != "owner-session" {
		t.Fatalf("native describe = %+v", describeFrame)
	}
	description, err := json.Marshal(ownerDescribeResult{
		OwnerToken: "owner-token", SessionID: "owner-session", Name: "native-name", CWD: "/work/native",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = fmt.Fprintf(nativeConn, "{\"version\":1,\"type\":\"response\",\"id\":\"h:1\",\"result\":%s}\n", description); err != nil {
		t.Fatal(err)
	}
	publicConn, publicScanner := ownerAccept(t, listener)
	identity := ownerHello(t, publicConn, publicScanner)
	if identity.SessionID != "owner-session" || identity.Name != "native-name" || identity.Info["cwd"] != "/work/native" {
		t.Fatalf("published identity = %+v", identity)
	}
	readyLine, err := reader.ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	var readyFrame struct {
		ID     string `json:"id"`
		Result struct {
			OwnerToken string `json:"owner_token"`
			SessionID  string `json:"session_id"`
		} `json:"result"`
	}
	if err = json.Unmarshal([]byte(readyLine), &readyFrame); err != nil {
		t.Fatal(err)
	}
	if readyFrame.ID != "n:1" || readyFrame.Result.OwnerToken != "owner-token" || readyFrame.Result.SessionID != "owner-session" {
		t.Fatalf("owner.ready response = %+v", readyFrame)
	}
	if err := <-handled; err != nil {
		t.Fatal(err)
	}
	ownerWaitPublished(t, registry, "owner-token")
}

func TestOwnerRegistryBridgeAssignmentWaitCancelsAndJoins(t *testing.T) {
	directory := t.TempDir()
	registry, err := NewOwnerRegistry(context.Background(), OwnerRegistryOptions{
		Topology: ownerTopologyInteractive, Socket: filepath.Join(directory, "bus.sock"), Directory: directory,
	})
	if err != nil {
		t.Fatal(err)
	}
	hostConn, nativeConn := net.Pipe()
	entered := make(chan struct{})
	handled := make(chan error, 1)
	hostBridge, err := pifamily.NewBridge(hostConn, pifamily.BridgeHost, func(ctx context.Context, method string, raw json.RawMessage) (json.RawMessage, error) {
		close(entered)
		body, err := registry.HandleBridge(ctx, method, raw)
		handled <- err
		return body, err
	}, pifamily.BridgeLimits{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = hostBridge.Close()
		_ = registry.Close()
		_ = nativeConn.Close()
	})
	if err := nativeConn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(nativeConn)
	if _, err := reader.ReadString('\n'); err != nil {
		t.Fatal(err)
	}
	if _, err := nativeConn.Write([]byte("{\"version\":1,\"type\":\"hello\",\"role\":\"native\"}\n{\"version\":1,\"type\":\"request\",\"id\":\"n:1\",\"method\":\"owner.ready\",\"params\":{}}\n")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-ownerTestContext(t).Done():
		t.Fatal("owner.ready did not reach the assignment gate")
	}
	closeResult := make(chan error, 1)
	go func() { closeResult <- hostBridge.Close() }()
	select {
	case err := <-handled:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled assignment wait = %v", err)
		}
	case <-ownerTestContext(t).Done():
		t.Fatal("bridge handler did not leave the assignment gate")
	}
	if err := <-closeResult; err != nil && !errors.Is(err, pifamily.ErrBridgeClosed) {
		t.Fatal(err)
	}
	if err := registry.Close(); err != nil {
		t.Fatal(err)
	}
	_ = nativeConn.Close()
}

func TestOwnerRegistryKeepsLanePrimaryAndChildCallersDistinct(t *testing.T) {
	listener := ownerBusListener(t)
	directory := t.TempDir()
	type publicCall struct {
		method string
		params any
	}
	primaryCalls := make(chan publicCall, 1)
	primaryCaller := kit.NewCaller(func(_ context.Context, method string, params any) (json.RawMessage, error) {
		primaryCalls <- publicCall{method, params}
		return json.RawMessage(`{"sessions":[]}`), nil
	})
	fixture := &ownerNativeFixture{descriptions: map[string]ownerDescribeResult{
		"main-token":  {OwnerToken: "main-token", SessionID: "main-session", Name: "", CWD: "/work/main"},
		"child-token": {OwnerToken: "child-token", SessionID: "child-session", Name: "child", CWD: "/work/child"},
	}}
	registry, native := ownerRegistryPair(t, OwnerRegistryOptions{
		Topology: ownerTopologyLane, Socket: listener.Addr().String(), Directory: directory,
		InitialName: "requested", Groups: []string{"shared"}, PrimaryCaller: primaryCaller,
	}, fixture)

	var readyResult struct {
		OwnerToken string `json:"owner_token"`
		SessionID  string `json:"session_id"`
	}
	if err := native.Call(ownerTestContext(t), "owner.ready", ownerReadyRequest{
		Topology: ownerTopologyLane, Directory: directory, Scope: ownerScopePrimary,
		Mode: ownerModeRPC, OwnerToken: "main-token", SessionID: "main-session",
	}, &readyResult); err != nil {
		t.Fatal(err)
	}
	if readyResult.OwnerToken != "main-token" || readyResult.SessionID != "main-session" {
		t.Fatalf("primary ready = %+v", readyResult)
	}
	select {
	case <-registry.Ready():
	case <-ownerTestContext(t).Done():
		t.Fatal("lane primary did not become ready")
	}
	primary, ok := registry.Primary()
	if !ok || primary.Name != "" || primary.CWD != "/work/main" || primary.Scope != ownerScopePrimary {
		t.Fatalf("primary binding = %+v, %v", primary, ok)
	}

	var toolResult struct {
		OwnerToken string          `json:"owner_token"`
		SessionID  string          `json:"session_id"`
		CallID     string          `json:"call_id"`
		Result     json.RawMessage `json:"result"`
	}
	if err := native.Call(ownerTestContext(t), "tool.call", ownerToolRequest{
		OwnerToken: "main-token", SessionID: "main-session", CallID: "main-call",
		Action: "list", Arguments: json.RawMessage(`{}`),
	}, &toolResult); err != nil {
		t.Fatal(err)
	}
	if toolResult.OwnerToken != "main-token" || toolResult.CallID != "main-call" || string(toolResult.Result) != `{"sessions":[]}` {
		t.Fatalf("primary tool result = %+v", toolResult)
	}
	if call := <-primaryCalls; call.method != "session.list" {
		t.Fatalf("primary public call = %+v", call)
	}

	childReady := make(chan error, 1)
	go func() {
		childReady <- native.Call(ownerTestContext(t), "owner.ready", ownerReadyRequest{
			Topology: ownerTopologyLane, Directory: directory, Scope: ownerScopeChild,
			Mode: ownerModePrint, OwnerToken: "child-token", SessionID: "child-session", Name: "event-name",
		}, nil)
	}()
	childConn, childScanner := ownerAccept(t, listener)
	hello := ownerHello(t, childConn, childScanner)
	if err := <-childReady; err != nil {
		t.Fatal(err)
	}
	ownerWaitPublished(t, registry, "child-token")
	if hello.Product != Product || hello.SessionID != "child-session" || hello.Name != "child" ||
		!slices.Equal(hello.Groups, []string{"shared"}) || hello.Info["cwd"] != "/work/child" {
		t.Fatalf("child identity = %+v", hello)
	}

	childTool := make(chan error, 1)
	go func() {
		childTool <- native.Call(ownerTestContext(t), "tool.call", ownerToolRequest{
			OwnerToken: "child-token", SessionID: "child-session", CallID: "child-call",
			Action: "list", Arguments: json.RawMessage(`{}`),
		}, nil)
	}()
	if err := childConn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	frame := ownerFrame(t, childScanner)
	if err := childConn.SetReadDeadline(time.Time{}); err != nil {
		t.Fatal(err)
	}
	if !frame.Request || frame.Method != "session.list" {
		t.Fatalf("child public tool frame = %+v", frame)
	}
	body, err := protocol.ResultBytes(frame.ID, frame.Method, kit.SessionListResult{Sessions: []kit.SessionSummary{}})
	if err != nil {
		t.Fatal(err)
	}
	ownerWrite(t, childConn, body)
	if err = <-childTool; err != nil {
		t.Fatal(err)
	}
	select {
	case extra := <-primaryCalls:
		t.Fatalf("child borrowed primary caller: %+v", extra)
	default:
	}

	if err = native.Call(ownerTestContext(t), "session_end", ownerEndRequest{
		Topology: ownerTopologyLane, Scope: ownerScopeChild, Mode: ownerModePrint,
		OwnerToken: "child-token", SessionID: "child-session", Reason: "task-complete",
	}, nil); err != nil {
		t.Fatal(err)
	}
	if childScanner.Scan() || childScanner.Err() != nil {
		t.Fatalf("child connection survived end: %v", childScanner.Err())
	}
	if got, ok := registry.Primary(); !ok || got.OwnerToken != "main-token" {
		t.Fatalf("child end changed primary = %+v, %v", got, ok)
	}

	if err = native.Call(ownerTestContext(t), "session_end", ownerEndRequest{
		Topology: ownerTopologyLane, Scope: ownerScopePrimary, Mode: ownerModeRPC,
		OwnerToken: "main-token", SessionID: "main-session", Reason: "quit",
	}, nil); err != nil {
		t.Fatal(err)
	}
	if reason, ok := registry.GracefulEnd(); !ok || reason != "quit" {
		t.Fatalf("graceful primary end = %q, %v", reason, ok)
	}
}

func TestOwnerRegistryAcceptsCleanBridgeEOFOnlyAfterAcknowledgedEnd(t *testing.T) {
	for _, test := range []struct {
		name     string
		priorErr error
	}{
		{name: "clean"},
		{name: "preserves earlier failure", priorErr: errors.New("earlier registry failure")},
	} {
		t.Run(test.name, func(t *testing.T) {
			directory := t.TempDir()
			caller := kit.NewCaller(func(context.Context, string, any) (json.RawMessage, error) {
				return json.RawMessage(`{}`), nil
			})
			fixture := &ownerNativeFixture{descriptions: map[string]ownerDescribeResult{
				"main-token": {OwnerToken: "main-token", SessionID: "main-session", CWD: "/work/main"},
			}}
			registry, native := ownerRegistryPair(t, OwnerRegistryOptions{
				Topology: ownerTopologyLane, Socket: filepath.Join(directory, "bus.sock"), Directory: directory,
				PrimaryCaller: caller,
			}, fixture)
			if err := native.Call(ownerTestContext(t), "owner.ready", ownerReadyRequest{
				Topology: ownerTopologyLane, Directory: directory, Scope: ownerScopePrimary, Mode: ownerModeRPC,
				OwnerToken: "main-token", SessionID: "main-session",
			}, nil); err != nil {
				t.Fatal(err)
			}
			if test.priorErr != nil {
				registry.recordError(test.priorErr)
			}
			if err := native.Call(ownerTestContext(t), "session_end", ownerEndRequest{
				Topology: ownerTopologyLane, Scope: ownerScopePrimary, Mode: ownerModeRPC,
				OwnerToken: "main-token", SessionID: "main-session", Reason: "quit",
			}, nil); err != nil {
				t.Fatal(err)
			}
			if err := native.Close(); err != nil {
				t.Fatal(err)
			}
			select {
			case <-registry.Done():
			case <-ownerTestContext(t).Done():
				t.Fatal("clean bridge EOF did not settle registry")
			}
			err := registry.Close()
			if test.priorErr == nil && err != nil {
				t.Fatalf("clean acknowledged end = %v", err)
			}
			if test.priorErr != nil && !errors.Is(err, test.priorErr) {
				t.Fatalf("earlier registry failure was lost: %v", err)
			}
		})
	}
}

func TestOwnerRegistryRejectsBridgeEOFBeforeAcknowledgedEnd(t *testing.T) {
	directory := t.TempDir()
	caller := kit.NewCaller(func(context.Context, string, any) (json.RawMessage, error) {
		return json.RawMessage(`{}`), nil
	})
	fixture := &ownerNativeFixture{descriptions: map[string]ownerDescribeResult{
		"main-token": {OwnerToken: "main-token", SessionID: "main-session", CWD: "/work/main"},
	}}
	registry, native := ownerRegistryPair(t, OwnerRegistryOptions{
		Topology: ownerTopologyLane, Socket: filepath.Join(directory, "bus.sock"), Directory: directory,
		PrimaryCaller: caller,
	}, fixture)
	if err := native.Call(ownerTestContext(t), "owner.ready", ownerReadyRequest{
		Topology: ownerTopologyLane, Directory: directory, Scope: ownerScopePrimary, Mode: ownerModeRPC,
		OwnerToken: "main-token", SessionID: "main-session",
	}, nil); err != nil {
		t.Fatal(err)
	}
	if err := native.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-registry.Done():
	case <-ownerTestContext(t).Done():
		t.Fatal("unacknowledged bridge EOF did not retire registry")
	}
	if err := registry.Close(); err == nil || !strings.Contains(err.Error(), "OMP private bridge ended") {
		t.Fatalf("unacknowledged bridge EOF = %v", err)
	}
}

func TestOwnerRegistryStagesInteractiveDeliveryAndConfirmsExactBatch(t *testing.T) {
	listener := ownerBusListener(t)
	directory := t.TempDir()
	fixture := &ownerNativeFixture{descriptions: map[string]ownerDescribeResult{
		"owner-one": {OwnerToken: "owner-one", SessionID: "session-one", Name: "native", CWD: "/work/one"},
	}}
	registry, native := ownerRegistryPair(t, OwnerRegistryOptions{
		Topology: ownerTopologyInteractive, Socket: listener.Addr().String(), Directory: directory,
		InitialName: "requested", Groups: []string{"interactive"},
	}, fixture)
	hello, public, scanner := ownerReady(t, registry, native, listener, ownerReadyRequest{
		Topology: ownerTopologyInteractive, Directory: directory, Scope: ownerScopePrimary,
		Mode: ownerModeTUI, OwnerToken: "owner-one", SessionID: "session-one", Name: "event",
	})
	if hello.Product != Product || hello.SessionID != "session-one" || hello.Name != "native" || hello.Info["cwd"] != "/work/one" {
		t.Fatalf("interactive identity = %+v", hello)
	}

	delivery := kit.DeliveryRequest{
		MessageID: " message opaque ",
		From:      kit.DeliverySource{SessionID: "sender", Product: "codex-peer", Groups: []string{"interactive"}},
		Body:      "hello",
	}
	body, err := protocol.RequestBytes(40, "message.deliver", delivery)
	if err != nil {
		t.Fatal(err)
	}
	ownerWrite(t, public, body)
	frame := ownerFrame(t, scanner)
	if frame.Request || frame.ID != 40 || frame.Error != nil {
		t.Fatalf("delivery receipt frame = %+v", frame)
	}
	var receipt kit.DeliveryReceipt
	if err = protocol.UnmarshalResult("message.deliver", frame.Result, &receipt); err != nil || receipt.Disposition != "queued_for_next_turn" {
		t.Fatalf("delivery receipt = %+v, %v", receipt, err)
	}
	fixture.mu.Lock()
	stages := slices.Clone(fixture.stages)
	fixture.mu.Unlock()
	if len(stages) != 1 || stages[0].OwnerToken != "owner-one" || stages[0].SessionID != "session-one" ||
		stages[0].MessageID != delivery.MessageID || !strings.Contains(stages[0].Body, "hello") {
		t.Fatalf("native stages = %+v", stages)
	}

	for index, phase := range []string{"claimed", "message_start", "message_end", "context"} {
		var observed ownerDeliveryObserveRequest
		err = native.Call(ownerTestContext(t), "delivery.observe", ownerDeliveryObserveRequest{
			OwnerToken: "owner-one", SessionID: "session-one", BatchToken: "batch-one",
			ReportSequence: uint64(index + 1), Phase: phase, MessageIDs: []string{delivery.MessageID},
		}, &observed)
		if err != nil || observed.Phase != phase || !reflect.DeepEqual(observed.MessageIDs, []string{delivery.MessageID}) {
			t.Fatalf("%s observation = %+v, %v", phase, observed, err)
		}
	}
	if err = native.Call(ownerTestContext(t), "delivery.observe", ownerDeliveryObserveRequest{
		OwnerToken: "owner-one", SessionID: "session-one", BatchToken: "batch-one",
		ReportSequence: 5, Phase: "context", MessageIDs: []string{delivery.MessageID},
	}, nil); err == nil {
		t.Fatal("duplicate completed delivery batch was accepted")
	}
	select {
	case <-registry.Done():
	case <-ownerTestContext(t).Done():
		t.Fatal("invalid delivery observation did not fail the registry")
	}
}

func TestOwnerRegistryReturnsNativeQueueCapacityWithoutRetiringOwner(t *testing.T) {
	listener := ownerBusListener(t)
	directory := t.TempDir()
	fixture := &ownerNativeFixture{descriptions: map[string]ownerDescribeResult{
		"owner-token": {OwnerToken: "owner-token", SessionID: "native-session", CWD: "/work"},
	}}
	fixture.stageResult = func(request ownerStageRequest) ownerStageResult {
		queued := request.MessageID != "full"
		result := ownerStageResult{
			OwnerToken: request.OwnerToken, SessionID: request.SessionID, MessageID: request.MessageID, Queued: &queued,
		}
		if !queued {
			result.Reason = "queue_full"
		}
		return result
	}
	registry, native := ownerRegistryPair(t, OwnerRegistryOptions{
		Topology: ownerTopologyInteractive, Socket: listener.Addr().String(), Directory: directory,
	}, fixture)
	_, _, _ = ownerReady(t, registry, native, listener, ownerReadyRequest{
		Topology: ownerTopologyInteractive, Directory: directory, Scope: ownerScopePrimary,
		Mode: ownerModeTUI, OwnerToken: "owner-token", SessionID: "native-session",
	})
	state, err := registry.current("owner-token", "native-session")
	if err != nil {
		t.Fatal(err)
	}
	source := kit.DeliverySource{SessionID: "sender", Product: "codex-peer", Groups: []string{}}
	receipt, err := registry.deliver(ownerTestContext(t), state, kit.DeliveryRequest{MessageID: "full", From: source, Body: "one"})
	if err != nil || receipt.Disposition != "rejected" || receipt.Reason != "queue_full" {
		t.Fatalf("native queue rejection = %+v, %v", receipt, err)
	}
	registry.mu.Lock()
	if len(state.staged) != 0 || state.retainedBytes != ownerBindingRetainedBytes(state.OwnerBinding) {
		registry.mu.Unlock()
		t.Fatalf("rejected stage was retained: staged=%d retained=%d", len(state.staged), state.retainedBytes)
	}
	registry.mu.Unlock()
	receipt, err = registry.deliver(ownerTestContext(t), state, kit.DeliveryRequest{MessageID: "next", From: source, Body: "two"})
	if err != nil || receipt.Disposition != "queued_for_next_turn" {
		t.Fatalf("delivery after capacity rejection = %+v, %v", receipt, err)
	}
	if err := registry.Err(); err != nil {
		t.Fatalf("native capacity rejection retired owner: %v", err)
	}
}

func TestOwnerRegistryRejectsAmbiguousNativeQueueCapacity(t *testing.T) {
	for _, test := range []struct {
		name string
		raw  string
	}{
		{name: "missing queued", raw: `{"owner_token":"owner-token","session_id":"native-session","message_id":"bad","reason":"queue_full"}`},
		{name: "null queued", raw: `{"owner_token":"owner-token","session_id":"native-session","message_id":"bad","queued":null,"reason":"queue_full"}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			listener := ownerBusListener(t)
			directory := t.TempDir()
			fixture := &ownerNativeFixture{descriptions: map[string]ownerDescribeResult{
				"owner-token": {OwnerToken: "owner-token", SessionID: "native-session", CWD: "/work"},
			}}
			fixture.stageRaw = func(ownerStageRequest) json.RawMessage { return json.RawMessage(test.raw) }
			registry, native := ownerRegistryPair(t, OwnerRegistryOptions{
				Topology: ownerTopologyInteractive, Socket: listener.Addr().String(), Directory: directory,
			}, fixture)
			_, _, _ = ownerReady(t, registry, native, listener, ownerReadyRequest{
				Topology: ownerTopologyInteractive, Directory: directory, Scope: ownerScopePrimary,
				Mode: ownerModeTUI, OwnerToken: "owner-token", SessionID: "native-session",
			})
			state, err := registry.current("owner-token", "native-session")
			if err != nil {
				t.Fatal(err)
			}
			_, err = registry.deliver(ownerTestContext(t), state, kit.DeliveryRequest{
				MessageID: "bad", From: kit.DeliverySource{SessionID: "sender", Product: "codex-peer", Groups: []string{}}, Body: "body",
			})
			if err == nil || !strings.Contains(err.Error(), "acknowledgement is invalid") {
				t.Fatalf("ambiguous native capacity result = %v", err)
			}
			select {
			case <-registry.Done():
			case <-ownerTestContext(t).Done():
				t.Fatal("ambiguous native capacity result did not retire registry")
			}
		})
	}
}

func TestOwnerRegistryRejectsCapacityAfterNativeClaim(t *testing.T) {
	listener := ownerBusListener(t)
	directory := t.TempDir()
	entered, release := make(chan struct{}), make(chan struct{})
	fixture := &ownerNativeFixture{descriptions: map[string]ownerDescribeResult{
		"owner-token": {OwnerToken: "owner-token", SessionID: "native-session", CWD: "/work"},
	}}
	fixture.stageHook = func(ownerStageRequest) { close(entered); <-release }
	fixture.stageResult = func(request ownerStageRequest) ownerStageResult {
		queued := false
		return ownerStageResult{
			OwnerToken: request.OwnerToken, SessionID: request.SessionID, MessageID: request.MessageID,
			Queued: &queued, Reason: "queue_full",
		}
	}
	registry, native := ownerRegistryPair(t, OwnerRegistryOptions{
		Topology: ownerTopologyInteractive, Socket: listener.Addr().String(), Directory: directory,
	}, fixture)
	_, _, _ = ownerReady(t, registry, native, listener, ownerReadyRequest{
		Topology: ownerTopologyInteractive, Directory: directory, Scope: ownerScopePrimary,
		Mode: ownerModeTUI, OwnerToken: "owner-token", SessionID: "native-session",
	})
	state, err := registry.current("owner-token", "native-session")
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		_, callErr := registry.deliver(ownerTestContext(t), state, kit.DeliveryRequest{
			MessageID: "claimed", From: kit.DeliverySource{SessionID: "sender", Product: "codex-peer", Groups: []string{}}, Body: "body",
		})
		result <- callErr
	}()
	select {
	case <-entered:
	case <-ownerTestContext(t).Done():
		t.Fatal("native stage did not begin")
	}
	if err = native.Call(ownerTestContext(t), "delivery.observe", ownerDeliveryObserveRequest{
		OwnerToken: "owner-token", SessionID: "native-session", ReportSequence: 1,
		BatchToken: "batch-one", Phase: "claimed", MessageIDs: []string{"claimed"},
	}, nil); err != nil {
		t.Fatal(err)
	}
	close(release)
	if err = <-result; err == nil || !strings.Contains(err.Error(), "already claimed") {
		t.Fatalf("capacity result after claim = %v", err)
	}
	select {
	case <-registry.Done():
	case <-ownerTestContext(t).Done():
		t.Fatal("capacity result after claim did not retire registry")
	}
}

func TestOwnerRegistryAdmitsHelloBeforeFollowingPublicRequest(t *testing.T) {
	listener := ownerBusListener(t)
	directory := t.TempDir()
	fixture := &ownerNativeFixture{descriptions: map[string]ownerDescribeResult{
		"owner-token": {OwnerToken: "owner-token", SessionID: "native-session", Name: "native", CWD: "/work"},
	}}
	registry, native := ownerRegistryPair(t, OwnerRegistryOptions{
		Topology: ownerTopologyInteractive, Socket: listener.Addr().String(), Directory: directory,
	}, fixture)
	ready := make(chan error, 1)
	go func() {
		ready <- native.Call(ownerTestContext(t), "owner.ready", ownerReadyRequest{
			Topology: ownerTopologyInteractive, Directory: directory, Scope: ownerScopePrimary,
			Mode: ownerModeTUI, OwnerToken: "owner-token", SessionID: "native-session", Name: "native",
		}, nil)
	}()
	public, scanner := ownerAccept(t, listener)
	hello := ownerFrame(t, scanner)
	helloResult, err := protocol.ResultBytes(hello.ID, hello.Method, struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	deliveryResult, err := protocol.RequestBytes(72, "message.deliver", kit.DeliveryRequest{
		MessageID: "immediate", From: kit.DeliverySource{SessionID: "sender", Product: "codex-peer", Groups: []string{}}, Body: "next frame",
	})
	if err != nil {
		t.Fatal(err)
	}
	ownerWrite(t, public, append(helloResult, deliveryResult...))
	if err = <-ready; err != nil {
		t.Fatal(err)
	}
	if primary, ok := registry.Primary(); !ok || primary.OwnerToken != "owner-token" {
		t.Fatalf("primary after combined hello/request write = %+v, %v", primary, ok)
	}
	frame := ownerFrame(t, scanner)
	if frame.ID != 72 || frame.Error != nil {
		t.Fatalf("immediate post-hello delivery = %+v", frame)
	}
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	if len(fixture.stages) != 1 || fixture.stages[0].MessageID != "immediate" {
		t.Fatalf("immediate native stage = %+v", fixture.stages)
	}
}

func TestOwnerRegistryStageCancellationBoundaries(t *testing.T) {
	t.Run("before stage admission leaves no reservation", func(t *testing.T) {
		listener := ownerBusListener(t)
		directory := t.TempDir()
		entered, release := make(chan struct{}), make(chan struct{})
		var once sync.Once
		fixture := &ownerNativeFixture{descriptions: map[string]ownerDescribeResult{
			"owner-token": {OwnerToken: "owner-token", SessionID: "native-session", Name: "native", CWD: "/work"},
		}}
		fixture.stageHook = func(ownerStageRequest) { once.Do(func() { close(entered); <-release }) }
		registry, native := ownerRegistryPair(t, OwnerRegistryOptions{
			Topology: ownerTopologyInteractive, Socket: listener.Addr().String(), Directory: directory,
		}, fixture)
		_, _, _ = ownerReady(t, registry, native, listener, ownerReadyRequest{
			Topology: ownerTopologyInteractive, Directory: directory, Scope: ownerScopePrimary,
			Mode: ownerModeTUI, OwnerToken: "owner-token", SessionID: "native-session", Name: "native",
		})
		state, err := registry.current("owner-token", "native-session")
		if err != nil {
			t.Fatal(err)
		}
		firstDone := make(chan error, 1)
		go func() {
			_, callErr := registry.deliver(ownerTestContext(t), state, kit.DeliveryRequest{
				MessageID: "first", From: kit.DeliverySource{SessionID: "sender", Product: "codex-peer", Groups: []string{}}, Body: "first",
			})
			firstDone <- callErr
		}()
		select {
		case <-entered:
		case <-ownerTestContext(t).Done():
			t.Fatal("first stage did not hold admission")
		}
		canceled, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err = registry.deliver(canceled, state, kit.DeliveryRequest{
			MessageID: "canceled", From: kit.DeliverySource{SessionID: "sender", Product: "codex-peer", Groups: []string{}}, Body: "canceled",
		}); !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled pre-stage delivery = %v", err)
		}
		if _, err = registry.observeDelivery(mustOwnerJSON(t, ownerDeliveryObserveRequest{
			OwnerToken: "owner-token", SessionID: "native-session", ReportSequence: 1,
			BatchToken: "first-batch", Phase: "claimed", MessageIDs: []string{"first"},
		})); err != nil {
			t.Fatal(err)
		}
		close(release)
		if err = <-firstDone; err != nil {
			t.Fatal(err)
		}
		if _, err = registry.deliver(ownerTestContext(t), state, kit.DeliveryRequest{
			MessageID: "third", From: kit.DeliverySource{SessionID: "sender", Product: "codex-peer", Groups: []string{}}, Body: "third",
		}); err != nil {
			t.Fatal(err)
		}
		if _, err = registry.observeDelivery(mustOwnerJSON(t, ownerDeliveryObserveRequest{
			OwnerToken: "owner-token", SessionID: "native-session", ReportSequence: 2,
			BatchToken: "third-batch", Phase: "claimed", MessageIDs: []string{"third"},
		})); err != nil {
			t.Fatalf("claim after canceled waiter = %v", err)
		}
		fixture.mu.Lock()
		defer fixture.mu.Unlock()
		got := []string{fixture.stages[0].MessageID, fixture.stages[1].MessageID}
		if !slices.Equal(got, []string{"first", "third"}) {
			t.Fatalf("native stages after pre-admission cancellation = %#v", got)
		}
	})

	t.Run("after native stage retires uncertain owner", func(t *testing.T) {
		listener := ownerBusListener(t)
		directory := t.TempDir()
		entered, release := make(chan struct{}), make(chan struct{})
		fixture := &ownerNativeFixture{descriptions: map[string]ownerDescribeResult{
			"owner-token": {OwnerToken: "owner-token", SessionID: "native-session", Name: "native", CWD: "/work"},
		}}
		fixture.stageHook = func(ownerStageRequest) { close(entered); <-release }
		registry, native := ownerRegistryPair(t, OwnerRegistryOptions{
			Topology: ownerTopologyInteractive, Socket: listener.Addr().String(), Directory: directory,
		}, fixture)
		_, _, _ = ownerReady(t, registry, native, listener, ownerReadyRequest{
			Topology: ownerTopologyInteractive, Directory: directory, Scope: ownerScopePrimary,
			Mode: ownerModeTUI, OwnerToken: "owner-token", SessionID: "native-session", Name: "native",
		})
		state, err := registry.current("owner-token", "native-session")
		if err != nil {
			t.Fatal(err)
		}
		callCtx, cancel := context.WithCancel(context.Background())
		result := make(chan error, 1)
		go func() {
			_, callErr := registry.deliver(callCtx, state, kit.DeliveryRequest{
				MessageID: "uncertain", From: kit.DeliverySource{SessionID: "sender", Product: "codex-peer", Groups: []string{}}, Body: "uncertain",
			})
			result <- callErr
		}()
		select {
		case <-entered:
		case <-ownerTestContext(t).Done():
			t.Fatal("native stage was not observed")
		}
		cancel()
		if err = <-result; !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled submitted stage = %v", err)
		}
		select {
		case <-registry.Done():
		case <-ownerTestContext(t).Done():
			t.Fatal("uncertain native stage did not retire registry")
		}
		if err = registry.Err(); err == nil || !strings.Contains(err.Error(), "stage outcome is uncertain") {
			t.Fatalf("uncertain stage registry error = %v", err)
		}
		close(release)
	})
}

func TestOwnerRegistryChildFirstDoesNotAdoptPrimary(t *testing.T) {
	listener := ownerBusListener(t)
	directory := t.TempDir()
	caller := kit.NewCaller(func(context.Context, string, any) (json.RawMessage, error) { return json.RawMessage(`{}`), nil })
	fixture := &ownerNativeFixture{descriptions: map[string]ownerDescribeResult{
		"child-token": {OwnerToken: "child-token", SessionID: "child-session", Name: "child", CWD: "/work/child"},
		"main-token":  {OwnerToken: "main-token", SessionID: "main-session", Name: "main", CWD: "/work/main"},
	}}
	registry, native := ownerRegistryPair(t, OwnerRegistryOptions{
		Topology: ownerTopologyLane, Socket: listener.Addr().String(), Directory: directory, PrimaryCaller: caller,
	}, fixture)
	childReady := make(chan error, 1)
	go func() {
		childReady <- native.Call(ownerTestContext(t), "owner.ready", ownerReadyRequest{
			Topology: ownerTopologyLane, Directory: directory, Scope: ownerScopeChild, Mode: ownerModePrint,
			OwnerToken: "child-token", SessionID: "child-session", Name: "child",
		}, nil)
	}()
	childConn, childScanner := ownerAccept(t, listener)
	_ = ownerHello(t, childConn, childScanner)
	if err := <-childReady; err != nil {
		t.Fatal(err)
	}
	select {
	case <-registry.Ready():
		t.Fatal("child-first readiness adopted the primary")
	default:
	}
	if _, ok := registry.Primary(); ok {
		t.Fatal("child-first binding was returned as primary")
	}
	if err := native.Call(ownerTestContext(t), "owner.ready", ownerReadyRequest{
		Topology: ownerTopologyLane, Directory: directory, Scope: ownerScopePrimary, Mode: ownerModeRPC,
		OwnerToken: "main-token", SessionID: "main-session", Name: "main",
	}, nil); err != nil {
		t.Fatal(err)
	}
	select {
	case <-registry.Ready():
	case <-ownerTestContext(t).Done():
		t.Fatal("explicit primary did not close readiness")
	}
}

func TestOwnerRegistryClaimCanPrecedeStageResponseAndReportsReorder(t *testing.T) {
	listener := ownerBusListener(t)
	directory := t.TempDir()
	stageEntered, releaseStage := make(chan struct{}), make(chan struct{})
	var stageOnce sync.Once
	fixture := &ownerNativeFixture{descriptions: map[string]ownerDescribeResult{
		"owner-token": {OwnerToken: "owner-token", SessionID: "native-session", Name: "native", CWD: "/work"},
	}}
	fixture.stageHook = func(ownerStageRequest) {
		stageOnce.Do(func() {
			close(stageEntered)
			<-releaseStage
		})
	}
	registry, native := ownerRegistryPair(t, OwnerRegistryOptions{
		Topology: ownerTopologyInteractive, Socket: listener.Addr().String(), Directory: directory,
	}, fixture)
	_, public, scanner := ownerReady(t, registry, native, listener, ownerReadyRequest{
		Topology: ownerTopologyInteractive, Directory: directory, Scope: ownerScopePrimary, Mode: ownerModeTUI,
		OwnerToken: "owner-token", SessionID: "native-session", Name: "native",
	})

	first := kit.DeliveryRequest{MessageID: "first", From: kit.DeliverySource{SessionID: "sender", Product: "codex-peer", Groups: []string{}}, Body: "first"}
	second := kit.DeliveryRequest{MessageID: "second", From: first.From, Body: "second"}
	firstBody, err := protocol.RequestBytes(50, "message.deliver", first)
	if err != nil {
		t.Fatal(err)
	}
	secondBody, err := protocol.RequestBytes(51, "message.deliver", second)
	if err != nil {
		t.Fatal(err)
	}
	ownerWrite(t, public, append(firstBody, secondBody...))
	select {
	case <-stageEntered:
	case <-ownerTestContext(t).Done():
		t.Fatal("first native stage did not begin")
	}
	if err = native.Call(ownerTestContext(t), "delivery.observe", ownerDeliveryObserveRequest{
		OwnerToken: "owner-token", SessionID: "native-session", ReportSequence: 1,
		BatchToken: "batch", Phase: "claimed", MessageIDs: []string{"first"},
	}, nil); err != nil {
		t.Fatalf("claim before stage response = %v", err)
	}
	close(releaseStage)
	for id := int64(50); id <= 51; id++ {
		frame := ownerFrame(t, scanner)
		if frame.ID != id || frame.Error != nil {
			t.Fatalf("ordered delivery receipt %d = %+v", id, frame)
		}
	}
	fixture.mu.Lock()
	stagedIDs := []string{fixture.stages[0].MessageID, fixture.stages[1].MessageID}
	fixture.mu.Unlock()
	if !slices.Equal(stagedIDs, []string{"first", "second"}) {
		t.Fatalf("native stage order = %#v", stagedIDs)
	}

	// Model Bridge handler scheduling independently of native issue order: the
	// later report can reach registry state first, but its source sequence keeps
	// it pending until the missing predecessor is applied.
	if _, err = registry.observeDelivery(mustOwnerJSON(t, ownerDeliveryObserveRequest{
		OwnerToken: "owner-token", SessionID: "native-session", ReportSequence: 3,
		BatchToken: "batch", Phase: "message_end", MessageIDs: []string{"first"},
	})); err != nil {
		t.Fatalf("later scheduled report = %v", err)
	}
	if _, err = registry.observeDelivery(mustOwnerJSON(t, ownerDeliveryObserveRequest{
		OwnerToken: "owner-token", SessionID: "native-session", ReportSequence: 2,
		BatchToken: "batch", Phase: "message_start", MessageIDs: []string{"first"},
	})); err != nil {
		t.Fatalf("earlier scheduled report = %v", err)
	}
	if err = native.Call(ownerTestContext(t), "delivery.observe", ownerDeliveryObserveRequest{
		OwnerToken: "owner-token", SessionID: "native-session", ReportSequence: 4,
		BatchToken: "batch", Phase: "context", MessageIDs: []string{"first"},
	}, nil); err != nil {
		t.Fatal(err)
	}
}

func mustOwnerJSON(t *testing.T, value any) json.RawMessage {
	t.Helper()
	body, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func TestOwnerRegistryInteractiveSwitchWithdrawsBeforeReplacement(t *testing.T) {
	listener := ownerBusListener(t)
	directory := t.TempDir()
	fixture := &ownerNativeFixture{descriptions: map[string]ownerDescribeResult{
		"old-token": {OwnerToken: "old-token", SessionID: "same-session", Name: "old", CWD: "/work/old"},
		"new-token": {OwnerToken: "new-token", SessionID: "same-session", Name: "new", CWD: "/work/new"},
	}}
	registry, native := ownerRegistryPair(t, OwnerRegistryOptions{
		Topology: ownerTopologyInteractive, Socket: listener.Addr().String(), Directory: directory,
	}, fixture)
	_, oldConn, oldScanner := ownerReady(t, registry, native, listener, ownerReadyRequest{
		Topology: ownerTopologyInteractive, Directory: directory, Scope: ownerScopePrimary,
		Mode: ownerModeTUI, OwnerToken: "old-token", SessionID: "same-session", Name: "old",
	})

	switched := make(chan error, 1)
	go func() {
		switched <- native.Call(ownerTestContext(t), "owner.switch", ownerSwitchRequest{
			Topology: ownerTopologyInteractive, Scope: ownerScopePrimary, Mode: ownerModeTUI,
			PreviousOwnerToken: "old-token", OwnerToken: "new-token",
			PreviousSessionID: "same-session", SessionID: "same-session", Name: "new", Reason: "reload",
		}, nil)
	}()
	newConn, newScanner := ownerAccept(t, listener)
	newHello := ownerHello(t, newConn, newScanner)
	if err := <-switched; err != nil {
		t.Fatal(err)
	}
	if newHello.SessionID != "same-session" || newHello.Name != "new" || newHello.Info["cwd"] != "/work/new" {
		t.Fatalf("replacement hello = %+v", newHello)
	}
	if oldScanner.Scan() || oldScanner.Err() != nil {
		t.Fatalf("old connection survived replacement: %v", oldScanner.Err())
	}
	_ = oldConn.Close()
	if primary, ok := registry.Primary(); !ok || primary.OwnerToken != "new-token" || primary.CWD != "/work/new" {
		t.Fatalf("replacement primary = %+v, %v", primary, ok)
	}

	err := native.Call(ownerTestContext(t), "session_end", ownerEndRequest{
		Topology: ownerTopologyInteractive, Scope: ownerScopePrimary, Mode: ownerModeTUI,
		OwnerToken: "old-token", SessionID: "same-session", Reason: "delayed",
	}, nil)
	var bridgeErr *pifamily.BridgeCallError
	if !errors.As(err, &bridgeErr) || bridgeErr.Code != "stale_owner" {
		t.Fatalf("delayed old end = %#v", err)
	}
	if primary, ok := registry.Primary(); !ok || primary.OwnerToken != "new-token" {
		t.Fatalf("delayed old end changed primary = %+v, %v", primary, ok)
	}
}

func TestOwnerRegistryToolCallPinsCallerGeneration(t *testing.T) {
	directory := t.TempDir()
	started, release := make(chan struct{}), make(chan struct{})
	caller := kit.NewCaller(func(context.Context, string, any) (json.RawMessage, error) {
		close(started)
		<-release
		return json.RawMessage(`{"sessions":[]}`), nil
	})
	fixture := &ownerNativeFixture{descriptions: map[string]ownerDescribeResult{
		"main-token": {OwnerToken: "main-token", SessionID: "main-session", CWD: "/work/main"},
	}}
	registry, native := ownerRegistryPair(t, OwnerRegistryOptions{
		Topology: ownerTopologyLane, Socket: filepath.Join(directory, "bus.sock"), Directory: directory, PrimaryCaller: caller,
	}, fixture)
	if err := native.Call(ownerTestContext(t), "owner.ready", ownerReadyRequest{
		Topology: ownerTopologyLane, Directory: directory, Scope: ownerScopePrimary, Mode: ownerModeRPC,
		OwnerToken: "main-token", SessionID: "main-session",
	}, nil); err != nil {
		t.Fatal(err)
	}
	if primary, ok := registry.Primary(); !ok || primary.OwnerToken != "main-token" {
		t.Fatalf("primary before caller withdrawal = %+v, %v", primary, ok)
	}
	toolDone := make(chan error, 1)
	go func() {
		toolDone <- native.Call(ownerTestContext(t), "tool.call", ownerToolRequest{
			OwnerToken: "main-token", SessionID: "main-session", CallID: "call-one",
			Action: "list", Arguments: json.RawMessage(`{}`),
		}, nil)
	}()
	select {
	case <-started:
	case <-ownerTestContext(t).Done():
		t.Fatal("public tool call did not start")
	}
	if err := native.Call(ownerTestContext(t), "session_end", ownerEndRequest{
		Topology: ownerTopologyLane, Scope: ownerScopePrimary, Mode: ownerModeRPC,
		OwnerToken: "main-token", SessionID: "main-session", Reason: "ended",
	}, nil); err != nil {
		t.Fatal(err)
	}
	close(release)
	err := <-toolDone
	var bridgeErr *pifamily.BridgeCallError
	if !errors.As(err, &bridgeErr) || bridgeErr.Code != "stale_owner" {
		t.Fatalf("tool result after owner withdrawal = %#v", err)
	}
}

func TestOwnerRegistryConsumedEvidenceDoesNotImposeLifetimeTurnLimit(t *testing.T) {
	directory := t.TempDir()
	caller := kit.NewCaller(func(context.Context, string, any) (json.RawMessage, error) { return json.RawMessage(`{}`), nil })
	fixture := &ownerNativeFixture{descriptions: map[string]ownerDescribeResult{
		"main-token": {OwnerToken: "main-token", SessionID: "main-session", CWD: "/work/main"},
	}}
	registry, native := ownerRegistryPair(t, OwnerRegistryOptions{
		Topology: ownerTopologyLane, Socket: filepath.Join(directory, "bus.sock"), Directory: directory, PrimaryCaller: caller,
	}, fixture)
	if err := native.Call(ownerTestContext(t), "owner.ready", ownerReadyRequest{
		Topology: ownerTopologyLane, Directory: directory, Scope: ownerScopePrimary, Mode: ownerModeRPC,
		OwnerToken: "main-token", SessionID: "main-session",
	}, nil); err != nil {
		t.Fatal(err)
	}
	var sequence uint64
	for index := 0; index < maxOwnerTrackedItems+8; index++ {
		sequence++
		runToken := fmt.Sprintf("run-%d", index)
		prompt := fmt.Sprintf("prompt-%d", index)
		if _, err := registry.recordPreflight(mustOwnerJSON(t, ownerPreflightRequest{
			OwnerToken: "main-token", SessionID: "main-session", ReportSequence: sequence,
			RunToken: runToken, Prompt: prompt,
		})); err != nil {
			t.Fatalf("preflight %d = %v", index, err)
		}
		if got, err := registry.takePreflight("main-token", "main-session", runToken); err != nil || got != prompt {
			t.Fatalf("consumed preflight %d = %q, %v", index, got, err)
		}
	}
	state, err := registry.current("main-token", "main-session")
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < maxOwnerTrackedItems+8; index++ {
		messageID := fmt.Sprintf("message-%d", index)
		batchToken := fmt.Sprintf("batch-%d", index)
		registry.mu.Lock()
		if err = registry.reserveStateLocked(state, len(messageID)); err != nil {
			registry.mu.Unlock()
			t.Fatal(err)
		}
		state.staged = append(state.staged, &ownerStagedDelivery{messageID: messageID, acknowledged: true})
		registry.mu.Unlock()
		for _, phase := range []string{"claimed", "message_start", "message_end", "context"} {
			sequence++
			if _, err = registry.observeDelivery(mustOwnerJSON(t, ownerDeliveryObserveRequest{
				OwnerToken: "main-token", SessionID: "main-session", ReportSequence: sequence,
				BatchToken: batchToken, Phase: phase, MessageIDs: []string{messageID},
			})); err != nil {
				t.Fatalf("batch %d phase %s = %v", index, phase, err)
			}
		}
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if len(state.preflights) != 0 || len(state.consumedPreflights) != maxOwnerTrackedItems || len(state.consumedPreflightSet) != maxOwnerTrackedItems ||
		len(state.batches) != 0 || len(state.completed) != maxOwnerTrackedItems || len(state.completedSet) != maxOwnerTrackedItems {
		t.Fatalf("retained evidence = preflights %d/%d/%d, batches %d/%d/%d", len(state.preflights), len(state.consumedPreflights),
			len(state.consumedPreflightSet), len(state.batches), len(state.completed), len(state.completedSet))
	}
}

func TestOwnerRegistryRecoversWhenDaemonStartsAfterNativeOwner(t *testing.T) {
	directory := testsocket.Directory(t)
	socket := filepath.Join(directory, "late-bus.sock")
	fixture := &ownerNativeFixture{descriptions: map[string]ownerDescribeResult{
		"owner-token": {OwnerToken: "owner-token", SessionID: "native-session", Name: "native", CWD: "/work/live"},
	}}
	registry, native := ownerRegistryPair(t, OwnerRegistryOptions{
		Topology: ownerTopologyInteractive, Socket: socket, Directory: directory, Groups: []string{"shared"},
	}, fixture)
	retryEntered := make(chan struct{}, 1)
	retry := make(chan struct{})
	registry.retryPublic = func(ctx context.Context) error {
		select {
		case retryEntered <- struct{}{}:
		default:
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-retry:
			return nil
		}
	}
	if err := native.Call(ownerTestContext(t), "owner.ready", ownerReadyRequest{
		Topology: ownerTopologyInteractive, Directory: directory, Scope: ownerScopePrimary,
		Mode: ownerModeTUI, OwnerToken: "owner-token", SessionID: "native-session", Name: "native",
	}, nil); err != nil {
		t.Fatal(err)
	}
	select {
	case <-retryEntered:
	case <-ownerTestContext(t).Done():
		t.Fatal("public publisher did not observe the absent daemon")
	}
	if binding, ok := registry.Primary(); !ok || binding.SessionID != "native-session" || binding.CWD != "/work/live" {
		t.Fatalf("private binding during daemon outage = %+v, %v", binding, ok)
	}
	select {
	case <-registry.Ready():
		t.Fatal("registry became ready before public hello")
	default:
	}
	err := native.Call(ownerTestContext(t), "tool.call", ownerToolRequest{
		OwnerToken: "owner-token", SessionID: "native-session", CallID: "outage-call",
		Action: "list", Arguments: json.RawMessage(`{}`),
	}, nil)
	var bridgeErr *pifamily.BridgeCallError
	if !errors.As(err, &bridgeErr) || bridgeErr.Code != "tool_error" {
		t.Fatalf("tool call during outage = %#v", err)
	}
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	close(retry)
	conn, scanner := ownerAccept(t, listener)
	hello := ownerHello(t, conn, scanner)
	if hello.SessionID != "native-session" || hello.Name != "native" || hello.Info["cwd"] != "/work/live" ||
		!slices.Equal(hello.Groups, []string{"shared"}) {
		t.Fatalf("recovered public hello = %+v", hello)
	}
	select {
	case <-registry.Ready():
	case <-ownerTestContext(t).Done():
		t.Fatal("registry did not become ready after daemon recovery")
	}
}

func TestOwnerRegistryReconnectsWithoutReplayingLostToolCall(t *testing.T) {
	listener := ownerBusListener(t)
	directory := t.TempDir()
	fixture := &ownerNativeFixture{descriptions: map[string]ownerDescribeResult{
		"owner-token": {OwnerToken: "owner-token", SessionID: "native-session", Name: "native", CWD: "/work/live"},
	}}
	registry, native := ownerRegistryPair(t, OwnerRegistryOptions{
		Topology: ownerTopologyInteractive, Socket: listener.Addr().String(), Directory: directory, Groups: []string{"shared"},
	}, fixture)
	retryEntered := make(chan struct{}, 1)
	retry := make(chan struct{})
	registry.retryPublic = func(ctx context.Context) error {
		select {
		case retryEntered <- struct{}{}:
		default:
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-retry:
			return nil
		}
	}
	hello, first, firstScanner := ownerReady(t, registry, native, listener, ownerReadyRequest{
		Topology: ownerTopologyInteractive, Directory: directory, Scope: ownerScopePrimary,
		Mode: ownerModeTUI, OwnerToken: "owner-token", SessionID: "native-session", Name: "native",
	})
	if hello.SessionID != "native-session" {
		t.Fatalf("first hello = %+v", hello)
	}
	lost := make(chan error, 1)
	go func() {
		lost <- native.Call(ownerTestContext(t), "tool.call", ownerToolRequest{
			OwnerToken: "owner-token", SessionID: "native-session", CallID: "lost-call",
			Action: "list", Arguments: json.RawMessage(`{}`),
		}, nil)
	}()
	frame := ownerFrame(t, firstScanner)
	if !frame.Request || frame.Method != "session.list" {
		t.Fatalf("old-wire action = %+v", frame)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	err := <-lost
	var bridgeErr *pifamily.BridgeCallError
	if !errors.As(err, &bridgeErr) || bridgeErr.Code != "tool_error" {
		t.Fatalf("lost old-wire action = %v; registry = %v", err, registry.Err())
	}
	select {
	case <-retryEntered:
	case <-ownerTestContext(t).Done():
		t.Fatal("publisher did not enter reconnect backoff")
	}
	if binding, ok := registry.Primary(); !ok || binding.SessionID != "native-session" {
		t.Fatalf("private binding after public loss = %+v, %v", binding, ok)
	}
	err = native.Call(ownerTestContext(t), "tool.call", ownerToolRequest{
		OwnerToken: "owner-token", SessionID: "native-session", CallID: "gap-call",
		Action: "list", Arguments: json.RawMessage(`{}`),
	}, nil)
	if !errors.As(err, &bridgeErr) || bridgeErr.Code != "tool_error" {
		t.Fatalf("outage action = %#v", err)
	}
	close(retry)
	second, secondScanner := ownerAccept(t, listener)
	secondHello := ownerHello(t, second, secondScanner)
	ownerWaitPublished(t, registry, "owner-token")
	if !reflect.DeepEqual(secondHello, hello) {
		t.Fatalf("reconnected hello changed identity:\nfirst  %+v\nsecond %+v", hello, secondHello)
	}
	fresh := make(chan error, 1)
	go func() {
		fresh <- native.Call(ownerTestContext(t), "tool.call", ownerToolRequest{
			OwnerToken: "owner-token", SessionID: "native-session", CallID: "fresh-call",
			Action: "list", Arguments: json.RawMessage(`{}`),
		}, nil)
	}()
	frame = ownerFrame(t, secondScanner)
	if !frame.Request || frame.Method != "session.list" {
		t.Fatalf("fresh action after reconnect = %+v", frame)
	}
	body, err := protocol.ResultBytes(frame.ID, frame.Method, kit.SessionListResult{Sessions: []kit.SessionSummary{}})
	if err != nil {
		t.Fatal(err)
	}
	ownerWrite(t, second, body)
	if err = <-fresh; err != nil {
		t.Fatal(err)
	}
}

func TestOwnerRegistryPublishesOnlyReplacementIdentityAfterOutage(t *testing.T) {
	directory := testsocket.Directory(t)
	socket := filepath.Join(directory, "replace-bus.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	fixture := &ownerNativeFixture{descriptions: map[string]ownerDescribeResult{
		"old-token": {OwnerToken: "old-token", SessionID: "old-session", Name: "old", CWD: "/work/old"},
		"new-token": {OwnerToken: "new-token", SessionID: "new-session", Name: "new", CWD: "/work/new"},
	}}
	registry, native := ownerRegistryPair(t, OwnerRegistryOptions{
		Topology: ownerTopologyInteractive, Socket: socket, Directory: directory, Groups: []string{"shared"},
	}, fixture)
	retryEntered := make(chan string, 2)
	retry := make(chan struct{})
	registry.retryPublic = func(ctx context.Context) error {
		registry.mu.Lock()
		token := ""
		for key, state := range registry.bindings {
			if state.publicCtx == ctx {
				token = key
				break
			}
		}
		registry.mu.Unlock()
		select {
		case retryEntered <- token:
		case <-ctx.Done():
			return ctx.Err()
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-retry:
			return nil
		}
	}
	_, oldConn, _ := ownerReady(t, registry, native, listener, ownerReadyRequest{
		Topology: ownerTopologyInteractive, Directory: directory, Scope: ownerScopePrimary,
		Mode: ownerModeTUI, OwnerToken: "old-token", SessionID: "old-session", Name: "old",
	})
	registry.mu.Lock()
	oldState := registry.bindings["old-token"]
	oldAttempt := oldState.attempt
	registry.mu.Unlock()
	if err = oldConn.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case token := <-retryEntered:
		if token != "old-token" {
			t.Fatalf("old reconnect token = %q", token)
		}
	case <-ownerTestContext(t).Done():
		t.Fatal("old publisher did not reach reconnect backoff")
	}
	if err = listener.Close(); err != nil {
		t.Fatal(err)
	}
	if err = native.Call(ownerTestContext(t), "owner.switch", ownerSwitchRequest{
		Topology: ownerTopologyInteractive, Scope: ownerScopePrimary, Mode: ownerModeTUI,
		PreviousOwnerToken: "old-token", OwnerToken: "new-token",
		PreviousSessionID: "old-session", SessionID: "new-session", Name: "new", Reason: "resume",
	}, nil); err != nil {
		t.Fatal(err)
	}
	select {
	case token := <-retryEntered:
		if token != "new-token" {
			t.Fatalf("replacement reconnect token = %q", token)
		}
	case <-ownerTestContext(t).Done():
		t.Fatal("replacement publisher did not reach reconnect backoff")
	}
	listener, err = net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	close(retry)
	conn, scanner := ownerAccept(t, listener)
	hello := ownerHello(t, conn, scanner)
	ownerWaitPublished(t, registry, "new-token")
	if hello.SessionID != "new-session" || hello.Name != "new" || hello.Info["cwd"] != "/work/new" {
		t.Fatalf("replacement hello = %+v", hello)
	}
	if binding, ok := registry.Primary(); !ok || binding.OwnerToken != "new-token" || binding.SessionID != "new-session" {
		t.Fatalf("replacement private binding = %+v, %v", binding, ok)
	}
	registry.handlePublic(context.Background(), oldState, oldAttempt, &kit.Request{Method: "session.superseded"})
	select {
	case <-registry.Done():
		t.Fatalf("stale supersession retired replacement: %v", registry.Err())
	default:
	}
	for len(retryEntered) < cap(retryEntered) {
		retryEntered <- "occupied"
	}
	retryCtx, cancelRetry := context.WithCancel(context.Background())
	retryDone := make(chan error, 1)
	go func() { retryDone <- registry.retryPublic(retryCtx) }()
	cancelRetry()
	select {
	case err = <-retryDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("full retry notification cancellation = %v", err)
		}
	case <-ownerTestContext(t).Done():
		t.Fatal("full retry notification did not observe cancellation")
	}
}

func TestOwnerRegistryJoinsCanceledPublisherDialOnSessionEnd(t *testing.T) {
	directory := t.TempDir()
	fixture := &ownerNativeFixture{descriptions: map[string]ownerDescribeResult{
		"child-token": {OwnerToken: "child-token", SessionID: "child-session", Name: "child", CWD: "/work/child"},
	}}
	caller := kit.NewCaller(func(context.Context, string, any) (json.RawMessage, error) { return json.RawMessage(`{}`), nil })
	registry, native := ownerRegistryPair(t, OwnerRegistryOptions{
		Topology: ownerTopologyLane, Socket: filepath.Join(directory, "missing.sock"), Directory: directory, PrimaryCaller: caller,
	}, fixture)
	dialEntered, dialReturned := make(chan struct{}), make(chan struct{})
	registry.dialPublic = func(ctx context.Context, _, _ string) (net.Conn, error) {
		close(dialEntered)
		<-ctx.Done()
		close(dialReturned)
		return nil, ctx.Err()
	}
	if err := native.Call(ownerTestContext(t), "owner.ready", ownerReadyRequest{
		Topology: ownerTopologyLane, Directory: directory, Scope: ownerScopeChild, Mode: ownerModePrint,
		OwnerToken: "child-token", SessionID: "child-session", Name: "child",
	}, nil); err != nil {
		t.Fatal(err)
	}
	select {
	case <-dialEntered:
	case <-ownerTestContext(t).Done():
		t.Fatal("public dial did not begin")
	}
	if err := native.Call(ownerTestContext(t), "session_end", ownerEndRequest{
		Topology: ownerTopologyLane, Scope: ownerScopeChild, Mode: ownerModePrint,
		OwnerToken: "child-token", SessionID: "child-session", Reason: "task-complete",
	}, nil); err != nil {
		t.Fatal(err)
	}
	select {
	case <-dialReturned:
	default:
		t.Fatal("session_end returned before its canceled public dial joined")
	}
}

func TestOwnerRegistryJoinsCanceledPublisherHelloOnSessionEnd(t *testing.T) {
	listener := ownerBusListener(t)
	directory := t.TempDir()
	fixture := &ownerNativeFixture{descriptions: map[string]ownerDescribeResult{
		"child-token": {OwnerToken: "child-token", SessionID: "child-session", Name: "child", CWD: "/work/child"},
	}}
	caller := kit.NewCaller(func(context.Context, string, any) (json.RawMessage, error) { return json.RawMessage(`{}`), nil })
	registry, native := ownerRegistryPair(t, OwnerRegistryOptions{
		Topology: ownerTopologyLane, Socket: listener.Addr().String(), Directory: directory, PrimaryCaller: caller,
	}, fixture)
	_ = registry
	if err := native.Call(ownerTestContext(t), "owner.ready", ownerReadyRequest{
		Topology: ownerTopologyLane, Directory: directory, Scope: ownerScopeChild, Mode: ownerModePrint,
		OwnerToken: "child-token", SessionID: "child-session", Name: "child",
	}, nil); err != nil {
		t.Fatal(err)
	}
	public, scanner := ownerAccept(t, listener)
	frame := ownerFrame(t, scanner)
	if !frame.Request || frame.Method != "session.hello" {
		t.Fatalf("pending child hello = %+v", frame)
	}
	if err := native.Call(ownerTestContext(t), "session_end", ownerEndRequest{
		Topology: ownerTopologyLane, Scope: ownerScopeChild, Mode: ownerModePrint,
		OwnerToken: "child-token", SessionID: "child-session", Reason: "task-complete",
	}, nil); err != nil {
		t.Fatal(err)
	}
	if scanner.Scan() || scanner.Err() != nil {
		t.Fatalf("pending hello connection survived session_end: %v", scanner.Err())
	}
	_ = public.Close()
}

func TestOwnerRegistryJoinsCanceledUnreadHelloWriteOnSessionEnd(t *testing.T) {
	directory := t.TempDir()
	client, server := net.Pipe()
	t.Cleanup(func() { _ = server.Close() })
	entered := make(chan struct{})
	fixture := &ownerNativeFixture{descriptions: map[string]ownerDescribeResult{
		"owner-token": {OwnerToken: "owner-token", SessionID: "native-session", Name: "native", CWD: "/work"},
	}}
	registry, native := ownerRegistryPair(t, OwnerRegistryOptions{
		Topology: ownerTopologyInteractive, Socket: filepath.Join(directory, "bus.sock"), Directory: directory,
	}, fixture)
	registry.dialPublic = func(context.Context, string, string) (net.Conn, error) {
		return &ownerHeldWriteConn{Conn: client, entered: entered}, nil
	}
	if err := native.Call(ownerTestContext(t), "owner.ready", ownerReadyRequest{
		Topology: ownerTopologyInteractive, Directory: directory, Scope: ownerScopePrimary, Mode: ownerModeTUI,
		OwnerToken: "owner-token", SessionID: "native-session", Name: "native",
	}, nil); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-ownerTestContext(t).Done():
		t.Fatal("public hello write did not begin")
	}
	if err := native.Call(ownerTestContext(t), "session_end", ownerEndRequest{
		Topology: ownerTopologyInteractive, Scope: ownerScopePrimary, Mode: ownerModeTUI,
		OwnerToken: "owner-token", SessionID: "native-session", Reason: "quit",
	}, nil); err != nil {
		t.Fatal(err)
	}
	registry.mu.Lock()
	_, retained := registry.bindings["owner-token"]
	registry.mu.Unlock()
	if retained {
		t.Fatal("session_end returned before unread-hello publisher withdrawal")
	}
}

func TestOwnerRegistryReconnectsChildWithoutChangingLanePrimary(t *testing.T) {
	listener := ownerBusListener(t)
	directory := t.TempDir()
	primaryCalls := make(chan string, 1)
	caller := kit.NewCaller(func(_ context.Context, method string, _ any) (json.RawMessage, error) {
		primaryCalls <- method
		return json.RawMessage(`{"sessions":[]}`), nil
	})
	fixture := &ownerNativeFixture{descriptions: map[string]ownerDescribeResult{
		"main-token":  {OwnerToken: "main-token", SessionID: "main-session", CWD: "/work/main"},
		"child-token": {OwnerToken: "child-token", SessionID: "child-session", Name: "child", CWD: "/work/child"},
	}}
	registry, native := ownerRegistryPair(t, OwnerRegistryOptions{
		Topology: ownerTopologyLane, Socket: listener.Addr().String(), Directory: directory, PrimaryCaller: caller,
	}, fixture)
	retryEntered, retry := make(chan struct{}, 1), make(chan struct{})
	registry.retryPublic = func(ctx context.Context) error {
		select {
		case retryEntered <- struct{}{}:
		default:
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-retry:
			return nil
		}
	}
	if err := native.Call(ownerTestContext(t), "owner.ready", ownerReadyRequest{
		Topology: ownerTopologyLane, Directory: directory, Scope: ownerScopePrimary, Mode: ownerModeRPC,
		OwnerToken: "main-token", SessionID: "main-session",
	}, nil); err != nil {
		t.Fatal(err)
	}
	hello, child, _ := ownerReady(t, registry, native, listener, ownerReadyRequest{
		Topology: ownerTopologyLane, Directory: directory, Scope: ownerScopeChild, Mode: ownerModePrint,
		OwnerToken: "child-token", SessionID: "child-session", Name: "child",
	})
	if err := child.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-retryEntered:
	case <-ownerTestContext(t).Done():
		t.Fatal("child publisher did not enter reconnect backoff")
	}
	if err := native.Call(ownerTestContext(t), "tool.call", ownerToolRequest{
		OwnerToken: "main-token", SessionID: "main-session", CallID: "primary-call",
		Action: "list", Arguments: json.RawMessage(`{}`),
	}, nil); err != nil {
		t.Fatal(err)
	}
	if method := <-primaryCalls; method != "session.list" {
		t.Fatalf("lane primary call during child outage = %q", method)
	}
	close(retry)
	reconnected, scanner := ownerAccept(t, listener)
	if got := ownerHello(t, reconnected, scanner); !reflect.DeepEqual(got, hello) {
		t.Fatalf("child rehello changed identity:\nfirst  %+v\nsecond %+v", hello, got)
	}
	ownerWaitPublished(t, registry, "child-token")
	if primary, ok := registry.Primary(); !ok || primary.OwnerToken != "main-token" || primary.SessionID != "main-session" {
		t.Fatalf("child reconnect changed lane primary = %+v, %v", primary, ok)
	}
}

func TestOwnerRegistryReleasesLostAttemptDeliveryAccounting(t *testing.T) {
	listener := ownerBusListener(t)
	directory := t.TempDir()
	fixture := &ownerNativeFixture{descriptions: map[string]ownerDescribeResult{
		"owner-token": {OwnerToken: "owner-token", SessionID: "native-session", Name: "native", CWD: "/work"},
	}}
	registry, native := ownerRegistryPair(t, OwnerRegistryOptions{
		Topology: ownerTopologyInteractive, Socket: listener.Addr().String(), Directory: directory,
	}, fixture)
	retryEntered := make(chan struct{}, 1)
	registry.retryPublic = func(ctx context.Context) error {
		select {
		case retryEntered <- struct{}{}:
		default:
		}
		<-ctx.Done()
		return ctx.Err()
	}
	_, public, _ := ownerReady(t, registry, native, listener, ownerReadyRequest{
		Topology: ownerTopologyInteractive, Directory: directory, Scope: ownerScopePrimary, Mode: ownerModeTUI,
		OwnerToken: "owner-token", SessionID: "native-session", Name: "native",
	})
	registry.mu.Lock()
	state := registry.bindings["owner-token"]
	attempt := state.attempt
	baseline := state.retainedBytes
	registry.mu.Unlock()
	if attempt == nil {
		t.Fatal("public attempt was not published")
	}
	<-state.stageGate
	body, err := protocol.RequestBytes(91, "message.deliver", kit.DeliveryRequest{
		MessageID: "lost-delivery", From: kit.DeliverySource{SessionID: "sender", Product: "codex-peer", Groups: []string{}}, Body: "held before native stage",
	})
	if err != nil {
		t.Fatal(err)
	}
	ownerWrite(t, public, body)
	deadline := time.After(5 * time.Second)
	for {
		registry.mu.Lock()
		retained := state.retainedBytes
		registry.mu.Unlock()
		if retained > baseline {
			break
		}
		select {
		case <-deadline:
			t.Fatal("public delivery was not retained before loss")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	if err = public.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-attempt.conn.Context().Done():
	case <-ownerTestContext(t).Done():
		t.Fatal("public attempt did not observe daemon loss")
	}
	state.stageGate <- struct{}{}
	select {
	case <-retryEntered:
	case <-ownerTestContext(t).Done():
		t.Fatal("lost delivery attempt did not join before reconnect")
	}
	registry.mu.Lock()
	retained := state.retainedBytes
	staged := len(state.staged)
	registry.mu.Unlock()
	if retained != baseline || staged != 0 {
		t.Fatalf("lost attempt accounting = retained %d, staged %d; want %d, 0", retained, staged, baseline)
	}
	fixture.mu.Lock()
	stages := len(fixture.stages)
	fixture.mu.Unlock()
	if stages != 0 {
		t.Fatalf("lost delivery reached native stage %d times", stages)
	}
}

func TestOwnerRegistrySupersessionIsTerminalBeforeHeldAcknowledgement(t *testing.T) {
	directory := t.TempDir()
	client, server := net.Pipe()
	t.Cleanup(func() { _ = server.Close() })
	fixture := &ownerNativeFixture{descriptions: map[string]ownerDescribeResult{
		"owner-token": {OwnerToken: "owner-token", SessionID: "native-session", Name: "native", CWD: "/work"},
	}}
	registry, native := ownerRegistryPair(t, OwnerRegistryOptions{
		Topology: ownerTopologyInteractive, Socket: filepath.Join(directory, "bus.sock"), Directory: directory,
	}, fixture)
	dials := 0
	registry.dialPublic = func(ctx context.Context, _, _ string) (net.Conn, error) {
		dials++
		if dials == 1 {
			return client, nil
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if err := native.Call(ownerTestContext(t), "owner.ready", ownerReadyRequest{
		Topology: ownerTopologyInteractive, Directory: directory, Scope: ownerScopePrimary, Mode: ownerModeTUI,
		OwnerToken: "owner-token", SessionID: "native-session", Name: "native",
	}, nil); err != nil {
		t.Fatal(err)
	}
	scanner := bufio.NewScanner(server)
	hello := ownerFrame(t, scanner)
	body, err := protocol.ResultBytes(hello.ID, hello.Method, struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	ownerWrite(t, server, body)
	ownerWaitPublished(t, registry, "owner-token")
	body, err = protocol.RequestBytes(90, "session.superseded", struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	ownerWrite(t, server, body)
	select {
	case <-registry.Done():
	case <-ownerTestContext(t).Done():
		t.Fatal("supersession did not retire registry before its acknowledgement was read")
	}
	registry.mu.Lock()
	state := registry.bindings["owner-token"]
	terminal := state != nil && state.terminal && !state.published && state.caller == nil
	registry.mu.Unlock()
	if !terminal {
		t.Fatal("superseded public binding remained admissible")
	}
	if err = registry.Close(); err == nil || !strings.Contains(err.Error(), "superseded") {
		t.Fatalf("superseded registry close = %v", err)
	}
	if dials != 1 {
		t.Fatalf("superseded owner redialed %d times", dials)
	}
}

func TestOwnerRegistryInvalidHelloIsTerminal(t *testing.T) {
	listener := ownerBusListener(t)
	directory := t.TempDir()
	fixture := &ownerNativeFixture{descriptions: map[string]ownerDescribeResult{
		"owner-token": {OwnerToken: "owner-token", SessionID: "native-session", Name: "native", CWD: "/work"},
	}}
	registry, native := ownerRegistryPair(t, OwnerRegistryOptions{
		Topology: ownerTopologyInteractive, Socket: listener.Addr().String(), Directory: directory,
	}, fixture)
	retried := make(chan struct{}, 1)
	registry.retryPublic = func(ctx context.Context) error {
		select {
		case retried <- struct{}{}:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if err := native.Call(ownerTestContext(t), "owner.ready", ownerReadyRequest{
		Topology: ownerTopologyInteractive, Directory: directory, Scope: ownerScopePrimary, Mode: ownerModeTUI,
		OwnerToken: "owner-token", SessionID: "native-session", Name: "native",
	}, nil); err != nil {
		t.Fatal(err)
	}
	public, scanner := ownerAccept(t, listener)
	hello := ownerFrame(t, scanner)
	body, err := protocol.ErrorBytes(hello.ID, protocol.InvalidHello, nil)
	if err != nil {
		t.Fatal(err)
	}
	ownerWrite(t, public, body)
	select {
	case <-registry.Done():
	case <-ownerTestContext(t).Done():
		t.Fatal("invalid hello did not retire the registry")
	}
	select {
	case <-retried:
		t.Fatal("invalid hello entered reconnect backoff")
	default:
	}
	if err = registry.Close(); err == nil || !strings.Contains(err.Error(), "hello was rejected") {
		t.Fatalf("invalid hello close = %v", err)
	}
}

func TestOwnerRegistryRejectsAdmissionAfterRecordedFailure(t *testing.T) {
	directory := t.TempDir()
	registry, _ := ownerRegistryPair(t, OwnerRegistryOptions{
		Topology: ownerTopologyInteractive, Socket: filepath.Join(directory, "bus.sock"), Directory: directory,
	}, &ownerNativeFixture{})
	registry.fail(errors.New("public owner became terminal before admission"))
	err := registry.admit(context.Background(), ownerScopePrimary, ownerModeTUI, ownerDescribeResult{
		OwnerToken: "new-token", SessionID: "new-session", Name: "late", CWD: "/work",
	})
	if err == nil {
		t.Fatal("recorded registry failure admitted a late native owner")
	}
	if binding, present := registry.Primary(); present {
		t.Fatalf("late native owner became primary: %+v", binding)
	}
}

func TestOwnerRegistrySupersessionWinsHeldReplacementDescription(t *testing.T) {
	listener := ownerBusListener(t)
	directory := t.TempDir()
	describeEntered, releaseDescribe := make(chan struct{}), make(chan struct{})
	fixture := &ownerNativeFixture{descriptions: map[string]ownerDescribeResult{
		"old-token": {OwnerToken: "old-token", SessionID: "old-session", Name: "old", CWD: "/work/old"},
		"new-token": {OwnerToken: "new-token", SessionID: "new-session", Name: "new", CWD: "/work/new"},
	}}
	fixture.describeHook = func(request ownerDescribeRequest) {
		if request.OwnerToken == "new-token" {
			close(describeEntered)
			<-releaseDescribe
		}
	}
	registry, native := ownerRegistryPair(t, OwnerRegistryOptions{
		Topology: ownerTopologyInteractive, Socket: listener.Addr().String(), Directory: directory,
	}, fixture)
	_, oldPublic, _ := ownerReady(t, registry, native, listener, ownerReadyRequest{
		Topology: ownerTopologyInteractive, Directory: directory, Scope: ownerScopePrimary, Mode: ownerModeTUI,
		OwnerToken: "old-token", SessionID: "old-session", Name: "old",
	})
	switched := make(chan error, 1)
	go func() {
		switched <- native.Call(ownerTestContext(t), "owner.switch", ownerSwitchRequest{
			Topology: ownerTopologyInteractive, Scope: ownerScopePrimary, Mode: ownerModeTUI,
			PreviousOwnerToken: "old-token", OwnerToken: "new-token",
			PreviousSessionID: "old-session", SessionID: "new-session", Name: "new", Reason: "resume",
		}, nil)
	}()
	select {
	case <-describeEntered:
	case <-ownerTestContext(t).Done():
		t.Fatal("replacement description did not begin")
	}
	body, err := protocol.RequestBytes(92, "session.superseded", struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	ownerWrite(t, oldPublic, body)
	select {
	case <-registry.Done():
	case <-ownerTestContext(t).Done():
		t.Fatal("supersession did not retire registry during replacement description")
	}
	close(releaseDescribe)
	if err = <-switched; err == nil {
		t.Fatal("replacement committed after supersession")
	}
	registry.mu.Lock()
	oldState := registry.bindings["old-token"]
	newState := registry.bindings["new-token"]
	primary := registry.primaryToken
	registry.mu.Unlock()
	if oldState == nil || !oldState.terminal || newState != nil || primary != "old-token" {
		t.Fatalf("terminal replacement boundary = old %+v, new %+v, primary %q", oldState, newState, primary)
	}
}

func TestOwnerRegistryWaitsForNextMatchingPreflightAndConsumesAccounting(t *testing.T) {
	directory := t.TempDir()
	caller := kit.NewCaller(func(context.Context, string, any) (json.RawMessage, error) { return json.RawMessage(`{}`), nil })
	fixture := &ownerNativeFixture{descriptions: map[string]ownerDescribeResult{
		"main-token": {OwnerToken: "main-token", SessionID: "main-session", CWD: "/work/main"},
	}}
	registry, native := ownerRegistryPair(t, OwnerRegistryOptions{
		Topology: ownerTopologyLane, Socket: filepath.Join(directory, "bus.sock"), Directory: directory, PrimaryCaller: caller,
	}, fixture)
	if err := native.Call(ownerTestContext(t), "owner.ready", ownerReadyRequest{
		Topology: ownerTopologyLane, Directory: directory, Scope: ownerScopePrimary, Mode: ownerModeRPC,
		OwnerToken: "main-token", SessionID: "main-session",
	}, nil); err != nil {
		t.Fatal(err)
	}
	type outcome struct {
		token string
		err   error
	}
	result := make(chan outcome, 1)
	go func() {
		token, err := registry.waitPreflight(ownerTestContext(t), "main-token", "main-session", "owned prompt")
		result <- outcome{token, err}
	}()
	if _, err := registry.recordPreflight(mustOwnerJSON(t, ownerPreflightRequest{
		OwnerToken: "main-token", SessionID: "main-session", ReportSequence: 1,
		RunToken: "run-one", Prompt: "owned prompt",
	})); err != nil {
		t.Fatal(err)
	}
	got := <-result
	if got.err != nil || got.token != "run-one" {
		t.Fatalf("preflight wait = %#v", got)
	}
	registry.mu.Lock()
	state := registry.bindings["main-token"]
	if len(state.preflights) != 0 || len(state.preflightOrder) != 0 || len(state.consumedPreflights) != 1 || state.consumedPreflights[0] != "run-one" {
		t.Fatalf("preflight evidence = pending %v/%v, consumed %v", state.preflights, state.preflightOrder, state.consumedPreflights)
	}
	registry.mu.Unlock()
}

func TestOwnerRegistryPreflightWaitDoesNotSearchPastForeignWitness(t *testing.T) {
	directory := t.TempDir()
	caller := kit.NewCaller(func(context.Context, string, any) (json.RawMessage, error) { return json.RawMessage(`{}`), nil })
	fixture := &ownerNativeFixture{descriptions: map[string]ownerDescribeResult{
		"main-token": {OwnerToken: "main-token", SessionID: "main-session", CWD: "/work/main"},
	}}
	registry, native := ownerRegistryPair(t, OwnerRegistryOptions{
		Topology: ownerTopologyLane, Socket: filepath.Join(directory, "bus.sock"), Directory: directory, PrimaryCaller: caller,
	}, fixture)
	if err := native.Call(ownerTestContext(t), "owner.ready", ownerReadyRequest{
		Topology: ownerTopologyLane, Directory: directory, Scope: ownerScopePrimary, Mode: ownerModeRPC,
		OwnerToken: "main-token", SessionID: "main-session",
	}, nil); err != nil {
		t.Fatal(err)
	}
	for sequence, value := range []struct{ token, prompt string }{{"foreign-run", "foreign"}, {"owned-run", "owned"}} {
		if _, err := registry.recordPreflight(mustOwnerJSON(t, ownerPreflightRequest{
			OwnerToken: "main-token", SessionID: "main-session", ReportSequence: uint64(sequence + 1),
			RunToken: value.token, Prompt: value.prompt,
		})); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := registry.waitPreflight(ownerTestContext(t), "main-token", "main-session", "owned"); err == nil {
		t.Fatal("foreign leading preflight was accepted")
	}
	if registry.Err() == nil {
		t.Fatal("foreign leading preflight did not retire the registry")
	}
}

func TestOwnerRegistryPreflightWaitWakesOnCancellationAndOwnerEnd(t *testing.T) {
	for _, ending := range []bool{false, true} {
		t.Run(fmt.Sprintf("ending-%v", ending), func(t *testing.T) {
			directory := t.TempDir()
			caller := kit.NewCaller(func(context.Context, string, any) (json.RawMessage, error) { return json.RawMessage(`{}`), nil })
			fixture := &ownerNativeFixture{descriptions: map[string]ownerDescribeResult{
				"main-token": {OwnerToken: "main-token", SessionID: "main-session", CWD: "/work/main"},
			}}
			registry, native := ownerRegistryPair(t, OwnerRegistryOptions{
				Topology: ownerTopologyLane, Socket: filepath.Join(directory, "bus.sock"), Directory: directory, PrimaryCaller: caller,
			}, fixture)
			if err := native.Call(ownerTestContext(t), "owner.ready", ownerReadyRequest{
				Topology: ownerTopologyLane, Directory: directory, Scope: ownerScopePrimary, Mode: ownerModeRPC,
				OwnerToken: "main-token", SessionID: "main-session",
			}, nil); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			result := make(chan error, 1)
			go func() {
				_, err := registry.waitPreflight(ctx, "main-token", "main-session", "owned")
				result <- err
			}()
			if ending {
				if err := native.Call(ownerTestContext(t), "session_end", ownerEndRequest{
					Topology: ownerTopologyLane, Scope: ownerScopePrimary, Mode: ownerModeRPC,
					OwnerToken: "main-token", SessionID: "main-session", Reason: "shutdown",
				}, nil); err != nil {
					t.Fatal(err)
				}
			} else {
				cancel()
			}
			if err := <-result; err == nil {
				t.Fatal("preflight wait did not report cancellation or owner end")
			}
			cancel()
		})
	}
}

func TestOwnerRegistryCanceledPreflightWaitPreservesArrivedEvidence(t *testing.T) {
	directory := t.TempDir()
	caller := kit.NewCaller(func(context.Context, string, any) (json.RawMessage, error) { return json.RawMessage(`{}`), nil })
	fixture := &ownerNativeFixture{descriptions: map[string]ownerDescribeResult{
		"main-token": {OwnerToken: "main-token", SessionID: "main-session", CWD: "/work/main"},
	}}
	registry, native := ownerRegistryPair(t, OwnerRegistryOptions{
		Topology: ownerTopologyLane, Socket: filepath.Join(directory, "bus.sock"), Directory: directory, PrimaryCaller: caller,
	}, fixture)
	if err := native.Call(ownerTestContext(t), "owner.ready", ownerReadyRequest{
		Topology: ownerTopologyLane, Directory: directory, Scope: ownerScopePrimary, Mode: ownerModeRPC,
		OwnerToken: "main-token", SessionID: "main-session",
	}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.recordPreflight(mustOwnerJSON(t, ownerPreflightRequest{
		OwnerToken: "main-token", SessionID: "main-session", ReportSequence: 1,
		RunToken: "run-one", Prompt: "owned prompt",
	})); err != nil {
		t.Fatal(err)
	}
	registry.mu.Lock()
	state := registry.bindings["main-token"]
	retained := state.retainedBytes
	registry.mu.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := registry.waitPreflight(ctx, "main-token", "main-session", "owned prompt"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled preflight wait = %v", err)
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if state.retainedBytes != retained || state.preflights["run-one"] != "owned prompt" ||
		!reflect.DeepEqual(state.preflightOrder, []string{"run-one"}) {
		t.Fatalf("canceled evidence = retained %d/%d, preflights %v, order %v", state.retainedBytes, retained, state.preflights, state.preflightOrder)
	}
}

func TestOwnerRegistryFailedPreflightWaitPreservesArrivedEvidence(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		t.Run(fmt.Sprintf("canceled-%v", canceled), func(t *testing.T) {
			directory := t.TempDir()
			caller := kit.NewCaller(func(context.Context, string, any) (json.RawMessage, error) { return json.RawMessage(`{}`), nil })
			fixture := &ownerNativeFixture{descriptions: map[string]ownerDescribeResult{
				"main-token": {OwnerToken: "main-token", SessionID: "main-session", CWD: "/work/main"},
			}}
			registry, native := ownerRegistryPair(t, OwnerRegistryOptions{
				Topology: ownerTopologyLane, Socket: filepath.Join(directory, "bus.sock"), Directory: directory, PrimaryCaller: caller,
			}, fixture)
			if err := native.Call(ownerTestContext(t), "owner.ready", ownerReadyRequest{
				Topology: ownerTopologyLane, Directory: directory, Scope: ownerScopePrimary, Mode: ownerModeRPC,
				OwnerToken: "main-token", SessionID: "main-session",
			}, nil); err != nil {
				t.Fatal(err)
			}
			if _, err := registry.recordPreflight(mustOwnerJSON(t, ownerPreflightRequest{
				OwnerToken: "main-token", SessionID: "main-session", ReportSequence: 1,
				RunToken: "run-one", Prompt: "owned prompt",
			})); err != nil {
				t.Fatal(err)
			}
			registry.mu.Lock()
			state := registry.bindings["main-token"]
			retained := state.retainedBytes
			registry.mu.Unlock()
			failure := errors.New("controlled registry loss")
			registry.recordError(failure)
			if canceled {
				registry.cancel()
			}
			if _, err := registry.waitPreflight(ownerTestContext(t), "main-token", "main-session", "owned prompt"); err == nil || !strings.Contains(err.Error(), failure.Error()) {
				t.Fatalf("failed preflight wait = %v", err)
			}
			registry.mu.Lock()
			defer registry.mu.Unlock()
			if state.retainedBytes != retained || state.preflights["run-one"] != "owned prompt" ||
				!reflect.DeepEqual(state.preflightOrder, []string{"run-one"}) {
				t.Fatalf("failed evidence = retained %d/%d, preflights %v, order %v", state.retainedBytes, retained, state.preflights, state.preflightOrder)
			}
		})
	}
}

func TestOwnerRegistryBoundsReorderedReportPayloadBytes(t *testing.T) {
	directory := t.TempDir()
	caller := kit.NewCaller(func(context.Context, string, any) (json.RawMessage, error) { return json.RawMessage(`{}`), nil })
	fixture := &ownerNativeFixture{descriptions: map[string]ownerDescribeResult{
		"main-token": {OwnerToken: "main-token", SessionID: "main-session", CWD: "/work/main"},
	}}
	registry, native := ownerRegistryPair(t, OwnerRegistryOptions{
		Topology: ownerTopologyLane, Socket: filepath.Join(directory, "bus.sock"), Directory: directory, PrimaryCaller: caller,
	}, fixture)
	if err := native.Call(ownerTestContext(t), "owner.ready", ownerReadyRequest{
		Topology: ownerTopologyLane, Directory: directory, Scope: ownerScopePrimary, Mode: ownerModeRPC,
		OwnerToken: "main-token", SessionID: "main-session",
	}, nil); err != nil {
		t.Fatal(err)
	}
	prompt := strings.Repeat("x", maxOwnerTextBytes)
	accepted := 0
	for index := 0; index < maxOwnerRetainedBytes/maxOwnerTextBytes+2; index++ {
		_, err := registry.recordPreflight(mustOwnerJSON(t, ownerPreflightRequest{
			OwnerToken: "main-token", SessionID: "main-session", ReportSequence: uint64(index + 2),
			RunToken: fmt.Sprintf("run-%d", index), Prompt: prompt,
		}))
		if err != nil {
			break
		}
		accepted++
	}
	if accepted == 0 || accepted >= maxOwnerRetainedBytes/maxOwnerTextBytes+2 {
		t.Fatalf("reordered reports accepted before byte bound = %d", accepted)
	}
	registry.mu.Lock()
	retained := registry.retainedBytes
	registry.mu.Unlock()
	if retained > maxOwnerRetainedBytes {
		t.Fatalf("retained report payload bytes = %d", retained)
	}
	select {
	case <-registry.Done():
	case <-ownerTestContext(t).Done():
		t.Fatal("retained report byte overflow did not retire registry")
	}
}

func TestOwnerRegistryShutdownIsPrimaryRequestOnly(t *testing.T) {
	directory := t.TempDir()
	caller := kit.NewCaller(func(context.Context, string, any) (json.RawMessage, error) { return json.RawMessage(`{}`), nil })
	fixture := &ownerNativeFixture{descriptions: map[string]ownerDescribeResult{
		"main-token": {OwnerToken: "main-token", SessionID: "main-session", CWD: "/work/main"},
	}}
	registry, native := ownerRegistryPair(t, OwnerRegistryOptions{
		Topology: ownerTopologyLane, Socket: filepath.Join(directory, "bus.sock"), Directory: directory, PrimaryCaller: caller,
	}, fixture)
	if err := native.Call(ownerTestContext(t), "owner.ready", ownerReadyRequest{
		Topology: ownerTopologyLane, Directory: directory, Scope: ownerScopePrimary, Mode: ownerModeRPC,
		OwnerToken: "main-token", SessionID: "main-session",
	}, nil); err != nil {
		t.Fatal(err)
	}
	if err := registry.shutdownPrimary(ownerTestContext(t)); err != nil {
		t.Fatal(err)
	}
	fixture.mu.Lock()
	shutdowns := slices.Clone(fixture.shutdowns)
	fixture.mu.Unlock()
	if !reflect.DeepEqual(shutdowns, []ownerDescribeRequest{{OwnerToken: "main-token", SessionID: "main-session"}}) {
		t.Fatalf("native shutdowns = %+v", shutdowns)
	}
	if err := native.Call(ownerTestContext(t), "session_end", ownerEndRequest{
		Topology: ownerTopologyLane, Scope: ownerScopePrimary, Mode: ownerModeRPC,
		OwnerToken: "main-token", SessionID: "main-session", Reason: "quit",
	}, nil); err != nil {
		t.Fatal(err)
	}
	if err := registry.shutdownPrimary(ownerTestContext(t)); err == nil {
		t.Fatal("shutdown without primary was accepted")
	}
}
