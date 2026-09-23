// SPDX-License-Identifier: MIT

package pi

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	kit "github.com/antst/sessionbus/bus/sdk/go"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
	"github.com/sessionbus/peer-common/host"
	"github.com/sessionbus/peer-common/testsocket"
	"github.com/sessionbus/pi-omp/wrappers/pifamily"
)

type interactiveNativeFixture struct {
	mu      sync.Mutex
	id      string
	name    string
	cwd     string
	replies []bool
	reasons []string
	appends []interactiveAppendRequest
}

type heldSupersededAckConn struct {
	net.Conn
	entered chan struct{}
	release chan struct{}
	closed  chan struct{}
	once    sync.Once
	close   sync.Once
}

func (c *heldSupersededAckConn) Write(body []byte) (int, error) {
	frame, err := protocol.DecodeFrame(bytes.TrimSpace(body))
	if err == nil && !frame.Request && frame.ID == 81 {
		c.once.Do(func() { close(c.entered) })
		select {
		case <-c.release:
		case <-c.closed:
			return 0, net.ErrClosed
		}
	}
	return c.Conn.Write(body)
}

func (c *heldSupersededAckConn) Close() error {
	c.close.Do(func() { close(c.closed) })
	return c.Conn.Close()
}

func (f *interactiveNativeFixture) set(id, name, cwd string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.id, f.name, f.cwd = id, name, cwd
}

func (f *interactiveNativeFixture) handle(_ context.Context, method string, raw json.RawMessage) (json.RawMessage, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var result any
	switch method {
	case "native.describe":
		var request struct {
			SessionID string `json:"session_id"`
		}
		if err := decodeInteractiveParams(raw, &request); err != nil || request.SessionID != f.id {
			return nil, pifamily.NewBridgeCallError("bad_request", "wrong native description")
		}
		result = interactiveDescribeResult{f.id, f.name, f.cwd}
	case "native.append":
		var request interactiveAppendRequest
		if err := decodeInteractiveParams(raw, &request); err != nil || request.SessionID != f.id {
			return nil, pifamily.NewBridgeCallError("bad_request", "wrong native append")
		}
		f.appends = append(f.appends, request)
		accepted := true
		if len(f.replies) > 0 {
			accepted, f.replies = f.replies[0], f.replies[1:]
		}
		if accepted {
			result = interactiveAppendResult{SessionID: f.id, MessageID: request.MessageID, Accepted: true}
		} else {
			reason := "busy"
			if len(f.reasons) > 0 {
				reason, f.reasons = f.reasons[0], f.reasons[1:]
			}
			result = interactiveAppendResult{SessionID: f.id, MessageID: request.MessageID, Reason: reason}
		}
	default:
		return nil, pifamily.NewBridgeCallError("method_not_found", "unexpected native method")
	}
	return json.Marshal(result)
}

func interactiveOwnerPair(t *testing.T, socket, directory, initialName string, groups []string, fixture *interactiveNativeFixture) (*interactiveOwner, *pifamily.Bridge) {
	t.Helper()
	left, right := net.Pipe()
	owner, err := newInteractiveOwner(context.Background(), socket, directory, initialName, groups)
	if err != nil {
		t.Fatal(err)
	}
	assigned := make(chan struct{})
	hostBridge, err := pifamily.NewBridge(left, pifamily.BridgeHost, func(ctx context.Context, method string, raw json.RawMessage) (json.RawMessage, error) {
		<-assigned
		return owner.handleBridge(ctx, method, raw)
	}, pifamily.BridgeLimits{})
	if err != nil {
		t.Fatal(err)
	}
	if err = owner.assignBridge(hostBridge); err != nil {
		t.Fatal(err)
	}
	close(assigned)
	native, err := pifamily.NewBridge(right, pifamily.BridgeNative, fixture.handle, pifamily.BridgeLimits{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err = hostBridge.Ready(ctx); err != nil {
		t.Fatal(err)
	}
	if err = native.Ready(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = owner.Close()
		_ = hostBridge.Close()
		_ = native.Close()
	})
	return owner, native
}

func interactiveBusListener(t *testing.T) net.Listener {
	t.Helper()
	path := filepath.Join(testsocket.Directory(t), "bus.sock")
	return interactiveBusListenerAt(t, path)
}

func interactiveBusListenerAt(t *testing.T, path string) net.Listener {
	t.Helper()
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	return listener
}

func interactiveAccept(t *testing.T, listener net.Listener) net.Conn {
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
	return conn
}

func interactiveFrame(t *testing.T, scanner *bufio.Scanner) protocol.Frame {
	t.Helper()
	if !scanner.Scan() {
		t.Fatalf("missing frame: %v", scanner.Err())
	}
	frame, err := protocol.DecodeFrame(scanner.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	return frame
}

func interactiveWrite(t *testing.T, conn net.Conn, body []byte) {
	t.Helper()
	if _, err := conn.Write(body); err != nil {
		t.Fatal(err)
	}
}

func interactiveHello(t *testing.T, conn net.Conn, scanner *bufio.Scanner) kit.PeerIdentity {
	t.Helper()
	hello, frame := interactiveHelloRequest(t, scanner)
	body, err := protocol.ResultBytes(frame.ID, frame.Method, struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	interactiveWrite(t, conn, body)
	return hello
}

func interactiveHelloRequest(t *testing.T, scanner *bufio.Scanner) (kit.PeerIdentity, protocol.Frame) {
	t.Helper()
	frame := interactiveFrame(t, scanner)
	if !frame.Request || frame.Method != "session.hello" {
		t.Fatalf("hello frame = %+v", frame)
	}
	params, err := protocol.DecodeParams(frame.Method, frame.Params)
	if err != nil {
		t.Fatal(err)
	}
	hello, ok := params.(*protocol.PeerHello)
	if !ok {
		t.Fatalf("hello params = %T", params)
	}
	return *hello, frame
}

func interactiveReady(t *testing.T, native *pifamily.Bridge, listener net.Listener, request interactiveReadyRequest) (kit.PeerIdentity, net.Conn, *bufio.Scanner) {
	t.Helper()
	done := make(chan error, 1)
	go func() {
		var result struct {
			SessionID string `json:"session_id"`
		}
		err := native.Call(context.Background(), "owner.ready", request, &result)
		if err == nil && result.SessionID != request.SessionID {
			err = context.Canceled
		}
		done <- err
	}()
	conn := interactiveAccept(t, listener)
	scanner := bufio.NewScanner(conn)
	hello := interactiveHello(t, conn, scanner)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("owner.ready did not finish")
	}
	return hello, conn, scanner
}

func interactiveReadyCall(native *pifamily.Bridge, request interactiveReadyRequest) <-chan error {
	done := make(chan error, 1)
	go func() {
		var result struct {
			SessionID string `json:"session_id"`
		}
		err := native.Call(context.Background(), "owner.ready", request, &result)
		if err == nil && result.SessionID != request.SessionID {
			err = errors.New("owner.ready changed session identity")
		}
		done <- err
	}()
	return done
}

func interactiveToolCall(native *pifamily.Bridge, sessionID, callID string) <-chan error {
	done := make(chan error, 1)
	go func() {
		var result json.RawMessage
		done <- native.Call(context.Background(), "tool.call", interactiveToolRequest{SessionID: sessionID, CallID: callID, Action: "list", Arguments: json.RawMessage(`{}`)}, &result)
	}()
	return done
}

func awaitInteractiveSignal(t *testing.T, signal <-chan struct{}, label string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", label)
	}
}

func awaitInteractiveError(t *testing.T, result <-chan error, label string) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", label)
		return context.DeadlineExceeded
	}
}

func awaitInteractiveFrame(t *testing.T, frames <-chan protocol.Frame, label string) protocol.Frame {
	t.Helper()
	select {
	case frame, ok := <-frames:
		if !ok {
			t.Fatalf("connection ended while waiting for %s", label)
		}
		return frame
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", label)
		return protocol.Frame{}
	}
}

func awaitInteractiveDisconnected(t *testing.T, owner *interactiveOwner) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		owner.mu.Lock()
		disconnected := owner.public != nil && owner.conn == nil && owner.connecting == nil
		owner.mu.Unlock()
		if disconnected {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("Pi public owner did not enter disconnected state")
}

func awaitInteractiveConnected(t *testing.T, owner *interactiveOwner, conn net.Conn) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		owner.mu.Lock()
		connected := owner.conn != nil && owner.connecting == nil
		owner.mu.Unlock()
		if connected {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("Pi public owner did not admit replacement connection %v", conn.RemoteAddr())
}

func TestInteractiveOwnerPublishesRebindsAndRoutesExactToolIdentity(t *testing.T) {
	listener := interactiveBusListener(t)
	nativeState := &interactiveNativeFixture{}
	owner, native := interactiveOwnerPair(t, listener.Addr().String(), t.TempDir(), "wrapper fallback", []string{"team"}, nativeState)
	nativeState.set("native-one", "renamed during ready", "/work/one")
	hello, first, firstScanner := interactiveReady(t, native, listener, interactiveReadyRequest{interactiveTopology, owner.directory, "native-one", "stale event title"})
	if hello.Product != Product || hello.SessionID != "native-one" || hello.Name != "renamed during ready" ||
		!reflect.DeepEqual(hello.Groups, []string{"team"}) || hello.Info["cwd"] != "/work/one" {
		t.Fatalf("first hello = %+v", hello)
	}

	toolDone := make(chan struct {
		result json.RawMessage
		err    error
	}, 1)
	go func() {
		var result json.RawMessage
		err := native.Call(context.Background(), "tool.call", interactiveToolRequest{
			SessionID: "native-one", CallID: "native-call", Action: "list", Arguments: json.RawMessage(`{}`),
		}, &result)
		toolDone <- struct {
			result json.RawMessage
			err    error
		}{result, err}
	}()
	call := interactiveFrame(t, firstScanner)
	if !call.Request || call.Method != "session.list" {
		t.Fatalf("public call = %+v", call)
	}
	list := kit.SessionListResult{SelfInfo: &kit.SessionSelfInfo{SessionID: "native-one", Product: Product, Groups: []string{"team"}}, Sessions: []kit.SessionSummary{}}
	body, err := protocol.ResultBytes(call.ID, call.Method, list)
	if err != nil {
		t.Fatal(err)
	}
	interactiveWrite(t, first, body)
	tool := <-toolDone
	if tool.err != nil {
		t.Fatal(tool.err)
	}
	var routed struct {
		SessionID string                `json:"session_id"`
		CallID    string                `json:"call_id"`
		Result    kit.SessionListResult `json:"result"`
	}
	if json.Unmarshal(tool.result, &routed) != nil || routed.SessionID != "native-one" || routed.CallID != "native-call" ||
		routed.Result.SelfInfo == nil || routed.Result.SelfInfo.SessionID != "native-one" {
		t.Fatalf("tool result = %s", tool.result)
	}

	var ended map[string]string
	if err = native.Call(context.Background(), "session_end", interactiveEndRequest{interactiveTopology, "native-one", "resume"}, &ended); err != nil || ended["session_id"] != "native-one" {
		t.Fatalf("session_end = %#v, %v", ended, err)
	}
	nativeState.set("native-two", "", "/work/two")
	hello, _, _ = interactiveReady(t, native, listener, interactiveReadyRequest{interactiveTopology, owner.directory, "native-two", ""})
	if hello.SessionID != "native-two" || hello.Name != "wrapper fallback" || hello.Info["cwd"] != "/work/two" {
		t.Fatalf("replacement hello = %+v", hello)
	}
}

func TestInteractiveOwnerReconnectsLatestIdentityWithoutReplayAndSessionEndStopsOldLifetime(t *testing.T) {
	root := testsocket.Directory(t)
	socket := filepath.Join(root, "bus.sock")
	originalDial, originalInterval := piInteractivePublicDial, piInteractiveReconnectInterval
	attempted := make(chan struct{}, 64)
	piInteractivePublicDial = func(ctx context.Context, network, address string) (net.Conn, error) {
		attempted <- struct{}{}
		return originalDial(ctx, network, address)
	}
	piInteractiveReconnectInterval = 10 * time.Millisecond
	t.Cleanup(func() {
		piInteractivePublicDial, piInteractiveReconnectInterval = originalDial, originalInterval
	})
	nativeState := &interactiveNativeFixture{}
	owner, native := interactiveOwnerPair(t, socket, root, "fallback", []string{"team"}, nativeState)
	nativeState.set("native-one", "before outage", "/work/one")
	readyDone := interactiveReadyCall(native, interactiveReadyRequest{interactiveTopology, owner.directory, "native-one", "event title"})
	awaitInteractiveSignal(t, attempted, "initial public dial")
	select {
	case <-owner.Ready():
		t.Fatal("owner became ready while Sessionbus was absent")
	case <-owner.Done():
		t.Fatalf("owner ended while Sessionbus was absent: %v", owner.Err())
	default:
	}

	firstListener := interactiveBusListenerAt(t, socket)
	first := interactiveAccept(t, firstListener)
	firstScanner := bufio.NewScanner(first)
	firstHello := interactiveHello(t, first, firstScanner)
	if firstHello.SessionID != "native-one" || firstHello.Name != "before outage" || firstHello.Info["cwd"] != "/work/one" {
		t.Fatalf("first hello = %+v", firstHello)
	}
	if err := awaitInteractiveError(t, readyDone, "first owner.ready"); err != nil {
		t.Fatal(err)
	}
	awaitInteractiveSignal(t, owner.Ready(), "first owner admission")
	owner.mu.Lock()
	oldPublic := owner.public
	owner.mu.Unlock()
	lostCall := interactiveToolCall(native, "native-one", "lost-call")
	lostFrame := interactiveFrame(t, firstScanner)
	if !lostFrame.Request || lostFrame.Method != "session.list" {
		t.Fatalf("lost public call = %+v", lostFrame)
	}

	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if err := firstListener.Close(); err != nil {
		t.Fatal(err)
	}
	if err := awaitInteractiveError(t, lostCall, "lost public call"); err == nil {
		t.Fatal("in-flight public call survived its lost connection")
	}
	awaitInteractiveDisconnected(t, owner)
	nativeState.set("native-one", "after outage", "/work/two")
	if err := awaitInteractiveError(t, interactiveReadyCall(native, interactiveReadyRequest{interactiveTopology, owner.directory, "native-one", "after outage"}), "outage identity update"); err != nil {
		t.Fatal(err)
	}
	toolDuringOutage := interactiveToolCall(native, "native-one", "offline-call")
	if err := awaitInteractiveError(t, toolDuringOutage, "outage tool call"); err == nil || !strings.Contains(err.Error(), "not_connected") {
		t.Fatalf("outage tool call = %v", err)
	}

	secondListener := interactiveBusListenerAt(t, socket)
	second := interactiveAccept(t, secondListener)
	secondScanner := bufio.NewScanner(second)
	secondHello, secondHelloFrame := interactiveHelloRequest(t, secondScanner)
	if secondHello.SessionID != "native-one" || secondHello.Name != "after outage" || secondHello.Info["cwd"] != "/work/two" || !reflect.DeepEqual(secondHello.Groups, firstHello.Groups) {
		t.Fatalf("replacement hello = %+v", secondHello)
	}
	nativeState.set("native-one", "during hello", "/work/three")
	if err := awaitInteractiveError(t, interactiveReadyCall(native, interactiveReadyRequest{interactiveTopology, owner.directory, "native-one", "during hello"}), "held-hello identity update"); err != nil {
		t.Fatal(err)
	}
	body, err := protocol.ResultBytes(secondHelloFrame.ID, secondHelloFrame.Method, struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	interactiveWrite(t, second, body)
	latestHello := interactiveHello(t, second, secondScanner)
	if latestHello.SessionID != "native-one" || latestHello.Name != "during hello" || latestHello.Info["cwd"] != "/work/three" || !reflect.DeepEqual(latestHello.Groups, firstHello.Groups) {
		t.Fatalf("latest rehello = %+v", latestHello)
	}
	awaitInteractiveConnected(t, owner, second)
	frames := make(chan protocol.Frame, 2)
	go func() {
		for secondScanner.Scan() {
			frame, decodeErr := protocol.DecodeFrame(secondScanner.Bytes())
			if decodeErr == nil {
				frames <- frame
			}
		}
		close(frames)
	}()
	select {
	case frame := <-frames:
		t.Fatalf("outage call replayed after reconnect: %+v", frame)
	case <-time.After(20 * time.Millisecond):
	}

	toolAfterReconnect := interactiveToolCall(native, "native-one", "online-call")
	frame := awaitInteractiveFrame(t, frames, "post-reconnect public call")
	if !frame.Request || frame.Method != "session.list" {
		t.Fatalf("post-reconnect public frame = %+v", frame)
	}
	listed := kit.SessionListResult{
		SelfInfo: &kit.SessionSelfInfo{SessionID: "native-one", Product: Product, Groups: []string{"team"}},
		Sessions: []kit.SessionSummary{},
	}
	body, err = protocol.ResultBytes(frame.ID, frame.Method, listed)
	if err != nil {
		t.Fatal(err)
	}
	interactiveWrite(t, second, body)
	if err = awaitInteractiveError(t, toolAfterReconnect, "post-reconnect tool call"); err != nil {
		t.Fatal(err)
	}

	var ended map[string]string
	if err = native.Call(context.Background(), "session_end", interactiveEndRequest{interactiveTopology, "native-one", "new"}, &ended); err != nil || ended["session_id"] != "native-one" {
		t.Fatalf("session_end = %#v, %v", ended, err)
	}
	owner.mu.Lock()
	if owner.public != nil || owner.conn != nil || owner.connecting != nil || owner.sessionID != "" {
		t.Fatalf("ended public lifetime retained state: public=%p conn=%p connecting=%p session=%q", owner.public, owner.conn, owner.connecting, owner.sessionID)
	}
	owner.mu.Unlock()
	if _, err = owner.callPublic(context.Background(), "native-one", oldPublic, "session.list", kit.SessionListRequest{}); err == nil || !strings.Contains(err.Error(), "current session") {
		t.Fatalf("ended lifetime call = %v", err)
	}

	nativeState.set("native-two", "replacement", "/work/four")
	replacementDone := interactiveReadyCall(native, interactiveReadyRequest{interactiveTopology, owner.directory, "native-two", "replacement"})
	third := interactiveAccept(t, secondListener)
	thirdHello := interactiveHello(t, third, bufio.NewScanner(third))
	if thirdHello.SessionID != "native-two" || thirdHello.Name != "replacement" || thirdHello.Info["cwd"] != "/work/four" {
		t.Fatalf("new-session hello = %+v", thirdHello)
	}
	if err = awaitInteractiveError(t, replacementDone, "replacement owner.ready"); err != nil {
		t.Fatal(err)
	}
}

func TestInteractiveOwnerSupersededIsTerminal(t *testing.T) {
	listener := interactiveBusListener(t)
	nativeState := &interactiveNativeFixture{}
	owner, native := interactiveOwnerPair(t, listener.Addr().String(), t.TempDir(), "", []string{"team"}, nativeState)
	nativeState.set("native-one", "title", "/work")
	_, bus, scanner := interactiveReady(t, native, listener, interactiveReadyRequest{interactiveTopology, owner.directory, "native-one", "title"})
	body, err := protocol.RequestBytes(81, "session.superseded", struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	interactiveWrite(t, bus, body)
	frame := interactiveFrame(t, scanner)
	if frame.Request || frame.ID != 81 || frame.Error != nil {
		t.Fatalf("superseded response = %+v", frame)
	}
	awaitInteractiveSignal(t, owner.Done(), "superseded owner termination")
	if err = owner.Err(); err == nil || !strings.Contains(err.Error(), "superseded") {
		t.Fatalf("superseded owner error = %v", err)
	}
}

func TestInteractiveOwnerSupersededOwnsTerminationBeforeHeldAck(t *testing.T) {
	listener := interactiveBusListener(t)
	originalDial, originalInterval := piInteractivePublicDial, piInteractiveReconnectInterval
	ackEntered, ackRelease := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	releaseAck := func() { releaseOnce.Do(func() { close(ackRelease) }) }
	dials := make(chan struct{}, 8)
	piInteractivePublicDial = func(ctx context.Context, network, address string) (net.Conn, error) {
		conn, err := originalDial(ctx, network, address)
		if err != nil {
			return nil, err
		}
		dials <- struct{}{}
		return &heldSupersededAckConn{Conn: conn, entered: ackEntered, release: ackRelease, closed: make(chan struct{})}, nil
	}
	piInteractiveReconnectInterval = 10 * time.Millisecond
	t.Cleanup(func() {
		releaseAck()
		piInteractivePublicDial, piInteractiveReconnectInterval = originalDial, originalInterval
	})
	nativeState := &interactiveNativeFixture{}
	owner, native := interactiveOwnerPair(t, listener.Addr().String(), t.TempDir(), "", []string{"team"}, nativeState)
	nativeState.set("native-one", "title", "/work")
	_, bus, scanner := interactiveReady(t, native, listener, interactiveReadyRequest{interactiveTopology, owner.directory, "native-one", "title"})
	awaitInteractiveSignal(t, dials, "first public dial")
	body, err := protocol.RequestBytes(81, "session.superseded", struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	interactiveWrite(t, bus, body)
	awaitInteractiveSignal(t, ackEntered, "held supersession acknowledgement")
	owner.mu.Lock()
	terminal := owner.ending
	ownerErr := owner.err
	owner.mu.Unlock()
	if !terminal || ownerErr == nil || !strings.Contains(ownerErr.Error(), "superseded") {
		t.Fatalf("supersession did not synchronously own termination: ending=%t err=%v", terminal, ownerErr)
	}
	nativeState.set("native-two", "replacement", "/replacement")
	if err = awaitInteractiveError(t, interactiveReadyCall(native, interactiveReadyRequest{interactiveTopology, owner.directory, "native-two", "replacement"}), "late replacement"); err == nil {
		t.Fatal("replacement owner.ready crossed held supersession acknowledgement")
	}
	select {
	case <-dials:
		t.Fatal("superseded public identity attempted reconnect before its acknowledgement")
	case <-time.After(30 * time.Millisecond):
	}
	releaseAck()
	frame := interactiveFrame(t, scanner)
	if frame.Request || frame.ID != 81 || frame.Error != nil {
		t.Fatalf("superseded response = %+v", frame)
	}
	awaitInteractiveSignal(t, owner.Done(), "superseded owner termination")
}

func TestInteractiveOwnerCloseJoinsHeldSupersededAck(t *testing.T) {
	listener := interactiveBusListener(t)
	originalDial := piInteractivePublicDial
	ackEntered, ackRelease := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	releaseAck := func() { releaseOnce.Do(func() { close(ackRelease) }) }
	piInteractivePublicDial = func(ctx context.Context, network, address string) (net.Conn, error) {
		conn, err := originalDial(ctx, network, address)
		if err != nil {
			return nil, err
		}
		return &heldSupersededAckConn{Conn: conn, entered: ackEntered, release: ackRelease, closed: make(chan struct{})}, nil
	}
	t.Cleanup(func() {
		releaseAck()
		piInteractivePublicDial = originalDial
	})
	nativeState := &interactiveNativeFixture{}
	owner, native := interactiveOwnerPair(t, listener.Addr().String(), t.TempDir(), "", []string{"team"}, nativeState)
	nativeState.set("native-one", "title", "/work")
	_, bus, _ := interactiveReady(t, native, listener, interactiveReadyRequest{interactiveTopology, owner.directory, "native-one", "title"})
	body, err := protocol.RequestBytes(81, "session.superseded", struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	interactiveWrite(t, bus, body)
	awaitInteractiveSignal(t, ackEntered, "held supersession acknowledgement")
	closed := make(chan error, 1)
	go func() { closed <- owner.Close() }()
	select {
	case err = <-closed:
		if err == nil || !strings.Contains(err.Error(), "superseded") {
			t.Fatalf("Close error omitted supersession: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not unblock and join held supersession acknowledgement")
	}
}

func TestInteractiveOwnerIgnoresSupersededBeforeHelloAdmission(t *testing.T) {
	root := testsocket.Directory(t)
	socket := filepath.Join(root, "bus.sock")
	originalInterval := piInteractiveReconnectInterval
	piInteractiveReconnectInterval = 10 * time.Millisecond
	t.Cleanup(func() { piInteractiveReconnectInterval = originalInterval })
	listener := interactiveBusListenerAt(t, socket)
	nativeState := &interactiveNativeFixture{}
	owner, native := interactiveOwnerPair(t, socket, root, "", []string{"team"}, nativeState)
	nativeState.set("native-one", "title", "/work")
	readyDone := interactiveReadyCall(native, interactiveReadyRequest{interactiveTopology, owner.directory, "native-one", "title"})
	stale := interactiveAccept(t, listener)
	staleScanner := bufio.NewScanner(stale)
	hello, _ := interactiveHelloRequest(t, staleScanner)
	if hello.SessionID != "native-one" {
		t.Fatalf("unadmitted hello = %+v", hello)
	}
	body, err := protocol.RequestBytes(81, "session.superseded", struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	interactiveWrite(t, stale, body)
	select {
	case <-owner.Done():
		t.Fatalf("stale pre-admission superseded retired owner: %v", owner.Err())
	case <-time.After(20 * time.Millisecond):
	}
	current := interactiveAccept(t, listener)
	currentHello := interactiveHello(t, current, bufio.NewScanner(current))
	if currentHello.SessionID != "native-one" || currentHello.Name != "title" || currentHello.Info["cwd"] != "/work" {
		t.Fatalf("admitted retry hello = %+v", currentHello)
	}
	if err = awaitInteractiveError(t, readyDone, "owner.ready after stale superseded"); err != nil {
		t.Fatal(err)
	}
	if owner.Err() != nil {
		t.Fatalf("stale pre-admission superseded poisoned owner: %v", owner.Err())
	}
}

func TestInteractiveOwnerSessionEndWhileDisconnectedStopsReconnect(t *testing.T) {
	root := testsocket.Directory(t)
	socket := filepath.Join(root, "bus.sock")
	originalDial, originalInterval := piInteractivePublicDial, piInteractiveReconnectInterval
	dials := make(chan struct{}, 64)
	piInteractivePublicDial = func(ctx context.Context, network, address string) (net.Conn, error) {
		dials <- struct{}{}
		return originalDial(ctx, network, address)
	}
	piInteractiveReconnectInterval = 10 * time.Millisecond
	t.Cleanup(func() {
		piInteractivePublicDial, piInteractiveReconnectInterval = originalDial, originalInterval
	})
	listener := interactiveBusListenerAt(t, socket)
	nativeState := &interactiveNativeFixture{}
	owner, native := interactiveOwnerPair(t, socket, root, "", []string{"team"}, nativeState)
	nativeState.set("native-one", "title", "/work")
	_, bus, _ := interactiveReady(t, native, listener, interactiveReadyRequest{interactiveTopology, owner.directory, "native-one", "title"})
	if err := bus.Close(); err != nil {
		t.Fatal(err)
	}
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	awaitInteractiveDisconnected(t, owner)
	owner.mu.Lock()
	public := owner.public
	owner.mu.Unlock()
	var ended map[string]string
	if err := native.Call(context.Background(), "session_end", interactiveEndRequest{interactiveTopology, "native-one", "reload"}, &ended); err != nil || ended["session_id"] != "native-one" {
		t.Fatalf("session_end = %#v, %v", ended, err)
	}
	awaitInteractiveSignal(t, public.done, "ended reconnect lifetime")
	for len(dials) > 0 {
		<-dials
	}
	select {
	case <-dials:
		t.Fatal("ended session attempted another public dial")
	case <-time.After(30 * time.Millisecond):
	}
	if owner.Err() != nil {
		t.Fatalf("disconnected SessionEnd retired native owner: %v", owner.Err())
	}
}

func TestInteractiveOwnerCloseJoinsAbsentDaemonReconnect(t *testing.T) {
	root := testsocket.Directory(t)
	socket := filepath.Join(root, "absent.sock")
	originalDial, originalInterval := piInteractivePublicDial, piInteractiveReconnectInterval
	dials := make(chan struct{}, 64)
	piInteractivePublicDial = func(ctx context.Context, network, address string) (net.Conn, error) {
		dials <- struct{}{}
		return originalDial(ctx, network, address)
	}
	piInteractiveReconnectInterval = 10 * time.Millisecond
	t.Cleanup(func() {
		piInteractivePublicDial, piInteractiveReconnectInterval = originalDial, originalInterval
	})
	nativeState := &interactiveNativeFixture{}
	owner, native := interactiveOwnerPair(t, socket, root, "", []string{"team"}, nativeState)
	nativeState.set("native-one", "title", "/work")
	readyDone := interactiveReadyCall(native, interactiveReadyRequest{interactiveTopology, owner.directory, "native-one", "title"})
	awaitInteractiveSignal(t, dials, "absent-daemon dial")
	if err := owner.Close(); err != nil {
		t.Fatalf("Close during absent-daemon retry = %v", err)
	}
	if err := awaitInteractiveError(t, readyDone, "owner.ready cancellation"); err == nil {
		t.Fatal("owner.ready survived owner Close")
	}
	for len(dials) > 0 {
		<-dials
	}
	select {
	case <-dials:
		t.Fatal("closed owner attempted another public dial")
	case <-time.After(30 * time.Millisecond):
	}
}

func TestInteractiveOwnerPreservesFIFOAcrossBusyAndIdleDeliveries(t *testing.T) {
	listener := interactiveBusListener(t)
	nativeState := &interactiveNativeFixture{replies: []bool{false, true, true, true}}
	owner, native := interactiveOwnerPair(t, listener.Addr().String(), t.TempDir(), "", []string{"team"}, nativeState)
	nativeState.set("native-one", "native title", "/work")
	_, bus, scanner := interactiveReady(t, native, listener, interactiveReadyRequest{interactiveTopology, owner.directory, "native-one", "native title"})

	delivery := kit.DeliveryRequest{MessageID: "opaque busy id", From: kit.DeliverySource{SessionID: "sender", Name: "Sender", Product: "fixture", Groups: []string{"team"}}, Body: "busy body"}
	body, err := protocol.RequestBytes(1, "message.deliver", delivery)
	if err != nil {
		t.Fatal(err)
	}
	interactiveWrite(t, bus, body)
	frame := interactiveFrame(t, scanner)
	var receipt kit.DeliveryReceipt
	if frame.Request || protocol.UnmarshalResult("message.deliver", frame.Result, &receipt) != nil || receipt.Disposition != "queued_for_next_turn" {
		t.Fatalf("busy receipt = %+v frame=%+v", receipt, frame)
	}

	// Native would accept now, but the older owned delivery must drain first.
	delivery.MessageID, delivery.Body = "opaque later id", "later body"
	body, err = protocol.RequestBytes(2, "message.deliver", delivery)
	if err != nil {
		t.Fatal(err)
	}
	interactiveWrite(t, bus, body)
	frame = interactiveFrame(t, scanner)
	if frame.Request || protocol.UnmarshalResult("message.deliver", frame.Result, &receipt) != nil || receipt.Disposition != "queued_for_next_turn" {
		t.Fatalf("ordered receipt = %+v frame=%+v", receipt, frame)
	}

	var drained struct {
		SessionID string `json:"session_id"`
		Drained   int    `json:"drained"`
	}
	if err = native.Call(context.Background(), "owner.drain", interactiveDrainRequest{"native-one", "before_agent_start"}, &drained); err != nil || drained.SessionID != "native-one" || drained.Drained != 2 {
		t.Fatalf("first drain = %+v, %v", drained, err)
	}
	if err = native.Call(context.Background(), "owner.drain", interactiveDrainRequest{"native-one", "agent_settled"}, &drained); err != nil || drained.Drained != 0 {
		t.Fatalf("second drain = %+v, %v", drained, err)
	}

	delivery.MessageID, delivery.Body = "delivery-idle", "idle body"
	body, err = protocol.RequestBytes(3, "message.deliver", delivery)
	if err != nil {
		t.Fatal(err)
	}
	interactiveWrite(t, bus, body)
	frame = interactiveFrame(t, scanner)
	if frame.Request || protocol.UnmarshalResult("message.deliver", frame.Result, &receipt) != nil || receipt.Disposition != "written" {
		t.Fatalf("idle receipt = %+v frame=%+v", receipt, frame)
	}

	nativeState.mu.Lock()
	appends := append([]interactiveAppendRequest(nil), nativeState.appends...)
	nativeState.mu.Unlock()
	if len(appends) != 4 || appends[0].MessageID != "opaque busy id" || appends[1].MessageID != "opaque busy id" ||
		appends[2].MessageID != "opaque later id" || appends[3].MessageID != "delivery-idle" {
		t.Fatalf("native appends = %+v", appends)
	}
	want, err := host.RenderNativeMessage(kit.DeliveryRequest{MessageID: "delivery-idle", From: delivery.From, Body: "idle body"})
	if err != nil || appends[3].Body != want {
		t.Fatalf("rendered append = %q, want %q, err=%v", appends[3].Body, want, err)
	}
}

func TestInteractiveOwnerRejectsUnobservableBranchSummaryBeforeRetention(t *testing.T) {
	listener := interactiveBusListener(t)
	nativeState := &interactiveNativeFixture{replies: []bool{false, true}, reasons: []string{"branch_summary_busy"}}
	owner, native := interactiveOwnerPair(t, listener.Addr().String(), t.TempDir(), "", []string{"team"}, nativeState)
	nativeState.set("native-one", "native title", "/work")
	_, bus, scanner := interactiveReady(t, native, listener, interactiveReadyRequest{interactiveTopology, owner.directory, "native-one", "native title"})
	var before struct {
		SessionID string `json:"session_id"`
		Pending   bool   `json:"pending"`
	}
	if err := native.Call(context.Background(), "owner.before_tree", interactiveBeforeTreeRequest{"native-one"}, &before); err != nil || before.Pending {
		t.Fatalf("before tree = %+v, %v", before, err)
	}

	delivery := kit.DeliveryRequest{MessageID: "branch-busy", From: kit.DeliverySource{SessionID: "sender", Product: "fixture", Groups: []string{}}, Body: "do not retain"}
	body, err := protocol.RequestBytes(1, "message.deliver", delivery)
	if err != nil {
		t.Fatal(err)
	}
	interactiveWrite(t, bus, body)
	frame := interactiveFrame(t, scanner)
	var receipt kit.DeliveryReceipt
	if frame.Request || protocol.UnmarshalResult("message.deliver", frame.Result, &receipt) != nil ||
		receipt.Disposition != "rejected" || receipt.Reason != "native_branch_summary_busy" {
		t.Fatalf("branch receipt = %+v frame=%+v", receipt, frame)
	}
	owner.mu.Lock()
	queued := len(owner.queue)
	owner.mu.Unlock()
	if queued != 0 {
		t.Fatalf("branch delivery retained %d entries", queued)
	}

	delivery.MessageID, delivery.Body = "after-branch", "wake now"
	body, _ = protocol.RequestBytes(2, "message.deliver", delivery)
	interactiveWrite(t, bus, body)
	frame = interactiveFrame(t, scanner)
	if frame.Request || protocol.UnmarshalResult("message.deliver", frame.Result, &receipt) != nil || receipt.Disposition != "written" {
		t.Fatalf("recovered receipt = %+v frame=%+v", receipt, frame)
	}
}

func TestInteractiveOwnerCancelsTreeBeforeRetainedWorkAndGatesFastPath(t *testing.T) {
	listener := interactiveBusListener(t)
	nativeState := &interactiveNativeFixture{replies: []bool{false, true, true}}
	owner, native := interactiveOwnerPair(t, listener.Addr().String(), t.TempDir(), "", []string{"team"}, nativeState)
	nativeState.set("native-one", "native title", "/work")
	_, bus, scanner := interactiveReady(t, native, listener, interactiveReadyRequest{interactiveTopology, owner.directory, "native-one", "native title"})

	send := func(id int, messageID string) kit.DeliveryReceipt {
		t.Helper()
		delivery := kit.DeliveryRequest{MessageID: messageID, From: kit.DeliverySource{SessionID: "sender", Product: "fixture", Groups: []string{}}, Body: messageID}
		body, err := protocol.RequestBytes(int64(id), "message.deliver", delivery)
		if err != nil {
			t.Fatal(err)
		}
		interactiveWrite(t, bus, body)
		frame := interactiveFrame(t, scanner)
		var receipt kit.DeliveryReceipt
		if frame.Request || protocol.UnmarshalResult("message.deliver", frame.Result, &receipt) != nil {
			t.Fatalf("delivery frame = %+v", frame)
		}
		return receipt
	}

	if receipt := send(1, "retained-before-tree"); receipt.Disposition != "queued_for_next_turn" {
		t.Fatalf("retained receipt = %+v", receipt)
	}
	var before struct {
		SessionID string `json:"session_id"`
		Pending   bool   `json:"pending"`
	}
	if err := native.Call(context.Background(), "owner.before_tree", interactiveBeforeTreeRequest{"native-one"}, &before); err != nil || !before.Pending {
		t.Fatalf("before tree = %+v, %v", before, err)
	}
	if receipt := send(2, "during-cancel-boundary"); receipt.Disposition != "rejected" || receipt.Reason != "native_branch_summary_busy" {
		t.Fatalf("tree-busy receipt = %+v", receipt)
	}
	var drained struct {
		SessionID string `json:"session_id"`
		Drained   int    `json:"drained"`
	}
	if err := native.Call(context.Background(), "owner.drain", interactiveDrainRequest{"native-one", "session_before_tree_cancelled"}, &drained); err != nil || drained.Drained != 1 {
		t.Fatalf("cancel drain = %+v, %v", drained, err)
	}
	if receipt := send(3, "after-cancel-drain"); receipt.Disposition != "written" {
		t.Fatalf("recovered receipt = %+v", receipt)
	}
	nativeState.mu.Lock()
	appends := append([]interactiveAppendRequest(nil), nativeState.appends...)
	nativeState.mu.Unlock()
	if len(appends) != 3 || appends[0].MessageID != "retained-before-tree" ||
		appends[1].MessageID != "retained-before-tree" || appends[2].MessageID != "after-cancel-drain" {
		t.Fatalf("native appends = %+v", appends)
	}
}

func TestInteractiveOwnerCanceledDeliveryBeforeAdmissionDoesNotRetireOwner(t *testing.T) {
	listener := interactiveBusListener(t)
	nativeState := &interactiveNativeFixture{}
	owner, native := interactiveOwnerPair(t, listener.Addr().String(), t.TempDir(), "", []string{"team"}, nativeState)
	nativeState.set("native-one", "title", "/work")
	_, _, _ = interactiveReady(t, native, listener, interactiveReadyRequest{interactiveTopology, owner.directory, "native-one", "title"})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := owner.deliver(ctx, owner.generation, kit.DeliveryRequest{
		MessageID: "opaque canceled id", From: kit.DeliverySource{SessionID: "sender", Product: "fixture"}, Body: "body",
	})
	if !errors.Is(err, errInteractiveDeliveryCanceled) || owner.Err() != nil {
		t.Fatalf("canceled delivery = %v, owner = %v", err, owner.Err())
	}
	nativeState.mu.Lock()
	defer nativeState.mu.Unlock()
	if len(nativeState.appends) != 0 {
		t.Fatalf("canceled delivery reached native: %+v", nativeState.appends)
	}
}

func TestInteractiveOwnerOldGenerationCannotUseSameIDReplacement(t *testing.T) {
	listener := interactiveBusListener(t)
	nativeState := &interactiveNativeFixture{}
	owner, native := interactiveOwnerPair(t, listener.Addr().String(), t.TempDir(), "", []string{"team"}, nativeState)
	nativeState.set("native-one", "first", "/work")
	_, first, _ := interactiveReady(t, native, listener, interactiveReadyRequest{interactiveTopology, owner.directory, "native-one", "first"})
	owner.mu.Lock()
	old := owner.public
	owner.mu.Unlock()
	var ended map[string]string
	if err := native.Call(context.Background(), "session_end", interactiveEndRequest{interactiveTopology, "native-one", "reload"}, &ended); err != nil {
		t.Fatal(err)
	}
	_ = first.Close()
	nativeState.set("native-one", "reloaded", "/work")
	_, _, _ = interactiveReady(t, native, listener, interactiveReadyRequest{interactiveTopology, owner.directory, "native-one", "reloaded"})
	if _, err := owner.callPublic(context.Background(), "native-one", old, "session.list", kit.SessionListRequest{}); err == nil || !strings.Contains(err.Error(), "current session") {
		t.Fatalf("old generation call = %v", err)
	}
}

func TestInteractiveOwnerCloseJoinsUnadmittedHelloWatcher(t *testing.T) {
	listener := interactiveBusListener(t)
	owner, err := newInteractiveOwner(context.Background(), listener.Addr().String(), t.TempDir(), "", []string{"team"})
	if err != nil {
		t.Fatal(err)
	}
	published := make(chan error, 1)
	go func() { published <- owner.publish(context.Background(), "native-one", "title", "/work") }()
	bus := interactiveAccept(t, listener)
	scanner := bufio.NewScanner(bus)
	frame := interactiveFrame(t, scanner)
	if !frame.Request || frame.Method != "session.hello" {
		t.Fatalf("held hello = %+v", frame)
	}
	closed := make(chan error, 1)
	go func() { closed <- owner.Close() }()
	select {
	case err = <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not join the unadmitted public watcher")
	}
	select {
	case err = <-published:
		if err == nil {
			t.Fatal("held hello was admitted after Close")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("held hello did not settle")
	}
}

func TestInteractiveOwnerRejectsPoisonedDeliveryBeforeExistingQueue(t *testing.T) {
	owner, err := newInteractiveOwner(context.Background(), "/bus.sock", t.TempDir(), "", []string{"team"})
	if err != nil {
		t.Fatal(err)
	}
	left, right := net.Pipe()
	defer right.Close()
	connection := kit.NewConnection(left, func(context.Context, *kit.Request) {})
	defer owner.Close()
	owner.mu.Lock()
	owner.conn = connection
	owner.sessionID = "native-one"
	owner.generation = 9
	owner.queue = []interactiveQueuedDelivery{{messageID: "older", body: "older", bytes: 10}}
	owner.queueBytes = 10
	owner.mu.Unlock()
	for _, request := range []kit.DeliveryRequest{
		{MessageID: strings.Repeat(" ", 257), From: kit.DeliverySource{SessionID: "sender", Product: "fixture"}, Body: "body"},
		{MessageID: "new", From: kit.DeliverySource{SessionID: "sender", Product: "fixture"}, Body: strings.Repeat("x", maxInteractiveTextBytes)},
	} {
		if _, err = owner.deliver(context.Background(), 9, request); err == nil {
			t.Fatalf("poisoned delivery accepted: id=%d body=%d", len(request.MessageID), len(request.Body))
		}
	}
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if len(owner.queue) != 1 || owner.queue[0].messageID != "older" || owner.queueBytes != 10 {
		t.Fatalf("queue changed after rejected delivery: %+v bytes=%d", owner.queue, owner.queueBytes)
	}
}
