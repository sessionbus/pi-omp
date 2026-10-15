// SPDX-License-Identifier: MIT

package pi

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sessionkit "github.com/antst/sessionbus/bus/sdk/go"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
	"github.com/sessionbus/peer-common/host"
	"github.com/sessionbus/pi-omp/wrappers/pifamily"
)

func TestPiOwnedRunJoinsNativeWitnessesAndCurrentHistory(t *testing.T) {
	wrapper, cwd := newPiTestWrapper(t, "run")
	if _, err := wrapper.Open(context.Background(), sessionkit.OpenRequest{Name: "managed@local", Open: sessionkit.OpenOptions{Cwd: cwd}}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = wrapper.Close(context.Background(), sessionkit.SessionCloseRequest{}) })
	turn, err := wrapper.startNativeTurn(context.Background(), nil, "owned prompt")
	if err != nil {
		t.Fatal(err)
	}
	result, err := turn.Wait(context.Background())
	if err != nil || result.Outcome != "completed" || result.Result != "native answer" || result.NativeStopReason != "stop" {
		t.Fatalf("Run result = %+v, %v", result, err)
	}
	if wrapper.active != nil {
		t.Fatal("completed native interval remained active")
	}
	if err = wrapper.Close(context.Background(), sessionkit.SessionCloseRequest{}); err != nil {
		t.Fatal(err)
	}
}

func TestPiRunUsesAppendCursorSeparatelyFromSelectedLeaf(t *testing.T) {
	wrapper, cwd := newPiTestWrapper(t, "run-offbranch")
	if _, err := wrapper.Open(context.Background(), sessionkit.OpenRequest{Name: "managed@local", Open: sessionkit.OpenOptions{Cwd: cwd}}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = wrapper.Close(context.Background(), sessionkit.SessionCloseRequest{}) })
	turn, err := wrapper.startNativeTurn(context.Background(), nil, "owned prompt")
	if err != nil {
		t.Fatal(err)
	}
	result, err := turn.Wait(context.Background())
	if err != nil || result.Outcome != "completed" || result.Result != "native answer" {
		t.Fatalf("off-branch result = %+v, %v", result, err)
	}
}

func TestPiHandledPromptConsumesSubmittedHandoff(t *testing.T) {
	wrapper, cwd := newPiTestWrapper(t, "handled")
	if _, err := wrapper.Open(context.Background(), sessionkit.OpenRequest{Name: "managed@local", Open: sessionkit.OpenOptions{Cwd: cwd}}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = wrapper.Close(context.Background(), sessionkit.SessionCloseRequest{}) })
	receipt, err := wrapper.handoff.Deliver(context.Background(), sessionkit.DeliveryRequest{
		MessageID: "queued", Body: "queued once",
		From: sessionkit.DeliverySource{SessionID: "sender@local", Product: "fixture"},
	}, nil)
	if err != nil || receipt.Disposition != "queued_for_next_turn" {
		t.Fatalf("stage delivery = %+v, %v", receipt, err)
	}
	_, err = wrapper.handoff.Run(context.Background(), &sessionkit.Run{}, "/handled", func(ctx context.Context, prompt string) (host.Turn, error) {
		return wrapper.startNativeTurn(ctx, nil, prompt)
	})
	if err == nil || !strings.Contains(err.Error(), "without the managed Run preflight") {
		t.Fatalf("handled prompt terminal = %v", err)
	}
	if queued := wrapper.handoff.Claim(); len(queued) != 0 {
		t.Fatalf("submitted delivery was replayable: %#v", queued)
	}
	if wrapper.active != nil {
		t.Fatal("handled prompt retained an active native interval")
	}
	if closeErr := wrapper.Close(context.Background(), sessionkit.SessionCloseRequest{}); closeErr == nil ||
		!strings.Contains(closeErr.Error(), "without the managed Run preflight") {
		t.Fatalf("handled prompt close = %v", closeErr)
	}
}

func TestPiOwnedInterruptJoinsAbortAndNativeTerminal(t *testing.T) {
	wrapper, cwd := newPiTestWrapper(t, "run-abort")
	if _, err := wrapper.Open(context.Background(), sessionkit.OpenRequest{Name: "managed@local", Open: sessionkit.OpenOptions{Cwd: cwd}}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = wrapper.Close(context.Background(), sessionkit.SessionCloseRequest{}) })
	turn, err := wrapper.startNativeTurn(context.Background(), nil, "owned prompt")
	if err != nil {
		t.Fatal(err)
	}
	if err = turn.Interrupt(context.Background()); err != nil {
		t.Fatal(err)
	}
	result, err := turn.Wait(context.Background())
	if err != nil || result.Outcome != "interrupted" || result.Result != "" || result.NativeStopReason != "aborted" {
		t.Fatalf("interrupted result = %+v, %v", result, err)
	}
	if err = wrapper.Close(context.Background(), sessionkit.SessionCloseRequest{}); err != nil {
		t.Fatal(err)
	}
}

// piSDKLane drives a Pi lane through the real SDK Worker, as the daemon does,
// so its Runs are the Worker's own.
type piSDKLane struct {
	t       *testing.T
	wrapper *Wrapper
	conn    net.Conn
	reader  *bufio.Reader
}

func newPiSDKLane(t *testing.T, mode string) *piSDKLane {
	t.Helper()
	wrapper, cwd := newPiTestWrapper(t, mode)
	listener, err := net.Listen("unix", wrapper.socket)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	t.Setenv(host.SocketEnv, wrapper.socket)
	t.Setenv(host.TokenEnv, "pi-worker-token")
	t.Setenv(host.LocalKeyEnv, "")
	worker := sessionkit.NewWorker(wrapper)
	wrapper.SetCaller(worker.Caller())
	wrapper.SetShutdown(worker.Shutdown)
	go func() { _ = worker.Serve(context.Background()) }()
	connection, err := listener.Accept()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = connection.Close()
		worker.Shutdown()
		<-worker.Closed()
		if wrapper.process != nil {
			wrapper.process.Force()
			_ = wrapper.process.Wait()
			_ = wrapper.process.Cleanup()
		}
	})
	lane := &piSDKLane{t: t, wrapper: wrapper, conn: connection, reader: bufio.NewReader(connection)}
	hello := lane.read()
	if hello.Method != "session.hello" {
		t.Fatalf("first Worker frame = %+v", hello)
	}
	lane.answer(hello)
	lane.request(1, "session.open", sessionkit.OpenRequest{Name: "managed@local", Groups: []string{}, Open: sessionkit.OpenOptions{Cwd: cwd}})
	if opened := lane.read(); opened.ID != 1 || opened.Error != nil {
		t.Fatalf("Open response = %+v", opened)
	}
	return lane
}

func (lane *piSDKLane) request(id int64, method string, params any) {
	lane.t.Helper()
	body, err := protocol.RequestBytes(id, method, params)
	if err == nil {
		_, err = lane.conn.Write(body)
	}
	if err != nil {
		lane.t.Fatal(err)
	}
}

func (lane *piSDKLane) answer(frame protocol.Frame) {
	lane.t.Helper()
	body, err := protocol.ResultBytes(frame.ID, frame.Method, struct{}{})
	if err == nil {
		_, err = lane.conn.Write(body)
	}
	if err != nil {
		lane.t.Fatal(err)
	}
}

func (lane *piSDKLane) read() protocol.Frame {
	lane.t.Helper()
	if err := lane.conn.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
		lane.t.Fatal(err)
	}
	line, err := lane.reader.ReadBytes('\n')
	if err != nil {
		lane.t.Fatal(err)
	}
	frame, err := protocol.DecodeFrame(line[:len(line)-1])
	if err != nil {
		lane.t.Fatal(err)
	}
	return frame
}

// terminal reads until the Run's turn.ready, acknowledges it and returns its
// state, outcome and reason. Responses read meanwhile go to responses.
func (lane *piSDKLane) terminal(responses map[int64]protocol.Frame) (string, string, string) {
	lane.t.Helper()
	for {
		frame := lane.read()
		if frame.Method != "turn.ready" {
			responses[frame.ID] = frame
			continue
		}
		var ready struct {
			State   string `json:"state"`
			Outcome string `json:"outcome"`
			Reason  string `json:"reason"`
		}
		if json.Unmarshal(frame.Params, &ready) != nil {
			lane.t.Fatalf("turn.ready = %s", frame.Params)
		}
		lane.answer(frame)
		return ready.State, ready.Outcome, ready.Reason
	}
}

// The lane steers a delivery into its running Run: the Worker's own admitted
// Run, whose steer carries the token Go got at that Run's preflight. Without
// a running Run it is not running, and the Run still ends by interrupt.
func TestPiLaneSteersDeliveryIntoActiveRun(t *testing.T) {
	lane := newPiSDKLane(t, "run-abort")
	delivery := sessionkit.DeliveryRequest{MessageID: "steer-1", Body: "peer note",
		From: sessionkit.DeliverySource{SessionID: "sender@local", Product: "fixture", Groups: []string{}}}
	var notRunning *sessionkit.ProtocolError
	if _, err := lane.wrapper.Deliver(context.Background(), delivery, nil); !errors.As(err, &notRunning) || notRunning.Code != -32004 {
		t.Fatalf("delivery without an active Run = %v", err)
	}
	lane.request(2, "turn.execute", protocol.ExecuteRequest{SessionID: "pi-fresh@local", RunID: "g/1", Input: "owned prompt"})
	if executing := lane.read(); executing.ID != 2 || executing.Error != nil {
		t.Fatalf("execute response = %+v", executing)
	}
	deadline := nativeRPCTestContext(t)
	for admitted := false; !admitted; {
		lane.wrapper.mu.Lock()
		turn := lane.wrapper.active
		lane.wrapper.mu.Unlock()
		if turn != nil {
			turn.mu.Lock()
			admitted = turn.admitted
			turn.mu.Unlock()
		}
		select {
		case <-deadline.Done():
			t.Fatal("owned Run was not admitted")
		default:
		}
	}
	lane.request(3, "message.deliver", delivery)
	steered := lane.read()
	var receipt sessionkit.DeliveryReceipt
	if steered.ID != 3 || steered.Error != nil || protocol.UnmarshalResult("message.deliver", steered.Result, &receipt) != nil ||
		receipt.Disposition != "injected" {
		t.Fatalf("delivery into the active Run = %+v, decoded %+v", steered, receipt)
	}
	lane.request(4, "turn.interrupt", map[string]string{"session_id": "pi-fresh@local"})
	responses := map[int64]protocol.Frame{}
	if state, outcome, _ := lane.terminal(responses); state != "done" || outcome != "interrupted" {
		t.Fatalf("interrupted terminal = %s, %s", state, outcome)
	}
	interrupted, ok := responses[4]
	if !ok {
		interrupted = lane.read()
	}
	if interrupted.ID != 4 || interrupted.Error != nil {
		t.Fatalf("interrupt response = %+v", interrupted)
	}
	if _, err := lane.wrapper.Deliver(context.Background(), delivery, nil); !errors.As(err, &notRunning) || notRunning.Code != -32004 {
		t.Fatalf("delivery after the Run = %v", err)
	}
}

type piHeldConn struct {
	net.Conn
	armed            atomic.Bool
	entered, release chan struct{}
	once             sync.Once
}

func (connection *piHeldConn) Read(body []byte) (int, error) {
	n, err := connection.Conn.Read(body)
	if n > 0 && connection.armed.Load() {
		connection.once.Do(func() { close(connection.entered) })
		<-connection.release
	}
	return n, err
}

// The worker dispatches a delivery apart from its Run, so a steer for one Run
// can reach the extension after a later Run holds the same native session.
// The steer carries its own Run's token and the extension, which holds the
// later Run's, refuses it.
func TestPiOldRunSteerCarriesItsRunToken(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	left, right := net.Pipe()
	held := &piHeldConn{Conn: right, entered: make(chan struct{}), release: make(chan struct{})}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(held.release) }) }
	defer release()
	oldRun, nextRun := &sessionkit.Run{}, &sessionkit.Run{}
	oldTurn := &piNativeTurn{admitted: true, token: "old-run-token"}
	wrapper := &Wrapper{opened: true, ctx: ctx, id: "native", run: oldRun, active: oldTurn}
	var tokenMu sync.Mutex
	extensionToken := "old-run-token"
	received := make(chan string, 1)
	hostBridge, err := pifamily.NewBridge(left, pifamily.BridgeHost, nil, pifamily.BridgeLimits{})
	if err != nil {
		t.Fatal(err)
	}
	nativeBridge, err := pifamily.NewBridge(held, pifamily.BridgeNative, func(_ context.Context, method string, raw json.RawMessage) (json.RawMessage, error) {
		var steer struct {
			SessionID string `json:"session_id"`
			MessageID string `json:"message_id"`
			Run       string `json:"run"`
		}
		if method != "native.steer" || json.Unmarshal(raw, &steer) != nil {
			return nil, pifamily.NewBridgeCallError("method_not_found", "unexpected method")
		}
		received <- steer.Run
		tokenMu.Lock()
		current := extensionToken
		tokenMu.Unlock()
		if steer.Run != current {
			return json.Marshal(map[string]any{"session_id": steer.SessionID, "message_id": steer.MessageID, "accepted": false, "reason": "idle"})
		}
		return json.Marshal(map[string]any{"session_id": steer.SessionID, "message_id": steer.MessageID, "accepted": true})
	}, pifamily.BridgeLimits{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		release()
		_ = hostBridge.Close()
		_ = nativeBridge.Close()
	}()
	if err = hostBridge.Ready(ctx); err != nil {
		t.Fatal(err)
	}
	if err = nativeBridge.Ready(ctx); err != nil {
		t.Fatal(err)
	}
	wrapper.bridge = hostBridge
	held.armed.Store(true)
	type outcome struct {
		receipt sessionkit.DeliveryReceipt
		err     error
	}
	done := make(chan outcome, 1)
	go func() {
		receipt, err := wrapper.Deliver(ctx, sessionkit.DeliveryRequest{MessageID: "old-run-message", Body: "note",
			From: sessionkit.DeliverySource{SessionID: "sender@local", Product: "fixture"}}, oldRun)
		done <- outcome{receipt, err}
	}()
	select {
	case <-held.entered:
	case <-ctx.Done():
		t.Fatal("steer frame was not held", ctx.Err())
	}
	oldTurn.mu.Lock()
	oldTurn.nativeSettled = true
	oldTurn.mu.Unlock()
	wrapper.mu.Lock()
	wrapper.run, wrapper.active = nextRun, &piNativeTurn{admitted: true, token: "next-run-token"}
	wrapper.mu.Unlock()
	tokenMu.Lock()
	extensionToken = "next-run-token"
	tokenMu.Unlock()
	release()
	var result outcome
	select {
	case result = <-done:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	select {
	case token := <-received:
		if token != "old-run-token" {
			t.Fatalf("steer for the old Run carried token %q", token)
		}
	case <-ctx.Done():
		t.Fatal("steer was not dispatched", ctx.Err())
	}
	var notRunning *sessionkit.ProtocolError
	if !errors.As(result.err, &notRunning) || notRunning.Code != -32004 {
		t.Fatalf("old Run steer during a later Run = %+v, %v", result.receipt, result.err)
	}
}

// Owner decision: a lane's Run ends when Pi settles, and work Pi runs of its
// own neither ends the lane nor reaches a Run's result. Each mode puts a run
// of Pi's own where it can cross into a Run: after its settle (its entries are
// saved before the Run's history is read), before its preflight (a send
// another extension deferred during the last settle goes first), with stdout
// frames read only after its preflight, and asking for a dialog after the
// owned settle, which the lane cancels as it cancels its own run's dialogs.
func TestPiLaneKeepsPiOwnWorkOutOfItsRuns(t *testing.T) {
	for _, test := range []struct{ name, mode string }{
		{"after the owned settle", "run-own-after"},
		{"before the owned preflight", "run-own-before"},
		{"stdout read after the owned preflight", "run-own-lag"},
		{"dialog after the owned settle", "run-own-dialog"},
	} {
		t.Run(test.name, func(t *testing.T) {
			wrapper, cwd := newPiTestWrapper(t, test.mode)
			if _, err := wrapper.Open(context.Background(), sessionkit.OpenRequest{Name: "managed@local", Open: sessionkit.OpenOptions{Cwd: cwd}}); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = wrapper.Close(context.Background(), sessionkit.SessionCloseRequest{}) })
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			for run := 1; run <= 2; run++ {
				turn, err := wrapper.startNativeTurn(ctx, nil, fmt.Sprintf("owned prompt %d", run))
				if err != nil {
					t.Fatal(err)
				}
				result, err := turn.Wait(ctx)
				if want := fmt.Sprintf("native answer %d", run); err != nil || result.Outcome != "completed" || result.Result != want {
					t.Fatalf("Run %d result = %+v, %v", run, result, err)
				}
			}
			if err := wrapper.Close(context.Background(), sessionkit.SessionCloseRequest{}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// Pi refuses a prompt while it runs work of its own, so the next Run waits
// for that run to settle before it reads history and sends its prompt. A
// refusal that still comes ends the Run without failing the lane.
func TestPiNextRunWaitsForPiOwnRun(t *testing.T) {
	rpc, peer := newNativeRPCTest(t, nil, nativeRPCLimits{})
	ctx, cancel := context.WithCancelCause(context.Background())
	t.Cleanup(func() { cancel(errors.New("test complete")) })
	wrapper := &Wrapper{ctx: ctx, cancel: cancel, rpc: rpc, opened: true, id: "native", extension: "/managed.mjs", ownWork: true}
	if err := wrapper.observeRunEvent("agent_start", json.RawMessage(`{"type":"agent_start"}`)); err != nil {
		t.Fatal(err)
	}
	started := make(chan error, 1)
	go func() {
		_, err := wrapper.startNativeTurn(context.Background(), nil, "owned prompt")
		started <- err
	}()
	deadline := nativeRPCTestContext(t)
	for parked := false; !parked; {
		wrapper.mu.Lock()
		parked = wrapper.nativeChanged != nil
		wrapper.mu.Unlock()
		select {
		case <-deadline.Done():
			t.Fatal("next Run did not wait for Pi's own run")
		case err := <-started:
			t.Fatalf("next Run went on during Pi's own run: %v", err)
		default:
		}
	}
	if err := wrapper.observeRunEvent("agent_settled", json.RawMessage(`{"type":"agent_settled"}`)); err != nil {
		t.Fatal(err)
	}
	request := peer.read(t)
	if nativeRPCField(t, request, "type") != "get_entries" {
		t.Fatalf("first command after Pi's own run = %#v", request)
	}
	peer.write(t, `{"id":"`+nativeRPCField(t, request, "id")+`","type":"response","command":"get_entries","success":true,"data":{"entries":[],"leafId":null}}`)
	request = peer.read(t)
	if nativeRPCField(t, request, "type") != "prompt" {
		t.Fatalf("second command = %#v", request)
	}
	peer.write(t, `{"id":"`+nativeRPCField(t, request, "id")+`","type":"response","command":"prompt","success":false,"error":"Agent is already processing."}`)
	var refusal *piPromptRefusal
	if err := <-started; !errors.As(err, &refusal) || !strings.Contains(err.Error(), "Agent is already processing") {
		t.Fatalf("refused prompt = %v", err)
	}
	wrapper.mu.Lock()
	active, failure := wrapper.active, wrapper.failure
	wrapper.mu.Unlock()
	if active != nil || failure != nil {
		t.Fatalf("refused prompt left turn %v, lane failure %v", active, failure)
	}
}

// Pi refuses the next Run's prompt while it runs work of its own. That Run's
// record is unavailable with Pi's answer, and the lane stays open for the
// following Run.
func TestPiRefusedPromptEndsOnlyItsRun(t *testing.T) {
	lane := newPiSDKLane(t, "run-refuse")
	responses := map[int64]protocol.Frame{}
	lane.request(2, "turn.execute", protocol.ExecuteRequest{SessionID: "pi-fresh@local", RunID: "g/1", Input: "refused prompt"})
	if state, _, reason := lane.terminal(responses); state != "unavailable" || !strings.Contains(reason, "Agent is already processing") {
		t.Fatalf("refused Run = %s, %q", state, reason)
	}
	lane.request(3, "turn.execute", protocol.ExecuteRequest{SessionID: "pi-fresh@local", RunID: "g/2", Input: "owned prompt"})
	if state, outcome, reason := lane.terminal(responses); state != "done" || outcome != "completed" {
		t.Fatalf("Run after the refusal = %s, %s, %q", state, outcome, reason)
	}
	for _, id := range []int64{2, 3} {
		if frame, ok := responses[id]; !ok || frame.Error != nil {
			t.Fatalf("execute %d response = %+v", id, frame)
		}
	}
	lane.wrapper.mu.Lock()
	failure := lane.wrapper.failure
	lane.wrapper.mu.Unlock()
	if failure != nil {
		t.Fatalf("lane failed after a refused prompt: %v", failure)
	}
}

func TestPiSDKInterruptCancelsHeldNativeAdmission(t *testing.T) {
	t.Run("execute", func(t *testing.T) {
		testPiSDKInterruptCancelsHeldNativeAdmission(t, false, false)
	})
	t.Run("delivery seed", func(t *testing.T) {
		testPiSDKInterruptCancelsHeldNativeAdmission(t, true, false)
	})
	t.Run("held", func(t *testing.T) {
		testPiSDKInterruptCancelsHeldNativeAdmission(t, true, true)
	})
}

type heldPiInterruptProduct struct {
	*Wrapper
	called  chan struct{}
	release chan struct{}
	result  chan error
}

func (product *heldPiInterruptProduct) Interrupt(ctx context.Context, run *sessionkit.Run) error {
	err := product.Wrapper.Interrupt(ctx, run)
	close(product.called)
	<-product.release
	product.result <- err
	return err
}

func testPiSDKInterruptCancelsHeldNativeAdmission(t *testing.T, deliverySeed, holdInterruptResponse bool) {
	wrapper, cwd := newPiTestWrapper(t, "run-hold")
	listener, err := net.Listen("unix", wrapper.socket)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	t.Setenv(host.SocketEnv, wrapper.socket)
	t.Setenv(host.TokenEnv, "pi-worker-token")
	t.Setenv(host.LocalKeyEnv, "")
	var callbacks sessionkit.WorkerCallbacks = wrapper
	var held *heldPiInterruptProduct
	heldReleased := false
	if holdInterruptResponse {
		held = &heldPiInterruptProduct{
			Wrapper: wrapper, called: make(chan struct{}), release: make(chan struct{}), result: make(chan error, 1),
		}
		callbacks = held
		t.Cleanup(func() {
			if !heldReleased {
				close(held.release)
			}
		})
	}
	worker := sessionkit.NewWorker(callbacks)
	wrapper.SetCaller(worker.Caller())
	wrapper.SetShutdown(worker.Shutdown)
	served := make(chan error, 1)
	go func() { served <- worker.Serve(context.Background()) }()
	connection, err := listener.Accept()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = connection.Close()
		worker.Shutdown()
		<-worker.Closed()
		if wrapper.process != nil {
			wrapper.process.Force()
			_ = wrapper.process.Wait()
			_ = wrapper.process.Cleanup()
		}
	})
	reader := bufio.NewReader(connection)
	write := func(body []byte) {
		t.Helper()
		if _, writeErr := connection.Write(body); writeErr != nil {
			t.Fatal(writeErr)
		}
	}
	read := func() protocol.Frame {
		t.Helper()
		line, readErr := reader.ReadBytes('\n')
		if readErr != nil {
			t.Fatal(readErr)
		}
		frame, decodeErr := protocol.DecodeFrame(line[:len(line)-1])
		if decodeErr != nil {
			t.Fatal(decodeErr)
		}
		return frame
	}
	request := func(id int64, method string, params any) {
		t.Helper()
		body, encodeErr := protocol.RequestBytes(id, method, params)
		if encodeErr != nil {
			t.Fatal(encodeErr)
		}
		write(body)
	}
	hello := read()
	if hello.Method != "session.hello" {
		t.Fatalf("first Worker frame = %+v", hello)
	}
	body, err := protocol.ResultBytes(hello.ID, hello.Method, struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	write(body)
	request(1, "session.open", sessionkit.OpenRequest{Name: "managed@local", Groups: []string{}, Open: sessionkit.OpenOptions{Cwd: cwd}})
	opened := read()
	if opened.ID != 1 || opened.Error != nil {
		t.Fatalf("Open response = %+v", opened)
	}
	if deliverySeed {
		request(2, "message.deliver", sessionkit.DeliveryRequest{
			RunID: "g/1", MessageID: "delivery-before-interrupt", Body: "held delivery prompt",
			From: sessionkit.DeliverySource{SessionID: "sender@local", Product: "fixture", Groups: []string{}},
		})
	} else {
		request(2, "turn.execute", protocol.ExecuteRequest{SessionID: "pi-fresh@local", RunID: "g/1", Input: "held prompt"})
		executing := read()
		if executing.ID != 2 || executing.Error != nil {
			t.Fatalf("execute response = %+v", executing)
		}
	}

	deadline, cancelDeadline := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelDeadline()
	var turn *piNativeTurn
	for turn == nil {
		wrapper.mu.Lock()
		turn = wrapper.active
		wrapper.mu.Unlock()
		if turn == nil {
			select {
			case <-deadline.Done():
				t.Fatal("held native prompt was not installed")
			default:
			}
		}
	}
	for {
		turn.mu.Lock()
		input, preflight, started, changed := turn.input, turn.preflight, turn.started, turn.changed
		turn.mu.Unlock()
		if input {
			if preflight || started != 0 {
				t.Fatalf("held admission advanced: preflight %v starts %d", preflight, started)
			}
			break
		}
		select {
		case <-changed:
		case <-deadline.Done():
			t.Fatal("native input witness was not recorded")
		}
	}
	request(3, "turn.interrupt", map[string]string{"session_id": "pi-fresh@local"})
	if held != nil {
		<-held.called
	}
	inputAnswered, interrupted, ready := !deliverySeed, false, false
	workerEOF := false
	for !inputAnswered || !interrupted || !ready {
		if err = connection.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
			t.Fatal(err)
		}
		line, readErr := reader.ReadBytes('\n')
		if readErr != nil {
			if inputAnswered && ready && errors.Is(readErr, io.EOF) {
				workerEOF = true
				break
			}
			t.Fatalf("Worker ended before delivery and terminal ownership were proven: %v", readErr)
		}
		frame, decodeErr := protocol.DecodeFrame(line[:len(line)-1])
		if decodeErr != nil {
			t.Fatal(decodeErr)
		}
		switch {
		case frame.Method == "turn.ready":
			var terminal struct {
				State   string `json:"state"`
				Outcome string `json:"outcome"`
			}
			if json.Unmarshal(frame.Params, &terminal) != nil || terminal.State != "done" || terminal.Outcome != "interrupted" {
				t.Fatalf("interrupted terminal = %s", frame.Params)
			}
			ack, encodeErr := protocol.ResultBytes(frame.ID, frame.Method, struct{}{})
			if encodeErr != nil {
				t.Fatal(encodeErr)
			}
			write(ack)
			ready = true
		case frame.ID == 3:
			if frame.Error != nil {
				t.Fatalf("interrupt response = %+v", frame.Error)
			}
			interrupted = true
		case deliverySeed && frame.ID == 2:
			var receipt sessionkit.DeliveryReceipt
			if frame.Error != nil || protocol.UnmarshalResult("message.deliver", frame.Result, &receipt) != nil ||
				receipt.Disposition != "rejected" || receipt.Reason == "not_submitted" ||
				!strings.Contains(receipt.Reason, errPiRunInterrupted.Error()) {
				t.Fatalf("uncertain submitted delivery receipt = %+v, decoded %+v", frame, receipt)
			}
			inputAnswered = true
		default:
			t.Fatalf("unexpected Worker frame = %+v", frame)
		}
	}
	if held != nil {
		if interrupted || !workerEOF {
			t.Fatalf("held interrupt response = observed %v, EOF %v", interrupted, workerEOF)
		}
		close(held.release)
		heldReleased = true
		if interruptErr := <-held.result; interruptErr != nil {
			t.Fatalf("held Pi interrupt callback = %v", interruptErr)
		}
	}
	t.Logf("interrupt response observed before retirement: %v", interrupted)
	_ = connection.SetReadDeadline(time.Time{})
	select {
	case <-worker.Closed():
	case <-deadline.Done():
		t.Fatal("interrupted Worker did not retire")
	}
	_ = connection.Close()
	_ = listener.Close()
	<-served
	turn.mu.Lock()
	preflight, started, nativeStarts := turn.preflight, turn.started, turn.nativeStarts
	turn.mu.Unlock()
	if preflight || started != 0 || nativeStarts != 0 {
		t.Fatalf("late native admission = preflight %v private %d wire %d", preflight, started, nativeStarts)
	}
	if queued := wrapper.handoff.Claim(); len(queued) != 0 {
		t.Fatalf("uncertain submitted prompt became replayable: %#v", queued)
	}
	select {
	case <-wrapper.process.done:
	default:
		t.Fatal("interrupted native child was not joined")
	}
}

func TestPiRunAllowsLaterNativeUsersAndFiltersExtensionErrors(t *testing.T) {
	wrapper := &Wrapper{extension: "/managed/extension.mjs"}
	turn := newPiNativeTurn(wrapper, nil, "", "", "original")
	if err := turn.recordInput("rpc", "original", false); err != nil {
		t.Fatal(err)
	}
	if _, err := turn.recordPreflight("expanded", false, "", 0, false); err != nil {
		t.Fatal(err)
	}
	if err := turn.recordStart(false); err != nil {
		t.Fatal(err)
	}
	if err := turn.recordEvent("agent_start", json.RawMessage(`{"type":"agent_start"}`)); err != nil {
		t.Fatal(err)
	}
	for _, text := range []string{"expanded", "owned continuation"} {
		raw, _ := json.Marshal(map[string]any{
			"type": "message_start", "message": map[string]any{"role": "user", "content": text},
		})
		if err := turn.recordEvent("message_start", raw); err != nil {
			t.Fatalf("later user %q: %v", text, err)
		}
	}
	if err := turn.recordEvent("extension_error", json.RawMessage(`{"type":"extension_error","extensionPath":"/ambient.mjs","event":"agent_start","error":"ambient failed"}`)); err != nil {
		t.Fatalf("unrelated extension failure became owned failure: %v", err)
	}
	err := turn.recordEvent("extension_error", json.RawMessage(`{"type":"extension_error","extensionPath":"/managed/extension.mjs","event":"agent_start","error":"managed failed"}`))
	if err == nil || !strings.Contains(err.Error(), "managed extension") {
		t.Fatalf("managed extension failure = %v", err)
	}
}

func TestPiReconcilesCrossTransportStartAtSettledBoundary(t *testing.T) {
	wrapper := &Wrapper{extension: "/managed/extension.mjs"}
	turn := newPiNativeTurn(wrapper, nil, "", "", "original")
	if err := turn.recordInput("rpc", "original", false); err != nil {
		t.Fatal(err)
	}
	if _, err := turn.recordPreflight("expanded", false, "", 0, false); err != nil {
		t.Fatal(err)
	}
	if err := turn.recordStart(false); err != nil {
		t.Fatal(err)
	}
	// The bridge settling witness can be dispatched before Go reads an earlier
	// stdout start. Pi's source order is reconciled where stdout itself settles.
	if err := turn.recordSettling(""); err != nil {
		t.Fatal(err)
	}
	if err := turn.recordEvent("agent_start", json.RawMessage(`{"type":"agent_start"}`)); err != nil {
		t.Fatal(err)
	}
	if err := turn.recordEvent("message_start", json.RawMessage(`{"type":"message_start","message":{"role":"user","content":"expanded"}}`)); err != nil {
		t.Fatal(err)
	}
	if err := turn.recordEvent("agent_settled", json.RawMessage(`{"type":"agent_settled"}`)); err != nil {
		t.Fatal(err)
	}

	missing := newPiNativeTurn(wrapper, nil, "", "", "original")
	if err := missing.recordInput("rpc", "original", false); err != nil {
		t.Fatal(err)
	}
	if _, err := missing.recordPreflight("expanded", false, "", 0, false); err != nil {
		t.Fatal(err)
	}
	if err := missing.recordStart(false); err != nil {
		t.Fatal(err)
	}
	if err := missing.recordSettling(""); err != nil {
		t.Fatal(err)
	}
	if err := missing.recordEvent("agent_settled", json.RawMessage(`{"type":"agent_settled"}`)); err == nil {
		t.Fatal("native settled without its stdout start")
	}
}

func TestPiIdleExtensionErrorsOnlyRetireForManagedPath(t *testing.T) {
	wrapper := &Wrapper{extension: "/managed/extension.mjs"}
	if err := wrapper.observeRunEvent("extension_error", json.RawMessage(`{"type":"extension_error","extensionPath":"/ambient.mjs","event":"session_start","error":"ambient failed"}`)); err != nil {
		t.Fatalf("idle ambient extension failure became owned failure: %v", err)
	}
	err := wrapper.observeRunEvent("extension_error", json.RawMessage(`{"type":"extension_error","extensionPath":"/managed/extension.mjs","event":"session_start","error":"managed failed"}`))
	if err == nil || !strings.Contains(err.Error(), "managed extension failed") {
		t.Fatalf("idle managed extension failure = %v", err)
	}
}

func TestPiRuntimeDialogCancellationUsesOneWayNativeInput(t *testing.T) {
	rpc, peer := newNativeRPCTest(t, nil, nativeRPCLimits{})
	ctx, cancel := context.WithCancelCause(context.Background())
	t.Cleanup(func() { cancel(errors.New("test complete")) })
	wrapper := &Wrapper{ctx: ctx, cancel: cancel, rpc: rpc, opened: true, id: "native", extension: "/managed.mjs"}
	turn := newPiNativeTurn(wrapper, nil, "", "", "prompt")
	wrapper.active = turn
	if err := wrapper.observeNativeUI(json.RawMessage(`{"type":"extension_ui_request","id":"dialog opaque","method":"confirm","title":"Proceed?","message":"Continue"}`)); err != nil {
		t.Fatal(err)
	}
	request := peer.read(t)
	if len(request) != 3 || nativeRPCField(t, request, "type") != "extension_ui_response" ||
		nativeRPCField(t, request, "id") != "dialog opaque" || string(request["cancelled"]) != "true" {
		t.Fatalf("native UI cancellation = %#v", request)
	}
	ctxWait := nativeRPCTestContext(t)
	for {
		turn.mu.Lock()
		pending, failure := turn.pendingUI, turn.failure
		turn.mu.Unlock()
		if failure != nil {
			t.Fatal(failure)
		}
		if pending == 0 {
			break
		}
		select {
		case <-ctxWait.Done():
			t.Fatal("one-way UI write waited for a response")
		default:
		}
	}
	if stats := rpc.Stats(); stats.pendingCalls != 0 || stats.pendingWrites != 0 || stats.retainedBytes != 0 {
		t.Fatalf("one-way UI cancellation retained work: %+v", stats)
	}
}

func TestPiRuntimeDialogAdmissionIsBoundedAndJoined(t *testing.T) {
	rpc, peer, held, release := newHeldNativeRPCTest(t, nativeRPCLimits{})
	held.enabled.Store(true)
	ctx, cancel := context.WithCancelCause(context.Background())
	wrapper := &Wrapper{ctx: ctx, cancel: cancel, rpc: rpc, opened: true, id: "native", extension: "/managed.mjs"}
	turn := newPiNativeTurn(wrapper, nil, "", "", "prompt")
	wrapper.active = turn
	for index := 0; index < maxPiTurnUIRequests; index++ {
		if err := turn.cancelUI(fmt.Sprintf("dialog-%d", index)); err != nil {
			t.Fatalf("dialog %d: %v", index, err)
		}
	}
	request := peer.read(t)
	if nativeRPCField(t, request, "type") != "extension_ui_response" {
		t.Fatalf("first UI cancellation = %#v", request)
	}
	<-held.wrote
	if err := turn.cancelUI("one-too-many"); err == nil || !strings.Contains(err.Error(), "request bound") {
		t.Fatalf("unbounded UI request = %v", err)
	}
	turn.mu.Lock()
	if len(turn.uiIDs) != maxPiTurnUIRequests || turn.pendingUI != maxPiTurnUIRequests {
		t.Fatalf("bounded UI ownership = ids %d pending %d", len(turn.uiIDs), turn.pendingUI)
	}
	turn.mu.Unlock()
	cancel(errors.New("test complete"))
	release()
	turn.joinOwnedWork()
	_ = rpc.Close()
	waitNativeRPCDone(t, rpc)
	turn.mu.Lock()
	pending := turn.pendingUI
	turn.mu.Unlock()
	if pending != 0 {
		t.Fatalf("joined UI work = %d", pending)
	}
	waitNativeRPCStats(t, rpc, nativeRPCStats{})
}

func TestPiResultFinalizationRejectsLateOwnedWork(t *testing.T) {
	for _, test := range []struct {
		name string
		late func(*piNativeTurn) error
		want string
	}{
		{
			name: "managed extension failure",
			late: func(turn *piNativeTurn) error {
				return turn.recordEvent("extension_error", json.RawMessage(`{"type":"extension_error","extensionPath":"/managed.mjs","event":"agent_end","error":"late failure"}`))
			},
			want: "managed extension failed",
		},
		{
			name: "late UI",
			late: func(turn *piNativeTurn) error { return turn.cancelUI("late-dialog") },
			want: "after the owned terminal",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			rpc, peer := newNativeRPCTest(t, nil, nativeRPCLimits{})
			ctx, cancel := context.WithCancelCause(context.Background())
			wrapper := &Wrapper{ctx: ctx, cancel: cancel, rpc: rpc, opened: true, id: "native", extension: "/managed.mjs"}
			turn := newPiNativeTurn(wrapper, nil, "base", "file-tail", "expanded")
			turn.admitted, turn.settling, turn.nativeSettled = true, true, true
			wrapper.active = turn
			result := make(chan error, 1)
			go func() {
				_, err := turn.Wait(context.Background())
				result <- err
			}()
			request := peer.read(t)
			if nativeRPCField(t, request, "since") != "file-tail" {
				t.Fatalf("history cursor = %#v", request)
			}
			if err := test.late(turn); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("late owned work = %v", err)
			}
			peer.write(t, `{"id":"`+nativeRPCField(t, request, "id")+`","type":"response","command":"get_entries","success":true,"data":{"entries":[{"id":"user","parentId":"base","type":"message","message":{"role":"user","content":"expanded"}},{"id":"answer","parentId":"user","type":"message","message":{"role":"assistant","content":"borrowed","stopReason":"stop"}}],"leafId":"answer"}}`)
			if err := <-result; err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("borrowed history result = %v", err)
			}
			if !turn.uiClosed || turn.finalized {
				t.Fatalf("finalization state = closed %v finalized %v", turn.uiClosed, turn.finalized)
			}
		})
	}
}
