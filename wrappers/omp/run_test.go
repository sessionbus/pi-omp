// SPDX-License-Identifier: MIT

package omp

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"path/filepath"
	"runtime"
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

type ompHeldDoneContext struct {
	context.Context
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (ctx *ompHeldDoneContext) Done() <-chan struct{} {
	ctx.once.Do(func() {
		close(ctx.entered)
		<-ctx.release
	})
	return ctx.Context.Done()
}

type ompRunFixture struct {
	wrapper  *Wrapper
	rpc      *nativeRPC
	peer     *nativeRPCTestPeer
	registry *OwnerRegistry
	native   *pifamily.Bridge
	binding  OwnerBinding
}

func newOMPRunFixture(t *testing.T) *ompRunFixture {
	t.Helper()
	directory := t.TempDir()
	caller := kit.NewCaller(func(context.Context, string, any) (json.RawMessage, error) {
		return json.RawMessage(`{}`), nil
	})
	ownerFixture := &ownerNativeFixture{descriptions: map[string]ownerDescribeResult{
		"main-token": {OwnerToken: "main-token", SessionID: "main-session", Name: "main", CWD: directory},
	}}
	registry, native := ownerRegistryPair(t, OwnerRegistryOptions{
		Topology: ownerTopologyLane, Socket: filepath.Join(directory, "daemon.sock"), Directory: directory, PrimaryCaller: caller,
	}, ownerFixture)
	if err := native.Call(ownerTestContext(t), "owner.ready", ownerReadyRequest{
		Topology: ownerTopologyLane, Directory: directory, Scope: ownerScopePrimary, Mode: ownerModeRPC,
		OwnerToken: "main-token", SessionID: "main-session", Name: "main",
	}, nil); err != nil {
		t.Fatal(err)
	}
	binding, ok := registry.Primary()
	if !ok {
		t.Fatal("primary binding missing")
	}
	wrapper := New(filepath.Join(directory, "daemon.sock"), "provisional", NativeExecutable{}, filepath.Join(directory, "extension.mjs"))
	wrapper.ctx, wrapper.cancel = context.WithCancelCause(context.Background())
	wrapper.binding, wrapper.opened = binding, true
	rpc, peer := newNativeRPCTest(t, wrapper.observeNative, nativeRPCLimits{})
	wrapper.owner = &NativeOwner{ctx: wrapper.ctx, rpc: rpc, registry: registry}
	t.Cleanup(func() { wrapper.cancel(errNativeOwnerClosed) })
	return &ompRunFixture{wrapper: wrapper, rpc: rpc, peer: peer, registry: registry, native: native, binding: binding}
}

func attachHeldOMPRPC(t *testing.T, fixture *ompRunFixture) (*heldNativeRPCWriter, func()) {
	t.Helper()
	commands, input := io.Pipe()
	output, events := io.Pipe()
	held := &heldNativeRPCWriter{WriteCloser: input, wrote: make(chan struct{}), release: make(chan struct{})}
	rpc, err := newNativeRPC(held, output, fixture.wrapper.observeNative, nativeRPCLimits{})
	if err != nil {
		t.Fatal(err)
	}
	peer := &nativeRPCTestPeer{commands: bufio.NewReader(commands), events: events}
	readyNativeRPCTest(t, rpc, peer)
	fixture.rpc, fixture.peer, fixture.wrapper.owner.rpc = rpc, peer, rpc
	var once sync.Once
	release := func() { once.Do(func() { close(held.release) }) }
	t.Cleanup(func() {
		release()
		_ = commands.Close()
		_ = events.Close()
		_ = rpc.Close()
	})
	return held, release
}

func (fixture *ompRunFixture) start(t *testing.T, prompt string) <-chan struct {
	turn *ompNativeTurn
	err  error
} {
	t.Helper()
	result := make(chan struct {
		turn *ompNativeTurn
		err  error
	}, 1)
	go func() {
		turn, err := fixture.wrapper.startNativeTurn(nativeRPCTestContext(t), nil, prompt)
		result <- struct {
			turn *ompNativeTurn
			err  error
		}{turn, err}
	}()
	return result
}

func (fixture *ompRunFixture) prompt(t *testing.T, prompt string, data string) (string, <-chan struct {
	turn *ompNativeTurn
	err  error
}) {
	t.Helper()
	result := fixture.start(t, prompt)
	command := fixture.peer.read(t)
	id := nativeRPCField(t, command, "id")
	if nativeRPCField(t, command, "type") != "prompt" || string(command["message"]) != mustOMPJSONText(t, prompt) {
		t.Fatalf("prompt command = %#v", command)
	}
	response := `{"id":` + mustOMPJSONText(t, id) + `,"type":"response","command":"prompt","success":true`
	if data != "" {
		response += `,"data":` + data
	}
	fixture.peer.write(t, response+`}`)
	return id, result
}

func (fixture *ompRunFixture) preflight(t *testing.T, sequence uint64, token, prompt string) {
	t.Helper()
	if err := fixture.native.Call(ownerTestContext(t), "run.preflight", ownerPreflightRequest{
		OwnerToken: fixture.binding.OwnerToken, SessionID: fixture.binding.SessionID,
		ReportSequence: sequence, RunToken: token, Prompt: prompt,
	}, nil); err != nil {
		t.Fatal(err)
	}
}

func mustOMPJSONText(t *testing.T, value string) string {
	t.Helper()
	body, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}

func TestOMPRunJoinsLatePreflightAndCurrentTerminalAssistant(t *testing.T) {
	fixture := newOMPRunFixture(t)
	_, started := fixture.prompt(t, "owned prompt", `{"agentInvoked":true}`)
	fixture.peer.write(t, `{"type":"agent_start"}`)
	fixture.peer.write(t, `{"type":"message_end","message":{"role":"assistant","content":[{"type":"text","text":"native answer"}],"stopReason":"stop"}}`)
	fixture.peer.write(t, `{"type":"agent_end","messages":[],"isTerminal":true}`)
	fixture.preflight(t, 1, "run-one", "owned prompt")
	start := <-started
	if start.err != nil || start.turn == nil {
		t.Fatalf("start = %#v, %v", start.turn, start.err)
	}
	result, err := start.turn.Wait(nativeRPCTestContext(t))
	if err != nil || result.Outcome != "completed" || result.Result != "native answer" || result.NativeStopReason != "stop" {
		t.Fatalf("result = %+v, %v", result, err)
	}
}

func TestOMPImmediateNoAgentIsFiniteWithoutBorrowedAssistant(t *testing.T) {
	fixture := newOMPRunFixture(t)
	_, started := fixture.prompt(t, "/local", `{"agentInvoked":false}`)
	start := <-started
	if start.err != nil || start.turn == nil {
		t.Fatalf("start = %#v, %v", start.turn, start.err)
	}
	result, err := start.turn.Wait(nativeRPCTestContext(t))
	if err != nil || result.Outcome != "completed" || result.Result != "" || result.NativeStopReason != "no_agent" {
		t.Fatalf("no-agent result = %+v, %v", result, err)
	}
	fixture.registry.mu.Lock()
	preflights := len(fixture.registry.bindings[fixture.binding.OwnerToken].preflights)
	fixture.registry.mu.Unlock()
	if preflights != 0 {
		t.Fatal("no-agent consumed a foreign preflight")
	}
}

func TestOMPPromptResultNoAgentIsFiniteAndCorrelated(t *testing.T) {
	fixture := newOMPRunFixture(t)
	id, started := fixture.prompt(t, "/local", "")
	fixture.peer.write(t, `{"type":"prompt_result","id":`+mustOMPJSONText(t, id)+`,"agentInvoked":false}`)
	start := <-started
	if start.err != nil || start.turn == nil {
		t.Fatalf("start = %#v, %v", start.turn, start.err)
	}
	result, err := start.turn.Wait(nativeRPCTestContext(t))
	if err != nil || result.Outcome != "completed" || result.Result != "" || result.NativeStopReason != "no_agent" {
		t.Fatalf("prompt_result no-agent = %+v, %v", result, err)
	}
}

type ompWorkerProduct struct{ *Wrapper }

func (product *ompWorkerProduct) Open(context.Context, kit.OpenRequest) (kit.OpenResult, error) {
	return kit.OpenResult{SessionID: product.binding.SessionID}, nil
}

func (*ompWorkerProduct) Close(context.Context, kit.SessionCloseRequest) error { return nil }

func TestOMPWorkerReportsSubmittedNoAgentDeliveryBeforeTerminal(t *testing.T) {
	fixture := newOMPRunFixture(t)
	listener, err := net.Listen("unix", filepath.Join(testsocket.Directory(t), "bus.sock"))
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv(host.SocketEnv, listener.Addr().String())
	t.Setenv(host.TokenEnv, "omp-worker-token")
	t.Setenv(host.LocalKeyEnv, "")
	worker := kit.NewWorker(&ompWorkerProduct{fixture.wrapper})
	fixture.wrapper.SetCaller(worker.Caller())
	served := make(chan error, 1)
	go func() { served <- worker.Serve(context.Background()) }()
	connection, err := listener.Accept()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = connection.Close()
		_ = listener.Close()
		worker.Shutdown()
		<-worker.Closed()
		<-served
	})
	reader := bufio.NewReader(connection)
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
	write := func(body []byte) {
		t.Helper()
		if _, writeErr := connection.Write(body); writeErr != nil {
			t.Fatal(writeErr)
		}
	}
	hello := read()
	ack, err := protocol.ResultBytes(hello.ID, hello.Method, struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	write(ack)
	open, err := protocol.RequestBytes(1, "session.open", kit.OpenRequest{Name: "managed@local", Groups: []string{}})
	if err != nil {
		t.Fatal(err)
	}
	write(open)
	if frame := read(); frame.ID != 1 || frame.Error != nil {
		t.Fatalf("Open response = %+v", frame)
	}
	delivery, err := protocol.RequestBytes(2, "message.deliver", kit.DeliveryRequest{
		RunID: "g/1", MessageID: "delivery-no-agent", Body: "/local",
		From: kit.DeliverySource{SessionID: "sender@local", Product: "fixture", Groups: []string{}},
	})
	if err != nil {
		t.Fatal(err)
	}
	write(delivery)
	command := fixture.peer.read(t)
	id := nativeRPCField(t, command, "id")
	fixture.peer.write(t, `{"id":`+mustOMPJSONText(t, id)+`,"type":"response","command":"prompt","success":true,"data":{"agentInvoked":false}}`)
	receiptFrame := read()
	var receipt kit.DeliveryReceipt
	if receiptFrame.ID != 2 || receiptFrame.Error != nil ||
		protocol.UnmarshalResult("message.deliver", receiptFrame.Result, &receipt) != nil ||
		receipt.Disposition != "rejected" || receipt.Reason != "native_submission_refused" {
		t.Fatalf("no-agent delivery receipt = %+v, decoded %+v", receiptFrame, receipt)
	}
	terminal := read()
	if terminal.Method != "turn.ready" {
		t.Fatalf("frame after delivery receipt = %+v", terminal)
	}
	var ready struct {
		State   string `json:"state"`
		Outcome string `json:"outcome"`
	}
	if json.Unmarshal(terminal.Params, &ready) != nil || ready.State != "done" || ready.Outcome != "completed" {
		t.Fatalf("no-agent terminal = %s", terminal.Params)
	}
	ack, err = protocol.ResultBytes(terminal.ID, terminal.Method, struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	write(ack)
}

func TestOMPRunRejectsForeignLeadingPreflight(t *testing.T) {
	fixture := newOMPRunFixture(t)
	started := fixture.start(t, "owned prompt")
	command := fixture.peer.read(t)
	id := nativeRPCField(t, command, "id")
	if nativeRPCField(t, command, "type") != "prompt" || string(command["message"]) != mustOMPJSONText(t, "owned prompt") {
		t.Fatalf("prompt command = %#v", command)
	}
	// Hold the prompt response so both candidates arrive before waitPreflight
	// retires the registry on the leading mismatch. A later report need not be
	// admitted after that retirement.
	fixture.preflight(t, 1, "foreign", "other prompt")
	fixture.preflight(t, 2, "matching", "owned prompt")
	fixture.peer.write(t, `{"id":`+mustOMPJSONText(t, id)+`,"type":"response","command":"prompt","success":true}`)
	fixture.peer.write(t, `{"type":"agent_start"}`)
	start := <-started
	if start.err != nil || start.turn == nil {
		t.Fatalf("start = %#v, %v", start.turn, start.err)
	}
	if _, err := start.turn.Wait(nativeRPCTestContext(t)); err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("foreign preflight result = %v", err)
	}
}

func TestOMPRunCancelsOwnedRuntimeUIAndJoinsWrite(t *testing.T) {
	fixture := newOMPRunFixture(t)
	_, started := fixture.prompt(t, "owned prompt", "")
	fixture.preflight(t, 1, "run-one", "owned prompt")
	fixture.peer.write(t, `{"type":"agent_start"}`)
	start := <-started
	if start.err != nil || start.turn == nil {
		t.Fatalf("start = %#v, %v", start.turn, start.err)
	}
	if err := fixture.wrapper.observeNative(json.RawMessage(`{"type":"extension_ui_request","id":"open-one","method":"open_url"}`)); err != nil {
		t.Fatalf("open_url notification = %v", err)
	}
	start.turn.mu.Lock()
	pendingBefore := start.turn.pendingUI
	start.turn.mu.Unlock()
	if pendingBefore != 0 {
		t.Fatal("open_url notification was treated as a pending dialog")
	}
	fixture.peer.write(t, `{"type":"extension_ui_request","id":"dialog-one","method":"confirm","title":"approve","message":"continue"}`)
	cancel := fixture.peer.read(t)
	if nativeRPCField(t, cancel, "type") != "extension_ui_response" || nativeRPCField(t, cancel, "id") != "dialog-one" || string(cancel["cancelled"]) != "true" {
		t.Fatalf("UI cancellation = %#v", cancel)
	}
	fixture.peer.write(t, `{"type":"message_end","message":{"role":"assistant","content":"","stopReason":"aborted"}}`)
	fixture.peer.write(t, `{"type":"agent_end","messages":[],"isTerminal":true}`)
	result, err := start.turn.Wait(nativeRPCTestContext(t))
	if err != nil || result.Outcome != "interrupted" || result.NativeStopReason != "aborted" {
		t.Fatalf("UI-canceled result = %+v, %v", result, err)
	}
}

func TestOMPAdmittedInterruptJoinsAbortAndPreservesNextRun(t *testing.T) {
	fixture := newOMPRunFixture(t)
	run := &kit.Run{}
	_, started := fixture.prompt(t, "first prompt", "")
	fixture.preflight(t, 1, "run-one", "first prompt")
	fixture.peer.write(t, `{"type":"agent_start"}`)
	first := <-started
	if first.err != nil || first.turn == nil {
		t.Fatalf("first start = %#v, %v", first.turn, first.err)
	}
	fixture.wrapper.mu.Lock()
	fixture.wrapper.run = run
	fixture.wrapper.mu.Unlock()
	interrupted := make(chan error, 1)
	go func() { interrupted <- fixture.wrapper.Interrupt(nativeRPCTestContext(t), run) }()
	abort := fixture.peer.read(t)
	if nativeRPCField(t, abort, "type") != "abort" {
		t.Fatalf("native abort = %#v", abort)
	}
	abortID := nativeRPCField(t, abort, "id")
	fixture.peer.write(t, `{"id":`+mustOMPJSONText(t, abortID)+`,"type":"response","command":"abort","success":true}`)
	if err := <-interrupted; err != nil {
		t.Fatalf("admitted Interrupt = %v", err)
	}
	fixture.peer.write(t, `{"type":"message_end","message":{"role":"assistant","content":"","stopReason":"aborted"}}`)
	fixture.peer.write(t, `{"type":"agent_end","messages":[],"isTerminal":true}`)
	result, err := first.turn.Wait(nativeRPCTestContext(t))
	if err != nil || result.Outcome != "interrupted" || result.Result != "" || result.NativeStopReason != "aborted" {
		t.Fatalf("interrupted result = %+v, %v", result, err)
	}

	fixture.wrapper.mu.Lock()
	fixture.wrapper.run = nil
	fixture.wrapper.mu.Unlock()
	_, started = fixture.prompt(t, "second prompt", "")
	fixture.preflight(t, 2, "run-two", "second prompt")
	fixture.peer.write(t, `{"type":"agent_start"}`)
	second := <-started
	if second.err != nil || second.turn == nil {
		t.Fatalf("second start = %#v, %v", second.turn, second.err)
	}
	fixture.peer.write(t, `{"type":"message_end","message":{"role":"assistant","content":"healthy","stopReason":"stop"}}`)
	fixture.peer.write(t, `{"type":"agent_end","messages":[],"isTerminal":true}`)
	result, err = second.turn.Wait(nativeRPCTestContext(t))
	if err != nil || result.Outcome != "completed" || result.Result != "healthy" {
		t.Fatalf("post-interrupt result = %+v, %v", result, err)
	}
	waitNativeRPCStats(t, fixture.rpc, nativeRPCStats{})
}

func TestOMPWorkerInterruptsSubmittedDeliveryBeforePreflight(t *testing.T) {
	fixture := newOMPRunFixture(t)
	listener, err := net.Listen("unix", filepath.Join(testsocket.Directory(t), "bus.sock"))
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv(host.SocketEnv, listener.Addr().String())
	t.Setenv(host.TokenEnv, "omp-interrupt-worker-token")
	t.Setenv(host.LocalKeyEnv, "")
	worker := kit.NewWorker(&ompWorkerProduct{fixture.wrapper})
	fixture.wrapper.SetCaller(worker.Caller())
	var shutdownOnce sync.Once
	fixture.wrapper.SetShutdown(func() {
		shutdownOnce.Do(func() {
			_ = fixture.rpc.Close()
			_ = fixture.registry.Close()
			_ = fixture.native.Close()
			worker.Shutdown()
		})
	})
	served := make(chan error, 1)
	go func() { served <- worker.Serve(context.Background()) }()
	servedJoined := false
	connection, err := listener.Accept()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = connection.Close()
		_ = listener.Close()
		fixture.wrapper.shutdown()
		<-worker.Closed()
		if !servedJoined {
			<-served
		}
	})
	reader := bufio.NewReader(connection)
	read := func() (protocol.Frame, error) {
		line, readErr := reader.ReadBytes('\n')
		if readErr != nil {
			return protocol.Frame{}, readErr
		}
		frame, decodeErr := protocol.DecodeFrame(line[:len(line)-1])
		return frame, decodeErr
	}
	write := func(body []byte) {
		t.Helper()
		if _, writeErr := connection.Write(body); writeErr != nil {
			t.Fatal(writeErr)
		}
	}
	request := func(id int64, method string, params any) {
		t.Helper()
		body, encodeErr := protocol.RequestBytes(id, method, params)
		if encodeErr != nil {
			t.Fatal(encodeErr)
		}
		write(body)
	}
	hello, err := read()
	if err != nil || hello.Method != "session.hello" {
		t.Fatalf("Worker hello = %+v, %v", hello, err)
	}
	ack, err := protocol.ResultBytes(hello.ID, hello.Method, struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	write(ack)
	request(1, "session.open", kit.OpenRequest{Name: "managed@local", Groups: []string{}})
	opened, err := read()
	if err != nil || opened.ID != 1 || opened.Error != nil {
		t.Fatalf("Open response = %+v, %v", opened, err)
	}
	seed := kit.DeliveryRequest{
		RunID: "g/1", MessageID: "delivery-before-preflight", Body: "held delivery prompt",
		From: kit.DeliverySource{SessionID: "sender@local", Product: "fixture", Groups: []string{}},
	}
	request(2, "message.deliver", seed)
	prompt := fixture.peer.read(t)
	promptID := nativeRPCField(t, prompt, "id")
	if nativeRPCField(t, prompt, "type") != "prompt" {
		t.Fatalf("native prompt = %#v", prompt)
	}
	seedEnvelope, err := host.RenderNativeMessage(seed)
	if err != nil {
		t.Fatal(err)
	}
	message := nativeRPCField(t, prompt, "message")
	if message != seedEnvelope {
		t.Fatalf("native prompt did not contain the seeded delivery exactly once: %q", message)
	}
	fixture.peer.write(t, `{"id":`+mustOMPJSONText(t, promptID)+`,"type":"response","command":"prompt","success":true}`)

	deadline, cancelDeadline := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelDeadline()
	var turn *ompNativeTurn
	for {
		fixture.wrapper.mu.Lock()
		turn = fixture.wrapper.active
		fixture.wrapper.mu.Unlock()
		if turn != nil {
			turn.mu.Lock()
			submitted, native := turn.submitted, turn.native
			turn.mu.Unlock()
			if submitted && native != nil {
				break
			}
			select {
			case <-deadline.Done():
				t.Fatal("submitted native prompt did not reach held preflight")
			default:
				runtime.Gosched()
			}
			continue
		}
		select {
		case <-deadline.Done():
			t.Fatal("native turn was not installed")
		default:
		}
	}
	request(3, "turn.interrupt", map[string]string{"session_id": fixture.binding.SessionID})
	receiptSeen, readySeen, interruptSeen, workerEOF := false, false, false, false
	for !receiptSeen || !readySeen || !interruptSeen {
		if err = connection.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
			t.Fatal(err)
		}
		frame, readErr := read()
		if readErr != nil {
			if receiptSeen && readySeen && errors.Is(readErr, io.EOF) {
				workerEOF = true
				break
			}
			t.Fatalf("Worker ended before receipt and terminal: %v", readErr)
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
			readySeen = true
		case frame.ID == 2:
			var receipt kit.DeliveryReceipt
			if frame.Error != nil || protocol.UnmarshalResult("message.deliver", frame.Result, &receipt) != nil ||
				receipt.Disposition != "rejected" || receipt.Reason == "not_submitted" ||
				!strings.Contains(receipt.Reason, errOMPRunInterrupted.Error()) {
				t.Fatalf("submitted delivery receipt = %+v, decoded %+v", frame, receipt)
			}
			receiptSeen = true
		case frame.ID == 3:
			if frame.Error != nil {
				t.Fatalf("interrupt response = %+v", frame.Error)
			}
			interruptSeen = true
		default:
			t.Fatalf("unexpected Worker frame = %+v", frame)
		}
	}
	t.Logf("interrupt response observed before retirement: %v; EOF: %v", interruptSeen, workerEOF)
	_ = connection.SetReadDeadline(time.Time{})
	select {
	case <-worker.Closed():
	case <-deadline.Done():
		t.Fatal("interrupted Worker did not retire")
	}
	serveErr := <-served
	servedJoined = true
	// The owned shutdown callback calls Worker.Shutdown, which now cancels Serve.
	if !errors.Is(serveErr, context.Canceled) {
		t.Fatalf("Worker Serve = %v", serveErr)
	}
	turn.mu.Lock()
	submitted, preflight, starts := turn.submitted, turn.preflight, turn.starts
	turn.mu.Unlock()
	if !submitted || preflight || starts != 0 {
		t.Fatalf("held admission = submitted %v, preflight %v, starts %d", submitted, preflight, starts)
	}
	if queued := fixture.wrapper.handoff.Claim(); len(queued) != 0 {
		t.Fatalf("submitted delivery became replayable: %#v", queued)
	}
	select {
	case <-fixture.rpc.Done():
	default:
		t.Fatal("interrupted native RPC was not joined")
	}
	select {
	case <-fixture.registry.Done():
	default:
		t.Fatal("interrupted owner registry was not joined")
	}
	select {
	case <-fixture.native.Done():
	default:
		t.Fatal("interrupted native bridge endpoint was not joined")
	}
	waitNativeRPCStats(t, fixture.rpc, nativeRPCStats{})
	if stats := fixture.native.Stats(); stats != (pifamily.BridgeStats{}) {
		t.Fatalf("native bridge ownership after cleanup = %#v", stats)
	}
}

func TestOMPTerminalCancelsAndJoinsUnsettledUIBeforeResult(t *testing.T) {
	for _, test := range []struct {
		name       string
		foreign    bool
		cancelWait bool
	}{
		{name: "wait context", cancelWait: true},
		{name: "foreign retirement", foreign: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newOMPRunFixture(t)
			held, release := attachHeldOMPRPC(t, fixture)
			_, started := fixture.prompt(t, "owned prompt", "")
			fixture.preflight(t, 1, "run-one", "owned prompt")
			fixture.peer.write(t, `{"type":"agent_start"}`)
			start := <-started
			if start.err != nil || start.turn == nil {
				t.Fatalf("start = %#v, %v", start.turn, start.err)
			}

			// The prompt response can make StartPrompt return before the writer's
			// completion callback. Settle that write before the hold can capture it.
			writeCtx := nativeRPCTestContext(t)
			for fixture.rpc.Stats().pendingWrites != 0 {
				select {
				case <-writeCtx.Done():
					t.Fatal("OMP prompt write did not settle before arming the UI hold")
				default:
					runtime.Gosched()
				}
			}
			held.enabled.Store(true)
			if err := fixture.wrapper.observeNative(json.RawMessage(`{"type":"extension_ui_request","id":"held-dialog","method":"confirm"}`)); err != nil {
				t.Fatal(err)
			}
			cancel := fixture.peer.read(t)
			if nativeRPCField(t, cancel, "type") != "extension_ui_response" || nativeRPCField(t, cancel, "id") != "held-dialog" {
				t.Fatalf("held UI cancellation = %#v", cancel)
			}
			<-held.wrote
			if err := start.turn.recordEvent("message_end", json.RawMessage(`{"type":"message_end","message":{"role":"assistant","content":"finished","stopReason":"stop"}}`)); err != nil {
				t.Fatal(err)
			}
			if err := start.turn.recordEvent("agent_end", json.RawMessage(`{"type":"agent_end","messages":[],"isTerminal":true}`)); err != nil {
				t.Fatal(err)
			}
			if test.foreign {
				retired := fixture.wrapper.observeNative(json.RawMessage(`{"type":"agent_start"}`))
				if retired == nil {
					t.Fatal("foreign post-terminal work was accepted")
				}
				fixture.rpc.stop(retired, false)
				<-fixture.rpc.ctx.Done()
			}
			waitCtx, cancelWait := context.WithCancel(nativeRPCTestContext(t))
			if test.cancelWait {
				cancelWait()
			} else {
				defer cancelWait()
			}
			waited := make(chan error, 1)
			go func() {
				_, err := start.turn.Wait(waitCtx)
				waited <- err
			}()
			var waitErr error
			select {
			case waitErr = <-waited:
			case <-nativeRPCTestContext(t).Done():
				t.Fatal("Wait did not cancel and join the unsettled UI operation")
			}
			if waitErr == nil || !errors.Is(waitErr, errOMPUIUnsettled) {
				t.Fatalf("unsettled UI result = %v", waitErr)
			}
			if test.cancelWait && !errors.Is(waitErr, context.Canceled) {
				t.Fatalf("Wait cancellation was lost: %v", waitErr)
			}
			start.turn.mu.Lock()
			pending := start.turn.pendingUI
			start.turn.mu.Unlock()
			if pending != 0 {
				t.Fatalf("Wait returned with %d owned UI operations", pending)
			}
			release()
			waitNativeRPCDone(t, fixture.rpc)
			waitNativeRPCStats(t, fixture.rpc, nativeRPCStats{})
		})
	}
}

func TestOMPManagedExtensionFailureAndPostTerminalWorkRetireRPC(t *testing.T) {
	fixture := newOMPRunFixture(t)
	ambient := `{"type":"extension_error","extensionPath":"/ambient.mjs","event":"agent_end","error":"ambient"}`
	if err := fixture.wrapper.observeNative(json.RawMessage(ambient)); err != nil {
		t.Fatalf("ambient extension error = %v", err)
	}
	managed := `{"type":"extension_error","extensionPath":` + mustOMPJSONText(t, fixture.wrapper.extension) + `,"event":"agent_end","error":"managed"}`
	if err := fixture.wrapper.observeNative(json.RawMessage(managed)); err == nil {
		t.Fatal("managed extension error outside Run was accepted")
	}
	inside := newOMPRunFixture(t)
	_, insideStarted := inside.prompt(t, "owned prompt", "")
	inside.preflight(t, 1, "run-one", "owned prompt")
	inside.peer.write(t, `{"type":"agent_start"}`)
	insideStart := <-insideStarted
	inside.peer.write(t, `{"type":"extension_error","extensionPath":`+mustOMPJSONText(t, inside.wrapper.extension)+`,"event":"before_agent_start","error":"managed failure"}`)
	if _, err := insideStart.turn.Wait(nativeRPCTestContext(t)); err == nil || !strings.Contains(err.Error(), "managed extension failed") {
		t.Fatalf("managed extension Run result = %v", err)
	}

	second := newOMPRunFixture(t)
	_, started := second.prompt(t, "owned prompt", "")
	second.preflight(t, 1, "run-one", "owned prompt")
	second.peer.write(t, `{"type":"agent_start"}`)
	start := <-started
	second.peer.write(t, `{"type":"message_end","message":{"role":"assistant","content":"done","stopReason":"stop"}}`)
	second.peer.write(t, `{"type":"agent_end","messages":[],"isTerminal":true}`)
	// A later foreign start retires future reuse without revoking the terminal
	// which was already snapshotted under the event lock.
	second.peer.write(t, `{"type":"agent_start"}`)
	waitNativeRPCDone(t, second.rpc)
	result, err := start.turn.Wait(nativeRPCTestContext(t))
	if err != nil || result.Outcome != "completed" || result.Result != "done" {
		t.Fatalf("cached terminal result = %+v, %v", result, err)
	}
	if second.rpc.Err() == nil || !strings.Contains(second.rpc.Err().Error(), "after the owned terminal") {
		t.Fatalf("post-terminal RPC error = %v", second.rpc.Err())
	}
	second.wrapper.mu.Lock()
	lost := second.wrapper.failure
	second.wrapper.mu.Unlock()
	if lost == nil || !strings.Contains(lost.Error(), "after the owned terminal") {
		t.Fatalf("post-terminal owner retirement = %v", lost)
	}
}

func TestOMPHeldCompletionWaitKeepsTerminalAcrossForeignRetirement(t *testing.T) {
	wrapper := &Wrapper{}
	wrapper.ctx, wrapper.cancel = context.WithCancelCause(context.Background())
	t.Cleanup(func() { wrapper.cancel(errNativeOwnerClosed) })
	turn := newOMPNativeTurn(wrapper, nil, "owned", OwnerBinding{})
	turn.native = &nativePrompt{pending: &nativeRPCPending{lateReady: make(chan struct{})}}
	held := &ompHeldDoneContext{Context: context.Background(), entered: make(chan struct{}), release: make(chan struct{})}
	waited := make(chan error, 1)
	go func() { waited <- turn.awaitCompletion(held) }()
	<-held.entered
	if err := turn.recordEvent("agent_start", json.RawMessage(`{"type":"agent_start"}`)); err != nil {
		t.Fatal(err)
	}
	if err := turn.recordEvent("message_end", json.RawMessage(`{"type":"message_end","message":{"role":"assistant","content":"owned result","stopReason":"stop"}}`)); err != nil {
		t.Fatal(err)
	}
	if err := turn.recordEvent("agent_end", json.RawMessage(`{"type":"agent_end","isTerminal":true}`)); err != nil {
		t.Fatal(err)
	}
	retired := turn.recordEvent("agent_start", json.RawMessage(`{"type":"agent_start"}`))
	if retired == nil {
		t.Fatal("foreign post-terminal work was accepted")
	}
	wrapper.cancel(retired)
	close(held.release)
	if err := <-waited; err != nil {
		t.Fatalf("held terminal wait = %v", err)
	}
	if !turn.hasResult() || !errors.Is(turn.retirement, retired) {
		t.Fatalf("terminal/retirement = %v, %v", turn.hasResult(), turn.retirement)
	}
}

func TestOMPAssistantDecoderRejectsUnknownStopAndUsesTextParts(t *testing.T) {
	assistant, ok, err := decodeOMPAssistant(json.RawMessage(`{"type":"message_end","message":{"role":"assistant","content":[{"type":"thinking","thinking":"hidden"},{"type":"text","text":"visible"}],"stopReason":"stop"}}`))
	if err != nil || !ok || assistant.text != "visible" {
		t.Fatalf("assistant = %+v, %v, %v", assistant, ok, err)
	}
	if _, _, err = decodeOMPAssistant(json.RawMessage(`{"type":"message_end","message":{"role":"assistant","content":"old","stopReason":"mystery"}}`)); err == nil {
		t.Fatal("unknown stop reason accepted")
	}
	if _, err = decodeOMPTerminal(json.RawMessage(`{"type":"agent_end","messages":[],"messageCount":2}`)); err == nil {
		t.Fatal("elided agent_end was accepted without terminal authority")
	}
	if terminal, err := decodeOMPTerminal(json.RawMessage(`{"type":"agent_end","isTerminal":false}`)); err != nil || terminal {
		t.Fatalf("nonterminal agent_end = %v, %v", terminal, err)
	}
	for name, body := range map[string]string{
		"null content": `{"type":"message_end","message":{"role":"assistant","content":null,"stopReason":"stop"}}`,
		"null text":    `{"type":"message_end","message":{"role":"assistant","content":[{"type":"text","text":null}],"stopReason":"stop"}}`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, _, err := decodeOMPAssistant(json.RawMessage(body)); err == nil {
				t.Fatal("null assistant text was accepted")
			}
		})
	}
}
