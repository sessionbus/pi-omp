// SPDX-License-Identifier: MIT

package omp

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	kit "github.com/antst/sessionbus/bus/sdk/go"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
	"github.com/sessionbus/peer-common/host"
	"github.com/sessionbus/peer-common/testsocket"
)

// failPrompt writes OMP's answer to a prompt it admitted and then failed
// before its run (rpc-mode.ts 18.12.2:1621-1622; rpc-prompt-results.ts:109-111,
// 149-166, 292-296): a late success:false for the same id, then prompt_result
// agentInvoked:false with status error.
func (fixture *ompRunFixture) failPrompt(t *testing.T, id, message string) {
	t.Helper()
	fixture.peer.write(t, `{"id":`+mustOMPJSONText(t, id)+`,"type":"response","command":"prompt","success":false,"error":`+mustOMPJSONText(t, message)+`}`)
	fixture.peer.write(t, `{"type":"prompt_result","id":`+mustOMPJSONText(t, id)+`,"agentInvoked":false,"status":"error","sessionSettled":true,"error":{"message":`+mustOMPJSONText(t, message)+`,"retryable":false}}`)
}

// OMP can admit a prompt and still fail it before its run, for example with
// no API key. Only the Run fails, with OMP's text; the lane stays and its
// next Run is unaffected by the failed prompt's trailing prompt_result. Before
// this change the failure ended the lane.
func TestOMPPromptFailedBeforeItsRunEndsOnlyTheRun(t *testing.T) {
	for name, preflight := range map[string]bool{"before our preflight": false, "after our preflight": true} {
		t.Run(name, func(t *testing.T) {
			fixture := newOMPRunFixture(t)
			id, started := fixture.prompt(t, "owned prompt", "")
			sequence := uint64(1)
			if preflight {
				fixture.preflight(t, sequence, "failed-run", "owned prompt")
				sequence++
			}
			fixture.failPrompt(t, id, "No API key found for deepseek.")
			start := <-started
			if start.err != nil || start.turn == nil {
				t.Fatalf("start = %#v, %v", start.turn, start.err)
			}
			_, err := start.turn.Wait(nativeRPCTestContext(t))
			var failure *nativePromptFailure
			if !errors.As(err, &failure) || !strings.Contains(err.Error(), "No API key found for deepseek.") {
				t.Fatalf("prompt failed before its run = %v", err)
			}
			fixture.requireLane(t)
			fixture.completeOwnedRun(t, sequence, "next prompt", "next answer")
			fixture.requireLane(t)
		})
	}
}

// Once OMP's run for our prompt started, a late failure still ends the lane.
func TestOMPPromptFailureAfterItsRunStartedStillEndsTheLane(t *testing.T) {
	fixture := newOMPRunFixture(t)
	id, started := fixture.prompt(t, "owned prompt", "")
	fixture.preflight(t, 1, "started-run", "owned prompt")
	fixture.peer.write(t, `{"type":"agent_start"}`)
	start := <-started
	if start.err != nil || start.turn == nil {
		t.Fatalf("start = %#v, %v", start.turn, start.err)
	}
	fixture.peer.write(t, `{"id":`+mustOMPJSONText(t, id)+`,"type":"response","command":"prompt","success":false,"error":"failed after start"}`)
	_, err := start.turn.Wait(nativeRPCTestContext(t))
	var failure *nativePromptFailure
	if err == nil || errors.As(err, &failure) || !strings.Contains(err.Error(), "failed after start") {
		t.Fatalf("late failure after the run started = %v", err)
	}
	fixture.wrapper.mu.Lock()
	lost := fixture.wrapper.failure
	fixture.wrapper.mu.Unlock()
	if lost == nil {
		t.Fatal("the lane stayed after a late failure of a started run")
	}
}

// OMP's run start can reach our turn before our preflight report does; the
// turn is then still waiting for admission. A late failure there followed a
// run start, so it still ends the lane. The control above fails only after
// admission, which does not reach this check.
func TestOMPPromptFailureAfterARunStartBeforeOurPreflightStillEndsTheLane(t *testing.T) {
	fixture := newOMPRunFixture(t)
	id, started := fixture.prompt(t, "owned prompt", "")
	fixture.peer.write(t, `{"type":"agent_start"}`)
	fixture.peer.write(t, `{"id":`+mustOMPJSONText(t, id)+`,"type":"response","command":"prompt","success":false,"error":"failed after start"}`)
	start := <-started
	if start.err != nil || start.turn == nil {
		t.Fatalf("start = %#v, %v", start.turn, start.err)
	}
	_, err := start.turn.Wait(nativeRPCTestContext(t))
	var failure *nativePromptFailure
	if err == nil || errors.As(err, &failure) || !strings.Contains(err.Error(), "failed after start") {
		t.Fatalf("late failure after a run start = %v", err)
	}
	fixture.wrapper.mu.Lock()
	lost := fixture.wrapper.failure
	fixture.wrapper.mu.Unlock()
	if lost == nil {
		t.Fatal("the lane stayed after a late failure that followed a run start")
	}
}

// A Run whose prompt our own validation refuses writes nothing to OMP; only
// the Run fails and the lane stays. Before this change the lane ended.
func TestOMPRunWithAnInvalidPromptEndsOnlyTheRun(t *testing.T) {
	for name, prompt := range map[string]string{
		"empty":                 " \n\t",
		"over the native bound": strings.Repeat("x", nativeRPCPhysicalFrameBytes-1023),
	} {
		t.Run(name, func(t *testing.T) {
			fixture := newOMPRunFixture(t)
			_, err := fixture.wrapper.Run(nativeRPCTestContext(t), &kit.Run{}, kit.RunInput{Text: &prompt})
			var failure *nativePromptFailure
			if !errors.As(err, &failure) || !strings.Contains(err.Error(), "empty or exceeds its native bound") {
				t.Fatalf("invalid prompt Run = %v", err)
			}
			fixture.requireLane(t)
			// The next prompt is the first command OMP reads.
			fixture.completeOwnedRun(t, 1, "next prompt", "next answer")
			fixture.requireLane(t)
		})
	}
}

// The late failure absorbs exactly one agentInvoked:false prompt_result for
// its own id; a repeat or another id's report still reaches the observer.
func TestNativeRPCLatePromptFailureAbsorbsItsNoAgentResultOnce(t *testing.T) {
	events := make(chan string, 4)
	rpc, peer := newNativeRPCTest(t, func(raw json.RawMessage) error {
		var event struct {
			Type   string `json:"type"`
			Status string `json:"status"`
		}
		if err := json.Unmarshal(raw, &event); err != nil {
			return err
		}
		events <- event.Type + ":" + event.Status
		return nil
	}, nativeRPCLimits{})
	type started struct {
		prompt *nativePrompt
		err    error
	}
	result := make(chan started, 1)
	go func() {
		prompt, _, err := rpc.StartPrompt(nativeRPCTestContext(t), "owned prompt")
		result <- started{prompt, err}
	}()
	id := nativeRPCField(t, peer.read(t), "id")
	peer.write(t, `{"id":"`+id+`","type":"response","command":"prompt","success":true}`)
	got := <-result
	if got.err != nil || got.prompt == nil {
		t.Fatalf("prompt start = %#v", got)
	}
	peer.write(t, `{"id":"`+id+`","type":"response","command":"prompt","success":false,"error":"No API key found for deepseek."}`)
	<-got.prompt.Late()
	peer.write(t, `{"type":"prompt_result","id":"`+id+`","agentInvoked":false,"status":"error"}`)
	peer.write(t, `{"type":"prompt_result","id":"`+id+`","agentInvoked":false,"status":"aborted"}`)
	peer.write(t, `{"type":"prompt_result","id":"omp:99","agentInvoked":false,"status":"completed"}`)
	for _, want := range []string{"prompt_result:aborted", "prompt_result:completed"} {
		if event := <-events; event != want {
			t.Fatalf("observed event = %q, want %q", event, want)
		}
	}
	var nativeErr *nativeRPCError
	if err := got.prompt.Finish(); !errors.As(err, &nativeErr) || nativeErr.Message != "No API key found for deepseek." {
		t.Fatalf("late prompt error = %v", err)
	}
	waitNativeRPCStats(t, rpc, nativeRPCStats{})
}

func TestNativeRPCStartPromptValidationIsAPromptFailure(t *testing.T) {
	rpc, _ := newNativeRPCTest(t, nil, nativeRPCLimits{})
	prompt, submitted, err := rpc.StartPrompt(nativeRPCTestContext(t), "")
	var failure *nativePromptFailure
	if prompt != nil || submitted || !errors.As(err, &failure) {
		t.Fatalf("empty prompt = %#v, %v, %v", prompt, submitted, err)
	}
}

// The Worker records a delivery seed whose prompt OMP failed before its run
// as not submitted, ends the Run unavailable with OMP's text, and keeps the
// lane for the next Run.
func TestOMPWorkerSeedFailedBeforeItsRunIsNotSubmittedAndLaneStays(t *testing.T) {
	fixture := newOMPRunFixture(t)
	listener, err := net.Listen("unix", filepath.Join(testsocket.Directory(t), "bus.sock"))
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv(host.SocketEnv, listener.Addr().String())
	t.Setenv(host.TokenEnv, "omp-failed-prompt-worker-token")
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
		if err := connection.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
			t.Fatal(err)
		}
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
	write := func(body []byte, err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		if _, writeErr := connection.Write(body); writeErr != nil {
			t.Fatal(writeErr)
		}
	}
	hello := read()
	write(protocol.ResultBytes(hello.ID, hello.Method, struct{}{}))
	write(protocol.RequestBytes(1, "session.open", kit.OpenRequest{Name: "managed@local", Groups: []string{}}))
	if frame := read(); frame.ID != 1 || frame.Error != nil {
		t.Fatalf("Open response = %+v", frame)
	}
	write(protocol.RequestBytes(2, "message.deliver", kit.DeliveryRequest{
		RunID: "g/1", MessageID: "delivery-failed-prompt", Body: "failed prompt delivery",
		From: kit.DeliverySource{SessionID: "sender@local", Product: "fixture", Groups: []string{}},
	}))
	id := nativeRPCField(t, fixture.peer.read(t), "id")
	fixture.peer.write(t, `{"id":`+mustOMPJSONText(t, id)+`,"type":"response","command":"prompt","success":true}`)
	fixture.failPrompt(t, id, "No API key found for deepseek.")
	receiptSeen, readySeen := false, false
	for !receiptSeen || !readySeen {
		frame := read()
		switch {
		case !frame.Request && frame.ID == 2:
			var receipt kit.DeliveryReceipt
			if frame.Error != nil || protocol.UnmarshalResult("message.deliver", frame.Result, &receipt) != nil ||
				receipt.Disposition != "rejected" || receipt.Reason != "not_submitted" {
				t.Fatalf("failed prompt delivery receipt = %+v, decoded %+v", frame, receipt)
			}
			receiptSeen = true
		case frame.Request && frame.Method == "turn.ready":
			var ready protocol.TurnReady
			if json.Unmarshal(frame.Params, &ready) != nil || ready.State != "unavailable" ||
				!strings.Contains(ready.Reason, "No API key found for deepseek.") {
				t.Fatalf("failed prompt terminal = %s", frame.Params)
			}
			write(protocol.ResultBytes(frame.ID, frame.Method, struct{}{}))
			readySeen = true
		default:
			t.Fatalf("unexpected Worker frame = %+v", frame)
		}
	}
	_ = connection.SetReadDeadline(time.Time{})
	fixture.completeOwnedRun(t, 1, "next prompt", "next answer")
	fixture.requireLane(t)
}
