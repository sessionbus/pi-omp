// SPDX-License-Identifier: MIT

package omp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	sessionkit "github.com/antst/sessionbus/bus/sdk/go"
)

// abandonNativeRPCPrompt writes a prompt and stops its start before OMP's
// first answer, as an interrupt does: the prompt becomes the abandoned one.
func abandonNativeRPCPrompt(t *testing.T, rpc *nativeRPC, peer *nativeRPCTestPeer) string {
	t.Helper()
	ctx, cancel := context.WithCancel(nativeRPCTestContext(t))
	type started struct {
		prompt    *nativePrompt
		submitted bool
		err       error
	}
	result := make(chan started, 1)
	go func() {
		prompt, submitted, err := rpc.StartPrompt(ctx, "abandoned prompt")
		result <- started{prompt, submitted, err}
	}()
	id := nativeRPCField(t, peer.read(t), "id")
	waitNativeRPCWritesSettled(t, rpc)
	cancel()
	got := <-result
	if got.prompt != nil || !got.submitted || !errors.Is(got.err, errNativePromptAbandoned) || !rpc.AbandonedLive() {
		t.Fatalf("abandoned start = prompt %v, submitted %v, %v; live %v", got.prompt != nil, got.submitted, got.err, rpc.AbandonedLive())
	}
	return id
}

// waitNativeRPCAbortsDecided waits for the abandoned prompt's aborts to end.
// A stale one ends at its guard; one that wrote an abort waits for an answer
// that never comes, and the test fails instead of hanging.
func waitNativeRPCAbortsDecided(t *testing.T, rpc *nativeRPC) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		rpc.aborts.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-nativeRPCTestContext(t).Done():
		t.Fatal("an abort for the abandoned prompt was written")
	}
}

func waitNativeRPCWritesSettled(t *testing.T, rpc *nativeRPC) {
	t.Helper()
	ctx := nativeRPCTestContext(t)
	for rpc.Stats().pendingWrites != 0 {
		select {
		case <-ctx.Done():
			t.Fatal("OMP command write did not settle")
		default:
			runtime.Gosched()
		}
	}
}

func waitNativeRPCAbandonedRetired(t *testing.T, rpc *nativeRPC) {
	t.Helper()
	ctx := nativeRPCTestContext(t)
	for rpc.AbandonedLive() {
		select {
		case <-ctx.Done():
			t.Fatal("abandoned prompt was not retired")
		default:
			runtime.Gosched()
		}
	}
}

func nativeRPCObserver() (func(json.RawMessage) error, func() []string) {
	var mu sync.Mutex
	var seen []string
	return func(raw json.RawMessage) error {
			mu.Lock()
			seen = append(seen, string(raw))
			mu.Unlock()
			return nil
		}, func() []string {
			mu.Lock()
			defer mu.Unlock()
			return slices.Clone(seen)
		}
}

// expectNativeRPCMarker writes a marker event and requires that it is the only
// frame the observer saw: nothing of the abandoned prompt reached it.
func expectNativeRPCMarker(t *testing.T, peer *nativeRPCTestPeer, seen func() []string) {
	t.Helper()
	peer.write(t, `{"type":"agent_end","marker":"after"}`)
	ctx := nativeRPCTestContext(t)
	for len(seen()) == 0 {
		select {
		case <-ctx.Done():
			t.Fatal("event after the abandoned prompt was not observed")
		default:
			runtime.Gosched()
		}
	}
	if got := seen(); len(got) != 1 || got[0] != `{"type":"agent_end","marker":"after"}` {
		t.Fatalf("observed = %q", got)
	}
}

// expectNativeRPCNextCommand runs one more call and requires that it, not an
// abort, is the next command OMP reads.
func expectNativeRPCNextCommand(t *testing.T, rpc *nativeRPC, peer *nativeRPCTestPeer) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- rpc.Call(nativeRPCTestContext(t), "get_state", nil, nil) }()
	command := peer.read(t)
	if nativeRPCField(t, command, "type") != "get_state" {
		t.Fatalf("next command = %#v", command)
	}
	peer.write(t, `{"id":"`+nativeRPCField(t, command, "id")+`","type":"response","command":"get_state","success":true,"data":{}}`)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestNativeRPCAbandonedPromptAbsorbsOMPsAnswerByID(t *testing.T) {
	t.Run("refused: its success and no-agent result, no abort", func(t *testing.T) {
		observe, seen := nativeRPCObserver()
		rpc, peer := newNativeRPCTest(t, observe, nativeRPCLimits{})
		id := abandonNativeRPCPrompt(t, rpc, peer)
		peer.write(t, `{"id":"`+id+`","type":"response","command":"prompt","success":false,"error":"Agent is busy"}`)
		peer.write(t, `{"id":"`+id+`","type":"response","command":"prompt","success":true}`)
		peer.write(t, `{"type":"prompt_result","id":"`+id+`","agentInvoked":false,"status":"error","sessionSettled":false}`)
		expectNativeRPCMarker(t, peer, seen)
		waitNativeRPCAbandonedRetired(t, rpc)
		expectNativeRPCNextCommand(t, rpc, peer)
		waitNativeRPCStats(t, rpc, nativeRPCStats{})
	})
	for name, data := range map[string]string{"admitted": `,"data":{"agentInvoked":true}`, "admission unknown": ``} {
		t.Run(name+": one abort, its late run and result absorbed", func(t *testing.T) {
			observe, seen := nativeRPCObserver()
			rpc, peer := newNativeRPCTest(t, observe, nativeRPCLimits{})
			id := abandonNativeRPCPrompt(t, rpc, peer)
			peer.write(t, `{"id":"`+id+`","type":"response","command":"prompt","success":true`+data+`}`)
			abort := peer.read(t)
			if nativeRPCField(t, abort, "type") != "abort" {
				t.Fatalf("command after the admitted answer = %#v", abort)
			}
			peer.write(t, `{"id":"`+nativeRPCField(t, abort, "id")+`","type":"response","command":"abort","success":true}`)
			peer.write(t, `{"id":"`+id+`","type":"response","command":"prompt","success":false,"error":"aborted"}`)
			peer.write(t, `{"type":"prompt_result","id":"`+id+`","agentInvoked":true,"status":"aborted","sessionSettled":true}`)
			waitNativeRPCAbandonedRetired(t, rpc)
			rpc.aborts.Wait()
			expectNativeRPCMarker(t, peer, seen)
			expectNativeRPCNextCommand(t, rpc, peer)
			waitNativeRPCStats(t, rpc, nativeRPCStats{})
		})
	}
	t.Run("resolved locally: retired at once, no abort", func(t *testing.T) {
		observe, seen := nativeRPCObserver()
		rpc, peer := newNativeRPCTest(t, observe, nativeRPCLimits{})
		id := abandonNativeRPCPrompt(t, rpc, peer)
		peer.write(t, `{"id":"`+id+`","type":"response","command":"prompt","success":true,"data":{"agentInvoked":false}}`)
		waitNativeRPCAbandonedRetired(t, rpc)
		expectNativeRPCMarker(t, peer, seen)
		expectNativeRPCNextCommand(t, rpc, peer)
	})
	t.Run("a second answer of the same kind is still a protocol error", func(t *testing.T) {
		rpc, peer := newNativeRPCTest(t, nil, nativeRPCLimits{})
		id := abandonNativeRPCPrompt(t, rpc, peer)
		peer.write(t, `{"id":"`+id+`","type":"response","command":"prompt","success":false,"error":"Agent is busy"}`)
		peer.write(t, `{"id":"`+id+`","type":"response","command":"prompt","success":false,"error":"Agent is busy"}`)
		waitNativeRPCDone(t, rpc)
		if err := rpc.Err(); !errors.Is(err, errNativeRPCProtocol) {
			t.Fatalf("duplicate answer = %v", err)
		}
	})
}

// The abort's guard runs where it is queued: an abort delayed past the
// abandoned prompt's retirement, or past a later prompt write, sends nothing.
func TestNativeRPCAbandonedPromptAbortIsGuardedAtItsQueueing(t *testing.T) {
	hold := func(rpc *nativeRPC) (<-chan struct{}, func()) {
		entered, release := make(chan struct{}), make(chan struct{})
		var once sync.Once
		rpc.abortStarted = func() {
			close(entered)
			<-release
		}
		return entered, func() { once.Do(func() { close(release) }) }
	}
	t.Run("retired, then a later prompt", func(t *testing.T) {
		rpc, peer := newNativeRPCTest(t, nil, nativeRPCLimits{})
		entered, release := hold(rpc)
		defer release()
		id := abandonNativeRPCPrompt(t, rpc, peer)
		peer.write(t, `{"id":"`+id+`","type":"response","command":"prompt","success":true,"data":{"agentInvoked":true}}`)
		<-entered
		peer.write(t, `{"type":"prompt_result","id":"`+id+`","agentInvoked":true,"status":"completed","sessionSettled":true}`)
		waitNativeRPCAbandonedRetired(t, rpc)
		started := make(chan error, 1)
		go func() {
			_, _, err := rpc.StartPrompt(nativeRPCTestContext(t), "later prompt")
			started <- err
		}()
		later := peer.read(t)
		if nativeRPCField(t, later, "type") != "prompt" {
			t.Fatalf("later command = %#v", later)
		}
		release()
		waitNativeRPCAbortsDecided(t, rpc)
		peer.write(t, `{"id":"`+nativeRPCField(t, later, "id")+`","type":"response","command":"prompt","success":true,"data":{"agentInvoked":false}}`)
		if err := <-started; err != nil {
			t.Fatal(err)
		}
		expectNativeRPCNextCommand(t, rpc, peer)
	})
	t.Run("a later prompt written while it is live", func(t *testing.T) {
		rpc, peer := newNativeRPCTest(t, nil, nativeRPCLimits{})
		entered, release := hold(rpc)
		defer release()
		id := abandonNativeRPCPrompt(t, rpc, peer)
		peer.write(t, `{"id":"`+id+`","type":"response","command":"prompt","success":true,"data":{"agentInvoked":true}}`)
		<-entered
		rpc.mu.Lock()
		rpc.promptWrites++
		rpc.mu.Unlock()
		release()
		waitNativeRPCAbortsDecided(t, rpc)
		expectNativeRPCNextCommand(t, rpc, peer)
		peer.write(t, `{"type":"prompt_result","id":"`+id+`","agentInvoked":true,"status":"completed","sessionSettled":true}`)
		waitNativeRPCAbandonedRetired(t, rpc)
	})
}

// After OMP's first answer, Abandon decides under the reader's lock: a
// prompt_result already passed to the observer goes to the turn, a late
// failure leaves only OMP's no-agent result to absorb.
func TestNativeRPCAbandonAfterTheFirstAnswer(t *testing.T) {
	start := func(t *testing.T, rpc *nativeRPC, peer *nativeRPCTestPeer) *nativePrompt {
		t.Helper()
		result := make(chan *nativePrompt, 1)
		go func() {
			prompt, _, err := rpc.StartPrompt(nativeRPCTestContext(t), "owned prompt")
			if err != nil {
				t.Error(err)
			}
			result <- prompt
		}()
		id := nativeRPCField(t, peer.read(t), "id")
		peer.write(t, `{"id":"`+id+`","type":"response","command":"prompt","success":true}`)
		return <-result
	}
	t.Run("not yet answered: abandoned and aborted", func(t *testing.T) {
		rpc, peer := newNativeRPCTest(t, nil, nativeRPCLimits{})
		prompt := start(t, rpc, peer)
		if seen, err := prompt.Abandon(); seen || err != nil || !rpc.AbandonedLive() {
			t.Fatalf("abandon = %v, %v; live %v", seen, err, rpc.AbandonedLive())
		}
		abort := peer.read(t)
		if nativeRPCField(t, abort, "type") != "abort" {
			t.Fatalf("command after abandon = %#v", abort)
		}
		peer.write(t, `{"id":"`+nativeRPCField(t, abort, "id")+`","type":"response","command":"abort","success":true}`)
		peer.write(t, `{"type":"prompt_result","id":"`+prompt.id+`","agentInvoked":true,"status":"aborted","sessionSettled":true}`)
		waitNativeRPCAbandonedRetired(t, rpc)
	})
	t.Run("its prompt_result already on its way to the turn", func(t *testing.T) {
		entered, release := make(chan struct{}), make(chan struct{})
		var once sync.Once
		rpc, peer := newNativeRPCTest(t, func(json.RawMessage) error {
			once.Do(func() { close(entered) })
			<-release
			return nil
		}, nativeRPCLimits{})
		defer func() {
			select {
			case <-release:
			default:
				close(release)
			}
		}()
		prompt := start(t, rpc, peer)
		peer.write(t, `{"type":"prompt_result","id":"`+prompt.id+`","agentInvoked":true,"status":"completed","sessionSettled":true}`)
		<-entered
		if seen, err := prompt.Abandon(); !seen || err != nil || rpc.AbandonedLive() {
			t.Fatalf("abandon = %v, %v; live %v", seen, err, rpc.AbandonedLive())
		}
		close(release)
		expectNativeRPCNextCommand(t, rpc, peer)
	})
	t.Run("after a late failure: awaits OMP's no-agent result, no abort", func(t *testing.T) {
		observe, seen := nativeRPCObserver()
		rpc, peer := newNativeRPCTest(t, observe, nativeRPCLimits{})
		prompt := start(t, rpc, peer)
		peer.write(t, `{"id":"`+prompt.id+`","type":"response","command":"prompt","success":false,"error":"No model"}`)
		waitNativeRPCPromptState(t, rpc, prompt.pending, true, true)
		if resultSeen, err := prompt.Abandon(); resultSeen || err != nil || !rpc.AbandonedLive() {
			t.Fatalf("abandon = %v, %v; live %v", resultSeen, err, rpc.AbandonedLive())
		}
		peer.write(t, `{"type":"prompt_result","id":"`+prompt.id+`","agentInvoked":false,"status":"error","sessionSettled":false}`)
		waitNativeRPCAbandonedRetired(t, rpc)
		expectNativeRPCMarker(t, peer, seen)
		expectNativeRPCNextCommand(t, rpc, peer)
		waitNativeRPCStats(t, rpc, nativeRPCStats{})
	})
}

// earlyStopOMPRun writes a lane prompt and interrupts its start before OMP's
// first answer; the stopped turn is returned before Wait.
func earlyStopOMPRun(t *testing.T, fixture *ompRunFixture, prompt string) (*ompNativeTurn, string) {
	t.Helper()
	ctx, cancel := context.WithCancelCause(nativeRPCTestContext(t))
	type started struct {
		turn *ompNativeTurn
		err  error
	}
	result := make(chan started, 1)
	go func() {
		turn, err := fixture.wrapper.startNativeTurn(ctx, nil, prompt)
		result <- started{turn, err}
	}()
	command := fixture.peer.read(t)
	if nativeRPCField(t, command, "type") != "prompt" {
		t.Fatalf("native command = %#v", command)
	}
	waitNativeRPCWritesSettled(t, fixture.rpc)
	cancel(errOMPRunInterrupted)
	start := <-result
	if start.err != nil || start.turn == nil {
		t.Fatalf("stopped start = %#v, %v", start.turn, start.err)
	}
	return start.turn, nativeRPCField(t, command, "id")
}

func ompLaneFailure(fixture *ompRunFixture) error {
	fixture.wrapper.mu.Lock()
	defer fixture.wrapper.mu.Unlock()
	return fixture.wrapper.failure
}

// An early stop ends only its Run. Until OMP's prompt_result for the stopped
// prompt, a new Run is refused busy before anything is written; then the
// lane serves. Before this change the early stop ended the lane.
func TestOMPEarlyStopRefusesBusyUntilOMPAnswersThenServes(t *testing.T) {
	fixture := newOMPRunFixture(t)
	turn, id := earlyStopOMPRun(t, fixture, "stopped prompt")
	_, err := turn.Wait(nativeRPCTestContext(t))
	var early ompEarlyStop
	if !errors.As(err, &early) {
		t.Fatalf("early-stopped Wait = %v", err)
	}
	if failure := ompLaneFailure(fixture); failure != nil {
		t.Fatalf("the early stop failed the lane: %v", failure)
	}

	_, err = fixture.wrapper.startNativeTurn(nativeRPCTestContext(t), nil, "refused prompt")
	var refusal *nativePromptRefusal
	if !errors.As(err, &refusal) || err.Error() != "OMP lane is unavailable or busy" {
		t.Fatalf("Run while the stopped prompt is live = %v", err)
	}
	fixture.wrapper.mu.Lock()
	active := fixture.wrapper.active
	fixture.wrapper.mu.Unlock()
	if active != nil || fixture.rpc.Stats().pendingWrites != 0 {
		t.Fatalf("refused Run left active %v, writes %d", active != nil, fixture.rpc.Stats().pendingWrites)
	}

	// OMP admits the stopped prompt: it is aborted, its run is OMP's own work
	// and its prompt_result is absorbed.
	fixture.peer.write(t, `{"id":`+mustOMPJSONText(t, id)+`,"type":"response","command":"prompt","success":true,"data":{"agentInvoked":true}}`)
	abort := fixture.peer.read(t)
	if nativeRPCField(t, abort, "type") != "abort" {
		t.Fatalf("command after the stopped prompt's answer = %#v", abort)
	}
	fixture.peer.write(t, `{"id":`+mustOMPJSONText(t, nativeRPCField(t, abort, "id"))+`,"type":"response","command":"abort","success":true}`)
	fixture.peer.write(t, `{"type":"agent_start"}`)
	fixture.peer.write(t, `{"type":"agent_end","messages":[],"isTerminal":true}`)
	fixture.promptResult(t, id, "aborted")
	waitNativeRPCAbandonedRetired(t, fixture.rpc)

	nextID, started := fixture.prompt(t, "next prompt", `{"agentInvoked":true}`)
	fixture.preflight(t, 1, "run-next", "next prompt")
	fixture.peer.write(t, `{"type":"agent_start"}`)
	fixture.peer.write(t, `{"type":"message_end","message":{"role":"assistant","content":"served","stopReason":"stop"}}`)
	fixture.peer.write(t, `{"type":"agent_end","messages":[],"isTerminal":true}`)
	fixture.promptResult(t, nextID, "completed")
	start := <-started
	if start.err != nil || start.turn == nil {
		t.Fatalf("next start = %#v, %v", start.turn, start.err)
	}
	result, err := start.turn.Wait(nativeRPCTestContext(t))
	if err != nil || result.Outcome != "completed" || result.Result != "served" {
		t.Fatalf("next result = %+v, %v", result, err)
	}
	if failure := ompLaneFailure(fixture); failure != nil {
		t.Fatalf("lane after the next Run: %v", failure)
	}
}

// A genuine failure during an early-stopped Run still ends the lane.
func TestOMPEarlyStopWithAGenuineFailureStillEndsTheLane(t *testing.T) {
	fixture := newOMPRunFixture(t)
	turn, _ := earlyStopOMPRun(t, fixture, "stopped prompt")
	fixture.peer.write(t, `{"type":"extension_error","extensionPath":`+mustOMPJSONText(t, fixture.wrapper.extension)+`,"event":"before_agent_start","error":"boom"}`)
	ctx := nativeRPCTestContext(t)
	for {
		turn.mu.Lock()
		failed := turn.failure != nil
		turn.mu.Unlock()
		if failed {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("managed extension failure was not recorded")
		default:
			runtime.Gosched()
		}
	}
	_, err := turn.Wait(nativeRPCTestContext(t))
	var early ompEarlyStop
	if err == nil || errors.As(err, &early) {
		t.Fatalf("failed early-stopped Wait = %v", err)
	}
	if ompLaneFailure(fixture) == nil {
		t.Fatal("a genuine failure did not end the lane")
	}
}

// A first success that stays registered for OMP's late failure is answered
// before the stop: abandoning it would drop the buffered answer, keep its
// bytes charged and miss the abort. The answer wins and the normal start
// follows, as for a refusal.
func TestNativeRPCAbandonKeepsABufferedFirstAnswer(t *testing.T) {
	rpc, peer := newNativeRPCTest(t, nil, nativeRPCLimits{})
	id, pending, _, err := rpc.admitPrompt(nativeRPCTestContext(t), map[string]any{"message": "owned prompt"})
	if err != nil {
		t.Fatal(err)
	}
	if got := nativeRPCField(t, peer.read(t), "id"); got != id {
		t.Fatalf("written prompt id = %q, want %q", got, id)
	}
	peer.write(t, `{"id":"`+id+`","type":"response","command":"prompt","success":true,"data":{"agentInvoked":true}}`)
	waitNativeRPCPromptState(t, rpc, pending, true, false)
	completed, answered, err := rpc.abandonPrompt(id, pending)
	if !answered || err != nil || rpc.AbandonedLive() {
		t.Fatalf("abandon after a buffered answer = answered %v, %v; live %v", answered, err, rpc.AbandonedLive())
	}
	prompt, submitted, err := rpc.completePromptStart(id, pending, completed)
	if err != nil || !submitted || prompt == nil {
		t.Fatalf("start from the buffered answer = %v, %v, %v", prompt != nil, submitted, err)
	}
	if invoked, known := prompt.ImmediateAgentInvoked(); !invoked || !known {
		t.Fatalf("buffered admission = %v, %v", invoked, known)
	}
	if err := prompt.Finish(); err != nil {
		t.Fatal(err)
	}
	waitNativeRPCStats(t, rpc, nativeRPCStats{})
}

// Only a prompt_result with agentInvoked true or false retires the abandoned
// prompt; another shape is a protocol error for the observer.
func TestNativeRPCAbandonedPromptResultMustBeWellFormed(t *testing.T) {
	for _, malformed := range []string{
		`{"type":"prompt_result","id":"%s"}`,
		`{"type":"prompt_result","id":"%s","agentInvoked":"true"}`,
	} {
		t.Run(malformed, func(t *testing.T) {
			observe, seen := nativeRPCObserver()
			rpc, peer := newNativeRPCTest(t, observe, nativeRPCLimits{})
			id := abandonNativeRPCPrompt(t, rpc, peer)
			peer.write(t, fmt.Sprintf(malformed, id))
			ctx := nativeRPCTestContext(t)
			for len(seen()) == 0 {
				select {
				case <-ctx.Done():
					t.Fatal("the malformed prompt_result did not reach the observer")
				default:
					runtime.Gosched()
				}
			}
			if !rpc.AbandonedLive() {
				t.Fatal("a malformed prompt_result retired the abandoned prompt")
			}
		})
	}
}

// After a late native failure, Abandon may discard only OMP's own answer; a
// lost transport stored in the same place still ends the lane.
func TestNativeRPCAbandonKeepsATransportFailure(t *testing.T) {
	rpc, peer := newNativeRPCTest(t, nil, nativeRPCLimits{})
	result := make(chan *nativePrompt, 1)
	go func() {
		prompt, _, err := rpc.StartPrompt(nativeRPCTestContext(t), "owned prompt")
		if err != nil {
			t.Error(err)
		}
		result <- prompt
	}()
	id := nativeRPCField(t, peer.read(t), "id")
	peer.write(t, `{"id":"`+id+`","type":"response","command":"prompt","success":true}`)
	prompt := <-result
	rpc.stop(errors.New("OMP transport lost"), false)
	waitNativeRPCPromptState(t, rpc, prompt.pending, true, true)
	if resultSeen, err := prompt.Abandon(); resultSeen || err == nil || rpc.AbandonedLive() {
		t.Fatalf("abandon after a lost transport = %v, %v; live %v", resultSeen, err, rpc.AbandonedLive())
	}
}

// A failed report drain is an early stop only when the interrupt cancelled
// it; any other failure keeps its own outcome.
func TestOMPStoppedByInterruptOnlyOnCancellation(t *testing.T) {
	interrupted, cancel := context.WithCancelCause(context.Background())
	cancel(errOMPRunInterrupted)
	if !stoppedByInterrupt(interrupted, context.Canceled) || !stoppedByInterrupt(interrupted, fmt.Errorf("drain: %w", context.Canceled)) {
		t.Fatal("a cancelled drain under the interrupt is not an early stop")
	}
	if stoppedByInterrupt(interrupted, errors.New("OMP bridge is unavailable")) {
		t.Fatal("a bridge failure under the interrupt became an early stop")
	}
	closing, cancelClosing := context.WithCancelCause(context.Background())
	cancelClosing(errors.New("closing"))
	if stoppedByInterrupt(closing, context.Canceled) {
		t.Fatal("a cancellation that is not the interrupt became an early stop")
	}
}

// The stopped prompt's preflight report can still be on the bridge when its
// prompt_result opens the gate. The next Run drains the extension's reports
// before its window opens, so that report is applied outside it. Before
// this change it landed in the next Run's window as foreign and ended the
// lane (DEV1's delayed-preflight ordering).
func TestOMPEarlyStopDrainsTheStoppedPromptsPreflightBeforeTheNextRun(t *testing.T) {
	fixture := newOMPRunFixture(t)
	turn, id := earlyStopOMPRun(t, fixture, "stopped prompt")
	var early ompEarlyStop
	if _, err := turn.Wait(nativeRPCTestContext(t)); !errors.As(err, &early) {
		t.Fatalf("early-stopped Wait = %v", err)
	}
	fixture.peer.write(t, `{"id":`+mustOMPJSONText(t, id)+`,"type":"response","command":"prompt","success":true,"data":{"agentInvoked":true}}`)
	abort := fixture.peer.read(t)
	if nativeRPCField(t, abort, "type") != "abort" {
		t.Fatalf("command after the stopped prompt's answer = %#v", abort)
	}
	fixture.peer.write(t, `{"id":`+mustOMPJSONText(t, nativeRPCField(t, abort, "id"))+`,"type":"response","command":"abort","success":true}`)
	fixture.promptResult(t, id, "aborted")
	waitNativeRPCAbandonedRetired(t, fixture.rpc)

	// The extension queued the stopped prompt's preflight before that
	// result; it is sent when the reports are drained, or else late.
	var sent atomic.Bool
	staleErr := make(chan error, 1)
	sendStale := func() {
		if sent.CompareAndSwap(false, true) {
			staleErr <- fixture.native.Call(ownerTestContext(t), "run.preflight", ownerPreflightRequest{
				OwnerToken: fixture.binding.OwnerToken, SessionID: fixture.binding.SessionID,
				ReportSequence: 1, RunToken: "run-stopped", Prompt: "stopped prompt",
			}, nil)
		}
	}
	fixture.owner.reportsDrained = sendStale
	nextID, started := fixture.prompt(t, "next prompt", `{"agentInvoked":true}`)
	sendStale()
	if err := <-staleErr; err != nil {
		t.Fatal(err)
	}
	fixture.preflight(t, 2, "run-next", "next prompt")
	fixture.peer.write(t, `{"type":"agent_start"}`)
	start := <-started
	if start.err != nil || start.turn == nil {
		t.Fatalf("next start = %#v, %v", start.turn, start.err)
	}
	start.turn.mu.Lock()
	failure := start.turn.failure
	start.turn.mu.Unlock()
	if failure != nil {
		t.Fatalf("the stopped prompt's preflight failed the next Run: %v", failure)
	}
	fixture.peer.write(t, `{"type":"message_end","message":{"role":"assistant","content":"served","stopReason":"stop"}}`)
	fixture.peer.write(t, `{"type":"agent_end","messages":[],"isTerminal":true}`)
	fixture.promptResult(t, nextID, "completed")
	result, err := start.turn.Wait(nativeRPCTestContext(t))
	if err != nil || result.Outcome != "completed" || result.Result != "served" {
		t.Fatalf("next result = %+v, %v", result, err)
	}
	if failure := ompLaneFailure(fixture); failure != nil {
		t.Fatalf("lane after the stopped prompt's late preflight: %v", failure)
	}
}

// startOMPRacedRun starts a lane Run whose prompt is written and waiting for
// OMP; Interrupt is driven by the caller through p.runCancel.
func startOMPRacedRun(t *testing.T, fixture *ompRunFixture, prompt string) (context.Context, context.CancelCauseFunc, <-chan struct {
	turn *ompNativeTurn
	err  error
}, string) {
	t.Helper()
	runCtx, cancelRun := context.WithCancelCause(nativeRPCTestContext(t))
	result := make(chan struct {
		turn *ompNativeTurn
		err  error
	}, 1)
	go func() {
		turn, err := fixture.wrapper.startNativeTurn(runCtx, nil, prompt)
		result <- struct {
			turn *ompNativeTurn
			err  error
		}{turn, err}
	}()
	command := fixture.peer.read(t)
	if nativeRPCField(t, command, "type") != "prompt" {
		t.Fatalf("native command = %#v", command)
	}
	waitNativeRPCWritesSettled(t, fixture.rpc)
	return runCtx, cancelRun, result, nativeRPCField(t, command, "id")
}

// interruptBetween calls Wrapper.Interrupt with a cancel that first runs
// between and then waits for the start, as if those frames won the race
// against the cancel.
func interruptBetween(t *testing.T, fixture *ompRunFixture, cancelRun context.CancelCauseFunc, result <-chan struct {
	turn *ompNativeTurn
	err  error
}, between func()) *ompNativeTurn {
	t.Helper()
	var turn *ompNativeTurn
	fixture.wrapper.mu.Lock()
	fixture.wrapper.runCancel = func(cause error) {
		between()
		start := <-result
		if start.err != nil || start.turn == nil {
			t.Errorf("raced start = %#v, %v", start.turn, start.err)
		}
		turn = start.turn
		cancelRun(cause)
	}
	fixture.wrapper.mu.Unlock()
	if err := fixture.wrapper.Interrupt(nativeRPCTestContext(t), nil); err != nil {
		t.Fatal(err)
	}
	if turn == nil {
		t.FailNow()
	}
	return turn
}

// OMP admits the prompt between Interrupt's decision and its cancel. The
// decision is taken under the lock admission is recorded under, so the Run
// still ends as an early stop and the lane serves the next Run. Before this
// change the late admission won, the cancelled Run failed and the lane ended.
func TestOMPInterruptKeepsTheLaneWhenAdmissionFollowsItsDecision(t *testing.T) {
	fixture := newOMPRunFixture(t)
	runCtx, cancelRun, result, id := startOMPRacedRun(t, fixture, "raced prompt")
	fixture.peer.write(t, `{"id":`+mustOMPJSONText(t, id)+`,"type":"response","command":"prompt","success":true,"data":{"agentInvoked":true}}`)
	turn := interruptBetween(t, fixture, cancelRun, result, func() {
		fixture.preflight(t, 1, "run-raced", "raced prompt")
		fixture.peer.write(t, `{"type":"agent_start"}`)
	})
	var early ompEarlyStop
	if _, err := turn.Wait(runCtx); !errors.As(err, &early) {
		t.Fatalf("raced Wait = %v; lane %v", err, ompLaneFailure(fixture))
	}
	abort := fixture.peer.read(t)
	if nativeRPCField(t, abort, "type") != "abort" {
		t.Fatalf("command after the raced stop = %#v", abort)
	}
	fixture.peer.write(t, `{"id":`+mustOMPJSONText(t, nativeRPCField(t, abort, "id"))+`,"type":"response","command":"abort","success":true}`)
	fixture.peer.write(t, `{"type":"agent_end","messages":[],"isTerminal":true}`)
	fixture.promptResult(t, id, "aborted")
	waitNativeRPCAbandonedRetired(t, fixture.rpc)

	nextID, started := fixture.prompt(t, "next prompt", `{"agentInvoked":true}`)
	fixture.preflight(t, 2, "run-next", "next prompt")
	fixture.peer.write(t, `{"type":"agent_start"}`)
	fixture.peer.write(t, `{"type":"message_end","message":{"role":"assistant","content":"served","stopReason":"stop"}}`)
	fixture.peer.write(t, `{"type":"agent_end","messages":[],"isTerminal":true}`)
	fixture.promptResult(t, nextID, "completed")
	start := <-started
	if start.err != nil || start.turn == nil {
		t.Fatalf("next start = %#v, %v", start.turn, start.err)
	}
	next, err := start.turn.Wait(nativeRPCTestContext(t))
	if err != nil || next.Outcome != "completed" {
		t.Fatalf("next result = %+v, %v", next, err)
	}
	if failure := ompLaneFailure(fixture); failure != nil {
		t.Fatalf("lane after the raced interrupt: %v", failure)
	}
}

// OMP answers agentInvoked:false after the interrupt won: the Run is an early
// stop whose seed gets its one receipt from Run, so Wait sends no
// native_submission_refused receipt, nothing is abandoned and the next Run
// works. Before this change Wait reported the seed refused and Run then
// reported it again.
func TestOMPInterruptBeforeANoAgentAnswerLeavesOneReceipt(t *testing.T) {
	fixture := newOMPRunFixture(t)
	runCtx, cancelRun, result, id := startOMPRacedRun(t, fixture, "/local")
	turn := interruptBetween(t, fixture, cancelRun, result, func() {
		fixture.peer.write(t, `{"id":`+mustOMPJSONText(t, id)+`,"type":"response","command":"prompt","success":true,"data":{"agentInvoked":false}}`)
	})
	var receipts []sessionkit.DeliveryReceipt
	turn.mu.Lock()
	turn.report = func(receipt sessionkit.DeliveryReceipt, _ error) error {
		receipts = append(receipts, receipt)
		return nil
	}
	turn.mu.Unlock()
	var early ompEarlyStop
	if _, err := turn.Wait(runCtx); !errors.As(err, &early) || len(receipts) != 0 {
		t.Fatalf("Wait = %v, receipts %+v", err, receipts)
	}
	if fixture.rpc.AbandonedLive() {
		t.Fatal("a prompt OMP already finished was abandoned")
	}
	nextID, started := fixture.prompt(t, "next prompt", `{"agentInvoked":true}`)
	fixture.preflight(t, 1, "run-next", "next prompt")
	fixture.peer.write(t, `{"type":"agent_start"}`)
	fixture.peer.write(t, `{"type":"message_end","message":{"role":"assistant","content":"served","stopReason":"stop"}}`)
	fixture.peer.write(t, `{"type":"agent_end","messages":[],"isTerminal":true}`)
	fixture.promptResult(t, nextID, "completed")
	start := <-started
	if start.err != nil || start.turn == nil {
		t.Fatalf("next start = %#v, %v", start.turn, start.err)
	}
	if next, err := start.turn.Wait(nativeRPCTestContext(t)); err != nil || next.Outcome != "completed" {
		t.Fatalf("next result = %+v, %v", next, err)
	}
	if failure := ompLaneFailure(fixture); failure != nil {
		t.Fatalf("lane after the no-agent early stop: %v", failure)
	}
}

// waitOMPPromptResultTaken waits until the reader has handed a prompt_result
// to the active turn, or the turn failed.
func waitOMPPromptResultTaken(t *testing.T, fixture *ompRunFixture) *ompNativeTurn {
	t.Helper()
	fixture.wrapper.mu.Lock()
	turn := fixture.wrapper.active
	fixture.wrapper.mu.Unlock()
	if turn == nil {
		t.Fatal("no active turn")
	}
	ctx := nativeRPCTestContext(t)
	for {
		turn.mu.Lock()
		seen, failure := turn.promptResultSeen, turn.failure
		turn.mu.Unlock()
		if seen || failure != nil {
			return turn
		}
		select {
		case <-ctx.Done():
			t.Fatal("the prompt_result did not reach the turn")
		default:
			runtime.Gosched()
		}
	}
}

// serveNextOMPRun runs the lane's next prompt to its end. Its prompt must be
// the next command OMP reads, so nothing, such as an abort, was written first.
func serveNextOMPRun(t *testing.T, fixture *ompRunFixture, sequence uint64) {
	t.Helper()
	nextID, started := fixture.prompt(t, "next prompt", `{"agentInvoked":true}`)
	fixture.preflight(t, sequence, "run-next", "next prompt")
	fixture.peer.write(t, `{"type":"agent_start"}`)
	fixture.peer.write(t, `{"type":"message_end","message":{"role":"assistant","content":"served","stopReason":"stop"}}`)
	fixture.peer.write(t, `{"type":"agent_end","messages":[],"isTerminal":true}`)
	fixture.promptResult(t, nextID, "completed")
	start := <-started
	if start.err != nil || start.turn == nil {
		t.Fatalf("next start = %#v, %v", start.turn, start.err)
	}
	result, err := start.turn.Wait(nativeRPCTestContext(t))
	if err != nil || result.Outcome != "completed" || result.Result != "served" {
		t.Fatalf("next result = %+v, %v", result, err)
	}
	if failure := ompLaneFailure(fixture); failure != nil {
		t.Fatalf("lane after the next Run: %v", failure)
	}
}

// The interrupt wins before admission; OMP's success for the prompt is
// buffered and its prompt_result reaches the turn before StartPrompt's
// continuation takes that success. The result is the stopped prompt's own
// end: the Run still ends as the interrupt, without the result, nothing is
// aborted and the next Run is served. Before this change the turn refused the
// result and the lane ended.
func TestOMPEarlyStopKeepsAResultReadBeforeItsStartResolves(t *testing.T) {
	fixture := newOMPRunFixture(t)
	ctx, cancel := context.WithCancelCause(nativeRPCTestContext(t))
	defer cancel(errors.New("test done"))
	turn := newOMPNativeTurn(fixture.wrapper, nil, "stopped prompt", fixture.binding)
	fixture.wrapper.mu.Lock()
	fixture.wrapper.active, fixture.wrapper.runCancel = turn, cancel
	fixture.wrapper.mu.Unlock()
	// StartPrompt up to its first answer; its continuation runs after the
	// result, as startNativeTurn resolves a success.
	id, pending, _, err := fixture.rpc.admitPrompt(ctx, map[string]any{"message": "stopped prompt"})
	if err != nil {
		t.Fatal(err)
	}
	if command := fixture.peer.read(t); nativeRPCField(t, command, "id") != id {
		t.Fatalf("native command = %#v", command)
	}
	waitNativeRPCWritesSettled(t, fixture.rpc)
	if err := fixture.wrapper.Interrupt(nativeRPCTestContext(t), nil); err != nil {
		t.Fatal(err)
	}
	fixture.peer.write(t, `{"id":`+mustOMPJSONText(t, id)+`,"type":"response","command":"prompt","success":true,"data":{"agentInvoked":true}}`)
	waitNativeRPCPromptState(t, fixture.rpc, pending, true, false)
	fixture.promptResult(t, id, "completed")
	waitOMPPromptResultTaken(t, fixture)
	native, submitted, err := fixture.rpc.completePromptStart(id, pending, <-pending.result)
	if err != nil || native == nil || !submitted {
		t.Fatalf("start = %v, %v, %v", native, submitted, err)
	}
	turn.mu.Lock()
	turn.submitted, turn.native = submitted, native
	failure := turn.failure
	turn.mu.Unlock()
	if failure != nil {
		t.Fatalf("the stopped prompt's result failed the turn: %v; lane %v", failure, ompLaneFailure(fixture))
	}
	result, err := turn.Wait(ctx)
	var early ompEarlyStop
	if !errors.As(err, &early) || result != (sessionkit.TurnResult{}) {
		t.Fatalf("Wait = %+v, %v", result, err)
	}
	if fixture.rpc.AbandonedLive() || ompLaneFailure(fixture) != nil {
		t.Fatalf("abandoned %v, lane %v", fixture.rpc.AbandonedLive(), ompLaneFailure(fixture))
	}
	serveNextOMPRun(t, fixture, 1)
}

// A prompt_result the turn took while StartPrompt was unresolved is checked
// where StartPrompt resolves, against the prompt OMP answered. The result is
// read ahead of OMP's answer so that the reader's order is fixed; the turn and
// the prompt then hold what the buffered answer above leaves them.
func TestOMPEarlyStopChecksAnEarlyResultWhereItsStartResolves(t *testing.T) {
	for name, own := range map[string]bool{"the stopped prompt's: kept": true, "another prompt's: the lane ends": false} {
		t.Run(name, func(t *testing.T) {
			fixture := newOMPRunFixture(t)
			runCtx, cancelRun, result, id := startOMPRacedRun(t, fixture, "stopped prompt")
			resultID := id
			if !own {
				resultID = id + "-other"
			}
			turn := interruptBetween(t, fixture, cancelRun, result, func() {
				fixture.promptResult(t, resultID, "completed")
				taken := waitOMPPromptResultTaken(t, fixture)
				taken.mu.Lock()
				refused := taken.failure != nil
				taken.mu.Unlock()
				// A turn that refused the result has already ended the lane,
				// and OMP's answer has nowhere to go.
				if !refused {
					fixture.peer.write(t, `{"id":`+mustOMPJSONText(t, id)+`,"type":"response","command":"prompt","success":true,"data":{"agentInvoked":true}}`)
				}
			})
			stopped, err := turn.Wait(runCtx)
			var early ompEarlyStop
			if !own {
				if err == nil || errors.As(err, &early) || ompLaneFailure(fixture) == nil {
					t.Fatalf("Wait = %+v, %v; lane %v", stopped, err, ompLaneFailure(fixture))
				}
				return
			}
			if !errors.As(err, &early) || stopped != (sessionkit.TurnResult{}) {
				t.Fatalf("Wait = %+v, %v; lane %v", stopped, err, ompLaneFailure(fixture))
			}
			if fixture.rpc.AbandonedLive() || ompLaneFailure(fixture) != nil {
				t.Fatalf("abandoned %v, lane %v", fixture.rpc.AbandonedLive(), ompLaneFailure(fixture))
			}
			serveNextOMPRun(t, fixture, 1)
		})
	}
}

// OMP answers a prompt before its prompt_result, so a prompt_result read
// while StartPrompt was unresolved belongs to no prompt of ours when
// StartPrompt answers none: the lane ends, as the reader ends it for an
// invalid result. Before this change a refused Run kept the lane with that
// result dropped, and an abandoned prompt waited for a result already taken,
// refusing every later Run as busy.
func TestOMPPromptResultWithoutAnAnsweredPromptEndsTheLane(t *testing.T) {
	t.Run("refused", func(t *testing.T) {
		fixture := newOMPRunFixture(t)
		_, _, result, id := startOMPRacedRun(t, fixture, "refused prompt")
		fixture.promptResult(t, id, "completed")
		waitOMPPromptResultTaken(t, fixture)
		fixture.peer.write(t, `{"id":`+mustOMPJSONText(t, id)+`,"type":"response","command":"prompt","success":false,"error":"Agent is busy"}`)
		start := <-result
		var refusal *nativePromptRefusal
		if start.turn != nil || !errors.As(start.err, &refusal) {
			t.Fatalf("refused start = %#v, %v", start.turn, start.err)
		}
		select {
		case <-fixture.rpc.Done():
		case <-nativeRPCTestContext(t).Done():
			t.Fatal("the lane stayed")
		}
		if err := fixture.rpc.Err(); !errors.Is(err, errOMPUnansweredPromptResult) {
			t.Fatalf("lane end = %v", err)
		}
	})
	t.Run("abandoned after an early stop", func(t *testing.T) {
		fixture := newOMPRunFixture(t)
		runCtx, cancelRun, result, id := startOMPRacedRun(t, fixture, "stopped prompt")
		fixture.promptResult(t, id, "completed")
		waitOMPPromptResultTaken(t, fixture)
		fixture.wrapper.mu.Lock()
		fixture.wrapper.runCancel = cancelRun
		fixture.wrapper.mu.Unlock()
		if err := fixture.wrapper.Interrupt(nativeRPCTestContext(t), nil); err != nil {
			t.Fatal(err)
		}
		start := <-result
		if start.err != nil || start.turn == nil {
			t.Fatalf("stopped start = %#v, %v", start.turn, start.err)
		}
		var early ompEarlyStop
		if _, err := start.turn.Wait(runCtx); !errors.Is(err, errOMPUnansweredPromptResult) || errors.As(err, &early) {
			t.Fatalf("Wait = %v", err)
		}
		if ompLaneFailure(fixture) == nil {
			t.Fatal("the lane stayed")
		}
	})
	t.Run("held by an event across a refusal", func(t *testing.T) {
		// observeNative takes p.active under p.mu and records outside it; a
		// test-only observer models a result whose event took the turn
		// before StartPrompt resolved and records after the turn was dropped.
		fixture := newOMPRunFixture(t)
		var held atomic.Pointer[ompNativeTurn]
		rpc, peer := newNativeRPCTest(t, func(raw json.RawMessage) error {
			var envelope struct {
				Type string `json:"type"`
			}
			if turn := held.Load(); turn != nil && json.Unmarshal(raw, &envelope) == nil && envelope.Type == "prompt_result" {
				return turn.recordEvent(envelope.Type, raw)
			}
			return fixture.wrapper.observeNative(raw)
		}, nativeRPCLimits{})
		fixture.rpc, fixture.peer, fixture.wrapper.owner.rpc = rpc, peer, rpc
		_, _, result, id := startOMPRacedRun(t, fixture, "refused prompt")
		fixture.wrapper.mu.Lock()
		held.Store(fixture.wrapper.active)
		fixture.wrapper.mu.Unlock()
		fixture.peer.write(t, `{"id":`+mustOMPJSONText(t, id)+`,"type":"response","command":"prompt","success":false,"error":"Agent is busy"}`)
		start := <-result
		var refusal *nativePromptRefusal
		if start.turn != nil || !errors.As(start.err, &refusal) || fixture.rpc.Err() != nil {
			t.Fatalf("refused start = %#v, %v; lane %v", start.turn, start.err, fixture.rpc.Err())
		}
		fixture.promptResult(t, id+"-other", "completed")
		select {
		case <-fixture.rpc.Done():
		case <-nativeRPCTestContext(t).Done():
			t.Fatal("the lane stayed")
		}
		if err := fixture.rpc.Err(); err == nil || !strings.Contains(err.Error(), "OMP native emitted prompt_result after the owned terminal") {
			t.Fatalf("lane end = %v", err)
		}
	})
	t.Run("direct: a sealed turn refuses a later result", func(t *testing.T) {
		// Helper tier: recordEvent on the dropped turn, without the reader.
		fixture := newOMPRunFixture(t)
		turn := newOMPNativeTurn(fixture.wrapper, nil, "refused prompt", fixture.binding)
		turn.sealWithoutPrompt(fixture.rpc)
		if fixture.rpc.Err() != nil {
			t.Fatalf("a refusal without a result ended the lane: %v", fixture.rpc.Err())
		}
		err := turn.recordEvent("prompt_result", json.RawMessage(`{"type":"prompt_result","id":"omp:2","agentInvoked":true,"status":"completed"}`))
		turn.mu.Lock()
		retirement := turn.retirement
		turn.mu.Unlock()
		if err == nil || retirement == nil {
			t.Fatalf("recordEvent = %v; retirement %v", err, retirement)
		}
		if err := turn.recordEvent("agent_start", json.RawMessage(`{"type":"agent_start"}`)); err != nil {
			t.Fatalf("OMP's own run event = %v", err)
		}
	})
	t.Run("another prompt's, after the start resolved without one", func(t *testing.T) {
		fixture := newOMPRunFixture(t)
		turn, id := earlyStopOMPRun(t, fixture, "stopped prompt")
		fixture.promptResult(t, id+"-other", "completed")
		select {
		case <-fixture.rpc.Done():
		case <-nativeRPCTestContext(t).Done():
			t.Fatal("the lane stayed")
		}
		if _, err := turn.Wait(nativeRPCTestContext(t)); err == nil || ompLaneFailure(fixture) == nil {
			t.Fatalf("Wait = %v; lane %v", err, ompLaneFailure(fixture))
		}
	})
}
