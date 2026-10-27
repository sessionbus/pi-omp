// SPDX-License-Identifier: MIT

package omp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"

	sessionkit "github.com/antst/sessionbus/bus/sdk/go"
	"github.com/sessionbus/peer-common/host"
)

const maxOMPTurnUIRequests = 256
const maxOMPAssistantMessages = 256
const maxOMPAssistantBytes = 8 << 20

var errOMPRunInterrupted = errors.New("OMP Run interrupted before native admission")
var errOMPUIUnsettled = errors.New("OMP native UI cancellation remained unsettled at turn completion")

type ompAssistant struct {
	text       string
	stopReason string
}

type ompNativeTurn struct {
	owner   *Wrapper
	run     *sessionkit.Run
	prompt  string
	binding OwnerBinding
	native  *nativePrompt
	window  *ownerPreflightWindow

	mu               sync.Mutex
	changed          chan struct{}
	failure          error
	submitted        bool
	preflight        bool
	preflightToken   string
	starts           int
	yielded          bool
	noAgent          bool
	promptResultSeen bool
	promptResultID   string
	promptInvoked    bool
	assistants       []ompAssistant
	assistantBytes   int
	result           sessionkit.TurnResult
	resultReady      bool
	promptFailure    *nativePromptFailure
	retirement       error
	finalized        bool
	uiClosed         bool
	pendingUI        int
	uiIDs            map[string]bool
	uiContext        context.Context
	uiCancel         context.CancelCauseFunc
	interruptOnce    sync.Once
	interruptDone    chan struct{}
	interruptStarted bool
	interruptError   error
	report           func(sessionkit.DeliveryReceipt, error) error
}

func newOMPNativeTurn(owner *Wrapper, run *sessionkit.Run, prompt string, binding OwnerBinding) *ompNativeTurn {
	uiContext, uiCancel := context.WithCancelCause(owner.ctx)
	return &ompNativeTurn{
		owner: owner, run: run, prompt: prompt, binding: binding,
		changed: make(chan struct{}), uiIDs: make(map[string]bool), uiContext: uiContext, uiCancel: uiCancel,
		interruptDone: make(chan struct{}),
	}
}

func (turn *ompNativeTurn) signalLocked() {
	close(turn.changed)
	turn.changed = make(chan struct{})
}

func (turn *ompNativeTurn) failLocked(err error) {
	turn.failure = errors.Join(turn.failure, err)
	turn.signalLocked()
}

func (turn *ompNativeTurn) fail(err error) {
	turn.mu.Lock()
	turn.failLocked(err)
	turn.mu.Unlock()
}

// recordPreflight takes our own preflight from the Run's window. A foreign
// preflight recorded before it fails the Run: once OMP admitted our prompt it
// was not busy with work of its own, so that earlier start was not ours.
func (turn *ompNativeTurn) recordPreflight(token string, foreign int) error {
	turn.mu.Lock()
	defer turn.mu.Unlock()
	if turn.failure != nil {
		return turn.failure
	}
	if foreign != 0 {
		err := fmt.Errorf("OMP native preflight does not match the owned Run (%d foreign preflights before it)", foreign)
		turn.failLocked(err)
		return err
	}
	if turn.preflight || !validOwnerToken(token) || turn.noAgent {
		err := errors.New("OMP native preflight is outside the owned Run")
		turn.failLocked(err)
		return err
	}
	turn.preflight = true
	turn.preflightToken = token
	turn.settleLocked()
	turn.signalLocked()
	return nil
}

// ownEndedLocked reports whether our prompt's work is over. OMP reports the
// prompt at its run's first yielded agent_end (rpc-prompt-results.ts:129-147),
// so later runs, until and after our prompt_result, are OMP's own work.
func (turn *ompNativeTurn) ownEndedLocked() bool {
	return turn.yielded || turn.noAgent || turn.resultReady || turn.finalized || turn.promptFailure != nil
}

// settleLocked ends the Run at our own agent-invoked prompt_result once our
// preflight and run start are known. The snapshot taken at our run's yielded
// agent_end is the result.
func (turn *ompNativeTurn) settleLocked() {
	if turn.resultReady || !turn.promptResultSeen || !turn.promptInvoked || !turn.preflight || turn.starts == 0 {
		return
	}
	if !turn.yielded {
		turn.failLocked(errors.New("OMP native prompt_result preceded the owned run's end"))
		return
	}
	turn.resultReady = true
}

func (turn *ompNativeTurn) recordEvent(kind string, raw json.RawMessage) error {
	turn.mu.Lock()
	defer turn.mu.Unlock()
	if turn.failure != nil {
		return turn.failure
	}
	switch kind {
	case "agent_start", "message_end", "agent_end":
		// A run event before our run starts or after it yielded is OMP's own
		// work (owner decision 2026-10-15): it is not ours and does not end
		// the lane.
		if turn.ownEndedLocked() || kind != "agent_start" && turn.starts == 0 {
			return nil
		}
	default:
		if turn.resultReady || turn.finalized {
			err := fmt.Errorf("OMP native emitted %s after the owned terminal", kind)
			turn.retirement = errors.Join(turn.retirement, err)
			turn.signalLocked()
			return err
		}
	}
	switch kind {
	case "agent_start":
		turn.starts++
	case "message_end":
		assistant, ok, err := decodeOMPAssistant(raw)
		if err != nil {
			turn.failLocked(err)
			return err
		}
		if ok {
			if len(turn.assistants) >= maxOMPAssistantMessages || turn.assistantBytes > maxOMPAssistantBytes-len(assistant.text) {
				err = errors.New("OMP native assistant result exceeds its owned bound")
				turn.failLocked(err)
				return err
			}
			turn.assistants = append(turn.assistants, assistant)
			turn.assistantBytes += len(assistant.text)
		}
	case "agent_end":
		yielded, err := decodeOMPYield(raw)
		if err != nil {
			turn.failLocked(err)
			return err
		}
		if yielded {
			turn.yielded = true
			turn.uiClosed = true
			if len(turn.assistants) == 0 {
				err = errors.New("OMP terminal has no current assistant result")
				turn.failLocked(err)
				return err
			}
			assistant := turn.assistants[len(turn.assistants)-1]
			turn.result.Result, turn.result.NativeStopReason = assistant.text, assistant.stopReason
			switch assistant.stopReason {
			case "stop":
				turn.result.Outcome = "completed"
			case "aborted":
				turn.result.Outcome = "interrupted"
			case "error", "length", "toolUse":
				turn.result.Outcome = "failed"
			}
			turn.settleLocked()
		}
	case "prompt_result":
		invoked, err := turn.decodePromptResult(raw)
		if err != nil {
			turn.failLocked(err)
			return err
		}
		if !invoked {
			if turn.starts != 0 || turn.preflight || turn.yielded || len(turn.assistants) != 0 {
				err = errors.New("OMP no-agent result contradicts native Run evidence")
				turn.failLocked(err)
				return err
			}
			turn.noAgent = true
			turn.uiClosed = true
			turn.result = sessionkit.TurnResult{Outcome: "completed", NativeStopReason: "no_agent"}
			turn.resultReady = true
		} else {
			turn.settleLocked()
		}
	case "extension_error":
		var event struct {
			Event string `json:"event"`
			Error string `json:"error"`
		}
		_ = json.Unmarshal(raw, &event)
		err := fmt.Errorf("OMP managed extension failed during %s: %s", event.Event, event.Error)
		turn.failLocked(err)
		return err
	}
	turn.signalLocked()
	return nil
}

func (turn *ompNativeTurn) decodePromptResult(raw json.RawMessage) (bool, error) {
	var event struct {
		ID           *string         `json:"id"`
		AgentInvoked json.RawMessage `json:"agentInvoked"`
	}
	if json.Unmarshal(raw, &event) != nil || event.ID == nil || !validOwnerID(*event.ID) || turn.promptResultSeen ||
		(turn.native != nil && *event.ID != turn.native.id) {
		return false, errors.New("OMP native prompt_result is invalid")
	}
	trimmed := bytes.TrimSpace(event.AgentInvoked)
	var invoked bool
	if json.Unmarshal(trimmed, &invoked) != nil || (!bytes.Equal(trimmed, []byte("true")) && !bytes.Equal(trimmed, []byte("false"))) {
		return false, errors.New("OMP native prompt_result is invalid")
	}
	turn.promptResultSeen = true
	turn.promptResultID = *event.ID
	turn.promptInvoked = invoked
	return invoked, nil
}

// decodeOMPYield applies OMP's own prompt-report rule, `yielded ??
// isTerminal !== false` (rpc-prompt-results.ts:136). OMP 18.11.0 writes both
// flags (agent-session.ts:5049-5070).
func decodeOMPYield(raw json.RawMessage) (bool, error) {
	var object map[string]json.RawMessage
	if json.Unmarshal(raw, &object) != nil || object == nil {
		return false, errors.New("OMP native agent_end is invalid")
	}
	terminal, ok, err := decodeOMPFlag(object, "isTerminal")
	if err != nil {
		return false, err
	}
	if !ok {
		return false, errors.New("OMP native agent_end omitted its terminal authority")
	}
	yielded, ok, err := decodeOMPFlag(object, "yielded")
	if err != nil {
		return false, err
	}
	if !ok {
		return terminal, nil
	}
	return yielded, nil
}

func decodeOMPFlag(object map[string]json.RawMessage, key string) (value, ok bool, err error) {
	raw, ok := object[key]
	if !ok {
		return false, false, nil
	}
	trimmed := bytes.TrimSpace(raw)
	if json.Unmarshal(trimmed, &value) != nil || (!bytes.Equal(trimmed, []byte("true")) && !bytes.Equal(trimmed, []byte("false"))) {
		return false, true, fmt.Errorf("OMP native agent_end %s flag is invalid", key)
	}
	return value, true, nil
}

func decodeOMPAssistant(raw json.RawMessage) (ompAssistant, bool, error) {
	var event struct {
		Message struct {
			Role       string          `json:"role"`
			Content    json.RawMessage `json:"content"`
			StopReason string          `json:"stopReason"`
		} `json:"message"`
	}
	if json.Unmarshal(raw, &event) != nil || event.Message.Role == "" {
		return ompAssistant{}, false, errors.New("OMP native message_end is invalid")
	}
	if event.Message.Role != "assistant" {
		return ompAssistant{}, false, nil
	}
	text, err := ompMessageText(event.Message.Content)
	if err != nil || !slicesContainsOMPStop(event.Message.StopReason) {
		if err == nil {
			err = fmt.Errorf("unknown OMP native stop reason %q", event.Message.StopReason)
		}
		return ompAssistant{}, false, err
	}
	return ompAssistant{text: text, stopReason: event.Message.StopReason}, true, nil
}

func ompMessageText(raw json.RawMessage) (string, error) {
	trimmed := bytes.TrimSpace(raw)
	var text string
	if len(trimmed) > 0 && trimmed[0] == '"' && json.Unmarshal(trimmed, &text) == nil {
		return text, nil
	}
	var parts []struct {
		Type string          `json:"type"`
		Text json.RawMessage `json:"text"`
	}
	if json.Unmarshal(raw, &parts) != nil || parts == nil {
		return "", errors.New("OMP native assistant content is invalid")
	}
	var result strings.Builder
	for _, part := range parts {
		if part.Type != "text" {
			continue
		}
		trimmed = bytes.TrimSpace(part.Text)
		if len(trimmed) == 0 || trimmed[0] != '"' || json.Unmarshal(trimmed, &text) != nil {
			return "", errors.New("OMP native assistant text is invalid")
		}
		result.WriteString(text)
	}
	return result.String(), nil
}

func slicesContainsOMPStop(value string) bool {
	switch value {
	case "stop", "aborted", "error", "length", "toolUse":
		return true
	default:
		return false
	}
}

func (turn *ompNativeTurn) admission() (bool, bool, <-chan struct{}, error) {
	turn.mu.Lock()
	defer turn.mu.Unlock()
	if turn.failure != nil {
		return false, false, turn.changed, turn.failure
	}
	return turn.preflight && turn.starts > 0, turn.noAgent, turn.changed, nil
}

func (turn *ompNativeTurn) completion() (bool, <-chan struct{}, error) {
	turn.mu.Lock()
	defer turn.mu.Unlock()
	if turn.failure != nil {
		return false, turn.changed, turn.failure
	}
	if turn.promptFailure != nil {
		return false, turn.changed, turn.promptFailure
	}
	return turn.resultReady && turn.pendingUI == 0, turn.changed, nil
}

func (turn *ompNativeTurn) hasResult() bool {
	turn.mu.Lock()
	defer turn.mu.Unlock()
	return turn.resultReady
}

// failBeforeRun records OMP's own late answer to our admitted prompt as the
// Run's failure when OMP's run for it never started: no run start, yield or
// assistant message. Our preflight may already be recorded, since OMP emits
// before_agent_start before it hands the prompt to the agent. Any other late
// failure is left to the caller.
func (turn *ompNativeTurn) failBeforeRun(err error) bool {
	var native *nativeRPCError
	if !errors.As(err, &native) || native.Command != "prompt" {
		return false
	}
	turn.mu.Lock()
	defer turn.mu.Unlock()
	if turn.failure != nil || turn.starts != 0 || turn.yielded || len(turn.assistants) != 0 ||
		turn.promptResultSeen || turn.noAgent || turn.resultReady {
		return false
	}
	turn.promptFailure = &nativePromptFailure{err: err}
	turn.signalLocked()
	return true
}

func (turn *ompNativeTurn) awaitCompletion(ctx context.Context) error {
	for {
		ready, changed, err := turn.completion()
		if err != nil || ready {
			return err
		}
		select {
		case <-ctx.Done():
			if turn.hasResult() {
				return nil
			}
			return context.Cause(ctx)
		case <-turn.owner.ctx.Done():
			if turn.hasResult() {
				return nil
			}
			return context.Cause(turn.owner.ctx)
		case <-turn.native.Late():
			if turn.hasResult() {
				// Wait owns Finish and will distinguish a correlated late
				// prompt failure from transport retirement after the snapshot.
				return nil
			}
			err := turn.native.Finish()
			if err == nil {
				err = errors.New("OMP native prompt correlation ended before terminal")
			}
			turn.fail(err)
			return err
		case <-changed:
		}
	}
}

func (turn *ompNativeTurn) finishPrompt(err error) error {
	if turn.native == nil {
		return err
	}
	finishErr := turn.native.Finish()
	turn.mu.Lock()
	if turn.resultReady && finishErr != nil && turn.retirement != nil && errors.Is(turn.retirement, finishErr) {
		// A later foreign event deliberately retired the RPC transport. The
		// already snapshotted native terminal remains authoritative; a distinct
		// correlated late prompt error still wins.
		finishErr = nil
	}
	turn.mu.Unlock()
	return errors.Join(err, finishErr)
}

// cancelUI cancels a dialog of our run and joins that cancellation before the
// Run's result. owned=false means our run's dialogs are closed, so the dialog
// is OMP's own work and the caller cancels it without a Run to join.
func (turn *ompNativeTurn) cancelUI(requestID string) (owned bool, err error) {
	turn.mu.Lock()
	if turn.failure != nil {
		err := turn.failure
		turn.mu.Unlock()
		return true, err
	}
	if turn.uiClosed {
		turn.mu.Unlock()
		return false, nil
	}
	if turn.uiIDs[requestID] || len(turn.uiIDs) >= maxOMPTurnUIRequests {
		err := errors.New("OMP native extension UI request is outside its owned bound")
		turn.failLocked(err)
		turn.mu.Unlock()
		return true, err
	}
	turn.uiIDs[requestID] = true
	turn.pendingUI++
	turn.signalLocked()
	turn.mu.Unlock()
	go func() {
		err := turn.owner.owner.rpc.CancelUI(turn.uiContext, requestID)
		turn.mu.Lock()
		turn.pendingUI--
		if err != nil {
			if cause := context.Cause(turn.uiContext); cause != nil {
				err = errors.Join(err, cause)
			}
			turn.failLocked(err)
		} else {
			turn.signalLocked()
		}
		turn.mu.Unlock()
	}()
	return true, nil
}

func (turn *ompNativeTurn) joinOwnedWork(cause error) {
	turn.mu.Lock()
	turn.uiClosed = true
	turn.signalLocked()
	turn.mu.Unlock()
	if cause == nil {
		cause = errOMPUIUnsettled
	}
	turn.uiCancel(cause)
	for {
		turn.mu.Lock()
		pending, changed, interruptStarted := turn.pendingUI, turn.changed, turn.interruptStarted
		turn.mu.Unlock()
		if pending == 0 {
			if interruptStarted {
				<-turn.interruptDone
			}
			return
		}
		<-changed
	}
}

func (turn *ompNativeTurn) Wait(ctx context.Context) (result sessionkit.TurnResult, err error) {
	defer turn.owner.finishNativeTurn(turn)
	turn.mu.Lock()
	admitted, noAgent := turn.preflight && turn.starts > 0, turn.noAgent
	turn.mu.Unlock()
	if admitted && turn.report != nil {
		err = turn.report(sessionkit.DeliveryReceipt{Disposition: "injected"}, nil)
	} else if noAgent && turn.report != nil {
		err = turn.report(sessionkit.DeliveryReceipt{Disposition: "rejected", Reason: "native_submission_refused"}, nil)
	}
	if err == nil && turn.run != nil && turn.run.Interrupted() && admitted {
		_ = turn.Interrupt(ctx)
	}
	if err == nil {
		err = turn.awaitCompletion(ctx)
	}
	err = turn.finishPrompt(err)
	// Join every UI cancellation and interrupt admitted by this turn before the
	// final failure/result snapshot. Their completion may record an ownership
	// failure after the native terminal event was received.
	joinCause := err
	if joinCause == nil {
		joinCause = errors.Join(context.Cause(ctx), errOMPUIUnsettled)
	}
	turn.joinOwnedWork(joinCause)
	turn.mu.Lock()
	interruptStarted := turn.interruptStarted
	if interruptStarted {
		err = errors.Join(err, turn.interruptError)
	}
	if turn.failure != nil {
		err = errors.Join(err, turn.failure)
	} else if err == nil {
		result = turn.result
	}
	if err == nil {
		turn.finalized = true
	}
	turn.uiClosed = true
	turn.signalLocked()
	retirement := turn.retirement
	promptFailure, failure := turn.promptFailure, turn.failure
	turn.mu.Unlock()
	if promptFailure != nil && failure == nil && !interruptStarted && retirement == nil {
		// OMP failed our prompt before its run: only the Run fails.
		return sessionkit.TurnResult{}, promptFailure
	}
	if err != nil {
		turn.owner.lose(err)
	} else if retirement != nil {
		turn.owner.lose(retirement)
	}
	return result, err
}

func (turn *ompNativeTurn) Interrupt(ctx context.Context) error {
	turn.interruptOnce.Do(func() {
		turn.mu.Lock()
		turn.interruptStarted = true
		terminal := turn.ownEndedLocked()
		turn.mu.Unlock()
		defer close(turn.interruptDone)
		if !terminal {
			turn.interruptError = turn.owner.owner.rpc.Call(ctx, "abort", nil, nil)
		}
	})
	<-turn.interruptDone
	return turn.interruptError
}

func (p *Wrapper) observeNative(raw json.RawMessage) error {
	var envelope struct {
		Type string `json:"type"`
	}
	if json.Unmarshal(raw, &envelope) != nil || !validOwnerText(envelope.Type, 128) || envelope.Type == "" {
		return errors.New("OMP native event is invalid")
	}
	if envelope.Type == "extension_ui_request" {
		return p.observeNativeUI(raw)
	}
	if envelope.Type == "extension_error" {
		managed, err := p.managedExtensionError(raw)
		if err != nil || !managed {
			return err
		}
	}
	switch envelope.Type {
	case "agent_start", "message_end", "agent_end", "prompt_result", "extension_error":
	default:
		return nil
	}
	p.mu.Lock()
	turn, closing := p.active, p.closing
	p.mu.Unlock()
	if closing {
		return nil
	}
	if turn == nil {
		if envelope.Type == "agent_start" || envelope.Type == "message_end" || envelope.Type == "agent_end" {
			// OMP's own work (owner decision 2026-10-15) does not end the lane.
			return nil
		}
		return fmt.Errorf("OMP native emitted %s outside an owned Run", envelope.Type)
	}
	return turn.recordEvent(envelope.Type, raw)
}

func (p *Wrapper) managedExtensionError(raw json.RawMessage) (bool, error) {
	var event struct {
		ExtensionPath string `json:"extensionPath"`
		Event         string `json:"event"`
		Error         string `json:"error"`
	}
	if json.Unmarshal(raw, &event) != nil || !validOwnerText(event.ExtensionPath, 32<<10) ||
		!validOwnerText(event.Event, 128) || !validOwnerText(event.Error, 4096) || event.Event == "" {
		return false, errors.New("OMP native extension error is invalid")
	}
	return event.ExtensionPath == p.extension, nil
}

func (p *Wrapper) observeNativeUI(raw json.RawMessage) error {
	var event struct {
		ID     string `json:"id"`
		Method string `json:"method"`
	}
	if json.Unmarshal(raw, &event) != nil || !validOwnerID(event.ID) || !validOwnerText(event.Method, 32) || event.Method == "" {
		return errors.New("OMP native extension UI request is invalid")
	}
	switch event.Method {
	case "cancel", "notify", "open_url", "setStatus", "setWidget", "setTitle", "set_editor_text":
		// These are native fire-and-forget notifications. Session-start hooks can
		// emit them after RPC readiness but before Open publishes its owner.
		return nil
	case "select", "confirm", "input", "editor":
	default:
		return fmt.Errorf("OMP native requested unknown extension UI method %q", event.Method)
	}
	p.mu.Lock()
	turn, opened, closing, owner, ctx := p.active, p.opened, p.closing, p.owner, p.ctx
	p.mu.Unlock()
	if !opened || closing || owner == nil {
		return fmt.Errorf("OMP native requested unsupported startup UI method %q", event.Method)
	}
	if turn != nil {
		if owned, err := turn.cancelUI(event.ID); owned {
			return err
		}
	}
	// A dialog of OMP's own work is cancelled the same way, with no Run to
	// join, as for Pi.
	rpc := owner.rpc
	go func() { _ = rpc.CancelUI(ctx, event.ID) }()
	return nil
}

func (p *Wrapper) startNativeTurn(ctx context.Context, run *sessionkit.Run, prompt string) (*ompNativeTurn, error) {
	if !validOMPArgument(prompt, nativeRPCPhysicalFrameBytes-1024) || strings.TrimSpace(prompt) == "" {
		return nil, &nativePromptFailure{err: errors.New("OMP lane prompt is empty or exceeds its native bound")}
	}
	p.mu.Lock()
	if !p.opened || p.closing || p.failure != nil || p.ctx.Err() != nil || p.active != nil || p.owner == nil {
		p.mu.Unlock()
		return nil, errors.New("OMP lane is unavailable or busy")
	}
	binding := p.binding
	turn := newOMPNativeTurn(p, run, prompt, binding)
	p.active = turn
	owner := p.owner
	p.mu.Unlock()
	// The window opens before OMP can read our prompt, so our own preflight
	// always falls inside it; see ownerPreflightWindow.
	window, err := owner.registry.openPreflightWindow(binding.OwnerToken, binding.SessionID, prompt)
	if err != nil {
		p.finishNativeTurn(turn)
		return nil, err
	}
	turn.window = window
	native, submitted, callErr := owner.rpc.StartPrompt(ctx, prompt)
	var refusal *nativePromptRefusal
	if errors.As(callErr, &refusal) {
		// Nothing of the prompt reached the agent. The turn and anything it
		// recorded from OMP's own work are dropped; the lane stays.
		p.finishNativeTurn(turn)
		return nil, refusal
	}
	turn.mu.Lock()
	turn.submitted = submitted
	turn.native = native
	if native != nil && turn.promptResultSeen && turn.promptResultID != native.id {
		turn.failLocked(errors.New("OMP native prompt_result changed request identity"))
	}
	if callErr != nil && submitted {
		turn.failLocked(callErr)
	}
	turn.mu.Unlock()
	if callErr != nil {
		if submitted {
			return turn, nil
		}
		p.finishNativeTurn(turn)
		return nil, callErr
	}
	invoked, known := native.ImmediateAgentInvoked()
	if known {
		turn.mu.Lock()
		if turn.failure != nil {
		} else if turn.promptResultSeen && turn.promptInvoked != invoked {
			turn.failLocked(errors.New("OMP prompt results contradict each other"))
		} else if !invoked && (turn.starts != 0 || turn.preflight || turn.yielded || len(turn.assistants) != 0) {
			turn.failLocked(errors.New("OMP immediate no-agent result contradicts native Run evidence"))
		} else if !invoked {
			turn.noAgent = true
			turn.uiClosed = true
			turn.result = sessionkit.TurnResult{Outcome: "completed", NativeStopReason: "no_agent"}
			turn.resultReady = true
			turn.signalLocked()
		}
		turn.mu.Unlock()
		if !invoked {
			return turn, nil
		}
	}
	for {
		admitted, noAgent, changed, admissionErr := turn.admission()
		if admissionErr != nil {
			return turn, nil
		}
		if admitted {
			turn.mu.Lock()
			turn.signalLocked()
			turn.mu.Unlock()
			if run != nil {
				run.Admitted()
			}
			return turn, nil
		}
		if noAgent {
			return turn, nil
		}
		// Read the turn before the window: a preflight applied before our
		// prompt_result was recorded is then always seen below.
		turn.mu.Lock()
		preflight, ended := turn.preflight, turn.promptResultSeen && turn.promptInvoked
		turn.mu.Unlock()
		runToken, foreign, preflightChanged, windowErr := window.observe()
		if windowErr != nil {
			turn.fail(windowErr)
			return turn, nil
		}
		if runToken != "" && !preflight {
			if err := turn.recordPreflight(runToken, foreign); err != nil {
				return turn, nil
			}
			continue
		}
		if ended {
			// Our agent-invoked prompt_result is OMP's report that all work of
			// our prompt settled; before this change the Run waited until it
			// was cancelled. Without our preflight by then, the prompt OMP ran
			// was not ours as sent.
			if runToken == "" {
				turn.fail(fmt.Errorf("OMP owned preflight is missing or rewritten (%d foreign preflights in the Run window)", foreign))
			} else {
				turn.fail(errors.New("OMP native reported agent work for the owned prompt without a run"))
			}
			return turn, nil
		}
		select {
		case <-preflightChanged:
		case <-native.Late():
			err := native.Finish()
			if err == nil {
				err = errors.New("OMP native prompt correlation ended before admission")
			} else if turn.failBeforeRun(err) {
				return turn, nil
			}
			turn.fail(err)
			return turn, nil
		case <-ctx.Done():
			turn.fail(context.Cause(ctx))
			return turn, nil
		case <-p.ctx.Done():
			turn.fail(context.Cause(p.ctx))
			return turn, nil
		case <-owner.registry.Done():
		case <-changed:
		}
	}
}

func (p *Wrapper) finishNativeTurn(turn *ompNativeTurn) {
	if turn.window != nil {
		turn.window.close()
	}
	p.mu.Lock()
	if p.active == turn {
		p.active = nil
	}
	p.mu.Unlock()
}

func (p *Wrapper) Interrupt(ctx context.Context, run *sessionkit.Run) error {
	p.mu.Lock()
	matching, turn, cancel := p.run == run, p.active, p.runCancel
	p.mu.Unlock()
	if matching {
		if turn != nil {
			turn.mu.Lock()
			admitted := turn.preflight && turn.starts > 0
			turn.mu.Unlock()
			if admitted {
				return turn.Interrupt(ctx)
			}
		}
		if cancel != nil {
			cancel(errOMPRunInterrupted)
			return nil
		}
	}
	return p.handoff.Interrupt(ctx, run)
}

func (p *Wrapper) Run(ctx context.Context, run *sessionkit.Run, seed sessionkit.RunInput) (result sessionkit.TurnResult, err error) {
	if run == nil {
		return result, errors.New("OMP Run requires its owned operation")
	}
	reject := func(failure error) (sessionkit.TurnResult, error) {
		if seed.Delivery != nil {
			if reportErr := run.ReportDelivery(sessionkit.DeliveryReceipt{Disposition: "rejected", Reason: "not_submitted"}, nil); reportErr != nil {
				failure = errors.Join(failure, reportErr)
			}
		}
		return sessionkit.TurnResult{}, failure
	}
	if ctx == nil || (seed.Text == nil) == (seed.Delivery == nil) {
		return reject(errors.New("OMP Run requires exactly one input"))
	}
	if err = ctx.Err(); err != nil {
		return reject(err)
	}
	if run.Interrupted() {
		return reject(errors.New("OMP Run was interrupted before native submission"))
	}
	var input string
	if input, err = runInput(seed); err != nil {
		return reject(err)
	}
	p.mu.Lock()
	if p.run != nil {
		select {
		case <-p.run.Done():
			p.run = nil
		default:
		}
	}
	if !p.opened || p.closing || p.failure != nil || p.run != nil {
		p.mu.Unlock()
		return reject(errors.New("OMP lane is unavailable or busy"))
	}
	runCtx, cancelRun := context.WithCancelCause(ctx)
	p.run, p.runCancel = run, cancelRun
	p.mu.Unlock()
	defer func() {
		cancelRun(nil)
		p.mu.Lock()
		if p.run == run {
			p.runCancel = nil
		}
		p.mu.Unlock()
	}()
	nativeSubmitted, nativeAdmitted := false, false
	result, err = p.handoff.Run(runCtx, run, input, func(startCtx context.Context, prompt string) (host.Turn, error) {
		turn, startErr := p.startNativeTurn(startCtx, run, prompt)
		if turn != nil {
			turn.mu.Lock()
			nativeSubmitted = turn.submitted
			nativeAdmitted = turn.preflight && turn.starts > 0
			if seed.Delivery != nil {
				turn.report = run.ReportDelivery
			}
			turn.mu.Unlock()
		}
		return turn, startErr
	})
	var refusal *nativePromptRefusal
	var failure *nativePromptFailure
	if errors.As(err, &refusal) || errors.As(err, &failure) {
		// The handoff returned its staged messages to the queue; the seed was
		// not submitted. As for Pi, the refused Run fails and the lane stays,
		// as does a Run whose prompt failed before OMP's run for it started.
		if seed.Delivery != nil {
			err = errors.Join(err, run.ReportDelivery(sessionkit.DeliveryReceipt{Disposition: "rejected", Reason: "not_submitted"}, nil))
		}
		return sessionkit.TurnResult{}, err
	}
	if !nativeAdmitted && errors.Is(context.Cause(runCtx), errOMPRunInterrupted) {
		var reportErr error
		if seed.Delivery != nil {
			if nativeSubmitted {
				reportErr = run.ReportDelivery(sessionkit.DeliveryReceipt{}, errOMPRunInterrupted)
			} else {
				reportErr = run.ReportDelivery(sessionkit.DeliveryReceipt{Disposition: "rejected", Reason: "not_submitted"}, nil)
			}
			err = errors.Join(err, reportErr)
		}
		p.lose(errOMPRunInterrupted)
		if reportErr != nil {
			return sessionkit.TurnResult{}, err
		}
		if err == nil || errors.Is(err, errOMPRunInterrupted) || errors.Is(err, context.Canceled) {
			return sessionkit.TurnResult{Outcome: "interrupted"}, nil
		}
	}
	if err == nil && !nativeSubmitted && !nativeAdmitted && result.Outcome == "interrupted" && seed.Delivery != nil {
		if reportErr := run.ReportDelivery(sessionkit.DeliveryReceipt{Disposition: "rejected", Reason: "not_submitted"}, nil); reportErr != nil {
			p.lose(reportErr)
			return sessionkit.TurnResult{}, reportErr
		}
	}
	if err != nil {
		p.lose(err)
		if seed.Delivery != nil {
			_ = run.ReportDelivery(sessionkit.DeliveryReceipt{}, err)
		}
	}
	return result, err
}

// runInput is a Run seed's prompt: the owner's text as given, or the rendered
// peer message with its file mentions escaped, so that OMP reads no file a
// peer names. Steered messages do not reach OMP's prompt path.
func runInput(seed sessionkit.RunInput) (string, error) {
	if seed.Text != nil {
		return *seed.Text, nil
	}
	rendered, err := host.RenderNativeMessage(*seed.Delivery)
	if err != nil {
		return "", err
	}
	return escapeFileMentions(rendered), nil
}
