// SPDX-License-Identifier: MIT

package pi

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"

	sessionkit "github.com/antst/sessionbus/bus/sdk/go"
	"github.com/sessionbus/peer-common/host"
)

const maxPiTurnUIRequests = 256

var errPiRunInterrupted = errors.New("Pi Run interrupted before native admission")

// errPiForwardedStartFailed ends the wait of a Run that follows a forwarded
// run which failed before it started.
var errPiForwardedStartFailed = errors.New("Pi forwarded prompt failed before its run started")

// piPromptHandled ends a Run whose prompt an extension received while Pi
// ran nothing for it: Pi answered "handled" before any preflight of ours, or
// the prompt an input extension forwarded failed before its run started.
// Only the Run ends; the handoff is consumed, not replayed, and a delivery
// seed is uncertain.
type piPromptHandled struct{ err error }

func (handled *piPromptHandled) Error() string { return handled.err.Error() }
func (handled *piPromptHandled) Unwrap() error { return handled.err }

// piPromptRefusal is Pi's error answer to the lane's own prompt command. Pi
// answers so only before it starts a run (rpc-mode.js:298-316), so nothing of
// the prompt was saved: the Run fails with that answer and the lane stays open.
type piPromptRefusal struct{ err error }

func (refusal *piPromptRefusal) Error() string { return refusal.err.Error() }
func (refusal *piPromptRefusal) Unwrap() error { return refusal.err }

// piPromptFailure is a lane prompt refused before anything reached Pi: our
// own validation, or a prompt that does not fit one native input frame once
// encoded. Like a refusal, only the Run fails and the lane stays.
type piPromptFailure struct{ err error }

func (failure *piPromptFailure) Error() string { return failure.err.Error() }
func (failure *piPromptFailure) Unwrap() error { return failure.err }

// newPiRunToken names one owned native run. The managed extension holds it
// from the run's preflight acknowledgement to its settle, and native.steer
// must carry it, so a steer that arrives after its Run ended is refused. A
// steer accepted during its Run that Pi has not read when the Run ends stays
// in Pi's queue and is read in the next Run.
func newPiRunToken() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(raw[:]), nil
}

type piNativeTurn struct {
	owner  *Wrapper
	run    *sessionkit.Run
	parent string
	cursor string
	prompt string

	mu               sync.Mutex
	changed          chan struct{}
	failure          error
	input            bool
	preflight        bool
	submitted        bool
	admitted         bool
	started          int
	nativeStarts     int
	user             bool
	userText         string
	settling         bool
	nativeSettled    bool
	token            string
	base             int
	leaf             string
	terminalOwned    bool
	finalized        bool
	uiClosed         bool
	pendingUI        int
	uiIDs            map[string]bool
	interruptOnce    sync.Once
	interruptDone    chan struct{}
	interruptStarted bool
	interruptError   error
	report           func(sessionkit.DeliveryReceipt, error) error
	// The forwarded-failure window: armed before our prompt is sent; a
	// forwarded prompt's start failure recorded in it (forwardFailed, with
	// Pi's text, which can be empty); following once Pi refused our prompt
	// for the forwarded run that our input started.
	forwardArmed     bool
	forwardFollowing bool
	forwardFailed    bool
	forwardFailure   string
	// handled: the Run's prompt was consumed (piPromptHandled); the turn is
	// no longer ours, and later frames, witnesses and dialogs are Pi's own work.
	handled error
}

func newPiNativeTurn(owner *Wrapper, run *sessionkit.Run, parent, cursor, prompt string) *piNativeTurn {
	return &piNativeTurn{
		owner: owner, run: run, parent: parent, cursor: cursor, prompt: prompt,
		changed: make(chan struct{}), interruptDone: make(chan struct{}), uiIDs: make(map[string]bool),
	}
}

func (turn *piNativeTurn) signalLocked() {
	close(turn.changed)
	turn.changed = make(chan struct{})
}

func (turn *piNativeTurn) failLocked(err error) {
	turn.failure = errors.Join(turn.failure, err)
	turn.signalLocked()
}

func (turn *piNativeTurn) fail(err error) {
	turn.mu.Lock()
	turn.failLocked(err)
	turn.mu.Unlock()
}

func (turn *piNativeTurn) recordInput(source, text string, settling bool) error {
	turn.mu.Lock()
	defer turn.mu.Unlock()
	if turn.failure != nil {
		return turn.failure
	}
	if turn.input || source != "rpc" || text != turn.prompt || settling {
		err := errors.New("Pi native input is outside the owned Run")
		turn.failLocked(err)
		return err
	}
	turn.input = true
	turn.signalLocked()
	return nil
}

// recordPreflight claims the run whose before_agent_start follows this Run's
// own input. leaf is the session leaf at that hook, after any work Pi ran
// before this run, and settled is the managed extension's count of settled Pi
// runs before it. A preflight that does not fit is Pi's own work when own is
// set, and answers no token.
func (turn *piNativeTurn) recordPreflight(prompt string, settling bool, leaf string, settled int, own bool) (string, error) {
	turn.mu.Lock()
	defer turn.mu.Unlock()
	if turn.failure != nil {
		return "", turn.failure
	}
	if turn.handled != nil {
		// Pi answered "handled" for this Run's prompt: a later preflight is
		// the extension's run, Pi's own work.
		return "", nil
	}
	if validPiText(prompt, 1<<20, true) && (!turn.input || turn.preflight || settling) && own {
		return "", nil
	}
	if !turn.input || turn.preflight || settling || !validPiText(prompt, 1<<20, true) {
		err := errors.New("Pi native preflight is outside the owned Run")
		turn.failLocked(err)
		return "", err
	}
	token, err := newPiRunToken()
	if err != nil {
		turn.failLocked(err)
		return "", err
	}
	turn.preflight, turn.prompt, turn.parent = true, prompt, leaf
	turn.base, turn.token = settled, token
	turn.signalLocked()
	return token, nil
}

func (turn *piNativeTurn) holds(token string) bool {
	turn.mu.Lock()
	defer turn.mu.Unlock()
	return turn.token != "" && turn.token == token
}

// ownsStdout reports whether a stdout run frame read after settled Pi runs
// belongs to this turn.
func (turn *piNativeTurn) ownsStdout(settled int) bool {
	turn.mu.Lock()
	defer turn.mu.Unlock()
	return turn.preflight && turn.base == settled
}

// recordForwardFailure keeps the first forwarded prompt's start failure seen
// inside this Run's own window: armed, our input and preflight witnessed, the
// frame in the preflight's settle generation, and no start yet.
func (turn *piNativeTurn) recordForwardFailure(settled int, text string) {
	turn.mu.Lock()
	defer turn.mu.Unlock()
	if !turn.forwardArmed || !turn.input || !turn.preflight || turn.base != settled ||
		turn.started != 0 || turn.nativeStarts != 0 || turn.failure != nil || turn.admitted || turn.forwardFailed {
		return
	}
	turn.forwardFailed, turn.forwardFailure = true, text
	turn.signalLocked()
}

// consumed reports whether the Run's prompt was consumed (piPromptHandled):
// the turn is no longer ours.
func (turn *piNativeTurn) consumed() bool {
	turn.mu.Lock()
	defer turn.mu.Unlock()
	return turn.handled != nil
}

func (turn *piNativeTurn) recordStart(settling bool) error {
	turn.mu.Lock()
	defer turn.mu.Unlock()
	if turn.handled != nil {
		// A witness that reached a consumed turn is Pi's own work.
		return nil
	}
	if turn.failure != nil {
		return turn.failure
	}
	if !turn.preflight || settling || turn.settling || turn.nativeSettled {
		err := errors.New("Pi native work started after the owned settling boundary")
		turn.failLocked(err)
		return err
	}
	turn.started++
	turn.signalLocked()
	return nil
}

// recordSettling marks the owned run settled. leaf is the session leaf in the
// managed extension's first settled handler: Pi has saved the run's messages
// by then, and work started after it is Pi's own.
func (turn *piNativeTurn) recordSettling(leaf string) error {
	turn.mu.Lock()
	defer turn.mu.Unlock()
	if turn.handled != nil {
		return nil
	}
	if turn.failure != nil {
		return turn.failure
	}
	if turn.started == 0 || turn.settling {
		err := errors.New("Pi native settling marker is outside the owned Run")
		turn.failLocked(err)
		return err
	}
	turn.settling, turn.leaf = true, leaf
	turn.signalLocked()
	return nil
}

func (turn *piNativeTurn) recordEvent(kind string, raw json.RawMessage) error {
	turn.mu.Lock()
	defer turn.mu.Unlock()
	if turn.handled != nil && kind != "extension_error" {
		// A run frame that reached a consumed turn, after its observer
		// sampled the turn before the consume, is Pi's own work. A failure of
		// our managed extension is not: it still fails the turn below.
		return nil
	}
	if turn.failure != nil {
		return turn.failure
	}
	switch kind {
	case "agent_start":
		if turn.nativeSettled {
			err := errors.New("Pi native work started after the owned settling boundary")
			turn.failLocked(err)
			return err
		}
		// The managed extension's run.start call and native stdout use separate
		// transports. A start emitted before the settling hook can therefore be
		// observed here after that hook. The settled frame is on the same stdout
		// stream and provides the boundary where both start counts must agree.
		turn.nativeStarts++
	case "message_start":
		if turn.nativeSettled {
			err := errors.New("Pi native message started after the owned terminal")
			turn.failLocked(err)
			return err
		}
		var event struct {
			Message struct {
				Role    string          `json:"role"`
				Content json.RawMessage `json:"content"`
			} `json:"message"`
		}
		if json.Unmarshal(raw, &event) != nil || event.Message.Role == "" {
			err := errors.New("Pi native message_start is invalid")
			turn.failLocked(err)
			return err
		}
		if event.Message.Role == "user" {
			text, err := messageText(event.Message.Content)
			if err != nil || turn.nativeStarts == 0 {
				if err == nil {
					err = errors.New("Pi native user is outside the owned Run")
				}
				turn.failLocked(err)
				return err
			}
			if !turn.user {
				turn.user, turn.userText = true, text
			}
		}
	case "agent_settled":
		if !turn.settling || turn.nativeSettled || turn.nativeStarts != turn.started {
			err := errors.New("Pi native settled without the owned settling marker")
			turn.failLocked(err)
			return err
		}
		turn.nativeSettled = true
	case "extension_error":
		event, err := decodePiExtensionError(raw)
		if err != nil {
			return err
		}
		if event.ExtensionPath == turn.owner.extension {
			failure := fmt.Errorf("Pi managed extension failed during %s: %s", event.Event, event.Error)
			turn.failLocked(failure)
			return failure
		}
	}
	turn.signalLocked()
	return nil
}

func (turn *piNativeTurn) admission() (bool, <-chan struct{}, error) {
	turn.mu.Lock()
	defer turn.mu.Unlock()
	if turn.failure != nil {
		return false, turn.changed, turn.failure
	}
	if turn.forwardFollowing && turn.forwardFailed {
		return false, turn.changed, errPiForwardedStartFailed
	}
	ready := turn.input && turn.preflight && turn.started > 0 && turn.nativeStarts > 0 &&
		turn.user && turn.userText == turn.prompt
	if turn.user && turn.userText != turn.prompt {
		err := errors.New("Pi native user does not match the owned preflight")
		turn.failLocked(err)
		return false, turn.changed, err
	}
	return ready, turn.changed, nil
}

func (turn *piNativeTurn) terminal() (bool, <-chan struct{}, error) {
	turn.mu.Lock()
	defer turn.mu.Unlock()
	ready := turn.settling && turn.nativeSettled && turn.pendingUI == 0
	if ready && !turn.terminalOwned {
		// Close UI admission at the same locked boundary which observes all
		// previously admitted cancellations joined. A late stdout request cannot
		// enter between the terminal predicate and result collection.
		turn.terminalOwned = true
		turn.uiClosed = true
		turn.signalLocked()
	}
	return ready, turn.changed, turn.failure
}

func (turn *piNativeTurn) cancelUI(requestID string) error {
	turn.mu.Lock()
	if turn.handled != nil {
		// A dialog that reached a consumed turn, after its observer sampled
		// the turn before the consume, is cancelled as Pi's own work.
		turn.mu.Unlock()
		go turn.owner.cancelOwnWorkUI(turn.owner.ctx, turn.owner.rpc, requestID)
		return nil
	}
	if turn.failure != nil {
		err := turn.failure
		turn.mu.Unlock()
		return err
	}
	if turn.uiClosed {
		err := errors.New("Pi native requested extension UI after the owned terminal")
		turn.failLocked(err)
		turn.mu.Unlock()
		return err
	}
	if turn.uiIDs[requestID] {
		err := errors.New("Pi native repeated an extension UI request")
		turn.failLocked(err)
		turn.mu.Unlock()
		return err
	}
	if len(turn.uiIDs) == maxPiTurnUIRequests {
		err := errors.New("Pi native exceeded the owned extension UI request bound")
		turn.failLocked(err)
		turn.mu.Unlock()
		return err
	}
	turn.uiIDs[requestID] = true
	turn.pendingUI++
	turn.signalLocked()
	turn.mu.Unlock()
	go func() {
		err := turn.owner.rpc.CancelUI(turn.owner.ctx, requestID)
		turn.mu.Lock()
		turn.pendingUI--
		if err != nil {
			turn.failLocked(err)
		} else {
			turn.signalLocked()
		}
		turn.mu.Unlock()
	}()
	return nil
}

func (turn *piNativeTurn) joinOwnedWork() {
	turn.mu.Lock()
	turn.uiClosed = true
	turn.signalLocked()
	turn.mu.Unlock()
	for {
		turn.mu.Lock()
		pending, changed := turn.pendingUI, turn.changed
		interruptStarted := turn.interruptStarted
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

func (turn *piNativeTurn) await(ctx context.Context, predicate func() (bool, <-chan struct{}, error)) error {
	for {
		ready, changed, err := predicate()
		if err != nil || ready {
			return err
		}
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-turn.owner.ctx.Done():
			return context.Cause(turn.owner.ctx)
		case <-changed:
		}
	}
}

func (turn *piNativeTurn) Wait(ctx context.Context) (result sessionkit.TurnResult, err error) {
	defer turn.owner.finishNativeTurn(turn)
	defer turn.joinOwnedWork()
	turn.mu.Lock()
	admitted, handled := turn.admitted, turn.handled
	turn.mu.Unlock()
	if handled != nil {
		// Pi ran nothing for this Run; no UI is owned without our preflight.
		return result, handled
	}
	if admitted && turn.report != nil {
		if err = turn.report(sessionkit.DeliveryReceipt{Disposition: "injected"}, nil); err != nil {
			turn.owner.lose(err)
			return result, err
		}
	}
	if turn.run != nil && turn.run.Interrupted() {
		_ = turn.Interrupt(ctx)
	}
	if err = turn.await(ctx, turn.terminal); err != nil {
		turn.owner.lose(err)
		return result, err
	}
	if turn.run != nil && turn.run.Interrupted() {
		_ = turn.Interrupt(ctx)
	}
	turn.mu.Lock()
	interruptStarted := turn.interruptStarted
	turn.mu.Unlock()
	if interruptStarted {
		<-turn.interruptDone
		if turn.interruptError != nil {
			turn.owner.lose(turn.interruptError)
			return result, turn.interruptError
		}
	}
	delta, err := turn.owner.readHistory(ctx, turn.cursor)
	if err == nil {
		delta, err = ownedHistory(delta, turn.parent, turn.leaf)
	}
	if err == nil {
		result, err = historyResult(turn.parent, delta, turn.prompt)
	}
	turn.mu.Lock()
	if turn.failure != nil {
		result, err = sessionkit.TurnResult{}, turn.failure
	} else if err == nil {
		turn.finalized = true
	}
	turn.uiClosed = true
	turn.signalLocked()
	turn.mu.Unlock()
	if err != nil {
		turn.owner.lose(err)
	}
	return result, err
}

func (turn *piNativeTurn) Interrupt(ctx context.Context) error {
	turn.interruptOnce.Do(func() {
		turn.mu.Lock()
		turn.interruptStarted = true
		terminal := turn.terminalOwned
		turn.mu.Unlock()
		defer close(turn.interruptDone)
		if !terminal {
			turn.interruptError = turn.owner.rpc.Call(ctx, "abort", nil, nil)
		}
	})
	<-turn.interruptDone
	return turn.interruptError
}

func (p *Wrapper) observeRunEvent(kind string, raw json.RawMessage) error {
	switch kind {
	case "agent_start", "message_start", "agent_settled", "extension_error":
	default:
		return nil
	}
	var extensionFailure error
	if kind == "extension_error" {
		event, err := decodePiExtensionError(raw)
		if err != nil {
			return err
		}
		if event.ExtensionPath == "<runtime>" && event.Event == "send_user_message" {
			// A forwarded prompt that failed before its run started reports
			// only this (agent-session.js 1.4.0:2774-2780). The active turn
			// records it inside its own window; it is never an observer error.
			p.mu.Lock()
			turn, closing, settled := p.active, p.closing, p.nativeSettles
			p.mu.Unlock()
			if turn != nil && !closing && !turn.consumed() {
				turn.recordForwardFailure(settled, event.Error)
			}
			return nil
		}
		if event.ExtensionPath != p.extension {
			return nil
		}
		extensionFailure = fmt.Errorf("Pi managed extension failed during %s: %s", event.Event, event.Error)
	}
	p.mu.Lock()
	turn, closing, settled, own := p.active, p.closing, p.nativeSettles, p.ownWork
	p.mu.Unlock()
	if closing {
		return nil
	}
	if turn != nil && turn.consumed() {
		// The extension owns what follows a consumed prompt.
		turn, own = nil, true
	}
	if extensionFailure != nil {
		if turn == nil {
			return extensionFailure
		}
		return turn.recordEvent(kind, raw)
	}
	// Pi runs one at a time and ends each with one agent_settled
	// (agent-session.js:1439, :690), which the managed extension counts before
	// it sends a preflight. A stdout frame belongs to the owned run only while
	// this count equals that preflight's, so Pi's own runs before and after it
	// stay out, however reading stdout and the bridge interleaves. Before Pi
	// can run work of its own, every frame must belong to an owned run.
	owned := turn != nil && turn.ownsStdout(settled)
	if !owned && !own {
		return fmt.Errorf("Pi native emitted %s outside an owned Run", kind)
	}
	var err error
	if owned {
		err = turn.recordEvent(kind, raw)
	}
	p.mu.Lock()
	switch kind {
	case "agent_start":
		p.nativeRunning = true
	case "agent_settled":
		p.nativeRunning = false
		p.nativeSettles++
		if owned && err == nil {
			p.ownWork = true
		}
	}
	p.signalNativeLocked()
	p.mu.Unlock()
	return err
}

func (p *Wrapper) signalNativeLocked() {
	if p.nativeChanged != nil {
		close(p.nativeChanged)
		p.nativeChanged = nil
	}
}

// awaitNativeIdle waits while stdout shows a Pi run without its settle. Pi
// refuses a prompt while it runs (agent-session.js:1634-1636), so the next
// Run waits for Pi's own work to settle; Pi answers a run that starts later.
func (p *Wrapper) awaitNativeIdle(ctx context.Context) error {
	for {
		p.mu.Lock()
		running := p.nativeRunning
		if p.nativeChanged == nil {
			p.nativeChanged = make(chan struct{})
		}
		changed := p.nativeChanged
		p.mu.Unlock()
		if !running {
			return nil
		}
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-p.ctx.Done():
			return context.Cause(p.ctx)
		case <-changed:
		}
	}
}

type piExtensionError struct {
	ExtensionPath string `json:"extensionPath"`
	Event         string `json:"event"`
	Error         string `json:"error"`
}

func decodePiExtensionError(raw json.RawMessage) (piExtensionError, error) {
	var event piExtensionError
	if json.Unmarshal(raw, &event) != nil || !validPiText(event.ExtensionPath, 4096, false) ||
		!validPiText(event.Event, 128, false) || !validPiText(event.Error, 4096, true) {
		return event, errors.New("Pi native extension error event is invalid")
	}
	return event, nil
}

func (p *Wrapper) observeNativeUI(raw json.RawMessage) error {
	var event struct {
		ID     string `json:"id"`
		Method string `json:"method"`
	}
	if json.Unmarshal(raw, &event) != nil || !validPiText(event.ID, 256, false) || !validPiText(event.Method, 32, false) {
		return errors.New("Pi native extension UI request is invalid")
	}
	switch event.Method {
	case "notify", "setStatus", "setWidget", "setTitle", "set_editor_text":
		// These are native fire-and-forget notifications. Native extensions can
		// emit them before Open publishes its owner or while the owner is idle.
		return nil
	case "select", "confirm", "input", "editor":
	default:
		return fmt.Errorf("Pi native requested unknown extension UI method %q", event.Method)
	}
	p.mu.Lock()
	turn, opened, closing := p.active, p.opened, p.closing
	settled, own, rpc, ctx := p.nativeSettles, p.ownWork, p.rpc, p.ctx
	p.mu.Unlock()
	if turn != nil && turn.consumed() {
		// A dialog after a consumed prompt is the extension's work.
		turn, own = nil, true
	}
	if !opened || closing || turn == nil && !own {
		return fmt.Errorf("Pi native requested unsupported startup UI method %q", event.Method)
	}
	// A dialog read while the open turn owns stdout is its run's, and before
	// Pi can run work of its own every dialog is. A dialog of Pi's own work
	// is cancelled the same way, with no Run to join.
	if turn != nil && (!own || turn.ownsStdout(settled)) {
		return turn.cancelUI(event.ID)
	}
	go p.cancelOwnWorkUI(ctx, rpc, event.ID)
	return nil
}

// cancelOwnWorkUI cancels a dialog of Pi's own work. If that cancellation
// fails while the lane is live, the dialog would block Pi's own work and
// every later Run, so the lane ends with the error through the usual loss
// path; nothing is retried. Close sets closing and ends native input before
// it cancels the wrapper context, so a cancellation under way then meets the
// RPC's closed error with the context live: that is the orderly end, not a
// loss. Every other error, and a closed RPC while the lane is not closing,
// still ends the lane.
func (p *Wrapper) cancelOwnWorkUI(ctx context.Context, rpc *nativeRPC, id string) {
	err := rpc.CancelUI(ctx, id)
	if err == nil || ctx.Err() != nil {
		return
	}
	if errors.Is(err, errNativeRPCClosed) {
		p.mu.Lock()
		closing := p.closing
		p.mu.Unlock()
		if closing {
			return
		}
	}
	p.lose(fmt.Errorf("Pi own-work dialog could not be cancelled: %w", err))
}

func (p *Wrapper) currentNativeTurn(sessionID string) (*piNativeTurn, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.opened || p.closing || p.failure != nil || p.active == nil || sessionID != p.id {
		return nil, errors.New("Pi Run witness has no matching native owner")
	}
	return p.active, nil
}

func (p *Wrapper) recordRunInput(sessionID, source, text string, settling bool) error {
	turn, err := p.currentNativeTurn(sessionID)
	if err != nil {
		return err
	}
	return turn.recordInput(source, text, settling)
}

// witnessTurn returns the open turn for a run witness and whether Pi can run
// work of its own: after an owned run settled or Pi refused an owned prompt
// (owner decision: the Run ends when Pi settles). A nil turn is then Pi's own
// work, which does not end the lane.
func (p *Wrapper) witnessTurn(sessionID string) (*piNativeTurn, bool, error) {
	p.mu.Lock()
	if !p.opened || p.closing || p.failure != nil || sessionID != p.id || (p.active == nil && !p.ownWork) {
		p.mu.Unlock()
		return nil, false, errors.New("Pi Run witness has no matching native owner")
	}
	turn, own := p.active, p.ownWork
	p.mu.Unlock()
	if turn != nil && turn.consumed() {
		// A consumed turn's witnesses are the extension's work.
		return nil, true, nil
	}
	return turn, own, nil
}

func (p *Wrapper) recordRunPreflight(sessionID, prompt string, settling bool, leaf string, settled int) (string, error) {
	turn, own, err := p.witnessTurn(sessionID)
	if err != nil || turn == nil {
		return "", err
	}
	return turn.recordPreflight(prompt, settling, leaf, settled, own)
}

// The managed extension sends start and settling witnesses only for a run
// whose preflight it got a token for. Another token is a run of a refused
// prompt, which is Pi's own work.
func (p *Wrapper) recordRunStart(sessionID, token string, settling bool) error {
	turn, own, err := p.witnessTurn(sessionID)
	if err != nil || turn == nil {
		return err
	}
	if !turn.holds(token) {
		if own {
			return nil
		}
		err = errors.New("Pi native start does not belong to the owned Run")
		turn.fail(err)
		return err
	}
	return turn.recordStart(settling)
}

func (p *Wrapper) recordRunSettling(sessionID, token, leaf string) error {
	turn, own, err := p.witnessTurn(sessionID)
	if err != nil || turn == nil {
		return err
	}
	if !turn.holds(token) {
		if own {
			return nil
		}
		err = errors.New("Pi native settling marker does not belong to the owned Run")
		turn.fail(err)
		return err
	}
	return turn.recordSettling(leaf)
}

func (p *Wrapper) readHistory(ctx context.Context, since string) (nativeHistory, error) {
	var raw json.RawMessage
	var fields map[string]any
	if since != "" {
		fields = map[string]any{"since": since}
	}
	if err := p.rpc.Call(ctx, "get_entries", fields, &raw); err != nil {
		return nativeHistory{}, err
	}
	return decodeHistory(raw)
}

func (p *Wrapper) startNativeTurn(ctx context.Context, run *sessionkit.Run, prompt string) (*piNativeTurn, error) {
	if !validPiText(prompt, 1<<20, false) || strings.TrimSpace(prompt) == "" {
		return nil, &piPromptFailure{err: errors.New("Pi lane prompt is empty or exceeds its native bound")}
	}
	if err := p.awaitNativeIdle(ctx); err != nil {
		return nil, err
	}
	pre, err := p.readHistory(ctx, "")
	if err != nil {
		return nil, err
	}
	p.mu.Lock()
	if !p.opened || p.closing || p.failure != nil || p.ctx.Err() != nil || p.active != nil {
		p.mu.Unlock()
		return nil, errors.New("Pi lane is unavailable or busy")
	}
	cursor := ""
	if len(pre.Entries) != 0 {
		cursor = pre.Entries[len(pre.Entries)-1].ID
	}
	turn := newPiNativeTurn(p, run, pre.Leaf, cursor, prompt)
	turn.forwardArmed = true
	p.active = turn
	p.mu.Unlock()
	var reply optionalRPCData
	submitted, callErr := p.rpc.callWithAdmission(ctx, "prompt", map[string]any{"message": prompt}, &reply)
	turn.mu.Lock()
	turn.submitted = submitted
	turn.mu.Unlock()
	var refused *nativeRPCError
	var refusal error
	if errors.As(callErr, &refused) {
		turn.mu.Lock()
		witnessed := turn.input && turn.preflight
		// A forwarded start failure ends the wait below, whether it was
		// recorded before this reply (the window is armed before the send)
		// or arrives after it.
		turn.forwardFollowing = witnessed
		turn.mu.Unlock()
		if witnessed {
			// Pi refused the prompt as busy, but the run it is running began
			// from our own submitted input inside this Run's preflight window
			// (an input extension forwarded it): the Run follows that run.
			refusal, callErr = callErr, nil
		} else {
			// Pi may be running work of its own: a run it started at the same
			// time is why it refuses a prompt with no streaming behaviour.
			p.mu.Lock()
			p.ownWork = true
			p.mu.Unlock()
			p.finishNativeTurn(turn)
			return nil, &piPromptRefusal{err: callErr}
		}
	}
	if callErr != nil {
		if submitted {
			turn.fail(callErr)
			return turn, nil
		}
		p.finishNativeTurn(turn)
		if errors.Is(callErr, errNativeRPCInputTooLarge) {
			// JSON escaping can grow a prompt within the raw bound past one
			// frame. Nothing was written, so only this Run fails.
			return nil, &piPromptFailure{err: fmt.Errorf("Pi lane prompt exceeds the %d-byte native input frame once encoded", p.rpc.limits.maxInputFrame)}
		}
		return nil, callErr
	}
	if err = context.Cause(ctx); err != nil {
		turn.fail(err)
		return turn, nil
	}
	// Pi answers a prompt with its disposition (rpc-mode.js 1.4.0:298-317).
	// Only an exact "handled" changes the path below; a missing, unknown or
	// other disposition keeps it. The reply data is Pi's open object, so only
	// the exact "disposition" key is read and added fields are ignored.
	var object map[string]json.RawMessage
	var disposition string
	handled := len(reply.data) != 0 && json.Unmarshal(reply.data, &object) == nil &&
		json.Unmarshal(object["disposition"], &disposition) == nil && disposition == "handled"
	if handled {
		// Closed as handled only if, under the same lock, neither our
		// preflight nor a failure came first; otherwise the paths below.
		if closed := p.handledBeforePreflight(turn, disposition); closed != nil {
			return closed, nil
		}
	}
	turn.mu.Lock()
	preflight := turn.input && turn.preflight
	if handled && preflight {
		// An extension consumed our prompt after our input and the run's
		// preflight: the Run follows that forwarded run, as after a witnessed
		// refusal.
		turn.forwardFollowing = true
	}
	turn.mu.Unlock()
	if handled && preflight {
		refusal = fmt.Errorf("Pi native prompt: %s", disposition)
	}
	if !preflight {
		turn.fail(errors.New("Pi native accepted a prompt without the managed Run preflight"))
		return turn, nil
	}
	if err = turn.await(ctx, turn.admission); err != nil {
		if errors.Is(err, errPiForwardedStartFailed) {
			turn.mu.Lock()
			forwardFailure := turn.forwardFailure
			turn.mu.Unlock()
			// Consumed, or the failed turn when an owned dialog's cancellation
			// failed (the existing loss path): the turn is returned either way,
			// so the handoff is consumed.
			p.refuseForwarded(turn, refusal, forwardFailure)
			return turn, nil
		}
		turn.mu.Lock()
		if turn.failure == nil {
			turn.failLocked(err)
		}
		turn.mu.Unlock()
		return turn, nil
	}
	turn.mu.Lock()
	if err = context.Cause(ctx); err != nil {
		turn.failLocked(err)
		turn.mu.Unlock()
		return turn, nil
	}
	turn.admitted = true
	turn.mu.Unlock()
	if run != nil {
		run.Admitted()
	}
	return turn, nil
}

// refuseForwarded consumes this Run: the prompt that an input extension
// forwarded from our input failed before its run started, so Pi ran nothing
// for this Run, while the extension received the content. A dialog in that
// window was ours: its cancellation is joined first and UI admission closed,
// as at the terminal. If a cancellation failed, the turn keeps its failure
// and takes the existing loss path; otherwise Wait answers piPromptHandled.
func (p *Wrapper) refuseForwarded(turn *piNativeTurn, refusal error, failure string) {
	// Join the owned dialogs and consume in one locked decision, so UI
	// admission is never closed on a turn that is not yet consumed.
	consumed := &piPromptHandled{err: fmt.Errorf("%w; the forwarded prompt failed before its run started: %s", refusal, failure)}
	committed := false
	err := turn.await(p.ctx, func() (bool, <-chan struct{}, error) {
		turn.mu.Lock()
		defer turn.mu.Unlock()
		if turn.failure != nil {
			return true, turn.changed, nil
		}
		if turn.pendingUI != 0 {
			return false, turn.changed, nil
		}
		turn.handled = consumed
		turn.terminalOwned, turn.uiClosed = true, true
		turn.signalLocked()
		committed = true
		return true, turn.changed, nil
	})
	if err != nil {
		turn.mu.Lock()
		if turn.failure == nil {
			turn.failLocked(err)
		}
		turn.mu.Unlock()
		return
	}
	if !committed {
		// A cancellation failed: the turn keeps its failure (the loss path).
		return
	}
	p.mu.Lock()
	p.ownWork = true
	p.mu.Unlock()
	p.finishNativeTurn(turn)
}

// handledBeforePreflight ends only this Run when Pi answered "handled" and
// no preflight of ours or failure came first: an extension consumed the
// prompt. The turn is returned, so the handoff is consumed and nothing is
// replayed, and Wait answers piPromptHandled. The refusal tombstone (own
// work, no active turn) records a later preflight as Pi's own work while no
// next Run has recorded its input; Pi's preflight carries no prompt
// identity beyond that.
func (p *Wrapper) handledBeforePreflight(turn *piNativeTurn, disposition string) *piNativeTurn {
	// Decide and close the owned window under one turn lock, before
	// detaching: if our preflight or a failure came first, the turn stays on
	// its path and nil is returned. Otherwise a preflight handler that
	// already holds this turn mints no token for it, no UI is admitted, and
	// an Interrupt sends no abort for the work that the extension now owns.
	// Dialogs our turn owned before the answer are joined first, inside the
	// same decision; only the consume closes UI admission.
	committed := false
	err := turn.await(p.ctx, func() (bool, <-chan struct{}, error) {
		turn.mu.Lock()
		defer turn.mu.Unlock()
		if turn.input && turn.preflight || turn.failure != nil {
			return true, turn.changed, nil
		}
		if turn.pendingUI != 0 {
			return false, turn.changed, nil
		}
		turn.handled = &piPromptHandled{err: fmt.Errorf("Pi native prompt: %s", disposition)}
		turn.terminalOwned, turn.uiClosed = true, true
		turn.signalLocked()
		committed = true
		return true, turn.changed, nil
	})
	if err != nil {
		turn.mu.Lock()
		if turn.failure == nil {
			turn.failLocked(err)
		}
		turn.mu.Unlock()
		return nil
	}
	if !committed {
		return nil
	}
	p.mu.Lock()
	p.ownWork = true
	p.mu.Unlock()
	p.finishNativeTurn(turn)
	return turn
}

func (p *Wrapper) finishNativeTurn(turn *piNativeTurn) {
	p.mu.Lock()
	if p.active == turn {
		p.active = nil
	}
	p.mu.Unlock()
}

func (p *Wrapper) Run(ctx context.Context, run *sessionkit.Run, seed sessionkit.RunInput) (result sessionkit.TurnResult, err error) {
	if run == nil {
		return result, errors.New("Pi Run requires its owned operation")
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
		return reject(errors.New("Pi Run requires exactly one input"))
	}
	if err = ctx.Err(); err != nil {
		return reject(err)
	}
	if run.Interrupted() {
		return reject(errors.New("Pi Run was interrupted before native submission"))
	}
	var input string
	if seed.Text != nil {
		input = *seed.Text
	} else if input, err = host.RenderNativeMessage(*seed.Delivery); err != nil {
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
		return reject(errors.New("Pi lane is unavailable or busy"))
	}
	runCtx, cancelRun := context.WithCancelCause(ctx)
	p.run = run
	p.runCancel = cancelRun
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
	result, err = p.handoff.Run(runCtx, run, input, func(ctx context.Context, prompt string) (host.Turn, error) {
		turn, startErr := p.startNativeTurn(ctx, run, prompt)
		if turn != nil {
			turn.mu.Lock()
			nativeSubmitted = turn.submitted
			nativeAdmitted = turn.admitted
			if seed.Delivery != nil {
				turn.report = run.ReportDelivery
			}
			turn.mu.Unlock()
		}
		return turn, startErr
	})
	var handled *piPromptHandled
	if errors.As(err, &handled) {
		// An extension received the prompt and Pi ran nothing for it: only
		// this Run ends. A delivery seed was received too, so its receipt is
		// uncertain (no_receipt) and the sender does not resend it.
		if seed.Delivery != nil {
			if reportErr := run.ReportDelivery(sessionkit.DeliveryReceipt{}, uncertainSubmission()); reportErr != nil {
				p.lose(reportErr)
				return sessionkit.TurnResult{}, errors.Join(err, reportErr)
			}
		}
		return sessionkit.TurnResult{}, err
	}
	var refusal *piPromptRefusal
	var failure *piPromptFailure
	if errors.As(err, &refusal) || errors.As(err, &failure) {
		// The handoff returned its staged messages to the queue; the seed was
		// not saved.
		if seed.Delivery != nil {
			err = errors.Join(err, run.ReportDelivery(sessionkit.DeliveryReceipt{Disposition: "rejected", Reason: "not_submitted"}, nil))
		}
		return sessionkit.TurnResult{}, err
	}
	if !nativeAdmitted && errors.Is(context.Cause(runCtx), errPiRunInterrupted) {
		var reportErr error
		if seed.Delivery != nil {
			if nativeSubmitted {
				reportErr = run.ReportDelivery(sessionkit.DeliveryReceipt{}, errPiRunInterrupted)
			} else {
				reportErr = run.ReportDelivery(sessionkit.DeliveryReceipt{Disposition: "rejected", Reason: "not_submitted"}, nil)
			}
			err = errors.Join(err, reportErr)
		}
		p.lose(errPiRunInterrupted)
		if reportErr != nil {
			p.lose(reportErr)
			return sessionkit.TurnResult{}, err
		}
		if err == nil || errors.Is(err, errPiRunInterrupted) || errors.Is(err, context.Canceled) {
			return sessionkit.TurnResult{Outcome: "interrupted"}, nil
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
