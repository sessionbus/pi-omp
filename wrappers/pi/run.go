// SPDX-License-Identifier: MIT

package pi

import (
	"context"
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

func (turn *piNativeTurn) recordPreflight(prompt string, settling bool) error {
	turn.mu.Lock()
	defer turn.mu.Unlock()
	if turn.failure != nil {
		return turn.failure
	}
	if !turn.input || turn.preflight || settling || !validPiText(prompt, 1<<20, true) {
		err := errors.New("Pi native preflight is outside the owned Run")
		turn.failLocked(err)
		return err
	}
	turn.preflight, turn.prompt = true, prompt
	turn.signalLocked()
	return nil
}

func (turn *piNativeTurn) recordStart(settling bool) error {
	turn.mu.Lock()
	defer turn.mu.Unlock()
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

func (turn *piNativeTurn) recordSettling() error {
	turn.mu.Lock()
	defer turn.mu.Unlock()
	if turn.failure != nil {
		return turn.failure
	}
	if turn.started == 0 || turn.settling {
		err := errors.New("Pi native settling marker is outside the owned Run")
		turn.failLocked(err)
		return err
	}
	turn.settling = true
	turn.signalLocked()
	return nil
}

func (turn *piNativeTurn) recordEvent(kind string, raw json.RawMessage) error {
	turn.mu.Lock()
	defer turn.mu.Unlock()
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
	admitted := turn.admitted
	turn.mu.Unlock()
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
		if event.ExtensionPath != p.extension {
			return nil
		}
		extensionFailure = fmt.Errorf("Pi managed extension failed during %s: %s", event.Event, event.Error)
	}
	p.mu.Lock()
	turn, closing := p.active, p.closing
	p.mu.Unlock()
	if closing {
		return nil
	}
	if turn == nil {
		if extensionFailure != nil {
			return extensionFailure
		}
		return fmt.Errorf("Pi native emitted %s outside an owned Run", kind)
	}
	return turn.recordEvent(kind, raw)
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
	p.mu.Unlock()
	if !opened || closing || turn == nil {
		return fmt.Errorf("Pi native requested unsupported startup UI method %q", event.Method)
	}
	return turn.cancelUI(event.ID)
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

func (p *Wrapper) recordRunPreflight(sessionID, prompt string, settling bool) error {
	turn, err := p.currentNativeTurn(sessionID)
	if err != nil {
		return err
	}
	return turn.recordPreflight(prompt, settling)
}

func (p *Wrapper) recordRunStart(sessionID string, settling bool) error {
	turn, err := p.currentNativeTurn(sessionID)
	if err != nil {
		return err
	}
	return turn.recordStart(settling)
}

func (p *Wrapper) recordRunSettling(sessionID string) error {
	turn, err := p.currentNativeTurn(sessionID)
	if err != nil {
		return err
	}
	return turn.recordSettling()
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
		return nil, errors.New("Pi lane prompt is empty or exceeds its native bound")
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
	p.active = turn
	p.mu.Unlock()
	submitted, callErr := p.rpc.callWithAdmission(ctx, "prompt", map[string]any{"message": prompt}, nil)
	turn.mu.Lock()
	turn.submitted = submitted
	turn.mu.Unlock()
	if callErr != nil {
		if submitted {
			turn.fail(callErr)
			return turn, nil
		}
		p.finishNativeTurn(turn)
		return nil, callErr
	}
	if err = context.Cause(ctx); err != nil {
		turn.fail(err)
		return turn, nil
	}
	turn.mu.Lock()
	preflight := turn.input && turn.preflight
	turn.mu.Unlock()
	if !preflight {
		turn.fail(errors.New("Pi native accepted a prompt without the managed Run preflight"))
		return turn, nil
	}
	if err = turn.await(ctx, turn.admission); err != nil {
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
