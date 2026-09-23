// SPDX-License-Identifier: MIT

package pi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"unicode/utf8"

	sessionkit "github.com/antst/sessionbus/bus/sdk/go"
	"github.com/sessionbus/peer-common/host"
	"github.com/sessionbus/pi-omp/wrappers/pifamily"
)

type Wrapper struct {
	socket, provisional, executable, extension string
	caller                                     *sessionkit.Caller
	shutdown                                   func()

	mu           sync.Mutex
	ctx          context.Context
	cancel       context.CancelCauseFunc
	process      *piProcess
	rpc          *nativeRPC
	bridge       *pifamily.Bridge
	owner        piOwnerReady
	ownerChanged chan struct{}
	startupDone  chan struct{}
	starting     bool
	id, cwd      string
	opened       bool
	closing      bool
	failure      error
	ended        bool
	run          *sessionkit.Run
	runCancel    context.CancelCauseFunc
	active       *piNativeTurn
	handoff      host.Handoff
	losing       bool
	closeOnce    sync.Once
	closeErr     error
	workers      sync.WaitGroup
}

type piOwnerReady struct {
	Topology  string `json:"topology"`
	Directory string `json:"directory"`
	SessionID string `json:"session_id"`
	Name      string `json:"name"`
}

type piNativeState struct {
	SessionID    string `json:"sessionId"`
	SessionName  string `json:"sessionName"`
	SessionFile  string `json:"sessionFile"`
	IsStreaming  bool   `json:"isStreaming"`
	IsCompacting bool   `json:"isCompacting"`
}

type piNativeDescription struct {
	SessionID string `json:"session_id"`
	Name      string `json:"name"`
	Cwd       string `json:"cwd"`
}

func New(socket, provisional, executable, extension string) *Wrapper {
	return &Wrapper{
		socket: socket, provisional: provisional, executable: executable,
		extension: extension, ownerChanged: make(chan struct{}),
	}
}

func (p *Wrapper) SetCaller(caller *sessionkit.Caller) { p.caller = caller }
func (p *Wrapper) SetShutdown(shutdown func())         { p.shutdown = shutdown }

func (*Wrapper) Hello(context.Context) (sessionkit.HelloDescription, error) {
	return sessionkit.HelloDescription{
		Product: Product, SupportsMessageRun: true,
		SupportedOpenFields: []string{"cwd", "permission_mode", "model", "reasoning_effort", "arguments"},
		ExtraArguments: []sessionkit.ExtraArgument{
			{Name: "--system-prompt", Description: "Replace the native system prompt", TakesValue: true},
			{Name: "--append-system-prompt", Description: "Append to the native system prompt", TakesValue: true},
			{Name: "--verbose", Description: "Enable native verbose output"},
			{Name: "--offline", Description: "Disable native network discovery"},
		},
	}, nil
}

func (p *Wrapper) Open(ctx context.Context, request sessionkit.OpenRequest) (result sessionkit.OpenResult, err error) {
	if ctx == nil {
		return result, errors.New("Pi Open requires context")
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if !filepath.IsAbs(p.executable) || !filepath.IsAbs(p.extension) ||
		strings.ContainsRune(p.executable, 0) || strings.ContainsRune(p.extension, 0) {
		return result, errors.New("Pi requires absolute native executable and extension paths")
	}
	if p.caller == nil {
		return result, errors.New("Pi lane Caller is unavailable")
	}
	name, err := piNamePart(request.Name)
	if err != nil {
		return result, err
	}
	cwd := request.Open.Cwd
	if cwd == "" {
		cwd = "."
	}
	cwd, err = filepath.Abs(cwd)
	if err == nil {
		cwd, err = filepath.EvalSymlinks(cwd)
	}
	if err != nil {
		return result, fmt.Errorf("resolve Pi working directory: %w", err)
	}
	arguments, err := launchArguments(request.Open, p.extension, request.ResumeSessionID)
	if err != nil {
		return result, err
	}

	p.mu.Lock()
	if p.ctx != nil || p.closing {
		p.mu.Unlock()
		return result, errors.New("Pi owner already used")
	}
	p.ctx, p.cancel = context.WithCancelCause(context.Background())
	p.cwd = cwd
	p.starting = true
	p.startupDone = make(chan struct{})
	p.mu.Unlock()
	startup, cancelStartup := context.WithCancelCause(p.ctx)
	stopStartup := context.AfterFunc(ctx, func() { cancelStartup(ctx.Err()) })
	defer stopStartup()
	defer func() {
		if err != nil {
			p.cancel(err)
			err = errors.Join(err, p.Close(context.WithoutCancel(ctx), sessionkit.SessionCloseRequest{}))
		}
	}()
	defer p.finishStartup()

	process, err := piStartProcess(p.socket, p.provisional, p.executable, cwd, arguments)
	if err != nil {
		return result, fmt.Errorf("start Pi native process: %w", err)
	}
	p.mu.Lock()
	p.process = process
	closing := p.closing
	p.mu.Unlock()
	if closing {
		return result, errors.New("Pi owner closed during startup")
	}
	p.workers.Add(1)
	go p.watchProcess(process)

	rpc, err := newNativeRPC(process.input, process.output, p.observeNative, nativeRPCLimits{})
	if err != nil {
		return result, err
	}
	p.mu.Lock()
	p.rpc = rpc
	p.mu.Unlock()
	p.workers.Add(1)
	go p.watchRPC(rpc)

	connection, err := process.accept(startup)
	if err != nil {
		return result, fmt.Errorf("accept Pi managed extension: %w", err)
	}
	bridge, err := pifamily.NewBridge(connection, pifamily.BridgeHost, p.handleBridge, pifamily.BridgeLimits{})
	if err != nil {
		return result, err
	}
	p.mu.Lock()
	p.bridge = bridge
	p.mu.Unlock()
	p.workers.Add(1)
	go p.watchBridge(bridge)
	if err = bridge.Ready(startup); err != nil {
		return result, fmt.Errorf("join Pi managed extension hello: %w", err)
	}
	ready, err := p.waitOwner(startup, func(value piOwnerReady) bool { return value.SessionID != "" })
	if err != nil {
		return result, err
	}

	var state piNativeState
	if err = rpc.Call(startup, "get_state", nil, &state); err != nil {
		return result, err
	}
	if err = validatePiState(state, ready.SessionID, request.ResumeSessionID); err != nil {
		return result, err
	}
	if err = process.lock.Rename(state.SessionID); err != nil {
		return result, err
	}
	if request.ResumeSessionID == "" {
		if err = rpc.Call(startup, "set_session_name", map[string]any{"name": name}, nil); err != nil {
			return result, err
		}
		ready, err = p.waitOwner(startup, func(value piOwnerReady) bool {
			return value.SessionID == state.SessionID && value.Name == name
		})
		if err != nil {
			return result, err
		}
	}
	var description piNativeDescription
	if err = bridge.Call(startup, "native.describe", map[string]string{"session_id": state.SessionID}, &description); err != nil {
		return result, err
	}
	if description.SessionID != state.SessionID || description.Cwd != cwd ||
		(request.ResumeSessionID == "" && description.Name != name) ||
		(request.ResumeSessionID != "" && description.Name != ready.Name) {
		return result, errors.New("Pi native description contradicts its managed owner")
	}
	p.mu.Lock()
	if p.failure != nil || p.closing || p.ctx.Err() != nil {
		err = errors.Join(p.failure, context.Cause(p.ctx))
		p.mu.Unlock()
		return result, err
	}
	p.id = state.SessionID
	p.opened = true
	p.mu.Unlock()
	return sessionkit.OpenResult{SessionID: state.SessionID}, nil
}

func (p *Wrapper) finishStartup() {
	p.mu.Lock()
	if p.starting {
		p.starting = false
		close(p.startupDone)
	}
	p.mu.Unlock()
}

func validatePiState(state piNativeState, readyID, requestedID string) error {
	if !validPiText(state.SessionID, 256, false) || !nativeSessionID.MatchString(state.SessionID) || state.SessionID != readyID {
		return errors.New("Pi native state changed its managed session identity")
	}
	if requestedID != "" && state.SessionID != requestedID {
		return errors.New("Pi native state changed the requested resume identity")
	}
	if state.IsStreaming || state.IsCompacting {
		return errors.New("Pi native session is busy during Open")
	}
	return nil
}

func (p *Wrapper) observeNative(frame json.RawMessage) error {
	var envelope struct {
		Type string `json:"type"`
	}
	if json.Unmarshal(frame, &envelope) != nil || envelope.Type == "" {
		return errors.New("Pi native emitted an invalid event")
	}
	if envelope.Type == "extension_ui_request" {
		return p.observeNativeUI(frame)
	}
	return p.observeRunEvent(envelope.Type, frame)
}

func (p *Wrapper) handleBridge(ctx context.Context, method string, raw json.RawMessage) (json.RawMessage, error) {
	var result any
	var err error
	switch method {
	case "owner.ready":
		var request piOwnerReady
		if err = decodePiBridge(raw, []string{"topology", "directory", "session_id", "name"}, &request); err == nil {
			err = p.recordOwner(request)
			result = map[string]string{"session_id": request.SessionID}
		}
	case "session_end":
		var request struct {
			Topology  string `json:"topology"`
			SessionID string `json:"session_id"`
			Reason    string `json:"reason"`
		}
		if err = decodePiBridge(raw, []string{"topology", "session_id", "reason"}, &request); err == nil {
			err = p.recordEnd(request.Topology, request.SessionID, request.Reason)
			result = map[string]string{"session_id": request.SessionID}
		}
	case "tool.call":
		var request struct {
			SessionID string          `json:"session_id"`
			CallID    string          `json:"call_id"`
			Action    string          `json:"action"`
			Arguments json.RawMessage `json:"arguments"`
		}
		if err = decodePiBridge(raw, []string{"session_id", "call_id", "action", "arguments"}, &request); err == nil {
			err = p.validateTool(request.SessionID, request.CallID, request.Action, request.Arguments)
		}
		var actionResult json.RawMessage
		if err == nil {
			actionResult, err = p.caller.Action(ctx, request.Action, request.Arguments)
			if err != nil {
				return nil, pifamily.NewBridgeCallError("action_failed", err.Error())
			}
		}
		if err == nil {
			result = struct {
				SessionID string          `json:"session_id"`
				CallID    string          `json:"call_id"`
				Result    json.RawMessage `json:"result"`
			}{request.SessionID, request.CallID, actionResult}
		}
	case "run.input":
		var request struct {
			SessionID string          `json:"session_id"`
			Source    string          `json:"source"`
			Text      string          `json:"text"`
			Settling  json.RawMessage `json:"settling"`
		}
		if err = decodePiBridge(raw, []string{"session_id", "source", "text", "settling"}, &request); err == nil {
			var settling bool
			settling, err = piBridgeBool(request.Settling)
			if err == nil {
				err = p.recordRunInput(request.SessionID, request.Source, request.Text, settling)
			}
			result = map[string]string{"session_id": request.SessionID}
		}
	case "run.preflight":
		var request struct {
			SessionID string          `json:"session_id"`
			Prompt    string          `json:"prompt"`
			Settling  json.RawMessage `json:"settling"`
		}
		if err = decodePiBridge(raw, []string{"session_id", "prompt", "settling"}, &request); err == nil {
			var settling bool
			settling, err = piBridgeBool(request.Settling)
			if err == nil {
				err = p.recordRunPreflight(request.SessionID, request.Prompt, settling)
			}
			result = map[string]string{"session_id": request.SessionID}
		}
	case "run.start":
		var request struct {
			SessionID string          `json:"session_id"`
			Settling  json.RawMessage `json:"settling"`
		}
		if err = decodePiBridge(raw, []string{"session_id", "settling"}, &request); err == nil {
			var settling bool
			settling, err = piBridgeBool(request.Settling)
			if err == nil {
				err = p.recordRunStart(request.SessionID, settling)
			}
			result = map[string]string{"session_id": request.SessionID}
		}
	case "run.settling":
		var request struct {
			SessionID string `json:"session_id"`
		}
		if err = decodePiBridge(raw, []string{"session_id"}, &request); err == nil {
			err = p.recordRunSettling(request.SessionID)
			result = map[string]string{"session_id": request.SessionID}
		}
	default:
		err = pifamily.NewBridgeCallError("method_not_found", "Pi bridge method is unavailable")
	}
	if err != nil {
		p.loseFromHandler(err)
		return nil, err
	}
	body, marshalErr := json.Marshal(result)
	return body, marshalErr
}

func piBridgeBool(raw json.RawMessage) (bool, error) {
	trimmed := bytes.TrimSpace(raw)
	if bytes.Equal(trimmed, []byte("true")) {
		return true, nil
	}
	if bytes.Equal(trimmed, []byte("false")) {
		return false, nil
	}
	return false, errors.New("Pi bridge boolean is invalid")
}

func decodePiBridge(raw json.RawMessage, fields []string, target any) error {
	var object map[string]json.RawMessage
	if json.Unmarshal(raw, &object) != nil || object == nil || len(object) != len(fields) {
		return errors.New("Pi bridge parameters are invalid")
	}
	for _, field := range fields {
		if _, ok := object[field]; !ok {
			return errors.New("Pi bridge parameters are invalid")
		}
	}
	if json.Unmarshal(raw, target) != nil {
		return errors.New("Pi bridge parameters are invalid")
	}
	return nil
}

func (p *Wrapper) recordOwner(request piOwnerReady) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if request.Topology != "lane" || p.process == nil || request.Directory != p.process.directory ||
		!validPiText(request.SessionID, 256, false) || !validPiText(request.Name, 4096, true) ||
		(p.owner.SessionID != "" && p.owner.SessionID != request.SessionID) {
		return errors.New("Pi managed owner readiness is invalid")
	}
	p.owner = request
	close(p.ownerChanged)
	p.ownerChanged = make(chan struct{})
	return nil
}

func (p *Wrapper) waitOwner(ctx context.Context, predicate func(piOwnerReady) bool) (piOwnerReady, error) {
	for {
		p.mu.Lock()
		value, changed, failure := p.owner, p.ownerChanged, p.failure
		p.mu.Unlock()
		if failure != nil {
			return value, failure
		}
		if predicate(value) {
			return value, nil
		}
		select {
		case <-ctx.Done():
			return value, context.Cause(ctx)
		case <-changed:
		}
	}
}

func (p *Wrapper) recordEnd(topology, sessionID, reason string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if topology != "lane" || sessionID == "" || sessionID != p.owner.SessionID ||
		!p.closing || reason != "quit" || p.ended {
		return errors.New("Pi native session ended outside owned Close")
	}
	p.ended = true
	return nil
}

func (p *Wrapper) validateTool(sessionID, callID, action string, arguments json.RawMessage) error {
	p.mu.Lock()
	valid := p.opened && !p.closing && p.failure == nil && sessionID == p.id
	p.mu.Unlock()
	trimmed := bytes.TrimSpace(arguments)
	if !valid || !validPiText(callID, 256, false) || !validPiText(action, 32, false) ||
		len(arguments) == 0 || len(arguments) > 1<<20 || !json.Valid(arguments) || bytes.Equal(trimmed, []byte("null")) || len(trimmed) == 0 || trimmed[0] != '{' {
		return errors.New("Pi native tool call is invalid")
	}
	return nil
}

func validPiText(value string, limit int, empty bool) bool {
	return utf8.ValidString(value) && !strings.ContainsRune(value, 0) && len(value) <= limit && (empty || value != "")
}

func piNamePart(name string) (string, error) {
	index := strings.LastIndexByte(name, '@')
	if index < 1 || !validPiText(name[:index], 4096, false) {
		return "", errors.New("invalid Pi lane name")
	}
	return name[:index], nil
}

func (p *Wrapper) watchProcess(process *piProcess) {
	defer p.workers.Done()
	err := process.Wait()
	p.mu.Lock()
	closing := p.closing
	p.mu.Unlock()
	if !closing {
		if err == nil {
			err = errors.New("Pi native process exited unexpectedly")
		}
		if stderr := strings.TrimSpace(process.stderr.String()); stderr != "" {
			err = errors.Join(err, errors.New(stderr))
		}
		p.lose(err)
	}
}

func (p *Wrapper) watchRPC(rpc *nativeRPC) {
	defer p.workers.Done()
	<-rpc.Done()
	p.mu.Lock()
	closing := p.closing
	if err := rpc.Err(); err != nil {
		p.failure = errors.Join(p.failure, err)
	}
	p.mu.Unlock()
	if !closing {
		err := rpc.Err()
		if err == nil {
			err = errors.New("Pi native RPC ended unexpectedly")
		}
		p.lose(err)
	}
}

func (p *Wrapper) watchBridge(bridge *pifamily.Bridge) {
	defer p.workers.Done()
	<-bridge.Done()
	p.mu.Lock()
	closing, ended := p.closing, p.ended
	if err := bridge.Err(); err != nil && !(closing && ended && errors.Is(err, pifamily.ErrBridgeClosed)) {
		p.failure = errors.Join(p.failure, err)
	}
	p.mu.Unlock()
	if !closing {
		err := bridge.Err()
		if err == nil {
			err = errors.New("Pi managed extension connection ended unexpectedly")
		}
		p.lose(err)
	}
}

func (p *Wrapper) lose(err error) {
	p.loseInternal(err, true)
}

// A bridge handler cannot join its own transport. Killing the direct child and
// closing native RPC makes the peer socket reach EOF after the handler returns;
// the bridge watcher then joins the connection normally.
func (p *Wrapper) loseFromHandler(err error) {
	p.loseInternal(err, false)
}

func (p *Wrapper) loseInternal(err error, closeBridge bool) {
	p.mu.Lock()
	p.failure = errors.Join(p.failure, err)
	if p.losing {
		p.mu.Unlock()
		return
	}
	p.losing = true
	opened, run, shutdown := p.opened, p.run, p.shutdown
	process, rpc, bridge, cancel := p.process, p.rpc, p.bridge, p.cancel
	p.mu.Unlock()
	if cancel != nil {
		cancel(err)
	}
	if process != nil {
		process.Force()
	}
	if rpc != nil {
		_ = rpc.Close()
	}
	if closeBridge && bridge != nil {
		_ = bridge.Close()
	}
	if opened && shutdown != nil {
		p.workers.Add(1)
		go func() {
			defer p.workers.Done()
			if run != nil {
				<-run.Done()
			}
			shutdown()
		}()
	}
}

func (p *Wrapper) Interrupt(ctx context.Context, run *sessionkit.Run) error {
	p.mu.Lock()
	matching, turn, cancel := p.run == run, p.active, p.runCancel
	p.mu.Unlock()
	if matching {
		if turn != nil {
			turn.mu.Lock()
			admitted := turn.admitted
			turn.mu.Unlock()
			if admitted {
				return turn.Interrupt(ctx)
			}
		}
		if cancel != nil {
			cancel(errPiRunInterrupted)
			return nil
		}
	}
	return p.handoff.Interrupt(ctx, run)
}

func (p *Wrapper) Deliver(ctx context.Context, request sessionkit.DeliveryRequest, _ *sessionkit.Run) (sessionkit.DeliveryReceipt, error) {
	if _, err := host.RenderNativeMessage(request); err != nil {
		return sessionkit.DeliveryReceipt{}, err
	}
	if err := ctx.Err(); err != nil {
		return sessionkit.DeliveryReceipt{}, err
	}
	p.mu.Lock()
	available := p.opened && !p.closing && p.failure == nil && p.ctx.Err() == nil
	p.mu.Unlock()
	if !available {
		return sessionkit.DeliveryReceipt{Disposition: "rejected", Reason: "lane_unavailable"}, nil
	}
	return sessionkit.DeliveryReceipt{}, host.NotRunning()
}

func (p *Wrapper) Close(ctx context.Context, _ sessionkit.SessionCloseRequest) error {
	p.closeOnce.Do(func() {
		p.mu.Lock()
		p.closing = true
		startupDone, starting, cancel := p.startupDone, p.starting, p.cancel
		p.mu.Unlock()
		if starting && cancel != nil {
			cancel(errors.New("Pi owner closed during startup"))
			<-startupDone
		}
		p.mu.Lock()
		process, rpc, bridge, failure := p.process, p.rpc, p.bridge, p.failure
		p.mu.Unlock()
		if process == nil {
			return
		}
		forceDone := make(chan struct{})
		forced := context.AfterFunc(ctx, func() {
			defer close(forceDone)
			process.Force()
			if rpc != nil {
				_ = rpc.Close()
			}
			if bridge != nil {
				_ = bridge.Close()
			}
		})
		defer func() {
			if forced() {
				close(forceDone)
			}
			<-forceDone
		}()
		if failure != nil || p.ctx.Err() != nil {
			process.Force()
		} else if rpc != nil {
			p.closeErr = errors.Join(p.closeErr, rpc.EndInput(ctx))
			if p.closeErr != nil {
				process.Force()
			}
		}
		childErr := process.Wait()
		if rpc != nil {
			p.closeErr = errors.Join(p.closeErr, rpc.Close())
		}
		if bridge != nil {
			bridgeErr := bridge.Close()
			p.mu.Lock()
			ended := p.ended
			p.mu.Unlock()
			if !(ended && errors.Is(bridgeErr, pifamily.ErrBridgeClosed)) {
				p.closeErr = errors.Join(p.closeErr, bridgeErr)
			}
		}
		p.workers.Wait()
		p.mu.Lock()
		ended := p.ended
		p.closeErr = errors.Join(p.closeErr, p.failure)
		p.mu.Unlock()
		if failure == nil && p.closeErr == nil && !ended {
			p.closeErr = errors.New("Pi native session did not confirm shutdown")
		}
		p.closeErr = errors.Join(p.closeErr, childErr, ctx.Err())
		p.closeErr = errors.Join(p.closeErr, process.Cleanup())
		if cancel != nil {
			cancel(errors.New("Pi owner closed"))
		}
	})
	return p.closeErr
}
