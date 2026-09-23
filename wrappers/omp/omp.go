// SPDX-License-Identifier: MIT

package omp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"unicode"
	"unicode/utf8"

	sessionkit "github.com/antst/sessionbus/bus/sdk/go"
	"github.com/sessionbus/peer-common/host"
)

var ompLaneRules = []host.ArgumentRule{
	{Name: "--system-prompt", TakesValue: true},
	{Name: "--append-system-prompt", TakesValue: true},
	{Name: "--cwd", TakesValue: true, ConflictField: "cwd"},
	{Name: "--provider", TakesValue: true, ConflictField: "model"},
	{Name: "--model", TakesValue: true, ConflictField: "model"},
	{Name: "--smol", TakesValue: true, ConflictField: "model"},
	{Name: "--slow", TakesValue: true, ConflictField: "model"},
	{Name: "--thinking", TakesValue: true, ConflictField: "reasoning_effort"},
	{Name: "--session", TakesValue: true, ConflictField: "session_id"},
	{Name: "--resume", TakesValue: true, ConflictField: "session_id"},
	{Name: "-r", TakesValue: true, ConflictField: "session_id"},
	{Name: "--continue", ConflictField: "session_id"},
	{Name: "--fork", TakesValue: true, ConflictField: "session_id"},
	{Name: "--session-dir", TakesValue: true, ConflictField: "session_id"},
	{Name: "--no-session", ConflictField: "session_id"},
	{Name: "--mode", TakesValue: true, ConflictField: "topology"},
	{Name: "--allow-home", ConflictField: "topology"},
	{Name: "--extension", TakesValue: true, ConflictField: "topology"},
	{Name: "-e", TakesValue: true, ConflictField: "topology"},
	{Name: "--trusted-extension", TakesValue: true, ConflictField: "topology"},
	{Name: "--no-extensions", ConflictField: "topology"},
	{Name: "--approval-mode", TakesValue: true, ConflictField: "permission_mode"},
	{Name: "--auto-approve", ConflictField: "permission_mode"},
	{Name: "--yolo", ConflictField: "permission_mode"},
	{Name: "--tools", TakesValue: true, ConflictField: "permission_mode"},
	{Name: "--no-tools", ConflictField: "permission_mode"},
}

type Wrapper struct {
	socket, provisional, extension string
	native                         NativeExecutable
	caller                         *sessionkit.Caller
	shutdown                       func()

	mu          sync.Mutex
	ctx         context.Context
	cancel      context.CancelCauseFunc
	owner       *NativeOwner
	binding     OwnerBinding
	starting    bool
	startupDone chan struct{}
	opened      bool
	closing     bool
	failure     error
	run         *sessionkit.Run
	runCancel   context.CancelCauseFunc
	active      *ompNativeTurn
	handoff     host.Handoff
	losing      bool
	closeOnce   sync.Once
	closeErr    error
	workers     sync.WaitGroup
}

func New(socket, provisional string, native NativeExecutable, extension string) *Wrapper {
	return &Wrapper{socket: socket, provisional: provisional, native: native, extension: extension}
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
		},
	}, nil
}

func (p *Wrapper) Open(ctx context.Context, request sessionkit.OpenRequest) (result sessionkit.OpenResult, err error) {
	if ctx == nil {
		return result, errors.New("OMP Open requires context")
	}
	if err = ctx.Err(); err != nil {
		return result, err
	}
	if p.caller == nil {
		return result, errors.New("OMP lane Caller is unavailable")
	}
	name, err := ompNamePart(request.Name)
	if err != nil {
		return result, err
	}
	explicitCWD := request.Open.Cwd != ""
	cwd := request.Open.Cwd
	if cwd == "" {
		cwd = "."
	}
	cwd, err = filepath.Abs(cwd)
	if err == nil {
		cwd, err = filepath.EvalSymlinks(cwd)
	}
	if err != nil {
		return result, fmt.Errorf("resolve OMP working directory: %w", err)
	}
	arguments, err := ompLaneArguments(request.Open, request.ResumeSessionID)
	if err != nil {
		return result, err
	}

	p.mu.Lock()
	if p.ctx != nil || p.closing {
		p.mu.Unlock()
		return result, errors.New("OMP owner already used")
	}
	p.ctx, p.cancel = context.WithCancelCause(context.Background())
	p.starting = true
	p.startupDone = make(chan struct{})
	p.mu.Unlock()
	startup, cancelStartup := context.WithCancelCause(p.ctx)
	startupCallbackDone := make(chan struct{})
	stopStartup := context.AfterFunc(ctx, func() {
		cancelStartup(ctx.Err())
		close(startupCallbackDone)
	})
	var joinStartupCallback sync.Once
	joinStartup := func() {
		joinStartupCallback.Do(func() {
			if stopStartup() {
				close(startupCallbackDone)
			}
			<-startupCallbackDone
		})
	}
	defer joinStartup()
	defer func() {
		if err != nil {
			p.cancel(err)
			err = errors.Join(err, p.Close(context.WithoutCancel(ctx), sessionkit.SessionCloseRequest{}))
		}
	}()
	defer p.finishStartup()

	owner, err := StartNativeOwner(p.ctx, NativeOwnerOptions{
		DaemonSocket: p.socket, Provisional: p.provisional, CWD: cwd,
		Topology: ownerTopologyLane, InitialName: name, Extension: p.extension,
		Groups: slices.Clone(request.Groups), Native: p.native, Arguments: arguments,
		PrimaryCaller: p.caller, NativeObserver: p.observeNative,
	})
	if err != nil {
		return result, err
	}
	p.mu.Lock()
	p.owner = owner
	closing := p.closing
	if !closing {
		p.workers.Add(1)
	}
	p.mu.Unlock()
	if closing {
		return result, errors.New("OMP owner closed during startup")
	}
	go p.watchOwner(owner)
	select {
	case <-owner.Ready():
	case <-owner.Done():
		return result, errors.Join(errors.New("OMP owner ended during Open"), owner.Err())
	case <-startup.Done():
		return result, context.Cause(startup)
	}
	binding, ok := owner.Primary()
	if !ok {
		return result, errors.New("OMP primary binding is unavailable during Open")
	}
	if err = validateOMPOpenBinding(binding, cwd, request.ResumeSessionID, explicitCWD); err != nil {
		return result, err
	}
	if request.ResumeSessionID == "" {
		if err = owner.rpc.Call(startup, "set_session_name", map[string]any{"name": name}, nil); err != nil {
			return result, fmt.Errorf("set OMP native name: %w", err)
		}
		var raw json.RawMessage
		if err = owner.rpc.Call(startup, "get_state", nil, &raw); err != nil {
			return result, fmt.Errorf("confirm OMP native name: %w", err)
		}
		state, stateErr := decodeNativeOwnerState(raw)
		if stateErr != nil || *state.SessionID != binding.SessionID {
			return result, errors.Join(errors.New("OMP native state changed during name adoption"), stateErr)
		}
		var currentName string
		if json.Unmarshal(state.SessionName, &currentName) != nil || currentName != name {
			return result, errors.New("OMP native name did not match its requested lane name")
		}
	}
	// Stop and join the caller-cancellation callback before committing Open.
	// A cancellation that won the callback boundary is therefore visible here,
	// while cancellation after this point is outside startup ownership.
	joinStartup()
	if err = p.adoptOpen(ctx, startup, binding); err != nil {
		return result, err
	}
	return sessionkit.OpenResult{SessionID: binding.SessionID}, nil
}

func validateOMPOpenBinding(binding OwnerBinding, launchCWD, resume string, explicitCWD bool) error {
	if binding.Scope != ownerScopePrimary || binding.Mode != ownerModeRPC {
		return errors.New("OMP primary binding contradicts Open")
	}
	if resume != "" && binding.SessionID != resume {
		return errors.New("OMP native state changed the requested resume identity")
	}
	// A resumed native session may restore its persisted project after process
	// launch. An explicitly requested cwd remains a public contract; an omitted
	// resume cwd leaves the validated live binding authoritative.
	if binding.CWD != launchCWD && (resume == "" || explicitCWD) {
		return errors.New("OMP native working directory contradicts Open")
	}
	return nil
}

func (p *Wrapper) adoptOpen(caller, startup context.Context, binding OwnerBinding) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.failure != nil || p.closing || p.ctx.Err() != nil || caller.Err() != nil || startup.Err() != nil {
		err := errors.Join(p.failure, context.Cause(p.ctx), context.Cause(startup), caller.Err())
		if err == nil {
			err = errNativeOwnerClosed
		}
		return err
	}
	if p.owner == nil {
		return errors.New("OMP native owner is unavailable during Open adoption")
	}
	if err := p.owner.adoptionFailure(binding); err != nil {
		return err
	}
	p.binding = binding
	p.opened = true
	return nil
}

// adoptionFailure validates the exact live primary and every owned component
// at Wrapper Open's final commit boundary. NativeOwner.Done closes only after
// all joins, so component-local failures must be observed directly here too.
func (owner *NativeOwner) adoptionFailure(binding OwnerBinding) error {
	if owner == nil {
		return errors.New("OMP native owner is unavailable during Open adoption")
	}
	owner.mu.Lock()
	ready, closing, ownerErr, bridge := owner.readySet, owner.closing, owner.err, owner.bridge
	owner.mu.Unlock()
	if !ready || closing || ownerErr != nil || owner.ctx.Err() != nil {
		return errors.Join(errors.New("OMP native owner is unavailable during Open adoption"), ownerErr, context.Cause(owner.ctx))
	}
	if owner.registry == nil {
		return errors.New("OMP owner registry is unavailable during Open adoption")
	}
	owner.registry.mu.Lock()
	registryErr, registryContextErr, ending := owner.registry.err, owner.registry.ctx.Err(), owner.registry.ending
	state := owner.registry.bindings[owner.registry.primaryToken]
	current := !ending && state != nil && state.admitted && state.OwnerBinding == binding &&
		owner.registry.primaryToken == binding.OwnerToken
	owner.registry.mu.Unlock()
	if registryErr != nil || registryContextErr != nil || !current {
		return errors.Join(errors.New("OMP owner registry changed during Open adoption"), registryErr, registryContextErr)
	}
	if bridge == nil {
		return errors.New("OMP managed extension is unavailable during Open adoption")
	}
	if err := bridge.Err(); err != nil {
		return errors.Join(errors.New("OMP managed extension failed during Open adoption"), err)
	}
	select {
	case <-bridge.Done():
		return errors.Join(errors.New("OMP managed extension ended during Open adoption"), bridge.Err())
	default:
	}
	if owner.rpc == nil {
		return errors.New("OMP native RPC is unavailable during Open adoption")
	}
	if err := owner.rpc.Err(); err != nil {
		return errors.Join(errors.New("OMP native RPC failed during Open adoption"), err)
	}
	select {
	case <-owner.rpc.Done():
		return errors.Join(errors.New("OMP native RPC ended during Open adoption"), owner.rpc.Err())
	default:
	}
	if owner.process == nil {
		return errors.New("OMP native process is unavailable during Open adoption")
	}
	select {
	case <-owner.process.done:
		return errors.Join(errors.New("OMP native process ended during Open adoption"), owner.process.Wait())
	default:
	}
	return nil
}

func (p *Wrapper) finishStartup() {
	p.mu.Lock()
	if p.starting {
		p.starting = false
		close(p.startupDone)
	}
	p.mu.Unlock()
}

func ompLaneArguments(open sessionkit.OpenOptions, resume string) ([]string, error) {
	if open.PermissionMode != "" && open.PermissionMode != "default" && open.PermissionMode != "bypassPermissions" {
		return nil, fmt.Errorf("unsupported value permission_mode=%s", open.PermissionMode)
	}
	if resume != "" && !validOwnerID(resume) {
		return nil, errors.New("invalid OMP native resume identity")
	}
	if open.Model != "" && (!validOMPArgument(open.Model, 256) || strings.TrimSpace(open.Model) != open.Model) {
		return nil, errors.New("invalid OMP native model selection")
	}
	if open.ReasoningEffort != "" && !slices.Contains([]string{"off", "minimal", "low", "medium", "high", "xhigh", "max", "auto"}, open.ReasoningEffort) {
		return nil, fmt.Errorf("unsupported value reasoning_effort=%s", open.ReasoningEffort)
	}
	for _, argument := range open.Arguments {
		if !validOMPArgument(argument, 32<<10) {
			return nil, errors.New("OMP argument is invalid")
		}
	}
	forwarded, err := host.BuildArguments(open.Arguments, ompLaneRules)
	if err != nil {
		return nil, err
	}
	arguments := make([]string, 0, len(forwarded)+6)
	if open.PermissionMode == "bypassPermissions" {
		arguments = append(arguments, "--approval-mode=yolo")
	}
	if resume != "" {
		arguments = append(arguments, "--session", resume)
	}
	if open.Model != "" {
		arguments = append(arguments, "--model", open.Model)
	}
	if open.ReasoningEffort != "" {
		arguments = append(arguments, "--thinking", open.ReasoningEffort)
	}
	arguments = append(arguments, forwarded...)
	return arguments, nil
}

func validOMPArgument(value string, limit int) bool {
	return utf8.ValidString(value) && len(value) <= limit && !strings.ContainsRune(value, 0)
}

func ompNamePart(name string) (string, error) {
	index := strings.LastIndexByte(name, '@')
	if index < 1 || !validOwnerText(name[:index], 4096) || strings.IndexFunc(name[:index], unicode.IsControl) >= 0 {
		return "", errors.New("invalid OMP lane name")
	}
	return name[:index], nil
}

func (p *Wrapper) watchOwner(owner *NativeOwner) {
	defer p.workers.Done()
	<-owner.Done()
	p.mu.Lock()
	closing := p.closing
	err := owner.Err()
	if err != nil {
		p.failure = errors.Join(p.failure, err)
	}
	p.mu.Unlock()
	if !closing {
		if err == nil {
			err = errors.New("OMP native owner ended unexpectedly")
		}
		p.lose(err)
	}
}

func (p *Wrapper) lose(err error) {
	if err == nil {
		return
	}
	p.mu.Lock()
	p.failure = errors.Join(p.failure, err)
	if p.losing {
		p.mu.Unlock()
		return
	}
	p.losing = true
	opened, closing, run, shutdown, cancel := p.opened, p.closing, p.run, p.shutdown, p.cancel
	startShutdown := opened && !closing && shutdown != nil
	if startShutdown {
		// Close sets closing while holding the same mutex before it calls Wait.
		// Reserving this worker here prevents Add from racing Wait.
		p.workers.Add(1)
	}
	p.mu.Unlock()
	if cancel != nil {
		cancel(err)
	}
	if startShutdown {
		go func() {
			defer p.workers.Done()
			if run != nil {
				<-run.Done()
			}
			shutdown()
		}()
	}
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
	if ctx == nil {
		return errors.New("OMP Close requires context")
	}
	p.closeOnce.Do(func() {
		p.mu.Lock()
		p.closing = true
		starting, startupDone, cancel := p.starting, p.startupDone, p.cancel
		p.mu.Unlock()
		if starting && cancel != nil {
			cancel(errNativeOwnerClosed)
			<-startupDone
		}
		p.mu.Lock()
		owner := p.owner
		p.mu.Unlock()
		if owner != nil {
			p.closeErr = owner.Close(ctx)
		}
		p.workers.Wait()
		p.mu.Lock()
		p.closeErr = errors.Join(p.closeErr, p.failure)
		p.mu.Unlock()
		if cancel != nil {
			cancel(errNativeOwnerClosed)
		}
		p.closeErr = errors.Join(p.closeErr, ctx.Err())
	})
	return p.closeErr
}
