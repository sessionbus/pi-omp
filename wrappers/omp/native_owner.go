// SPDX-License-Identifier: MIT

package omp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"unicode/utf8"

	kit "github.com/antst/sessionbus/bus/sdk/go"
	"github.com/sessionbus/pi-omp/wrappers/pifamily"
)

var errNativeOwnerClosed = errors.New("OMP native owner closed")

// NativeOwnerOptions describes one directly owned OMP process. Extension is
// the physically resolved managed extension. Arguments are the already
// validated native arguments which follow the owner's fixed launch prefix.
type NativeOwnerOptions struct {
	DaemonSocket  string
	Provisional   string
	CWD           string
	Topology      string
	InitialName   string
	Extension     string
	Groups        []string
	Native        NativeExecutable
	Arguments     []string
	PrimaryCaller *kit.Caller
	// NativeObserver is installed before the lane RPC reader starts. It must
	// synchronously record or reject every native event; interactive topology
	// has no RPC stream and leaves it nil.
	NativeObserver func(json.RawMessage) error
}

type nativeOwnerState struct {
	SessionID          *string         `json:"sessionId"`
	SessionName        json.RawMessage `json:"sessionName"`
	IsStreaming        *bool           `json:"isStreaming"`
	IsCompacting       *bool           `json:"isCompacting"`
	QueuedMessageCount *int            `json:"queuedMessageCount"`
}

type nativeOwnerClose struct {
	ctx context.Context
}

// NativeOwner is the single lifecycle owner for the direct Bun process, OMP
// RPC stream, managed extension bridge, public owner registry, session lock,
// and private launch directory.
type NativeOwner struct {
	ctx    context.Context
	cancel context.CancelCauseFunc

	process  *ompProcess
	rpc      *nativeRPC
	registry *OwnerRegistry

	mu       sync.Mutex
	bridge   *pifamily.Bridge
	readySet bool
	closing  bool
	err      error
	observer func(json.RawMessage) error

	ready        chan struct{}
	done         chan struct{}
	closeRequest chan nativeOwnerClose
	closeOnce    sync.Once
}

func StartNativeOwner(ctx context.Context, options NativeOwnerOptions) (*NativeOwner, error) {
	if ctx == nil {
		return nil, errors.New("OMP native owner requires context")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := validateNativeOwnerOptions(options); err != nil {
		return nil, err
	}
	arguments := make([]string, 0, len(options.Arguments)+5)
	arguments = append(arguments, "--extension", options.Extension)
	if options.Topology == ownerTopologyLane {
		arguments = append(arguments, "--mode", "rpc-ui", "--allow-home")
	}
	arguments = append(arguments, options.Arguments...)

	lifetime, cancel := context.WithCancelCause(ctx)
	process, err := ompStartProcess(options.DaemonSocket, options.Provisional, options.Topology, options.Native, options.CWD, arguments)
	if err != nil {
		cancel(err)
		return nil, fmt.Errorf("start OMP native process: %w", err)
	}
	owner := &NativeOwner{
		ctx: lifetime, cancel: cancel, process: process,
		ready: make(chan struct{}), done: make(chan struct{}),
		closeRequest: make(chan nativeOwnerClose, 1),
		observer:     options.NativeObserver,
	}
	registry, err := NewOwnerRegistry(lifetime, OwnerRegistryOptions{
		Topology: options.Topology, Socket: options.DaemonSocket,
		Directory: process.directory, InitialName: options.InitialName,
		Groups: options.Groups, PrimaryCaller: options.PrimaryCaller,
	})
	if err != nil {
		process.Force()
		waitErr := process.Wait()
		cleanErr := process.Cleanup()
		cancel(err)
		return nil, errors.Join(err, waitErr, cleanErr)
	}
	owner.registry = registry
	if options.Topology == ownerTopologyLane {
		rpc, rpcErr := newNativeRPC(process.input, process.output, owner.observeNative, nativeRPCLimits{})
		if rpcErr != nil {
			process.Force()
			waitErr := process.Wait()
			registryErr := registry.Close()
			cleanErr := process.Cleanup()
			cancel(rpcErr)
			return nil, errors.Join(rpcErr, waitErr, registryErr, cleanErr)
		}
		owner.rpc = rpc
	}
	go owner.run(options)
	return owner, nil
}

func validateNativeOwnerOptions(options NativeOwnerOptions) error {
	if options.Topology != ownerTopologyLane && options.Topology != ownerTopologyInteractive {
		return errors.New("OMP native owner topology is invalid")
	}
	if (options.Topology == ownerTopologyLane) != (options.PrimaryCaller != nil) {
		return errors.New("OMP native owner primary Caller does not match topology")
	}
	if (options.Topology == ownerTopologyLane) != (options.NativeObserver != nil) {
		return errors.New("OMP native event observer does not match topology")
	}
	if !filepath.IsAbs(options.DaemonSocket) || !validOwnerText(options.DaemonSocket, 32<<10) ||
		!validOwnerID(options.Provisional) || !validOwnerText(options.InitialName, 4096) {
		return errors.New("OMP native owner identity is invalid")
	}
	if !filepath.IsAbs(options.CWD) || !validOwnerText(options.CWD, 32<<10) {
		return errors.New("OMP native owner working directory is invalid")
	}
	cwd, err := filepath.EvalSymlinks(options.CWD)
	if err != nil || cwd != options.CWD {
		return errors.New("OMP native owner working directory is not physically resolved")
	}
	info, err := os.Stat(cwd)
	if err != nil || !info.IsDir() {
		return errors.New("OMP native owner working directory is not a directory")
	}
	if err = validateOMPExtension(options.Extension); err != nil {
		return err
	}
	if len(options.Arguments) > 256 || len(options.Groups) > maxOwnerTrackedItems {
		return errors.New("OMP native owner arguments exceed capacity")
	}
	retained := 0
	for _, argument := range options.Arguments {
		if !utf8.ValidString(argument) || strings.ContainsRune(argument, 0) || len(argument) > 32<<10 {
			return errors.New("OMP native owner argument is invalid")
		}
		retained += len(argument)
	}
	if retained > 1<<20 {
		return errors.New("OMP native owner arguments exceed retained capacity")
	}
	return nil
}

func validateOMPExtension(extension string) error {
	if !filepath.IsAbs(extension) || filepath.Clean(extension) != extension || filepath.Base(extension) != "extension.mjs" ||
		!validOwnerText(extension, 32<<10) {
		return errors.New("OMP managed extension path is invalid")
	}
	physical, err := filepath.EvalSymlinks(extension)
	if err != nil || physical != extension {
		return errors.New("OMP managed extension is not physically resolved")
	}
	if _, err = ompRegularPrefix(extension, 1); err != nil {
		return fmt.Errorf("OMP managed extension is invalid: %w", err)
	}
	return nil
}

func (owner *NativeOwner) Ready() <-chan struct{} { return owner.ready }
func (owner *NativeOwner) Done() <-chan struct{}  { return owner.done }

func (owner *NativeOwner) Err() error {
	owner.mu.Lock()
	defer owner.mu.Unlock()
	return owner.err
}

func (owner *NativeOwner) Primary() (OwnerBinding, bool) {
	return owner.registry.Primary()
}

func (owner *NativeOwner) GracefulEnd() (string, bool) {
	return owner.registry.GracefulEnd()
}

func (owner *NativeOwner) observeNative(frame json.RawMessage) error {
	owner.mu.Lock()
	observe := owner.observer
	owner.mu.Unlock()
	if observe == nil {
		return nil
	}
	return observe(bytes.Clone(frame))
}

func (owner *NativeOwner) run(options NativeOwnerOptions) {
	startupErr := owner.bootstrap(options)
	if startupErr != nil {
		owner.recordError(startupErr)
		owner.finish(true, nil)
		return
	}
	owner.mu.Lock()
	if owner.closing || owner.ctx.Err() != nil {
		owner.mu.Unlock()
		owner.finish(true, nil)
		return
	}
	owner.readySet = true
	close(owner.ready)
	bridge := owner.bridge
	owner.mu.Unlock()

	select {
	case request := <-owner.closeRequest:
		owner.finish(false, request.ctx)
	case <-owner.ctx.Done():
		owner.recordError(context.Cause(owner.ctx))
		owner.finish(true, nil)
	case <-owner.process.done:
		graceful := owner.graceful()
		if !graceful && owner.process.Wait() == nil {
			owner.recordError(errors.New("OMP native process exited unexpectedly"))
		}
		owner.finish(!graceful, nil)
	case <-owner.rpcDone():
		graceful := owner.graceful()
		if !graceful {
			owner.recordError(errors.Join(errors.New("OMP native RPC ended unexpectedly"), owner.rpc.Err()))
		}
		owner.finish(!graceful, nil)
	case <-owner.registry.Done():
		graceful := owner.graceful()
		if !graceful {
			owner.recordError(errors.Join(errors.New("OMP owner registry ended unexpectedly"), owner.registry.Err()))
		}
		owner.finish(!graceful, nil)
	case <-bridge.Done():
		graceful := owner.graceful()
		if !graceful {
			owner.recordError(errors.Join(errors.New("OMP managed extension ended unexpectedly"), bridge.Err()))
		}
		owner.finish(!graceful, nil)
	}
}

func (owner *NativeOwner) bootstrap(options NativeOwnerOptions) error {
	startup, cancelStartup := context.WithCancelCause(owner.ctx)
	watchDone := make(chan struct{})
	go func() {
		defer close(watchDone)
		select {
		case <-owner.process.done:
			err := owner.process.Wait()
			if err == nil {
				err = errors.New("OMP native process exited during startup")
			}
			cancelStartup(err)
		case <-startup.Done():
		}
	}()
	defer func() {
		cancelStartup(errNativeOwnerClosed)
		<-watchDone
	}()

	connection, err := owner.process.accept(startup)
	if err != nil {
		return fmt.Errorf("accept OMP managed extension: %w", errors.Join(err, context.Cause(startup)))
	}
	bridge, err := pifamily.NewBridge(connection, pifamily.BridgeHost, owner.registry.HandleBridge, pifamily.BridgeLimits{})
	if err != nil {
		return err
	}
	owner.mu.Lock()
	owner.bridge = bridge
	owner.mu.Unlock()
	if err = owner.registry.AssignBridge(bridge); err != nil {
		_ = bridge.Close()
		return err
	}
	if err = bridge.Ready(startup); err != nil {
		return fmt.Errorf("join OMP managed extension hello: %w", err)
	}

	lossDone := make(chan struct{})
	go func() {
		defer close(lossDone)
		select {
		case <-owner.registry.Done():
			cancelStartup(errors.Join(errors.New("OMP owner registry ended during startup"), owner.registry.Err()))
		case <-bridge.Done():
			cancelStartup(errors.Join(errors.New("OMP managed extension ended during startup"), bridge.Err()))
		case <-owner.rpcDone():
			cancelStartup(errors.Join(errors.New("OMP native RPC ended during startup"), owner.rpcError()))
		case <-startup.Done():
		}
	}()
	defer func() {
		cancelStartup(errNativeOwnerClosed)
		<-lossDone
	}()

	var rpcReady chan error
	rpcJoined := owner.rpc == nil
	if owner.rpc != nil {
		rpcReady = make(chan error, 1)
		go func() { rpcReady <- owner.rpc.Ready(startup) }()
	}
	registryReady := false
	defer func() {
		if !rpcJoined {
			cancelStartup(errNativeOwnerClosed)
			<-rpcReady
		}
	}()
	registryReadySignal := owner.registry.Ready()
	for !registryReady || !rpcJoined {
		select {
		case err = <-rpcReady:
			rpcJoined = true
			if err != nil {
				return fmt.Errorf("join OMP native RPC: %w", err)
			}
		case <-registryReadySignal:
			registryReady = true
			registryReadySignal = nil
		case <-startup.Done():
			return context.Cause(startup)
		}
	}
	binding, ok := owner.registry.Primary()
	if !ok || binding.Scope != ownerScopePrimary {
		return errors.New("OMP primary owner is unavailable after readiness")
	}
	// options.CWD is the direct child's physical launch directory. OMP may
	// legitimately select another live project during startup (for example a
	// resumed session). The validated managed description is authoritative for
	// the native session cwd.
	if owner.rpc != nil {
		var raw json.RawMessage
		if err = owner.rpc.Call(startup, "get_state", nil, &raw); err != nil {
			return fmt.Errorf("read OMP native state: %w", err)
		}
		state, stateErr := decodeNativeOwnerState(raw)
		if stateErr != nil {
			return stateErr
		}
		if *state.SessionID != binding.SessionID || *state.IsStreaming || *state.IsCompacting || *state.QueuedMessageCount != 0 {
			return errors.New("OMP native state contradicts its managed owner")
		}
		if len(state.SessionName) != 0 && !bytes.Equal(bytes.TrimSpace(state.SessionName), []byte("null")) {
			var name string
			if json.Unmarshal(state.SessionName, &name) != nil || !validOwnerText(name, 4096) || name != binding.Name {
				return errors.New("OMP native name contradicts its managed owner")
			}
		}
	}
	if err = owner.process.lock.Rename(binding.SessionID); err != nil {
		return fmt.Errorf("bind OMP native session lock: %w", err)
	}
	if err = owner.startupFailure(startup, bridge); err != nil {
		return err
	}
	return nil
}

func (owner *NativeOwner) startupFailure(startup context.Context, bridge *pifamily.Bridge) error {
	if err := context.Cause(startup); err != nil {
		return err
	}
	select {
	case <-owner.process.done:
		err := owner.process.Wait()
		if err == nil {
			err = errors.New("OMP native process exited during startup")
		}
		return err
	default:
	}
	select {
	case <-owner.registry.Done():
		return errors.Join(errors.New("OMP owner registry ended during startup"), owner.registry.Err())
	default:
	}
	select {
	case <-bridge.Done():
		return errors.Join(errors.New("OMP managed extension ended during startup"), bridge.Err())
	default:
	}
	select {
	case <-owner.rpcDone():
		return errors.Join(errors.New("OMP native RPC ended during startup"), owner.rpcError())
	default:
	}
	return nil
}

func (owner *NativeOwner) rpcDone() <-chan struct{} {
	if owner.rpc == nil {
		return nil
	}
	return owner.rpc.Done()
}

func (owner *NativeOwner) rpcError() error {
	if owner.rpc == nil {
		return nil
	}
	return owner.rpc.Err()
}

func decodeNativeOwnerState(raw json.RawMessage) (nativeOwnerState, error) {
	var state nativeOwnerState
	if len(raw) == 0 || json.Unmarshal(raw, &state) != nil || state.SessionID == nil ||
		state.IsStreaming == nil || state.IsCompacting == nil || state.QueuedMessageCount == nil ||
		!validOwnerID(*state.SessionID) || *state.QueuedMessageCount < 0 {
		return state, errors.New("OMP native state is invalid")
	}
	return state, nil
}

func (owner *NativeOwner) graceful() bool {
	_, graceful := owner.registry.GracefulEnd()
	return graceful
}

func (owner *NativeOwner) finish(force bool, closeCtx context.Context) {
	if closeCtx == nil {
		closeCtx = context.Background()
	}
	owner.mu.Lock()
	owner.closing = true
	bridge := owner.bridge
	ready := owner.readySet
	owner.observer = nil
	owner.mu.Unlock()
	operationCtx, cancelOperations := context.WithCancelCause(closeCtx)
	joinOperations := watchNativeOwnerContext(owner.ctx, func() {
		cancelOperations(context.Cause(owner.ctx))
	})
	finishOperations := func() {
		cancelOperations(errNativeOwnerClosed)
		joinOperations()
	}

	var inputErr error
	if !force && ready {
		if owner.graceful() {
			// Native quit may reach us before an explicit Close request.
		} else if err := owner.registry.shutdownPrimary(operationCtx); err != nil {
			owner.recordError(fmt.Errorf("request OMP native shutdown: %w", err))
			force = true
		}
		if !force {
			if owner.rpc != nil {
				inputErr = owner.rpc.EndInput(operationCtx)
			}
		}
	}
	if force || !ready {
		owner.process.Force()
		if owner.rpc != nil {
			_ = owner.rpc.Close()
		}
		if bridge != nil {
			_ = bridge.Close()
		}
	}

	childErr := owner.waitProcess(operationCtx)
	graceful := owner.graceful()
	var rpcErr error
	if owner.rpc != nil {
		if !force && childErr == nil && graceful {
			rpcErr = drainNativeOwnerRPC(operationCtx, owner.rpc)
		} else {
			rpcErr = owner.rpc.Close()
		}
	}
	registryErr := owner.registry.Close()
	if inputErr != nil && !(graceful && childErr == nil && expectedNativeOwnerRPCShutdown(inputErr)) {
		owner.recordError(fmt.Errorf("close OMP native input: %w", inputErr))
	}
	if !graceful || childErr != nil {
		owner.recordError(rpcErr)
	} else if rpcErr != nil && !errors.Is(rpcErr, io.EOF) && !errors.Is(rpcErr, errNativeRPCClosed) {
		owner.recordError(rpcErr)
	}
	owner.recordError(registryErr)
	if !force && ready && !graceful {
		owner.recordError(errors.New("OMP primary owner did not confirm shutdown"))
	}
	if childErr != nil {
		if stderr := strings.TrimSpace(owner.process.stderr.String()); stderr != "" {
			childErr = errors.Join(childErr, errors.New(stderr))
		}
		owner.recordError(childErr)
	}
	if closeCtx.Err() != nil {
		owner.recordError(closeCtx.Err())
	}
	finishOperations()
	owner.recordError(owner.process.Cleanup())
	owner.cancel(errNativeOwnerClosed)
	close(owner.done)
}

// Once a graceful native process has exited successfully, its stdout has a
// finite owned end. Let the RPC reader consume every buffered frame before
// closing it so an already-written protocol failure cannot be hidden by the
// owner's intentional shutdown. The operation context still bounds inherited
// or otherwise held stdout, and its cancellation remains part of the result.
func drainNativeOwnerRPC(ctx context.Context, rpc *nativeRPC) error {
	select {
	case <-rpc.Done():
		return errors.Join(rpc.Err(), context.Cause(ctx))
	case <-ctx.Done():
		return errors.Join(context.Cause(ctx), rpc.Close())
	}
}

func watchNativeOwnerContext(ctx context.Context, callback func()) func() {
	done := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		callback()
		close(done)
	})
	return func() {
		if stop() {
			close(done)
		}
		<-done
	}
}

func expectedNativeOwnerRPCShutdown(err error) bool {
	return errors.Is(err, io.EOF) || errors.Is(err, errNativeRPCClosed)
}

func (owner *NativeOwner) waitProcess(ctx context.Context) error {
	select {
	case <-owner.process.done:
		return owner.process.Wait()
	case <-ctx.Done():
		owner.process.Force()
		return errors.Join(owner.process.Wait(), ctx.Err())
	case <-owner.ctx.Done():
		owner.process.Force()
		return errors.Join(owner.process.Wait(), context.Cause(owner.ctx))
	}
}

func (owner *NativeOwner) recordError(err error) {
	if err == nil {
		return
	}
	owner.mu.Lock()
	owner.err = errors.Join(owner.err, err)
	owner.mu.Unlock()
}

func (owner *NativeOwner) Close(ctx context.Context) error {
	if ctx == nil {
		return errors.New("OMP native owner Close requires context")
	}
	first := false
	owner.closeOnce.Do(func() {
		first = true
		owner.mu.Lock()
		owner.closing = true
		ready := owner.readySet
		owner.mu.Unlock()
		if !ready {
			owner.cancel(errNativeOwnerClosed)
			owner.process.Force()
		}
		owner.closeRequest <- nativeOwnerClose{ctx: ctx}
	})
	if first {
		callbackDone := make(chan struct{})
		stop := context.AfterFunc(ctx, func() {
			owner.cancel(ctx.Err())
			owner.process.Force()
			if owner.rpc != nil {
				_ = owner.rpc.Close()
			}
			owner.mu.Lock()
			bridge := owner.bridge
			owner.mu.Unlock()
			if bridge != nil {
				_ = bridge.Close()
			}
			close(callbackDone)
		})
		<-owner.done
		if stop() {
			close(callbackDone)
		}
		<-callbackDone
	} else {
		<-owner.done
	}
	return owner.Err()
}
