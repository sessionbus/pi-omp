// SPDX-License-Identifier: MIT

package omp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	kit "github.com/antst/sessionbus/bus/sdk/go"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
	"github.com/sessionbus/peer-common/host"
	"github.com/sessionbus/pi-omp/wrappers/pifamily"
)

const (
	Product = "omp-peer"

	ownerTopologyLane        = "lane"
	ownerTopologyInteractive = "interactive"
	ownerScopePrimary        = "primary"
	ownerScopeChild          = "child"
	ownerModeRPC             = "rpc"
	ownerModeTUI             = "tui"
	ownerModePrint           = "print"

	maxOwnerBindings      = 256
	maxOwnerWork          = 256
	maxOwnerTrackedItems  = 256
	maxOwnerTextBytes     = 1 << 20
	maxOwnerRetainedBytes = 32 << 20
	ownerPublicRetryDelay = 2 * time.Second
)

type OwnerRegistryOptions struct {
	Topology      string
	Socket        string
	Directory     string
	InitialName   string
	Groups        []string
	PrimaryCaller *kit.Caller
}

type OwnerBinding struct {
	OwnerToken string
	SessionID  string
	Name       string
	CWD        string
	Scope      string
	Mode       string
}

type ownerRegistryState struct {
	OwnerBinding
	caller               *kit.Caller
	generation           uint64
	admitted             bool
	published            bool
	terminal             bool
	publicCtx            context.Context
	publicCancel         context.CancelFunc
	publicDone           chan struct{}
	attempt              *ownerPublicAttempt
	stageGate            chan struct{}
	staged               []*ownerStagedDelivery
	batches              map[string]*ownerDeliveryBatch
	completed            []string
	completedSet         map[string]struct{}
	preflights           map[string]string
	preflightOrder       []string
	preflightChanged     chan struct{}
	consumedPreflights   []string
	consumedPreflightSet map[string]struct{}
	nextReport           uint64
	reports              map[uint64]ownerReport
	retainedBytes        int
}

type ownerPublicAttempt struct {
	conn         *kit.Connection
	caller       *kit.Caller
	deliveries   chan ownerPublicDelivery
	deliveryDone chan struct{}
	stopCancel   func() bool
	cancelDone   chan struct{}

	mu       sync.Mutex
	closed   bool
	handlers sync.WaitGroup
}

type ownerPublicDelivery struct {
	ctx           context.Context
	conn          *kit.Connection
	request       *kit.Request
	value         kit.DeliveryRequest
	retainedBytes int
}

type ownerStagedDelivery struct {
	messageID    string
	acknowledged bool
}

type ownerDeliveryBatch struct {
	messageIDs []string
	phase      int
}

type ownerReport struct {
	preflight     *ownerPreflightRequest
	delivery      *ownerDeliveryObserveRequest
	retainedBytes int
}

type OwnerRegistry struct {
	ctx    context.Context
	cancel context.CancelFunc

	topology, socket, directory, initialName string
	groups                                   []string
	primaryCaller                            *kit.Caller

	mu                sync.Mutex
	bridge            *pifamily.Bridge
	bridgeAssigned    chan struct{}
	bindings          map[string]*ownerRegistryState
	retiredTokens     map[string]struct{}
	retiredOrder      []string
	primaryToken      string
	everPrimary       bool
	lastPrimaryReason string
	lastPrimaryToken  string
	lastPrimaryID     string
	nextGeneration    uint64
	ending            bool
	err               error
	retainedBytes     int

	ready     chan struct{}
	readyOnce sync.Once
	gate      chan struct{}
	work      sync.WaitGroup
	closeOnce sync.Once
	closed    chan struct{}

	dialPublic  func(context.Context, string, string) (net.Conn, error)
	retryPublic func(context.Context) error
}

type ownerReadyRequest struct {
	Topology   string `json:"topology"`
	Directory  string `json:"directory"`
	Scope      string `json:"scope"`
	Mode       string `json:"mode"`
	OwnerToken string `json:"owner_token"`
	SessionID  string `json:"session_id"`
	Name       string `json:"name"`
}

type ownerSwitchRequest struct {
	Topology           string `json:"topology"`
	Scope              string `json:"scope"`
	Mode               string `json:"mode"`
	PreviousOwnerToken string `json:"previous_owner_token"`
	OwnerToken         string `json:"owner_token"`
	PreviousSessionID  string `json:"previous_session_id"`
	SessionID          string `json:"session_id"`
	Name               string `json:"name"`
	Reason             string `json:"reason"`
}

type ownerEndRequest struct {
	Topology   string `json:"topology"`
	Scope      string `json:"scope"`
	Mode       string `json:"mode"`
	OwnerToken string `json:"owner_token"`
	SessionID  string `json:"session_id"`
	Reason     string `json:"reason"`
}

type ownerToolRequest struct {
	OwnerToken string          `json:"owner_token"`
	SessionID  string          `json:"session_id"`
	CallID     string          `json:"call_id"`
	Action     string          `json:"action"`
	Arguments  json.RawMessage `json:"arguments"`
}

type ownerDescribeRequest struct {
	OwnerToken string `json:"owner_token"`
	SessionID  string `json:"session_id"`
}

type ownerDescribeResult struct {
	OwnerToken string `json:"owner_token"`
	SessionID  string `json:"session_id"`
	Name       string `json:"name"`
	CWD        string `json:"cwd"`
}

type ownerStageRequest struct {
	OwnerToken string `json:"owner_token"`
	SessionID  string `json:"session_id"`
	MessageID  string `json:"message_id"`
	Body       string `json:"body"`
}

type ownerStageResult struct {
	OwnerToken string `json:"owner_token"`
	SessionID  string `json:"session_id"`
	MessageID  string `json:"message_id"`
	Queued     *bool  `json:"queued"`
	Reason     string `json:"reason,omitempty"`
}

type ownerShutdownResult struct {
	OwnerToken string `json:"owner_token"`
	SessionID  string `json:"session_id"`
	Requested  bool   `json:"requested"`
}

type ownerPreflightRequest struct {
	OwnerToken     string `json:"owner_token"`
	SessionID      string `json:"session_id"`
	ReportSequence uint64 `json:"report_sequence"`
	RunToken       string `json:"run_token"`
	Prompt         string `json:"prompt"`
}

type ownerDeliveryObserveRequest struct {
	OwnerToken     string   `json:"owner_token"`
	SessionID      string   `json:"session_id"`
	ReportSequence uint64   `json:"report_sequence"`
	BatchToken     string   `json:"batch_token"`
	Phase          string   `json:"phase"`
	MessageIDs     []string `json:"message_ids"`
}

func NewOwnerRegistry(ctx context.Context, options OwnerRegistryOptions) (*OwnerRegistry, error) {
	if ctx == nil {
		return nil, errors.New("OMP owner registry requires context")
	}
	if options.Topology != ownerTopologyLane && options.Topology != ownerTopologyInteractive {
		return nil, errors.New("OMP owner registry topology is invalid")
	}
	if !filepath.IsAbs(options.Socket) || !filepath.IsAbs(options.Directory) ||
		!validOwnerText(options.Socket, 32<<10) || !validOwnerText(options.Directory, 32<<10) {
		return nil, errors.New("OMP owner registry paths must be absolute")
	}
	if !validOwnerText(options.InitialName, 4096) {
		return nil, errors.New("OMP owner registry initial name is invalid")
	}
	if (options.Topology == ownerTopologyLane) != (options.PrimaryCaller != nil) {
		return nil, errors.New("OMP primary caller does not match topology")
	}
	baseRetained := len(options.Topology) + len(options.Socket) + len(options.Directory) + len(options.InitialName)
	if len(options.Groups) > maxOwnerTrackedItems {
		return nil, errors.New("OMP owner registry groups exceed capacity")
	}
	for _, group := range options.Groups {
		if !validOwnerText(group, 4096) {
			return nil, errors.New("OMP owner registry group is invalid")
		}
		baseRetained += len(group)
	}
	if baseRetained > maxOwnerRetainedBytes {
		return nil, errors.New("OMP owner registry retained bytes exceed capacity")
	}
	lifetime, cancel := context.WithCancel(ctx)
	groups := slices.Clone(options.Groups)
	if groups == nil {
		groups = []string{}
	}
	registry := &OwnerRegistry{
		ctx: lifetime, cancel: cancel, topology: options.Topology, socket: options.Socket,
		directory: options.Directory, initialName: options.InitialName,
		groups: groups, primaryCaller: options.PrimaryCaller,
		bindings: make(map[string]*ownerRegistryState), retiredTokens: make(map[string]struct{}),
		ready: make(chan struct{}), gate: make(chan struct{}, 1), bridgeAssigned: make(chan struct{}),
		closed: make(chan struct{}), retainedBytes: baseRetained,
		dialPublic: func(ctx context.Context, network, address string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, network, address)
		},
		retryPublic: func(ctx context.Context) error {
			timer := time.NewTimer(ownerPublicRetryDelay)
			defer timer.Stop()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-timer.C:
				return nil
			}
		},
	}
	registry.gate <- struct{}{}
	return registry, nil
}

func (registry *OwnerRegistry) AssignBridge(bridge *pifamily.Bridge) error {
	if bridge == nil {
		return errors.New("OMP owner bridge is missing")
	}
	registry.mu.Lock()
	if registry.ending || registry.bridge != nil {
		registry.mu.Unlock()
		return errors.New("OMP owner bridge is already assigned")
	}
	registry.bridge = bridge
	close(registry.bridgeAssigned)
	registry.work.Add(1)
	registry.mu.Unlock()
	go func() {
		defer registry.work.Done()
		<-bridge.Done()
		bridgeErr := bridge.Err()
		registry.mu.Lock()
		unexpected := !registry.ending && registry.bridge == bridge
		graceful := unexpected && errors.Is(bridgeErr, pifamily.ErrBridgeClosed) && registry.everPrimary && registry.primaryToken == "" &&
			registry.lastPrimaryReason != "" && len(registry.bindings) == 0
		registry.mu.Unlock()
		if graceful {
			// The native process closes its bridge after every admitted factory has
			// acknowledged session_end. NativeOwner still owns the process join, but
			// the registry itself has reached an orderly terminal state.
			registry.cancel()
		} else if unexpected {
			registry.fail(errors.Join(errors.New("OMP private bridge ended"), bridgeErr))
		}
	}()
	return nil
}

func (registry *OwnerRegistry) Ready() <-chan struct{} { return registry.ready }
func (registry *OwnerRegistry) Done() <-chan struct{}  { return registry.ctx.Done() }

func (registry *OwnerRegistry) Err() error {
	registry.mu.Lock()
	defer registry.mu.Unlock()
	return registry.err
}

func (registry *OwnerRegistry) Primary() (OwnerBinding, bool) {
	registry.mu.Lock()
	defer registry.mu.Unlock()
	state := registry.bindings[registry.primaryToken]
	if state == nil || !state.admitted {
		return OwnerBinding{}, false
	}
	return state.OwnerBinding, true
}

func (registry *OwnerRegistry) GracefulEnd() (string, bool) {
	registry.mu.Lock()
	defer registry.mu.Unlock()
	return registry.lastPrimaryReason, registry.everPrimary && registry.primaryToken == "" && registry.lastPrimaryReason != ""
}

func (registry *OwnerRegistry) HandleBridge(ctx context.Context, method string, raw json.RawMessage) (json.RawMessage, error) {
	if ctx == nil {
		return nil, errors.New("OMP owner bridge handler requires context")
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-registry.ctx.Done():
		return nil, context.Cause(registry.ctx)
	case <-registry.bridgeAssigned:
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := context.Cause(registry.ctx); err != nil {
		return nil, err
	}
	var value any
	var err error
	switch method {
	case "owner.ready":
		value, err = registry.ownerReady(ctx, raw)
	case "owner.switch":
		value, err = registry.ownerSwitch(ctx, raw)
	case "session_end":
		value, err = registry.ownerEnd(ctx, raw)
	case "tool.call":
		value, err = registry.toolCall(ctx, raw)
	case "run.preflight":
		value, err = registry.recordPreflight(raw)
	case "delivery.observe":
		value, err = registry.observeDelivery(raw)
	default:
		return nil, pifamily.NewBridgeCallError("method_not_found", "OMP owner bridge method is unavailable")
	}
	if err != nil {
		return nil, err
	}
	body, err := json.Marshal(value)
	if err != nil {
		registry.fail(err)
		return nil, err
	}
	return body, nil
}

func (registry *OwnerRegistry) ownerReady(ctx context.Context, raw json.RawMessage) (any, error) {
	var request ownerReadyRequest
	if err := decodeOwnerJSON(raw, &request); err != nil || !registry.validClassification(request.Topology, request.Scope, request.Mode) ||
		request.Directory != registry.directory || !validOwnerToken(request.OwnerToken) || !validOwnerID(request.SessionID) ||
		!validOwnerText(request.Name, 4096) {
		return nil, registry.protocolFailure("invalid OMP owner.ready", err)
	}
	if err := registry.acquire(ctx); err != nil {
		return nil, err
	}
	defer registry.release()
	description, err := registry.describe(ctx, request.OwnerToken, request.SessionID)
	if err != nil {
		return nil, registry.protocolFailure("OMP native description failed", err)
	}
	if err = registry.admit(ctx, request.Scope, request.Mode, description); err != nil {
		return nil, registry.protocolFailure("OMP owner admission failed", err)
	}
	return map[string]string{"owner_token": request.OwnerToken, "session_id": request.SessionID}, nil
}

func (registry *OwnerRegistry) ownerSwitch(ctx context.Context, raw json.RawMessage) (any, error) {
	var request ownerSwitchRequest
	if err := decodeOwnerJSON(raw, &request); err != nil || !registry.validClassification(request.Topology, request.Scope, request.Mode) ||
		request.Scope != ownerScopePrimary || !validOwnerToken(request.PreviousOwnerToken) || !validOwnerToken(request.OwnerToken) ||
		request.PreviousOwnerToken == request.OwnerToken || !validOwnerID(request.PreviousSessionID) || !validOwnerID(request.SessionID) ||
		!validOwnerText(request.Name, 4096) || !validOwnerText(request.Reason, 256) || request.Reason == "" {
		return nil, registry.protocolFailure("invalid OMP owner.switch", err)
	}
	if registry.topology == ownerTopologyLane {
		return nil, registry.protocolFailure("OMP lane cannot replace its admitted native session", nil)
	}
	if err := registry.acquire(ctx); err != nil {
		return nil, err
	}
	defer registry.release()
	registry.mu.Lock()
	previous := registry.bindings[request.PreviousOwnerToken]
	current := previous != nil && !previous.terminal && previous.OwnerToken == registry.primaryToken && previous.SessionID == request.PreviousSessionID &&
		previous.Scope == request.Scope && previous.Mode == request.Mode
	registry.mu.Unlock()
	if !current {
		return nil, pifamily.NewBridgeCallError("stale_owner", "OMP owner.switch does not own the current primary")
	}
	description, err := registry.describe(ctx, request.OwnerToken, request.SessionID)
	if err != nil {
		return nil, registry.protocolFailure("OMP replacement description failed", err)
	}
	if err = registry.removeForSwitch(ctx, previous); err != nil {
		return nil, err
	}
	if err = registry.admit(ctx, request.Scope, request.Mode, description); err != nil {
		return nil, registry.protocolFailure("OMP replacement admission failed", err)
	}
	return map[string]string{"owner_token": request.OwnerToken, "session_id": request.SessionID}, nil
}

func (registry *OwnerRegistry) ownerEnd(ctx context.Context, raw json.RawMessage) (any, error) {
	var request ownerEndRequest
	if err := decodeOwnerJSON(raw, &request); err != nil || !registry.validClassification(request.Topology, request.Scope, request.Mode) ||
		!validOwnerToken(request.OwnerToken) || !validOwnerID(request.SessionID) || !validOwnerText(request.Reason, 256) || request.Reason == "" {
		return nil, registry.protocolFailure("invalid OMP session_end", err)
	}
	if err := registry.acquire(ctx); err != nil {
		return nil, err
	}
	defer registry.release()
	registry.mu.Lock()
	state := registry.bindings[request.OwnerToken]
	current := state != nil && state.SessionID == request.SessionID && state.Scope == request.Scope && state.Mode == request.Mode
	registry.mu.Unlock()
	if !current {
		return nil, pifamily.NewBridgeCallError("stale_owner", "OMP session_end does not own a current binding")
	}
	registry.remove(state, request.Reason)
	return map[string]string{"owner_token": request.OwnerToken, "session_id": request.SessionID}, nil
}

func (registry *OwnerRegistry) toolCall(ctx context.Context, raw json.RawMessage) (any, error) {
	var request ownerToolRequest
	if err := decodeOwnerJSON(raw, &request); err != nil || !validOwnerToken(request.OwnerToken) || !validOwnerID(request.SessionID) ||
		!validOwnerID(request.CallID) || !slices.Contains(kit.Actions, request.Action) || request.Arguments == nil || !json.Valid(request.Arguments) {
		return nil, registry.protocolFailure("invalid OMP tool.call", err)
	}
	state, caller, generation, err := registry.currentCaller(request.OwnerToken, request.SessionID)
	if err != nil {
		return nil, err
	}
	result, err := caller.Action(ctx, request.Action, request.Arguments)
	if err != nil {
		return nil, pifamily.NewBridgeCallError("tool_error", err.Error())
	}
	if err = registry.checkGeneration(state, generation); err != nil {
		return nil, err
	}
	return struct {
		OwnerToken string          `json:"owner_token"`
		SessionID  string          `json:"session_id"`
		CallID     string          `json:"call_id"`
		Result     json.RawMessage `json:"result"`
	}{request.OwnerToken, request.SessionID, request.CallID, result}, nil
}

func (registry *OwnerRegistry) recordPreflight(raw json.RawMessage) (any, error) {
	var request ownerPreflightRequest
	if err := decodeOwnerJSON(raw, &request); err != nil || !validOwnerToken(request.OwnerToken) || !validOwnerID(request.SessionID) ||
		request.ReportSequence == 0 || !validOwnerToken(request.RunToken) || !validOwnerText(request.Prompt, maxOwnerTextBytes) {
		return nil, registry.protocolFailure("invalid OMP run.preflight", err)
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	state := registry.bindings[request.OwnerToken]
	if registry.ending || state == nil || !state.admitted || state.SessionID != request.SessionID {
		return nil, pifamily.NewBridgeCallError("stale_owner", "OMP preflight does not own a current binding")
	}
	if err := registry.enqueueReportLocked(state, request.ReportSequence, ownerReport{preflight: &request, retainedBytes: len(raw)}); err != nil {
		return nil, err
	}
	return struct {
		OwnerToken     string `json:"owner_token"`
		SessionID      string `json:"session_id"`
		ReportSequence uint64 `json:"report_sequence"`
		RunToken       string `json:"run_token"`
	}{request.OwnerToken, request.SessionID, request.ReportSequence, request.RunToken}, nil
}

func (registry *OwnerRegistry) observeDelivery(raw json.RawMessage) (any, error) {
	var request ownerDeliveryObserveRequest
	if err := decodeOwnerJSON(raw, &request); err != nil || !validOwnerToken(request.OwnerToken) || !validOwnerID(request.SessionID) ||
		request.ReportSequence == 0 || !validOwnerToken(request.BatchToken) || !validDeliveryPhase(request.Phase) || !validOwnerMessageIDs(request.MessageIDs) {
		return nil, registry.protocolFailure("invalid OMP delivery.observe", err)
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	state := registry.bindings[request.OwnerToken]
	if registry.ending || state == nil || !state.admitted || state.SessionID != request.SessionID {
		return nil, pifamily.NewBridgeCallError("stale_owner", "OMP delivery observation does not own a current binding")
	}
	if err := registry.enqueueReportLocked(state, request.ReportSequence, ownerReport{delivery: &request, retainedBytes: len(raw)}); err != nil {
		return nil, err
	}
	return struct {
		OwnerToken     string   `json:"owner_token"`
		SessionID      string   `json:"session_id"`
		ReportSequence uint64   `json:"report_sequence"`
		BatchToken     string   `json:"batch_token"`
		Phase          string   `json:"phase"`
		MessageIDs     []string `json:"message_ids"`
	}{request.OwnerToken, request.SessionID, request.ReportSequence, request.BatchToken, request.Phase, slices.Clone(request.MessageIDs)}, nil
}

func (registry *OwnerRegistry) enqueueReportLocked(state *ownerRegistryState, sequence uint64, report ownerReport) error {
	if sequence <= state.nextReport || sequence-state.nextReport > maxOwnerTrackedItems {
		return registry.protocolFailureLocked("OMP owner report sequence is invalid")
	}
	if _, duplicate := state.reports[sequence]; duplicate || len(state.reports) >= maxOwnerTrackedItems {
		return registry.protocolFailureLocked("OMP owner report capacity or identity is invalid")
	}
	if err := registry.reserveStateLocked(state, report.retainedBytes); err != nil {
		return err
	}
	state.reports[sequence] = report
	for {
		next := state.nextReport + 1
		pending, ok := state.reports[next]
		if !ok {
			return nil
		}
		delete(state.reports, next)
		if err := registry.applyReportLocked(state, pending); err != nil {
			return err
		}
		state.nextReport = next
	}
}

func (registry *OwnerRegistry) applyReportLocked(state *ownerRegistryState, report ownerReport) error {
	registry.releaseStateLocked(state, report.retainedBytes)
	if (report.preflight == nil) == (report.delivery == nil) {
		return registry.protocolFailureLocked("OMP owner report kind is invalid")
	}
	if report.preflight != nil {
		request := report.preflight
		_, consumed := state.consumedPreflightSet[request.RunToken]
		if _, duplicate := state.preflights[request.RunToken]; duplicate || consumed || len(state.preflights) >= maxOwnerTrackedItems {
			return registry.protocolFailureLocked("OMP preflight capacity or identity is invalid")
		}
		if err := registry.reserveStateLocked(state, len(request.RunToken)+len(request.Prompt)); err != nil {
			return err
		}
		state.preflights[request.RunToken] = request.Prompt
		state.preflightOrder = append(state.preflightOrder, request.RunToken)
		registry.signalPreflightLocked(state)
		return nil
	}
	request := report.delivery
	phase := deliveryPhaseIndex(request.Phase)
	batch := state.batches[request.BatchToken]
	if batch == nil {
		if phase != 1 {
			return registry.protocolFailureLocked("OMP delivery observation preceded its claim")
		}
		if _, completed := state.completedSet[request.BatchToken]; completed {
			return registry.protocolFailureLocked("OMP completed delivery batch was reused")
		}
		if len(state.batches) >= maxOwnerTrackedItems {
			return registry.protocolFailureLocked("OMP delivery batch capacity is exhausted")
		}
		if len(request.MessageIDs) > len(state.staged) || !stagedPrefixEqual(request.MessageIDs, state.staged) {
			return registry.protocolFailureLocked("OMP claimed delivery batch is invalid")
		}
		if err := registry.reserveStateLocked(state, len(request.BatchToken)); err != nil {
			return err
		}
		batch = &ownerDeliveryBatch{messageIDs: slices.Clone(request.MessageIDs)}
		state.batches[request.BatchToken] = batch
		state.staged = state.staged[len(request.MessageIDs):]
	} else if !slices.Equal(request.MessageIDs, batch.messageIDs) {
		return registry.protocolFailureLocked("OMP delivery observation changed batch identity")
	}
	if phase != batch.phase+1 {
		return registry.protocolFailureLocked("OMP delivery observation phase is out of order")
	}
	// Bridge handlers execute concurrently even though their native request IDs
	// were read in order. report_sequence reconstructs that per-factory order.
	batch.phase = phase
	if phase == 4 {
		for _, messageID := range batch.messageIDs {
			registry.releaseStateLocked(state, len(messageID))
		}
		delete(state.batches, request.BatchToken)
		state.completed = append(state.completed, request.BatchToken)
		state.completedSet[request.BatchToken] = struct{}{}
		if len(state.completed) > maxOwnerTrackedItems {
			registry.releaseStateLocked(state, len(state.completed[0]))
			delete(state.completedSet, state.completed[0])
			state.completed = state.completed[1:]
		}
	}
	return nil
}

func (registry *OwnerRegistry) admit(ctx context.Context, scope, mode string, description ownerDescribeResult) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	state := &ownerRegistryState{
		OwnerBinding: OwnerBinding{
			OwnerToken: description.OwnerToken, SessionID: description.SessionID,
			Name: description.Name, CWD: description.CWD, Scope: scope, Mode: mode,
		},
		stageGate: make(chan struct{}, 1), batches: make(map[string]*ownerDeliveryBatch),
		completedSet: make(map[string]struct{}), preflights: make(map[string]string),
		preflightChanged:     make(chan struct{}),
		consumedPreflightSet: make(map[string]struct{}), reports: make(map[uint64]ownerReport),
	}
	state.stageGate <- struct{}{}
	if scope == ownerScopePrimary && registry.topology == ownerTopologyLane {
		state.caller = registry.primaryCaller
	}
	registry.mu.Lock()
	if err := registry.admissionErrorLocked(ctx); err != nil {
		registry.mu.Unlock()
		return err
	}
	if _, retired := registry.retiredTokens[state.OwnerToken]; retired || registry.bindings[state.OwnerToken] != nil || len(registry.bindings) >= maxOwnerBindings {
		registry.mu.Unlock()
		return errors.New("OMP owner token is duplicate or capacity is exhausted")
	}
	if scope == ownerScopePrimary && registry.primaryToken != "" {
		registry.mu.Unlock()
		return errors.New("OMP primary owner is already admitted")
	}
	if err := registry.reserveStateLocked(state, ownerBindingRetainedBytes(state.OwnerBinding)); err != nil {
		registry.mu.Unlock()
		return err
	}
	registry.nextGeneration++
	state.generation = registry.nextGeneration
	state.admitted = true
	if !(scope == ownerScopePrimary && registry.topology == ownerTopologyLane) {
		state.publicCtx, state.publicCancel = context.WithCancel(registry.ctx)
		state.publicDone = make(chan struct{})
		registry.work.Add(1)
	}
	registry.bindings[state.OwnerToken] = state
	if scope == ownerScopePrimary {
		registry.primaryToken = state.OwnerToken
	}
	registry.mu.Unlock()

	if scope == ownerScopePrimary && registry.topology == ownerTopologyLane {
		return registry.finishLaneAdmission(state)
	}
	go registry.publicLoop(state)
	return nil
}

func (registry *OwnerRegistry) admissionErrorLocked(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if registry.ending || registry.err != nil || registry.ctx.Err() != nil {
		return errors.Join(errors.New("OMP owner registry is unavailable for admission"), registry.err, registry.ctx.Err())
	}
	return nil
}

func (registry *OwnerRegistry) finishLaneAdmission(state *ownerRegistryState) error {
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.ending || registry.bindings[state.OwnerToken] != state {
		return errors.New("OMP lane binding changed before admission")
	}
	if state.Scope == ownerScopePrimary {
		registry.everPrimary = true
		registry.lastPrimaryReason = ""
		registry.readyOnce.Do(func() { close(registry.ready) })
	}
	return nil
}

func (registry *OwnerRegistry) publicLoop(state *ownerRegistryState) {
	defer registry.work.Done()
	defer close(state.publicDone)
	for state.publicCtx.Err() == nil {
		retry, err := registry.connectPublicAttempt(state)
		if err != nil {
			registry.fail(err)
			return
		}
		if !retry || state.publicCtx.Err() != nil {
			return
		}
		if err = registry.retryPublic(state.publicCtx); err != nil {
			return
		}
	}
}

func (registry *OwnerRegistry) connectPublicAttempt(state *ownerRegistryState) (bool, error) {
	fd, err := registry.dialPublic(state.publicCtx, "unix", registry.socket)
	if err != nil {
		return state.publicCtx.Err() == nil, nil
	}
	attempt := &ownerPublicAttempt{
		deliveries:   make(chan ownerPublicDelivery, maxOwnerWork),
		deliveryDone: make(chan struct{}),
	}
	assigned := make(chan struct{})
	var conn *kit.Connection
	conn = kit.NewConnection(fd, func(callCtx context.Context, request *kit.Request) {
		<-assigned
		if !attempt.beginHandler() {
			_ = conn.Close()
			return
		}
		defer attempt.handlers.Done()
		registry.handlePublic(callCtx, state, attempt, request)
	})
	attempt.conn = conn
	attempt.caller = kit.NewCaller(func(callCtx context.Context, method string, params any) (json.RawMessage, error) {
		return registry.callPublic(callCtx, state, attempt, method, params)
	})
	attempt.cancelDone = make(chan struct{})
	attempt.stopCancel = context.AfterFunc(state.publicCtx, func() {
		defer close(attempt.cancelDone)
		_ = conn.Close()
	})
	go registry.deliveryLoop(state, attempt)
	registry.mu.Lock()
	if registry.ending || registry.bindings[state.OwnerToken] != state || state.terminal || state.attempt != nil {
		registry.mu.Unlock()
		close(assigned)
		attempt.stop()
		return false, nil
	}
	registry.nextGeneration++
	state.generation = registry.nextGeneration
	state.attempt = attempt
	registry.mu.Unlock()
	close(assigned)

	registry.mu.Lock()
	name := state.Name
	if state.Scope == ownerScopePrimary && !registry.everPrimary && name == "" {
		name = registry.initialName
	}
	identity := kit.PeerIdentity{
		Protocol: 1, Product: Product, SessionID: state.SessionID, Name: name,
		Groups: slices.Clone(registry.groups), Info: map[string]any{"cwd": state.CWD},
	}
	registry.mu.Unlock()
	var response json.RawMessage
	err = conn.CallObserved(state.publicCtx, "session.hello", identity, &response, func() error {
		registry.mu.Lock()
		defer registry.mu.Unlock()
		if registry.ending || registry.bindings[state.OwnerToken] != state || state.terminal || state.attempt != attempt {
			return errors.New("OMP public binding changed before hello")
		}
		if extra := len(name) - len(state.Name); extra > 0 {
			if err := registry.reserveStateLocked(state, extra); err != nil {
				return err
			}
		} else if extra < 0 {
			registry.releaseStateLocked(state, -extra)
		}
		state.Name = name
		state.caller = attempt.caller
		state.published = true
		if state.Scope == ownerScopePrimary {
			registry.everPrimary = true
			registry.lastPrimaryReason = ""
			registry.readyOnce.Do(func() { close(registry.ready) })
		}
		return nil
	})
	if err != nil {
		registry.detachPublicAttempt(state, attempt)
		attempt.stop()
		var rpcErr *protocol.RPCError
		if errors.As(err, &rpcErr) && rpcErr.Code == protocol.InvalidHello {
			return false, errors.Join(errors.New("OMP public owner hello was rejected"), err)
		}
		return state.publicCtx.Err() == nil, nil
	}
	select {
	case <-state.publicCtx.Done():
	case <-registry.ctx.Done():
	case <-conn.Done():
	}
	registry.detachPublicAttempt(state, attempt)
	attempt.stop()
	return state.publicCtx.Err() == nil, nil
}

func (attempt *ownerPublicAttempt) beginHandler() bool {
	attempt.mu.Lock()
	defer attempt.mu.Unlock()
	if attempt.closed {
		return false
	}
	attempt.handlers.Add(1)
	return true
}

func (attempt *ownerPublicAttempt) stop() {
	attempt.mu.Lock()
	if attempt.closed {
		attempt.mu.Unlock()
		return
	}
	attempt.closed = true
	attempt.mu.Unlock()
	if !attempt.stopCancel() {
		<-attempt.cancelDone
	}
	_ = attempt.conn.Close()
	attempt.handlers.Wait()
	<-attempt.deliveryDone
}

func (registry *OwnerRegistry) detachPublicAttempt(state *ownerRegistryState, attempt *ownerPublicAttempt) {
	registry.mu.Lock()
	if registry.bindings[state.OwnerToken] == state && state.attempt == attempt {
		state.attempt = nil
		state.caller = nil
		state.published = false
		registry.nextGeneration++
		state.generation = registry.nextGeneration
	}
	registry.mu.Unlock()
}

func (registry *OwnerRegistry) handlePublic(ctx context.Context, state *ownerRegistryState, attempt *ownerPublicAttempt, request *kit.Request) {
	registry.mu.Lock()
	if registry.ending || registry.bindings[state.OwnerToken] != state || state.attempt != attempt || !state.admitted || !state.published || state.terminal {
		registry.mu.Unlock()
		_ = attempt.conn.Close()
		return
	}
	if request.Method == "session.superseded" {
		// Terminal admission is atomic with the exact-attempt check. An old
		// reader must never retire a replacement binding while its ACK is held.
		state.terminal = true
		state.published = false
		state.caller = nil
		registry.nextGeneration++
		state.generation = registry.nextGeneration
		registry.mu.Unlock()
		registry.fail(errors.New("OMP public session was superseded"))
		responseErr := attempt.conn.Result(request, struct{}{})
		if responseErr != nil && ctx.Err() == nil {
			registry.fail(responseErr)
		}
		_ = attempt.conn.Close()
		return
	}
	var responseErr error
	if request.Method == "message.deliver" {
		delivery, ok := request.Params.(*kit.DeliveryRequest)
		if !ok {
			registry.mu.Unlock()
			responseErr = errors.New("invalid OMP delivery parameters")
		} else {
			retained := ownerPublicDeliveryRetainedBytes(*delivery)
			if err := registry.reserveStateLocked(state, retained); err != nil {
				registry.mu.Unlock()
				responseErr = err
			} else {
				value := *delivery
				value.From.Groups = slices.Clone(delivery.From.Groups)
				select {
				case attempt.deliveries <- ownerPublicDelivery{ctx: ctx, conn: attempt.conn, request: request, value: value, retainedBytes: retained}:
					registry.mu.Unlock()
					return
				default:
					registry.releaseStateLocked(state, retained)
					registry.mu.Unlock()
					responseErr = errors.New("OMP public delivery capacity exhausted")
				}
			}
		}
	} else {
		registry.mu.Unlock()
	}
	if responseErr != nil {
		_ = attempt.conn.Close()
		if ctx.Err() == nil {
			registry.fail(responseErr)
		}
		return
	}
	responseErr = attempt.conn.Error(request, -32601, nil)
	if responseErr != nil {
		_ = attempt.conn.Close()
		if ctx.Err() == nil {
			registry.fail(responseErr)
		}
	}
}

func (registry *OwnerRegistry) deliveryLoop(state *ownerRegistryState, attempt *ownerPublicAttempt) {
	defer close(attempt.deliveryDone)
	defer func() {
		for {
			select {
			case work := <-attempt.deliveries:
				registry.releasePublicDelivery(state, work.retainedBytes)
			default:
				return
			}
		}
	}()
	for {
		select {
		case <-registry.ctx.Done():
			return
		case <-attempt.conn.Context().Done():
			return
		case work := <-attempt.deliveries:
			receipt, err := registry.deliver(work.ctx, state, work.value)
			var responseErr error
			if err != nil {
				responseErr = work.conn.Error(work.request, -32603, "OMP native delivery failed; no replay")
				if work.ctx.Err() == nil {
					registry.fail(err)
				}
			} else {
				responseErr = work.conn.Result(work.request, receipt)
			}
			if responseErr != nil {
				_ = work.conn.Close()
				registry.releasePublicDelivery(state, work.retainedBytes)
				return
			}
			registry.releasePublicDelivery(state, work.retainedBytes)
		}
	}
}

func (registry *OwnerRegistry) releasePublicDelivery(state *ownerRegistryState, size int) {
	registry.mu.Lock()
	defer registry.mu.Unlock()
	registry.releaseStateLocked(state, size)
}

func (registry *OwnerRegistry) deliver(ctx context.Context, state *ownerRegistryState, request kit.DeliveryRequest) (kit.DeliveryReceipt, error) {
	body, err := host.RenderNativeMessage(request)
	if err != nil {
		return kit.DeliveryReceipt{}, err
	}
	if !validOwnerMessageID(request.MessageID) || !validOwnerText(body, maxOwnerTextBytes) {
		return kit.DeliveryReceipt{}, errors.New("invalid OMP native staged delivery")
	}
	params := ownerStageRequest{OwnerToken: state.OwnerToken, SessionID: state.SessionID, MessageID: request.MessageID, Body: body}
	encoded, err := json.Marshal(params)
	if err != nil || len(encoded) > pifamily.DefaultBridgeLimits.MaxFrameBytes-1024 {
		return kit.DeliveryReceipt{}, errors.New("OMP native staged delivery exceeds the private bridge frame")
	}
	select {
	case <-ctx.Done():
		return kit.DeliveryReceipt{}, ctx.Err()
	case <-registry.ctx.Done():
		return kit.DeliveryReceipt{}, errors.New("OMP owner registry ended before stage admission")
	case <-state.stageGate:
	}
	defer func() { state.stageGate <- struct{}{} }()
	select {
	case <-ctx.Done():
		return kit.DeliveryReceipt{}, ctx.Err()
	case <-registry.ctx.Done():
		return kit.DeliveryReceipt{}, errors.New("OMP owner registry ended before stage admission")
	default:
	}
	bridge := registry.currentBridge()
	if bridge == nil {
		return kit.DeliveryReceipt{}, errors.New("OMP bridge is unavailable before stage admission")
	}
	record := &ownerStagedDelivery{messageID: request.MessageID}
	registry.mu.Lock()
	if registry.ending || registry.bindings[state.OwnerToken] != state || !state.admitted {
		registry.mu.Unlock()
		return kit.DeliveryReceipt{}, errors.New("OMP staged delivery does not own the current binding")
	}
	if len(state.staged) >= maxOwnerTrackedItems || ownerMessageTracked(state, request.MessageID) {
		registry.mu.Unlock()
		return kit.DeliveryReceipt{}, errors.New("OMP staged delivery capacity or identity is invalid")
	}
	if err := registry.reserveStateLocked(state, len(request.MessageID)); err != nil {
		registry.mu.Unlock()
		return kit.DeliveryReceipt{}, err
	}
	// Reserve native FIFO order before the bridge call. native.stage can enqueue
	// and a hook can report its claim before the correlated response reaches Go.
	state.staged = append(state.staged, record)
	registry.mu.Unlock()
	var raw json.RawMessage
	if err = bridge.Call(ctx, "native.stage", params, &raw); err != nil {
		registry.fail(fmt.Errorf("OMP native stage outcome is uncertain: %w", err))
		return kit.DeliveryReceipt{}, err
	}
	var result ownerStageResult
	if err = decodeOwnerJSON(raw, &result); err != nil || result.OwnerToken != state.OwnerToken || result.SessionID != state.SessionID ||
		result.MessageID != request.MessageID || result.Queued == nil || (*result.Queued && result.Reason != "") || (!*result.Queued && result.Reason != "queue_full") {
		registry.fail(errors.New("OMP native stage acknowledgement is invalid"))
		return kit.DeliveryReceipt{}, errors.New("OMP native stage acknowledgement is invalid")
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.ending || registry.bindings[state.OwnerToken] != state || !state.admitted {
		return kit.DeliveryReceipt{}, errors.New("OMP native stage crossed an owner generation")
	}
	if !*result.Queued {
		index := slices.Index(state.staged, record)
		if index < 0 || record.acknowledged {
			failure := registry.protocolFailureLocked("OMP rejected native stage was already claimed")
			return kit.DeliveryReceipt{}, failure
		}
		state.staged = slices.Delete(state.staged, index, index+1)
		registry.releaseStateLocked(state, len(record.messageID))
		return kit.DeliveryReceipt{Disposition: "rejected", Reason: result.Reason}, nil
	}
	record.acknowledged = true
	return kit.DeliveryReceipt{Disposition: "queued_for_next_turn"}, nil
}

func (registry *OwnerRegistry) describe(ctx context.Context, token, sessionID string) (ownerDescribeResult, error) {
	bridge := registry.currentBridge()
	if bridge == nil {
		return ownerDescribeResult{}, errors.New("OMP bridge is unavailable")
	}
	var raw json.RawMessage
	if err := bridge.Call(ctx, "native.describe", ownerDescribeRequest{OwnerToken: token, SessionID: sessionID}, &raw); err != nil {
		return ownerDescribeResult{}, err
	}
	var result ownerDescribeResult
	if err := decodeOwnerJSON(raw, &result); err != nil || result.OwnerToken != token || result.SessionID != sessionID ||
		!validOwnerText(result.Name, 4096) || !validOwnerCWD(result.CWD) {
		return ownerDescribeResult{}, errors.New("OMP native description is invalid")
	}
	return result, nil
}

func (registry *OwnerRegistry) shutdownPrimary(ctx context.Context) error {
	state, ok := registry.Primary()
	if !ok || state.Scope != ownerScopePrimary {
		return errors.New("OMP primary owner is unavailable")
	}
	bridge := registry.currentBridge()
	if bridge == nil {
		return errors.New("OMP bridge is unavailable")
	}
	var raw json.RawMessage
	if err := bridge.Call(ctx, "native.shutdown", ownerDescribeRequest{OwnerToken: state.OwnerToken, SessionID: state.SessionID}, &raw); err != nil {
		return err
	}
	var result ownerShutdownResult
	if err := decodeOwnerJSON(raw, &result); err != nil || result.OwnerToken != state.OwnerToken || result.SessionID != state.SessionID || !result.Requested {
		return errors.New("OMP native shutdown acknowledgement is invalid")
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	current := registry.bindings[state.OwnerToken]
	if !registry.ending && current != nil && current.admitted && current.SessionID == state.SessionID &&
		registry.primaryToken == state.OwnerToken {
		return nil
	}
	if !registry.ending && registry.primaryToken == "" && registry.lastPrimaryReason != "" &&
		registry.lastPrimaryToken == state.OwnerToken && registry.lastPrimaryID == state.SessionID {
		return nil
	}
	return pifamily.NewBridgeCallError("stale_owner", "OMP native shutdown crossed an owner generation")
}

func (registry *OwnerRegistry) current(token, sessionID string) (*ownerRegistryState, error) {
	registry.mu.Lock()
	defer registry.mu.Unlock()
	state := registry.bindings[token]
	if registry.ending || state == nil || !state.admitted || state.SessionID != sessionID {
		return nil, pifamily.NewBridgeCallError("stale_owner", "OMP request does not own a current binding")
	}
	return state, nil
}

func (registry *OwnerRegistry) currentCaller(token, sessionID string) (*ownerRegistryState, *kit.Caller, uint64, error) {
	registry.mu.Lock()
	defer registry.mu.Unlock()
	state := registry.bindings[token]
	if registry.ending || state == nil || !state.admitted || state.SessionID != sessionID {
		return nil, nil, 0, pifamily.NewBridgeCallError("stale_owner", "OMP request does not own a current binding")
	}
	if state.caller == nil || (!state.published && !(registry.topology == ownerTopologyLane && state.Scope == ownerScopePrimary)) {
		return nil, nil, 0, pifamily.NewBridgeCallError("tool_error", "OMP public owner is unavailable")
	}
	return state, state.caller, state.generation, nil
}

func (registry *OwnerRegistry) checkGeneration(state *ownerRegistryState, generation uint64) error {
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.ending || registry.bindings[state.OwnerToken] != state || !state.admitted || state.terminal {
		return pifamily.NewBridgeCallError("stale_owner", "OMP request crossed an owner generation")
	}
	// A decoded public response belongs to the immutable connection captured by
	// its Caller. That accepted result wins a following EOF/reconnect. Lane
	// primary callers are external to this registry and still require the exact
	// binding generation.
	if !(registry.topology == ownerTopologyLane && state.Scope == ownerScopePrimary) || state.generation == generation {
		return nil
	}
	return pifamily.NewBridgeCallError("stale_owner", "OMP request crossed an owner generation")
}

func (registry *OwnerRegistry) takePreflight(token, sessionID, runToken string) (string, error) {
	registry.mu.Lock()
	defer registry.mu.Unlock()
	state := registry.bindings[token]
	if registry.ending || state == nil || !state.admitted || state.SessionID != sessionID {
		return "", pifamily.NewBridgeCallError("stale_owner", "OMP preflight does not own a current binding")
	}
	prompt, ok := state.preflights[runToken]
	if !ok {
		return "", pifamily.NewBridgeCallError("missing_preflight", "OMP native preflight is unavailable")
	}
	return registry.consumePreflightLocked(state, runToken, prompt), nil
}

// waitPreflight joins the next native preflight for one exact binding. The
// run token is generated inside the extension, so the controller must consume
// source order rather than search past an earlier foreign witness for equal
// text. Cancellation leaves any arrived witness owned by the registry.
func (registry *OwnerRegistry) waitPreflight(ctx context.Context, token, sessionID, expectedPrompt string) (string, error) {
	if ctx == nil {
		return "", errors.New("OMP preflight wait requires context")
	}
	for {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		registry.mu.Lock()
		state := registry.bindings[token]
		if registry.err != nil || registry.ctx.Err() != nil {
			err := errors.Join(errors.New("OMP owner registry ended while awaiting preflight"), registry.err)
			registry.mu.Unlock()
			return "", err
		}
		if registry.ending || state == nil || !state.admitted || state.SessionID != sessionID {
			registry.mu.Unlock()
			return "", pifamily.NewBridgeCallError("stale_owner", "OMP preflight does not own a current binding")
		}
		if err := ctx.Err(); err != nil {
			registry.mu.Unlock()
			return "", err
		}
		if len(state.preflightOrder) != 0 {
			runToken := state.preflightOrder[0]
			prompt, ok := state.preflights[runToken]
			if !ok || prompt != expectedPrompt {
				err := registry.protocolFailureLocked("OMP native preflight does not match the owned Run")
				registry.mu.Unlock()
				return "", err
			}
			registry.consumePreflightLocked(state, runToken, prompt)
			registry.mu.Unlock()
			return runToken, nil
		}
		changed := state.preflightChanged
		done := registry.ctx.Done()
		registry.mu.Unlock()
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-done:
			return "", errors.Join(errors.New("OMP owner registry ended while awaiting preflight"), registry.Err())
		case <-changed:
		}
	}
}

func (registry *OwnerRegistry) consumePreflightLocked(state *ownerRegistryState, runToken, prompt string) string {
	delete(state.preflights, runToken)
	if index := slices.Index(state.preflightOrder, runToken); index >= 0 {
		state.preflightOrder = slices.Delete(state.preflightOrder, index, index+1)
	}
	registry.releaseStateLocked(state, len(prompt))
	state.consumedPreflights = append(state.consumedPreflights, runToken)
	state.consumedPreflightSet[runToken] = struct{}{}
	if len(state.consumedPreflights) > maxOwnerTrackedItems {
		registry.releaseStateLocked(state, len(state.consumedPreflights[0]))
		delete(state.consumedPreflightSet, state.consumedPreflights[0])
		state.consumedPreflights = state.consumedPreflights[1:]
	}
	return prompt
}

func (registry *OwnerRegistry) signalPreflightLocked(state *ownerRegistryState) {
	if state.preflightChanged == nil {
		return
	}
	close(state.preflightChanged)
	state.preflightChanged = make(chan struct{})
}

func (registry *OwnerRegistry) callPublic(ctx context.Context, state *ownerRegistryState, attempt *ownerPublicAttempt, method string, params any) (json.RawMessage, error) {
	registry.mu.Lock()
	current := !registry.ending && registry.bindings[state.OwnerToken] == state && state.attempt == attempt &&
		state.admitted && state.published && !state.terminal
	registry.mu.Unlock()
	if !current {
		return nil, errors.New("OMP public caller does not own the current binding")
	}
	var result json.RawMessage
	if err := attempt.conn.Call(ctx, method, params, &result); err != nil {
		return nil, err
	}
	return result, nil
}

func (registry *OwnerRegistry) remove(state *ownerRegistryState, reason string) {
	_ = registry.removeBinding(nil, state, reason, false)
}

func (registry *OwnerRegistry) removeForSwitch(ctx context.Context, state *ownerRegistryState) error {
	return registry.removeBinding(ctx, state, "", true)
}

func (registry *OwnerRegistry) removeBinding(ctx context.Context, state *ownerRegistryState, reason string, requireHealthy bool) error {
	registry.mu.Lock()
	if requireHealthy {
		if err := registry.admissionErrorLocked(ctx); err != nil {
			registry.mu.Unlock()
			return err
		}
		if state.terminal {
			registry.mu.Unlock()
			return pifamily.NewBridgeCallError("stale_owner", "OMP replacement owner is terminal")
		}
	}
	if registry.bindings[state.OwnerToken] != state {
		registry.mu.Unlock()
		if requireHealthy {
			return pifamily.NewBridgeCallError("stale_owner", "OMP replacement does not own the current binding")
		}
		return nil
	}
	registry.signalPreflightLocked(state)
	delete(registry.bindings, state.OwnerToken)
	registry.retiredTokens[state.OwnerToken] = struct{}{}
	registry.retiredOrder = append(registry.retiredOrder, state.OwnerToken)
	if registry.primaryToken == state.OwnerToken {
		registry.primaryToken = ""
		registry.lastPrimaryReason = reason
		registry.lastPrimaryToken = state.OwnerToken
		registry.lastPrimaryID = state.SessionID
	}
	publicCancel := state.publicCancel
	publicDone := state.publicDone
	state.caller, state.admitted, state.published = nil, false, false
	registry.mu.Unlock()
	if publicCancel != nil {
		publicCancel()
	}
	if publicDone != nil {
		<-publicDone
	}
	registry.mu.Lock()
	if !registry.ending {
		registry.retainedBytes -= state.retainedBytes
		registry.retainedBytes += len(state.OwnerToken)
		if len(registry.retiredOrder) > maxOwnerTrackedItems {
			registry.retainedBytes -= len(registry.retiredOrder[0])
			delete(registry.retiredTokens, registry.retiredOrder[0])
			registry.retiredOrder = registry.retiredOrder[1:]
		}
	}
	state.retainedBytes = 0
	state.staged, state.batches, state.preflights, state.reports = nil, nil, nil, nil
	state.preflightOrder, state.preflightChanged = nil, nil
	state.completed, state.consumedPreflights = nil, nil
	state.completedSet, state.consumedPreflightSet = nil, nil
	registry.mu.Unlock()
	return nil
}

func (registry *OwnerRegistry) Close() error {
	registry.closeOnce.Do(func() {
		registry.mu.Lock()
		gracefulBridgeEOF := registry.everPrimary && registry.primaryToken == "" &&
			registry.lastPrimaryReason != "" && len(registry.bindings) == 0
		registry.ending = true
		bridge := registry.bridge
		publicCancels := make([]context.CancelFunc, 0, len(registry.bindings))
		for _, state := range registry.bindings {
			registry.signalPreflightLocked(state)
			if state.publicCancel != nil {
				publicCancels = append(publicCancels, state.publicCancel)
			}
			state.caller, state.admitted, state.published = nil, false, false
		}
		registry.bindings = make(map[string]*ownerRegistryState)
		registry.primaryToken = ""
		registry.mu.Unlock()
		registry.cancel()
		go func() {
			for _, cancel := range publicCancels {
				cancel()
			}
			if bridge != nil {
				if err := bridge.Close(); err != nil && !(gracefulBridgeEOF && errors.Is(err, pifamily.ErrBridgeClosed)) {
					registry.recordError(err)
				}
			}
			registry.work.Wait()
			registry.mu.Lock()
			registry.retiredTokens = make(map[string]struct{})
			registry.retiredOrder = nil
			registry.retainedBytes = 0
			registry.mu.Unlock()
			close(registry.closed)
		}()
	})
	<-registry.closed
	return registry.Err()
}

func (registry *OwnerRegistry) fail(err error) {
	if err == nil {
		return
	}
	registry.recordError(err)
	registry.cancel()
}

func (registry *OwnerRegistry) recordError(err error) {
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.err == nil {
		registry.err = err
	} else {
		registry.err = errors.Join(registry.err, err)
	}
}

func (registry *OwnerRegistry) protocolFailure(message string, cause error) error {
	err := errors.New(message)
	if cause != nil {
		err = fmt.Errorf("%s: %w", message, cause)
	}
	registry.fail(err)
	return pifamily.NewBridgeCallError("protocol_error", message)
}

func (registry *OwnerRegistry) protocolFailureLocked(message string) error {
	err := errors.New(message)
	if registry.err == nil {
		registry.err = err
	} else {
		registry.err = errors.Join(registry.err, err)
	}
	registry.cancel()
	return pifamily.NewBridgeCallError("protocol_error", message)
}

func (registry *OwnerRegistry) reserveStateLocked(state *ownerRegistryState, size int) error {
	if size < 0 || registry.retainedBytes > maxOwnerRetainedBytes-size {
		return registry.protocolFailureLocked("OMP owner retained-byte capacity is exhausted")
	}
	registry.retainedBytes += size
	state.retainedBytes += size
	return nil
}

func (registry *OwnerRegistry) releaseStateLocked(state *ownerRegistryState, size int) {
	if size < 0 || size > state.retainedBytes || size > registry.retainedBytes {
		panic("invalid OMP owner retained-byte accounting")
	}
	state.retainedBytes -= size
	registry.retainedBytes -= size
}

func ownerBindingRetainedBytes(binding OwnerBinding) int {
	return len(binding.OwnerToken) + len(binding.SessionID) + len(binding.Name) + len(binding.CWD) + len(binding.Scope) + len(binding.Mode)
}

func ownerPublicDeliveryRetainedBytes(delivery kit.DeliveryRequest) int {
	size := len(delivery.RunID) + len(delivery.MessageID) + len(delivery.Body) + len(delivery.From.SessionID) + len(delivery.From.Name) + len(delivery.From.Product)
	for _, group := range delivery.From.Groups {
		size += len(group)
	}
	return size
}

func (registry *OwnerRegistry) acquire(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-registry.ctx.Done():
		if err := registry.Err(); err != nil {
			return err
		}
		return context.Canceled
	case <-registry.gate:
		return nil
	}
}

func (registry *OwnerRegistry) release() { registry.gate <- struct{}{} }

func (registry *OwnerRegistry) currentBridge() *pifamily.Bridge {
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.ending {
		return nil
	}
	return registry.bridge
}

func (registry *OwnerRegistry) validClassification(topology, scope, mode string) bool {
	if topology != registry.topology {
		return false
	}
	if scope == ownerScopeChild {
		return mode == ownerModePrint
	}
	if scope != ownerScopePrimary {
		return false
	}
	return (registry.topology == ownerTopologyLane && mode == ownerModeRPC) ||
		(registry.topology == ownerTopologyInteractive && mode == ownerModeTUI)
}

func decodeOwnerJSON(raw json.RawMessage, value any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return errors.New("OMP owner JSON contains trailing data")
	}
	return nil
}

func validOwnerToken(value string) bool {
	return value != "" && len(value) <= 256 && utf8.ValidString(value) && strings.IndexFunc(value, unicode.IsControl) < 0 && !strings.ContainsRune(value, ' ')
}

func validOwnerID(value string) bool {
	return value != "" && len(value) <= 256 && utf8.ValidString(value) && strings.IndexFunc(value, unicode.IsSpace) < 0 && strings.IndexFunc(value, unicode.IsControl) < 0
}

func validOwnerMessageID(value string) bool {
	return value != "" && len(value) <= 256 && utf8.ValidString(value) && !strings.ContainsRune(value, 0)
}

func validOwnerText(value string, limit int) bool {
	return len(value) <= limit && utf8.ValidString(value) && !strings.ContainsRune(value, 0)
}

func validOwnerCWD(value string) bool {
	return filepath.IsAbs(value) && validOwnerText(value, 32<<10)
}

func validOwnerMessageIDs(values []string) bool {
	if len(values) == 0 || len(values) > maxOwnerTrackedItems {
		return false
	}
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if !validOwnerMessageID(value) {
			return false
		}
		if _, duplicate := seen[value]; duplicate {
			return false
		}
		seen[value] = struct{}{}
	}
	return true
}

func stagedPrefixEqual(messageIDs []string, staged []*ownerStagedDelivery) bool {
	for index, messageID := range messageIDs {
		if staged[index].messageID != messageID {
			return false
		}
	}
	return true
}

func ownerMessageTracked(state *ownerRegistryState, messageID string) bool {
	for _, staged := range state.staged {
		if staged.messageID == messageID {
			return true
		}
	}
	for _, batch := range state.batches {
		if slices.Contains(batch.messageIDs, messageID) {
			return true
		}
	}
	return false
}

func validDeliveryPhase(phase string) bool { return deliveryPhaseIndex(phase) != 0 }

func deliveryPhaseIndex(phase string) int {
	switch phase {
	case "claimed":
		return 1
	case "message_start":
		return 2
	case "message_end":
		return 3
	case "context":
		return 4
	default:
		return 0
	}
}
