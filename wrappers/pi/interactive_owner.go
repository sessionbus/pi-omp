// SPDX-License-Identifier: MIT

package pi

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
	"unicode/utf8"

	kit "github.com/antst/sessionbus/bus/sdk/go"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
	"github.com/sessionbus/peer-common/host"
	"github.com/sessionbus/pi-omp/wrappers/pifamily"
)

const (
	interactiveTopology      = "interactive"
	maxInteractiveQueueItems = 256
	maxInteractiveQueueBytes = 8 << 20
	maxInteractiveTextBytes  = 1 << 20
)

var (
	piInteractivePublicDial        = (&net.Dialer{}).DialContext
	piInteractiveReconnectInterval = 2 * time.Second
)

var errInteractiveDeliveryCanceled = errors.New("Pi delivery canceled before native submission")

type interactiveReadyRequest struct {
	Topology  string `json:"topology"`
	Directory string `json:"directory"`
	SessionID string `json:"session_id"`
	Name      string `json:"name"`
}

type interactiveEndRequest struct {
	Topology  string `json:"topology"`
	SessionID string `json:"session_id"`
	Reason    string `json:"reason"`
}

type interactiveToolRequest struct {
	SessionID string          `json:"session_id"`
	CallID    string          `json:"call_id"`
	Action    string          `json:"action"`
	Arguments json.RawMessage `json:"arguments"`
}

type interactiveDrainRequest struct {
	SessionID string `json:"session_id"`
	Witness   string `json:"witness"`
}

type interactiveBeforeTreeRequest struct {
	SessionID string `json:"session_id"`
}

type interactiveDescribeResult struct {
	SessionID string `json:"session_id"`
	Name      string `json:"name"`
	CWD       string `json:"cwd"`
}

type interactiveAppendRequest struct {
	SessionID string `json:"session_id"`
	MessageID string `json:"message_id"`
	Body      string `json:"body"`
}

type interactiveAppendResult struct {
	SessionID string `json:"session_id"`
	MessageID string `json:"message_id"`
	Accepted  bool   `json:"accepted"`
	EntryID   string `json:"entry_id,omitempty"`
	Reason    string `json:"reason,omitempty"`
}

type interactiveQueuedDelivery struct {
	messageID string
	body      string
	bytes     int
}

type interactivePublicLifetime struct {
	ctx       context.Context
	cancel    context.CancelFunc
	sessionID string
	ready     chan struct{}
	done      chan struct{}
	wake      chan struct{}
	readyOnce sync.Once
}

// interactiveOwner binds the current native Pi session to exactly one public
// Peer identity. Pi's session_shutdown/start ordering retires an old public
// connection before a replacement session is admitted.
type interactiveOwner struct {
	ctx                            context.Context
	cancel                         context.CancelFunc
	bridge                         *pifamily.Bridge
	socket, directory, initialName string
	groups                         []string

	mu                          sync.Mutex
	conn, connecting            *kit.Connection
	public                      *interactivePublicLifetime
	sessionID, name, cwd        string
	generation, identityVersion uint64
	everReady, ending           bool
	lastEndReason               string
	queue                       []interactiveQueuedDelivery
	queueBytes                  int
	treeBusy                    bool
	err                         error
	readySignal                 chan struct{}
	readyOnce                   sync.Once

	gate  chan struct{}
	slots chan struct{}
	work  sync.WaitGroup
}

func newInteractiveOwner(ctx context.Context, socket, directory, initialName string, groups []string) (*interactiveOwner, error) {
	if ctx == nil {
		return nil, errors.New("Pi interactive owner requires context")
	}
	if !filepath.IsAbs(socket) || !filepath.IsAbs(directory) {
		return nil, errors.New("Pi interactive owner paths must be absolute")
	}
	lifetime, cancel := context.WithCancel(ctx)
	o := &interactiveOwner{
		ctx: lifetime, cancel: cancel, socket: socket, directory: directory,
		initialName: initialName, groups: slices.Clone(groups),
		readySignal: make(chan struct{}), gate: make(chan struct{}, 1), slots: make(chan struct{}, 256),
	}
	o.gate <- struct{}{}
	return o, nil
}

func (o *interactiveOwner) assignBridge(bridge *pifamily.Bridge) error {
	if bridge == nil {
		return errors.New("Pi interactive bridge is missing")
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.bridge != nil || o.ending {
		return errors.New("Pi interactive bridge is already assigned")
	}
	o.bridge = bridge
	return nil
}

func (o *interactiveOwner) Done() <-chan struct{}  { return o.ctx.Done() }
func (o *interactiveOwner) Ready() <-chan struct{} { return o.readySignal }

func (o *interactiveOwner) Err() error {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.err
}

func (o *interactiveOwner) fail(err error) {
	if err == nil {
		return
	}
	o.mu.Lock()
	o.addErrorLocked(err)
	ending := o.ending
	o.mu.Unlock()
	if !ending {
		o.cancel()
	}
}

func (o *interactiveOwner) addErrorLocked(err error) {
	if o.err == nil {
		o.err = err
	} else {
		o.err = errors.Join(o.err, err)
	}
}

func (o *interactiveOwner) Close() error {
	o.mu.Lock()
	o.ending = true
	public := o.public
	conn, connecting := o.conn, o.connecting
	o.public = nil
	o.conn, o.connecting = nil, nil
	o.sessionID, o.name, o.cwd = "", "", ""
	o.queue, o.queueBytes, o.treeBusy = nil, 0, false
	o.mu.Unlock()
	o.cancel()
	if public != nil {
		public.cancel()
	}
	if conn != nil {
		_ = conn.Close()
	}
	if connecting != nil && connecting != conn {
		_ = connecting.Close()
	}
	o.work.Wait()
	return o.Err()
}

func (o *interactiveOwner) handleBridge(ctx context.Context, method string, raw json.RawMessage) (json.RawMessage, error) {
	var result any
	var err error
	switch method {
	case "owner.ready":
		result, err = o.ready(ctx, raw)
	case "session_end":
		result, err = o.sessionEnd(ctx, raw)
	case "owner.drain":
		result, err = o.drain(ctx, raw)
	case "owner.before_tree":
		result, err = o.beforeTree(ctx, raw)
	case "tool.call":
		result, err = o.toolCall(ctx, raw)
	default:
		return nil, pifamily.NewBridgeCallError("method_not_found", "Pi interactive bridge method is unavailable")
	}
	if err != nil {
		return nil, err
	}
	body, err := json.Marshal(result)
	if err != nil {
		return nil, err
	}
	return body, nil
}

func (o *interactiveOwner) beforeTree(ctx context.Context, raw json.RawMessage) (any, error) {
	var request interactiveBeforeTreeRequest
	if err := decodeInteractiveParams(raw, &request); err != nil || !validEntryID(request.SessionID) {
		return nil, o.protocolFailure("invalid Pi owner.before_tree", err)
	}
	if err := o.acquire(ctx); err != nil {
		return nil, err
	}
	defer o.release()
	o.mu.Lock()
	if o.ending || o.sessionID != request.SessionID || o.public == nil {
		o.mu.Unlock()
		return nil, o.protocolFailure("Pi owner.before_tree does not own the current session", nil)
	}
	o.treeBusy = true
	pending := len(o.queue) != 0
	o.mu.Unlock()
	return struct {
		SessionID string `json:"session_id"`
		Pending   bool   `json:"pending"`
	}{request.SessionID, pending}, nil
}

func (o *interactiveOwner) ready(ctx context.Context, raw json.RawMessage) (any, error) {
	var request interactiveReadyRequest
	if err := decodeInteractiveParams(raw, &request); err != nil || request.Topology != interactiveTopology ||
		request.Directory != o.directory || !validEntryID(request.SessionID) || !validInteractiveText(request.Name, 4096) {
		return nil, o.protocolFailure("invalid Pi owner.ready", err)
	}
	bridge := o.currentBridge()
	if bridge == nil {
		return nil, o.protocolFailure("Pi bridge is unavailable", nil)
	}
	var native interactiveDescribeResult
	if err := bridge.Call(ctx, "native.describe", map[string]string{"session_id": request.SessionID}, &native); err != nil {
		return nil, o.protocolFailure("Pi native description failed", err)
	}
	// owner.ready is an event snapshot. A native rename can complete while the
	// nested describe call is in flight, so describe is the current metadata
	// authority once its session identity still matches this ready event.
	if native.SessionID != request.SessionID || !validInteractiveText(native.Name, 4096) || !validInteractiveCWD(native.CWD) {
		return nil, o.protocolFailure("Pi native description contradicted owner.ready", nil)
	}
	if err := o.acquire(ctx); err != nil {
		return nil, err
	}
	defer o.release()
	if err := o.publish(ctx, request.SessionID, native.Name, native.CWD); err != nil {
		o.mu.Lock()
		terminating := o.ending || o.ctx.Err() != nil
		o.mu.Unlock()
		if terminating {
			return nil, pifamily.NewBridgeCallError("unavailable", "Pi owner is ending")
		}
		return nil, o.protocolFailure("Pi public identity failed", err)
	}
	return map[string]string{"session_id": request.SessionID}, nil
}

func (o *interactiveOwner) sessionEnd(ctx context.Context, raw json.RawMessage) (any, error) {
	var request interactiveEndRequest
	if err := decodeInteractiveParams(raw, &request); err != nil || request.Topology != interactiveTopology ||
		!validEntryID(request.SessionID) || !slices.Contains([]string{"quit", "reload", "new", "resume", "fork"}, request.Reason) {
		return nil, o.protocolFailure("invalid Pi session_end", err)
	}
	if err := o.acquire(ctx); err != nil {
		return nil, err
	}
	defer o.release()
	o.mu.Lock()
	if o.ending || o.sessionID != request.SessionID || o.public == nil {
		o.mu.Unlock()
		return nil, o.protocolFailure("Pi session_end does not own the current session", nil)
	}
	public, conn, connecting := o.public, o.conn, o.connecting
	o.public = nil
	o.conn = nil
	o.connecting = nil
	o.sessionID, o.name, o.cwd = "", "", ""
	o.lastEndReason = request.Reason
	o.queue, o.queueBytes, o.treeBusy = nil, 0, false
	o.mu.Unlock()
	public.cancel()
	if conn != nil {
		_ = conn.Close()
	}
	if connecting != nil && connecting != conn {
		_ = connecting.Close()
	}
	<-public.done
	return map[string]string{"session_id": request.SessionID}, nil
}

func (o *interactiveOwner) toolCall(ctx context.Context, raw json.RawMessage) (any, error) {
	var request interactiveToolRequest
	if err := decodeInteractiveParams(raw, &request); err != nil || !validEntryID(request.SessionID) ||
		!validEntryID(request.CallID) || request.Action == "" || request.Arguments == nil || !json.Valid(request.Arguments) {
		return nil, o.protocolFailure("invalid Pi tool.call", err)
	}
	o.mu.Lock()
	public := o.public
	current := !o.ending && o.sessionID == request.SessionID && public != nil
	o.mu.Unlock()
	if !current {
		return nil, pifamily.NewBridgeCallError("stale_session", "Pi tool call does not belong to the current session")
	}
	caller := kit.NewCaller(func(callCtx context.Context, method string, params any) (json.RawMessage, error) {
		return o.callPublic(callCtx, request.SessionID, public, method, params)
	})
	result, err := caller.Action(ctx, request.Action, request.Arguments)
	if err != nil {
		return nil, pifamily.NewBridgeCallError("tool_error", err.Error())
	}
	o.mu.Lock()
	current = !o.ending && o.sessionID == request.SessionID && o.public == public
	o.mu.Unlock()
	if !current {
		return nil, pifamily.NewBridgeCallError("stale_session", "Pi tool call crossed a native owner generation")
	}
	return struct {
		SessionID string          `json:"session_id"`
		CallID    string          `json:"call_id"`
		Result    json.RawMessage `json:"result"`
	}{request.SessionID, request.CallID, result}, nil
}

func (o *interactiveOwner) drain(ctx context.Context, raw json.RawMessage) (any, error) {
	var request interactiveDrainRequest
	if err := decodeInteractiveParams(raw, &request); err != nil || !validEntryID(request.SessionID) ||
		!slices.Contains([]string{"agent_settled", "before_agent_start", "session_before_tree_cancelled", "session_compact", "session_compact_failed", "session_tree"}, request.Witness) {
		return nil, o.protocolFailure("invalid Pi owner.drain", err)
	}
	if err := o.acquire(ctx); err != nil {
		return nil, err
	}
	defer o.release()
	if request.Witness == "session_before_tree_cancelled" || request.Witness == "session_tree" {
		o.mu.Lock()
		o.treeBusy = false
		o.mu.Unlock()
	}
	drained := 0
	for {
		o.mu.Lock()
		if o.ending || o.sessionID != request.SessionID || o.public == nil {
			o.mu.Unlock()
			return nil, o.protocolFailure("Pi drain does not belong to the current session", nil)
		}
		if len(o.queue) == 0 {
			o.mu.Unlock()
			break
		}
		delivery := o.queue[0]
		o.mu.Unlock()
		accepted, reason, err := o.appendNative(ctx, request.SessionID, delivery.messageID, delivery.body)
		if err != nil {
			return nil, o.protocolFailure("Pi queued delivery failed", err)
		}
		if !accepted {
			if reason != "busy" && reason != "branch_summary_busy" {
				return nil, o.protocolFailure("Pi queued delivery rejection is invalid", nil)
			}
			break
		}
		o.mu.Lock()
		if len(o.queue) == 0 || o.queue[0].messageID != delivery.messageID || o.sessionID != request.SessionID {
			o.mu.Unlock()
			return nil, o.protocolFailure("Pi delivery queue changed during drain", nil)
		}
		o.queue = o.queue[1:]
		o.queueBytes -= delivery.bytes
		o.mu.Unlock()
		drained++
	}
	return struct {
		SessionID string `json:"session_id"`
		Drained   int    `json:"drained"`
	}{request.SessionID, drained}, nil
}

func (o *interactiveOwner) publish(ctx context.Context, sessionID, nativeName, cwd string) error {
	o.mu.Lock()
	if o.ending {
		o.mu.Unlock()
		return context.Canceled
	}
	conn, currentID := o.conn, o.sessionID
	if currentID != "" && currentID != sessionID {
		o.mu.Unlock()
		return errors.New("Pi owner.ready replaced a session without session_end")
	}
	name := nativeName
	// Every native replacement may begin without a session title. Retain the
	// explicit wrapper name as a stable public fallback until Pi publishes a
	// native title for that session.
	if name == "" {
		name = o.initialName
	}
	identity := kit.PeerIdentity{Protocol: 1, Product: Product, SessionID: sessionID, Name: name, Groups: slices.Clone(o.groups), Info: map[string]any{"cwd": cwd}}
	if currentID == "" {
		o.mu.Unlock()
		return o.connectPublic(ctx, identity)
	}
	public := o.public
	if public == nil {
		o.mu.Unlock()
		return errors.New("Pi public lifetime is unavailable")
	}
	o.name, o.cwd = name, cwd
	o.identityVersion++
	version := o.identityVersion
	if conn == nil {
		select {
		case public.wake <- struct{}{}:
		default:
		}
		o.mu.Unlock()
		return nil
	}
	generation := o.generation
	o.mu.Unlock()
	var response json.RawMessage
	err := conn.CallObserved(ctx, "session.hello", identity, &response, func() error {
		o.mu.Lock()
		defer o.mu.Unlock()
		if o.ending || o.public != public || o.conn != conn || o.generation != generation || o.sessionID != sessionID || o.identityVersion != version {
			return errors.New("Pi public identity changed before rehello")
		}
		o.everReady, o.lastEndReason = true, ""
		return nil
	})
	if err != nil {
		select {
		case <-conn.Done():
			return nil
		default:
		}
	}
	return err
}

func (o *interactiveOwner) connectPublic(ctx context.Context, identity kit.PeerIdentity) error {
	o.mu.Lock()
	if o.ending || o.public != nil || o.sessionID != "" || o.conn != nil || o.connecting != nil {
		o.mu.Unlock()
		return errors.New("Pi public owner changed before reconnect lifetime admission")
	}
	publicCtx, cancel := context.WithCancel(o.ctx)
	public := &interactivePublicLifetime{ctx: publicCtx, cancel: cancel, sessionID: identity.SessionID, ready: make(chan struct{}), done: make(chan struct{}), wake: make(chan struct{}, 1)}
	o.public = public
	o.sessionID, o.name, o.cwd = identity.SessionID, identity.Name, identity.Info["cwd"].(string)
	o.identityVersion++
	o.work.Add(1)
	o.mu.Unlock()
	go o.runPublic(public)
	select {
	case <-public.ready:
		return nil
	case <-public.done:
		if err := o.Err(); err != nil {
			return err
		}
		return errors.New("Pi public reconnect lifetime ended before admission")
	case <-ctx.Done():
		return ctx.Err()
	case <-o.ctx.Done():
		if err := o.Err(); err != nil {
			return err
		}
		return context.Canceled
	}
}

func (o *interactiveOwner) runPublic(public *interactivePublicLifetime) {
	defer o.work.Done()
	defer close(public.done)
	for {
		fd, err := piInteractivePublicDial(public.ctx, "unix", o.socket)
		if err != nil {
			if !o.waitPublicRetry(public) {
				return
			}
			continue
		}
		assigned := make(chan struct{})
		var conn *kit.Connection
		o.mu.Lock()
		if o.ending || o.public != public || o.sessionID != public.sessionID {
			o.mu.Unlock()
			_ = fd.Close()
			return
		}
		o.generation++
		generation := o.generation
		conn = kit.NewConnection(fd, func(callCtx context.Context, request *kit.Request) {
			<-assigned
			o.handlePublic(callCtx, conn, generation, request)
		})
		o.connecting = conn
		o.mu.Unlock()
		close(assigned)

		err = o.admitPublic(public, conn, generation)
		transportEnded := false
		select {
		case <-conn.Done():
			transportEnded = true
		default:
		}
		o.mu.Lock()
		if o.conn == conn {
			o.conn = nil
		}
		if o.connecting == conn {
			o.connecting = nil
		}
		o.mu.Unlock()
		_ = conn.Close()
		if public.ctx.Err() != nil {
			return
		}
		if err != nil && !transportEnded {
			o.fail(fmt.Errorf("Pi public identity failed: %w", err))
			return
		}
		if !o.waitPublicRetry(public) {
			return
		}
	}
}

func (o *interactiveOwner) admitPublic(public *interactivePublicLifetime, conn *kit.Connection, generation uint64) error {
	for {
		o.mu.Lock()
		if o.ending || o.public != public || o.sessionID != public.sessionID || o.connecting != conn || o.generation != generation {
			o.mu.Unlock()
			return context.Canceled
		}
		identity := kit.PeerIdentity{Protocol: 1, Product: Product, SessionID: o.sessionID, Name: o.name, Groups: slices.Clone(o.groups), Info: map[string]any{"cwd": o.cwd}}
		version := o.identityVersion
		o.mu.Unlock()
		installed := false
		var response json.RawMessage
		err := conn.CallObserved(public.ctx, "session.hello", identity, &response, func() error {
			o.mu.Lock()
			defer o.mu.Unlock()
			if o.ending || o.public != public || o.sessionID != public.sessionID || o.connecting != conn || o.generation != generation {
				return context.Canceled
			}
			if o.identityVersion != version {
				return nil
			}
			o.conn, o.connecting = conn, nil
			o.everReady, o.lastEndReason = true, ""
			installed = true
			o.readyOnce.Do(func() { close(o.readySignal) })
			public.readyOnce.Do(func() { close(public.ready) })
			return nil
		})
		if err != nil {
			return err
		}
		if installed {
			select {
			case <-conn.Done():
				return nil
			case <-public.ctx.Done():
				return public.ctx.Err()
			}
		}
	}
}

func (o *interactiveOwner) waitPublicRetry(public *interactivePublicLifetime) bool {
	o.mu.Lock()
	current := !o.ending && o.public == public && o.sessionID == public.sessionID
	o.mu.Unlock()
	if !current {
		return false
	}
	timer := time.NewTimer(piInteractiveReconnectInterval)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-public.wake:
		return true
	case <-public.ctx.Done():
		return false
	}
}

func (o *interactiveOwner) handlePublic(ctx context.Context, conn *kit.Connection, generation uint64, request *kit.Request) {
	o.mu.Lock()
	if o.ending || o.conn != conn || o.generation != generation {
		o.mu.Unlock()
		_ = conn.Close()
		return
	}
	superseded := request.Method == "session.superseded"
	if superseded {
		// Supersession owns the admitted public identity as soon as the exact
		// request is classified. Keep the old wire alive long enough to return
		// its acknowledgement, but exclude reconnect, rehello, SessionEnd, and
		// replacement admission while that acknowledgement is in flight.
		o.addErrorLocked(errors.New("Pi public session was superseded"))
		o.ending = true
	}
	select {
	case o.slots <- struct{}{}:
	default:
		o.mu.Unlock()
		_ = conn.Close()
		if superseded {
			o.cancel()
		} else {
			o.fail(errors.New("Pi public request capacity exhausted"))
		}
		return
	}
	o.work.Add(1)
	o.mu.Unlock()
	go func() {
		defer o.work.Done()
		defer func() { <-o.slots }()
		var responseErr error
		switch request.Method {
		case "session.superseded":
			responseErr = conn.Result(request, struct{}{})
			if responseErr != nil {
				o.mu.Lock()
				o.addErrorLocked(responseErr)
				o.mu.Unlock()
				_ = conn.Close()
			}
			o.cancel()
			return
		case "message.deliver":
			delivery, ok := request.Params.(*kit.DeliveryRequest)
			if !ok {
				responseErr = errors.New("invalid Pi delivery parameters")
				break
			}
			receipt, err := o.deliver(ctx, generation, *delivery)
			if err != nil {
				responseErr = conn.Error(request, -32603, "Pi native delivery failed; no replay")
				if !errors.Is(err, errInteractiveDeliveryCanceled) {
					o.fail(err)
				}
			} else {
				responseErr = conn.Result(request, receipt)
			}
		default:
			responseErr = conn.Error(request, -32601, nil)
		}
		if responseErr != nil {
			_ = conn.Close()
			// A request canceled before queue admission or a native call has no
			// uncertain side effect. The reconnect lifetime owns connection loss;
			// it need not turn that canceled request into a second owner failure.
			if ctx.Err() == nil {
				o.fail(responseErr)
			}
		}
	}()
}

func (o *interactiveOwner) deliver(ctx context.Context, generation uint64, request kit.DeliveryRequest) (kit.DeliveryReceipt, error) {
	body, err := host.RenderNativeMessage(request)
	if err != nil {
		return kit.DeliveryReceipt{}, err
	}
	if err = validateInteractiveAppend(request.MessageID, body); err != nil {
		return kit.DeliveryReceipt{}, err
	}
	if err = o.acquire(ctx); err != nil {
		if ctx.Err() != nil {
			return kit.DeliveryReceipt{}, fmt.Errorf("%w: %v", errInteractiveDeliveryCanceled, ctx.Err())
		}
		return kit.DeliveryReceipt{}, err
	}
	defer o.release()
	if err = ctx.Err(); err != nil {
		return kit.DeliveryReceipt{}, fmt.Errorf("%w: %v", errInteractiveDeliveryCanceled, err)
	}
	o.mu.Lock()
	if o.ending || o.generation != generation || o.conn == nil || o.sessionID == "" {
		o.mu.Unlock()
		return kit.DeliveryReceipt{}, errors.New("Pi delivery does not belong to the current public session")
	}
	sessionID := o.sessionID
	queued := len(o.queue) != 0
	treeBusy := o.treeBusy
	o.mu.Unlock()
	if treeBusy && queued {
		return kit.DeliveryReceipt{Disposition: "rejected", Reason: "native_branch_summary_busy"}, nil
	}
	if queued {
		if err = o.queueDelivery(ctx, generation, sessionID, request.MessageID, body); err != nil {
			return kit.DeliveryReceipt{}, err
		}
		return kit.DeliveryReceipt{Disposition: "queued_for_next_turn"}, nil
	}
	accepted, reason, err := o.appendNative(ctx, sessionID, request.MessageID, body)
	if err != nil {
		return kit.DeliveryReceipt{}, err
	}
	if accepted {
		if treeBusy {
			o.mu.Lock()
			if o.sessionID == sessionID {
				o.treeBusy = false
			}
			o.mu.Unlock()
		}
		return kit.DeliveryReceipt{Disposition: "written"}, nil
	}
	if reason == "branch_summary_busy" {
		return kit.DeliveryReceipt{Disposition: "rejected", Reason: "native_branch_summary_busy"}, nil
	}
	if reason != "busy" {
		return kit.DeliveryReceipt{}, errors.New("Pi native append rejection is invalid")
	}
	if err = o.queueDelivery(ctx, generation, sessionID, request.MessageID, body); err != nil {
		return kit.DeliveryReceipt{}, err
	}
	return kit.DeliveryReceipt{Disposition: "queued_for_next_turn"}, nil
}

func (o *interactiveOwner) queueDelivery(ctx context.Context, generation uint64, sessionID, messageID, body string) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("%w: %v", errInteractiveDeliveryCanceled, err)
	}
	item := interactiveQueuedDelivery{messageID: messageID, body: body, bytes: len(messageID) + len(body)}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.ending || o.generation != generation || o.sessionID != sessionID || o.conn == nil {
		return errors.New("Pi session ended before delivery could be queued")
	}
	if len(o.queue) >= maxInteractiveQueueItems || item.bytes > maxInteractiveQueueBytes-o.queueBytes {
		return errors.New("Pi delivery queue is full")
	}
	o.queue = append(o.queue, item)
	o.queueBytes += item.bytes
	return nil
}

func (o *interactiveOwner) appendNative(ctx context.Context, sessionID, messageID, body string) (bool, string, error) {
	if !validEntryID(sessionID) {
		return false, "", errors.New("invalid Pi native append")
	}
	if err := validateInteractiveAppend(messageID, body); err != nil {
		return false, "", err
	}
	bridge := o.currentBridge()
	if bridge == nil {
		return false, "", errors.New("Pi bridge is unavailable")
	}
	var result interactiveAppendResult
	if err := bridge.Call(ctx, "native.append", interactiveAppendRequest{sessionID, messageID, body}, &result); err != nil {
		return false, "", err
	}
	if result.SessionID != sessionID || result.MessageID != messageID {
		return false, "", errors.New("Pi native append response changed identity")
	}
	if result.Accepted {
		if result.EntryID != "" || result.Reason != "" {
			return false, "", errors.New("Pi native append acknowledgement is invalid")
		}
		return true, "", nil
	}
	if (result.Reason != "busy" && result.Reason != "branch_summary_busy") || result.EntryID != "" {
		return false, "", errors.New("Pi native append rejection is invalid")
	}
	return false, result.Reason, nil
}

func (o *interactiveOwner) callPublic(ctx context.Context, sessionID string, public *interactivePublicLifetime, method string, params any) (json.RawMessage, error) {
	o.mu.Lock()
	current := !o.ending && public != nil && o.public == public && o.sessionID == sessionID
	conn := o.conn
	o.mu.Unlock()
	if !current {
		return nil, errors.New("Pi public caller does not belong to the current session")
	}
	if conn == nil {
		return nil, &kit.ProtocolError{Code: protocol.NotConnected, Message: "not_connected"}
	}
	var result json.RawMessage
	if err := conn.Call(ctx, method, params, &result); err != nil {
		return nil, err
	}
	return result, nil
}

func (o *interactiveOwner) acquire(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-o.ctx.Done():
		if err := o.Err(); err != nil {
			return err
		}
		return context.Canceled
	case <-o.gate:
		return nil
	}
}

func (o *interactiveOwner) release() { o.gate <- struct{}{} }

func (o *interactiveOwner) currentBridge() *pifamily.Bridge {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.bridge
}

func (o *interactiveOwner) gracefulNativeEnd() bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.everReady && o.sessionID == "" && o.conn == nil && o.lastEndReason == "quit"
}

func (o *interactiveOwner) protocolFailure(message string, cause error) error {
	err := errors.New(message)
	if cause != nil {
		err = fmt.Errorf("%s: %w", message, cause)
	}
	o.fail(err)
	return pifamily.NewBridgeCallError("protocol_error", message)
}

func decodeInteractiveParams(raw json.RawMessage, result any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(result); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return errors.New("Pi bridge parameters contain trailing JSON")
	}
	return nil
}

func validInteractiveText(value string, limit int) bool {
	return utf8.ValidString(value) && len(value) <= limit && !strings.ContainsRune(value, 0)
}

func validOpaqueMessageID(value string) bool {
	return value != "" && utf8.ValidString(value) && len(value) <= 256
}

func validateInteractiveAppend(messageID, body string) error {
	if !validOpaqueMessageID(messageID) || !validInteractiveText(body, maxInteractiveTextBytes) {
		return errors.New("invalid Pi native append")
	}
	params, err := json.Marshal(interactiveAppendRequest{SessionID: "x", MessageID: messageID, Body: body})
	// Reserve room for the maximum native session ID and the bridge request
	// envelope/sequence. This check runs before the current session is read so a
	// queued item can never exceed the later native.append frame.
	if err != nil || len(params) > pifamily.DefaultBridgeLimits.MaxFrameBytes-1024 {
		return errors.New("Pi native append exceeds the private bridge frame")
	}
	return nil
}

func validInteractiveCWD(value string) bool {
	return filepath.IsAbs(value) && validInteractiveText(value, 32<<10)
}
