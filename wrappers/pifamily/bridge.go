// SPDX-License-Identifier: MIT

// Package pifamily contains transport shared only by the Pi-family wrappers.
package pifamily

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"unicode/utf8"
)

const (
	BridgeProtocolVersion = 1
	maxBridgeRequestID    = uint64(1<<53 - 1)
)

var (
	ErrBridgeBusy     = errors.New("Pi-family bridge is busy")
	ErrBridgeClosed   = errors.New("Pi-family bridge is closed")
	ErrBridgeProtocol = errors.New("Pi-family bridge protocol violation")
)

type BridgeRole string

const (
	BridgeHost   BridgeRole = "host"
	BridgeNative BridgeRole = "native"
)

type BridgeLimits struct {
	MaxFrameBytes    int
	MaxPendingCalls  int
	MaxActiveCalls   int
	MaxPendingWrites int
	MaxRetainedBytes int
}

var DefaultBridgeLimits = BridgeLimits{
	MaxFrameBytes:    1 << 20,
	MaxPendingCalls:  256,
	MaxActiveCalls:   256,
	MaxPendingWrites: 256,
	MaxRetainedBytes: 32 << 20,
}

func (limits BridgeLimits) normalized() (BridgeLimits, error) {
	if limits == (BridgeLimits{}) {
		limits = DefaultBridgeLimits
	}
	if limits.MaxFrameBytes < 256 || limits.MaxPendingCalls < 1 || limits.MaxActiveCalls < 1 ||
		limits.MaxPendingWrites < 1 || limits.MaxRetainedBytes < limits.MaxFrameBytes ||
		limits.MaxFrameBytes > DefaultBridgeLimits.MaxFrameBytes ||
		limits.MaxPendingCalls > DefaultBridgeLimits.MaxPendingCalls ||
		limits.MaxActiveCalls > DefaultBridgeLimits.MaxActiveCalls ||
		limits.MaxPendingWrites > DefaultBridgeLimits.MaxPendingWrites ||
		limits.MaxRetainedBytes > DefaultBridgeLimits.MaxRetainedBytes {
		return BridgeLimits{}, errors.New("Pi-family bridge limits are invalid")
	}
	return limits, nil
}

// BridgeCallError is an error returned by the other bridge endpoint. Handlers
// may return one to preserve a stable code; other errors use "handler_error".
type BridgeCallError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (e *BridgeCallError) Error() string {
	if e == nil {
		return ""
	}
	return e.Message
}

func NewBridgeCallError(code, message string) *BridgeCallError {
	return &BridgeCallError{Code: code, Message: message}
}

// BridgeHandler validates product methods and payloads above this transport.
// A nil result is encoded as JSON null.
type BridgeHandler func(context.Context, string, json.RawMessage) (json.RawMessage, error)

type bridgePending struct {
	result    chan bridgeCallResult
	abandoned bool
}

type bridgeCallResult struct {
	result json.RawMessage
	err    error
	bytes  int
}

type bridgeInbound struct {
	cancel    context.CancelFunc
	cancelled bool
	bytes     int
}

type bridgeWrite struct {
	body []byte
	done chan struct{}
	err  error
}

func newBridgeWrite(body []byte) *bridgeWrite {
	return &bridgeWrite{body: body, done: make(chan struct{})}
}

func (write *bridgeWrite) complete(err error) {
	write.err = err
	close(write.done)
}

type BridgeStats struct {
	PendingCalls  int
	ActiveCalls   int
	PendingWrites int
	RetainedBytes int
}

// Bridge owns one full-duplex connection. Its reader never awaits a handler,
// allowing a handler to call back through the same connection without a
// deadlock. Close cancels and joins the reader, writer, and all handlers.
type Bridge struct {
	conn     io.ReadWriteCloser
	role     BridgeRole
	peerRole BridgeRole
	handler  BridgeHandler
	limits   BridgeLimits

	ctx    context.Context
	cancel context.CancelFunc

	mu             sync.Mutex
	outboundMu     sync.Mutex
	stopped        bool
	ready          bool
	intentional    bool
	failure        error
	nextOutboundID uint64
	nextInboundID  uint64
	pending        map[string]*bridgePending
	inbound        map[string]*bridgeInbound
	retainedBytes  int
	pendingWrites  int
	pendingBytes   int

	writes     chan *bridgeWrite
	peerReady  chan struct{}
	readyOnce  sync.Once
	helloWrite *bridgeWrite
	readerDone chan struct{}
	writerDone chan struct{}
	done       chan struct{}
	handlers   sync.WaitGroup
	closeOnce  sync.Once
}

// NewBridge starts a bridge over an already-owned connection. Both endpoints
// send a versioned hello. Call Ready before Call or relying on incoming work.
func NewBridge(conn io.ReadWriteCloser, role BridgeRole, handler BridgeHandler, limits BridgeLimits) (*Bridge, error) {
	if conn == nil {
		return nil, errors.New("Pi-family bridge connection is nil")
	}
	limits, err := limits.normalized()
	if err != nil {
		return nil, err
	}
	peerRole := BridgeHost
	if role == BridgeHost {
		peerRole = BridgeNative
	} else if role != BridgeNative {
		return nil, fmt.Errorf("Pi-family bridge role %q is invalid", role)
	}
	ctx, cancel := context.WithCancel(context.Background())
	bridge := &Bridge{
		conn: conn, role: role, peerRole: peerRole, handler: handler, limits: limits,
		ctx: ctx, cancel: cancel, pending: make(map[string]*bridgePending),
		inbound: make(map[string]*bridgeInbound), writes: make(chan *bridgeWrite, limits.MaxPendingWrites),
		peerReady: make(chan struct{}), readerDone: make(chan struct{}), writerDone: make(chan struct{}), done: make(chan struct{}),
	}
	hello, err := bridge.encode(map[string]any{
		"version": BridgeProtocolVersion,
		"type":    "hello",
		"role":    role,
	})
	if err != nil {
		cancel()
		_ = conn.Close()
		return nil, err
	}
	bridge.helloWrite, err = bridge.enqueue(hello)
	if err != nil {
		cancel()
		_ = conn.Close()
		return nil, err
	}
	// Queue our hello before the reader can dispatch any peer request. This
	// makes hello the first frame on the wire even when the peer wrote eagerly.
	go bridge.writeLoop()
	go bridge.readLoop()
	go bridge.join()
	return bridge, nil
}

func (bridge *Bridge) Ready(ctx context.Context) error {
	if ctx == nil {
		return errors.New("Pi-family bridge readiness requires context")
	}
	if err := waitBridgeWrite(ctx, bridge.helloWrite); err != nil {
		if ctx.Err() != nil {
			bridge.stop(ctx.Err(), false)
		}
		return err
	}
	select {
	case <-ctx.Done():
		bridge.stop(ctx.Err(), false)
		return ctx.Err()
	case <-bridge.peerReady:
		return nil
	case <-bridge.done:
		return bridge.connectionError()
	}
}

func (bridge *Bridge) Done() <-chan struct{} { return bridge.done }

func (bridge *Bridge) Err() error {
	bridge.mu.Lock()
	defer bridge.mu.Unlock()
	return bridge.failure
}

func (bridge *Bridge) Stats() BridgeStats {
	bridge.mu.Lock()
	defer bridge.mu.Unlock()
	return BridgeStats{
		PendingCalls: len(bridge.pending), ActiveCalls: len(bridge.inbound),
		PendingWrites: bridge.pendingWrites, RetainedBytes: bridge.retainedBytes + bridge.pendingBytes,
	}
}

func (bridge *Bridge) Call(ctx context.Context, method string, params, result any) error {
	if ctx == nil {
		return errors.New("Pi-family bridge call requires context")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateBridgeMethod(method); err != nil {
		return err
	}
	paramsBody, err := json.Marshal(params)
	if err != nil {
		return fmt.Errorf("encode Pi-family bridge parameters: %w", err)
	}
	if !json.Valid(paramsBody) {
		return fmt.Errorf("%w: parameters are not JSON", ErrBridgeProtocol)
	}

	id, pending, write, err := bridge.admitCall(method, paramsBody)
	if err != nil {
		return err
	}
	if err := waitBridgeWrite(ctx, write); err != nil {
		if callResult, completed := bridge.abandonPending(id, pending); completed {
			return bridge.consumeCallResult(callResult, result)
		}
		// The request may be partly written. Retire the connection so native work
		// cannot outlive an uncertain pre-write cancellation boundary.
		if ctx.Err() != nil {
			bridge.stop(ctx.Err(), false)
			return ctx.Err()
		}
		return err
	}

	select {
	case callResult := <-pending.result:
		return bridge.consumeCallResult(callResult, result)
	case <-ctx.Done():
		if callResult, completed := bridge.abandonPending(id, pending); completed {
			return bridge.consumeCallResult(callResult, result)
		}
		cancelBody, encodeErr := bridge.encode(map[string]any{
			"version": BridgeProtocolVersion,
			"type":    "cancel",
			"id":      id,
		})
		if encodeErr != nil {
			bridge.stop(encodeErr, false)
		} else if _, enqueueErr := bridge.enqueue(cancelBody); enqueueErr != nil {
			bridge.stop(enqueueErr, false)
		}
		return ctx.Err()
	}
}

// admitCall serializes request ID assignment with write admission. A request
// ID is consumed only after its complete frame has a reserved queue slot, and
// frames enter the writer in exactly ID order.
func (bridge *Bridge) admitCall(method string, params json.RawMessage) (string, *bridgePending, *bridgeWrite, error) {
	bridge.outboundMu.Lock()
	defer bridge.outboundMu.Unlock()

	bridge.mu.Lock()
	if bridge.stopped {
		err := bridge.connectionErrorLocked()
		bridge.mu.Unlock()
		return "", nil, nil, err
	}
	if !bridge.ready {
		bridge.mu.Unlock()
		return "", nil, nil, fmt.Errorf("%w: bridge is not ready", ErrBridgeProtocol)
	}
	if len(bridge.pending) >= bridge.limits.MaxPendingCalls {
		bridge.mu.Unlock()
		return "", nil, nil, ErrBridgeBusy
	}
	if bridge.nextOutboundID >= maxBridgeRequestID {
		bridge.mu.Unlock()
		return "", nil, nil, fmt.Errorf("%w: request id space exhausted", ErrBridgeProtocol)
	}
	sequence := bridge.nextOutboundID + 1
	bridge.mu.Unlock()

	id := bridge.requestID(bridge.role, sequence)
	body, err := bridge.encodeRawRequest(id, method, params)
	if err != nil {
		return "", nil, nil, err
	}
	pending := &bridgePending{result: make(chan bridgeCallResult, 1)}
	write := newBridgeWrite(body)

	bridge.mu.Lock()
	defer bridge.mu.Unlock()
	if bridge.stopped {
		return "", nil, nil, bridge.connectionErrorLocked()
	}
	if len(bridge.pending) >= bridge.limits.MaxPendingCalls ||
		bridge.pendingWrites >= bridge.limits.MaxPendingWrites ||
		bridge.retainedBytes+bridge.pendingBytes > bridge.limits.MaxRetainedBytes-len(body) {
		return "", nil, nil, ErrBridgeBusy
	}
	bridge.nextOutboundID = sequence
	bridge.pending[id] = pending
	bridge.pendingWrites++
	bridge.pendingBytes += len(body)
	bridge.writes <- write
	return id, pending, write, nil
}

func (bridge *Bridge) Close() error {
	bridge.closeOnce.Do(func() { bridge.stop(nil, true) })
	<-bridge.done
	return bridge.Err()
}

func (bridge *Bridge) join() {
	<-bridge.readerDone
	<-bridge.writerDone
	bridge.handlers.Wait()
	close(bridge.done)
}

func (bridge *Bridge) stop(err error, intentional bool) {
	bridge.mu.Lock()
	if bridge.stopped {
		if err != nil && bridge.failure == nil && !bridge.intentional {
			bridge.failure = err
		}
		bridge.mu.Unlock()
		return
	}
	bridge.stopped = true
	bridge.intentional = intentional
	if err != nil && !intentional {
		bridge.failure = err
	}
	pending := bridge.pending
	bridge.pending = make(map[string]*bridgePending)
	inbound := bridge.inbound
	bridge.inbound = make(map[string]*bridgeInbound)
	for _, call := range inbound {
		bridge.retainedBytes -= call.bytes
	}
	bridge.mu.Unlock()

	bridge.cancel()
	_ = bridge.conn.Close()
	connectionErr := ErrBridgeClosed
	if err != nil {
		connectionErr = err
	}
	for _, call := range pending {
		if !call.abandoned {
			call.result <- bridgeCallResult{err: connectionErr}
		}
	}
	for _, call := range inbound {
		call.cancel()
	}
}

func (bridge *Bridge) connectionError() error {
	bridge.mu.Lock()
	defer bridge.mu.Unlock()
	return bridge.connectionErrorLocked()
}

func (bridge *Bridge) connectionErrorLocked() error {
	if bridge.failure != nil {
		return bridge.failure
	}
	return ErrBridgeClosed
}

func (bridge *Bridge) encode(value any) ([]byte, error) {
	body, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("encode Pi-family bridge frame: %w", err)
	}
	if len(body) == 0 || len(body) > bridge.limits.MaxFrameBytes {
		return nil, fmt.Errorf("%w: frame exceeds %d bytes", ErrBridgeProtocol, bridge.limits.MaxFrameBytes)
	}
	body = append(body, '\n')
	return body, nil
}

func (bridge *Bridge) encodeRawRequest(id, method string, params json.RawMessage) ([]byte, error) {
	return bridge.encode(struct {
		Version int             `json:"version"`
		Type    string          `json:"type"`
		ID      string          `json:"id"`
		Method  string          `json:"method"`
		Params  json.RawMessage `json:"params"`
	}{BridgeProtocolVersion, "request", id, method, params})
}

func (bridge *Bridge) enqueue(body []byte) (*bridgeWrite, error) {
	write := newBridgeWrite(body)
	bridge.mu.Lock()
	defer bridge.mu.Unlock()
	if bridge.stopped {
		return nil, bridge.connectionErrorLocked()
	}
	if bridge.pendingWrites >= bridge.limits.MaxPendingWrites ||
		bridge.retainedBytes+bridge.pendingBytes > bridge.limits.MaxRetainedBytes-len(body) {
		return nil, ErrBridgeBusy
	}
	bridge.pendingWrites++
	bridge.pendingBytes += len(body)
	// pendingWrites includes the single frame a writer may already own, so the
	// bounded channel always has space while this lock excludes stop/drain.
	bridge.writes <- write
	return write, nil
}

func (bridge *Bridge) releaseWrite(bytes int) {
	bridge.mu.Lock()
	bridge.pendingWrites--
	bridge.pendingBytes -= bytes
	bridge.mu.Unlock()
}

func (bridge *Bridge) writeLoop() {
	defer close(bridge.writerDone)
	for {
		select {
		case <-bridge.ctx.Done():
			bridge.drainWrites(bridge.connectionError())
			return
		case write := <-bridge.writes:
			err := writeAll(bridge.conn, write.body)
			bridge.releaseWrite(len(write.body))
			write.complete(err)
			if err != nil {
				bridge.stop(fmt.Errorf("Pi-family bridge write: %w", err), false)
				bridge.drainWrites(bridge.connectionError())
				return
			}
		}
	}
}

func (bridge *Bridge) drainWrites(err error) {
	for {
		select {
		case write := <-bridge.writes:
			bridge.releaseWrite(len(write.body))
			write.complete(err)
		default:
			return
		}
	}
}

func writeAll(writer io.Writer, body []byte) error {
	for len(body) > 0 {
		n, err := writer.Write(body)
		if err != nil {
			return err
		}
		if n <= 0 {
			return io.ErrShortWrite
		}
		body = body[n:]
	}
	return nil
}

func waitBridgeWrite(ctx context.Context, write *bridgeWrite) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-write.done:
		return write.err
	}
}

func (bridge *Bridge) readLoop() {
	defer close(bridge.readerDone)
	scanner := bufio.NewScanner(bridge.conn)
	initialBuffer := bridge.limits.MaxFrameBytes + 1
	if initialBuffer > 4096 {
		initialBuffer = 4096
	}
	scanner.Buffer(make([]byte, initialBuffer), bridge.limits.MaxFrameBytes+1)
	ready := false
	for scanner.Scan() {
		line := append([]byte(nil), scanner.Bytes()...)
		if len(line) == 0 || len(line) > bridge.limits.MaxFrameBytes {
			bridge.stop(fmt.Errorf("%w: invalid frame size", ErrBridgeProtocol), false)
			return
		}
		kind, object, err := decodeBridgeEnvelope(line)
		if err != nil {
			bridge.stop(err, false)
			return
		}
		if !ready {
			if kind != "hello" {
				bridge.stop(fmt.Errorf("%w: first frame is %q, want hello", ErrBridgeProtocol, kind), false)
				return
			}
			if err := bridge.acceptHello(object); err != nil {
				bridge.stop(err, false)
				return
			}
			ready = true
			bridge.readyOnce.Do(func() { close(bridge.peerReady) })
			continue
		}
		if kind == "hello" {
			bridge.stop(fmt.Errorf("%w: duplicate hello", ErrBridgeProtocol), false)
			return
		}
		switch kind {
		case "request":
			err = bridge.acceptRequest(object, line)
		case "response":
			err = bridge.acceptResponse(object, line)
		case "cancel":
			err = bridge.acceptCancel(object)
		default:
			err = fmt.Errorf("%w: unknown frame type %q", ErrBridgeProtocol, kind)
		}
		if err != nil {
			bridge.stop(err, false)
			return
		}
	}
	if bridge.ctx.Err() != nil {
		return
	}
	if err := scanner.Err(); err != nil {
		bridge.stop(fmt.Errorf("%w: read frame: %v", ErrBridgeProtocol, err), false)
	} else {
		bridge.stop(fmt.Errorf("%w: peer closed the connection", ErrBridgeClosed), false)
	}
}

func decodeBridgeEnvelope(line []byte) (string, map[string]json.RawMessage, error) {
	var object map[string]json.RawMessage
	if json.Unmarshal(line, &object) != nil || object == nil {
		return "", nil, fmt.Errorf("%w: frame is not a JSON object", ErrBridgeProtocol)
	}
	if err := requireBridgeVersion(object); err != nil {
		return "", nil, err
	}
	kind, err := bridgeString(object, "type")
	if err != nil || kind == "" {
		return "", nil, fmt.Errorf("%w: frame type is invalid", ErrBridgeProtocol)
	}
	return kind, object, nil
}

func requireBridgeVersion(object map[string]json.RawMessage) error {
	var version int
	if raw, ok := object["version"]; !ok || json.Unmarshal(raw, &version) != nil || version != BridgeProtocolVersion {
		return fmt.Errorf("%w: unsupported bridge version", ErrBridgeProtocol)
	}
	return nil
}

func bridgeString(object map[string]json.RawMessage, key string) (string, error) {
	raw, ok := object[key]
	if !ok {
		return "", fmt.Errorf("%w: frame omitted %s", ErrBridgeProtocol, key)
	}
	var value string
	if json.Unmarshal(raw, &value) != nil || !utf8.ValidString(value) {
		return "", fmt.Errorf("%w: frame %s is not a string", ErrBridgeProtocol, key)
	}
	return value, nil
}

func exactBridgeKeys(object map[string]json.RawMessage, keys ...string) error {
	if len(object) != len(keys) {
		return fmt.Errorf("%w: frame fields are invalid", ErrBridgeProtocol)
	}
	for _, key := range keys {
		if _, ok := object[key]; !ok {
			return fmt.Errorf("%w: frame omitted %s", ErrBridgeProtocol, key)
		}
	}
	return nil
}

func (bridge *Bridge) acceptHello(object map[string]json.RawMessage) error {
	if err := exactBridgeKeys(object, "version", "type", "role"); err != nil {
		return err
	}
	role, err := bridgeString(object, "role")
	if err != nil || BridgeRole(role) != bridge.peerRole {
		return fmt.Errorf("%w: peer role is invalid", ErrBridgeProtocol)
	}
	bridge.mu.Lock()
	bridge.ready = true
	bridge.mu.Unlock()
	return nil
}

func (bridge *Bridge) acceptRequest(object map[string]json.RawMessage, line []byte) error {
	if err := exactBridgeKeys(object, "version", "type", "id", "method", "params"); err != nil {
		return err
	}
	id, err := bridgeString(object, "id")
	if err != nil {
		return err
	}
	sequence, err := parseBridgeRequestID(id, bridge.peerRole)
	if err != nil {
		return err
	}
	method, err := bridgeString(object, "method")
	if err != nil {
		return err
	}
	if err := validateBridgeMethod(method); err != nil {
		return err
	}
	params := object["params"]
	if !json.Valid(params) {
		return fmt.Errorf("%w: request parameters are invalid", ErrBridgeProtocol)
	}

	bridge.mu.Lock()
	if sequence != bridge.nextInboundID+1 {
		bridge.mu.Unlock()
		return fmt.Errorf("%w: duplicate or out-of-order request id %q", ErrBridgeProtocol, id)
	}
	bridge.nextInboundID = sequence
	if len(bridge.inbound) >= bridge.limits.MaxActiveCalls ||
		bridge.retainedBytes+bridge.pendingBytes > bridge.limits.MaxRetainedBytes-len(line) {
		bridge.mu.Unlock()
		return bridge.sendErrorResponse(id, NewBridgeCallError("busy", "Pi-family bridge handler capacity is exhausted"), false)
	}
	ctx, cancel := context.WithCancel(bridge.ctx)
	inbound := &bridgeInbound{cancel: cancel, bytes: len(line)}
	bridge.inbound[id] = inbound
	bridge.retainedBytes += len(line)
	bridge.handlers.Add(1)
	bridge.mu.Unlock()

	go bridge.runHandler(ctx, id, method, params, inbound)
	return nil
}

func (bridge *Bridge) runHandler(ctx context.Context, id, method string, params json.RawMessage, inbound *bridgeInbound) {
	defer bridge.handlers.Done()
	var result json.RawMessage
	var handlerErr error
	func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				handlerErr = fmt.Errorf("handler panic: %v", recovered)
			}
		}()
		if bridge.handler == nil {
			handlerErr = NewBridgeCallError("method_not_found", "Pi-family bridge method is not available")
			return
		}
		result, handlerErr = bridge.handler(ctx, method, params)
	}()

	if handlerErr != nil {
		if err := bridge.sendErrorResponse(id, bridge.handlerCallError(handlerErr), true); err != nil && bridge.ctx.Err() == nil {
			bridge.stop(err, false)
		}
	} else {
		if result == nil {
			result = json.RawMessage("null")
		}
		if !json.Valid(result) {
			bridge.stop(fmt.Errorf("%w: handler returned invalid JSON", ErrBridgeProtocol), false)
		} else {
			body, err := bridge.encodeRawResponse(id, result, nil)
			if err != nil {
				bridge.stop(err, false)
			} else if write, err := bridge.enqueue(body); err != nil {
				bridge.stop(err, false)
			} else if err := waitBridgeWrite(bridge.ctx, write); err != nil && bridge.ctx.Err() == nil {
				bridge.stop(err, false)
			}
		}
	}
	inbound.cancel()
	bridge.mu.Lock()
	if bridge.inbound[id] == inbound {
		delete(bridge.inbound, id)
		bridge.retainedBytes -= inbound.bytes
	}
	bridge.mu.Unlock()
}

func (bridge *Bridge) handlerCallError(err error) *BridgeCallError {
	var callErr *BridgeCallError
	if errors.As(err, &callErr) && validBridgeCode(callErr.Code) {
		return NewBridgeCallError(callErr.Code, nonemptyBridgeMessage(truncateBridgeMessage(callErr.Message)))
	}
	if errors.Is(err, context.Canceled) {
		return NewBridgeCallError("cancelled", "Pi-family bridge call was cancelled")
	}
	return NewBridgeCallError("handler_error", nonemptyBridgeMessage(truncateBridgeMessage(err.Error())))
}

func nonemptyBridgeMessage(message string) string {
	if message == "" {
		return "Pi-family bridge handler failed"
	}
	return message
}

func truncateBridgeMessage(message string) string {
	if !utf8.ValidString(message) {
		return "invalid handler error"
	}
	max := 1024
	if len(message) <= max {
		return message
	}
	for max > 0 && !utf8.RuneStart(message[max]) {
		max--
	}
	return message[:max]
}

func (bridge *Bridge) sendErrorResponse(id string, callErr *BridgeCallError, wait bool) error {
	body, err := bridge.encodeRawResponse(id, nil, callErr)
	if err != nil {
		return err
	}
	write, err := bridge.enqueue(body)
	if err != nil {
		return err
	}
	if wait {
		return waitBridgeWrite(bridge.ctx, write)
	}
	return nil
}

func (bridge *Bridge) encodeRawResponse(id string, result json.RawMessage, callErr *BridgeCallError) ([]byte, error) {
	if callErr != nil {
		return bridge.encode(struct {
			Version int              `json:"version"`
			Type    string           `json:"type"`
			ID      string           `json:"id"`
			Error   *BridgeCallError `json:"error"`
		}{BridgeProtocolVersion, "response", id, callErr})
	}
	return bridge.encode(struct {
		Version int             `json:"version"`
		Type    string          `json:"type"`
		ID      string          `json:"id"`
		Result  json.RawMessage `json:"result"`
	}{BridgeProtocolVersion, "response", id, result})
}

func (bridge *Bridge) acceptResponse(object map[string]json.RawMessage, line []byte) error {
	_, hasResult := object["result"]
	_, hasError := object["error"]
	if hasResult == hasError {
		return fmt.Errorf("%w: response must contain exactly one of result or error", ErrBridgeProtocol)
	}
	keys := []string{"version", "type", "id", "result"}
	if hasError {
		keys[3] = "error"
	}
	if err := exactBridgeKeys(object, keys...); err != nil {
		return err
	}
	id, err := bridgeString(object, "id")
	if err != nil {
		return err
	}
	if _, err := parseBridgeRequestID(id, bridge.role); err != nil {
		return err
	}
	var callErr error
	var result json.RawMessage
	if hasError {
		var wire struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		}
		var errorObject map[string]json.RawMessage
		if json.Unmarshal(object["error"], &errorObject) != nil || exactBridgeKeys(errorObject, "code", "message") != nil ||
			json.Unmarshal(object["error"], &wire) != nil || !validBridgeCode(wire.Code) ||
			wire.Message == "" || len(wire.Message) > 1024 || !utf8.ValidString(wire.Message) {
			return fmt.Errorf("%w: response error is invalid", ErrBridgeProtocol)
		}
		callErr = NewBridgeCallError(wire.Code, wire.Message)
	} else {
		result = object["result"]
		if !json.Valid(result) {
			return fmt.Errorf("%w: response result is invalid", ErrBridgeProtocol)
		}
	}

	bridge.mu.Lock()
	pending := bridge.pending[id]
	if pending == nil {
		bridge.mu.Unlock()
		return fmt.Errorf("%w: unknown or duplicate response id %q", ErrBridgeProtocol, id)
	}
	if pending.abandoned {
		delete(bridge.pending, id)
		bridge.mu.Unlock()
		return nil
	}
	if bridge.retainedBytes+bridge.pendingBytes > bridge.limits.MaxRetainedBytes-len(line) {
		bridge.mu.Unlock()
		return fmt.Errorf("%w: retained response capacity is exhausted", ErrBridgeBusy)
	}
	delete(bridge.pending, id)
	bridge.retainedBytes += len(line)
	pending.result <- bridgeCallResult{result: result, err: callErr, bytes: len(line)}
	bridge.mu.Unlock()
	return nil
}

func (bridge *Bridge) acceptCancel(object map[string]json.RawMessage) error {
	if err := exactBridgeKeys(object, "version", "type", "id"); err != nil {
		return err
	}
	id, err := bridgeString(object, "id")
	if err != nil {
		return err
	}
	sequence, err := parseBridgeRequestID(id, bridge.peerRole)
	if err != nil {
		return err
	}
	bridge.mu.Lock()
	if sequence > bridge.nextInboundID {
		bridge.mu.Unlock()
		return fmt.Errorf("%w: cancellation references future request %q", ErrBridgeProtocol, id)
	}
	inbound := bridge.inbound[id]
	if inbound != nil && !inbound.cancelled {
		inbound.cancelled = true
		inbound.cancel()
	}
	bridge.mu.Unlock()
	return nil
}

func (bridge *Bridge) abandonPending(id string, pending *bridgePending) (bridgeCallResult, bool) {
	bridge.mu.Lock()
	if bridge.pending[id] == pending {
		pending.abandoned = true
		bridge.mu.Unlock()
		return bridgeCallResult{}, false
	}
	bridge.mu.Unlock()
	select {
	case result := <-pending.result:
		return result, true
	default:
		return bridgeCallResult{}, false
	}
}

func (bridge *Bridge) consumeCallResult(callResult bridgeCallResult, result any) error {
	if callResult.bytes > 0 {
		bridge.mu.Lock()
		bridge.retainedBytes -= callResult.bytes
		bridge.mu.Unlock()
	}
	if callResult.err != nil {
		return callResult.err
	}
	if result == nil {
		return nil
	}
	if err := json.Unmarshal(callResult.result, result); err != nil {
		return fmt.Errorf("%w: decode response result: %v", ErrBridgeProtocol, err)
	}
	return nil
}

func (bridge *Bridge) requestID(role BridgeRole, sequence uint64) string {
	prefix := "h:"
	if role == BridgeNative {
		prefix = "n:"
	}
	return prefix + strconv.FormatUint(sequence, 10)
}

func parseBridgeRequestID(id string, role BridgeRole) (uint64, error) {
	prefix := "h:"
	if role == BridgeNative {
		prefix = "n:"
	}
	if !strings.HasPrefix(id, prefix) {
		return 0, fmt.Errorf("%w: request id %q has the wrong owner", ErrBridgeProtocol, id)
	}
	digits := strings.TrimPrefix(id, prefix)
	if digits == "" || (len(digits) > 1 && digits[0] == '0') || len(digits) > 20 {
		return 0, fmt.Errorf("%w: request id %q is invalid", ErrBridgeProtocol, id)
	}
	sequence, err := strconv.ParseUint(digits, 10, 64)
	if err != nil || sequence == 0 || sequence > maxBridgeRequestID {
		return 0, fmt.Errorf("%w: request id %q is invalid", ErrBridgeProtocol, id)
	}
	return sequence, nil
}

func validateBridgeMethod(method string) error {
	if method == "" || len(method) > 128 || !utf8.ValidString(method) {
		return fmt.Errorf("%w: method is invalid", ErrBridgeProtocol)
	}
	for index, r := range method {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (index > 0 && r >= '0' && r <= '9') ||
			(index > 0 && (r == '.' || r == '_' || r == '-')) {
			continue
		}
		return fmt.Errorf("%w: method %q is invalid", ErrBridgeProtocol, method)
	}
	return nil
}

func validBridgeCode(code string) bool {
	if code == "" || len(code) > 64 || !utf8.ValidString(code) {
		return false
	}
	for index, r := range code {
		if (r >= 'a' && r <= 'z') || (index > 0 && r >= '0' && r <= '9') || (index > 0 && r == '_') {
			continue
		}
		return false
	}
	return true
}
