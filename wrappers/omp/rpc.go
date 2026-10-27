// SPDX-License-Identifier: MIT
package omp

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"unicode"
	"unicode/utf8"
)

const maxNativeRPCID = uint64(1<<53 - 1)
const nativeRPCPhysicalFrameBytes = 1 << 20
const nativeRPCLogicalFrameBytes = 64 << 20
const nativeRPCChunkPayloadBytes = 256 << 10

var (
	errNativeRPCBusy     = errors.New("OMP native RPC is busy")
	errNativeRPCClosed   = errors.New("OMP native RPC is closed")
	errNativeRPCProtocol = errors.New("OMP native RPC protocol violation")
	// errNativeRPCInputTooLarge is a command that does not fit one native
	// input frame once encoded. It is refused before anything is written.
	errNativeRPCInputTooLarge = fmt.Errorf("%w: input frame exceeds its bound", errNativeRPCProtocol)
)

type nativeRPCLimits struct {
	maxInputFrame    int
	maxPhysicalFrame int
	maxLogicalFrame  int
	maxPendingCalls  int
	maxPendingWrites int
	maxRetainedBytes int
}

var defaultNativeRPCLimits = nativeRPCLimits{
	maxInputFrame:    nativeRPCPhysicalFrameBytes,
	maxPhysicalFrame: nativeRPCPhysicalFrameBytes,
	maxLogicalFrame:  nativeRPCLogicalFrameBytes,
	maxPendingCalls:  256,
	maxPendingWrites: 256,
	maxRetainedBytes: nativeRPCLogicalFrameBytes,
}

func (limits nativeRPCLimits) normalized() (nativeRPCLimits, error) {
	if limits == (nativeRPCLimits{}) {
		limits = defaultNativeRPCLimits
	}
	if limits.maxInputFrame < 256 || limits.maxPhysicalFrame < 256 ||
		limits.maxLogicalFrame < limits.maxPhysicalFrame ||
		limits.maxPendingCalls < 1 || limits.maxPendingWrites < 1 ||
		limits.maxRetainedBytes < limits.maxLogicalFrame ||
		limits.maxInputFrame > defaultNativeRPCLimits.maxInputFrame ||
		limits.maxPhysicalFrame > defaultNativeRPCLimits.maxPhysicalFrame ||
		limits.maxLogicalFrame > defaultNativeRPCLimits.maxLogicalFrame ||
		limits.maxPendingCalls > defaultNativeRPCLimits.maxPendingCalls ||
		limits.maxPendingWrites > defaultNativeRPCLimits.maxPendingWrites ||
		limits.maxRetainedBytes > defaultNativeRPCLimits.maxRetainedBytes {
		return nativeRPCLimits{}, errors.New("OMP native RPC limits are invalid")
	}
	return limits, nil
}

// nativeRPCError is an authoritative success:false response from OMP. The
// controller handles it before decoding command-specific response data.
type nativeRPCError struct {
	Command string
	Message string
}

func (e *nativeRPCError) Error() string {
	if e == nil {
		return ""
	}
	if e.Message == "" {
		return fmt.Sprintf("OMP native %s failed", e.Command)
	}
	return fmt.Sprintf("OMP native %s: %s", e.Command, e.Message)
}

// nativePromptRefusal is OMP's error answer as the first response to our own
// prompt. OMP 18.11.0 answers so before admission, for example with
// AgentBusyError while its own work runs (agent-session.ts:7356-7364), so the
// prompt never reached the agent.
type nativePromptRefusal struct{ err error }

func (refusal *nativePromptRefusal) Error() string { return refusal.err.Error() }
func (refusal *nativePromptRefusal) Unwrap() error { return refusal.err }

// nativePromptFailure is our prompt failing before OMP's run for it started:
// our own validation refused it before anything was written, or OMP admitted
// it and then answered success:false before handing it to the agent, for
// example with no model or no API key (agent-session.ts 18.12.2:8196-8301).
// Nothing of the prompt reached the agent, so as for a refusal only the Run
// fails and the lane stays.
type nativePromptFailure struct{ err error }

func (failure *nativePromptFailure) Error() string { return failure.err.Error() }
func (failure *nativePromptFailure) Unwrap() error { return failure.err }

// nativeRefusedPrompt is the one prompt whose first response was an error.
// OMP 18.11.0 still writes a success response for the same id and then a
// prompt_result with agentInvoked:false (rpc-prompt-results.ts:286-296,
// rpc-mode.ts:1835-1882). Each may be absorbed once, by that exact id.
type nativeRefusedPrompt struct {
	id            string
	success       bool
	noAgentResult bool
}

type nativeRPCResult struct {
	data    json.RawMessage
	err     error
	bytes   int
	refusal bool
}

type nativeRPCPending struct {
	command   string
	result    chan nativeRPCResult
	late      nativeRPCResult
	lateReady chan struct{}
	lateSet   bool
	abandoned bool
	prompt    bool
	responded bool
}

type nativePrompt struct {
	rpc                   *nativeRPC
	id                    string
	pending               *nativeRPCPending
	immediateAgentInvoked bool
	immediateKnown        bool
	once                  sync.Once
	done                  chan struct{}
	err                   error
}

type nativeRPCChunks struct {
	id             string
	count, length  int
	next, received int
	body           bytes.Buffer
}

type nativeRPCWrite struct {
	body []byte
	end  bool
	done chan struct{}
	err  error
}

func newNativeRPCWrite(body []byte) *nativeRPCWrite {
	return &nativeRPCWrite{body: body, done: make(chan struct{})}
}

func (write *nativeRPCWrite) complete(err error) {
	write.err = err
	write.body = nil
	close(write.done)
}

type nativeRPCStats struct {
	pendingCalls  int
	pendingWrites int
	retainedBytes int
}

// nativeRPC owns OMP's stdin/stdout JSONL protocol. observe runs in the reader
// goroutine and therefore must validate and record an event without blocking
// or calling back into native RPC. The reader remains the sole stdout owner.
type nativeRPC struct {
	input   io.WriteCloser
	output  io.ReadCloser
	observe func(json.RawMessage) error
	limits  nativeRPCLimits

	ctx    context.Context
	cancel context.CancelFunc

	mu             sync.Mutex
	outboundMu     sync.Mutex
	negotiateMu    sync.Mutex
	stopped        bool
	intentional    bool
	failure        error
	inputEnding    bool
	inputEnded     bool
	inputCloseErr  error
	ready          bool
	negotiated     bool
	readyChanged   chan struct{}
	readySignaled  bool
	chunks         *nativeRPCChunks
	nextID         uint64
	pending        map[string]*nativeRPCPending
	refused        nativeRefusedPrompt
	retainedBytes  int
	pendingWrites  int
	pendingBytes   int
	writes         chan *nativeRPCWrite
	inputCloseDone chan struct{}
	readerDone     chan struct{}
	writerDone     chan struct{}
	done           chan struct{}
	closeOnce      sync.Once
}

func newNativeRPC(input io.WriteCloser, output io.ReadCloser, observe func(json.RawMessage) error, limits nativeRPCLimits) (*nativeRPC, error) {
	if input == nil || output == nil {
		return nil, errors.New("OMP native RPC requires stdin and stdout")
	}
	limits, err := limits.normalized()
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	rpc := &nativeRPC{
		input: input, output: output, observe: observe, limits: limits,
		ctx: ctx, cancel: cancel, pending: make(map[string]*nativeRPCPending),
		// One extra slot owns the zero-byte EOF marker. It can be queued behind
		// every admitted write without weakening the normal write bound.
		writes:         make(chan *nativeRPCWrite, limits.maxPendingWrites+1),
		readyChanged:   make(chan struct{}),
		inputCloseDone: make(chan struct{}), readerDone: make(chan struct{}),
		writerDone: make(chan struct{}), done: make(chan struct{}),
	}
	go rpc.writeLoop()
	go rpc.readLoop()
	go rpc.join()
	return rpc, nil
}

func (rpc *nativeRPC) Done() <-chan struct{} { return rpc.done }

func (rpc *nativeRPC) Err() error {
	rpc.mu.Lock()
	defer rpc.mu.Unlock()
	return rpc.failure
}

func (rpc *nativeRPC) Stats() nativeRPCStats {
	rpc.mu.Lock()
	defer rpc.mu.Unlock()
	return nativeRPCStats{
		pendingCalls: len(rpc.pending), pendingWrites: rpc.pendingWrites,
		retainedBytes: rpc.retainedBytes + rpc.pendingBytes,
	}
}

// Ready joins OMP's protocol advertisement and negotiates protocol version 2
// before any product command is admitted. OMP emits ordinary startup events
// between these frames, so the reader continues to dispatch them while the
// correlated negotiation response is pending.
func (rpc *nativeRPC) Ready(ctx context.Context) error {
	if ctx == nil {
		return errors.New("OMP native RPC readiness requires context")
	}
	rpc.negotiateMu.Lock()
	defer rpc.negotiateMu.Unlock()

	rpc.mu.Lock()
	if rpc.stopped {
		err := rpc.connectionErrorLocked()
		rpc.mu.Unlock()
		return err
	}
	if rpc.negotiated {
		rpc.mu.Unlock()
		return nil
	}
	changed := rpc.readyChanged
	rpc.mu.Unlock()
	select {
	case <-changed:
	case <-ctx.Done():
		return ctx.Err()
	}

	rpc.mu.Lock()
	if !rpc.ready {
		err := rpc.connectionErrorLocked()
		rpc.mu.Unlock()
		return err
	}
	rpc.mu.Unlock()
	var result struct {
		ProtocolVersion int `json:"protocolVersion"`
	}
	if _, err := rpc.callWithAdmission(ctx, "negotiate_protocol", map[string]any{"protocolVersion": 2}, &result); err != nil {
		return err
	}
	if result.ProtocolVersion != 2 {
		err := fmt.Errorf("%w: native negotiation selected protocol %d", errNativeRPCProtocol, result.ProtocolVersion)
		rpc.stop(err, false)
		return err
	}
	rpc.mu.Lock()
	rpc.negotiated = true
	rpc.mu.Unlock()
	return nil
}

// Call writes one correlated OMP command. fields must not replace the owned id
// or type. A nil result accepts a successful response without data.
func (rpc *nativeRPC) Call(ctx context.Context, command string, fields map[string]any, result any) error {
	if command == "prompt" || command == "negotiate_protocol" {
		return fmt.Errorf("%w: command %q requires its owned API", errNativeRPCProtocol, command)
	}
	rpc.mu.Lock()
	negotiated := rpc.negotiated
	rpc.mu.Unlock()
	if !negotiated {
		return fmt.Errorf("%w: protocol is not negotiated", errNativeRPCProtocol)
	}
	_, err := rpc.callWithAdmission(ctx, command, fields, result)
	return err
}

// StartPrompt admits one prompt and joins its immediate response while keeping
// the correlation alive for OMP's legal later success:false response. The
// caller must Finish after a terminal/no-agent outcome; a Late signal is
// authoritative and Finish returns the retained failure.
func (rpc *nativeRPC) StartPrompt(ctx context.Context, message string) (*nativePrompt, bool, error) {
	rpc.mu.Lock()
	negotiated := rpc.negotiated
	rpc.mu.Unlock()
	if !negotiated {
		return nil, false, fmt.Errorf("%w: protocol is not negotiated", errNativeRPCProtocol)
	}
	if message == "" || !utf8.ValidString(message) || len(message) > rpc.limits.maxInputFrame {
		return nil, false, &nativePromptFailure{err: errors.New("OMP native prompt is invalid")}
	}
	id, pending, write, err := rpc.admitPrompt(ctx, map[string]any{"message": message})
	if errors.Is(err, errNativeRPCInputTooLarge) {
		// JSON escaping can grow a prompt within the raw bound past one
		// frame. Nothing was written, so only this Run fails.
		return nil, false, &nativePromptFailure{err: fmt.Errorf("OMP lane prompt exceeds the %d-byte native input frame once encoded", rpc.limits.maxInputFrame)}
	}
	if err != nil {
		return nil, false, err
	}
	completed, responded, writeErr := waitNativeRPCAdmission(ctx, write, pending)
	if responded {
		return rpc.completePromptStart(id, pending, completed)
	}
	if writeErr != nil {
		if completed, ok := rpc.abandonPending(id, pending); ok {
			return rpc.completePromptStart(id, pending, completed)
		}
		if ctx.Err() != nil {
			rpc.stop(ctx.Err(), false)
			return nil, true, ctx.Err()
		}
		return nil, true, writeErr
	}
	select {
	case completed := <-pending.result:
		return rpc.completePromptStart(id, pending, completed)
	case <-ctx.Done():
		if completed, ok := rpc.abandonPending(id, pending); ok {
			return rpc.completePromptStart(id, pending, completed)
		}
		return nil, true, ctx.Err()
	}
}

func (rpc *nativeRPC) completePromptStart(id string, pending *nativeRPCPending, completed nativeRPCResult) (*nativePrompt, bool, error) {
	if completed.refusal {
		// The refusal was the first response and removed the correlation, so
		// no late result can be retained for it.
		_ = rpc.consumeResult(completed, nil)
		return nil, true, &nativePromptRefusal{err: completed.err}
	}
	agentInvoked, known, err := rpc.consumePromptStart(completed)
	if err != nil {
		var late nativeRPCResult
		var lateSet bool
		rpc.mu.Lock()
		if rpc.pending[id] == pending {
			delete(rpc.pending, id)
		} else if pending.lateSet {
			late = pending.late
			lateSet = true
		}
		rpc.mu.Unlock()
		if lateSet {
			// The initial response remains the visible protocol failure. A late
			// native error may already have won the correlation while that
			// response was being decoded, so release its retained frame even
			// though no nativePrompt is returned to own Finish.
			_ = rpc.consumeResult(late, nil)
		}
		return nil, true, err
	}
	return &nativePrompt{
		rpc: rpc, id: id, pending: pending, done: make(chan struct{}),
		immediateAgentInvoked: agentInvoked, immediateKnown: known,
	}, true, nil
}

func (rpc *nativeRPC) consumePromptStart(completed nativeRPCResult) (bool, bool, error) {
	data := append(json.RawMessage(nil), completed.data...)
	if err := rpc.consumeResult(completed, nil); err != nil {
		return false, false, err
	}
	if len(bytes.TrimSpace(data)) == 0 {
		return false, false, nil
	}
	var object map[string]json.RawMessage
	if json.Unmarshal(data, &object) != nil || len(object) != 1 {
		return false, false, errors.New("invalid OMP native prompt response data")
	}
	raw, ok := object["agentInvoked"]
	var invoked bool
	if !ok || json.Unmarshal(raw, &invoked) != nil ||
		(!bytes.Equal(bytes.TrimSpace(raw), []byte("true")) && !bytes.Equal(bytes.TrimSpace(raw), []byte("false"))) {
		return false, false, errors.New("invalid OMP native prompt response data")
	}
	return invoked, true, nil
}

// Late closes when the prompt correlation has received an authoritative late
// failure or its transport has been lost. It does not transfer ownership of
// the retained result; Finish consumes and releases that result exactly once.
func (prompt *nativePrompt) Late() <-chan struct{} { return prompt.pending.lateReady }

// ImmediateAgentInvoked reports the optional native admission fact carried by
// the initial prompt response. known=false means the response omitted data and
// later preflight/events or prompt_result must provide authority.
func (prompt *nativePrompt) ImmediateAgentInvoked() (invoked, known bool) {
	if prompt == nil {
		return false, false
	}
	return prompt.immediateAgentInvoked, prompt.immediateKnown
}

// Finish closes the prompt correlation after native terminal authority. A
// late native error accepted at the same boundary wins over local completion.
func (prompt *nativePrompt) Finish() error {
	if prompt == nil || prompt.rpc == nil {
		return errors.New("OMP native prompt is invalid")
	}
	prompt.once.Do(func() {
		rpc := prompt.rpc
		rpc.mu.Lock()
		if rpc.pending[prompt.id] == prompt.pending {
			delete(rpc.pending, prompt.id)
			rpc.mu.Unlock()
		} else if prompt.pending.lateSet {
			completed := prompt.pending.late
			rpc.mu.Unlock()
			prompt.err = rpc.consumeResult(completed, nil)
		} else {
			rpc.mu.Unlock()
			prompt.err = fmt.Errorf("%w: prompt result ownership is unavailable", errNativeRPCProtocol)
		}
		close(prompt.done)
	})
	<-prompt.done
	return prompt.err
}

func (rpc *nativeRPC) admitPrompt(ctx context.Context, fields map[string]any) (string, *nativeRPCPending, *nativeRPCWrite, error) {
	return rpc.admitCallMode(ctx, "prompt", fields, true)
}

// callWithAdmission reports whether the command crossed the atomic writer
// queue boundary. Its caller can then distinguish a pre-write rejection from
// a submitted command whose response or write disposition became uncertain.
func (rpc *nativeRPC) callWithAdmission(ctx context.Context, command string, fields map[string]any, result any) (bool, error) {
	if ctx == nil {
		return false, errors.New("OMP native RPC call requires context")
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if err := validNativeRPCCommand(command); err != nil {
		return false, err
	}
	id, pending, write, err := rpc.admitCall(ctx, command, fields)
	if err != nil {
		return false, err
	}
	completed, responded, writeErr := waitNativeRPCAdmission(ctx, write, pending)
	if responded {
		return true, rpc.consumeResult(completed, result)
	}
	if writeErr != nil {
		if completed, ok := rpc.abandonPending(id, pending); ok {
			return true, rpc.consumeResult(completed, result)
		}
		// A cancelled or failed write can be partial. Retire the transport so
		// an uncertain command cannot outlive its owner.
		if ctx.Err() != nil {
			rpc.stop(ctx.Err(), false)
			return true, ctx.Err()
		}
		return true, writeErr
	}

	select {
	case completed := <-pending.result:
		return true, rpc.consumeResult(completed, result)
	case <-ctx.Done():
		if completed, ok := rpc.abandonPending(id, pending); ok {
			return true, rpc.consumeResult(completed, result)
		}
		return true, ctx.Err()
	}
}

// A correlated response proves that the peer received the complete command,
// even if the local writer has not yet returned from its completion callback.
func waitNativeRPCAdmission(ctx context.Context, write *nativeRPCWrite, pending *nativeRPCPending) (nativeRPCResult, bool, error) {
	select {
	case completed := <-pending.result:
		return completed, true, nil
	default:
	}
	select {
	case <-write.done:
		return nativeRPCResult{}, false, write.err
	default:
	}
	select {
	case completed := <-pending.result:
		return completed, true, nil
	case <-write.done:
		return nativeRPCResult{}, false, write.err
	case <-ctx.Done():
		select {
		case completed := <-pending.result:
			return completed, true, nil
		default:
		}
		select {
		case <-write.done:
			return nativeRPCResult{}, false, write.err
		default:
			return nativeRPCResult{}, false, ctx.Err()
		}
	}
}

func (rpc *nativeRPC) admitCall(ctx context.Context, command string, fields map[string]any) (string, *nativeRPCPending, *nativeRPCWrite, error) {
	return rpc.admitCallMode(ctx, command, fields, false)
}

func (rpc *nativeRPC) admitCallMode(ctx context.Context, command string, fields map[string]any, prompt bool) (string, *nativeRPCPending, *nativeRPCWrite, error) {
	rpc.outboundMu.Lock()
	defer rpc.outboundMu.Unlock()
	if err := ctx.Err(); err != nil {
		return "", nil, nil, err
	}

	rpc.mu.Lock()
	if rpc.stopped || rpc.inputEnding {
		err := rpc.connectionErrorLocked()
		rpc.mu.Unlock()
		return "", nil, nil, err
	}
	if command == "negotiate_protocol" {
		if !rpc.ready || rpc.negotiated {
			rpc.mu.Unlock()
			return "", nil, nil, fmt.Errorf("%w: invalid protocol negotiation state", errNativeRPCProtocol)
		}
	} else if !rpc.negotiated {
		rpc.mu.Unlock()
		return "", nil, nil, fmt.Errorf("%w: protocol is not negotiated", errNativeRPCProtocol)
	}
	if len(rpc.pending) >= rpc.limits.maxPendingCalls {
		rpc.mu.Unlock()
		return "", nil, nil, errNativeRPCBusy
	}
	if rpc.nextID >= maxNativeRPCID {
		rpc.mu.Unlock()
		return "", nil, nil, fmt.Errorf("%w: request id space exhausted", errNativeRPCProtocol)
	}
	sequence := rpc.nextID + 1
	rpc.mu.Unlock()

	id := "omp:" + strconv.FormatUint(sequence, 10)
	body, err := rpc.encodeCommand(id, command, fields)
	if err != nil {
		return "", nil, nil, err
	}
	pending := &nativeRPCPending{command: command, result: make(chan nativeRPCResult, 1), prompt: prompt}
	if prompt {
		pending.lateReady = make(chan struct{})
	}
	write := newNativeRPCWrite(body)

	rpc.mu.Lock()
	defer rpc.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return "", nil, nil, err
	}
	if rpc.stopped || rpc.inputEnding {
		return "", nil, nil, rpc.connectionErrorLocked()
	}
	if command == "negotiate_protocol" {
		if !rpc.ready || rpc.negotiated {
			return "", nil, nil, fmt.Errorf("%w: invalid protocol negotiation state", errNativeRPCProtocol)
		}
	} else if !rpc.negotiated {
		return "", nil, nil, fmt.Errorf("%w: protocol is not negotiated", errNativeRPCProtocol)
	}
	if len(rpc.pending) >= rpc.limits.maxPendingCalls ||
		rpc.pendingWrites >= rpc.limits.maxPendingWrites ||
		rpc.retainedBytes+rpc.pendingBytes > rpc.limits.maxRetainedBytes-len(body) {
		return "", nil, nil, errNativeRPCBusy
	}
	rpc.nextID = sequence
	rpc.pending[id] = pending
	rpc.pendingWrites++
	rpc.pendingBytes += len(body)
	// pendingWrites includes a frame already owned by the writer. The bounded
	// channel therefore has room while this lock excludes shutdown/drain.
	rpc.writes <- write
	return id, pending, write, nil
}

func (rpc *nativeRPC) encodeCommand(id, command string, fields map[string]any) ([]byte, error) {
	object := make(map[string]any, len(fields)+2)
	object["id"] = id
	object["type"] = command
	for key, value := range fields {
		if key == "id" || key == "type" {
			return nil, fmt.Errorf("%w: command field %q is reserved", errNativeRPCProtocol, key)
		}
		object[key] = value
	}
	body, err := json.Marshal(object)
	if err != nil {
		return nil, fmt.Errorf("encode OMP native RPC command: %w", err)
	}
	if len(body) == 0 || len(body)+1 > rpc.limits.maxInputFrame {
		return nil, fmt.Errorf("%w (%d bytes)", errNativeRPCInputTooLarge, rpc.limits.maxInputFrame)
	}
	return append(body, '\n'), nil
}

// CancelUI writes the only uncorrelated native input used by a managed OMP
// lane. Dialog cancellation has the native request ID but produces no response
// frame, so representing it as Call would retain a result wait forever.
func (rpc *nativeRPC) CancelUI(ctx context.Context, requestID string) error {
	if ctx == nil {
		return errors.New("OMP native UI cancellation requires context")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if !utf8.ValidString(requestID) || requestID == "" || len(requestID) > 256 || strings.IndexFunc(requestID, unicode.IsControl) >= 0 {
		return errors.New("OMP native UI request identity is invalid")
	}
	body, err := json.Marshal(map[string]any{
		"type": "extension_ui_response", "id": requestID, "cancelled": true,
	})
	if err != nil || len(body) == 0 || len(body)+1 > rpc.limits.maxInputFrame {
		return fmt.Errorf("%w: invalid UI cancellation frame", errNativeRPCProtocol)
	}
	body = append(body, '\n')
	write := newNativeRPCWrite(body)

	rpc.outboundMu.Lock()
	if err = ctx.Err(); err != nil {
		rpc.outboundMu.Unlock()
		return err
	}
	rpc.mu.Lock()
	if rpc.stopped || rpc.inputEnding {
		err = rpc.connectionErrorLocked()
		rpc.mu.Unlock()
		rpc.outboundMu.Unlock()
		return err
	}
	if rpc.pendingWrites >= rpc.limits.maxPendingWrites ||
		rpc.retainedBytes+rpc.pendingBytes > rpc.limits.maxRetainedBytes-len(body) {
		rpc.mu.Unlock()
		rpc.outboundMu.Unlock()
		return errNativeRPCBusy
	}
	rpc.pendingWrites++
	rpc.pendingBytes += len(body)
	rpc.writes <- write
	rpc.mu.Unlock()
	rpc.outboundMu.Unlock()

	if err = waitNativeRPCWrite(ctx, write); err != nil {
		// Cancellation while an admitted write is unresolved leaves its byte
		// boundary uncertain. Retire the transport rather than pretending that
		// the native dialog was or was not canceled.
		if ctx.Err() != nil {
			rpc.stop(ctx.Err(), false)
			return ctx.Err()
		}
		return err
	}
	return nil
}

// EndInput closes native stdin only after all correlated calls and retained
// responses have settled. OMP then owns its graceful shutdown and stdout EOF.
func (rpc *nativeRPC) EndInput(ctx context.Context) error {
	if ctx == nil {
		return errors.New("OMP native RPC input close requires context")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	rpc.outboundMu.Lock()
	rpc.mu.Lock()
	if err := ctx.Err(); err != nil {
		rpc.mu.Unlock()
		rpc.outboundMu.Unlock()
		return err
	}
	if rpc.stopped || rpc.inputEnding {
		err := rpc.connectionErrorLocked()
		rpc.mu.Unlock()
		rpc.outboundMu.Unlock()
		return err
	}
	if len(rpc.pending) != 0 || rpc.retainedBytes != 0 {
		rpc.mu.Unlock()
		rpc.outboundMu.Unlock()
		return errNativeRPCBusy
	}
	write := newNativeRPCWrite(nil)
	write.end = true
	rpc.inputEnding = true
	rpc.writes <- write
	rpc.mu.Unlock()
	rpc.outboundMu.Unlock()

	if err := waitNativeRPCWrite(ctx, write); err != nil {
		if ctx.Err() != nil {
			rpc.stop(ctx.Err(), false)
			return ctx.Err()
		}
		return err
	}
	return nil
}

func (rpc *nativeRPC) Close() error {
	rpc.closeOnce.Do(func() { rpc.stop(nil, true) })
	<-rpc.done
	return rpc.Err()
}

func (rpc *nativeRPC) join() {
	<-rpc.readerDone
	<-rpc.writerDone
	close(rpc.done)
}

func (rpc *nativeRPC) stop(err error, intentional bool) {
	rpc.mu.Lock()
	if rpc.stopped {
		if err != nil && rpc.failure == nil && !rpc.intentional {
			rpc.failure = err
		}
		rpc.mu.Unlock()
		return
	}
	rpc.stopped = true
	rpc.intentional = intentional
	rpc.refused = nativeRefusedPrompt{}
	if err != nil && !intentional {
		rpc.failure = err
	}
	connectionErr := rpc.connectionErrorLocked()
	if !rpc.readySignaled {
		close(rpc.readyChanged)
		rpc.readySignaled = true
	}
	if rpc.chunks != nil {
		rpc.retainedBytes -= rpc.chunks.received
		rpc.chunks = nil
	}
	for id, pending := range rpc.pending {
		delete(rpc.pending, id)
		if !pending.abandoned {
			if pending.prompt && pending.responded {
				rpc.setPromptLateLocked(pending, nativeRPCResult{err: connectionErr})
			} else {
				pending.result <- nativeRPCResult{err: connectionErr}
			}
		}
	}
	rpc.mu.Unlock()

	rpc.cancel()
	_ = rpc.input.Close()
	_ = rpc.output.Close()
}

func (rpc *nativeRPC) connectionErrorLocked() error {
	if rpc.failure != nil {
		return rpc.failure
	}
	return errNativeRPCClosed
}

func (rpc *nativeRPC) writeLoop() {
	defer close(rpc.writerDone)
	for {
		select {
		case <-rpc.ctx.Done():
			rpc.drainWrites(rpc.connectionError())
			return
		case write := <-rpc.writes:
			if write.end {
				err := rpc.input.Close()
				rpc.mu.Lock()
				rpc.inputCloseErr = err
				rpc.inputEnded = err == nil
				close(rpc.inputCloseDone)
				rpc.mu.Unlock()
				write.complete(err)
				if err != nil {
					rpc.stop(fmt.Errorf("close OMP native stdin: %w", err), false)
				}
				return
			}
			err := writeNativeRPCAll(rpc.input, write.body)
			rpc.releaseWrite(len(write.body))
			write.complete(err)
			if err != nil {
				rpc.stop(fmt.Errorf("write OMP native RPC: %w", err), false)
				rpc.drainWrites(rpc.connectionError())
				return
			}
		}
	}
}

func (rpc *nativeRPC) releaseWrite(bytes int) {
	rpc.mu.Lock()
	rpc.pendingWrites--
	rpc.pendingBytes -= bytes
	rpc.mu.Unlock()
}

func (rpc *nativeRPC) drainWrites(err error) {
	for {
		select {
		case write := <-rpc.writes:
			if !write.end {
				rpc.releaseWrite(len(write.body))
			}
			write.complete(err)
		default:
			return
		}
	}
}

func writeNativeRPCAll(writer io.Writer, body []byte) error {
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

func waitNativeRPCWrite(ctx context.Context, write *nativeRPCWrite) error {
	select {
	case <-write.done:
		return write.err
	default:
	}
	select {
	case <-ctx.Done():
		// A write which settled at the same boundary owns the disposition.
		select {
		case <-write.done:
			return write.err
		default:
			return ctx.Err()
		}
	case <-write.done:
		return write.err
	}
}

func (rpc *nativeRPC) readLoop() {
	defer close(rpc.readerDone)
	reader := bufio.NewReaderSize(rpc.output, 4096)
	for {
		line, err := readNativeRPCLine(reader, rpc.limits.maxPhysicalFrame)
		if err != nil {
			if rpc.ctx.Err() != nil {
				return
			}
			if errors.Is(err, io.EOF) {
				expected, closeErr := rpc.awaitOutputEOF()
				if rpc.ctx.Err() != nil {
					return
				}
				if expected && closeErr == nil {
					rpc.stop(nil, true)
					return
				}
				if expected {
					rpc.stop(closeErr, false)
					return
				}
			}
			rpc.stop(err, false)
			return
		}
		err = rpc.acceptPhysicalFrame(line)
		if err != nil {
			rpc.stop(err, false)
			return
		}
	}
}

func readNativeRPCLine(reader *bufio.Reader, limit int) ([]byte, error) {
	var line []byte
	for {
		part, err := reader.ReadSlice('\n')
		if len(line)+len(part) > limit {
			return nil, fmt.Errorf("%w: output frame exceeds %d bytes", errNativeRPCProtocol, limit)
		}
		line = append(line, part...)
		if err == nil {
			if len(line) < 2 || line[len(line)-1] != '\n' || line[len(line)-2] == '\r' {
				return nil, fmt.Errorf("%w: output frame is not LF-delimited", errNativeRPCProtocol)
			}
			return line[:len(line)-1], nil
		}
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		if errors.Is(err, io.EOF) {
			if len(line) == 0 {
				return nil, io.EOF
			}
			return nil, fmt.Errorf("%w: partial output frame at EOF", errNativeRPCProtocol)
		}
		return nil, fmt.Errorf("read OMP native RPC: %w", err)
	}
}

func (rpc *nativeRPC) acceptPhysicalFrame(line []byte) error {
	kind, object, err := decodeNativeRPCEnvelope(line)
	if err != nil {
		return err
	}
	if kind == "rpc_chunk" {
		logical, err := rpc.acceptChunk(object)
		if err != nil || logical == nil {
			return err
		}
		return rpc.acceptLogicalFrame(logical)
	}
	rpc.mu.Lock()
	interrupted := rpc.chunks != nil
	rpc.mu.Unlock()
	if interrupted {
		return fmt.Errorf("%w: RPC chunk sequence interrupted", errNativeRPCProtocol)
	}
	return rpc.acceptLogicalFrame(line)
}

func (rpc *nativeRPC) acceptLogicalFrame(line []byte) error {
	if len(line) == 0 || len(line) > rpc.limits.maxLogicalFrame || !utf8.Valid(line) {
		return fmt.Errorf("%w: invalid logical output frame", errNativeRPCProtocol)
	}
	kind, object, err := decodeNativeRPCEnvelope(line)
	if err != nil {
		return err
	}
	rpc.mu.Lock()
	ready, negotiated := rpc.ready, rpc.negotiated
	rpc.mu.Unlock()
	if !ready {
		if kind != "ready" {
			return fmt.Errorf("%w: output before readiness", errNativeRPCProtocol)
		}
		return rpc.acceptReady(object)
	}
	if kind == "ready" {
		return fmt.Errorf("%w: duplicate readiness", errNativeRPCProtocol)
	}
	if kind == "rpc_chunk" {
		return fmt.Errorf("%w: nested RPC chunk frame", errNativeRPCProtocol)
	}
	if kind == "rpc_frame_error" {
		message, parseErr := nativeRPCText(object, "error", 64<<10)
		if parseErr != nil {
			return parseErr
		}
		return fmt.Errorf("%w: native frame error: %s", errNativeRPCProtocol, message)
	}
	if kind == "response" {
		return rpc.acceptResponse(object, line)
	}
	if kind == "prompt_result" && rpc.absorbRefusedPromptResult(object) {
		return nil
	}
	if !negotiated {
		// OMP can publish startup metadata after ready and before the
		// negotiation response. These are valid protocol-v1 physical events.
		if rpc.observe != nil {
			return rpc.observe(append(json.RawMessage(nil), line...))
		}
		return nil
	}
	if rpc.observe != nil {
		return rpc.observe(append(json.RawMessage(nil), line...))
	}
	return nil
}

func (rpc *nativeRPC) acceptReady(object map[string]json.RawMessage) error {
	if len(object) != 5 {
		return fmt.Errorf("%w: readiness fields are invalid", errNativeRPCProtocol)
	}
	var protocolVersion, maxFrame, maxReassembled int
	var supported []int
	if json.Unmarshal(object["protocolVersion"], &protocolVersion) != nil || protocolVersion != 1 ||
		json.Unmarshal(object["supportedProtocolVersions"], &supported) != nil || len(supported) != 2 || supported[0] != 1 || supported[1] != 2 ||
		json.Unmarshal(object["maxFrameBytes"], &maxFrame) != nil || maxFrame != nativeRPCPhysicalFrameBytes ||
		json.Unmarshal(object["maxReassembledFrameBytes"], &maxReassembled) != nil || maxReassembled != nativeRPCLogicalFrameBytes {
		return fmt.Errorf("%w: readiness capabilities are invalid", errNativeRPCProtocol)
	}
	rpc.mu.Lock()
	if rpc.ready || rpc.stopped {
		rpc.mu.Unlock()
		return fmt.Errorf("%w: duplicate or late readiness", errNativeRPCProtocol)
	}
	rpc.ready = true
	if !rpc.readySignaled {
		close(rpc.readyChanged)
		rpc.readySignaled = true
	}
	rpc.mu.Unlock()
	return nil
}

func (rpc *nativeRPC) acceptChunk(object map[string]json.RawMessage) ([]byte, error) {
	if len(object) != 6 {
		return nil, fmt.Errorf("%w: RPC chunk fields are invalid", errNativeRPCProtocol)
	}
	id, err := nativeRPCString(object, "chunkId", 128)
	if err != nil {
		return nil, err
	}
	var index, count, length int
	if json.Unmarshal(object["index"], &index) != nil || json.Unmarshal(object["count"], &count) != nil ||
		json.Unmarshal(object["byteLength"], &length) != nil || index < 0 || count < 2 ||
		count > nativeRPCLogicalFrameBytes/nativeRPCChunkPayloadBytes || index >= count ||
		length < nativeRPCPhysicalFrameBytes || length > rpc.limits.maxLogicalFrame ||
		length <= (count-1)*nativeRPCChunkPayloadBytes || length > count*nativeRPCChunkPayloadBytes {
		return nil, fmt.Errorf("%w: invalid RPC chunk metadata", errNativeRPCProtocol)
	}
	var encoded string
	if raw := bytes.TrimSpace(object["data"]); len(raw) == 0 || raw[0] != '"' || json.Unmarshal(raw, &encoded) != nil || encoded == "" {
		return nil, fmt.Errorf("%w: invalid RPC chunk data", errNativeRPCProtocol)
	}
	decoded, err := base64.StdEncoding.Strict().DecodeString(encoded)
	if err != nil || base64.StdEncoding.EncodeToString(decoded) != encoded || len(decoded) == 0 || len(decoded) > nativeRPCChunkPayloadBytes {
		return nil, fmt.Errorf("%w: invalid RPC chunk data", errNativeRPCProtocol)
	}
	if index < count-1 && len(decoded) != nativeRPCChunkPayloadBytes {
		return nil, fmt.Errorf("%w: short non-final RPC chunk", errNativeRPCProtocol)
	}

	rpc.mu.Lock()
	defer rpc.mu.Unlock()
	if !rpc.negotiated {
		return nil, fmt.Errorf("%w: RPC chunk before protocol negotiation", errNativeRPCProtocol)
	}
	if rpc.chunks == nil {
		if index != 0 {
			return nil, fmt.Errorf("%w: RPC chunk sequence must start at zero", errNativeRPCProtocol)
		}
		rpc.chunks = &nativeRPCChunks{id: id, count: count, length: length}
	}
	pending := rpc.chunks
	if pending.id != id || pending.count != count || pending.length != length || pending.next != index {
		return nil, fmt.Errorf("%w: RPC chunk sequence mismatch", errNativeRPCProtocol)
	}
	if rpc.retainedBytes+rpc.pendingBytes > rpc.limits.maxRetainedBytes-len(decoded) || pending.received > length-len(decoded) {
		return nil, errNativeRPCBusy
	}
	_, _ = pending.body.Write(decoded)
	pending.received += len(decoded)
	pending.next++
	rpc.retainedBytes += len(decoded)
	if pending.next < pending.count {
		return nil, nil
	}
	if pending.received != pending.length {
		return nil, fmt.Errorf("%w: RPC chunk sequence length mismatch", errNativeRPCProtocol)
	}
	logical := append([]byte(nil), pending.body.Bytes()...)
	rpc.retainedBytes -= pending.received
	rpc.chunks = nil
	if !utf8.Valid(logical) {
		return nil, fmt.Errorf("%w: RPC chunk payload is not UTF-8", errNativeRPCProtocol)
	}
	return logical, nil
}

func decodeNativeRPCEnvelope(line []byte) (string, map[string]json.RawMessage, error) {
	if len(line) == 0 || !utf8.Valid(line) {
		return "", nil, fmt.Errorf("%w: invalid output frame", errNativeRPCProtocol)
	}
	var object map[string]json.RawMessage
	if json.Unmarshal(line, &object) != nil || object == nil {
		return "", nil, fmt.Errorf("%w: output frame is not a JSON object", errNativeRPCProtocol)
	}
	kind, err := nativeRPCString(object, "type", 128)
	if err != nil {
		return "", nil, err
	}
	return kind, object, nil
}

func (rpc *nativeRPC) acceptResponse(object map[string]json.RawMessage, line []byte) error {
	id, err := nativeRPCString(object, "id", 128)
	if err != nil {
		return err
	}
	command, err := nativeRPCString(object, "command", 64)
	if err != nil {
		return err
	}
	var success bool
	if raw, ok := object["success"]; !ok || json.Unmarshal(raw, &success) != nil || (!bytes.Equal(bytes.TrimSpace(raw), []byte("true")) && !bytes.Equal(bytes.TrimSpace(raw), []byte("false"))) {
		return fmt.Errorf("%w: response success is invalid", errNativeRPCProtocol)
	}
	var completed nativeRPCResult
	if success {
		if _, ok := object["error"]; ok {
			return fmt.Errorf("%w: successful response contains error", errNativeRPCProtocol)
		}
		completed.data = append(json.RawMessage(nil), object["data"]...)
	} else {
		if _, ok := object["data"]; ok {
			return fmt.Errorf("%w: failed response contains data", errNativeRPCProtocol)
		}
		message, parseErr := nativeRPCText(object, "error", 64<<10)
		if parseErr != nil {
			return parseErr
		}
		completed.err = &nativeRPCError{Command: command, Message: message}
	}
	negotiatedResponse := false
	if success && command == "negotiate_protocol" {
		var data map[string]json.RawMessage
		var version int
		if json.Unmarshal(completed.data, &data) != nil || len(data) != 1 ||
			json.Unmarshal(data["protocolVersion"], &version) != nil || version != 2 {
			return fmt.Errorf("%w: invalid protocol negotiation response", errNativeRPCProtocol)
		}
		negotiatedResponse = true
	}

	rpc.mu.Lock()
	pending := rpc.pending[id]
	if pending == nil {
		if success && command == "prompt" && rpc.refused.success && rpc.refused.id == id {
			rpc.refused.success = false
			rpc.mu.Unlock()
			return nil
		}
		rpc.mu.Unlock()
		return fmt.Errorf("%w: unknown or duplicate response id %q", errNativeRPCProtocol, id)
	}
	if pending.command != command {
		rpc.mu.Unlock()
		return fmt.Errorf("%w: response command mismatch", errNativeRPCProtocol)
	}
	if pending.abandoned {
		delete(rpc.pending, id)
		rpc.mu.Unlock()
		return nil
	}
	if rpc.retainedBytes+rpc.pendingBytes > rpc.limits.maxRetainedBytes-len(line) {
		completed.err = errNativeRPCBusy
		if pending.prompt && pending.responded {
			delete(rpc.pending, id)
			rpc.setPromptLateLocked(pending, completed)
		} else {
			delete(rpc.pending, id)
			pending.result <- completed
		}
		rpc.mu.Unlock()
		return errNativeRPCBusy
	}
	if negotiatedResponse {
		if rpc.negotiated {
			rpc.mu.Unlock()
			return fmt.Errorf("%w: duplicate protocol negotiation", errNativeRPCProtocol)
		}
		rpc.negotiated = true
	}
	completed.bytes = len(line)
	rpc.retainedBytes += completed.bytes
	if pending.prompt {
		if !pending.responded {
			pending.responded = true
			if !success {
				delete(rpc.pending, id)
				completed.refusal = true
				rpc.refused = nativeRefusedPrompt{id: id, success: true, noAgentResult: true}
			} else {
				rpc.refused = nativeRefusedPrompt{}
			}
			pending.result <- completed
			rpc.mu.Unlock()
			return nil
		}
		if success {
			rpc.retainedBytes -= completed.bytes
			rpc.mu.Unlock()
			return fmt.Errorf("%w: duplicate successful prompt response", errNativeRPCProtocol)
		}
		delete(rpc.pending, id)
		rpc.setPromptLateLocked(pending, completed)
		// OMP writes the failed prompt's agentInvoked:false prompt_result right
		// after this answer (rpc-prompt-results.ts 18.12.2:109-111, 149-166,
		// 292-296). Absorb it once, as for a refusal, so that it reaches
		// neither the next Run nor the lane.
		rpc.refused = nativeRefusedPrompt{id: id, noAgentResult: true}
		rpc.mu.Unlock()
		return nil
	}
	delete(rpc.pending, id)
	pending.result <- completed
	rpc.mu.Unlock()
	return nil
}

func (rpc *nativeRPC) setPromptLateLocked(pending *nativeRPCPending, completed nativeRPCResult) {
	if pending.lateSet {
		return
	}
	pending.late = completed
	pending.lateSet = true
	close(pending.lateReady)
}

// absorbRefusedPromptResult takes the refused prompt's own agentInvoked:false
// prompt_result before any turn sees it. Any other prompt_result, including
// agentInvoked:true for that id, is left to the observer.
func (rpc *nativeRPC) absorbRefusedPromptResult(object map[string]json.RawMessage) bool {
	id, err := nativeRPCString(object, "id", 128)
	if err != nil || !bytes.Equal(bytes.TrimSpace(object["agentInvoked"]), []byte("false")) {
		return false
	}
	rpc.mu.Lock()
	defer rpc.mu.Unlock()
	if !rpc.refused.noAgentResult || rpc.refused.id != id {
		return false
	}
	rpc.refused.noAgentResult = false
	return true
}

func (rpc *nativeRPC) abandonPending(id string, pending *nativeRPCPending) (nativeRPCResult, bool) {
	rpc.mu.Lock()
	if rpc.pending[id] == pending {
		pending.abandoned = true
		rpc.mu.Unlock()
		return nativeRPCResult{}, false
	}
	rpc.mu.Unlock()
	return <-pending.result, true
}

func (rpc *nativeRPC) consumeResult(completed nativeRPCResult, result any) error {
	if completed.bytes != 0 {
		rpc.mu.Lock()
		rpc.retainedBytes -= completed.bytes
		rpc.mu.Unlock()
	}
	if completed.err != nil {
		return completed.err
	}
	if result == nil {
		return nil
	}
	if len(completed.data) == 0 || json.Unmarshal(completed.data, result) != nil {
		return errors.New("invalid OMP native RPC response data")
	}
	return nil
}

func (rpc *nativeRPC) awaitOutputEOF() (bool, error) {
	rpc.mu.Lock()
	if !rpc.inputEnding {
		rpc.mu.Unlock()
		return false, nil
	}
	done := rpc.inputCloseDone
	rpc.mu.Unlock()
	select {
	case <-done:
	case <-rpc.ctx.Done():
		return true, context.Cause(rpc.ctx)
	}
	rpc.mu.Lock()
	defer rpc.mu.Unlock()
	if rpc.inputCloseErr != nil {
		return true, fmt.Errorf("close OMP native stdin: %w", rpc.inputCloseErr)
	}
	if !rpc.inputEnded || len(rpc.pending) != 0 || rpc.pendingWrites != 0 || rpc.retainedBytes != 0 || rpc.chunks != nil {
		return true, fmt.Errorf("%w: native stdout ended before RPC ownership settled", errNativeRPCProtocol)
	}
	return true, nil
}

func (rpc *nativeRPC) connectionError() error {
	rpc.mu.Lock()
	defer rpc.mu.Unlock()
	return rpc.connectionErrorLocked()
}

func validNativeRPCCommand(command string) error {
	if command == "" || len(command) > 64 || !utf8.ValidString(command) {
		return fmt.Errorf("%w: invalid command", errNativeRPCProtocol)
	}
	for index, r := range command {
		if (r < 'a' || r > 'z') && r != '_' && (index == 0 || r < '0' || r > '9') {
			return fmt.Errorf("%w: invalid command", errNativeRPCProtocol)
		}
	}
	return nil
}

func nativeRPCString(object map[string]json.RawMessage, key string, limit int) (string, error) {
	raw, ok := object[key]
	if !ok {
		return "", fmt.Errorf("%w: frame omitted %s", errNativeRPCProtocol, key)
	}
	var value string
	if json.Unmarshal(raw, &value) != nil || value == "" || len(value) > limit || !utf8.ValidString(value) || strings.ContainsFunc(value, unicode.IsControl) {
		return "", fmt.Errorf("%w: frame %s is invalid", errNativeRPCProtocol, key)
	}
	return value, nil
}

func nativeRPCText(object map[string]json.RawMessage, key string, limit int) (string, error) {
	raw, ok := object[key]
	if !ok {
		return "", fmt.Errorf("%w: frame omitted %s", errNativeRPCProtocol, key)
	}
	trimmed := bytes.TrimSpace(raw)
	var value string
	if len(trimmed) == 0 || trimmed[0] != '"' || json.Unmarshal(trimmed, &value) != nil || len(value) > limit || !utf8.ValidString(value) {
		return "", fmt.Errorf("%w: frame %s is invalid", errNativeRPCProtocol, key)
	}
	return value, nil
}
