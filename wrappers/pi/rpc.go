// SPDX-License-Identifier: MIT
package pi

import (
	"bufio"
	"bytes"
	"context"
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

var (
	errNativeRPCBusy     = errors.New("Pi native RPC is busy")
	errNativeRPCClosed   = errors.New("Pi native RPC is closed")
	errNativeRPCProtocol = errors.New("Pi native RPC protocol violation")
	// errNativeRPCInputTooLarge is a command that does not fit one native
	// input frame once encoded. It is refused before anything is written.
	errNativeRPCInputTooLarge = fmt.Errorf("%w: input frame exceeds its bound", errNativeRPCProtocol)
)

type nativeRPCLimits struct {
	maxInputFrame    int
	maxOutputFrame   int
	maxPendingCalls  int
	maxPendingWrites int
	maxRetainedBytes int
}

var defaultNativeRPCLimits = nativeRPCLimits{
	maxInputFrame:    1 << 20,
	maxOutputFrame:   maxHistoryFrame,
	maxPendingCalls:  256,
	maxPendingWrites: 256,
	maxRetainedBytes: 32 << 20,
}

func (limits nativeRPCLimits) normalized() (nativeRPCLimits, error) {
	if limits == (nativeRPCLimits{}) {
		limits = defaultNativeRPCLimits
	}
	if limits.maxInputFrame < 256 || limits.maxOutputFrame < 256 ||
		limits.maxPendingCalls < 1 || limits.maxPendingWrites < 1 ||
		limits.maxRetainedBytes < limits.maxOutputFrame ||
		limits.maxInputFrame > defaultNativeRPCLimits.maxInputFrame ||
		limits.maxOutputFrame > defaultNativeRPCLimits.maxOutputFrame ||
		limits.maxPendingCalls > defaultNativeRPCLimits.maxPendingCalls ||
		limits.maxPendingWrites > defaultNativeRPCLimits.maxPendingWrites ||
		limits.maxRetainedBytes > defaultNativeRPCLimits.maxRetainedBytes {
		return nativeRPCLimits{}, errors.New("Pi native RPC limits are invalid")
	}
	return limits, nil
}

// nativeRPCError is an authoritative success:false response from Pi. The
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
		return fmt.Sprintf("Pi native %s failed", e.Command)
	}
	return fmt.Sprintf("Pi native %s: %s", e.Command, e.Message)
}

type nativeRPCResult struct {
	data  json.RawMessage
	err   error
	bytes int
}

type nativeRPCPending struct {
	command   string
	result    chan nativeRPCResult
	abandoned bool
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

// nativeRPC owns Pi's stdin/stdout JSONL protocol. observe runs in the reader
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
	stopped        bool
	intentional    bool
	failure        error
	inputEnding    bool
	inputEnded     bool
	inputCloseErr  error
	nextID         uint64
	pending        map[string]*nativeRPCPending
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
		return nil, errors.New("Pi native RPC requires stdin and stdout")
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

// Call writes one correlated Pi command. fields must not replace the owned id
// or type. A nil result accepts a successful response without data.
func (rpc *nativeRPC) Call(ctx context.Context, command string, fields map[string]any, result any) error {
	_, err := rpc.callWithAdmission(ctx, command, fields, result)
	return err
}

// callWithAdmission reports whether the command crossed the atomic writer
// queue boundary. Its caller can then distinguish a pre-write rejection from
// a submitted command whose response or write disposition became uncertain.
func (rpc *nativeRPC) callWithAdmission(ctx context.Context, command string, fields map[string]any, result any) (bool, error) {
	if ctx == nil {
		return false, errors.New("Pi native RPC call requires context")
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

	id := "pi:" + strconv.FormatUint(sequence, 10)
	body, err := rpc.encodeCommand(id, command, fields)
	if err != nil {
		return "", nil, nil, err
	}
	pending := &nativeRPCPending{command: command, result: make(chan nativeRPCResult, 1)}
	write := newNativeRPCWrite(body)

	rpc.mu.Lock()
	defer rpc.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return "", nil, nil, err
	}
	if rpc.stopped || rpc.inputEnding {
		return "", nil, nil, rpc.connectionErrorLocked()
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
		return nil, fmt.Errorf("encode Pi native RPC command: %w", err)
	}
	if len(body) == 0 || len(body) > rpc.limits.maxInputFrame {
		return nil, fmt.Errorf("%w (%d bytes)", errNativeRPCInputTooLarge, rpc.limits.maxInputFrame)
	}
	return append(body, '\n'), nil
}

// CancelUI writes the only uncorrelated native input used by a managed Pi
// lane. Dialog cancellation has the native request ID but produces no response
// frame, so representing it as Call would retain a result wait forever.
func (rpc *nativeRPC) CancelUI(ctx context.Context, requestID string) error {
	if ctx == nil {
		return errors.New("Pi native UI cancellation requires context")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if !utf8.ValidString(requestID) || requestID == "" || len(requestID) > 256 || strings.IndexFunc(requestID, unicode.IsControl) >= 0 {
		return errors.New("Pi native UI request identity is invalid")
	}
	body, err := json.Marshal(map[string]any{
		"type": "extension_ui_response", "id": requestID, "cancelled": true,
	})
	if err != nil || len(body) == 0 || len(body) > rpc.limits.maxInputFrame {
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
// responses have settled. Pi then owns its graceful shutdown and stdout EOF.
func (rpc *nativeRPC) EndInput(ctx context.Context) error {
	if ctx == nil {
		return errors.New("Pi native RPC input close requires context")
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
	if err != nil && !intentional {
		rpc.failure = err
	}
	connectionErr := rpc.connectionErrorLocked()
	for id, pending := range rpc.pending {
		delete(rpc.pending, id)
		if !pending.abandoned {
			pending.result <- nativeRPCResult{err: connectionErr}
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
					rpc.stop(fmt.Errorf("close Pi native stdin: %w", err), false)
				}
				return
			}
			err := writeNativeRPCAll(rpc.input, write.body)
			rpc.releaseWrite(len(write.body))
			write.complete(err)
			if err != nil {
				rpc.stop(fmt.Errorf("write Pi native RPC: %w", err), false)
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
		line, err := readNativeRPCLine(reader, rpc.limits.maxOutputFrame)
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
		kind, object, err := decodeNativeRPCEnvelope(line)
		if err != nil {
			rpc.stop(err, false)
			return
		}
		if kind == "response" {
			err = rpc.acceptResponse(object, line)
		} else if rpc.observe != nil {
			err = rpc.observe(append(json.RawMessage(nil), line...))
		}
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
		if len(line)+len(part) > limit+1 {
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
		return nil, fmt.Errorf("read Pi native RPC: %w", err)
	}
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

	rpc.mu.Lock()
	pending := rpc.pending[id]
	if pending == nil {
		rpc.mu.Unlock()
		return fmt.Errorf("%w: unknown or duplicate response id %q", errNativeRPCProtocol, id)
	}
	if pending.command != command {
		rpc.mu.Unlock()
		return fmt.Errorf("%w: response command mismatch", errNativeRPCProtocol)
	}
	delete(rpc.pending, id)
	if pending.abandoned {
		rpc.mu.Unlock()
		return nil
	}
	if rpc.retainedBytes+rpc.pendingBytes > rpc.limits.maxRetainedBytes-len(line) {
		completed.err = errNativeRPCBusy
		pending.result <- completed
		rpc.mu.Unlock()
		return errNativeRPCBusy
	}
	completed.bytes = len(line)
	rpc.retainedBytes += completed.bytes
	pending.result <- completed
	rpc.mu.Unlock()
	return nil
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
		return errors.New("invalid Pi native RPC response data")
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
		return true, fmt.Errorf("close Pi native stdin: %w", rpc.inputCloseErr)
	}
	if !rpc.inputEnded || len(rpc.pending) != 0 || rpc.pendingWrites != 0 || rpc.retainedBytes != 0 {
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
