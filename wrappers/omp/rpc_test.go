// SPDX-License-Identifier: MIT
package omp

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const nativeRPCReadyFrame = `{"type":"ready","protocolVersion":1,"supportedProtocolVersions":[1,2],"maxFrameBytes":1048576,"maxReassembledFrameBytes":67108864}`

type nativeRPCTestPeer struct {
	commands *bufio.Reader
	events   *io.PipeWriter
}

func nativeRPCTestContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func newUnreadyNativeRPCTest(t *testing.T, observe func(json.RawMessage) error, limits nativeRPCLimits) (*nativeRPC, *nativeRPCTestPeer) {
	t.Helper()
	commands, input := io.Pipe()
	output, events := io.Pipe()
	rpc, err := newNativeRPC(input, output, observe, limits)
	if err != nil {
		t.Fatal(err)
	}
	peer := &nativeRPCTestPeer{commands: bufio.NewReader(commands), events: events}
	t.Cleanup(func() {
		_ = commands.Close()
		_ = events.Close()
		_ = rpc.Close()
	})
	return rpc, peer
}

func newNativeRPCTest(t *testing.T, observe func(json.RawMessage) error, limits nativeRPCLimits) (*nativeRPC, *nativeRPCTestPeer) {
	t.Helper()
	rpc, peer := newUnreadyNativeRPCTest(t, observe, limits)
	readyNativeRPCTest(t, rpc, peer)
	return rpc, peer
}

func readyNativeRPCTest(t *testing.T, rpc *nativeRPC, peer *nativeRPCTestPeer) {
	t.Helper()
	ready := make(chan error, 1)
	go func() { ready <- rpc.Ready(nativeRPCTestContext(t)) }()
	peer.write(t, nativeRPCReadyFrame)
	request := peer.read(t)
	if nativeRPCField(t, request, "id") != "omp:1" || nativeRPCField(t, request, "type") != "negotiate_protocol" || string(request["protocolVersion"]) != "2" {
		t.Fatalf("protocol negotiation = %#v", request)
	}
	peer.write(t, `{"id":"omp:1","type":"response","command":"negotiate_protocol","success":true,"data":{"protocolVersion":2}}`)
	if err := <-ready; err != nil {
		t.Fatal(err)
	}
}

func TestNativeRPCRequiresAndNegotiatesExactReadiness(t *testing.T) {
	events := make(chan string, 1)
	rpc, peer := newUnreadyNativeRPCTest(t, func(raw json.RawMessage) error {
		var event struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(raw, &event); err != nil {
			return err
		}
		events <- event.Type
		return nil
	}, nativeRPCLimits{})
	if err := rpc.Call(nativeRPCTestContext(t), "get_state", nil, nil); !errors.Is(err, errNativeRPCProtocol) {
		t.Fatalf("call before readiness = %v", err)
	}
	ready := make(chan error, 1)
	go func() { ready <- rpc.Ready(nativeRPCTestContext(t)) }()
	peer.write(t, nativeRPCReadyFrame)
	request := peer.read(t)
	if nativeRPCField(t, request, "id") != "omp:1" || nativeRPCField(t, request, "type") != "negotiate_protocol" || string(request["protocolVersion"]) != "2" {
		t.Fatalf("negotiation command = %#v", request)
	}
	peer.write(t, `{"type":"available_commands_update","commands":[]}`)
	if got := <-events; got != "available_commands_update" {
		t.Fatalf("startup event = %q", got)
	}
	peer.write(t, `{"id":"omp:1","type":"response","command":"negotiate_protocol","success":true,"data":{"protocolVersion":2}}`)
	if err := <-ready; err != nil {
		t.Fatal(err)
	}
	if err := rpc.Ready(nativeRPCTestContext(t)); err != nil {
		t.Fatalf("repeated joined readiness = %v", err)
	}

	peer.write(t, nativeRPCReadyFrame)
	waitNativeRPCDone(t, rpc)
	if !errors.Is(rpc.Err(), errNativeRPCProtocol) {
		t.Fatalf("duplicate readiness = %v", rpc.Err())
	}
}

func TestNativeRPCRejectsInvalidReadiness(t *testing.T) {
	for name, frame := range map[string]string{
		"wrong protocol": `{"type":"ready","protocolVersion":2,"supportedProtocolVersions":[1,2],"maxFrameBytes":1048576,"maxReassembledFrameBytes":67108864}`,
		"wrong versions": `{"type":"ready","protocolVersion":1,"supportedProtocolVersions":[2,1],"maxFrameBytes":1048576,"maxReassembledFrameBytes":67108864}`,
		"wrong physical": `{"type":"ready","protocolVersion":1,"supportedProtocolVersions":[1,2],"maxFrameBytes":1,"maxReassembledFrameBytes":67108864}`,
		"extra field":    `{"type":"ready","protocolVersion":1,"supportedProtocolVersions":[1,2],"maxFrameBytes":1048576,"maxReassembledFrameBytes":67108864,"extra":true}`,
		"event first":    `{"type":"agent_start"}`,
	} {
		t.Run(name, func(t *testing.T) {
			rpc, peer := newUnreadyNativeRPCTest(t, nil, nativeRPCLimits{})
			ready := make(chan error, 1)
			go func() { ready <- rpc.Ready(nativeRPCTestContext(t)) }()
			peer.write(t, frame)
			if err := <-ready; !errors.Is(err, errNativeRPCProtocol) {
				t.Fatalf("readiness error = %v", err)
			}
			waitNativeRPCDone(t, rpc)
		})
	}
}

func (peer *nativeRPCTestPeer) read(t *testing.T) map[string]json.RawMessage {
	t.Helper()
	type readResult struct {
		line string
		err  error
	}
	result := make(chan readResult, 1)
	go func() {
		line, err := peer.commands.ReadString('\n')
		result <- readResult{line, err}
	}()
	select {
	case got := <-result:
		if got.err != nil {
			t.Fatal(got.err)
		}
		var object map[string]json.RawMessage
		if json.Unmarshal([]byte(got.line), &object) != nil || object == nil {
			t.Fatalf("command = %q", got.line)
		}
		return object
	case <-nativeRPCTestContext(t).Done():
		t.Fatal("reading OMP native RPC command timed out")
	}
	return nil
}

func (peer *nativeRPCTestPeer) write(t *testing.T, frame string) {
	t.Helper()
	if _, err := peer.events.Write([]byte(frame + "\n")); err != nil {
		t.Fatal(err)
	}
}

func nativeRPCField(t *testing.T, object map[string]json.RawMessage, key string) string {
	t.Helper()
	var value string
	if json.Unmarshal(object[key], &value) != nil {
		t.Fatalf("%s = %s", key, object[key])
	}
	return value
}

func waitNativeRPCDone(t *testing.T, rpc *nativeRPC) {
	t.Helper()
	select {
	case <-rpc.Done():
	case <-nativeRPCTestContext(t).Done():
		t.Fatal("waiting for OMP native RPC shutdown timed out")
	}
}

func waitNativeRPCStats(t *testing.T, rpc *nativeRPC, want nativeRPCStats) {
	t.Helper()
	ctx := nativeRPCTestContext(t)
	for rpc.Stats() != want {
		select {
		case <-ctx.Done():
			t.Fatalf("OMP native RPC stats = %#v, want %#v", rpc.Stats(), want)
		default:
			runtime.Gosched()
		}
	}
}

func TestNativeRPCOutOfOrderResponsesAndNativeError(t *testing.T) {
	rpc, peer := newNativeRPCTest(t, nil, nativeRPCLimits{})
	type value struct {
		Value string `json:"value"`
	}
	type outcome struct {
		value value
		err   error
	}
	results := make(chan outcome, 2)
	for range 2 {
		go func() {
			var result value
			err := rpc.Call(nativeRPCTestContext(t), "get_state", nil, &result)
			results <- outcome{result, err}
		}()
	}
	first, second := peer.read(t), peer.read(t)
	firstID, secondID := nativeRPCField(t, first, "id"), nativeRPCField(t, second, "id")
	if firstID != "omp:2" || secondID != "omp:3" {
		t.Fatalf("request IDs = %q, %q", firstID, secondID)
	}
	peer.write(t, `{"id":"`+secondID+`","type":"response","command":"get_state","success":true,"data":{"value":"second"}}`)
	peer.write(t, `{"id":"`+firstID+`","type":"response","command":"get_state","success":true,"data":{"value":"first"}}`)
	seen := map[string]bool{}
	for range 2 {
		got := <-results
		if got.err != nil {
			t.Fatal(got.err)
		}
		seen[got.value.Value] = true
	}
	if !seen["first"] || !seen["second"] {
		t.Fatalf("results = %#v", seen)
	}

	nativeError := make(chan error, 1)
	go func() {
		var history json.RawMessage
		nativeError <- rpc.Call(nativeRPCTestContext(t), "get_entries", map[string]any{"since": "gone"}, &history)
	}()
	request := peer.read(t)
	peer.write(t, `{"id":"`+nativeRPCField(t, request, "id")+`","type":"response","command":"get_entries","success":false,"error":"Entry not found: gone"}`)
	err := <-nativeError
	var rejected *nativeRPCError
	if !errors.As(err, &rejected) || rejected.Command != "get_entries" || rejected.Message != "Entry not found: gone" {
		t.Fatalf("native error = %#v", err)
	}

	emptyNativeError := make(chan error, 1)
	go func() { emptyNativeError <- rpc.Call(nativeRPCTestContext(t), "abort", nil, nil) }()
	request = peer.read(t)
	peer.write(t, `{"id":"`+nativeRPCField(t, request, "id")+`","type":"response","command":"abort","success":false,"error":""}`)
	err = <-emptyNativeError
	rejected = nil
	if !errors.As(err, &rejected) || rejected.Command != "abort" || rejected.Message != "" || err.Error() != "OMP native abort failed" {
		t.Fatalf("empty native error = %#v", err)
	}
	waitNativeRPCStats(t, rpc, nativeRPCStats{})
}

func TestNativeRPCPromptKeepsCorrelationForLateNativeFailure(t *testing.T) {
	events := make(chan string, 1)
	rpc, peer := newNativeRPCTest(t, func(raw json.RawMessage) error {
		var event struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(raw, &event); err != nil {
			return err
		}
		events <- event.Type
		return nil
	}, nativeRPCLimits{})
	type started struct {
		prompt    *nativePrompt
		submitted bool
		err       error
	}
	result := make(chan started, 1)
	go func() {
		prompt, submitted, err := rpc.StartPrompt(nativeRPCTestContext(t), "owned prompt")
		result <- started{prompt, submitted, err}
	}()
	request := peer.read(t)
	id := nativeRPCField(t, request, "id")
	if id != "omp:2" || nativeRPCField(t, request, "type") != "prompt" || nativeRPCField(t, request, "message") != "owned prompt" {
		t.Fatalf("prompt command = %#v", request)
	}
	peer.write(t, `{"id":"`+id+`","type":"response","command":"prompt","success":true}`)
	got := <-result
	if got.err != nil || !got.submitted || got.prompt == nil {
		t.Fatalf("prompt start = %#v", got)
	}
	if invoked, known := got.prompt.ImmediateAgentInvoked(); invoked || known {
		t.Fatalf("omitted immediate admission = %v, %v", invoked, known)
	}
	peer.write(t, `{"type":"prompt_result","id":"`+id+`","agentInvoked":false}`)
	if event := <-events; event != "prompt_result" {
		t.Fatalf("prompt event = %q", event)
	}
	peer.write(t, `{"id":"`+id+`","type":"response","command":"prompt","success":false,"error":"late failure"}`)
	<-got.prompt.Late()
	err := got.prompt.Finish()
	var nativeErr *nativeRPCError
	if !errors.As(err, &nativeErr) || nativeErr.Command != "prompt" || nativeErr.Message != "late failure" {
		t.Fatalf("late prompt error = %#v", err)
	}
	if repeat := got.prompt.Finish(); repeat == nil || repeat.Error() != err.Error() {
		t.Fatalf("repeated finish = %v, want %v", repeat, err)
	}
	waitNativeRPCStats(t, rpc, nativeRPCStats{})
}

func TestNativeRPCPromptPreservesTypedImmediateAgentOutcome(t *testing.T) {
	for name, test := range map[string]struct {
		data    string
		invoked bool
		known   bool
		valid   bool
	}{
		"true":          {data: `{"agentInvoked":true}`, invoked: true, known: true, valid: true},
		"false":         {data: `{"agentInvoked":false}`, known: true, valid: true},
		"missing field": {data: `{}`},
		"null field":    {data: `{"agentInvoked":null}`},
		"wrong type":    {data: `{"agentInvoked":"false"}`},
		"extra field":   {data: `{"agentInvoked":true,"extra":1}`},
		"null data":     {data: `null`},
	} {
		t.Run(name, func(t *testing.T) {
			rpc, peer := newNativeRPCTest(t, nil, nativeRPCLimits{})
			type started struct {
				prompt    *nativePrompt
				submitted bool
				err       error
			}
			result := make(chan started, 1)
			go func() {
				prompt, submitted, err := rpc.StartPrompt(nativeRPCTestContext(t), "owned")
				result <- started{prompt, submitted, err}
			}()
			request := peer.read(t)
			id := nativeRPCField(t, request, "id")
			peer.write(t, `{"id":"`+id+`","type":"response","command":"prompt","success":true,"data":`+test.data+`}`)
			got := <-result
			if !got.submitted {
				t.Fatal("immediate prompt response was not submitted")
			}
			if !test.valid {
				if got.err == nil || got.prompt != nil {
					t.Fatalf("invalid prompt data = %#v", got)
				}
				waitNativeRPCStats(t, rpc, nativeRPCStats{})
				return
			}
			if got.err != nil || got.prompt == nil {
				t.Fatalf("valid prompt data = %#v", got)
			}
			if invoked, known := got.prompt.ImmediateAgentInvoked(); invoked != test.invoked || known != test.known {
				t.Fatalf("immediate admission = %v, %v; want %v, %v", invoked, known, test.invoked, test.known)
			}
			if err := got.prompt.Finish(); err != nil {
				t.Fatal(err)
			}
			waitNativeRPCStats(t, rpc, nativeRPCStats{})
		})
	}
}

func TestNativeRPCInvalidPromptStartReleasesAlreadyAcceptedLateError(t *testing.T) {
	rpc, peer := newNativeRPCTest(t, nil, nativeRPCLimits{})
	id, pending, write, err := rpc.admitPrompt(nativeRPCTestContext(t), map[string]any{"message": "owned"})
	if err != nil {
		t.Fatal(err)
	}
	request := peer.read(t)
	if got := nativeRPCField(t, request, "id"); got != id {
		t.Fatalf("prompt id = %q, want %q", got, id)
	}
	if err = waitNativeRPCWrite(nativeRPCTestContext(t), write); err != nil {
		t.Fatal(err)
	}

	// Hold the initial result in its buffered channel while the reader accepts
	// the legal late error for the same prompt correlation.
	peer.write(t, `{"id":"`+id+`","type":"response","command":"prompt","success":true,"data":{}}`)
	peer.write(t, `{"id":"`+id+`","type":"response","command":"prompt","success":false,"error":"late failure"}`)
	waitNativeRPCPromptState(t, rpc, pending, true, true)

	completed := <-pending.result
	prompt, submitted, err := rpc.completePromptStart(id, pending, completed)
	if !submitted || prompt != nil || err == nil || err.Error() != "invalid OMP native prompt response data" {
		t.Fatalf("invalid prompt start = prompt %#v, submitted %v, error %v", prompt, submitted, err)
	}
	waitNativeRPCStats(t, rpc, nativeRPCStats{})
}

func waitNativeRPCPromptState(t *testing.T, rpc *nativeRPC, pending *nativeRPCPending, responded, late bool) {
	t.Helper()
	ctx := nativeRPCTestContext(t)
	for {
		rpc.mu.Lock()
		gotResponded, gotLate := pending.responded, pending.lateSet
		rpc.mu.Unlock()
		if gotResponded == responded && gotLate == late {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("prompt state = responded %v, late %v; want %v, %v", gotResponded, gotLate, responded, late)
		default:
			runtime.Gosched()
		}
	}
}

func TestNativeRPCPromptTerminalFinishRejectsLaterResponse(t *testing.T) {
	rpc, peer := newNativeRPCTest(t, nil, nativeRPCLimits{})
	result := make(chan *nativePrompt, 1)
	go func() {
		prompt, _, err := rpc.StartPrompt(nativeRPCTestContext(t), "owned")
		if err != nil {
			t.Error(err)
		}
		result <- prompt
	}()
	request := peer.read(t)
	id := nativeRPCField(t, request, "id")
	peer.write(t, `{"id":"`+id+`","type":"response","command":"prompt","success":true}`)
	prompt := <-result
	if err := prompt.Finish(); err != nil {
		t.Fatal(err)
	}
	peer.write(t, `{"id":"`+id+`","type":"response","command":"prompt","success":false,"error":"after terminal"}`)
	waitNativeRPCDone(t, rpc)
	if !errors.Is(rpc.Err(), errNativeRPCProtocol) {
		t.Fatalf("post-terminal prompt response = %v", rpc.Err())
	}
}

func TestNativeRPCPromptFinishJoinsConcurrentTransportLoss(t *testing.T) {
	rpc, peer := newNativeRPCTest(t, nil, nativeRPCLimits{})
	started := make(chan *nativePrompt, 1)
	go func() {
		prompt, _, err := rpc.StartPrompt(nativeRPCTestContext(t), "owned")
		if err != nil {
			t.Error(err)
		}
		started <- prompt
	}()
	request := peer.read(t)
	peer.write(t, `{"id":"`+nativeRPCField(t, request, "id")+`","type":"response","command":"prompt","success":true}`)
	prompt := <-started
	if err := peer.events.Close(); err != nil {
		t.Fatal(err)
	}
	<-prompt.Late()
	results := make(chan error, 2)
	go func() { results <- prompt.Finish() }()
	go func() { results <- prompt.Finish() }()
	first, second := <-results, <-results
	if first == nil || second == nil || first.Error() != second.Error() {
		t.Fatalf("concurrent finish errors = %v, %v", first, second)
	}
	waitNativeRPCDone(t, rpc)
	waitNativeRPCStats(t, rpc, nativeRPCStats{})
}

func writeNativeRPCChunked(t *testing.T, peer *nativeRPCTestPeer, logical []byte, mutate func(index int, frame map[string]any)) {
	t.Helper()
	count := (len(logical) + nativeRPCChunkPayloadBytes - 1) / nativeRPCChunkPayloadBytes
	for index := range count {
		frame := nativeRPCChunkFrame(logical, index)
		if mutate != nil {
			mutate(index, frame)
		}
		body, err := json.Marshal(frame)
		if err != nil {
			t.Fatal(err)
		}
		peer.write(t, string(body))
	}
}

func nativeRPCChunkFrame(logical []byte, index int) map[string]any {
	count := (len(logical) + nativeRPCChunkPayloadBytes - 1) / nativeRPCChunkPayloadBytes
	start := index * nativeRPCChunkPayloadBytes
	end := min(len(logical), start+nativeRPCChunkPayloadBytes)
	return map[string]any{
		"type": "rpc_chunk", "chunkId": "owned-chunks", "index": index,
		"count": count, "byteLength": len(logical),
		"data": base64.StdEncoding.EncodeToString(logical[start:end]),
	}
}

func TestNativeRPCReassemblesProtocolTwoResponse(t *testing.T) {
	events := make(chan json.RawMessage, 1)
	rpc, peer := newNativeRPCTest(t, func(raw json.RawMessage) error {
		events <- append(json.RawMessage(nil), raw...)
		return nil
	}, nativeRPCLimits{})
	want := strings.Repeat("λ", 600_000)
	logical, err := json.Marshal(map[string]any{
		"type": "message_end", "message": map[string]any{"role": "assistant", "content": want},
	})
	if err != nil || len(logical) <= nativeRPCPhysicalFrameBytes {
		t.Fatalf("logical frame bytes = %d, error = %v", len(logical), err)
	}
	writeNativeRPCChunked(t, peer, logical, nil)
	var got struct {
		Type    string `json:"type"`
		Message struct {
			Content string `json:"content"`
		} `json:"message"`
	}
	if err = json.Unmarshal(<-events, &got); err != nil || got.Type != "message_end" || got.Message.Content != want {
		t.Fatalf("chunked event bytes = %d, error = %v", len(got.Message.Content), err)
	}
	waitNativeRPCStats(t, rpc, nativeRPCStats{})
}

func TestNativeRPCRejectsInvalidChunkSequences(t *testing.T) {
	logical := bytes.Repeat([]byte{'x'}, nativeRPCPhysicalFrameBytes+1)
	for name, test := range map[string]struct {
		frames int
		mutate func(int, map[string]any)
	}{
		"nonzero first": {frames: 1, mutate: func(index int, frame map[string]any) {
			if index == 0 {
				frame["index"] = 1
			}
		}},
		"changed identity": {frames: 2, mutate: func(index int, frame map[string]any) {
			if index == 1 {
				frame["chunkId"] = "foreign"
			}
		}},
		"short nonfinal": {frames: 1, mutate: func(index int, frame map[string]any) {
			if index == 0 {
				frame["data"] = base64.StdEncoding.EncodeToString([]byte("short"))
			}
		}},
		"bad base64": {frames: 1, mutate: func(index int, frame map[string]any) {
			if index == 0 {
				frame["data"] = "%%%"
			}
		}},
	} {
		t.Run(name, func(t *testing.T) {
			rpc, peer := newNativeRPCTest(t, nil, nativeRPCLimits{})
			for index := range test.frames {
				frame := nativeRPCChunkFrame(logical, index)
				test.mutate(index, frame)
				peer.write(t, string(mustNativeRPCJSON(t, frame)))
			}
			waitNativeRPCDone(t, rpc)
			if !errors.Is(rpc.Err(), errNativeRPCProtocol) {
				t.Fatalf("chunk sequence error = %v", rpc.Err())
			}
		})
	}

	t.Run("interrupted", func(t *testing.T) {
		rpc, peer := newNativeRPCTest(t, nil, nativeRPCLimits{})
		count := (len(logical) + nativeRPCChunkPayloadBytes - 1) / nativeRPCChunkPayloadBytes
		peer.write(t, string(mustNativeRPCJSON(t, map[string]any{
			"type": "rpc_chunk", "chunkId": "held", "index": 0, "count": count,
			"byteLength": len(logical), "data": base64.StdEncoding.EncodeToString(logical[:nativeRPCChunkPayloadBytes]),
		})))
		peer.write(t, `{"type":"agent_start"}`)
		waitNativeRPCDone(t, rpc)
		if !errors.Is(rpc.Err(), errNativeRPCProtocol) {
			t.Fatalf("interrupted chunk sequence = %v", rpc.Err())
		}
	})
}

func mustNativeRPCJSON(t *testing.T, value any) []byte {
	t.Helper()
	body, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func TestNativeRPCRejectedCommandDoesNotConsumeID(t *testing.T) {
	limits := defaultNativeRPCLimits
	limits.maxInputFrame = 256
	rpc, peer := newNativeRPCTest(t, nil, limits)
	if err := rpc.Call(nativeRPCTestContext(t), "set_session_name", map[string]any{"name": strings.Repeat("x", 300)}, nil); !errors.Is(err, errNativeRPCProtocol) {
		t.Fatalf("oversized call error = %v", err)
	}
	if err := rpc.Call(nativeRPCTestContext(t), "get_state", map[string]any{"id": "foreign"}, nil); !errors.Is(err, errNativeRPCProtocol) {
		t.Fatalf("reserved field error = %v", err)
	}
	result := make(chan error, 1)
	go func() { result <- rpc.Call(nativeRPCTestContext(t), "get_state", nil, nil) }()
	request := peer.read(t)
	if id := nativeRPCField(t, request, "id"); id != "omp:2" {
		t.Fatalf("request ID after local rejection = %q", id)
	}
	peer.write(t, `{"id":"omp:2","type":"response","command":"get_state","success":true,"data":{}}`)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
}

func TestNativeRPCUICancellationIsOneWayAndDoesNotConsumeCallID(t *testing.T) {
	rpc, peer := newNativeRPCTest(t, nil, nativeRPCLimits{})
	result := make(chan error, 1)
	go func() { result <- rpc.CancelUI(nativeRPCTestContext(t), "dialog opaque") }()
	request := peer.read(t)
	if len(request) != 3 || nativeRPCField(t, request, "type") != "extension_ui_response" ||
		nativeRPCField(t, request, "id") != "dialog opaque" || string(request["cancelled"]) != "true" {
		t.Fatalf("UI cancellation = %#v", request)
	}
	if err := <-result; err != nil {
		t.Fatal(err)
	}

	result = make(chan error, 1)
	go func() { result <- rpc.Call(nativeRPCTestContext(t), "get_state", nil, nil) }()
	request = peer.read(t)
	if id := nativeRPCField(t, request, "id"); id != "omp:2" {
		t.Fatalf("first correlated ID after UI cancellation = %q", id)
	}
	peer.write(t, `{"id":"omp:2","type":"response","command":"get_state","success":true,"data":{}}`)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	waitNativeRPCStats(t, rpc, nativeRPCStats{})
}

type observedCancellationContext struct {
	context.Context
	once    sync.Once
	checked chan struct{}
}

func (ctx *observedCancellationContext) Err() error {
	ctx.once.Do(func() { close(ctx.checked) })
	return ctx.Context.Err()
}

func TestNativeRPCCancellationBeforeAtomicAdmissionWritesNothing(t *testing.T) {
	for _, operation := range []string{"call", "cancel UI", "end input"} {
		t.Run(operation, func(t *testing.T) {
			rpc, peer := newNativeRPCTest(t, nil, nativeRPCLimits{})
			base, cancel := context.WithCancel(context.Background())
			cancelled := &observedCancellationContext{Context: base, checked: make(chan struct{})}
			rpc.outboundMu.Lock()
			result := make(chan error, 1)
			go func() {
				if operation == "call" {
					result <- rpc.Call(cancelled, "set_session_name", map[string]any{"name": "must not write"}, nil)
				} else if operation == "cancel UI" {
					result <- rpc.CancelUI(cancelled, "must-not-write")
				} else {
					result <- rpc.EndInput(cancelled)
				}
			}()
			<-cancelled.checked
			cancel()
			rpc.outboundMu.Unlock()
			err := <-result
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("cancelled %s error = %v", operation, err)
			}

			result = make(chan error, 1)
			go func() { result <- rpc.Call(nativeRPCTestContext(t), "get_state", nil, nil) }()
			request := peer.read(t)
			if id, kind := nativeRPCField(t, request, "id"), nativeRPCField(t, request, "type"); id != "omp:2" || kind != "get_state" {
				t.Fatalf("first admitted command = %q %q", id, kind)
			}
			peer.write(t, `{"id":"omp:2","type":"response","command":"get_state","success":true,"data":{}}`)
			if err := <-result; err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestNativeRPCReportsAtomicQueueAdmission(t *testing.T) {
	rpc, _ := newNativeRPCTest(t, nil, nativeRPCLimits{})
	base, cancel := context.WithCancel(context.Background())
	cancelled := &observedCancellationContext{Context: base, checked: make(chan struct{})}
	rpc.outboundMu.Lock()
	type outcome struct {
		submitted bool
		err       error
	}
	result := make(chan outcome, 1)
	go func() {
		submitted, err := rpc.callWithAdmission(cancelled, "prompt", map[string]any{"message": "must not write"}, nil)
		result <- outcome{submitted, err}
	}()
	<-cancelled.checked
	cancel()
	rpc.outboundMu.Unlock()
	got := <-result
	if got.submitted || !errors.Is(got.err, context.Canceled) {
		t.Fatalf("pre-admission cancellation = submitted %v, error %v", got.submitted, got.err)
	}
}

func TestNativeRPCCompletedWriteWinsSimultaneousCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	write := newNativeRPCWrite([]byte("owned"))
	write.complete(nil)
	cancel()
	if err := waitNativeRPCWrite(ctx, write); err != nil {
		t.Fatalf("completed write disposition = %v", err)
	}
	if write.body != nil {
		t.Fatalf("completed write retained %d bytes", len(write.body))
	}
}

func TestNativeRPCAcceptedResponseWinsSimultaneousCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	write := newNativeRPCWrite([]byte("held"))
	pending := &nativeRPCPending{result: make(chan nativeRPCResult, 1)}
	pending.result <- nativeRPCResult{data: json.RawMessage(`{"accepted":true}`)}
	completed, responded, err := waitNativeRPCAdmission(ctx, write, pending)
	if err != nil || !responded || string(completed.data) != `{"accepted":true}` {
		t.Fatalf("accepted response disposition = %s, %v, %v", completed.data, responded, err)
	}
}

func TestNativeRPCResponseViolationsRetireAndReleaseCaller(t *testing.T) {
	tests := map[string]string{
		"unknown":          `{"id":"omp:99","type":"response","command":"get_state","success":true,"data":{}}`,
		"command mismatch": `{"id":"omp:2","type":"response","command":"abort","success":true}`,
		"invalid shape":    `{"id":"omp:2","type":"response","command":"get_state","success":true,"error":"bad"}`,
		"null error":       `{"id":"omp:2","type":"response","command":"get_state","success":false,"error":null}`,
	}
	for name, frame := range tests {
		t.Run(name, func(t *testing.T) {
			rpc, peer := newNativeRPCTest(t, nil, nativeRPCLimits{})
			if name == "unknown" {
				peer.write(t, frame)
				waitNativeRPCDone(t, rpc)
			} else {
				result := make(chan error, 1)
				go func() { result <- rpc.Call(nativeRPCTestContext(t), "get_state", nil, nil) }()
				_ = peer.read(t)
				peer.write(t, frame)
				waitNativeRPCDone(t, rpc)
				if err := <-result; !errors.Is(err, errNativeRPCProtocol) {
					t.Fatalf("call error = %v", err)
				}
			}
			if !errors.Is(rpc.Err(), errNativeRPCProtocol) {
				t.Fatalf("RPC error = %v", rpc.Err())
			}
		})
	}
}

func TestNativeRPCDuplicateResponseRetiresAfterAcceptedResult(t *testing.T) {
	rpc, peer := newNativeRPCTest(t, nil, nativeRPCLimits{})
	result := make(chan error, 1)
	go func() { result <- rpc.Call(nativeRPCTestContext(t), "abort", nil, nil) }()
	request := peer.read(t)
	id := nativeRPCField(t, request, "id")
	frame := `{"id":"` + id + `","type":"response","command":"abort","success":true}`
	peer.write(t, frame)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	peer.write(t, frame)
	waitNativeRPCDone(t, rpc)
	if !errors.Is(rpc.Err(), errNativeRPCProtocol) {
		t.Fatalf("RPC error = %v", rpc.Err())
	}
}

func TestNativeRPCPostWriteCancellationDrainsLateResponse(t *testing.T) {
	rpc, peer := newNativeRPCTest(t, nil, nativeRPCLimits{})
	callCtx, cancel := context.WithCancel(nativeRPCTestContext(t))
	result := make(chan error, 1)
	go func() { result <- rpc.Call(callCtx, "get_state", nil, nil) }()
	first := peer.read(t)
	ctx := nativeRPCTestContext(t)
	for rpc.Stats().pendingWrites != 0 {
		select {
		case <-ctx.Done():
			t.Fatal("OMP command write did not settle")
		default:
			runtime.Gosched()
		}
	}
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled call error = %v", err)
	}
	peer.write(t, `{"id":"`+nativeRPCField(t, first, "id")+`","type":"response","command":"get_state","success":true,"data":{}}`)

	healthy := make(chan error, 1)
	go func() { healthy <- rpc.Call(nativeRPCTestContext(t), "abort", nil, nil) }()
	second := peer.read(t)
	peer.write(t, `{"id":"`+nativeRPCField(t, second, "id")+`","type":"response","command":"abort","success":true}`)
	if err := <-healthy; err != nil {
		t.Fatal(err)
	}
	waitNativeRPCStats(t, rpc, nativeRPCStats{})
}

func TestNativeRPCPendingCapacityRejectsBeforeWrite(t *testing.T) {
	limits := defaultNativeRPCLimits
	limits.maxPendingCalls = 1
	rpc, peer := newNativeRPCTest(t, nil, limits)
	first := make(chan error, 1)
	go func() { first <- rpc.Call(nativeRPCTestContext(t), "get_state", nil, nil) }()
	request := peer.read(t)
	if err := rpc.Call(nativeRPCTestContext(t), "abort", nil, nil); !errors.Is(err, errNativeRPCBusy) {
		t.Fatalf("second call error = %v", err)
	}
	if stats := rpc.Stats(); stats.pendingCalls != 1 {
		t.Fatalf("stats after rejected call = %#v", stats)
	}
	peer.write(t, `{"id":"`+nativeRPCField(t, request, "id")+`","type":"response","command":"get_state","success":true,"data":{}}`)
	if err := <-first; err != nil {
		t.Fatal(err)
	}
	waitNativeRPCStats(t, rpc, nativeRPCStats{})
}

type heldNativeRPCWriter struct {
	io.WriteCloser
	enabled atomic.Bool
	held    atomic.Bool
	wrote   chan struct{}
	release chan struct{}
}

func (writer *heldNativeRPCWriter) Write(body []byte) (int, error) {
	n, err := writer.WriteCloser.Write(body)
	if writer.enabled.Load() && writer.held.CompareAndSwap(false, true) {
		close(writer.wrote)
		<-writer.release
	}
	return n, err
}

func newHeldNativeRPCTest(t *testing.T, limits nativeRPCLimits) (*nativeRPC, *nativeRPCTestPeer, *heldNativeRPCWriter, func()) {
	t.Helper()
	commands, input := io.Pipe()
	output, events := io.Pipe()
	held := &heldNativeRPCWriter{WriteCloser: input, wrote: make(chan struct{}), release: make(chan struct{})}
	rpc, err := newNativeRPC(held, output, nil, limits)
	if err != nil {
		t.Fatal(err)
	}
	peer := &nativeRPCTestPeer{commands: bufio.NewReader(commands), events: events}
	readyNativeRPCTest(t, rpc, peer)
	// Ready may return before negotiation's Write callback. Settle that write
	// before callers arm enabled, or the hold can capture negotiation instead
	// of the operation whose admission the test intends to observe.
	waitNativeRPCStats(t, rpc, nativeRPCStats{})
	var once sync.Once
	release := func() { once.Do(func() { close(held.release) }) }
	t.Cleanup(func() {
		release()
		_ = commands.Close()
		_ = events.Close()
		_ = rpc.Close()
	})
	return rpc, peer, held, release
}

func TestNativeRPCAcceptedResponseCompletesBeforeWriteCallback(t *testing.T) {
	rpc, peer, held, release := newHeldNativeRPCTest(t, nativeRPCLimits{})
	held.enabled.Store(true)
	callCtx := nativeRPCTestContext(t)
	type state struct {
		SessionID string `json:"sessionId"`
	}
	result := make(chan struct {
		state state
		err   error
	}, 1)
	go func() {
		var value state
		err := rpc.Call(callCtx, "get_state", nil, &value)
		result <- struct {
			state state
			err   error
		}{value, err}
	}()
	request := peer.read(t)
	<-held.wrote
	peer.write(t, `{"id":"`+nativeRPCField(t, request, "id")+`","type":"response","command":"get_state","success":true,"data":{"sessionId":"owned"}}`)
	got := <-result
	if got.err != nil || got.state.SessionID != "owned" {
		t.Fatalf("accepted result = %#v, %v", got.state, got.err)
	}
	if stats := rpc.Stats(); stats.pendingWrites != 1 || stats.retainedBytes == 0 {
		t.Fatalf("held write ownership after accepted response = %#v", stats)
	}
	release()
	waitNativeRPCStats(t, rpc, nativeRPCStats{})
}

func TestNativeRPCEndInputQueuesBehindAcceptedResponseWrite(t *testing.T) {
	rpc, peer, held, release := newHeldNativeRPCTest(t, nativeRPCLimits{})
	held.enabled.Store(true)
	call := make(chan error, 1)
	go func() { call <- rpc.Call(nativeRPCTestContext(t), "get_state", nil, nil) }()
	request := peer.read(t)
	<-held.wrote
	peer.write(t, `{"id":"`+nativeRPCField(t, request, "id")+`","type":"response","command":"get_state","success":true,"data":{}}`)
	if err := <-call; err != nil {
		t.Fatal(err)
	}
	if stats := rpc.Stats(); stats.pendingWrites != 1 || stats.retainedBytes == 0 {
		t.Fatalf("accepted call write ownership = %#v", stats)
	}

	ended := make(chan error, 1)
	go func() { ended <- rpc.EndInput(nativeRPCTestContext(t)) }()
	select {
	case err := <-ended:
		t.Fatalf("EndInput returned before the earlier write callback: %v", err)
	default:
	}
	release()
	if err := <-ended; err != nil {
		t.Fatal(err)
	}
	if _, err := peer.commands.ReadByte(); !errors.Is(err, io.EOF) {
		t.Fatalf("native stdin after queued end = %v", err)
	}
	if err := peer.events.Close(); err != nil {
		t.Fatal(err)
	}
	waitNativeRPCDone(t, rpc)
	if err := rpc.Err(); err != nil {
		t.Fatalf("queued orderly end = %v", err)
	}
	waitNativeRPCStats(t, rpc, nativeRPCStats{})
}

func TestNativeRPCUICancellationDuringUnsettledWriteRetires(t *testing.T) {
	rpc, peer, held, release := newHeldNativeRPCTest(t, nativeRPCLimits{})
	held.enabled.Store(true)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	result := make(chan error, 1)
	go func() { result <- rpc.CancelUI(ctx, "dialog") }()
	request := peer.read(t)
	if nativeRPCField(t, request, "type") != "extension_ui_response" {
		t.Fatalf("UI cancellation = %#v", request)
	}
	<-held.wrote
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("unsettled UI write cancellation = %v", err)
	}
	release()
	waitNativeRPCDone(t, rpc)
	waitNativeRPCStats(t, rpc, nativeRPCStats{})
}

func TestNativeRPCHeldUICancellationDoesNotOwnAdmissionLock(t *testing.T) {
	for _, operation := range []string{"call", "end input"} {
		t.Run(operation, func(t *testing.T) {
			rpc, peer, held, release := newHeldNativeRPCTest(t, nativeRPCLimits{})
			held.enabled.Store(true)
			ui := make(chan error, 1)
			go func() { ui <- rpc.CancelUI(nativeRPCTestContext(t), "held-dialog") }()
			request := peer.read(t)
			if nativeRPCField(t, request, "type") != "extension_ui_response" {
				t.Fatalf("UI cancellation = %#v", request)
			}
			<-held.wrote

			// Hold the next atomic admission boundary after the UI frame has
			// entered the writer. Cancel the independent operation there. A UI
			// waiter which still owned outboundMu would deadlock this lock.
			rpc.outboundMu.Lock()
			base, cancel := context.WithCancel(context.Background())
			cancelled := &observedCancellationContext{Context: base, checked: make(chan struct{})}
			result := make(chan error, 1)
			go func() {
				if operation == "call" {
					result <- rpc.Call(cancelled, "get_state", nil, nil)
				} else {
					result <- rpc.EndInput(cancelled)
				}
			}()
			<-cancelled.checked
			cancel()
			rpc.outboundMu.Unlock()
			if err := <-result; !errors.Is(err, context.Canceled) {
				t.Fatalf("independent %s cancellation = %v", operation, err)
			}
			if stats := rpc.Stats(); stats.pendingCalls != 0 || stats.pendingWrites != 1 {
				t.Fatalf("ownership before UI release = %#v", stats)
			}

			release()
			if err := <-ui; err != nil {
				t.Fatalf("held UI cancellation = %v", err)
			}
			call := make(chan error, 1)
			go func() { call <- rpc.Call(nativeRPCTestContext(t), "get_state", nil, nil) }()
			request = peer.read(t)
			if id := nativeRPCField(t, request, "id"); id != "omp:2" {
				t.Fatalf("first ID after canceled admission = %q", id)
			}
			peer.write(t, `{"id":"omp:2","type":"response","command":"get_state","success":true,"data":{}}`)
			if err := <-call; err != nil {
				t.Fatal(err)
			}
			waitNativeRPCStats(t, rpc, nativeRPCStats{})
		})
	}
}

func TestNativeRPCCancellationDuringPartialWriteRetiresAndJoins(t *testing.T) {
	rpc, peer := newNativeRPCTest(t, nil, nativeRPCLimits{})
	callCtx, cancel := context.WithCancel(nativeRPCTestContext(t))
	result := make(chan error, 1)
	go func() {
		result <- rpc.Call(callCtx, "set_session_name", map[string]any{"name": strings.Repeat("x", 900_000)}, nil)
	}()
	read := make(chan struct{})
	go func() {
		buffer := make([]byte, 128)
		_, _ = peer.commands.Read(buffer)
		close(read)
	}()
	<-read
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("partial-write cancellation error = %v", err)
	}
	waitNativeRPCDone(t, rpc)
	waitNativeRPCStats(t, rpc, nativeRPCStats{})
}

func TestNativeRPCAggregateRetainedCapacityReleasesCaller(t *testing.T) {
	limits := defaultNativeRPCLimits
	limits.maxInputFrame = 512
	limits.maxPhysicalFrame = 512
	limits.maxLogicalFrame = 512
	limits.maxRetainedBytes = 512
	rpc, peer, held, release := newHeldNativeRPCTest(t, limits)
	held.enabled.Store(true)
	result := make(chan error, 1)
	go func() {
		result <- rpc.Call(nativeRPCTestContext(t), "set_session_name", map[string]any{"name": strings.Repeat("x", 180)}, nil)
	}()
	request := peer.read(t)
	<-held.wrote
	peer.write(t, `{"id":"`+nativeRPCField(t, request, "id")+`","type":"response","command":"set_session_name","success":true,"data":{"padding":"`+strings.Repeat("y", 340)+`"}}`)
	if err := <-result; !errors.Is(err, errNativeRPCBusy) {
		t.Fatalf("aggregate capacity error = %v", err)
	}
	release()
	waitNativeRPCDone(t, rpc)
	waitNativeRPCStats(t, rpc, nativeRPCStats{})
}

func TestNativeRPCEventsAreObservedAndObserverFailureRetires(t *testing.T) {
	events := make(chan string, 2)
	rpc, peer := newNativeRPCTest(t, func(raw json.RawMessage) error {
		var event struct {
			Type string `json:"type"`
		}
		if json.Unmarshal(raw, &event) != nil {
			return errors.New("bad event")
		}
		events <- event.Type
		if event.Type == "extension_ui_request" {
			return errors.New("unattended dialog")
		}
		return nil
	}, nativeRPCLimits{})
	peer.write(t, `{"type":"agent_start"}`)
	if event := <-events; event != "agent_start" {
		t.Fatalf("event = %q", event)
	}
	peer.write(t, `{"type":"extension_ui_request","id":"dialog","method":"confirm"}`)
	waitNativeRPCDone(t, rpc)
	if rpc.Err() == nil || rpc.Err().Error() != "unattended dialog" {
		t.Fatalf("observer failure = %v", rpc.Err())
	}
}

func TestNativeRPCEndInputRequiresSettledCallsAndAcceptsCleanEOF(t *testing.T) {
	rpc, peer := newNativeRPCTest(t, nil, nativeRPCLimits{})
	result := make(chan error, 1)
	go func() { result <- rpc.Call(nativeRPCTestContext(t), "get_state", nil, nil) }()
	request := peer.read(t)
	if err := rpc.EndInput(nativeRPCTestContext(t)); !errors.Is(err, errNativeRPCBusy) {
		t.Fatalf("EndInput with pending call = %v", err)
	}
	peer.write(t, `{"id":"`+nativeRPCField(t, request, "id")+`","type":"response","command":"get_state","success":true,"data":{}}`)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	if err := rpc.EndInput(nativeRPCTestContext(t)); err != nil {
		t.Fatal(err)
	}
	eof := make(chan error, 1)
	go func() {
		_, err := peer.commands.ReadByte()
		eof <- err
	}()
	if err := <-eof; !errors.Is(err, io.EOF) {
		t.Fatalf("native stdin end = %v", err)
	}
	if err := peer.events.Close(); err != nil {
		t.Fatal(err)
	}
	waitNativeRPCDone(t, rpc)
	if err := rpc.Err(); err != nil {
		t.Fatalf("clean RPC shutdown = %v", err)
	}
}

type heldNativeRPCCloseWriter struct {
	*io.PipeWriter
	held    atomic.Bool
	closed  chan struct{}
	release chan struct{}
	err     error
}

func (writer *heldNativeRPCCloseWriter) Close() error {
	err := writer.PipeWriter.Close()
	if writer.held.CompareAndSwap(false, true) {
		close(writer.closed)
		<-writer.release
		if writer.err != nil {
			return writer.err
		}
	}
	return err
}

func TestNativeRPCOrderlyOutputEOFJoinsInputCloseCompletion(t *testing.T) {
	closeFailure := errors.New("held stdin close failed")
	for _, test := range []struct {
		name string
		err  error
	}{
		{name: "success"},
		{name: "failure", err: closeFailure},
	} {
		t.Run(test.name, func(t *testing.T) {
			commands, input := io.Pipe()
			output, events := io.Pipe()
			held := &heldNativeRPCCloseWriter{
				PipeWriter: input, closed: make(chan struct{}), release: make(chan struct{}), err: test.err,
			}
			rpc, err := newNativeRPC(held, output, nil, nativeRPCLimits{})
			if err != nil {
				t.Fatal(err)
			}
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(held.release) }) }
			t.Cleanup(func() {
				release()
				_ = commands.Close()
				_ = events.Close()
				_ = rpc.Close()
			})

			endResult := make(chan error, 1)
			go func() { endResult <- rpc.EndInput(nativeRPCTestContext(t)) }()
			<-held.closed
			if _, err := commands.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
				t.Fatalf("native stdin = %v", err)
			}
			if err := events.Close(); err != nil {
				t.Fatal(err)
			}
			select {
			case <-rpc.Done():
				t.Fatal("RPC completed before stdin Close returned")
			default:
			}
			release()
			endErr := <-endResult
			waitNativeRPCDone(t, rpc)
			if test.err == nil {
				if endErr != nil || rpc.Err() != nil {
					t.Fatalf("orderly close = %v, RPC = %v", endErr, rpc.Err())
				}
			} else {
				if !errors.Is(endErr, closeFailure) || !errors.Is(rpc.Err(), closeFailure) {
					t.Fatalf("failed close = %v, RPC = %v", endErr, rpc.Err())
				}
			}
		})
	}
}

func TestNativeRPCRejectsPartialCRLFAndOversizedOutput(t *testing.T) {
	tests := map[string]struct {
		body   string
		close  bool
		limits nativeRPCLimits
	}{
		"partial EOF":   {body: `{"type":"agent_start"}`, close: true},
		"CRLF":          {body: "{\"type\":\"agent_start\"}\r\n"},
		"trailing JSON": {body: "{\"type\":\"agent_start\"}{}\n"},
		"oversized": {
			body: `{"type":"event","padding":"` + strings.Repeat("x", 300) + "\"}\n",
			limits: nativeRPCLimits{
				maxInputFrame: 256, maxPhysicalFrame: 256, maxPendingCalls: 1,
				maxLogicalFrame: 256, maxPendingWrites: 1, maxRetainedBytes: 256,
			},
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			rpc, peer := newNativeRPCTest(t, nil, test.limits)
			if _, err := peer.events.Write([]byte(test.body)); err != nil {
				t.Fatal(err)
			}
			if test.close {
				_ = peer.events.Close()
			}
			waitNativeRPCDone(t, rpc)
			if !errors.Is(rpc.Err(), errNativeRPCProtocol) {
				t.Fatalf("RPC error = %v", rpc.Err())
			}
		})
	}
}

// OMP 18.11.0 answers a prompt it refuses before admission (AgentBusyError
// while its own work runs) with an error response, then a success response
// for the same id, then prompt_result agentInvoked:false (rpc-prompt-results.ts
// :286-296, rpc-mode.ts:1835-1882). Before this change the success was an
// unknown response id, so a busy refusal stopped the transport and lost the lane.
func TestNativeRPCRefusedPromptAbsorbsItsTrailingSuccessAndNoAgentResult(t *testing.T) {
	refuse := func(t *testing.T, rpc *nativeRPC, peer *nativeRPCTestPeer) string {
		t.Helper()
		result := make(chan error, 1)
		go func() {
			prompt, submitted, err := rpc.StartPrompt(nativeRPCTestContext(t), "owned prompt")
			if prompt != nil || !submitted {
				err = errors.Join(err, errors.New("refused prompt returned an owned correlation"))
			}
			result <- err
		}()
		id := nativeRPCField(t, peer.read(t), "id")
		peer.write(t, `{"id":"`+id+`","type":"response","command":"prompt","success":false,"error":"Agent is busy"}`)
		err := <-result
		var nativeErr *nativeRPCError
		if !errors.As(err, &nativeErr) || nativeErr.Command != "prompt" || nativeErr.Message != "Agent is busy" {
			t.Fatalf("refused prompt start = %#v", err)
		}
		return id
	}
	observer := func() (func(json.RawMessage) error, func() []string) {
		var mu sync.Mutex
		var seen []string
		return func(raw json.RawMessage) error {
				mu.Lock()
				seen = append(seen, string(raw))
				mu.Unlock()
				return nil
			}, func() []string {
				mu.Lock()
				defer mu.Unlock()
				return slices.Clone(seen)
			}
	}
	t.Run("absorbed once and the next prompt runs", func(t *testing.T) {
		observe, seen := observer()
		rpc, peer := newNativeRPCTest(t, observe, nativeRPCLimits{})
		id := refuse(t, rpc, peer)
		peer.write(t, `{"id":"`+id+`","type":"response","command":"prompt","success":true}`)
		peer.write(t, `{"type":"prompt_result","id":"`+id+`","agentInvoked":false,"status":"error","sessionSettled":false,"error":{"message":"Agent is busy","retryable":false}}`)
		peer.write(t, `{"type":"agent_end","marker":"after"}`)
		ctx := nativeRPCTestContext(t)
		for len(seen()) == 0 {
			select {
			case <-ctx.Done():
				t.Fatal("event after the refusal was not observed")
			default:
				runtime.Gosched()
			}
		}
		if got := seen(); len(got) != 1 || got[0] != `{"type":"agent_end","marker":"after"}` {
			t.Fatalf("observed after refusal = %q", got)
		}
		started := make(chan *nativePrompt, 1)
		go func() {
			prompt, _, err := rpc.StartPrompt(nativeRPCTestContext(t), "next prompt")
			if err != nil {
				t.Error(err)
			}
			started <- prompt
		}()
		next := nativeRPCField(t, peer.read(t), "id")
		if next == id {
			t.Fatalf("next prompt reused refused id %q", id)
		}
		peer.write(t, `{"id":"`+next+`","type":"response","command":"prompt","success":true}`)
		prompt := <-started
		if prompt == nil || rpc.Err() != nil {
			t.Fatalf("next prompt = %#v, transport %v", prompt, rpc.Err())
		}
		if err := prompt.Finish(); err != nil {
			t.Fatal(err)
		}
		waitNativeRPCStats(t, rpc, nativeRPCStats{})
	})
	t.Run("a second success for the refused id still stops", func(t *testing.T) {
		rpc, peer := newNativeRPCTest(t, nil, nativeRPCLimits{})
		id := refuse(t, rpc, peer)
		peer.write(t, `{"id":"`+id+`","type":"response","command":"prompt","success":true}`)
		peer.write(t, `{"id":"`+id+`","type":"response","command":"prompt","success":true}`)
		waitNativeRPCDone(t, rpc)
		if err := rpc.Err(); !errors.Is(err, errNativeRPCProtocol) || !strings.Contains(err.Error(), "unknown or duplicate response id") {
			t.Fatalf("second trailing success = %v", err)
		}
	})
	t.Run("an agent-invoked result for the refused id is not absorbed", func(t *testing.T) {
		observe, seen := observer()
		rpc, peer := newNativeRPCTest(t, observe, nativeRPCLimits{})
		id := refuse(t, rpc, peer)
		frame := `{"type":"prompt_result","id":"` + id + `","agentInvoked":true,"status":"completed","sessionSettled":true}`
		peer.write(t, frame)
		ctx := nativeRPCTestContext(t)
		for len(seen()) == 0 {
			select {
			case <-ctx.Done():
				t.Fatal("agent-invoked prompt_result was absorbed")
			default:
				runtime.Gosched()
			}
		}
		if got := seen(); got[0] != frame {
			t.Fatalf("observed = %q", got)
		}
	})
	t.Run("the next admitted prompt clears the slot", func(t *testing.T) {
		rpc, peer := newNativeRPCTest(t, nil, nativeRPCLimits{})
		id := refuse(t, rpc, peer)
		// The error-only setup failure writes nothing more for the refused id.
		started := make(chan *nativePrompt, 1)
		go func() {
			prompt, _, err := rpc.StartPrompt(nativeRPCTestContext(t), "next prompt")
			if err != nil {
				t.Error(err)
			}
			started <- prompt
		}()
		next := nativeRPCField(t, peer.read(t), "id")
		peer.write(t, `{"id":"`+next+`","type":"response","command":"prompt","success":true}`)
		if prompt := <-started; prompt == nil {
			t.Fatal("next prompt was not admitted")
		}
		peer.write(t, `{"id":"`+id+`","type":"response","command":"prompt","success":true}`)
		waitNativeRPCDone(t, rpc)
		if err := rpc.Err(); !errors.Is(err, errNativeRPCProtocol) || !strings.Contains(err.Error(), "unknown or duplicate response id") {
			t.Fatalf("success after the slot cleared = %v", err)
		}
	})
}
