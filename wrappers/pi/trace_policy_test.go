// SPDX-License-Identifier: MIT

package pi

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func TestInteractiveSpawnResultPreservesTrace(t *testing.T) {
	listener := interactiveBusListener(t)
	state := &interactiveNativeFixture{}
	state.set("native-trace", "", "/work")
	owner, native := interactiveOwnerPair(t, listener.Addr().String(), t.TempDir(), "", []string{"test"}, state)
	_, conn, scanner := interactiveReady(t, native, listener, interactiveReadyRequest{interactiveTopology, owner.directory, "native-trace", ""})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, action := range []string{"fresh", "resume"} {
		args := json.RawMessage(`{"name":"child","product":"fixture-worker","open":{}}`)
		if action == "resume" {
			args = json.RawMessage(`{"resume_session_id":"child@local"}`)
		}
		var response struct {
			SessionID string          `json:"session_id"`
			CallID    string          `json:"call_id"`
			Result    json.RawMessage `json:"result"`
		}
		done := make(chan error, 1)
		go func() {
			done <- native.Call(ctx, "tool.call", interactiveToolRequest{SessionID: "native-trace", CallID: action, Action: "spawn", Arguments: args}, &response)
		}()
		call := interactiveFrame(t, scanner)
		if call.Method != "lane.spawn" {
			t.Fatalf("public method = %s", call.Method)
		}
		want := json.RawMessage(`{"session_id":"child@local","policy":{"persistent":false,"auto_close_ms":60000,"notify":true,"trace":"events"}}`)
		// Raw daemon bytes exercise the pinned SDK's response decoder as well
		// as the native bridge; a permissive mock Caller would miss this break.
		wire, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": call.ID, "result": want})
		if err != nil {
			t.Fatal(err)
		}
		interactiveWrite(t, conn, append(wire, '\n'))
		if err = awaitInteractiveError(t, done, action+" response"); err != nil {
			t.Fatal(err)
		}
		if response.SessionID != "native-trace" || response.CallID != action || string(response.Result) != string(want) {
			t.Fatalf("bridge response = %+v", response)
		}
	}
}
