// SPDX-License-Identifier: MIT

package omp

import (
	"context"
	"encoding/json"
	"testing"

	kit "github.com/antst/sessionbus/bus/sdk/go"
)

func TestOwnerRegistrySpawnResultPreservesTrace(t *testing.T) {
	want := json.RawMessage(`{"session_id":"child@local","policy":{"persistent":false,"auto_close_ms":60000,"notify":true,"trace":"content"}}`)
	caller := kit.NewCaller(func(_ context.Context, method string, _ any) (json.RawMessage, error) {
		if method != "lane.spawn" {
			t.Errorf("public method = %s", method)
		}
		return want, nil
	})
	directory := t.TempDir()
	listener := ownerBusListener(t)
	fixture := &ownerNativeFixture{descriptions: map[string]ownerDescribeResult{
		"main-token": {OwnerToken: "main-token", SessionID: "main-session", CWD: "/work"},
	}}
	_, native := ownerRegistryPair(t, OwnerRegistryOptions{
		Topology: ownerTopologyLane, Socket: listener.Addr().String(), Directory: directory, PrimaryCaller: caller,
	}, fixture)
	if err := native.Call(ownerTestContext(t), "owner.ready", ownerReadyRequest{
		Topology: ownerTopologyLane, Directory: directory, Scope: ownerScopePrimary,
		Mode: ownerModeRPC, OwnerToken: "main-token", SessionID: "main-session",
	}, nil); err != nil {
		t.Fatal(err)
	}
	for _, action := range []string{"fresh", "resume"} {
		args := json.RawMessage(`{"name":"child","product":"fixture-worker","open":{}}`)
		if action == "resume" {
			args = json.RawMessage(`{"resume_session_id":"child@local"}`)
		}
		var response struct {
			OwnerToken string          `json:"owner_token"`
			SessionID  string          `json:"session_id"`
			CallID     string          `json:"call_id"`
			Result     json.RawMessage `json:"result"`
		}
		if err := native.Call(ownerTestContext(t), "tool.call", ownerToolRequest{
			OwnerToken: "main-token", SessionID: "main-session", CallID: action, Action: "spawn", Arguments: args,
		}, &response); err != nil {
			t.Fatal(err)
		}
		if response.OwnerToken != "main-token" || response.SessionID != "main-session" || response.CallID != action || string(response.Result) != string(want) {
			t.Fatalf("bridge response = %+v", response)
		}
	}
}
