// SPDX-License-Identifier: MIT

package omp

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	kit "github.com/antst/sessionbus/bus/sdk/go"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
	"github.com/sessionbus/pi-omp/wrappers/pifamily"
)

// The existing owner fixture, with actual SDK transfer negotiation. No custom
// executor: preparation and publication use the shipped filesystem default.
func transferOwner(t *testing.T) (*OwnerRegistry, *pifamily.Bridge, *ownerNativeFixture, net.Conn, *bufio.Scanner, string) {
	t.Helper()
	listener := ownerBusListener(t)
	cwd := t.TempDir()
	fixture := &ownerNativeFixture{descriptions: map[string]ownerDescribeResult{
		"transfer-owner": {OwnerToken: "transfer-owner", SessionID: "native-transfer", Name: "native", CWD: cwd},
	}}
	registry, native := ownerRegistryPair(t, OwnerRegistryOptions{Topology: ownerTopologyInteractive, Socket: listener.Addr().String(), Directory: cwd}, fixture)
	hello, conn, reader := ownerReady(t, registry, native, listener, ownerReadyRequest{Topology: ownerTopologyInteractive, Directory: cwd, Scope: ownerScopePrimary, Mode: ownerModeTUI, OwnerToken: "transfer-owner", SessionID: "native-transfer", Name: "native"}, protocol.TransferCapability)
	if hello.Info[protocol.TransferInfoKey] != float64(1) || hello.Info["cwd"] != cwd {
		t.Fatalf("transfer hello = %+v", hello)
	}
	if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	return registry, native, fixture, conn, reader, cwd
}

func transferRequest(t *testing.T, conn net.Conn, id int64, method string, params any) {
	t.Helper()
	body, err := protocol.RequestBytes(id, method, params)
	if err != nil {
		t.Fatal(err)
	}
	ownerWrite(t, conn, body)
}
func transferResult(t *testing.T, conn net.Conn, request protocol.Frame, result any) {
	t.Helper()
	body, err := protocol.ResultBytes(request.ID, request.Method, result)
	if err != nil {
		t.Fatal(err)
	}
	ownerWrite(t, conn, body)
}
func transferCall(t *testing.T, native *pifamily.Bridge, action string, args any) <-chan error {
	t.Helper()
	raw, err := json.Marshal(args)
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		result <- native.Call(ownerTestContext(t), "tool.call", ownerToolRequest{OwnerToken: "transfer-owner", SessionID: "native-transfer", CallID: "transfer-call", Action: action, Arguments: raw}, nil)
	}()
	return result
}

func TestOwnerTransferOfferUsesDefaultFilesystemAndRetiresOnClose(t *testing.T) {
	registry, native, _, conn, reader, cwd := transferOwner(t)
	source := filepath.Join(cwd, "source.txt")
	if err := os.WriteFile(source, []byte("body\n"), 0600); err != nil {
		t.Fatal(err)
	}
	offered := transferCall(t, native, "transfer.offer", map[string]any{"target": "peer@local", "source": source})
	offer := ownerFrame(t, reader)
	if offer.Method != "transfer.offer" || string(offer.Params) != `{"target":"peer@local","name":"source.txt"}` {
		t.Fatalf("wire offer contains path or wrong fields: %+v", offer)
	}
	transferResult(t, conn, offer, protocol.TransferDescriptor{ID: "outbound", Sender: "native-transfer", Receiver: "peer@local", Name: "source.txt", CreatedBy: "native-transfer", State: "offered"})
	if err := <-offered; err != nil {
		t.Fatal(err)
	}
	transferRequest(t, conn, 101, "transfer.prepare", protocol.TransferPrepare{ID: "outbound", Role: "source"})
	prepared := ownerFrame(t, reader)
	if prepared.ID != 101 || prepared.Error != nil || !strings.Contains(string(prepared.Result), `"size":5`) {
		t.Fatalf("source prepare = %+v", prepared)
	}
	registry.mu.Lock()
	attempt := registry.bindings["transfer-owner"].attempt
	registry.mu.Unlock()
	if err := registry.Close(); err != nil {
		t.Fatal(err)
	}
	// A caller retained from the retired generation cannot act on another wire.
	if _, err := attempt.caller.Action(context.Background(), "transfer.status", json.RawMessage(`{"id":"outbound"}`)); err == nil {
		t.Fatal("retired transfer caller remained usable")
	}
}

func TestOwnerTransferReceiveProjectsTerminalOffReaderAndKeepsDelivery(t *testing.T) {
	registry, native, fixture, conn, reader, cwd := transferOwner(t)
	content := []byte("received by OMP\n")
	hash := sha256.Sum256(content)
	sum := hex.EncodeToString(hash[:])
	size := protocol.Integer(len(content))
	from := kit.DeliverySource{SessionID: "peer@local", Product: "fixture", Groups: []string{}}
	transferRequest(t, conn, 110, "transfer.notice", protocol.TransferNotice{Kind: "offer", ID: "inbound", From: &from, Name: "in.txt", Size: &size})
	if ack := ownerFrame(t, reader); ack.ID != 110 || ack.Error != nil {
		t.Fatalf("offer notice = %+v", ack)
	}
	destination := filepath.Join(cwd, "in.txt")
	accepted := transferCall(t, native, "transfer.accept", map[string]any{"id": "inbound", "destination": destination})
	accept := ownerFrame(t, reader)
	if accept.Method != "transfer.accept" || string(accept.Params) != `{"id":"inbound"}` {
		t.Fatalf("wire accept = %+v", accept)
	}
	transferResult(t, conn, accept, protocol.TransferDescriptor{ID: "inbound", Sender: "peer@local", Receiver: "native-transfer", Name: "in.txt", CreatedBy: "peer@local", State: "preparing"})
	if err := <-accepted; err != nil {
		t.Fatal(err)
	}
	steps := []struct {
		method string
		params any
	}{
		{"transfer.prepare", protocol.TransferPrepare{ID: "inbound", Role: "destination", Size: &size}},
		{"transfer.start", protocol.TransferStart{ID: "inbound", Role: "destination", Size: size}},
		{"transfer.chunk", protocol.TransferChunk{ID: "inbound", Data: base64.StdEncoding.EncodeToString(content)}},
		{"transfer.finish", protocol.TransferFinish{ID: "inbound", Bytes: size, SHA256: sum}},
	}
	for i, step := range steps {
		id := int64(120 + i)
		transferRequest(t, conn, id, step.method, step.params)
		if frame := ownerFrame(t, reader); frame.ID != id || frame.Error != nil {
			t.Fatalf("%s = %+v", step.method, frame)
		}
	}
	if got, err := os.ReadFile(destination); err != nil || string(got) != string(content) {
		t.Fatalf("published file = %q, %v", got, err)
	}
	terminal := protocol.TransferNotice{Kind: "terminal", ID: "inbound", Outcome: "completed", Bytes: &size, SHA256: sum}
	body, err := json.Marshal(terminal)
	if err != nil {
		t.Fatal(err)
	}
	transferRequest(t, conn, 130, "message.deliver", kit.DeliveryRequest{MessageID: "transfer-notice-inbound", From: from, Body: string(body)})
	transferRequest(t, conn, 131, "message.deliver", kit.DeliveryRequest{MessageID: "after-transfer", From: from, Body: "ordinary delivery"})
	// Projection waits for this machine notice. Its ACK proves the shared
	// reader continues while the existing ordered delivery worker waits.
	transferRequest(t, conn, 132, "transfer.notice", terminal)
	seen := map[int64]protocol.Frame{}
	for len(seen) < 3 {
		f := ownerFrame(t, reader)
		if f.Error != nil {
			t.Fatalf("terminal/continuity response = %+v", f)
		}
		seen[f.ID] = f
	}
	for _, id := range []int64{130, 131, 132} {
		if _, ok := seen[id]; !ok {
			t.Fatalf("missing response %d: %+v", id, seen)
		}
	}
	fixture.mu.Lock()
	stages := append([]ownerStageRequest(nil), fixture.stages...)
	fixture.mu.Unlock()
	if len(stages) != 2 || stages[0].MessageID != "transfer-notice-inbound" || stages[1].MessageID != "after-transfer" {
		t.Fatalf("native ordered stages = %+v", stages)
	}
	rendered := stages[0].Body
	if !strings.Contains(rendered, `"destination":"`+destination+`"`) || !strings.Contains(rendered, `"outcome":"completed"`) {
		t.Fatalf("terminal projection = %s", rendered)
	}
	if err := registry.Err(); err != nil {
		t.Fatal(err)
	}
}

func TestOwnerProjectedDeliveryCapacityRefusesBeforeNativeAndReleasesOriginalReservation(t *testing.T) {
	registry, _, fixture, _, _, _ := transferOwner(t)
	registry.mu.Lock()
	state := registry.bindings["transfer-owner"]
	originalTotal, originalState := registry.retainedBytes, state.retainedBytes
	registry.retainedBytes = maxOwnerPeerRetainedBytes
	state.retainedBytes += maxOwnerPeerRetainedBytes - originalTotal
	registry.mu.Unlock()
	work := ownerPublicDelivery{ctx: context.Background(), value: kit.DeliveryRequest{Body: "small"}, retainedBytes: 5, project: func(_ context.Context, d *kit.DeliveryRequest) error { d.Body = "larger projected body"; return nil }}
	receipt, err := registry.deliverPublic(state, &work)
	if err != nil || receipt.Disposition != "rejected" || receipt.Reason != "queue_full" || work.retainedBytes != 5 {
		t.Fatalf("projection overflow = %+v, %v, reservation %d", receipt, err, work.retainedBytes)
	}
	fixture.mu.Lock()
	staged := len(fixture.stages)
	fixture.mu.Unlock()
	if staged != 0 {
		t.Fatal("over-cap projection reached native")
	}
	registry.mu.Lock()
	retained := registry.retainedBytes
	registry.retainedBytes = originalTotal
	state.retainedBytes = originalState
	registry.mu.Unlock()
	if retained != maxOwnerPeerRetainedBytes {
		t.Fatal("refused projection changed reservation")
	}
}

func TestOwnerTransferCloseRemovesUnpublishedDestination(t *testing.T) {
	registry, native, _, conn, reader, cwd := transferOwner(t)
	from := kit.DeliverySource{SessionID: "peer@local", Product: "fixture", Groups: []string{}}
	transferRequest(t, conn, 140, "transfer.notice", protocol.TransferNotice{Kind: "offer", ID: "unfinished", From: &from, Name: "unfinished.txt"})
	if ack := ownerFrame(t, reader); ack.ID != 140 || ack.Error != nil {
		t.Fatalf("offer notice = %+v", ack)
	}
	accepted := transferCall(t, native, "transfer.accept", map[string]any{"id": "unfinished", "destination": filepath.Join(cwd, "unfinished.txt")})
	accept := ownerFrame(t, reader)
	if accept.Method != "transfer.accept" {
		t.Fatalf("accept = %+v", accept)
	}
	transferResult(t, conn, accept, protocol.TransferDescriptor{ID: "unfinished", Sender: "peer@local", Receiver: "native-transfer", Name: "unfinished.txt", CreatedBy: "peer@local", State: "preparing"})
	if err := <-accepted; err != nil {
		t.Fatal(err)
	}
	size := protocol.Integer(10)
	transferRequest(t, conn, 141, "transfer.prepare", protocol.TransferPrepare{ID: "unfinished", Role: "destination", Size: &size})
	if prepared := ownerFrame(t, reader); prepared.ID != 141 || prepared.Error != nil {
		t.Fatalf("destination prepare = %+v", prepared)
	}
	entries, err := os.ReadDir(cwd)
	if err != nil || len(entries) != 1 || entries[0].Name() == "unfinished.txt" {
		t.Fatalf("prepared temporary destination = %v, %v", entries, err)
	}
	if err := registry.Close(); err != nil {
		t.Fatal(err)
	}
	entries, err = os.ReadDir(cwd)
	if err != nil || len(entries) != 0 {
		t.Fatalf("unpublished destination survived owner close: %v, %v", entries, err)
	}
}
