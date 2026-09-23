// SPDX-License-Identifier: MIT

package omp

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	kit "github.com/antst/sessionbus/bus/sdk/go"
	"github.com/sessionbus/peer-common/host"
	"github.com/sessionbus/peer-common/testsocket"
	"github.com/sessionbus/pi-omp/wrappers/pifamily"
)

const (
	ompNativeOwnerHelperEnv   = "OMP_SESSIONBUS_NATIVE_OWNER_HELPER"
	ompNativeOwnerScenarioEnv = "OMP_SESSIONBUS_NATIVE_OWNER_SCENARIO"
	ompNativeOwnerCaptureEnv  = "OMP_SESSIONBUS_NATIVE_OWNER_CAPTURE"
	ompNativeOwnerSID         = "omp-native-session-1"
	ompNativeOwnerToken       = "omp-owner-token-1"
	ompNativeOwnerName        = "OMP native fixture"
	ompNativeOwnerReadyFrame  = `{"type":"ready","protocolVersion":1,"supportedProtocolVersions":[1,2],"maxFrameBytes":1048576,"maxReassembledFrameBytes":67108864}`
)

type ompNativeOwnerCapture struct {
	Launch ompLaunch `json:"launch"`
	Args   []string  `json:"args"`
}

type ompNativeOwnerHelper struct {
	launch         ompLaunch
	cwd            string
	name           string
	scenario       string
	shutdown       chan struct{}
	state          chan struct{}
	stateRequested chan struct{}
	releaseState   chan struct{}
	releaseExit    chan struct{}
	endAck         chan struct{}
	once           sync.Once
	exitOnce       sync.Once
	stateOnce      sync.Once
}

func TestOMPNativeOwnerHelper(t *testing.T) {
	if os.Getenv(ompNativeOwnerHelperEnv) == "" {
		return
	}
	if err := runOMPNativeOwnerHelper(); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(17)
	}
	os.Exit(0)
}

func runOMPNativeOwnerHelper() error {
	var launch ompLaunch
	if err := json.Unmarshal([]byte(os.Getenv(launchEnvironmentName)), &launch); err != nil {
		return err
	}
	if launch.OwnerPID != os.Getppid() || (launch.Topology != ownerTopologyLane && launch.Topology != ownerTopologyInteractive) {
		return errors.New("invalid native owner launch descriptor")
	}
	separator := slices.Index(os.Args, "--")
	if separator < 0 {
		return errors.New("native owner helper arguments are missing")
	}
	capture := ompNativeOwnerCapture{Launch: launch, Args: slices.Clone(os.Args[separator+1:])}
	body, err := json.Marshal(capture)
	if err != nil {
		return err
	}
	// The parent uses final-path existence as its startup barrier before Close.
	// Publish complete JSON atomically so it cannot kill us between create/write.
	capturePath := os.Getenv(ompNativeOwnerCaptureEnv)
	if err = os.WriteFile(capturePath+".pending", body, 0o600); err != nil {
		return err
	}
	if err = os.Rename(capturePath+".pending", capturePath); err != nil {
		return err
	}
	if launch.Topology == ownerTopologyLane {
		if _, err = fmt.Fprintln(os.Stdout, ompNativeOwnerReadyFrame); err != nil {
			return err
		}
		if os.Getenv(ompNativeOwnerScenarioEnv) == "startup_event" {
			if _, err = fmt.Fprintln(os.Stdout, `{"type":"agent_start"}`); err != nil {
				return err
			}
		}
	}
	if os.Getenv(ompNativeOwnerScenarioEnv) == "changed_cwd" {
		launchCWD, cwdErr := os.Getwd()
		if cwdErr != nil {
			return cwdErr
		}
		selected := filepath.Join(launchCWD, "persisted-project")
		if cwdErr = os.Mkdir(selected, 0o700); cwdErr != nil {
			return cwdErr
		}
		if cwdErr = os.Chdir(selected); cwdErr != nil {
			return cwdErr
		}
	}
	connection, err := net.Dial("unix", launch.Socket)
	if err != nil {
		return err
	}
	cwd, err := os.Getwd()
	if err != nil {
		_ = connection.Close()
		return err
	}
	helper := &ompNativeOwnerHelper{
		launch: launch, cwd: cwd, name: ompNativeOwnerName, scenario: os.Getenv(ompNativeOwnerScenarioEnv),
		shutdown: make(chan struct{}), state: make(chan struct{}), stateRequested: make(chan struct{}),
		releaseState: make(chan struct{}), releaseExit: make(chan struct{}), endAck: make(chan struct{}),
	}
	bridge, err := pifamily.NewBridge(connection, pifamily.BridgeNative, helper.handleBridge, pifamily.BridgeLimits{})
	if err != nil {
		return err
	}
	if err = bridge.Ready(context.Background()); err != nil {
		return err
	}
	if helper.scenario == "hold_owner_ready" {
		<-bridge.Done()
		return bridge.Err()
	}
	mode := ownerModeRPC
	if launch.Topology == ownerTopologyInteractive {
		mode = ownerModeTUI
	}
	if err = bridge.Call(context.Background(), "owner.ready", ownerReadyRequest{
		Topology: launch.Topology, Directory: launch.Directory, Scope: ownerScopePrimary,
		Mode: mode, OwnerToken: ompNativeOwnerToken, SessionID: ompNativeOwnerSID, Name: ompNativeOwnerName,
	}, nil); err != nil {
		return err
	}
	if helper.scenario == "startup_widget" {
		if _, err = fmt.Fprintln(os.Stdout, `{"type":"extension_ui_request","id":"startup-widget","method":"setWidget","widgetKey":"autoresearch"}`); err != nil {
			return err
		}
	}
	if helper.scenario == "startup_dialog" {
		if _, err = fmt.Fprintln(os.Stdout, `{"type":"extension_ui_request","id":"startup-dialog","method":"confirm","title":"startup","message":"continue"}`); err != nil {
			return err
		}
	}
	var commandsDone chan error
	if launch.Topology == ownerTopologyLane {
		commandsDone = make(chan error, 1)
		go func() { commandsDone <- helper.serveCommands() }()
	} else {
		close(helper.state)
	}
	if helper.scenario == "wrong_state" {
		<-bridge.Done()
		return bridge.Err()
	}
	if helper.scenario == "close_bridge_during_state" {
		<-helper.stateRequested
		if err = bridge.Close(); err != nil && !errors.Is(err, pifamily.ErrBridgeClosed) {
			return err
		}
		close(helper.releaseState)
		return <-commandsDone
	}
	<-helper.state
	if helper.scenario == "exit_after_ready" {
		<-helper.releaseExit
		os.Exit(23)
	}
	<-helper.shutdown
	if helper.scenario == "hold_shutdown_response" {
		<-bridge.Done()
		return bridge.Err()
	}
	if helper.scenario == "hold_shutdown" {
		select {}
	}
	if helper.scenario != "end_before_shutdown_response" {
		if err = waitOMPNativeOwnerBridgeHandlers(bridge); err != nil {
			return err
		}
	}
	if helper.scenario == "malformed_shutdown" || helper.scenario == "delayed_malformed_shutdown" {
		_, _ = fmt.Fprintln(os.Stdout, "{")
	}
	if err = bridge.Call(context.Background(), "session_end", ownerEndRequest{
		Topology: launch.Topology, Scope: ownerScopePrimary, Mode: mode,
		OwnerToken: ompNativeOwnerToken, SessionID: ompNativeOwnerSID, Reason: "quit",
	}, nil); err != nil {
		return err
	}
	if helper.scenario == "end_before_shutdown_response" {
		close(helper.endAck)
	}
	if commandsDone != nil {
		if err = <-commandsDone; err != nil {
			return err
		}
	}
	if err = bridge.Close(); err != nil && !errors.Is(err, pifamily.ErrBridgeClosed) {
		return err
	}
	return nil
}

func waitOMPNativeOwnerBridgeHandlers(bridge *pifamily.Bridge) error {
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for bridge.Stats().ActiveCalls != 0 {
		select {
		case <-bridge.Done():
			return bridge.Err()
		case <-deadline.C:
			return errors.New("native owner bridge handler did not finish")
		case <-ticker.C:
		}
	}
	return nil
}

func (helper *ompNativeOwnerHelper) handleBridge(ctx context.Context, method string, raw json.RawMessage) (json.RawMessage, error) {
	switch method {
	case "native.describe":
		var request ownerDescribeRequest
		if decodeOwnerJSON(raw, &request) != nil || request.OwnerToken != ompNativeOwnerToken || request.SessionID != ompNativeOwnerSID {
			return nil, pifamily.NewBridgeCallError("bad_request", "invalid native description")
		}
		return json.Marshal(ownerDescribeResult{
			OwnerToken: ompNativeOwnerToken, SessionID: ompNativeOwnerSID,
			Name: ompNativeOwnerName, CWD: helper.cwd,
		})
	case "native.shutdown":
		var request ownerDescribeRequest
		if decodeOwnerJSON(raw, &request) != nil || request.OwnerToken != ompNativeOwnerToken || request.SessionID != ompNativeOwnerSID {
			return nil, pifamily.NewBridgeCallError("bad_request", "invalid native shutdown")
		}
		helper.once.Do(func() { close(helper.shutdown) })
		if helper.scenario == "end_before_shutdown_response" {
			<-helper.endAck
		}
		if helper.scenario == "hold_shutdown_response" {
			_ = os.WriteFile(os.Getenv(ompNativeOwnerCaptureEnv)+".shutdown", []byte("entered\n"), 0o600)
			<-ctx.Done()
			return nil, ctx.Err()
		}
		return json.Marshal(ownerShutdownResult{
			OwnerToken: ompNativeOwnerToken, SessionID: ompNativeOwnerSID, Requested: true,
		})
	case "fixture.exit":
		if helper.scenario != "exit_after_ready" {
			return nil, pifamily.NewBridgeCallError("bad_request", "unexpected fixture exit")
		}
		helper.exitOnce.Do(func() { close(helper.releaseExit) })
		return json.Marshal(struct{}{})
	default:
		return nil, pifamily.NewBridgeCallError("method_not_found", "unexpected native owner method")
	}
}

func (helper *ompNativeOwnerHelper) serveCommands() error {
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 4096), nativeRPCPhysicalFrameBytes)
	for scanner.Scan() {
		var command map[string]json.RawMessage
		if json.Unmarshal(scanner.Bytes(), &command) != nil {
			return errors.New("invalid native owner command")
		}
		var id, kind string
		if json.Unmarshal(command["id"], &id) != nil || json.Unmarshal(command["type"], &kind) != nil {
			return errors.New("invalid native owner command envelope")
		}
		var data any
		switch kind {
		case "negotiate_protocol":
			data = map[string]int{"protocolVersion": 2}
		case "set_session_name":
			name, parseErr := nativeRPCText(command, "name", 4096)
			if parseErr != nil || !validOwnerText(name, 4096) || name == "" {
				return errors.New("invalid native owner fixture name")
			}
			helper.name = name
		case "get_state":
			if helper.scenario == "close_bridge_during_state" {
				close(helper.stateRequested)
				<-helper.releaseState
				continue
			}
			sessionID := ompNativeOwnerSID
			if helper.scenario == "wrong_state" {
				sessionID = "wrong-native-session"
			}
			data = map[string]any{
				"sessionId": sessionID, "sessionName": helper.name,
				"isStreaming": false, "isCompacting": false, "queuedMessageCount": 0,
			}
		default:
			return fmt.Errorf("unexpected native owner command %q", kind)
		}
		response, err := json.Marshal(map[string]any{
			"id": id, "type": "response", "command": kind, "success": true, "data": data,
		})
		if err != nil {
			return err
		}
		if _, err = fmt.Fprintln(os.Stdout, string(response)); err != nil {
			return err
		}
		if kind == "get_state" {
			helper.stateOnce.Do(func() { close(helper.state) })
			if helper.scenario == "event_after_ready" || helper.scenario == "delayed_malformed_shutdown" {
				if _, err = fmt.Fprintln(os.Stdout, `{"type":"agent_start"}`); err != nil {
					return err
				}
			}
		}
	}
	return scanner.Err()
}

func ompNativeOwnerHelperCommand(name string, arguments ...string) *exec.Cmd {
	owned := []string{"-test.run=^TestOMPNativeOwnerHelper$", "--", name}
	owned = append(owned, arguments...)
	return exec.Command(os.Args[0], owned...)
}

func nativeOwnerTestContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func nativeOwnerFixture(t *testing.T, scenario string) (NativeOwnerOptions, string) {
	t.Helper()
	directory := testsocket.Directory(t)
	extension := filepath.Join(directory, "extension.mjs")
	if err := os.WriteFile(extension, []byte("export default {}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	capture := filepath.Join(directory, "capture.json")
	t.Setenv(ompNativeOwnerHelperEnv, "1")
	t.Setenv(ompNativeOwnerScenarioEnv, scenario)
	t.Setenv(ompNativeOwnerCaptureEnv, capture)
	previous := ompCommand
	ompCommand = ompNativeOwnerHelperCommand
	t.Cleanup(func() { ompCommand = previous })
	return NativeOwnerOptions{
		DaemonSocket: filepath.Join(directory, "daemon.sock"), Provisional: "provisional",
		CWD: directory, Topology: ownerTopologyLane, InitialName: "fixture@local",
		Extension: extension, PrimaryCaller: &kit.Caller{},
		Native: NativeExecutable{
			RuntimePath: filepath.Join(directory, "bun"),
			EntryPath:   filepath.Join(directory, "package", "dist", "cli.js"),
		},
		Arguments:      []string{"--model", "fixture", "--", "literal"},
		NativeObserver: func(json.RawMessage) error { return nil },
	}, capture
}

func waitNativeOwnerReady(t *testing.T, owner *NativeOwner) {
	t.Helper()
	select {
	case <-owner.Ready():
	case <-owner.Done():
		t.Fatalf("OMP native owner ended before ready: %v", owner.Err())
	case <-nativeOwnerTestContext(t).Done():
		t.Fatal("OMP native owner readiness timed out")
	}
}

func TestNativeOwnerJoinsStartupShutdownAndOwnedResources(t *testing.T) {
	options, capturePath := nativeOwnerFixture(t, "")
	owner, err := StartNativeOwner(nativeOwnerTestContext(t), options)
	if err != nil {
		t.Fatal(err)
	}
	waitNativeOwnerReady(t, owner)
	binding, ok := owner.Primary()
	if !ok || binding.SessionID != ompNativeOwnerSID || binding.OwnerToken != ompNativeOwnerToken ||
		binding.Name != ompNativeOwnerName || binding.CWD != options.CWD || binding.Scope != ownerScopePrimary || binding.Mode != ownerModeRPC {
		t.Fatalf("primary = %+v, %v", binding, ok)
	}
	closeResults := make(chan error, 2)
	closeContext := nativeOwnerTestContext(t)
	go func() { closeResults <- owner.Close(closeContext) }()
	go func() { closeResults <- owner.Close(closeContext) }()
	for range 2 {
		if err = <-closeResults; err != nil {
			t.Fatal(err)
		}
	}
	if reason, ok := owner.GracefulEnd(); !ok || reason != "quit" {
		t.Fatalf("graceful end = %q, %v", reason, ok)
	}
	var capture ompNativeOwnerCapture
	body, err := os.ReadFile(capturePath)
	if err != nil || json.Unmarshal(body, &capture) != nil {
		t.Fatalf("capture = %q, %v", body, err)
	}
	want := []string{
		options.Native.RuntimePath, options.Native.EntryPath,
		"--extension", options.Extension, "--mode", "rpc-ui", "--allow-home",
		"--model", "fixture", "--", "literal",
	}
	if !slices.Equal(capture.Args, want) {
		t.Fatalf("native argv = %#v, want %#v", capture.Args, want)
	}
	if capture.Launch.Topology != ownerTopologyLane || capture.Launch.Directory == "" || capture.Launch.Socket == "" {
		t.Fatalf("launch = %+v", capture.Launch)
	}
	if _, err = os.Stat(capture.Launch.Directory); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("private directory survived Close: %v", err)
	}
	lock, err := host.AcquireSessionLock(options.DaemonSocket, "omp", ompNativeOwnerSID)
	if err != nil {
		t.Fatalf("session lock remained owned after Close: %v", err)
	}
	_ = lock.Close()
}

func TestNativeOwnerInteractiveUsesTerminalAndExtensionReadiness(t *testing.T) {
	options, capturePath := nativeOwnerFixture(t, "")
	listener := ownerBusListener(t)
	options.DaemonSocket = listener.Addr().String()
	options.Topology = ownerTopologyInteractive
	options.PrimaryCaller = nil
	options.NativeObserver = nil
	owner, err := StartNativeOwner(nativeOwnerTestContext(t), options)
	if err != nil {
		t.Fatal(err)
	}
	conn, scanner := ownerAccept(t, listener)
	hello := ownerHello(t, conn, scanner)
	if hello.SessionID != ompNativeOwnerSID || hello.Product != Product {
		t.Fatalf("interactive hello = %+v", hello)
	}
	waitNativeOwnerReady(t, owner)
	if owner.rpc != nil || owner.process.input != nil || owner.process.output != nil ||
		owner.process.command.Stdin != os.Stdin || owner.process.command.Stdout != os.Stdout || owner.process.command.Stderr != os.Stderr {
		t.Fatal("interactive owner constructed an RPC transport instead of inheriting the terminal")
	}
	if err = owner.Close(nativeOwnerTestContext(t)); err != nil {
		t.Fatal(err)
	}
	var capture ompNativeOwnerCapture
	body, err := os.ReadFile(capturePath)
	if err != nil || json.Unmarshal(body, &capture) != nil {
		t.Fatalf("capture = %q, %v", body, err)
	}
	want := []string{
		options.Native.RuntimePath, options.Native.EntryPath,
		"--extension", options.Extension, "--model", "fixture", "--", "literal",
	}
	if !slices.Equal(capture.Args, want) || capture.Launch.Topology != ownerTopologyInteractive {
		t.Fatalf("interactive capture = %+v, want args %#v", capture, want)
	}
}

func TestNativeOwnerBypassRetainsManagedExtension(t *testing.T) {
	for _, test := range []struct {
		name string
		lane bool
		args []string
	}{
		{"lane", true, nil},
		{"peer-yolo", false, []string{"--yolo"}},
		{"peer-auto-approve", false, []string{"--auto-approve"}},
		{"peer-approval-mode", false, []string{"--approval-mode=yolo"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			options, capturePath := nativeOwnerFixture(t, "")
			var listener net.Listener
			var err error
			if test.lane {
				options.Arguments, err = ompLaneArguments(kit.OpenOptions{PermissionMode: "bypassPermissions"}, "")
				if err != nil {
					t.Fatal(err)
				}
			} else {
				plan, passthrough, err := InteractivePlan(options.Native, test.args, nil)
				if err != nil || passthrough {
					t.Fatalf("plan = %#v, %t, %v", plan, passthrough, err)
				}
				options.Arguments = plan.Args[1:]
				listener = ownerBusListener(t)
				options.DaemonSocket = listener.Addr().String()
				options.Topology = ownerTopologyInteractive
				options.PrimaryCaller = nil
				options.NativeObserver = nil
			}
			owner, err := StartNativeOwner(nativeOwnerTestContext(t), options)
			if err != nil {
				t.Fatal(err)
			}
			if listener != nil {
				conn, scanner := ownerAccept(t, listener)
				ownerHello(t, conn, scanner)
			}
			waitNativeOwnerReady(t, owner)
			if err = owner.Close(nativeOwnerTestContext(t)); err != nil {
				t.Fatal(err)
			}
			var capture ompNativeOwnerCapture
			body, err := os.ReadFile(capturePath)
			if err != nil || json.Unmarshal(body, &capture) != nil {
				t.Fatalf("capture = %q, %v", body, err)
			}
			want := []string{options.Native.RuntimePath, options.Native.EntryPath, "--extension", options.Extension}
			if test.lane {
				want = append(want, "--mode", "rpc-ui", "--allow-home")
			}
			want = append(want, options.Arguments...)
			if !slices.Equal(capture.Args, want) {
				t.Fatalf("argv = %#v, want %#v", capture.Args, want)
			}
		})
	}
}

func TestNativeOwnerRequiresObserverOnlyForLane(t *testing.T) {
	options, _ := nativeOwnerFixture(t, "")
	options.NativeObserver = nil
	if err := validateNativeOwnerOptions(options); err == nil {
		t.Fatal("lane owner without a native event observer was accepted")
	}
	options.Topology = ownerTopologyInteractive
	options.PrimaryCaller = nil
	options.NativeObserver = func(json.RawMessage) error { return nil }
	if err := validateNativeOwnerOptions(options); err == nil {
		t.Fatal("interactive owner with an RPC observer was accepted")
	}
}

func TestNativeOwnerRetainsStartupNativeEventFailure(t *testing.T) {
	options, _ := nativeOwnerFixture(t, "startup_event")
	want := errors.New("fixture rejected startup native event")
	options.NativeObserver = func(raw json.RawMessage) error {
		if !bytes.Contains(raw, []byte(`"agent_start"`)) {
			return errors.New("unexpected startup native event")
		}
		return want
	}
	owner, err := StartNativeOwner(nativeOwnerTestContext(t), options)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-owner.Ready():
		t.Fatal("rejected startup event reached readiness")
	case <-owner.Done():
	case <-nativeOwnerTestContext(t).Done():
		t.Fatal("rejected startup event did not retire owner")
	}
	if !errors.Is(owner.Err(), want) {
		t.Fatalf("startup event failure = %v", owner.Err())
	}
}

func TestNativeOwnerObservesPostReadinessNativeEvent(t *testing.T) {
	options, _ := nativeOwnerFixture(t, "event_after_ready")
	observed := make(chan struct{})
	release := make(chan struct{})
	want := errors.New("fixture rejected foreign native event")
	options.NativeObserver = func(raw json.RawMessage) error {
		if bytes.Contains(raw, []byte(`"agent_start"`)) {
			close(observed)
			<-release
			return want
		}
		return nil
	}
	owner, err := StartNativeOwner(nativeOwnerTestContext(t), options)
	if err != nil {
		t.Fatal(err)
	}
	waitNativeOwnerReady(t, owner)
	select {
	case <-observed:
	case <-nativeOwnerTestContext(t).Done():
		t.Fatal("post-readiness native event was not observed")
	}
	close(release)
	<-owner.Done()
	if !errors.Is(owner.Err(), want) {
		t.Fatalf("post-readiness event failure = %v", owner.Err())
	}
}

func TestNativeOwnerCloseCancelsHeldStartupAndJoins(t *testing.T) {
	options, capturePath := nativeOwnerFixture(t, "hold_owner_ready")
	owner, err := StartNativeOwner(nativeOwnerTestContext(t), options)
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err = os.Stat(capturePath); err == nil {
			break
		}
		if !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
		select {
		case <-owner.Done():
			t.Fatalf("owner ended before held startup: %v", owner.Err())
		default:
		}
	}
	err = owner.Close(nativeOwnerTestContext(t))
	if err == nil {
		t.Fatal("held startup Close unexpectedly succeeded")
	}
	select {
	case <-owner.Done():
	default:
		t.Fatal("held startup Close did not join owner")
	}
	var capture ompNativeOwnerCapture
	body, readErr := os.ReadFile(capturePath)
	if readErr != nil || json.Unmarshal(body, &capture) != nil {
		t.Fatalf("capture = %q, %v", body, readErr)
	}
	if _, statErr := os.Stat(capture.Launch.Directory); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("held startup directory survived: %v", statErr)
	}
}

func TestNativeOwnerRejectsContradictoryStateAndJoins(t *testing.T) {
	options, capturePath := nativeOwnerFixture(t, "wrong_state")
	owner, err := StartNativeOwner(nativeOwnerTestContext(t), options)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-owner.Ready():
		t.Fatal("contradictory native state reached readiness")
	case <-owner.Done():
	case <-nativeOwnerTestContext(t).Done():
		t.Fatal("contradictory native state did not retire")
	}
	if owner.Err() == nil {
		t.Fatal("contradictory native state was not retained")
	}
	var capture ompNativeOwnerCapture
	body, readErr := os.ReadFile(capturePath)
	if readErr != nil || json.Unmarshal(body, &capture) != nil {
		t.Fatalf("capture = %q, %v", body, readErr)
	}
	if _, statErr := os.Stat(capture.Launch.Directory); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("failed startup directory survived: %v", statErr)
	}
}

func TestNativeOwnerBridgeLossCancelsHeldStateReadAndJoins(t *testing.T) {
	options, capturePath := nativeOwnerFixture(t, "close_bridge_during_state")
	owner, err := StartNativeOwner(nativeOwnerTestContext(t), options)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-owner.Ready():
		t.Fatal("bridge loss during state read reached readiness")
	case <-owner.Done():
	case <-nativeOwnerTestContext(t).Done():
		t.Fatal("bridge loss during state read did not retire owner")
	}
	if owner.Err() == nil {
		t.Fatal("bridge loss during state read was not retained")
	}
	var capture ompNativeOwnerCapture
	body, readErr := os.ReadFile(capturePath)
	if readErr != nil || json.Unmarshal(body, &capture) != nil {
		t.Fatalf("capture = %q, %v", body, readErr)
	}
	if _, statErr := os.Stat(capture.Launch.Directory); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("bridge-loss directory survived: %v", statErr)
	}
}

func TestNativeOwnerRetiresUnexpectedChildExit(t *testing.T) {
	options, _ := nativeOwnerFixture(t, "exit_after_ready")
	owner, err := StartNativeOwner(nativeOwnerTestContext(t), options)
	if err != nil {
		t.Fatal(err)
	}
	waitNativeOwnerReady(t, owner)
	select {
	case <-owner.Done():
		t.Fatal("fixture child exited before the explicit post-readiness release")
	default:
	}
	if err = owner.bridge.Call(nativeOwnerTestContext(t), "fixture.exit", nil, nil); err != nil && !errors.Is(err, pifamily.ErrBridgeClosed) {
		t.Fatal(err)
	}
	select {
	case <-owner.Done():
	case <-nativeOwnerTestContext(t).Done():
		t.Fatal("unexpected child exit did not retire owner")
	}
	if owner.Err() == nil {
		t.Fatal("unexpected child exit was not retained")
	}
}

func TestNativeOwnerAdoptsLiveCWDSeparateFromLaunchDirectory(t *testing.T) {
	options, _ := nativeOwnerFixture(t, "changed_cwd")
	owner, err := StartNativeOwner(nativeOwnerTestContext(t), options)
	if err != nil {
		t.Fatal(err)
	}
	waitNativeOwnerReady(t, owner)
	binding, ok := owner.Primary()
	want := filepath.Join(options.CWD, "persisted-project")
	if !ok || binding.CWD != want || binding.CWD == options.CWD {
		t.Fatalf("live cwd binding = %+v, want %q", binding, want)
	}
	if err = owner.Close(nativeOwnerTestContext(t)); err != nil {
		t.Fatal(err)
	}
}

func TestNativeOwnerParentCancellationForcesAndJoins(t *testing.T) {
	options, capturePath := nativeOwnerFixture(t, "")
	ctx, cancel := context.WithCancel(context.Background())
	owner, err := StartNativeOwner(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	waitNativeOwnerReady(t, owner)
	cancel()
	select {
	case <-owner.Done():
	case <-nativeOwnerTestContext(t).Done():
		t.Fatal("parent cancellation did not join owner")
	}
	if !errors.Is(owner.Err(), context.Canceled) {
		t.Fatalf("parent cancellation = %v", owner.Err())
	}
	var capture ompNativeOwnerCapture
	body, readErr := os.ReadFile(capturePath)
	if readErr != nil || json.Unmarshal(body, &capture) != nil {
		t.Fatalf("capture = %q, %v", body, readErr)
	}
	if _, statErr := os.Stat(capture.Launch.Directory); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("canceled owner directory survived: %v", statErr)
	}
}

func TestNativeOwnerCloseCancellationForcesAndJoinsChild(t *testing.T) {
	options, capturePath := nativeOwnerFixture(t, "hold_shutdown")
	owner, err := StartNativeOwner(nativeOwnerTestContext(t), options)
	if err != nil {
		t.Fatal(err)
	}
	waitNativeOwnerReady(t, owner)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	err = owner.Close(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("forced Close = %v", err)
	}
	var capture ompNativeOwnerCapture
	body, readErr := os.ReadFile(capturePath)
	if readErr != nil || json.Unmarshal(body, &capture) != nil {
		t.Fatalf("capture = %q, %v", body, readErr)
	}
	if _, statErr := os.Stat(capture.Launch.Directory); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("forced Close directory survived: %v", statErr)
	}
}

func TestNativeOwnerParentCancellationInterruptsHeldShutdownResponse(t *testing.T) {
	options, capturePath := nativeOwnerFixture(t, "hold_shutdown_response")
	parent, cancel := context.WithCancel(context.Background())
	owner, err := StartNativeOwner(parent, options)
	if err != nil {
		t.Fatal(err)
	}
	waitNativeOwnerReady(t, owner)
	closed := make(chan error, 1)
	go func() { closed <- owner.Close(context.Background()) }()
	marker := capturePath + ".shutdown"
	for {
		if _, err = os.Stat(marker); err == nil {
			break
		}
		if !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
		select {
		case err = <-closed:
			t.Fatalf("Close returned before held shutdown response: %v", err)
		default:
		}
	}
	cancel()
	if err = <-closed; !errors.Is(err, context.Canceled) {
		t.Fatalf("parent-canceled held shutdown = %v", err)
	}
	select {
	case <-owner.Done():
	default:
		t.Fatal("parent-canceled shutdown did not join owner")
	}
}

func TestNativeOwnerAcceptsEndBeforeShutdownResponse(t *testing.T) {
	options, _ := nativeOwnerFixture(t, "end_before_shutdown_response")
	owner, err := StartNativeOwner(nativeOwnerTestContext(t), options)
	if err != nil {
		t.Fatal(err)
	}
	waitNativeOwnerReady(t, owner)
	if err = owner.Close(nativeOwnerTestContext(t)); err != nil {
		t.Fatal(err)
	}
	if reason, ok := owner.GracefulEnd(); !ok || reason != "quit" {
		t.Fatalf("end-before-response graceful result = %q, %v", reason, ok)
	}
}

func TestNativeOwnerContextWatcherJoinsRunningCallback(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	entered := make(chan struct{})
	release := make(chan struct{})
	join := watchNativeOwnerContext(ctx, func() {
		close(entered)
		<-release
	})
	cancel()
	<-entered
	joined := make(chan struct{})
	go func() {
		join()
		close(joined)
	}()
	select {
	case <-joined:
		t.Fatal("context watcher returned before its callback")
	default:
	}
	close(release)
	<-joined
}

func TestNativeOwnerPreservesClosingProtocolFailure(t *testing.T) {
	options, _ := nativeOwnerFixture(t, "malformed_shutdown")
	owner, err := StartNativeOwner(nativeOwnerTestContext(t), options)
	if err != nil {
		t.Fatal(err)
	}
	waitNativeOwnerReady(t, owner)
	err = owner.Close(nativeOwnerTestContext(t))
	if !errors.Is(err, errNativeRPCProtocol) {
		t.Fatalf("closing protocol failure = %v", err)
	}
	if reason, ok := owner.GracefulEnd(); !ok || reason != "quit" {
		t.Fatalf("graceful native end was lost: %q, %v", reason, ok)
	}
}

func TestNativeOwnerDrainsClosingProtocolFailureAfterChildExit(t *testing.T) {
	options, _ := nativeOwnerFixture(t, "delayed_malformed_shutdown")
	observed := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseReader := func() { releaseOnce.Do(func() { close(release) }) }
	options.NativeObserver = func(raw json.RawMessage) error {
		if bytes.Contains(raw, []byte(`"agent_start"`)) {
			close(observed)
			<-release
		}
		return nil
	}
	owner, err := StartNativeOwner(nativeOwnerTestContext(t), options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		releaseReader()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = owner.Close(ctx)
	})
	waitNativeOwnerReady(t, owner)
	select {
	case <-observed:
	case <-nativeOwnerTestContext(t).Done():
		t.Fatal("native RPC reader did not enter the held event")
	}
	closed := make(chan error, 1)
	go func() { closed <- owner.Close(nativeOwnerTestContext(t)) }()
	select {
	case <-owner.process.done:
	case <-nativeOwnerTestContext(t).Done():
		t.Fatal("native child did not exit before the reader was released")
	}
	select {
	case err = <-closed:
		t.Fatalf("Close returned before the held RPC reader was released: %v", err)
	default:
	}
	releaseReader()
	if err = <-closed; !errors.Is(err, errNativeRPCProtocol) {
		t.Fatalf("delayed closing protocol failure = %v", err)
	}
	if reason, ok := owner.GracefulEnd(); !ok || reason != "quit" {
		t.Fatalf("delayed graceful native end was lost: %q, %v", reason, ok)
	}
}

func TestNativeOwnerGracefulRPCDrainCancellationJoinsHeldOutput(t *testing.T) {
	inputRead, inputWrite, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer inputRead.Close()
	outputRead, outputWrite, err := os.Pipe()
	if err != nil {
		_ = inputWrite.Close()
		t.Fatal(err)
	}
	defer outputWrite.Close()
	rpc, err := newNativeRPC(inputWrite, outputRead, nil, nativeRPCLimits{})
	if err != nil {
		t.Fatal(err)
	}
	defer rpc.Close()
	ownerCtx, cancelOwner := context.WithCancelCause(context.Background())
	operationCtx, cancelOperations := context.WithCancelCause(context.Background())
	joinOperations := watchNativeOwnerContext(ownerCtx, func() {
		cancelOperations(context.Cause(ownerCtx))
	})
	defer func() {
		cancelOwner(errNativeOwnerClosed)
		cancelOperations(errNativeOwnerClosed)
		joinOperations()
	}()
	drained := make(chan error, 1)
	go func() { drained <- drainNativeOwnerRPC(operationCtx, rpc) }()
	select {
	case err = <-drained:
		t.Fatalf("held stdout drained before cancellation: %v", err)
	default:
	}
	want := errors.New("cancel held native stdout")
	cancelOwner(want)
	select {
	case err = <-drained:
		if !errors.Is(err, want) {
			t.Fatalf("held stdout cancellation = %v", err)
		}
	case <-nativeOwnerTestContext(t).Done():
		t.Fatal("held stdout cancellation did not join native RPC")
	}
	select {
	case <-rpc.Done():
	default:
		t.Fatal("held stdout cancellation returned before native RPC joined")
	}
}

func TestDecodeNativeOwnerStateRequiresTypedReadiness(t *testing.T) {
	for name, body := range map[string]string{
		"missing id":       `{"isStreaming":false,"isCompacting":false,"queuedMessageCount":0}`,
		"null streaming":   `{"sessionId":"id","isStreaming":null,"isCompacting":false,"queuedMessageCount":0}`,
		"missing compact":  `{"sessionId":"id","isStreaming":false,"queuedMessageCount":0}`,
		"missing queue":    `{"sessionId":"id","isStreaming":false,"isCompacting":false}`,
		"negative queue":   `{"sessionId":"id","isStreaming":false,"isCompacting":false,"queuedMessageCount":-1}`,
		"wrong session id": `{"sessionId":"bad id","isStreaming":false,"isCompacting":false,"queuedMessageCount":0}`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeNativeOwnerState(json.RawMessage(body)); err == nil {
				t.Fatal("invalid native state was accepted")
			}
		})
	}
}

func TestNativeOwnerRejectsNonphysicalExtension(t *testing.T) {
	directory := t.TempDir()
	target := filepath.Join(directory, "target.mjs")
	if err := os.WriteFile(target, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	extension := filepath.Join(directory, "extension.mjs")
	if err := os.Symlink(target, extension); err != nil {
		t.Fatal(err)
	}
	err := validateNativeOwnerOptions(NativeOwnerOptions{
		DaemonSocket: filepath.Join(directory, "daemon.sock"), Provisional: "provisional",
		CWD: directory, Topology: ownerTopologyLane, Extension: extension,
		PrimaryCaller:  &kit.Caller{},
		NativeObserver: func(json.RawMessage) error { return nil },
	})
	if err == nil {
		t.Fatal("symlinked managed extension was accepted")
	}
}
