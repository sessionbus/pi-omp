// SPDX-License-Identifier: MIT

package pi

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	sessionkit "github.com/antst/sessionbus/bus/sdk/go"
	"github.com/sessionbus/peer-common/host"
	"github.com/sessionbus/pi-omp/wrappers/pifamily"

	"github.com/sessionbus/peer-common/testsocket"
)

const piNativeHelperEnv = "PI_SESSIONBUS_TEST_NATIVE"

func TestPiNativeHelper(t *testing.T) {
	mode := os.Getenv(piNativeHelperEnv)
	if mode == "" {
		return
	}
	if err := runPiNativeHelper(mode); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(9)
	}
	os.Exit(0)
}

func runPiNativeHelper(mode string) error {
	var launch piLaunch
	if json.Unmarshal([]byte(os.Getenv(launchEnvironmentName)), &launch) != nil ||
		launch.Topology != "lane" || launch.OwnerPID != os.Getppid() {
		return errors.New("invalid helper launch descriptor")
	}
	for _, name := range []string{
		host.SocketEnv, host.LocalKeyEnv, host.TokenEnv, host.SessionIDEnv,
		host.NameEnv, host.GroupsEnv, "SESSIONBUS_OMP_LAUNCH",
	} {
		if _, present := os.LookupEnv(name); present {
			return fmt.Errorf("helper inherited %s", name)
		}
	}
	if mode == "exit" {
		return errors.New("controlled native exit")
	}
	if mode == "startup-ui" {
		_, _ = fmt.Fprintln(os.Stdout, `{"type":"extension_ui_request","id":"ui-1","method":"confirm"}`)
		_, _ = io.Copy(io.Discard, os.Stdin)
		return nil
	}
	connection, err := net.Dial("unix", launch.Socket)
	if err != nil {
		return err
	}
	defer connection.Close()
	cwd, err := os.Getwd()
	if err != nil {
		return err
	}
	id, name := "pi-fresh", ""
	arguments := os.Args[slices.Index(os.Args, "--")+1:]
	if len(arguments) < 4 || arguments[0] != "--extension" || !filepath.IsAbs(arguments[1]) ||
		arguments[2] != "--mode" || arguments[3] != "rpc" {
		return errors.New("helper received invalid managed arguments")
	}
	for index := 0; index < len(arguments); index++ {
		if arguments[index] == "--session" && index+1 < len(arguments) {
			id, name = arguments[index+1], "retained native title"
		}
	}
	sessionFile := filepath.Join(cwd, id+".jsonl")
	if err = os.WriteFile(sessionFile, []byte("owned native history\n"), 0o600); err != nil {
		return err
	}
	var bridge *pifamily.Bridge
	bridge, err = pifamily.NewBridge(connection, pifamily.BridgeNative,
		func(_ context.Context, method string, raw json.RawMessage) (json.RawMessage, error) {
			switch method {
			case "native.describe":
				return json.Marshal(piNativeDescription{SessionID: id, Name: name, Cwd: cwd})
			default:
				return nil, pifamily.NewBridgeCallError("method_not_found", "unexpected helper method")
			}
		}, pifamily.BridgeLimits{})
	if err != nil {
		return err
	}
	defer bridge.Close()
	if err = bridge.Ready(context.Background()); err != nil {
		return err
	}
	ready := piOwnerReady{Topology: "lane", Directory: launch.Directory, SessionID: id, Name: name}
	var acknowledged struct {
		SessionID string `json:"session_id"`
	}
	if err = bridge.Call(context.Background(), "owner.ready", ready, &acknowledged); err != nil {
		return err
	}
	if acknowledged.SessionID != id {
		return errors.New("helper owner changed native identity")
	}
	if mode == "startup-widget" {
		if _, err = fmt.Fprintln(os.Stdout, `{"type":"extension_ui_request","id":"startup-widget","method":"setWidget","widgetKey":"fixture"}`); err != nil {
			return err
		}
	}
	if mode == "startup-dialog" {
		if _, err = fmt.Fprintln(os.Stdout, `{"type":"extension_ui_request","id":"startup-dialog","method":"confirm","title":"startup","message":"continue"}`); err != nil {
			return err
		}
	}

	reader := bufio.NewScanner(os.Stdin)
	writer := bufio.NewWriter(os.Stdout)
	history := make([]map[string]any, 0)
	var leaf any
	if mode == "run-offbranch" {
		history = []map[string]any{
			{"id": "selected", "parentId": nil, "type": "custom", "data": map[string]any{"owned": true}},
			{"id": "file-tail", "parentId": nil, "type": "custom", "data": map[string]any{"other": true}},
		}
		leaf = "selected"
	}
	for reader.Scan() {
		var command map[string]json.RawMessage
		if json.Unmarshal(reader.Bytes(), &command) != nil {
			return errors.New("helper received invalid native RPC")
		}
		var requestID, kind string
		if json.Unmarshal(command["id"], &requestID) != nil || json.Unmarshal(command["type"], &kind) != nil {
			return errors.New("helper received invalid native RPC identity")
		}
		response := map[string]any{"id": requestID, "type": "response", "command": kind, "success": true}
		switch kind {
		case "get_state":
			response["data"] = piNativeState{SessionID: id, SessionName: name, SessionFile: sessionFile}
		case "set_session_name":
			if json.Unmarshal(command["name"], &name) != nil || name == "" {
				return errors.New("helper received invalid native name")
			}
		case "get_entries":
			if mode == "run-offbranch" {
				var since string
				if raw, present := command["since"]; present {
					if json.Unmarshal(raw, &since) != nil || since != "file-tail" {
						return fmt.Errorf("helper received wrong append cursor %q", since)
					}
					response["data"] = map[string]any{"entries": []map[string]any{
						{"id": "entry-user", "parentId": "selected", "type": "message", "message": map[string]any{"role": "user", "content": "owned prompt expanded"}},
						{"id": "entry-answer", "parentId": "entry-user", "type": "message", "message": map[string]any{"role": "assistant", "content": "native answer", "stopReason": "stop"}},
					}, "leafId": "entry-answer"}
				} else {
					response["data"] = map[string]any{"entries": history, "leafId": leaf}
				}
			} else {
				response["data"] = map[string]any{"entries": history, "leafId": leaf}
			}
		case "prompt":
			var prompt string
			if mode != "run" && mode != "run-abort" && mode != "run-offbranch" && mode != "run-hold" && mode != "handled" || json.Unmarshal(command["message"], &prompt) != nil || prompt == "" {
				return errors.New("helper received invalid native prompt")
			}
			var echo struct {
				SessionID string `json:"session_id"`
			}
			witnesses := []struct {
				method string
				params any
			}{
				{"run.input", map[string]any{"session_id": id, "source": "rpc", "text": prompt, "settling": false}},
				{"run.preflight", map[string]any{"session_id": id, "prompt": prompt + " expanded", "settling": false}},
				{"run.start", map[string]any{"session_id": id, "settling": false}},
			}
			if mode == "handled" {
				witnesses = nil
			} else if mode == "run-hold" {
				witnesses = witnesses[:1]
			}
			for _, witness := range witnesses {
				if err = bridge.Call(context.Background(), witness.method, witness.params, &echo); err != nil || echo.SessionID != id {
					return errors.Join(err, errors.New("helper Run witness was not acknowledged"))
				}
			}
			if mode == "run-hold" {
				continue
			}
			body, _ := json.Marshal(response)
			if _, err = writer.Write(append(body, '\n')); err != nil {
				return err
			}
			if mode == "handled" {
				if err = writer.Flush(); err != nil {
					return err
				}
				continue
			}
			for _, event := range []any{
				map[string]any{"type": "agent_start"},
				map[string]any{"type": "message_start", "message": map[string]any{"role": "user", "content": []any{map[string]string{"type": "text", "text": prompt + " expanded"}}}},
			} {
				body, _ = json.Marshal(event)
				if _, err = writer.Write(append(body, '\n')); err != nil {
					return err
				}
			}
			if err = writer.Flush(); err != nil {
				return err
			}
			if mode == "run-abort" {
				continue
			}
			if mode != "run-offbranch" {
				history = []map[string]any{
					{"id": "entry-user", "parentId": nil, "type": "message", "message": map[string]any{"role": "user", "content": prompt + " expanded"}},
					{"id": "entry-answer", "parentId": "entry-user", "type": "message", "message": map[string]any{"role": "assistant", "content": "native answer", "stopReason": "stop"}},
				}
				leaf = "entry-answer"
			}
			if err = bridge.Call(context.Background(), "run.settling", map[string]string{"session_id": id}, &echo); err != nil || echo.SessionID != id {
				return errors.Join(err, errors.New("helper settling witness was not acknowledged"))
			}
			if _, err = fmt.Fprintln(writer, `{"type":"agent_settled"}`); err != nil {
				return err
			}
			if err = writer.Flush(); err != nil {
				return err
			}
			continue
		case "abort":
			if mode == "run-abort" {
				history = []map[string]any{
					{"id": "entry-user", "parentId": nil, "type": "message", "message": map[string]any{"role": "user", "content": "owned prompt expanded"}},
					{"id": "entry-answer", "parentId": "entry-user", "type": "message", "message": map[string]any{"role": "assistant", "content": "", "stopReason": "aborted"}},
				}
				leaf = "entry-answer"
				var echo struct {
					SessionID string `json:"session_id"`
				}
				if err = bridge.Call(context.Background(), "run.settling", map[string]string{"session_id": id}, &echo); err != nil || echo.SessionID != id {
					return errors.Join(err, errors.New("helper abort settling witness was not acknowledged"))
				}
				if _, err = fmt.Fprintln(writer, `{"type":"agent_settled"}`); err != nil {
					return err
				}
				if err = writer.Flush(); err != nil {
					return err
				}
			}
		default:
			return fmt.Errorf("helper received unexpected command %s", kind)
		}
		body, _ := json.Marshal(response)
		if _, err = writer.Write(append(body, '\n')); err != nil {
			return err
		}
		if err = writer.Flush(); err != nil {
			return err
		}
		if kind == "set_session_name" {
			ready.Name = name
			if err = bridge.Call(context.Background(), "owner.ready", ready, &acknowledged); err != nil {
				return err
			}
		}
	}
	if err = reader.Err(); err != nil {
		return err
	}
	if mode == "malformed-shutdown" {
		_, err = fmt.Fprintln(os.Stdout, `{"id":"pi:999","type":"response","command":"get_state","success":true,"data":{}}`)
		return err
	}
	var ended struct {
		SessionID string `json:"session_id"`
	}
	err = bridge.Call(context.Background(), "session_end", map[string]string{
		"topology": "lane", "session_id": id, "reason": "quit",
	}, &ended)
	if err == nil && ended.SessionID != id {
		err = errors.New("helper shutdown identity changed")
	}
	return err
}

func piHelperCommand(_ string, arguments ...string) *exec.Cmd {
	owned := append([]string{"-test.run=^TestPiNativeHelper$", "--"}, arguments...)
	return exec.Command(os.Args[0], owned...)
}

func newPiTestWrapper(t *testing.T, mode string) (*Wrapper, string) {
	t.Helper()
	t.Setenv(piNativeHelperEnv, mode)
	previous := piCommand
	piCommand = piHelperCommand
	t.Cleanup(func() { piCommand = previous })
	directory := testsocket.Directory(t)
	extension := filepath.Join(directory, "extension.mjs")
	if err := os.WriteFile(extension, []byte("export default () => {}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	wrapper := New(filepath.Join(directory, "daemon.sock"), "provisional", executable, extension)
	wrapper.SetCaller(sessionkit.NewCaller(func(_ context.Context, _ string, _ any) (json.RawMessage, error) {
		return json.RawMessage(`{}`), nil
	}))
	return wrapper, directory
}

func TestPiOwnedOpenCloseFreshAndResume(t *testing.T) {
	for _, test := range []struct {
		name, resume, wantID, wantName string
	}{
		{name: "fresh", wantID: "pi-fresh", wantName: "managed"},
		{name: "resume", resume: "pi-retained", wantID: "pi-retained", wantName: "retained native title"},
	} {
		t.Run(test.name, func(t *testing.T) {
			wrapper, cwd := newPiTestWrapper(t, "normal")
			result, err := wrapper.Open(context.Background(), sessionkit.OpenRequest{
				Name: "managed@local", ResumeSessionID: test.resume,
				Open: sessionkit.OpenOptions{Cwd: cwd},
			})
			if err != nil {
				t.Fatal(err)
			}
			if result.SessionID != test.wantID || wrapper.owner.Name != test.wantName {
				t.Fatalf("result=%+v owner=%+v", result, wrapper.owner)
			}
			launchDirectory := wrapper.process.directory
			if err = wrapper.Close(context.Background(), sessionkit.SessionCloseRequest{}); err != nil {
				t.Fatal(err)
			}
			if _, err = os.Stat(launchDirectory); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("private launch directory survived Close: %v", err)
			}
			if stats := wrapper.rpc.Stats(); stats.pendingCalls != 0 || stats.pendingWrites != 0 || stats.retainedBytes != 0 {
				t.Fatalf("native RPC retained work: %+v", stats)
			}
			if stats := wrapper.bridge.Stats(); stats.PendingCalls != 0 || stats.ActiveCalls != 0 || stats.PendingWrites != 0 || stats.RetainedBytes != 0 {
				t.Fatalf("bridge retained work: %+v", stats)
			}
		})
	}
}

func TestPiOpenAcceptsStartupAndIdleNotifications(t *testing.T) {
	wrapper, cwd := newPiTestWrapper(t, "startup-widget")
	result, err := wrapper.Open(context.Background(), sessionkit.OpenRequest{
		Name: "managed@local", Open: sessionkit.OpenOptions{Cwd: cwd},
	})
	if err != nil || result.SessionID != "pi-fresh" {
		t.Fatalf("fresh Open after startup setWidget = %+v, %v", result, err)
	}
	if err = wrapper.observeNative(json.RawMessage(
		`{"type":"extension_ui_request","id":"idle-widget","method":"setWidget","widgetKey":"fixture"}`,
	)); err != nil {
		t.Fatalf("idle setWidget = %v", err)
	}
	wrapper.mu.Lock()
	failure := wrapper.failure
	wrapper.mu.Unlock()
	if failure != nil {
		t.Fatalf("idle setWidget retired wrapper: %v", failure)
	}
	if err = wrapper.Close(context.Background(), sessionkit.SessionCloseRequest{}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-wrapper.process.done:
	default:
		t.Fatal("Close returned before native process joined")
	}
}

func TestPiOpenRejectsStartupDialogWithMethod(t *testing.T) {
	wrapper, cwd := newPiTestWrapper(t, "startup-dialog")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result, err := wrapper.Open(ctx, sessionkit.OpenRequest{
		Name: "managed@local", Open: sessionkit.OpenOptions{Cwd: cwd},
	})
	if err == nil || !strings.Contains(err.Error(), `unsupported startup UI method "confirm"`) {
		t.Fatalf("startup dialog Open = %+v, %v", result, err)
	}
	if wrapper.opened {
		t.Fatal("startup dialog committed Open")
	}
	if wrapper.process != nil {
		select {
		case <-wrapper.process.done:
		default:
			t.Fatal("failed Open returned before native process joined")
		}
	}
	unknown := &Wrapper{}
	if err = unknown.observeNativeUI(json.RawMessage(
		`{"type":"extension_ui_request","id":"startup-unknown","method":"future"}`,
	)); err == nil || !strings.Contains(err.Error(), `unknown extension UI method "future"`) {
		t.Fatalf("unknown startup method = %v", err)
	}
}

func TestPiCloseForgetLeavesNativeHistoryToDaemonRowOwnership(t *testing.T) {
	wrapper, cwd := newPiTestWrapper(t, "normal")
	if _, err := wrapper.Open(context.Background(), sessionkit.OpenRequest{
		Name: "managed@local", Open: sessionkit.OpenOptions{Cwd: cwd},
	}); err != nil {
		t.Fatal(err)
	}
	history := filepath.Join(cwd, "pi-fresh.jsonl")
	launchDirectory := wrapper.process.directory
	if err := wrapper.Close(context.Background(), sessionkit.SessionCloseRequest{Forget: true}); err != nil {
		t.Fatal(err)
	}
	if body, err := os.ReadFile(history); err != nil || string(body) != "owned native history\n" {
		t.Fatalf("native history after row forget = %q, %v", body, err)
	}
	if _, err := os.Stat(launchDirectory); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("private launch directory survived Forget Close: %v", err)
	}
}

func TestPiOpenFailureJoinsOwnedProcessAndResources(t *testing.T) {
	for _, mode := range []string{"exit", "startup-ui"} {
		t.Run(mode, func(t *testing.T) {
			wrapper, cwd := newPiTestWrapper(t, mode)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, err := wrapper.Open(ctx, sessionkit.OpenRequest{Name: "managed@local", Open: sessionkit.OpenOptions{Cwd: cwd}})
			if err == nil {
				t.Fatal("Open unexpectedly succeeded")
			}
			if wrapper.process != nil {
				select {
				case <-wrapper.process.done:
				case <-ctx.Done():
					t.Fatal("owned native process was not joined")
				}
				if _, statErr := os.Stat(wrapper.process.directory); !errors.Is(statErr, os.ErrNotExist) {
					t.Fatalf("private launch directory survived failed Open: %v", statErr)
				}
			}
		})
	}
}

func TestPiCancelledCloseForcesAndJoinsExactChild(t *testing.T) {
	wrapper, cwd := newPiTestWrapper(t, "normal")
	if _, err := wrapper.Open(context.Background(), sessionkit.OpenRequest{
		Name: "managed@local", Open: sessionkit.OpenOptions{Cwd: cwd},
	}); err != nil {
		t.Fatal(err)
	}
	launchDirectory := wrapper.process.directory
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := wrapper.Close(ctx, sessionkit.SessionCloseRequest{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Close error %v does not retain cancellation", err)
	}
	select {
	case <-wrapper.process.done:
	default:
		t.Fatal("forced child was not joined")
	}
	if _, statErr := os.Stat(launchDirectory); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("private launch directory survived forced Close: %v", statErr)
	}
}

func TestPiCloseWaitsForStartupResourceAdmission(t *testing.T) {
	wrapper, cwd := newPiTestWrapper(t, "normal")
	entered, release := make(chan struct{}), make(chan struct{})
	previous := piStartProcess
	piStartProcess = func(socket, provisional, executable, cwd string, arguments []string) (*piProcess, error) {
		close(entered)
		<-release
		return startPiProcess(socket, provisional, executable, cwd, arguments)
	}
	t.Cleanup(func() { piStartProcess = previous })
	opened := make(chan error, 1)
	go func() {
		_, err := wrapper.Open(context.Background(), sessionkit.OpenRequest{
			Name: "managed@local", Open: sessionkit.OpenOptions{Cwd: cwd},
		})
		opened <- err
	}()
	<-entered
	closed := make(chan error, 1)
	go func() { closed <- wrapper.Close(context.Background(), sessionkit.SessionCloseRequest{}) }()
	for {
		wrapper.mu.Lock()
		closing := wrapper.closing
		wrapper.mu.Unlock()
		if closing {
			break
		}
		runtime.Gosched()
	}
	select {
	case err := <-closed:
		t.Fatalf("Close returned before startup resource ownership settled: %v", err)
	default:
	}
	close(release)
	if err := <-opened; err == nil {
		t.Fatal("Open succeeded across Close")
	}
	if err := <-closed; err == nil {
		t.Fatal("Close did not retain canceled startup failure")
	}
	if wrapper.process == nil {
		t.Fatal("held process was not published for cleanup")
	}
	if _, err := os.Stat(wrapper.process.directory); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("held startup directory survived Close: %v", err)
	}
}

func TestPiClosePreservesMalformedTerminalWire(t *testing.T) {
	wrapper, cwd := newPiTestWrapper(t, "malformed-shutdown")
	if _, err := wrapper.Open(context.Background(), sessionkit.OpenRequest{
		Name: "managed@local", Open: sessionkit.OpenOptions{Cwd: cwd},
	}); err != nil {
		t.Fatal(err)
	}
	err := wrapper.Close(context.Background(), sessionkit.SessionCloseRequest{})
	if !errors.Is(err, errNativeRPCProtocol) {
		t.Fatalf("Close error %v omitted terminal protocol failure", err)
	}
}

func TestPiCloseJoinsLossShutdownWork(t *testing.T) {
	wrapper, cwd := newPiTestWrapper(t, "normal")
	entered, release := make(chan struct{}), make(chan struct{})
	wrapper.SetShutdown(func() { close(entered); <-release })
	if _, err := wrapper.Open(context.Background(), sessionkit.OpenRequest{
		Name: "managed@local", Open: sessionkit.OpenOptions{Cwd: cwd},
	}); err != nil {
		t.Fatal(err)
	}
	wrapper.lose(errors.New("controlled owner loss"))
	<-entered
	closed := make(chan error, 1)
	go func() { closed <- wrapper.Close(context.Background(), sessionkit.SessionCloseRequest{}) }()
	select {
	case <-closed:
		t.Fatal("Close returned before loss shutdown work joined")
	default:
	}
	close(release)
	if err := <-closed; !strings.Contains(err.Error(), "controlled owner loss") {
		t.Fatalf("Close error %v omitted owner loss", err)
	}
}

func TestPiConcurrentTransportAndHandlerLossDoesNotSelfJoin(t *testing.T) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()
	handlerEntered, reportHandlerLoss := make(chan struct{}), make(chan struct{})
	wrapper := New("/socket", "provisional", "/native", "/extension")
	wrapper.ctx, wrapper.cancel = context.WithCancelCause(context.Background())
	wrapper.opened, wrapper.id = true, "native"
	wrapper.SetCaller(sessionkit.NewCaller(func(_ context.Context, _ string, _ any) (json.RawMessage, error) {
		close(handlerEntered)
		<-reportHandlerLoss
		wrapper.loseFromHandler(errors.New("handler loss"))
		return nil, errors.New("controlled action failure")
	}))
	hostBridge, err := pifamily.NewBridge(left, pifamily.BridgeHost, wrapper.handleBridge, pifamily.BridgeLimits{})
	if err != nil {
		t.Fatal(err)
	}
	nativeBridge, err := pifamily.NewBridge(right, pifamily.BridgeNative, nil, pifamily.BridgeLimits{})
	if err != nil {
		t.Fatal(err)
	}
	wrapper.bridge = hostBridge
	ready := make(chan error, 2)
	go func() { ready <- hostBridge.Ready(context.Background()) }()
	go func() { ready <- nativeBridge.Ready(context.Background()) }()
	if err = <-ready; err != nil {
		t.Fatal(err)
	}
	if err = <-ready; err != nil {
		t.Fatal(err)
	}
	callDone := make(chan error, 1)
	go func() {
		var result json.RawMessage
		callDone <- nativeBridge.Call(context.Background(), "tool.call", map[string]any{
			"session_id": "native", "call_id": "call", "action": "list", "arguments": map[string]any{},
		}, &result)
	}()
	<-handlerEntered
	lossDone := make(chan struct{})
	go func() { wrapper.lose(errors.New("RPC loss")); close(lossDone) }()
	for {
		wrapper.mu.Lock()
		losing := wrapper.losing
		wrapper.mu.Unlock()
		if losing {
			break
		}
		runtime.Gosched()
	}
	close(reportHandlerLoss)
	select {
	case <-lossDone:
	case <-time.After(time.Second):
		t.Fatal("transport loss waited on its own active handler")
	}
	select {
	case <-callDone:
	case <-time.After(time.Second):
		t.Fatal("native caller did not settle after joined loss")
	}
	_ = nativeBridge.Close()
}

func TestPiHelloAndLaneDeliveryPolicy(t *testing.T) {
	wrapper := New("/socket", "provisional", "/native", "/extension")
	hello, err := wrapper.Hello(context.Background())
	if err != nil || hello.Product != Product || !hello.SupportsMessageRun {
		t.Fatalf("hello=%+v err=%v", hello, err)
	}
	wrapper.mu.Lock()
	wrapper.opened = true
	wrapper.ctx = context.Background()
	wrapper.mu.Unlock()
	receipt, err := wrapper.Deliver(context.Background(), sessionkit.DeliveryRequest{
		MessageID: "message", Body: "body",
		From: sessionkit.DeliverySource{SessionID: "source@local", Product: "example-peer"},
	}, nil)
	var notRunning *sessionkit.ProtocolError
	if !errors.As(err, &notRunning) || notRunning.Code != -32004 || receipt.Disposition != "" {
		t.Fatalf("receipt=%+v err=%v", receipt, err)
	}
}

func TestPiNameAndStateValidation(t *testing.T) {
	if _, err := piNamePart("missing"); err == nil {
		t.Fatal("unqualified name accepted")
	}
	if err := validatePiState(piNativeState{SessionID: "actual"}, "actual", "wanted"); err == nil {
		t.Fatal("resume identity substitution accepted")
	}
	if err := validatePiState(piNativeState{SessionID: "actual", IsStreaming: true}, "actual", ""); err == nil {
		t.Fatal("busy native state accepted")
	}
	if !strings.Contains((&piLog{data: []byte("stderr")}).String(), "stderr") {
		t.Fatal("bounded stderr unavailable")
	}
}
