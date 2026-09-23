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
	"os/signal"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	kit "github.com/antst/sessionbus/bus/sdk/go"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
	"github.com/sessionbus/peer-common/host"
	"github.com/sessionbus/peer-common/testsocket"
	"github.com/sessionbus/pi-omp/wrappers/pifamily"
)

const piInteractiveChildEnv = "PI_INTERACTIVE_TEST_CHILD"

type piInteractiveChildCapture struct {
	Args       []string                    `json:"args"`
	Descriptor interactiveLaunchDescriptor `json:"descriptor"`
	PID        int                         `json:"pid"`
	Parent     int                         `json:"parent"`
	Leaked     []string                    `json:"leaked"`
	Signal     string                      `json:"signal,omitempty"`
}

func TestPiInteractiveNativeChild(t *testing.T) {
	mode := os.Getenv(piInteractiveChildEnv)
	if mode == "" {
		return
	}
	index := 0
	for index < len(os.Args) && os.Args[index] != "--" {
		index++
	}
	var descriptor interactiveLaunchDescriptor
	if index == len(os.Args) || json.Unmarshal([]byte(os.Getenv(InteractiveLaunchEnv)), &descriptor) != nil {
		os.Exit(91)
	}
	capture := piInteractiveChildCapture{Args: slicesClone(os.Args[index+1:]), Descriptor: descriptor, PID: os.Getpid(), Parent: os.Getppid()}
	for _, entry := range os.Environ() {
		key, _, _ := strings.Cut(entry, "=")
		if strings.HasPrefix(key, "SESSIONBUS_") && key != InteractiveLaunchEnv {
			capture.Leaked = append(capture.Leaked, key)
		}
	}
	writeCapture := func() {
		body, marshalErr := json.Marshal(capture)
		if marshalErr != nil || os.WriteFile(os.Getenv("PI_INTERACTIVE_TEST_CAPTURE"), body, 0o600) != nil {
			os.Exit(95)
		}
	}
	conn, err := net.Dial("unix", descriptor.Socket)
	if err != nil {
		os.Exit(92)
	}
	if mode == "connect-exit" {
		writeCapture()
		// Connecting does not prove the parent has accepted the descriptor.
		// Exit only after the held listener owns it, before its handoff.
		_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		var accepted [1]byte
		if _, readErr := io.ReadFull(conn, accepted[:]); readErr != nil || accepted[0] != 1 {
			os.Exit(98)
		}
		_ = conn.Close()
		os.Exit(38)
	}
	native, err := pifamily.NewBridge(conn, pifamily.BridgeNative, func(_ context.Context, method string, raw json.RawMessage) (json.RawMessage, error) {
		var result any
		switch method {
		case "native.describe":
			var request struct {
				SessionID string `json:"session_id"`
			}
			if decodeInteractiveParams(raw, &request) != nil || request.SessionID != "native-session" {
				return nil, pifamily.NewBridgeCallError("bad_request", "wrong native description")
			}
			result = interactiveDescribeResult{"native-session", "native title", "/native/work"}
		case "native.append":
			return nil, pifamily.NewBridgeCallError("bad_request", "unexpected append")
		default:
			return nil, pifamily.NewBridgeCallError("method_not_found", "unexpected native call")
		}
		return json.Marshal(result)
	}, pifamily.BridgeLimits{})
	if err != nil || native.Ready(context.Background()) != nil {
		os.Exit(93)
	}
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM, syscall.SIGHUP)
	if mode == "initial-absence" {
		writeCapture()
	}
	var ready map[string]string
	if native.Call(context.Background(), "owner.ready", interactiveReadyRequest{
		Topology: interactiveTopology, Directory: descriptor.Directory, SessionID: "native-session", Name: "ready title",
	}, &ready) != nil || ready["session_id"] != "native-session" {
		os.Exit(94)
	}
	writeCapture()
	if mode == "bridge-loss" {
		_ = native.Close()
	}
	if mode == "exit-37" {
		var ended map[string]string
		if native.Call(context.Background(), "session_end", interactiveEndRequest{interactiveTopology, "native-session", "quit"}, &ended) != nil {
			os.Exit(96)
		}
		_ = native.Close()
		os.Exit(37)
	}
	received := <-signals
	signal.Stop(signals)
	capture.Signal = received.String()
	writeCapture()
	if mode != "bridge-loss" {
		var ended map[string]string
		if native.Call(context.Background(), "session_end", interactiveEndRequest{interactiveTopology, "native-session", "quit"}, &ended) != nil || ended["session_id"] != "native-session" {
			os.Exit(97)
		}
		_ = native.Close()
	}
	os.Exit(0)
}

func slicesClone[T any](values []T) []T { return append([]T(nil), values...) }

func installPiInteractiveCommandFixture(t *testing.T) {
	t.Helper()
	original := piInteractiveCommand
	piInteractiveCommand = func(_ string, arguments ...string) *exec.Cmd {
		args := []string{"-test.run=^TestPiInteractiveNativeChild$", "--"}
		return exec.Command(os.Args[0], append(args, arguments...)...)
	}
	t.Cleanup(func() { piInteractiveCommand = original })
}

type heldPiAcceptListener struct {
	net.Listener
	accepted chan struct{}
	release  chan struct{}
	closed   chan struct{}
}

type observedPiConn struct {
	net.Conn
	closed chan struct{}
	once   sync.Once
}

func (c *observedPiConn) Close() error {
	c.once.Do(func() { close(c.closed) })
	return c.Conn.Close()
}

func (l *heldPiAcceptListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	if _, err = conn.Write([]byte{1}); err != nil {
		_ = conn.Close()
		return nil, err
	}
	close(l.accepted)
	<-l.release
	return &observedPiConn{Conn: conn, closed: l.closed}, nil
}

func readPiChildCapture(t *testing.T, path string) piInteractiveChildCapture {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		body, err := os.ReadFile(path)
		var capture piInteractiveChildCapture
		if err == nil && json.Unmarshal(body, &capture) == nil {
			return capture
		}
		if time.Now().After(deadline) {
			t.Fatalf("missing native capture: %v", err)
		}
		time.Sleep(time.Millisecond)
	}
}

func servePiInteractiveHello(t *testing.T, listener net.Listener, cancel context.CancelFunc) (kit.PeerIdentity, <-chan struct{}) {
	t.Helper()
	identity := make(chan kit.PeerIdentity, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		scanner := bufio.NewScanner(conn)
		if !scanner.Scan() {
			return
		}
		frame, err := protocol.DecodeFrame(scanner.Bytes())
		if err != nil || !frame.Request || frame.Method != "session.hello" {
			return
		}
		params, err := protocol.DecodeParams(frame.Method, frame.Params)
		hello, ok := params.(*protocol.PeerHello)
		if err != nil || !ok {
			return
		}
		body, err := protocol.ResultBytes(frame.ID, frame.Method, struct{}{})
		if err != nil {
			return
		}
		if _, err = conn.Write(body); err != nil {
			return
		}
		identity <- *hello
		if cancel != nil {
			cancel()
		}
		for scanner.Scan() {
		}
	}()
	select {
	case hello := <-identity:
		return hello, done
	case <-time.After(5 * time.Second):
		t.Fatal("Pi public hello did not arrive")
		return kit.PeerIdentity{}, done
	}
}

func piInteractiveLaunchPlan(t *testing.T, listener net.Listener, capture, mode string) host.ExecPlan {
	t.Helper()
	return piInteractiveLaunchPlanAt(t, listener.Addr().String(), capture, mode)
}

func piInteractiveLaunchPlanAt(t *testing.T, socket, capture, mode string) host.ExecPlan {
	t.Helper()
	t.Setenv(piInteractiveChildEnv, mode)
	t.Setenv("PI_INTERACTIVE_TEST_CAPTURE", capture)
	return host.ExecPlan{
		Path: "/native/pi",
		Args: []string{"--model", "fixture/model"},
		Env: append(os.Environ(),
			host.SocketEnv+"="+socket,
			host.GroupsEnv+`=["team"]`,
			host.NameEnv+"=wrapper title",
			host.SessionIDEnv+"=stale",
			host.TokenEnv+"=stale",
		),
	}
}

func TestRunPiInteractiveWaitsForInitiallyAbsentDaemonWithoutRestartingNative(t *testing.T) {
	installPiInteractiveCommandFixture(t)
	originalInterval := piInteractiveReconnectInterval
	piInteractiveReconnectInterval = 10 * time.Millisecond
	t.Cleanup(func() { piInteractiveReconnectInterval = originalInterval })
	root := testsocket.Directory(t)
	socket := filepath.Join(root, "absent.sock")
	ctx, cancel := context.WithCancel(context.Background())
	capturePath := filepath.Join(t.TempDir(), "native.json")
	plan := piInteractiveLaunchPlanAt(t, socket, capturePath, "initial-absence")
	result := make(chan error, 1)
	go func() { result <- runInteractiveResolved(ctx, plan, "/native/pi", "/plugin/pi/extension.mjs") }()
	initial := readPiChildCapture(t, capturePath)
	if initial.PID <= 0 || initial.Parent != os.Getpid() || syscall.Kill(initial.PID, 0) != nil {
		t.Fatalf("initial native generation = %+v", initial)
	}
	select {
	case err := <-result:
		t.Fatalf("launcher ended while daemon was absent: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	listener := interactiveBusListenerAt(t, socket)
	conn := interactiveAccept(t, listener)
	hello := interactiveHello(t, conn, bufio.NewScanner(conn))
	if hello.SessionID != "native-session" || hello.Name != "native title" || hello.Info["cwd"] != "/native/work" {
		t.Fatalf("delayed hello = %+v", hello)
	}
	cancel()
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	final := readPiChildCapture(t, capturePath)
	if final.PID != initial.PID || final.Parent != initial.Parent || final.Descriptor != initial.Descriptor || final.Signal != "terminated" {
		t.Fatalf("native generation changed across absent daemon: initial=%+v final=%+v", initial, final)
	}
	if _, err := os.Stat(final.Descriptor.Directory); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("private directory remains: %v", err)
	}
}

func TestRunPiInteractiveOwnsBridgeIdentityAndGracefulSignalCleanup(t *testing.T) {
	installPiInteractiveCommandFixture(t)
	listener := interactiveBusListener(t)
	ctx, cancel := context.WithCancel(context.Background())
	capturePath := filepath.Join(t.TempDir(), "native.json")
	plan := piInteractiveLaunchPlan(t, listener, capturePath, "signal")
	result := make(chan error, 1)
	go func() { result <- runInteractiveResolved(ctx, plan, "/native/pi", "/plugin/pi/extension.mjs") }()
	hello, busDone := servePiInteractiveHello(t, listener, cancel)
	if hello.Product != Product || hello.SessionID != "native-session" || hello.Name != "native title" || hello.Info["cwd"] != "/native/work" || !reflect.DeepEqual(hello.Groups, []string{"team"}) {
		t.Fatalf("hello = %+v", hello)
	}
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	<-busDone
	capture := readPiChildCapture(t, capturePath)
	if capture.Parent != os.Getpid() || capture.Descriptor.OwnerPID != os.Getpid() || capture.Descriptor.Topology != interactiveTopology ||
		capture.Descriptor.Directory == "" || capture.Descriptor.Socket != filepath.Join(capture.Descriptor.Directory, interactiveBridgeSocket) ||
		!reflect.DeepEqual(capture.Args, []string{"--extension", "/plugin/pi/extension.mjs", "--model", "fixture/model"}) || len(capture.Leaked) != 0 || capture.Signal != "terminated" {
		t.Fatalf("native capture = %+v", capture)
	}
	if _, err := os.Stat(capture.Descriptor.Directory); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("private directory remains: %v", err)
	}
}

func TestPiInteractiveBridgeResultOnlyNormalizesJoinedGracefulQuit(t *testing.T) {
	closed := fmt.Errorf("%w: peer closed the connection", pifamily.ErrBridgeClosed)
	protocolErr := fmt.Errorf("%w: malformed frame", pifamily.ErrBridgeProtocol)
	childErr := errors.New("native child failed")
	graceful := &interactiveOwner{everReady: true, lastEndReason: "quit"}
	nonGraceful := &interactiveOwner{everReady: true, lastEndReason: "reload"}

	if err := piInteractiveJoinedBridgeResult(graceful, nil, closed); err != nil {
		t.Fatalf("joined graceful quit retained expected bridge EOF: %v", err)
	}
	if err := piInteractiveJoinedBridgeResult(nonGraceful, nil, closed); !errors.Is(err, pifamily.ErrBridgeClosed) {
		t.Fatalf("non-graceful bridge loss was hidden: %v", err)
	}
	if err := piInteractiveJoinedBridgeResult(graceful, nil, protocolErr); !errors.Is(err, pifamily.ErrBridgeProtocol) {
		t.Fatalf("graceful child hid bridge protocol failure: %v", err)
	}
	if err := piInteractiveJoinedBridgeResult(graceful, childErr, closed); !errors.Is(err, childErr) || !errors.Is(err, pifamily.ErrBridgeClosed) {
		t.Fatalf("failed child or its bridge loss was hidden: %v", err)
	}
}

func TestRunPiInteractiveReconnectsPublicIdentityWithoutRestartingNative(t *testing.T) {
	installPiInteractiveCommandFixture(t)
	originalInterval := piInteractiveReconnectInterval
	piInteractiveReconnectInterval = 10 * time.Millisecond
	t.Cleanup(func() { piInteractiveReconnectInterval = originalInterval })
	listener := interactiveBusListener(t)
	ctx, cancel := context.WithCancel(context.Background())
	capturePath := filepath.Join(t.TempDir(), "native.json")
	plan := piInteractiveLaunchPlan(t, listener, capturePath, "signal")
	result := make(chan error, 1)
	go func() { result <- runInteractiveResolved(ctx, plan, "/native/pi", "/plugin/pi/extension.mjs") }()

	first := interactiveAccept(t, listener)
	firstHello := interactiveHello(t, first, bufio.NewScanner(first))
	if firstHello.SessionID != "native-session" || firstHello.Name != "native title" || firstHello.Info["cwd"] != "/native/work" {
		t.Fatalf("first hello = %+v", firstHello)
	}
	initial := readPiChildCapture(t, capturePath)
	if initial.PID <= 0 || initial.Parent != os.Getpid() || syscall.Kill(initial.PID, 0) != nil {
		t.Fatalf("initial native generation = %+v", initial)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	second := interactiveAccept(t, listener)
	secondHello := interactiveHello(t, second, bufio.NewScanner(second))
	if !reflect.DeepEqual(secondHello, firstHello) {
		t.Fatalf("reconnected hello = %+v, want %+v", secondHello, firstHello)
	}
	if syscall.Kill(initial.PID, 0) != nil {
		t.Fatalf("native generation %d ended across public reconnect", initial.PID)
	}

	cancel()
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	_ = second.Close()
	final := readPiChildCapture(t, capturePath)
	if final.PID != initial.PID || final.Parent != initial.Parent || final.Descriptor != initial.Descriptor || final.Signal != "terminated" {
		t.Fatalf("native generation changed across reconnect: initial=%+v final=%+v", initial, final)
	}
	if _, err := os.Stat(final.Descriptor.Directory); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("private directory remains: %v", err)
	}
}

func TestRunPiInteractivePropagatesNativeExitAndContainsBridgeLoss(t *testing.T) {
	for _, mode := range []string{"exit-37", "bridge-loss"} {
		t.Run(mode, func(t *testing.T) {
			installPiInteractiveCommandFixture(t)
			listener := interactiveBusListener(t)
			capturePath := filepath.Join(t.TempDir(), "native.json")
			plan := piInteractiveLaunchPlan(t, listener, capturePath, mode)
			result := make(chan error, 1)
			go func() {
				result <- runInteractiveResolved(context.Background(), plan, "/native/pi", "/plugin/pi/extension.mjs")
			}()
			hello, busDone := servePiInteractiveHello(t, listener, nil)
			if hello.SessionID != "native-session" {
				t.Fatalf("hello = %+v", hello)
			}
			err := <-result
			<-busDone
			capture := readPiChildCapture(t, capturePath)
			if mode == "exit-37" {
				var exit *exec.ExitError
				if !errors.As(err, &exit) || exit.ExitCode() != 37 {
					t.Fatalf("native exit = %v", err)
				}
			} else if err == nil || !errors.Is(err, pifamily.ErrBridgeClosed) || capture.Signal != "terminated" {
				t.Fatalf("bridge loss = %v, capture=%+v", err, capture)
			}
			if _, statErr := os.Stat(capture.Descriptor.Directory); !errors.Is(statErr, os.ErrNotExist) {
				t.Fatalf("private directory remains: %v", statErr)
			}
		})
	}
}

func TestRunPiInteractiveClosesAcceptedConnectionWhenChildAlreadyExited(t *testing.T) {
	installPiInteractiveCommandFixture(t)
	originalListen := piInteractiveListen
	accepted, release, closed := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	releaseAccept := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseAccept()
	piInteractiveListen = func(network, address string) (net.Listener, error) {
		listener, err := originalListen(network, address)
		if err != nil {
			return nil, err
		}
		return &heldPiAcceptListener{Listener: listener, accepted: accepted, release: release, closed: closed}, nil
	}
	t.Cleanup(func() { piInteractiveListen = originalListen })
	listener := interactiveBusListener(t)
	capturePath := filepath.Join(t.TempDir(), "native.json")
	plan := piInteractiveLaunchPlan(t, listener, capturePath, "connect-exit")
	result := make(chan error, 1)
	go func() {
		result <- runInteractiveResolved(context.Background(), plan, "/native/pi", "/plugin/pi/extension.mjs")
	}()
	select {
	case <-accepted:
	case err := <-result:
		t.Fatalf("launcher ended before held accept: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("native connection was not accepted")
	}
	capture := readPiChildCapture(t, capturePath)
	deadline := time.Now().Add(5 * time.Second)
	for syscall.Kill(capture.PID, 0) == nil && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if syscall.Kill(capture.PID, 0) == nil {
		t.Fatal("native child did not exit before accept handoff")
	}
	releaseAccept()
	var exit *exec.ExitError
	if err := <-result; !errors.As(err, &exit) || exit.ExitCode() != 38 {
		t.Fatalf("native exit = %v", err)
	}
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("accepted connection was not closed")
	}
	if _, err := os.Stat(capture.Descriptor.Directory); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("private directory remains: %v", err)
	}
}
