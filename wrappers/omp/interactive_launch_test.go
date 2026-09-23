// SPDX-License-Identifier: MIT

package omp

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/sessionbus/peer-common/host"

	"github.com/sessionbus/peer-common/testsocket"
)

type fakeInteractiveNativeOwner struct {
	ready   chan struct{}
	done    chan struct{}
	binding OwnerBinding
	err     error
	closed  int
	mu      sync.Mutex
}

func newFakeInteractiveNativeOwner() *fakeInteractiveNativeOwner {
	return &fakeInteractiveNativeOwner{ready: make(chan struct{}), done: make(chan struct{})}
}

func (owner *fakeInteractiveNativeOwner) Ready() <-chan struct{} { return owner.ready }
func (owner *fakeInteractiveNativeOwner) Done() <-chan struct{}  { return owner.done }
func (owner *fakeInteractiveNativeOwner) Err() error             { return owner.err }
func (owner *fakeInteractiveNativeOwner) Primary() (OwnerBinding, bool) {
	return owner.binding, owner.binding.SessionID != ""
}
func (owner *fakeInteractiveNativeOwner) Close(context.Context) error {
	owner.mu.Lock()
	owner.closed++
	owner.mu.Unlock()
	return nil
}

func TestInteractiveOwnerOptionsPreserveNativeArgumentsAndBindIdentity(t *testing.T) {
	root := testsocket.Directory(t)
	plugin := managedPluginFixture(t, root)
	extension := filepath.Join(plugin, "omp", "extension.mjs")
	native := NativeExecutable{
		Path: filepath.Join(root, "bin", "omp"), EntryPath: filepath.Join(root, "package", "dist", "cli.js"),
		RuntimePath: filepath.Join(root, "bin", "bun"), PackageRoot: filepath.Join(root, "package"),
	}
	arguments := []string{native.EntryPath, "--cwd", "relative/project", "--resume=session-id", "--profile", "work", "--", "literal"}
	plan := host.ExecPlan{Path: native.RuntimePath, Args: arguments, Env: []string{
		"KEEP=value", host.SocketEnv + "=" + filepath.Join(root, "daemon.sock"),
		host.GroupsEnv + `=["one","two"]`, host.NameEnv + "=initial name",
	}}
	options, err := interactiveOwnerOptions(context.Background(), plan, native, extension, bytes.NewReader(bytes.Repeat([]byte{0x2a}, 16)))
	if err != nil {
		t.Fatal(err)
	}
	wantArguments := arguments[1:]
	if options.DaemonSocket != filepath.Join(root, "daemon.sock") || options.Provisional != "interactive-"+strings.Repeat("2a", 16) ||
		options.Topology != ownerTopologyInteractive || options.InitialName != "initial name" || options.Extension != extension ||
		options.PrimaryCaller != nil || options.NativeObserver != nil || options.Native != native ||
		!reflect.DeepEqual(options.Groups, []string{"one", "two"}) || !reflect.DeepEqual(options.Arguments, wantArguments) {
		t.Fatalf("options = %#v", options)
	}
	physical, err := filepath.EvalSymlinks(mustGetwd(t))
	if err != nil || options.CWD != physical {
		t.Fatalf("cwd = %q, want %q (%v)", options.CWD, physical, err)
	}
	arguments[1] = "changed"
	if options.Arguments[0] != "--cwd" {
		t.Fatal("native arguments were not retained independently")
	}
}

func TestInteractivePlanSocketPolicyPassesRealOwnerOptionsBoundary(t *testing.T) {
	root := testsocket.Directory(t)
	t.Setenv(host.SocketEnv, "")
	t.Setenv("XDG_RUNTIME_DIR", filepath.Join(root, "runtime"))
	plugin := managedPluginFixture(t, root)
	extension := filepath.Join(plugin, "omp", "extension.mjs")
	native := NativeExecutable{
		Path: filepath.Join(root, "bin", "omp"), EntryPath: filepath.Join(root, "package", "dist", "cli.js"),
		RuntimePath: filepath.Join(root, "bin", "bun"), PackageRoot: filepath.Join(root, "package"),
	}

	for _, test := range []struct {
		name        string
		environment []string
		wantSocket  string
	}{
		{
			name:        "sdk-default",
			environment: []string{"KEEP=value"},
			wantSocket:  filepath.Join(root, "runtime", "sessionbus", "presence.sock"),
		},
		{
			name:        "explicit",
			environment: []string{"KEEP=value", host.SocketEnv + "=" + filepath.Join(root, "explicit.sock")},
			wantSocket:  filepath.Join(root, "explicit.sock"),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			plan, passthrough, err := InteractivePlan(native, []string{"--peer-name", "fixture"}, test.environment)
			if err != nil || passthrough {
				t.Fatalf("plan = %#v, passthrough = %t, error = %v", plan, passthrough, err)
			}
			options, err := interactiveOwnerOptions(context.Background(), plan, native, extension, bytes.NewReader(make([]byte, 16)))
			if err != nil {
				t.Fatal(err)
			}
			if options.DaemonSocket != test.wantSocket || ompLastEnvironmentValue(plan.Env, host.SocketEnv) != test.wantSocket {
				t.Fatalf("socket plan=%q options=%q, want %q", ompLastEnvironmentValue(plan.Env, host.SocketEnv), options.DaemonSocket, test.wantSocket)
			}
			if ompLastEnvironmentValue(plan.Env, "KEEP") != "value" {
				t.Fatalf("unrelated environment = %#v", plan.Env)
			}
		})
	}
}

func TestInteractiveOwnerOptionsRejectMismatchedPlanAndStaleOwnership(t *testing.T) {
	root := testsocket.Directory(t)
	plugin := managedPluginFixture(t, root)
	extension := filepath.Join(plugin, "omp", "extension.mjs")
	native := NativeExecutable{RuntimePath: "/runtime/bun", EntryPath: "/package/dist/cli.js"}
	base := host.ExecPlan{Path: native.RuntimePath, Args: []string{native.EntryPath}, Env: []string{
		host.SocketEnv + "=" + filepath.Join(root, "daemon.sock"), host.GroupsEnv + "=[]",
	}}
	for name, mutate := range map[string]func(*host.ExecPlan){
		"runtime": func(plan *host.ExecPlan) { plan.Path = "/other/bun" },
		"entry":   func(plan *host.ExecPlan) { plan.Args[0] = "/other/cli.js" },
		"token":   func(plan *host.ExecPlan) { plan.Env = append(plan.Env, host.TokenEnv+"=stale") },
		"session": func(plan *host.ExecPlan) { plan.Env = append(plan.Env, host.SessionIDEnv+"=stale") },
		"key":     func(plan *host.ExecPlan) { plan.Env = append(plan.Env, host.LocalKeyEnv+"=stale") },
		"groups":  func(plan *host.ExecPlan) { plan.Env = ompSetEnvironment(plan.Env, host.GroupsEnv, "null") },
		"socket":  func(plan *host.ExecPlan) { plan.Env = ompSetEnvironment(plan.Env, host.SocketEnv, "relative.sock") },
	} {
		t.Run(name, func(t *testing.T) {
			plan := host.ExecPlan{Path: base.Path, Args: append([]string(nil), base.Args...), Env: append([]string(nil), base.Env...)}
			mutate(&plan)
			if _, err := interactiveOwnerOptions(context.Background(), plan, native, extension, bytes.NewReader(make([]byte, 16))); err == nil {
				t.Fatal("invalid managed plan accepted")
			}
		})
	}
}

func TestWaitInteractiveOwnerRequiresPrimaryAndPropagatesTerminalState(t *testing.T) {
	owner := newFakeInteractiveNativeOwner()
	close(owner.ready)
	if err := waitInteractiveOwner(context.Background(), owner); err == nil || !strings.Contains(err.Error(), "primary") {
		t.Fatalf("missing primary = %v", err)
	}

	want := errors.New("native terminal failure")
	owner = newFakeInteractiveNativeOwner()
	owner.binding = OwnerBinding{SessionID: "native-id", Scope: ownerScopePrimary, Mode: ownerModeTUI}
	owner.err = want
	close(owner.ready)
	close(owner.done)
	if err := waitInteractiveOwner(context.Background(), owner); !errors.Is(err, want) {
		t.Fatalf("terminal error = %v", err)
	}
}

func TestWaitInteractiveOwnerCancellationBeforeAndAfterReady(t *testing.T) {
	for _, ready := range []bool{false, true} {
		owner := newFakeInteractiveNativeOwner()
		owner.binding = OwnerBinding{SessionID: "native-id", Scope: ownerScopePrimary, Mode: ownerModeTUI}
		if ready {
			close(owner.ready)
		}
		ctx, cancel := context.WithCancelCause(context.Background())
		want := errors.New("fixture canceled")
		cancel(want)
		if err := waitInteractiveOwner(ctx, owner); !errors.Is(err, want) {
			t.Fatalf("ready=%v cancellation = %v", ready, err)
		}
	}
}

func TestRunInteractiveOwnerClosesExactlyOnceAndRetainsTerminalError(t *testing.T) {
	want := errors.New("native terminal failure")
	owner := newFakeInteractiveNativeOwner()
	owner.binding = OwnerBinding{SessionID: "native-id", Scope: ownerScopePrimary, Mode: ownerModeTUI}
	owner.err = want
	close(owner.ready)
	close(owner.done)
	if err := runInteractiveOwner(context.Background(), owner); !errors.Is(err, want) {
		t.Fatalf("terminal error = %v", err)
	}
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if owner.closed != 1 {
		t.Fatalf("Close calls = %d", owner.closed)
	}
}

func TestNewOMPInteractiveProvisionalUsesExactEntropy(t *testing.T) {
	value, err := newOMPInteractiveProvisional(bytes.NewReader(bytes.Repeat([]byte{0xff}, 16)))
	if err != nil || value != "interactive-ffffffffffffffffffffffffffffffff" || !validOwnerID(value) {
		t.Fatalf("provisional = %q, %v", value, err)
	}
	if _, err = newOMPInteractiveProvisional(io.LimitReader(bytes.NewReader(nil), 0)); err == nil {
		t.Fatal("short entropy accepted")
	}
}

func mustGetwd(t *testing.T) string {
	t.Helper()
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	return cwd
}
