// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/sessionbus/peer-common/host"
	"github.com/sessionbus/pi-omp/wrappers/omp"
)

func resetOMPCommandHooks(t *testing.T) {
	t.Helper()
	resolveNative, resolveExtension := ompResolveNative, ompResolveExtension
	runInteractive, execNative, executable := ompRunInteractive, ompExecNative, ompExecutable
	t.Cleanup(func() {
		ompResolveNative, ompResolveExtension = resolveNative, resolveExtension
		ompRunInteractive, ompExecNative, ompExecutable = runInteractive, execNative, executable
	})
}

func unsetOMPLaneMode(t *testing.T) {
	t.Helper()
	value, present := os.LookupEnv(host.TokenEnv)
	if err := os.Unsetenv(host.TokenEnv); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if present {
			_ = os.Setenv(host.TokenEnv, value)
		} else {
			_ = os.Unsetenv(host.TokenEnv)
		}
	})
}

func ompCommandNativeFixture() omp.NativeExecutable {
	return omp.NativeExecutable{RuntimePath: "/native/bun", EntryPath: "/native/omp/dist/cli.js", PackageRoot: "/native/omp"}
}

func TestOMPNonTerminalInputOrOutputUsesExactNativePassthrough(t *testing.T) {
	for _, terminal := range []struct {
		name             string
		stdin, stdoutTTY bool
	}{
		{"stdin", false, true},
		{"stdout", true, false},
	} {
		t.Run(terminal.name, func(t *testing.T) {
			resetOMPCommandHooks(t)
			unsetOMPLaneMode(t)
			native := ompCommandNativeFixture()
			ompResolveNative = func(front string) (omp.NativeExecutable, error) {
				if front != "omp" {
					t.Fatalf("front door = %q", front)
				}
				return native, nil
			}
			var gotPath string
			var gotArgs, gotEnv []string
			ompExecNative = func(path string, args, environment []string) error {
				gotPath, gotArgs, gotEnv = path, append([]string(nil), args...), append([]string(nil), environment...)
				return errors.New("exec fixture")
			}
			arguments := []string{"--peer-name", "identity", "update"}
			err := run(context.Background(), arguments, terminal.stdin, terminal.stdoutTTY)
			wantArgs := []string{native.RuntimePath, native.EntryPath, "--peer-name", "identity", "update"}
			if err == nil || err.Error() != "exec fixture" || gotPath != native.RuntimePath ||
				!reflect.DeepEqual(gotArgs, wantArgs) || len(gotEnv) == 0 {
				t.Fatalf("passthrough path=%q args=%q env=%d err=%v", gotPath, gotArgs, len(gotEnv), err)
			}
		})
	}
}

func TestOMPTTYRoutesManagedAndNativeInvocations(t *testing.T) {
	resetOMPCommandHooks(t)
	unsetOMPLaneMode(t)
	t.Setenv(host.SocketEnv, "")
	runtimeRoot := filepath.Join(t.TempDir(), "runtime")
	t.Setenv("XDG_RUNTIME_DIR", runtimeRoot)
	wantSocket := filepath.Join(runtimeRoot, "sessionbus", "presence.sock")
	native := ompCommandNativeFixture()
	ompResolveNative = func(front string) (omp.NativeExecutable, error) {
		if front != "omp" {
			t.Fatalf("front door = %q", front)
		}
		return native, nil
	}
	ompResolveExtension = func(path string) (string, error) {
		if path != "/installed/omp-peer" {
			t.Fatalf("peer executable = %q", path)
		}
		return "/installed/plugin/omp/extension.mjs", nil
	}
	ompExecutable = func() (string, error) { return "/installed/omp-peer", nil }
	ompRunInteractive = func(_ context.Context, plan host.ExecPlan, gotNative omp.NativeExecutable, extension string) error {
		if gotNative != native || extension != "/installed/plugin/omp/extension.mjs" || plan.Path != native.RuntimePath ||
			!reflect.DeepEqual(plan.Args, []string{native.EntryPath, "--model", "fixture"}) ||
			ompCommandEnvironmentValue(plan.Env, host.GroupsEnv) != `["team"]` ||
			ompCommandEnvironmentValue(plan.Env, host.SocketEnv) != wantSocket {
			t.Fatalf("managed native=%+v plan=%+v extension=%q", gotNative, plan, extension)
		}
		return errors.New("managed fixture")
	}
	if err := run(context.Background(), []string{"--model", "fixture", "-g", "team"}, true, true); err == nil || err.Error() != "managed fixture" {
		t.Fatalf("managed run = %v", err)
	}

	var gotPath string
	var gotArgs []string
	ompExecNative = func(path string, args, _ []string) error {
		gotPath, gotArgs = path, append([]string(nil), args...)
		return errors.New("native fixture")
	}
	if err := run(context.Background(), []string{"--help"}, true, true); err == nil || err.Error() != "native fixture" {
		t.Fatalf("native run = %v", err)
	}
	if gotPath != native.RuntimePath || !reflect.DeepEqual(gotArgs, []string{native.RuntimePath, native.EntryPath, "--help"}) {
		t.Fatalf("native path=%q args=%q", gotPath, gotArgs)
	}
}

func TestOMPLanePrecedesNonTerminalPassthroughAndMaintenancePrecedesLane(t *testing.T) {
	resetOMPCommandHooks(t)
	t.Setenv(host.TokenEnv, "token")
	ompResolveNative = func(string) (omp.NativeExecutable, error) {
		t.Fatal("native resolver reached across an earlier routing boundary")
		return omp.NativeExecutable{}, nil
	}
	if err := run(context.Background(), []string{"argument"}, false, false); err == nil || err.Error() != "lane mode accepts no arguments" {
		t.Fatalf("lane arguments = %v", err)
	}
	if err := run(context.Background(), []string{"--sessionbus-install"}, false, false); err == nil || err.Error() != "usage: omp-peer --sessionbus-install --plugin-dir ABSOLUTE_PATH" {
		t.Fatalf("maintenance = %v", err)
	}
}

func TestOMPTerminalRejectsRegularFiles(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "file")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if ompTerminal(file) {
		t.Fatal("regular file reported as terminal")
	}
	null, err := os.Open(os.DevNull)
	if err != nil {
		t.Fatal(err)
	}
	defer null.Close()
	if ompTerminal(null) {
		t.Fatal("non-terminal character device reported as terminal")
	}
}

func ompCommandEnvironmentValue(environment []string, name string) string {
	prefix := name + "="
	for index := len(environment) - 1; index >= 0; index-- {
		if len(environment[index]) >= len(prefix) && environment[index][:len(prefix)] == prefix {
			return environment[index][len(prefix):]
		}
	}
	return ""
}
