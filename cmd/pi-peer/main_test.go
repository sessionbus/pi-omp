// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"errors"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/sessionbus/peer-common/host"
	"github.com/sessionbus/pi-omp/wrappers/pi"
)

func resetPiCommandHooks(t *testing.T) {
	t.Helper()
	resolveNative, resolveExtension := piResolveNative, piResolveExtension
	runInteractive, execNative, executable := piRunInteractive, piExecNative, piExecutable
	t.Cleanup(func() {
		piResolveNative, piResolveExtension = resolveNative, resolveExtension
		piRunInteractive, piExecNative, piExecutable = runInteractive, execNative, executable
	})
}

func unsetPiLaneMode(t *testing.T) {
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

func TestPiNonTerminalInputOrOutputUsesExactNativePassthrough(t *testing.T) {
	for _, terminal := range []struct {
		name             string
		stdin, stdoutTTY bool
	}{
		{"stdin", false, true},
		{"stdout", true, false},
	} {
		t.Run(terminal.name, func(t *testing.T) {
			resetPiCommandHooks(t)
			unsetPiLaneMode(t)
			piResolveNative = func(front string) (pi.NativeExecutable, error) {
				if front != "pi" {
					t.Fatalf("front door = %q", front)
				}
				return pi.NativeExecutable{Path: "/native/pi"}, nil
			}
			var gotPath string
			var gotArgs, gotEnv []string
			piExecNative = func(path string, args, environment []string) error {
				gotPath, gotArgs, gotEnv = path, append([]string(nil), args...), append([]string(nil), environment...)
				return errors.New("exec fixture")
			}
			arguments := []string{"--peer-name", "identity", "auth", "login"}
			err := run(context.Background(), arguments, terminal.stdin, terminal.stdoutTTY)
			if err == nil || err.Error() != "exec fixture" || gotPath != "/native/pi" ||
				!reflect.DeepEqual(gotArgs, []string{"/native/pi", "--peer-name", "identity", "auth", "login"}) || len(gotEnv) == 0 {
				t.Fatalf("passthrough path=%q args=%q env=%d err=%v", gotPath, gotArgs, len(gotEnv), err)
			}
		})
	}
}

func TestPiTTYRoutesManagedAndNativeInvocations(t *testing.T) {
	resetPiCommandHooks(t)
	unsetPiLaneMode(t)
	t.Setenv(host.SocketEnv, "/bus.sock")
	piResolveExtension = func(path string) (string, error) {
		if path != "/installed/pi-peer" {
			t.Fatalf("peer executable = %q", path)
		}
		return "/installed/plugin/pi/extension.mjs", nil
	}
	piExecutable = func() (string, error) { return "/installed/pi-peer", nil }
	piRunInteractive = func(_ context.Context, plan host.ExecPlan, extension string) error {
		if extension != "/installed/plugin/pi/extension.mjs" || !reflect.DeepEqual(plan.Args, []string{"--model", "fixture"}) {
			t.Fatalf("managed plan=%+v extension=%q", plan, extension)
		}
		return errors.New("managed fixture")
	}
	if err := run(context.Background(), []string{"--model", "fixture", "-g", "team"}, true, true); err == nil || err.Error() != "managed fixture" {
		t.Fatalf("managed run = %v", err)
	}

	piResolveNative = func(string) (pi.NativeExecutable, error) { return pi.NativeExecutable{Path: "/native/pi"}, nil }
	piExecNative = func(path string, args, _ []string) error {
		if path != "/native/pi" || !reflect.DeepEqual(args, []string{"/native/pi", "--help"}) {
			t.Fatalf("native path=%q args=%q", path, args)
		}
		return errors.New("native fixture")
	}
	if err := run(context.Background(), []string{"--help"}, true, true); err == nil || err.Error() != "native fixture" {
		t.Fatalf("native run = %v", err)
	}
}

func TestPiTTYRejectsDisabledManagedToolBeforeNativeLaunch(t *testing.T) {
	resetPiCommandHooks(t)
	unsetPiLaneMode(t)
	launched := false
	piRunInteractive = func(context.Context, host.ExecPlan, string) error {
		launched = true
		return nil
	}
	piExecNative = func(string, []string, []string) error {
		launched = true
		return nil
	}
	if err := run(context.Background(), []string{"--tools", "read,bash"}, true, true); err == nil ||
		!strings.Contains(err.Error(), "disables managed Pi Sessionbus tool") {
		t.Fatalf("managed disable = %v", err)
	}
	if launched {
		t.Fatal("managed Pi native was launched after tool-disable rejection")
	}
}

func TestPiLaneAndMaintenanceBoundaries(t *testing.T) {
	t.Setenv(host.TokenEnv, "token")
	if err := run(context.Background(), []string{"argument"}, true, true); err == nil || err.Error() != "lane mode accepts no arguments" {
		t.Fatalf("lane arguments = %v", err)
	}
	if err := run(context.Background(), []string{"--sessionbus-install"}, true, true); err == nil || err.Error() != "usage: pi-peer --sessionbus-install --plugin-dir ABSOLUTE_PATH" {
		t.Fatalf("maintenance = %v", err)
	}
}

func TestPiTerminalRejectsRegularFiles(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "file")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if piTerminal(file) {
		t.Fatal("regular file reported as terminal")
	}
	null, err := os.Open(os.DevNull)
	if err != nil {
		t.Fatal(err)
	}
	defer null.Close()
	if piTerminal(null) {
		t.Fatal("non-terminal character device reported as terminal")
	}
}
