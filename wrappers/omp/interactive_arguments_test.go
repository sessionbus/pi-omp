// SPDX-License-Identifier: MIT

package omp

import (
	"reflect"
	"strings"
	"testing"

	"github.com/sessionbus/peer-common/host"
)

var argumentTestNative = NativeExecutable{
	Path: "/front/omp", EntryPath: "/package/dist/cli.js", RuntimePath: "/runtime/bun", PackageRoot: "/package",
}

func TestOMPInteractiveBypassPreservesNativeOrder(t *testing.T) {
	for _, bypass := range [][]string{{"--yolo"}, {"--auto-approve"}, {"--approval-mode", "yolo"}, {"--approval-mode=yolo"}} {
		input := append([]string{"--model", "provider/model"}, bypass...)
		input = append(input, "-g", "team", "prompt")
		plan, passthrough, err := InteractivePlan(argumentTestNative, input, nil)
		want := append([]string{argumentTestNative.EntryPath, "--model", "provider/model"}, bypass...)
		want = append(want, "prompt")
		if err != nil || passthrough || !reflect.DeepEqual(plan.Args, want) {
			t.Fatalf("bypass %v = %#v, passthrough %t, %v; want %#v", bypass, plan, passthrough, err, want)
		}
	}
}

func TestOMPInteractivePlanProjectsIdentityAfterNativeValues(t *testing.T) {
	input := []string{"--model", "deepseek/model", "-g", "one", "--peer-name", "named", "hello"}
	environment := []string{
		"KEEP=value", host.GroupsEnv + `=["old"]`, host.NameEnv + "=old", host.SessionIDEnv + "=inherited",
	}
	plan, passthrough, err := InteractivePlan(argumentTestNative, input, environment)
	if err != nil || passthrough {
		t.Fatalf("managed plan = %#v, %t, %v", plan, passthrough, err)
	}
	wantArgs := []string{argumentTestNative.EntryPath, "--model", "deepseek/model", "hello"}
	if !reflect.DeepEqual(plan.Args, wantArgs) || plan.Path != argumentTestNative.RuntimePath {
		t.Fatalf("execution = %#v, want path %q args %#v", plan, argumentTestNative.RuntimePath, wantArgs)
	}
	if got := ompEnvironmentValue(plan.Env, host.GroupsEnv); got != `["one"]` {
		t.Fatalf("groups = %q", got)
	}
	if got := ompEnvironmentValue(plan.Env, host.NameEnv); got != "named" {
		t.Fatalf("name = %q", got)
	}
	if got := ompEnvironmentValue(plan.Env, host.SessionIDEnv); got != "" {
		t.Fatalf("inherited session ID = %q", got)
	}
	if got := ompEnvironmentValue(plan.Env, "KEEP"); got != "value" {
		t.Fatalf("unrelated environment = %q", got)
	}
}

func TestOMPInteractivePlanNativeValuesProtectWrapperTokens(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want []string
	}{
		{"model", []string{"--model", "-g", "prompt"}, []string{"--model", "-g", "prompt"}},
		{"extension", []string{"--extension", "--peer-name", "prompt"}, []string{"--extension", "--peer-name", "prompt"}},
		{"trusted-as-model-value", []string{"--model", "--trusted-extension", "prompt"}, []string{"--model", "--trusted-extension", "prompt"}},
		{"empty-required-value", []string{"--model", "", "-g", "team"}, []string{"--model", ""}},
		{"empty-unknown-value", []string{"--example", "", "-g", "team"}, []string{"--example", ""}},
		{"empty-optional-is-wrapper", []string{"--resume", "", "-g", "team"}, []string{"--resume", ""}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			plan, passthrough, err := InteractivePlan(argumentTestNative, test.args, nil)
			if err != nil || passthrough {
				t.Fatalf("plan = %#v, %t, %v", plan, passthrough, err)
			}
			if got := plan.Args[1:]; !reflect.DeepEqual(got, test.want) {
				t.Fatalf("forwarded = %#v, want %#v", got, test.want)
			}
		})
	}
}

func TestOMPInteractivePlanProtectsOwnedWrapperValuesDuringRouting(t *testing.T) {
	tests := []struct {
		args   []string
		groups string
		name   string
	}{
		{[]string{"-g", "--print"}, `["--print"]`, ""},
		{[]string{"--peer-name", "--help"}, `[]`, "--help"},
		{[]string{"-n", "acp"}, `[]`, "acp"},
		{[]string{"-g", "update"}, `["update"]`, ""},
		{[]string{"-g", "--profile"}, `["--profile"]`, ""},
		{[]string{"--peer-name", "--alias"}, `[]`, "--alias"},
	}
	for _, test := range tests {
		t.Run(strings.Join(test.args, "_"), func(t *testing.T) {
			plan, native, err := InteractivePlan(argumentTestNative, test.args, nil)
			if err != nil || native {
				t.Fatalf("plan = %#v, native = %t, error = %v", plan, native, err)
			}
			if !reflect.DeepEqual(plan.Args, []string{argumentTestNative.EntryPath}) ||
				ompEnvironmentValue(plan.Env, host.GroupsEnv) != test.groups ||
				ompEnvironmentValue(plan.Env, host.NameEnv) != test.name {
				t.Fatalf("projection = %#v", plan)
			}
		})
	}
	plan, native, err := InteractivePlan(argumentTestNative, []string{"--model", "-g", "update"}, nil)
	if err != nil || !native || !reflect.DeepEqual(plan.Args, []string{argumentTestNative.EntryPath, "--model", "-g", "update"}) {
		t.Fatalf("native-owned wrapper token = %#v, %t, %v", plan, native, err)
	}
}

func TestOMPInteractivePlanRoutesProjectedNativeArguments(t *testing.T) {
	for _, arguments := range [][]string{{"-g", "--", "--print"}, {"--peer-name", "--"}} {
		if _, native, err := InteractivePlan(argumentTestNative, arguments, nil); err == nil || native {
			t.Fatalf("wrapper boundary value %#v = native %t, error %v", arguments, native, err)
		}
	}
	tests := []struct {
		name string
		args []string
		want []string
	}{
		{"help-after-name", []string{"-n", "name", "help"}, []string{"help"}},
		{"list-after-name", []string{"-n", "name", "list"}, []string{"list"}},
		{"command-keeps-remainder", []string{"-n", "name", "update", "-g", "native", "--peer-name", "native"}, []string{"update", "-g", "native", "--peer-name", "native"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			plan, native, err := InteractivePlan(argumentTestNative, test.args, []string{"KEEP=value"})
			want := append([]string{argumentTestNative.EntryPath}, test.want...)
			if err != nil || !native || !reflect.DeepEqual(plan.Args, want) {
				t.Fatalf("projected native plan = %#v, %t, %v; want %#v", plan, native, err, want)
			}
		})
	}

	marker := []string{"--sessionbus-wrapper-value", "update"}
	plan, native, err := InteractivePlan(argumentTestNative, marker, nil)
	if err != nil || native || !reflect.DeepEqual(plan.Args, append([]string{argumentTestNative.EntryPath}, marker...)) {
		t.Fatalf("literal former marker = %#v, %t, %v", plan, native, err)
	}
}

func TestOMPInteractivePlanNativePassthroughIsExact(t *testing.T) {
	t.Setenv(host.SocketEnv, "/ambient/presence.sock")
	environment := []string{"KEEP=value", host.GroupsEnv + `=["native"]`}
	tests := [][]string{
		{"--help"}, {"-h"}, {"--version"}, {"-v"}, {"help"}, {"--smoke-test"}, {"--license"},
		{"--alias", "work"}, {"--alias=work"}, {"--profile"}, {"--profile="},
		{"launch", "--help"}, {"launch", "--mode", "rpc"}, {"--mode=rpc"}, {"--mode="},
		{"--print"}, {"-p"}, {"--export", "out.html"}, {"--export=out.html"}, {"--export", "--print"},
	}
	for _, arguments := range tests {
		t.Run(strings.Join(arguments, "_"), func(t *testing.T) {
			plan, passthrough, err := InteractivePlan(argumentTestNative, arguments, environment)
			if err != nil || !passthrough {
				t.Fatalf("plan = %#v, %t, %v", plan, passthrough, err)
			}
			want := append([]string{argumentTestNative.EntryPath}, arguments...)
			if plan.Path != argumentTestNative.RuntimePath || !reflect.DeepEqual(plan.Args, want) || !reflect.DeepEqual(plan.Env, environment) {
				t.Fatalf("passthrough changed: %#v", plan)
			}
		})
	}
}

func TestOMPInteractivePlanRoutesEveryRegisteredNativeCommand(t *testing.T) {
	for command := range ompCommands {
		if command == "launch" {
			continue
		}
		t.Run(command, func(t *testing.T) {
			plan, passthrough, err := InteractivePlan(argumentTestNative, []string{command, "literal"}, []string{"KEEP=value"})
			if err != nil || !passthrough || !reflect.DeepEqual(plan.Args, []string{argumentTestNative.EntryPath, command, "literal"}) {
				t.Fatalf("command plan = %#v, %t, %v", plan, passthrough, err)
			}
		})
	}
}

func TestOMPInteractivePlanMirrorsNativeCommandOwnership(t *testing.T) {
	tests := []struct {
		name        string
		args        []string
		passthrough bool
	}{
		{"model-value", []string{"--model", "acp"}, false},
		{"leading-command", []string{"--model", "gpt", "acp"}, true},
		{"unknown-extension-value", []string{"--unknown", "acp", "prompt"}, false},
		{"attached-leading-command", []string{"--approval-mode=yolo", "acp"}, true},
		{"profile-before-command", []string{"--profile", "work", "update"}, true},
		{"profile-after-explicit-launch", []string{"launch", "--profile", "work"}, false},
		{"profile-after-launch-text", []string{"launch", "grep", "--profile", "work"}, false},
		{"profile-owned-by-model", []string{"--model", "--profile", "work"}, false},
		{"plan-value-then-command", []string{"--plan", "--profile", "work", "update"}, true},
		{"unknown-attached", []string{"--unknown=value", "acp"}, true},
		{"explicit-launch", []string{"launch", "hello"}, false},
		{"launch-profile-command-text", []string{"launch", "--profile", "work", "update"}, false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, passthrough, err := InteractivePlan(argumentTestNative, test.args, nil)
			if err != nil || passthrough != test.passthrough {
				t.Fatalf("passthrough = %t, error = %v", passthrough, err)
			}
		})
	}
}

func TestOMPProfileTraversalPreservesNativeBoundaries(t *testing.T) {
	tests := []struct {
		name    string
		input   []string
		residue []string
		oneShot bool
	}{
		{"required-owns-profile", []string{"--system-prompt", "--profile", "work"}, []string{"--system-prompt", "--profile", "work"}, false},
		{"optional-boundary", []string{"--resume", "--profile", "work", "update"}, []string{"--resume", ompProfileBoundary, "update"}, false},
		{"plan-boundary", []string{"--plan", "--profile", "work", "update"}, []string{"--plan", ompProfileBoundary, "update"}, false},
		{"unknown-boundary", []string{"--example", "--profile", "work", "update"}, []string{"--example", ompProfileBoundary, "update"}, false},
		{"profile-before-command", []string{"--profile", "work", "grep", "--profile", "literal"}, []string{"grep", "--profile", "literal"}, false},
		{"profile-after-launch", []string{"launch", "--profile", "work", "hello"}, []string{"launch", "hello"}, false},
		{"alias", []string{"--profile", "work", "--alias", "work-omp"}, nil, true},
		{"missing-profile", []string{"--profile", "--model", "gpt"}, nil, true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			residue, oneShot := ompProfileResidual(test.input)
			if oneShot != test.oneShot || !test.oneShot && !reflect.DeepEqual(residue, test.residue) {
				t.Fatalf("residue = %#v, oneShot = %t; want %#v, %t", residue, oneShot, test.residue, test.oneShot)
			}
		})
	}
}

func TestOMPInteractivePlanReservedNativeOutcomes(t *testing.T) {
	tests := []struct {
		args        []string
		passthrough bool
	}{
		{[]string{"list"}, true},
		{[]string{"list", "all", "files"}, false},
		{[]string{"uninstall", "foo@bar"}, true},
		{[]string{"marketplace", "add", "x"}, true},
		{[]string{"marketplace", "ideas", "please"}, false},
	}
	for _, test := range tests {
		_, passthrough, err := InteractivePlan(argumentTestNative, test.args, nil)
		if err != nil || passthrough != test.passthrough {
			t.Fatalf("%#v: passthrough = %t, error = %v", test.args, passthrough, err)
		}
	}
}

func TestOMPInteractivePlanBoundaryAndWrapperValidation(t *testing.T) {
	plan, passthrough, err := InteractivePlan(argumentTestNative, []string{"-g", "team", "--", "-n", "native"}, nil)
	if err != nil || passthrough {
		t.Fatalf("boundary plan = %#v, %t, %v", plan, passthrough, err)
	}
	if !reflect.DeepEqual(plan.Args, []string{argumentTestNative.EntryPath, "--", "-n", "native"}) || ompEnvironmentValue(plan.Env, host.GroupsEnv) != `["team"]` {
		t.Fatalf("boundary projection = %#v", plan)
	}
	for _, arguments := range [][]string{{"-g"}, {"--group="}, {"-n", ""}, {"--peer-name", "--"}} {
		if _, native, err := InteractivePlan(argumentTestNative, arguments, nil); err == nil || native {
			t.Fatalf("expected managed wrapper error for %#v, native=%t err=%v", arguments, native, err)
		}
	}
	for _, arguments := range [][]string{{"--help", "-g"}, {"update", "--peer-name"}} {
		plan, native, err := InteractivePlan(argumentTestNative, arguments, []string{"KEEP=value"})
		if err != nil || !native || !reflect.DeepEqual(plan.Args, append([]string{argumentTestNative.EntryPath}, arguments...)) {
			t.Fatalf("native wrapper-shaped argv %#v = %#v, %t, %v", arguments, plan, native, err)
		}
	}
}

func TestOMPInteractivePlanAttachedWrapperIdentity(t *testing.T) {
	plan, native, err := InteractivePlan(argumentTestNative, []string{"--group=one", "-g=two", "--peer-name=first", "-n=last", "hello"}, nil)
	if err != nil || native {
		t.Fatalf("plan = %#v, native = %t, error = %v", plan, native, err)
	}
	if !reflect.DeepEqual(plan.Args, []string{argumentTestNative.EntryPath, "hello"}) ||
		ompEnvironmentValue(plan.Env, host.GroupsEnv) != `["one","two"]` ||
		ompEnvironmentValue(plan.Env, host.NameEnv) != "last" {
		t.Fatalf("projection = %#v", plan)
	}
}

func TestOMPInteractivePlanRejectsManagedExtensionConflict(t *testing.T) {
	for _, arguments := range [][]string{{"--trusted-extension", "/tmp/other.mjs"}, {"--trusted-extension=/tmp/other.mjs"}} {
		if _, native, err := InteractivePlan(argumentTestNative, arguments, nil); err == nil || native || !strings.Contains(err.Error(), "trusted-extension") {
			t.Fatalf("conflict %#v = native %t, error %v", arguments, native, err)
		}
	}
	arguments := []string{"update", "--trusted-extension", "/tmp/other.mjs"}
	plan, native, err := InteractivePlan(argumentTestNative, arguments, nil)
	if err != nil || !native || !reflect.DeepEqual(plan.Args, append([]string{argumentTestNative.EntryPath}, arguments...)) {
		t.Fatalf("native conflict-shaped argv = %#v, %t, %v", plan, native, err)
	}
}

func TestOMPInteractivePlanValidatesExecutionInputs(t *testing.T) {
	for _, native := range []NativeExecutable{
		{RuntimePath: "bun", EntryPath: "/entry"},
		{RuntimePath: "/bun", EntryPath: "entry"},
		{RuntimePath: "/bun\x00bad", EntryPath: "/entry"},
	} {
		if _, _, err := InteractivePlan(native, nil, nil); err == nil {
			t.Fatalf("accepted native %#v", native)
		}
	}
	if _, _, err := InteractivePlan(argumentTestNative, []string{"bad\x00arg"}, nil); err == nil {
		t.Fatal("accepted NUL argument")
	}
}

func TestOMPInteractivePlanModeAndExportMissingValuesStayManaged(t *testing.T) {
	for _, arguments := range [][]string{{"--mode"}, {"--export"}, {"--export="}} {
		_, native, err := InteractivePlan(argumentTestNative, arguments, nil)
		if err != nil || native {
			t.Fatalf("%#v = native %t, error %v", arguments, native, err)
		}
	}
}

func ompEnvironmentValue(environment []string, name string) string {
	prefix := name + "="
	for index := len(environment) - 1; index >= 0; index-- {
		if strings.HasPrefix(environment[index], prefix) {
			return strings.TrimPrefix(environment[index], prefix)
		}
	}
	return ""
}
