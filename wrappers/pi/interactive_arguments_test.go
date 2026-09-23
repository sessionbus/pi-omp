// SPDX-License-Identifier: MIT

package pi

import (
	"reflect"
	"strings"
	"testing"

	"github.com/sessionbus/peer-common/host"
)

func TestInteractivePlanProjectsWrapperIdentityAndKeepsNativeArguments(t *testing.T) {
	t.Setenv("XDG_RUNTIME_DIR", t.TempDir())
	args := []string{"--model", "deepseek/model", "-g", "one,two", "--name", "native title", "-n", "peer title", "--resume"}
	plan, passthrough, err := InteractivePlan(args, []string{"OTHER=kept", host.SocketEnv + "=/bus.sock"})
	if err != nil || passthrough {
		t.Fatalf("plan: %#v passthrough=%v err=%v", plan, passthrough, err)
	}
	wantArgs := []string{"--model", "deepseek/model", "--name", "native title", "--resume"}
	if !reflect.DeepEqual(plan.Args, wantArgs) {
		t.Fatalf("args = %#v, want %#v", plan.Args, wantArgs)
	}
	if got := interactiveEnvironmentValue(plan.Env, host.GroupsEnv); got != `["one,two"]` {
		t.Fatalf("groups = %q", got)
	}
	if got := interactiveEnvironmentValue(plan.Env, host.NameEnv); got != "peer title" {
		t.Fatalf("name = %q", got)
	}
	if got := interactiveEnvironmentValue(plan.Env, host.SocketEnv); got != "/bus.sock" {
		t.Fatalf("socket = %q", got)
	}
}

func TestInteractivePlanNativeValueAndBoundaryProtectWrapperFlags(t *testing.T) {
	t.Setenv("XDG_RUNTIME_DIR", t.TempDir())
	for _, args := range [][]string{
		{"--model", "-g", "prompt"},
		{"--model", "--mode", "prompt"},
		{"-t", "-g,sessionbus", "prompt"},
		{"-xt", "--peer-name", "prompt"},
		{"--tui-mode", "-g", "prompt"},
		{"--model=-g", "prompt"},
		{"--tools=--peer-name", "prompt"},
		{"--model", "-g", "--"},
		{"-t", "--peer-name,sessionbus", "--"},
		{"--", "-g", "--mode"},
	} {
		plan, passthrough, err := InteractivePlan(args, nil)
		if err != nil || passthrough || !reflect.DeepEqual(plan.Args, args) {
			t.Fatalf("%#v -> %#v passthrough=%v err=%v", args, plan.Args, passthrough, err)
		}
	}
}

func TestInteractivePlanNativeMaintenancePassthroughIsBytePreserving(t *testing.T) {
	for _, args := range [][]string{
		{"--help", "-g"},
		{"install", "-g", "package"},
		{"--print", "-g"},
		{"-p", "prompt", "-g"},
		{"--export", "session.jsonl", "output.html"},
		{"--list-models", "deepseek"},
	} {
		environment := []string{"BAD WRAPPER ENV", "OTHER=value"}
		plan, passthrough, err := InteractivePlan(args, environment)
		if err != nil || !passthrough {
			t.Fatalf("%#v passthrough=%v err=%v", args, passthrough, err)
		}
		if !reflect.DeepEqual(plan.Args, args) || !reflect.DeepEqual(plan.Env, environment) {
			t.Fatalf("passthrough changed: %#v", plan)
		}
	}
}

func TestInteractivePlanDoesNotTreatExtensionFlagValueAsCommand(t *testing.T) {
	t.Setenv("XDG_RUNTIME_DIR", t.TempDir())
	for _, args := range [][]string{
		{"--extension-flag", "auth", "prompt"},
		{"--extension-flag", "install", "prompt"},
		{"--model", "--help", "prompt"},
		{"--export"},
	} {
		plan, passthrough, err := InteractivePlan(args, nil)
		if err != nil || passthrough || !reflect.DeepEqual(plan.Args, args) {
			t.Fatalf("%#v -> %#v passthrough=%v err=%v", args, plan.Args, passthrough, err)
		}
	}
}

func TestInteractivePlanRecognizesMaintenanceOnlyAsFirstArgument(t *testing.T) {
	t.Setenv("XDG_RUNTIME_DIR", t.TempDir())
	plan, passthrough, err := InteractivePlan([]string{"--offline", "auth", "print-api-key", "-g", "team"}, nil)
	if err != nil || passthrough {
		t.Fatalf("passthrough=%v err=%v", passthrough, err)
	}
	if !reflect.DeepEqual(plan.Args, []string{"--offline", "auth", "print-api-key"}) {
		t.Fatalf("args = %#v", plan.Args)
	}
	if got := interactiveEnvironmentValue(plan.Env, host.GroupsEnv); got != `["team"]` {
		t.Fatalf("groups = %q", got)
	}
}

func TestInteractivePlanProtectsWrapperValuesBeforeNativeClassification(t *testing.T) {
	t.Setenv("XDG_RUNTIME_DIR", t.TempDir())
	for _, test := range []struct {
		args     []string
		wantArgs []string
		name     string
		groups   string
	}{
		{[]string{"-n", "--help"}, []string{}, "--help", `[]`},
		{[]string{"--peer-name", "--print"}, []string{}, "--print", `[]`},
		{[]string{"-g", "--list-models"}, []string{}, "", `["--list-models"]`},
		{[]string{"-g", "-n", "--"}, []string{"--"}, "", `["-n"]`},
	} {
		plan, passthrough, err := InteractivePlan(test.args, nil)
		if err != nil || passthrough || !reflect.DeepEqual(plan.Args, test.wantArgs) {
			t.Fatalf("%#v -> %#v passthrough=%v err=%v", test.args, plan.Args, passthrough, err)
		}
		if got := interactiveEnvironmentValue(plan.Env, host.NameEnv); got != test.name {
			t.Fatalf("%#v name = %q", test.args, got)
		}
		if got := interactiveEnvironmentValue(plan.Env, host.GroupsEnv); got != test.groups {
			t.Fatalf("%#v groups = %q", test.args, got)
		}
	}
}

func TestInteractivePlanSelectsPeerShortNameAndNativeLongName(t *testing.T) {
	t.Setenv("XDG_RUNTIME_DIR", t.TempDir())
	plan, passthrough, err := InteractivePlan([]string{"-n", "peer name", "--name", "native title"}, nil)
	if err != nil || passthrough {
		t.Fatalf("passthrough=%v err=%v", passthrough, err)
	}
	if !reflect.DeepEqual(plan.Args, []string{"--name", "native title"}) {
		t.Fatalf("args = %#v", plan.Args)
	}
	if got := interactiveEnvironmentValue(plan.Env, host.NameEnv); got != "peer name" {
		t.Fatalf("peer name = %q", got)
	}
}

func TestInteractivePlanRejectsTopologyOverride(t *testing.T) {
	t.Setenv("XDG_RUNTIME_DIR", t.TempDir())
	for _, args := range [][]string{{"--mode", "rpc"}, {"--mode=json"}} {
		if _, native, err := InteractivePlan(args, nil); err == nil || native {
			t.Fatalf("%#v accepted: native=%v err=%v", args, native, err)
		}
	}
}

func TestInteractivePlanRejectsManagedToolDisable(t *testing.T) {
	t.Setenv("XDG_RUNTIME_DIR", t.TempDir())
	for _, args := range [][]string{
		{"--no-tools"},
		{"-nt"},
		{"--tools", "read,bash"},
		{"-t", "-g"},
		{"-t", "--peer-name"},
		{"-t", "read, SessionBus"},
		{"--tools", " , "},
		{"--exclude-tools", "read, sessionbus ,bash"},
		{"-xt", "sessionbus"},
		{"--tools", "sessionbus", "--exclude-tools", "sessionbus"},
		{"--no-tools", "--tools", "read"},
		{"--tools", "read", "--no-tools"},
	} {
		if _, native, err := InteractivePlan(args, nil); err == nil || native ||
			!strings.Contains(err.Error(), "disables managed Pi Sessionbus tool") {
			t.Fatalf("%#v accepted: native=%v err=%v", args, native, err)
		}
	}
}

func TestInteractivePlanPreservesEffectiveManagedToolSelection(t *testing.T) {
	t.Setenv("XDG_RUNTIME_DIR", t.TempDir())
	for _, args := range [][]string{
		{"--tools", "read, sessionbus ,write"},
		{"-t", "sessionbus"},
		{"--exclude-tools", "read,bash"},
		{"-xt", "read"},
		{"--no-builtin-tools"},
		{"--tools", "read", "--tools", "sessionbus"},
		{"--exclude-tools", "sessionbus", "--exclude-tools", "read"},
		{"--no-tools", "--tools", "sessionbus"},
		{"--tools", "sessionbus", "--no-tools"},
		{"--tools=read"},
		{"--exclude-tools=sessionbus"},
		{"--model", "--no-tools"},
		{"--", "--no-tools", "--tools", "read", "--exclude-tools", "sessionbus"},
	} {
		plan, passthrough, err := InteractivePlan(args, nil)
		if err != nil || passthrough || !reflect.DeepEqual(plan.Args, args) {
			t.Fatalf("%#v -> %#v passthrough=%v err=%v", args, plan.Args, passthrough, err)
		}
	}
}

func TestInteractivePlanKeepsNativeNoPromptToolControls(t *testing.T) {
	for _, args := range [][]string{
		{"--no-tools", "-p", "prompt"},
		{"--tools", "read", "--print", "prompt"},
		{"--exclude-tools", "sessionbus", "-p", "prompt"},
	} {
		environment := []string{"KEEP=value"}
		plan, passthrough, err := InteractivePlan(args, environment)
		if err != nil || !passthrough || !reflect.DeepEqual(plan.Args, args) || !reflect.DeepEqual(plan.Env, environment) {
			t.Fatalf("%#v -> %#v passthrough=%v err=%v", args, plan, passthrough, err)
		}
	}
}

func TestInteractivePlanYoloCoexistsWithManagedToolSelection(t *testing.T) {
	t.Setenv("XDG_RUNTIME_DIR", t.TempDir())
	for _, test := range []struct {
		args []string
		want []string
	}{
		{[]string{"--tools", "sessionbus", "--yolo"}, []string{"--tools", "sessionbus", "--approve"}},
		{[]string{"--yolo", "--tools", "read,sessionbus"}, []string{"--approve", "--tools", "read,sessionbus"}},
		{[]string{"--approve", "--tools", "sessionbus"}, []string{"--approve", "--tools", "sessionbus"}},
		{[]string{"--model", "--yolo", "--tools", "sessionbus"}, []string{"--model", "--yolo", "--tools", "sessionbus"}},
		{[]string{"--tools", "sessionbus", "--", "--yolo", "--approve"}, []string{"--tools", "sessionbus", "--", "--yolo", "--approve"}},
	} {
		plan, passthrough, err := InteractivePlan(test.args, nil)
		if err != nil || passthrough || !reflect.DeepEqual(plan.Args, test.want) {
			t.Fatalf("%#v -> %#v passthrough=%v err=%v, want %#v", test.args, plan.Args, passthrough, err, test.want)
		}
	}
}

func TestInteractivePlanRejectsInvalidWrapperValue(t *testing.T) {
	for _, args := range [][]string{{"-g"}, {"-g", ""}, {"-g", "--"}, {"--peer-name="}, {"-n", "--"}} {
		if _, native, err := InteractivePlan(args, nil); err == nil || native {
			t.Fatalf("%#v accepted: native=%v err=%v", args, native, err)
		}
	}
}
