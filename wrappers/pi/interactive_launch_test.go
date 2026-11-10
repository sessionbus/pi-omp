// SPDX-License-Identifier: MIT

package pi

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/sessionbus/peer-common/host"
)

func TestPiInteractiveExecPlanIsExactAndLeavesNoLauncherArtifact(t *testing.T) {
	root := t.TempDir()
	before, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	plan := host.ExecPlan{
		Path: "pi",
		Args: []string{"--model", "fixture/model"},
		Env: []string{
			"OTHER=kept",
			host.SocketEnv + "=" + filepath.Join(root, "bus.sock"),
			host.NameEnv + "=peer name",
			host.GroupsEnv + `=["team","ops"]`,
			host.SessionIDEnv + "=stale",
		},
	}
	result, err := interactiveExecPlanResolved(plan, "/native/pi", "/plugin/pi/extension.mjs")
	if err != nil {
		t.Fatal(err)
	}
	if result.Path != "/native/pi" || !reflect.DeepEqual(result.Args, []string{
		"--extension", "/plugin/pi/extension.mjs", "--model", "fixture/model",
	}) {
		t.Fatalf("exec plan = %+v", result)
	}
	var descriptor interactiveLaunchDescriptor
	leaked := make([]string, 0)
	for _, entry := range result.Env {
		key, value, _ := strings.Cut(entry, "=")
		if key == InteractiveLaunchEnv {
			if err = json.Unmarshal([]byte(value), &descriptor); err != nil {
				t.Fatal(err)
			}
		} else if strings.HasPrefix(key, "SESSIONBUS_") {
			leaked = append(leaked, key)
		}
	}
	want := interactiveLaunchDescriptor{
		Socket: filepath.Join(root, "bus.sock"), Name: "peer name",
		Groups: []string{"team", "ops"}, Topology: interactiveTopology,
	}
	if !reflect.DeepEqual(descriptor, want) || len(leaked) != 0 || !containsString(result.Env, "OTHER=kept") {
		t.Fatalf("descriptor=%+v leaked=%v env=%q", descriptor, leaked, result.Env)
	}
	after, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("exec planning created launcher artifacts: before=%v after=%v", before, after)
	}
}

func TestPiInteractiveManagedExtensionPrecedesUserExtensions(t *testing.T) {
	t.Setenv("XDG_RUNTIME_DIR", t.TempDir())
	user := []string{"--extension", "/user/first.mjs", "-g", "team", "--model", "fixture/model", "-e", "/user/second.mjs"}
	plan, passthrough, err := InteractivePlan(user, []string{host.SocketEnv + "=/bus.sock"})
	if err != nil || passthrough {
		t.Fatalf("plan=%+v passthrough=%v err=%v", plan, passthrough, err)
	}
	result, err := interactiveExecPlanResolved(plan, "/native/pi", "/plugin/pi/extension.mjs")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"--extension", "/plugin/pi/extension.mjs",
		"--extension", "/user/first.mjs", "--model", "fixture/model", "-e", "/user/second.mjs",
	}
	if !reflect.DeepEqual(result.Args, want) {
		t.Fatalf("args = %q, want %q", result.Args, want)
	}
}

func TestPiInteractiveExecPlanRejectsInvalidBootstrap(t *testing.T) {
	base := host.ExecPlan{Env: []string{
		host.SocketEnv + "=/bus.sock", host.NameEnv + "=name", host.GroupsEnv + "=[]",
	}}
	for _, test := range []struct {
		name string
		plan host.ExecPlan
	}{
		{"relative socket", host.ExecPlan{Env: []string{host.SocketEnv + "=bus.sock", host.GroupsEnv + "=[]"}}},
		{"local key", host.ExecPlan{Env: append(append([]string(nil), base.Env...), host.LocalKeyEnv+"=key")}},
		{"lane token", host.ExecPlan{Env: append(append([]string(nil), base.Env...), host.TokenEnv+"=token")}},
		{"groups", host.ExecPlan{Env: []string{host.SocketEnv + "=/bus.sock", host.GroupsEnv + "=null"}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := interactiveExecPlanResolved(test.plan, "/native/pi", "/plugin/pi/extension.mjs"); err == nil {
				t.Fatal("invalid interactive bootstrap accepted")
			}
		})
	}
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
