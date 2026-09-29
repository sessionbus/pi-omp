// SPDX-License-Identifier: MIT
package pi

import (
	"reflect"
	"testing"

	sessionkit "github.com/antst/sessionbus/bus/sdk/go"
)

func TestPiLaunchPreservesNativeSelectionAndOptionArity(t *testing.T) {
	args, err := launchArguments(sessionkit.OpenOptions{
		Model: "deepseek/deepseek-v4-pro", ReasoningEffort: "high",
		Arguments: []string{"--append-system-prompt", "--model=literal data", "--system-prompt=other", "--verbose"},
	}, "/managed/extension.mjs", "native-existing")
	want := []string{"--mode", "rpc", "--session", "native-existing",
		"--model", "deepseek/deepseek-v4-pro", "--thinking", "high", "--append-system-prompt", "--model=literal data", "--system-prompt", "other", "--verbose", "--extension", "/managed/extension.mjs"}
	if err != nil || !reflect.DeepEqual(args, want) {
		t.Fatalf("%q, %v", args, err)
	}
	fresh, err := launchArguments(sessionkit.OpenOptions{}, "/managed/extension.mjs", "")
	if err != nil || !reflect.DeepEqual(fresh, []string{"--mode", "rpc", "--extension", "/managed/extension.mjs"}) {
		t.Fatalf("fresh identity and policy must remain native-owned: %q, %v", fresh, err)
	}
}

func TestPiRejectsLifecycleAndTypedFieldOverrides(t *testing.T) {
	for _, args := range [][]string{
		{"--mode", "json"}, {"--session=other"}, {"--model", "other"}, {"--thinking=low"},
		{"--name", "other"}, {"--extension", "/other"}, {"--no-extensions"}, {"--tools=read"},
		{"--approve"}, {"--no-session"}, {"-r"}, {"--", "prompt"}, {"positional prompt"},
		{"--system-prompt"}, {"--verbose=yes"}, {"--unknown"}, {"--system-prompt", "nul\x00value"},
	} {
		if _, err := launchArguments(sessionkit.OpenOptions{Arguments: args}, "/managed/extension.mjs", ""); err == nil {
			t.Fatalf("accepted %q", args)
		}
	}
}

func TestPiRejectsUnsupportedNativeSelections(t *testing.T) {
	for _, options := range []sessionkit.OpenOptions{
		{PermissionMode: "bypassPermissions"}, {ReasoningEffort: "automatic"}, {Model: " bad"}, {Model: "x\x00y"},
	} {
		if _, err := launchArguments(options, "/managed/extension.mjs", ""); err == nil {
			t.Fatalf("accepted %+v", options)
		}
	}
	for _, id := range []string{"../other", "prefix@host", "-leading", "trailing-", "white space"} {
		if _, err := launchArguments(sessionkit.OpenOptions{}, "/managed/extension.mjs", id); err == nil {
			t.Fatalf("accepted non-native ID %q", id)
		}
	}
}
