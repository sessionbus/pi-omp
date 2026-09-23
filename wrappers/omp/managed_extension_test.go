// SPDX-License-Identifier: MIT

package omp

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sessionbus/peer-common/testsocket"
)

func managedPluginFixture(t *testing.T, root string) string {
	t.Helper()
	plugin := filepath.Join(root, "plugin")
	for _, name := range managedPluginFiles {
		path := filepath.Join(plugin, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte("// fixture\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return plugin
}

func TestResolveManagedExtensionFromInstalledPeer(t *testing.T) {
	root := testsocket.Directory(t)
	plugin := managedPluginFixture(t, root)
	peer := filepath.Join(root, "omp-peer")
	if err := os.WriteFile(peer, []byte("fixture"), 0o755); err != nil {
		t.Fatal(err)
	}
	front := filepath.Join(testsocket.Directory(t), "omp-peer")
	if err := os.Symlink(peer, front); err != nil {
		t.Fatal(err)
	}
	got, err := ResolveManagedExtension(front)
	if err != nil {
		t.Fatal(err)
	}
	want := filepath.Join(plugin, "omp", "extension.mjs")
	if got != want {
		t.Fatalf("extension = %q, want %q", got, want)
	}
}

func TestResolveManagedExtensionRejectsUnrelatedExecutable(t *testing.T) {
	root := testsocket.Directory(t)
	managedPluginFixture(t, root)
	peer := filepath.Join(root, "other")
	if err := os.WriteFile(peer, []byte("fixture"), 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := ResolveManagedExtension(peer); err == nil || !strings.Contains(err.Error(), "layout") {
		t.Fatalf("unrelated executable accepted: %v", err)
	}
}

func TestValidateManagedPluginRejectsMissingSymlinkAndOversizedFiles(t *testing.T) {
	for _, mode := range []string{"missing", "symlink", "oversized"} {
		t.Run(mode, func(t *testing.T) {
			plugin := managedPluginFixture(t, testsocket.Directory(t))
			target := filepath.Join(plugin, "omp", "extension.mjs")
			switch mode {
			case "missing":
				if err := os.Remove(target); err != nil {
					t.Fatal(err)
				}
			case "symlink":
				if err := os.Remove(target); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(filepath.Join(plugin, "pifamily", "extension", "bridge.mjs"), target); err != nil {
					t.Fatal(err)
				}
			case "oversized":
				if err := os.WriteFile(target, make([]byte, (1<<20)+1), 0o644); err != nil {
					t.Fatal(err)
				}
			}
			if err := ValidateManagedPlugin(plugin); err == nil {
				t.Fatalf("%s plugin accepted", mode)
			}
		})
	}
}

func TestInstallPluginValidatesOnlyExplicitLocalPayload(t *testing.T) {
	plugin := managedPluginFixture(t, testsocket.Directory(t))
	if err := InstallPlugin([]string{"--plugin-dir", plugin}); err != nil {
		t.Fatal(err)
	}
	for _, arguments := range [][]string{nil, {"--remove"}, {"--plugin-dir", "relative"}, {"--plugin-dir", plugin, "extra"}} {
		if err := InstallPlugin(arguments); err == nil {
			t.Fatalf("arguments %#v accepted", arguments)
		}
	}
}
