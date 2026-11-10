// SPDX-License-Identifier: MIT

package omp

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/sys/unix"

	"github.com/sessionbus/peer-common/testsocket"
)

type ompExecutableFixture struct {
	root, front, entry, runtime string
}

func newOMPExecutableFixture(t *testing.T) ompExecutableFixture {
	t.Helper()
	directory := testsocket.Directory(t)
	root := filepath.Join(directory, "lib", "node_modules", "@oh-my-pi", "pi-coding-agent")
	entry := filepath.Join(root, filepath.FromSlash(nativePackageEntry))
	if err := os.MkdirAll(filepath.Dir(entry), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(entry, []byte("#!/usr/bin/env bun\nprocess.exit(97)\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	manifest := `{"name":"@oh-my-pi/pi-coding-agent","version":"18.1.17","bin":{"omp":"dist/cli.js"},"engines":{"bun":">=1.3.14"}}`
	if err := os.WriteFile(filepath.Join(root, "package.json"), []byte(manifest), 0o644); err != nil {
		t.Fatal(err)
	}
	bin := filepath.Join(directory, "bin")
	if err := os.MkdirAll(bin, 0o755); err != nil {
		t.Fatal(err)
	}
	front := filepath.Join(bin, "omp")
	if err := os.Symlink(entry, front); err != nil {
		t.Fatal(err)
	}
	runtime := filepath.Join(directory, "runtime", "bun.exe")
	if err := os.MkdirAll(filepath.Dir(runtime), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(runtime, []byte("fixture Bun\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(runtime, filepath.Join(bin, "bun")); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin)
	t.Setenv(NativeBinPathEnv, "")
	// No Bun in its standard install directory unless a test adds one.
	t.Setenv("HOME", filepath.Join(directory, "home"))
	t.Setenv(nativeRuntimeInstallEnv, "")
	return ompExecutableFixture{root: root, front: front, entry: entry, runtime: runtime}
}

// writeStandardBun puts an executable Bun at directory/bin/bun, the shape of
// Bun's own install location.
func writeStandardBun(t *testing.T, directory string) string {
	t.Helper()
	bun := filepath.Join(directory, "bin", "bun")
	if err := os.MkdirAll(filepath.Dir(bun), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(bun, []byte("standard Bun\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	resolved, err := filepath.EvalSymlinks(bun)
	if err != nil {
		t.Fatal(err)
	}
	return resolved
}

// A daemon-started lane has the service PATH, which usually omits Bun's own
// install directory. Bun is then found there: $BUN_INSTALL/bin/bun when
// BUN_INSTALL is set, else $HOME/.bun/bin/bun. Before this change the lane
// failed with "exec: \"bun\": executable file not found in $PATH".
func TestResolveNativeExecutableFindsBunInItsStandardInstallDirectory(t *testing.T) {
	fixture := newOMPExecutableFixture(t)
	if err := os.Remove(filepath.Join(filepath.Dir(fixture.front), "bun")); err != nil {
		t.Fatal(err)
	}
	home := os.Getenv("HOME")
	homeBun := writeStandardBun(t, filepath.Join(home, ".bun"))
	result, err := ResolveNativeExecutable(fixture.front)
	if err != nil || result.RuntimePath != homeBun {
		t.Fatalf("home Bun = %#v, %v", result, err)
	}

	installBun := writeStandardBun(t, filepath.Join(filepath.Dir(home), "bun-install"))
	t.Setenv(nativeRuntimeInstallEnv, filepath.Dir(filepath.Dir(installBun)))
	result, err = ResolveNativeExecutable(fixture.front)
	if err != nil || result.RuntimePath != installBun {
		t.Fatalf("BUN_INSTALL Bun = %#v, %v", result, err)
	}

	// BUN_INSTALL names Bun's location; when it holds no Bun, HOME is not
	// tried, and the error is PATH's as before.
	t.Setenv(nativeRuntimeInstallEnv, filepath.Join(filepath.Dir(home), "empty-install"))
	if _, err = ResolveNativeExecutable(fixture.front); err == nil ||
		!strings.Contains(err.Error(), `runtime: exec: "bun": executable file not found in $PATH`) {
		t.Fatalf("missing BUN_INSTALL Bun = %v", err)
	}
}

func TestResolveNativeExecutablePrefersBunOnPATH(t *testing.T) {
	fixture := newOMPExecutableFixture(t)
	writeStandardBun(t, filepath.Join(os.Getenv("HOME"), ".bun"))
	result, err := ResolveNativeExecutable(fixture.front)
	if err != nil || result.RuntimePath != fixture.runtime {
		t.Fatalf("PATH Bun not preferred: %#v, %v", result, err)
	}
}

func TestResolveNativeExecutableValidatesTheStandardBun(t *testing.T) {
	fixture := newOMPExecutableFixture(t)
	if err := os.Remove(filepath.Join(filepath.Dir(fixture.front), "bun")); err != nil {
		t.Fatal(err)
	}
	bun := writeStandardBun(t, filepath.Join(os.Getenv("HOME"), ".bun"))
	if err := os.Chmod(bun, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := ResolveNativeExecutable(fixture.front); err == nil ||
		!strings.Contains(err.Error(), `runtime: exec: "bun": executable file not found in $PATH`) {
		t.Fatalf("non-executable standard Bun accepted: %v", err)
	}
}

func TestResolveNativeExecutablePublishedPackageAndBun(t *testing.T) {
	fixture := newOMPExecutableFixture(t)
	result, err := ResolveNativeExecutable(fixture.front)
	if err != nil {
		t.Fatal(err)
	}
	if result.Path != fixture.front || result.EntryPath != fixture.entry ||
		result.RuntimePath != fixture.runtime || result.PackageRoot != fixture.root {
		t.Fatalf("result = %#v", result)
	}
}

func TestResolveNativeExecutableOverrideUsesPATHAndStillValidates(t *testing.T) {
	fixture := newOMPExecutableFixture(t)
	selected := filepath.Join(filepath.Dir(fixture.front), "selected-omp")
	if err := os.Rename(fixture.front, selected); err != nil {
		t.Fatal(err)
	}
	t.Setenv(NativeBinPathEnv, "selected-omp")
	result, err := ResolveNativeExecutable("missing-front-door")
	if err != nil {
		t.Fatal(err)
	}
	if result.Path != selected || result.EntryPath != fixture.entry {
		t.Fatalf("result = %#v", result)
	}
}

func TestResolveNativeExecutableRejectsPackageDrift(t *testing.T) {
	fixture := newOMPExecutableFixture(t)
	manifest := filepath.Join(fixture.root, "package.json")
	for _, body := range []string{
		`{"name":"other","version":"18.1.17","bin":{"omp":"dist/cli.js"},"engines":{"bun":">=1.3.14"}}`,
		`{"name":"@oh-my-pi/pi-coding-agent","version":"18.1.17","bin":{"omp":"src/cli.ts"},"engines":{"bun":">=1.3.14"}}`,
	} {
		if err := os.WriteFile(manifest, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := ResolveNativeExecutable(fixture.front); err == nil || !strings.Contains(err.Error(), "metadata") {
			t.Fatalf("metadata %s accepted: %v", body, err)
		}
	}
}

func TestResolveNativeExecutableDoesNotFencePackageOrEngineVersion(t *testing.T) {
	fixture := newOMPExecutableFixture(t)
	manifest := filepath.Join(fixture.root, "package.json")
	body := `{"name":"@oh-my-pi/pi-coding-agent","version":"18.1.18","bin":{"omp":"dist/cli.js"},"engines":{"bun":">=1.3.15"}}`
	if err := os.WriteFile(manifest, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := ResolveNativeExecutable(fixture.front); err != nil {
		t.Fatalf("compatible package metadata rejected: %v", err)
	}
}

func TestResolveNativeExecutableRejectsWrongEntryRuntimeAndMode(t *testing.T) {
	fixture := newOMPExecutableFixture(t)
	if err := os.WriteFile(fixture.entry, []byte("#!/usr/bin/env node\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := ResolveNativeExecutable(fixture.front); err == nil || !strings.Contains(err.Error(), "unsupported runtime") {
		t.Fatalf("wrong shebang accepted: %v", err)
	}
	if err := os.WriteFile(fixture.entry, []byte("#!/usr/bin/env bun\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(fixture.entry, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := ResolveNativeExecutable(fixture.front); err == nil || !strings.Contains(err.Error(), "OMP executable") {
		t.Fatalf("non-executable entry accepted: %v", err)
	}
}

func TestResolveNativeExecutableRejectsInvalidBunWithoutFallback(t *testing.T) {
	fixture := newOMPExecutableFixture(t)
	if err := os.Chmod(fixture.runtime, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := ResolveNativeExecutable(fixture.front); err == nil || !strings.Contains(err.Error(), "runtime") {
		t.Fatalf("non-executable Bun accepted: %v", err)
	}
	if err := os.Remove(filepath.Join(filepath.Dir(fixture.front), "bun")); err != nil {
		t.Fatal(err)
	}
	if _, err := ResolveNativeExecutable(fixture.front); err == nil || !strings.Contains(err.Error(), "runtime") {
		t.Fatalf("missing Bun accepted: %v", err)
	}
}

func TestResolveNativeExecutableRejectsNonregularMetadataAndRuntime(t *testing.T) {
	fixture := newOMPExecutableFixture(t)
	manifest := filepath.Join(fixture.root, "package.json")
	if err := os.Remove(manifest); err != nil {
		t.Fatal(err)
	}
	if err := unix.Mkfifo(manifest, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := ResolveNativeExecutable(fixture.front); err == nil {
		t.Fatal("FIFO package metadata accepted")
	}
	if err := os.Remove(manifest); err != nil {
		t.Fatal(err)
	}
	body := `{"name":"@oh-my-pi/pi-coding-agent","version":"18.1.17","bin":{"omp":"dist/cli.js"},"engines":{"bun":">=1.3.14"}}`
	if err := os.WriteFile(manifest, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(fixture.runtime); err != nil {
		t.Fatal(err)
	}
	if err := unix.Mkfifo(fixture.runtime, 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := ResolveNativeExecutable(fixture.front); err == nil || !strings.Contains(err.Error(), "runtime") {
		t.Fatalf("FIFO Bun accepted: %v", err)
	}
}
