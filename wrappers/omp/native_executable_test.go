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
	return ompExecutableFixture{root: root, front: front, entry: entry, runtime: runtime}
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
		`{"name":"@oh-my-pi/pi-coding-agent","version":"18.1.18","bin":{"omp":"dist/cli.js"},"engines":{"bun":">=1.3.14"}}`,
		`{"name":"@oh-my-pi/pi-coding-agent","version":"18.1.17","bin":{"omp":"src/cli.ts"},"engines":{"bun":">=1.3.14"}}`,
		`{"name":"@oh-my-pi/pi-coding-agent","version":"18.1.17","bin":{"omp":"dist/cli.js"},"engines":{"bun":">=1.3.15"}}`,
	} {
		if err := os.WriteFile(manifest, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := ResolveNativeExecutable(fixture.front); err == nil || !strings.Contains(err.Error(), "metadata") {
			t.Fatalf("metadata %s accepted: %v", body, err)
		}
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
