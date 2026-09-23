// SPDX-License-Identifier: MIT

package pi

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/sys/unix"

	"github.com/sessionbus/peer-common/testsocket"
)

func piExecutableFixture(t *testing.T) (string, string) {
	t.Helper()
	root := filepath.Join(testsocket.Directory(t), "node_modules", "@earendil-works", "pi-coding-agent")
	entry := filepath.Join(root, filepath.FromSlash(nativePackageEntry))
	if err := os.MkdirAll(filepath.Dir(entry), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(entry, []byte("#!/usr/bin/env node\nprocess.exit(97)\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	manifest := `{"name":"@earendil-works/pi-coding-agent","version":"0.85.1","bin":{"pi":"dist/bundle/cli.js"}}`
	if err := os.WriteFile(filepath.Join(root, "package.json"), []byte(manifest), 0o644); err != nil {
		t.Fatal(err)
	}
	front := filepath.Join(testsocket.Directory(t), "pi")
	if err := os.Symlink(entry, front); err != nil {
		t.Fatal(err)
	}
	return root, front
}

func TestResolveNativeExecutablePublishedPackage(t *testing.T) {
	root, front := piExecutableFixture(t)
	t.Setenv(NativeBinPathEnv, "")
	result, err := ResolveNativeExecutable(front)
	if err != nil {
		t.Fatal(err)
	}
	if result.Path != front || result.PackageRoot != root {
		t.Fatalf("result = %#v", result)
	}
}

func TestResolveNativeExecutableOverrideUsesPATHAndStillValidates(t *testing.T) {
	root, front := piExecutableFixture(t)
	dir := filepath.Dir(front)
	if err := os.Rename(front, filepath.Join(dir, "selected-pi")); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir)
	t.Setenv(NativeBinPathEnv, "selected-pi")
	result, err := ResolveNativeExecutable("missing-front-door")
	if err != nil {
		t.Fatal(err)
	}
	if result.Path != filepath.Join(dir, "selected-pi") || result.PackageRoot != root {
		t.Fatalf("result = %#v", result)
	}
}

func TestResolveNativeExecutableRejectsPackageDrift(t *testing.T) {
	root, front := piExecutableFixture(t)
	t.Setenv(NativeBinPathEnv, "")
	for _, body := range []string{
		`{"name":"other","version":"0.85.1","bin":{"pi":"dist/bundle/cli.js"}}`,
		`{"name":"@earendil-works/pi-coding-agent","version":"0.85.2","bin":{"pi":"dist/bundle/cli.js"}}`,
		`{"name":"@earendil-works/pi-coding-agent","version":"0.85.1","bin":{"pi":"dist/cli.js"}}`,
	} {
		if err := os.WriteFile(filepath.Join(root, "package.json"), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := ResolveNativeExecutable(front); err == nil || !strings.Contains(err.Error(), "metadata") {
			t.Fatalf("metadata %s accepted: %v", body, err)
		}
	}
}

func TestResolveNativeExecutableBadOverrideDoesNotFallBack(t *testing.T) {
	_, front := piExecutableFixture(t)
	bad := filepath.Join(testsocket.Directory(t), "pi")
	if err := os.WriteFile(bad, []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv(NativeBinPathEnv, bad)
	if _, err := ResolveNativeExecutable(front); err == nil || !strings.Contains(err.Error(), NativeBinPathEnv) {
		t.Fatalf("invalid override fell back: %v", err)
	}
}

func TestResolveNativeExecutableRejectsNonregularMetadataAndRuntime(t *testing.T) {
	root, front := piExecutableFixture(t)
	t.Setenv(NativeBinPathEnv, "")
	manifest := filepath.Join(root, "package.json")
	if err := os.Remove(manifest); err != nil {
		t.Fatal(err)
	}
	if err := unix.Mkfifo(manifest, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := ResolveNativeExecutable(front); err == nil {
		t.Fatal("FIFO package metadata accepted")
	}
	if err := os.Remove(manifest); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(manifest, []byte(`{"name":"@earendil-works/pi-coding-agent","version":"0.85.1","bin":{"pi":"dist/bundle/cli.js"}}`), 0o644); err != nil {
		t.Fatal(err)
	}
	entry := filepath.Join(root, filepath.FromSlash(nativePackageEntry))
	if err := os.WriteFile(entry, []byte("#!/usr/bin/env bun\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := ResolveNativeExecutable(front); err == nil || !strings.Contains(err.Error(), "runtime") {
		t.Fatalf("wrong runtime accepted: %v", err)
	}
}
