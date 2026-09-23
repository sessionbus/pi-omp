// SPDX-License-Identifier: MIT

package omp

import (
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"testing"

	"github.com/sessionbus/peer-common/testsocket"
)

func TestOMPArchiveInstallsOnlyTheManagedLaunchPayload(t *testing.T) {
	repo, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	out, extracted := testsocket.Directory(t), testsocket.Directory(t)
	build := exec.Command("sh", "scripts/package-product", "omp", out)
	build.Dir = repo
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("package OMP: %v\n%s", err, output)
	}
	archive := filepath.Join(out, "omp-peer-"+runtime.GOOS+"-"+runtime.GOARCH+".tar.gz")
	if output, err := exec.Command("tar", "-xzf", archive, "-C", extracted).CombinedOutput(); err != nil {
		t.Fatalf("extract OMP: %v\n%s", err, output)
	}
	wantFiles := []string{
		"LICENSE", "README.md", "ROLE", "SOURCE.txt", "THIRD-PARTY-NOTICES.txt", "install", "omp-peer",
		"plugin/omp/extension.mjs", "plugin/pifamily/extension/bridge.mjs",
	}
	if got := ompArchiveFiles(t, extracted); !reflect.DeepEqual(got, wantFiles) {
		t.Fatalf("archive files = %v, want %v", got, wantFiles)
	}
	for source, installed := range map[string]string{
		"wrappers/omp/extension.mjs":              "plugin/omp/extension.mjs",
		"wrappers/pifamily/extension/bridge.mjs":  "plugin/pifamily/extension/bridge.mjs",
		"omp/README.md":                           "README.md",
		"scripts/release/THIRD-PARTY-NOTICES.txt": "THIRD-PARTY-NOTICES.txt",
	} {
		ompEqualFile(t, filepath.Join(repo, source), filepath.Join(extracted, installed))
	}
	if body, err := os.ReadFile(filepath.Join(extracted, "ROLE")); err != nil || string(body) != "omp\n" {
		t.Fatalf("archive role = %q (%v)", body, err)
	}
	if body, err := os.ReadFile(filepath.Join(extracted, "SOURCE.txt")); err != nil || len(strings.TrimSpace(string(body))) != 40 {
		t.Fatalf("archive source = %q (%v)", body, err)
	}

	home, tools := testsocket.Directory(t), testsocket.Directory(t)
	if err := os.WriteFile(filepath.Join(tools, "omp"), []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	unrelated := filepath.Join(home, ".omp", "agent", "settings.json")
	if err := os.MkdirAll(filepath.Dir(unrelated), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(unrelated, []byte("preserve native config\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	permanent := filepath.Join(home, ".local", "libexec", "sessionbus", "omp")
	for iteration := 0; iteration < 2; iteration++ {
		stale := filepath.Join(permanent, "plugin", "obsolete.mjs")
		if err := os.MkdirAll(filepath.Dir(stale), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(stale, []byte("obsolete\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		install := exec.Command("sh", filepath.Join(extracted, "install"))
		install.Stdin = nil
		install.Env = append(os.Environ(), "HOME="+home, "PATH="+tools+":"+os.Getenv("PATH"))
		if output, err := install.CombinedOutput(); err != nil {
			t.Fatalf("install OMP %d: %v\n%s", iteration, err, output)
		}
		if got := ompArchiveFiles(t, filepath.Join(permanent, "plugin")); !reflect.DeepEqual(got, []string{"omp/extension.mjs", "pifamily/extension/bridge.mjs"}) {
			t.Fatalf("installed plugin files = %v", got)
		}
		for _, name := range []string{"omp/extension.mjs", "pifamily/extension/bridge.mjs"} {
			ompEqualFile(t, filepath.Join(extracted, "plugin", name), filepath.Join(permanent, "plugin", name))
		}
	}
	if body, err := os.ReadFile(unrelated); err != nil || string(body) != "preserve native config\n" {
		t.Fatalf("native config changed: %q (%v)", body, err)
	}
	public := filepath.Join(home, ".local", "bin", "omp-peer")
	target, err := filepath.EvalSymlinks(public)
	if err != nil || target != filepath.Join(permanent, "omp-peer") {
		t.Fatalf("public binary = %q (%v)", target, err)
	}
	if extension, err := ResolveManagedExtension(public); err != nil || extension != filepath.Join(permanent, "plugin", "omp", "extension.mjs") {
		t.Fatalf("managed extension = %q (%v)", extension, err)
	}
}

func ompArchiveFiles(t *testing.T, root string) []string {
	t.Helper()
	var result []string
	if err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.Type().IsRegular() {
			relative, err := filepath.Rel(root, path)
			if err != nil {
				return err
			}
			result = append(result, filepath.ToSlash(relative))
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	sort.Strings(result)
	return result
}

func ompEqualFile(t *testing.T, left, right string) {
	t.Helper()
	a, err := os.ReadFile(left)
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(right)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(a, b) {
		t.Fatalf("files differ: %s and %s", left, right)
	}
}
