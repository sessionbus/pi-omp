// SPDX-License-Identifier: MIT

package omp

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"golang.org/x/sys/unix"
)

const NativeBinPathEnv = "OMP_BIN_PATH"
const nativePackageName = "@oh-my-pi/pi-coding-agent"
const nativePackageEntry = "dist/cli.js"
const nativeRuntimeCommand = "bun"

// NativeExecutable identifies OMP's published command, package entry, and the
// Bun executable selected by the same PATH inherited by the native child.
type NativeExecutable struct {
	Path        string
	EntryPath   string
	RuntimePath string
	PackageRoot string
}

// ResolveNativeExecutable accepts only the published OMP package layout and its
// declared Bun entry. OMP_BIN_PATH selects a front door explicitly; it never
// bypasses package, entry, or runtime validation.
func ResolveNativeExecutable(frontDoor string) (NativeExecutable, error) {
	selected := frontDoor
	label := "OMP executable"
	if override := os.Getenv(NativeBinPathEnv); override != "" {
		selected = override
		label = NativeBinPathEnv
	}
	if selected == "" || strings.ContainsRune(selected, 0) {
		return NativeExecutable{}, fmt.Errorf("%s is invalid", label)
	}
	path, err := exec.LookPath(selected)
	if err != nil {
		return NativeExecutable{}, fmt.Errorf("%s: %w", label, err)
	}
	path, err = filepath.Abs(path)
	if err != nil {
		return NativeExecutable{}, fmt.Errorf("%s: %w", label, err)
	}
	entry, err := filepath.EvalSymlinks(path)
	if err != nil {
		return NativeExecutable{}, fmt.Errorf("%s: %w", label, err)
	}
	root := filepath.Dir(filepath.Dir(entry))
	if entry != filepath.Join(root, filepath.FromSlash(nativePackageEntry)) {
		return NativeExecutable{}, fmt.Errorf("%s: unsupported OMP package entry", label)
	}

	manifestBody, err := ompRegularFile(filepath.Join(root, "package.json"), 256<<10)
	if err != nil {
		return NativeExecutable{}, fmt.Errorf("%s: invalid OMP package metadata: %w", label, err)
	}
	var manifest struct {
		Name string            `json:"name"`
		Bin  map[string]string `json:"bin"`
	}
	if json.Unmarshal(manifestBody, &manifest) != nil ||
		manifest.Name != nativePackageName ||
		filepath.ToSlash(strings.TrimPrefix(manifest.Bin["omp"], "./")) != nativePackageEntry {
		return NativeExecutable{}, fmt.Errorf("%s: unsupported OMP package metadata", label)
	}

	prefix, err := ompRegularPrefix(entry, 64)
	if err != nil {
		return NativeExecutable{}, fmt.Errorf("%s: invalid OMP package entry: %w", label, err)
	}
	entryInfo, err := os.Stat(entry)
	if err != nil || entryInfo.Mode().Perm()&0o111 == 0 {
		return NativeExecutable{}, fmt.Errorf("%s: OMP package entry is not executable", label)
	}
	if !strings.HasPrefix(string(prefix), "#!/usr/bin/env bun\n") {
		return NativeExecutable{}, fmt.Errorf("%s: OMP package entry has an unsupported runtime", label)
	}

	runtime, err := exec.LookPath(nativeRuntimeCommand)
	if err != nil {
		return NativeExecutable{}, fmt.Errorf("%s runtime: %w", label, err)
	}
	runtime, err = filepath.Abs(runtime)
	if err != nil {
		return NativeExecutable{}, fmt.Errorf("%s runtime: %w", label, err)
	}
	runtime, err = filepath.EvalSymlinks(runtime)
	if err != nil {
		return NativeExecutable{}, fmt.Errorf("%s runtime: %w", label, err)
	}
	if _, err = ompRegularPrefix(runtime, 1); err != nil {
		return NativeExecutable{}, fmt.Errorf("%s runtime: invalid Bun executable: %w", label, err)
	}
	runtimeInfo, err := os.Stat(runtime)
	if err != nil || runtimeInfo.Mode().Perm()&0o111 == 0 {
		return NativeExecutable{}, fmt.Errorf("%s runtime: Bun is not executable", label)
	}

	return NativeExecutable{
		Path: path, EntryPath: entry, RuntimePath: runtime, PackageRoot: root,
	}, nil
}

func ompRegularFile(path string, limit int64) ([]byte, error) {
	body, err := ompRegularPrefix(path, limit+1)
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > limit {
		return nil, errors.New("expected a bounded regular file")
	}
	return body, nil
}

func ompRegularPrefix(path string, limit int64) ([]byte, error) {
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_NONBLOCK|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(fd), path)
	if file == nil {
		_ = unix.Close(fd)
		return nil, errors.New("cannot own file descriptor")
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, errors.New("expected a regular file")
	}
	return io.ReadAll(io.LimitReader(file, limit))
}
