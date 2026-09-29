// SPDX-License-Identifier: MIT

package pi

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

const NativeBinPathEnv = "PI_BIN_PATH"
const nativePackageName = "@earendil-works/pi-coding-agent"
const nativePackageEntry = "dist/bundle/cli.js"

// NativeExecutable identifies Pi's product-native JavaScript entry. It runs in
// Pi's existing Node runtime through the published executable shebang; the
// Sessionbus wrapper does not add a Node process or dependency.
type NativeExecutable struct {
	Path        string
	PackageRoot string
}

// ResolveNativeExecutable accepts only the published package layout.
// PI_BIN_PATH is an explicit operator override of the front door, not a bypass
// of package identity or entry validation.
func ResolveNativeExecutable(frontDoor string) (NativeExecutable, error) {
	selected := frontDoor
	label := "Pi executable"
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
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return NativeExecutable{}, fmt.Errorf("%s: %w", label, err)
	}
	root := filepath.Dir(filepath.Dir(filepath.Dir(resolved)))
	wantEntry := filepath.Join(root, filepath.FromSlash(nativePackageEntry))
	if resolved != wantEntry {
		return NativeExecutable{}, fmt.Errorf("%s: unsupported Pi package entry", label)
	}
	manifestBody, err := piRegularFile(filepath.Join(root, "package.json"), 256<<10)
	if err != nil {
		return NativeExecutable{}, fmt.Errorf("%s: invalid Pi package metadata: %w", label, err)
	}
	var manifest struct {
		Name string            `json:"name"`
		Bin  map[string]string `json:"bin"`
	}
	if json.Unmarshal(manifestBody, &manifest) != nil ||
		manifest.Name != nativePackageName ||
		filepath.ToSlash(strings.TrimPrefix(manifest.Bin["pi"], "./")) != nativePackageEntry {
		return NativeExecutable{}, fmt.Errorf("%s: unsupported Pi package metadata", label)
	}
	entry, err := piRegularPrefix(resolved, 64)
	if err != nil {
		return NativeExecutable{}, fmt.Errorf("%s: invalid Pi package entry: %w", label, err)
	}
	info, err := os.Stat(resolved)
	if err != nil || info.Mode().Perm()&0o111 == 0 {
		return NativeExecutable{}, fmt.Errorf("%s: Pi package entry is not executable", label)
	}
	if !strings.HasPrefix(string(entry), "#!/usr/bin/env node\n") {
		return NativeExecutable{}, fmt.Errorf("%s: Pi package entry has an unsupported runtime", label)
	}
	return NativeExecutable{Path: path, PackageRoot: root}, nil
}

func piRegularFile(path string, limit int64) ([]byte, error) {
	body, err := piRegularPrefix(path, limit+1)
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > limit {
		return nil, errors.New("expected a bounded regular file")
	}
	return body, nil
}

func piRegularPrefix(path string, limit int64) ([]byte, error) {
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
