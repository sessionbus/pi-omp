// SPDX-License-Identifier: MIT

package omp

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

var managedPluginFiles = []string{
	"omp/extension.mjs",
	"pifamily/extension/bridge.mjs",
	"pifamily/extension/inherited.mjs",
	"package.json",
	"package-lock.json",
	"node_modules/.package-lock.json",
	"node_modules/@sessionbus/kit/package.json",
	"node_modules/@sessionbus/kit/sdk/go/protocol/session.schema.json",
	"node_modules/@sessionbus/kit/sdk/js/LICENSE",
	"node_modules/@sessionbus/kit/sdk/js/TYPES.md",
	"node_modules/@sessionbus/kit/sdk/js/caller.js",
	"node_modules/@sessionbus/kit/sdk/js/connection.js",
	"node_modules/@sessionbus/kit/sdk/js/index.js",
	"node_modules/@sessionbus/kit/sdk/js/protocol.d.ts",
	"node_modules/@sessionbus/kit/sdk/js/schema.js",
}

// ResolveManagedExtension locates the fixed extension payload next to an
// installed omp-peer. It does not search or modify global OMP extension
// configuration.
func ResolveManagedExtension(peerExecutable string) (string, error) {
	if !filepath.IsAbs(peerExecutable) {
		return "", errors.New("omp-peer executable path must be absolute")
	}
	resolved, err := filepath.EvalSymlinks(peerExecutable)
	if err != nil {
		return "", err
	}
	if filepath.Base(resolved) != "omp-peer" {
		return "", errors.New("unsupported omp-peer installation layout")
	}
	plugin := filepath.Join(filepath.Dir(resolved), "plugin")
	if err := ValidateManagedPlugin(plugin); err != nil {
		return "", err
	}
	return filepath.Join(plugin, "omp", "extension.mjs"), nil
}

// ValidateManagedPlugin checks only the fixed extension files and bundled kit
// loaded for a managed launch.
func ValidateManagedPlugin(directory string) error {
	if !filepath.IsAbs(directory) {
		return errors.New("managed OMP plugin directory must be absolute")
	}
	info, err := os.Lstat(directory)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("managed OMP plugin directory must be a physical directory")
	}
	for _, name := range managedPluginFiles {
		path := filepath.Join(directory, filepath.FromSlash(name))
		if _, err := ompRegularFile(path, 1<<20); err != nil {
			return fmt.Errorf("managed OMP plugin file %s: %w", name, err)
		}
	}
	return nil
}

// InstallPlugin is the archive's maintenance validation entry. OMP is
// activated per managed launch with --extension; installation writes no
// global native extension registration.
func InstallPlugin(arguments []string) error {
	if len(arguments) != 2 || arguments[0] != "--plugin-dir" {
		return errors.New("usage: omp-peer --sessionbus-install --plugin-dir ABSOLUTE_PATH")
	}
	return ValidateManagedPlugin(arguments[1])
}
