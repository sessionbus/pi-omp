// SPDX-License-Identifier: MIT

package omp

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/sessionbus/peer-common/host"
)

type interactiveNativeOwner interface {
	Ready() <-chan struct{}
	Done() <-chan struct{}
	Err() error
	Primary() (OwnerBinding, bool)
	Close(context.Context) error
}

// RunInteractive launches one managed OMP terminal through NativeOwner. The
// owner remains the sole process, bridge, registry, and cleanup authority.
func RunInteractive(ctx context.Context, plan host.ExecPlan, native NativeExecutable, extension string) error {
	options, err := interactiveOwnerOptions(ctx, plan, native, extension, rand.Reader)
	if err != nil {
		return err
	}
	owner, err := StartNativeOwner(ctx, options)
	if err != nil {
		return err
	}
	return runInteractiveOwner(ctx, owner)
}

func runInteractiveOwner(ctx context.Context, owner interactiveNativeOwner) error {
	waitErr := waitInteractiveOwner(ctx, owner)
	closeErr := owner.Close(context.Background())
	if waitErr == nil || (closeErr != nil && errors.Is(closeErr, waitErr)) {
		return closeErr
	}
	return errors.Join(waitErr, closeErr)
}

func interactiveOwnerOptions(ctx context.Context, plan host.ExecPlan, native NativeExecutable, extension string, random io.Reader) (NativeOwnerOptions, error) {
	if ctx == nil {
		return NativeOwnerOptions{}, errors.New("OMP interactive launch requires context")
	}
	if err := context.Cause(ctx); err != nil {
		return NativeOwnerOptions{}, err
	}
	if plan.Path != native.RuntimePath || len(plan.Args) == 0 || plan.Args[0] != native.EntryPath {
		return NativeOwnerOptions{}, errors.New("managed OMP plan does not match the resolved native executable")
	}
	if err := validateManagedExtension(extension); err != nil {
		return NativeOwnerOptions{}, err
	}
	socket := ompLastEnvironmentValue(plan.Env, host.SocketEnv)
	if !filepath.IsAbs(socket) {
		return NativeOwnerOptions{}, errors.New("managed Sessionbus socket must be absolute")
	}
	for _, name := range []string{host.LocalKeyEnv, host.TokenEnv, host.SessionIDEnv} {
		if ompLastEnvironmentValue(plan.Env, name) != "" {
			return NativeOwnerOptions{}, errors.New("managed OMP plan contains stale Sessionbus ownership")
		}
	}
	var groups []string
	if err := json.Unmarshal([]byte(ompLastEnvironmentValue(plan.Env, host.GroupsEnv)), &groups); err != nil || groups == nil {
		return NativeOwnerOptions{}, errors.New("managed OMP groups are invalid")
	}
	cwd, err := os.Getwd()
	if err != nil {
		return NativeOwnerOptions{}, err
	}
	cwd, err = filepath.EvalSymlinks(cwd)
	if err != nil {
		return NativeOwnerOptions{}, err
	}
	info, err := os.Stat(cwd)
	if err != nil || !info.IsDir() {
		return NativeOwnerOptions{}, errors.Join(errors.New("managed OMP launch directory is invalid"), err)
	}
	provisional, err := newOMPInteractiveProvisional(random)
	if err != nil {
		return NativeOwnerOptions{}, err
	}
	return NativeOwnerOptions{
		DaemonSocket: socket,
		Provisional:  provisional,
		CWD:          cwd,
		Topology:     ownerTopologyInteractive,
		InitialName:  ompLastEnvironmentValue(plan.Env, host.NameEnv),
		Extension:    extension,
		Groups:       slices.Clone(groups),
		Native:       native,
		Arguments:    slices.Clone(plan.Args[1:]),
	}, nil
}

func waitInteractiveOwner(ctx context.Context, owner interactiveNativeOwner) error {
	select {
	case <-owner.Ready():
		binding, ok := owner.Primary()
		if !ok || binding.Scope != ownerScopePrimary || binding.Mode != ownerModeTUI || binding.SessionID == "" {
			return errors.New("OMP interactive primary owner is unavailable after readiness")
		}
	case <-owner.Done():
		return owner.Err()
	case <-ctx.Done():
		return context.Cause(ctx)
	}
	select {
	case <-owner.Done():
		return owner.Err()
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

func newOMPInteractiveProvisional(reader io.Reader) (string, error) {
	if reader == nil {
		return "", errors.New("OMP interactive identity source is unavailable")
	}
	var body [16]byte
	if _, err := io.ReadFull(reader, body[:]); err != nil {
		return "", errors.Join(errors.New("create OMP interactive provisional identity"), err)
	}
	return "interactive-" + hex.EncodeToString(body[:]), nil
}

func ompLastEnvironmentValue(environment []string, name string) string {
	prefix := name + "="
	for index := len(environment) - 1; index >= 0; index-- {
		if strings.HasPrefix(environment[index], prefix) {
			return strings.TrimPrefix(environment[index], prefix)
		}
	}
	return ""
}

func validateManagedExtension(extension string) error {
	if !filepath.IsAbs(extension) || filepath.Clean(extension) != extension ||
		filepath.Base(extension) != "extension.mjs" || filepath.Base(filepath.Dir(extension)) != "omp" {
		return errors.New("managed OMP extension path is invalid")
	}
	physical, err := filepath.EvalSymlinks(extension)
	if err != nil || physical != extension {
		return errors.Join(errors.New("managed OMP extension path is not physical"), err)
	}
	plugin := filepath.Dir(filepath.Dir(extension))
	if err = ValidateManagedPlugin(plugin); err != nil {
		return err
	}
	if extension != filepath.Join(plugin, "omp", "extension.mjs") {
		return errors.New("managed OMP extension path is not canonical")
	}
	return nil
}
