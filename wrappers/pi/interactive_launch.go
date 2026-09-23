// SPDX-License-Identifier: MIT

package pi

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"syscall"

	"github.com/sessionbus/peer-common/host"
	"github.com/sessionbus/pi-omp/wrappers/pifamily"
)

const interactiveBridgeSocket = "owner.sock"

var (
	piInteractiveCommand = exec.Command
	piInteractiveListen  = net.Listen
)

type interactiveLaunchDescriptor struct {
	Directory string `json:"directory"`
	OwnerPID  int    `json:"owner_pid"`
	Socket    string `json:"socket"`
	Topology  string `json:"topology"`
}

type interactiveAcceptResult struct {
	conn net.Conn
	err  error
}

// RunInteractive launches the pinned native Pi entry as this process's direct
// child. The per-launch extension, private bridge, public Peer, and native
// process all share one lifetime; ordinary Pi invocations load none of them.
func RunInteractive(ctx context.Context, plan host.ExecPlan, extension string) error {
	if ctx == nil {
		return errors.New("Pi interactive launch requires context")
	}
	native, err := ResolveNativeExecutable(plan.Path)
	if err != nil {
		return err
	}
	if err = validateManagedExtension(extension); err != nil {
		return err
	}
	return runInteractiveResolved(ctx, plan, native.Path, extension)
}

func validateManagedExtension(extension string) error {
	if !filepath.IsAbs(extension) || filepath.Base(extension) != "extension.mjs" || filepath.Base(filepath.Dir(extension)) != "pi" {
		return errors.New("managed Pi extension path is invalid")
	}
	plugin := filepath.Dir(filepath.Dir(extension))
	if err := ValidateManagedPlugin(plugin); err != nil {
		return err
	}
	want := filepath.Join(plugin, "pi", "extension.mjs")
	if extension != want {
		return errors.New("managed Pi extension path is not canonical")
	}
	return nil
}

func runInteractiveResolved(ctx context.Context, plan host.ExecPlan, native, extension string) (result error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !filepath.IsAbs(native) || !filepath.IsAbs(extension) {
		return errors.New("managed Pi executable and extension must be absolute")
	}
	publicSocket := interactiveEnvironmentValue(plan.Env, host.SocketEnv)
	if !filepath.IsAbs(publicSocket) {
		return errors.New("managed Sessionbus socket must be absolute")
	}
	if interactiveEnvironmentValue(plan.Env, host.LocalKeyEnv) != "" {
		return errors.New("local key transport is not supported")
	}
	var groups []string
	if err := json.Unmarshal([]byte(interactiveEnvironmentValue(plan.Env, host.GroupsEnv)), &groups); err != nil || groups == nil {
		return errors.New("managed Pi groups are invalid")
	}
	initialName := interactiveEnvironmentValue(plan.Env, host.NameEnv)
	if !validInteractiveText(initialName, 4096) {
		return errors.New("managed Pi initial name is invalid")
	}

	parent := filepath.Dir(publicSocket)
	if err := os.MkdirAll(parent, 0o700); err != nil {
		return err
	}
	parent, err := filepath.EvalSymlinks(parent)
	if err != nil {
		return err
	}
	info, err := os.Stat(parent)
	if err != nil || !info.IsDir() {
		return errors.Join(errors.New("managed Sessionbus socket parent is not a directory"), err)
	}
	directory, err := os.MkdirTemp(parent, "pi-")
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, os.RemoveAll(directory)) }()
	if err = os.Chmod(directory, 0o700); err != nil {
		return err
	}
	endpoint := filepath.Join(directory, interactiveBridgeSocket)
	if len(endpoint) >= 104 {
		return errors.New("managed Pi bridge path exceeds Unix socket limit")
	}
	listener, err := piInteractiveListen("unix", endpoint)
	if err != nil {
		return err
	}
	listenerClosed := false
	closeListener := func() error {
		if listenerClosed {
			return nil
		}
		listenerClosed = true
		err := listener.Close()
		if errors.Is(err, net.ErrClosed) {
			return nil
		}
		return err
	}
	defer func() { result = errors.Join(result, closeListener()) }()
	if err = os.Chmod(endpoint, 0o600); err != nil {
		return err
	}
	if err = validateInteractiveEndpoint(directory, endpoint); err != nil {
		return err
	}

	descriptor, err := json.Marshal(interactiveLaunchDescriptor{
		Directory: directory, OwnerPID: os.Getpid(), Socket: endpoint, Topology: interactiveTopology,
	})
	if err != nil {
		return err
	}
	if len(descriptor) > 64<<10 {
		return errors.New("managed Pi launch descriptor exceeds 64 KiB")
	}
	environment := slices.DeleteFunc(slices.Clone(plan.Env), func(entry string) bool {
		key, _, _ := strings.Cut(entry, "=")
		return strings.HasPrefix(key, "SESSIONBUS_")
	})
	environment = append(environment, InteractiveLaunchEnv+"="+string(descriptor))

	ownerLifetime, cancelOwner := context.WithCancel(context.Background())
	defer cancelOwner()
	owner, err := newInteractiveOwner(ownerLifetime, publicSocket, directory, initialName, groups)
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, owner.Close()) }()

	accepted := make(chan interactiveAcceptResult, 1)
	go func() {
		conn, acceptErr := listener.Accept()
		accepted <- interactiveAcceptResult{conn, acceptErr}
	}()
	acceptTransferred := false
	defer func() {
		if acceptTransferred {
			return
		}
		_ = closeListener()
		accept := <-accepted
		if accept.conn != nil {
			result = errors.Join(result, accept.conn.Close())
		}
	}()

	args := append([]string{"--extension", extension}, plan.Args...)
	child := piInteractiveCommand(native, args...)
	child.Env = environment
	child.Stdin, child.Stdout, child.Stderr = os.Stdin, os.Stdout, os.Stderr
	if err = ctx.Err(); err != nil {
		return err
	}
	if err = child.Start(); err != nil {
		return err
	}
	childDone := make(chan error, 1)
	go func() { childDone <- child.Wait() }()
	childJoined := false
	joinChild := func(stop bool) error {
		if childJoined {
			return nil
		}
		if stop {
			stopErr := child.Process.Signal(piInteractiveStopSignal(ctx))
			if stopErr != nil && !errors.Is(stopErr, os.ErrProcessDone) {
				childErr := <-childDone
				childJoined = true
				return errors.Join(stopErr, childErr)
			}
		}
		childErr := <-childDone
		childJoined = true
		return childErr
	}
	defer func() {
		if !childJoined {
			result = errors.Join(result, joinChild(true))
		}
	}()

	var acceptedConn net.Conn
	select {
	case accept := <-accepted:
		acceptTransferred = true
		if accept.err != nil {
			return errors.Join(accept.err, joinChild(true))
		}
		acceptedConn = accept.conn
	case childErr := <-childDone:
		childJoined = true
		return childErr
	case <-ctx.Done():
		return joinChild(true)
	}
	if err = validateInteractiveEndpoint(directory, endpoint); err != nil {
		_ = acceptedConn.Close()
		return errors.Join(err, joinChild(true))
	}
	if err = closeListener(); err != nil {
		_ = acceptedConn.Close()
		return errors.Join(err, joinChild(true))
	}

	assigned := make(chan struct{})
	bridge, err := pifamily.NewBridge(acceptedConn, pifamily.BridgeHost, func(callCtx context.Context, method string, raw json.RawMessage) (json.RawMessage, error) {
		<-assigned
		return owner.handleBridge(callCtx, method, raw)
	}, pifamily.BridgeLimits{})
	if err != nil {
		close(assigned)
		_ = acceptedConn.Close()
		return errors.Join(err, joinChild(true))
	}
	defer func() {
		bridgeErr := bridge.Close()
		if childJoined && errors.Is(bridgeErr, pifamily.ErrBridgeClosed) {
			bridgeErr = nil
		}
		result = errors.Join(result, bridgeErr)
	}()
	if err = owner.assignBridge(bridge); err != nil {
		close(assigned)
		return errors.Join(err, joinChild(true))
	}
	close(assigned)

	bridgeReady := make(chan error, 1)
	go func() { bridgeReady <- bridge.Ready(ownerLifetime) }()
	select {
	case err = <-bridgeReady:
		if err != nil {
			return piInteractiveJoinedBridgeResult(owner, joinChild(true), err)
		}
	case childErr := <-childDone:
		childJoined = true
		return piInteractiveJoinedBridgeResult(owner, childErr, bridge.Close(), <-bridgeReady)
	case <-ctx.Done():
		return piInteractiveJoinedBridgeResult(owner, joinChild(true), bridge.Close(), <-bridgeReady)
	}

	// A bridge hello proves transport only. Managed ownership starts only after
	// native owner.ready and the nested current-session description are both
	// accepted by the public daemon.
	for {
		select {
		case <-owner.Ready():
			goto active
		case childErr := <-childDone:
			childJoined = true
			return childErr
		case <-owner.Done():
			return errors.Join(owner.Err(), joinChild(true))
		case <-bridge.Done():
			if owner.gracefulNativeEnd() {
				return joinChild(false)
			}
			return errors.Join(bridge.Err(), joinChild(true))
		case <-ctx.Done():
			return joinChild(true)
		}
	}

active:
	select {
	case childErr := <-childDone:
		childJoined = true
		if ownerErr := owner.Err(); ownerErr != nil {
			return errors.Join(childErr, ownerErr)
		}
		return childErr
	case <-owner.Done():
		return errors.Join(owner.Err(), joinChild(true))
	case <-bridge.Done():
		if owner.gracefulNativeEnd() {
			return joinChild(false)
		}
		return errors.Join(bridge.Err(), joinChild(true))
	case <-ctx.Done():
		return joinChild(true)
	}
}

func piInteractiveJoinedBridgeResult(owner *interactiveOwner, childErr error, bridgeErrs ...error) error {
	if childErr == nil && owner.gracefulNativeEnd() {
		for index, bridgeErr := range bridgeErrs {
			if errors.Is(bridgeErr, pifamily.ErrBridgeClosed) {
				bridgeErrs[index] = nil
			}
		}
	}
	return errors.Join(append([]error{childErr}, bridgeErrs...)...)
}

func validateInteractiveEndpoint(directory, endpoint string) error {
	dir, err := os.Lstat(directory)
	if err != nil || !dir.IsDir() || dir.Mode().Perm()&0o077 != 0 {
		return errors.Join(errors.New("managed Pi launch directory is not private"), err)
	}
	info, err := os.Lstat(endpoint)
	if err != nil || info.Mode()&os.ModeSocket == 0 || info.Mode().Perm()&0o177 != 0 {
		return errors.Join(errors.New("managed Pi bridge is not a private Unix socket"), err)
	}
	return nil
}

func piInteractiveStopSignal(ctx context.Context) os.Signal {
	var caught interface{ CaughtSignal() os.Signal }
	if errors.As(context.Cause(ctx), &caught) {
		if signal := caught.CaughtSignal(); signal == syscall.SIGHUP || signal == syscall.SIGTERM {
			return signal
		}
	}
	return syscall.SIGTERM
}
