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
	"sync"

	"github.com/sessionbus/peer-common/host"
)

const launchEnvironmentName = "SESSIONBUS_PI_LAUNCH"

var piCommand = exec.Command
var piStartProcess = startPiProcess

type piLaunch struct {
	Directory string `json:"directory"`
	OwnerPID  int    `json:"owner_pid"`
	Socket    string `json:"socket"`
	Topology  string `json:"topology"`
}

type piProcess struct {
	command   *exec.Cmd
	lock      *host.SessionLock
	listener  *net.UnixListener
	directory string
	socket    string
	input     *os.File
	output    *os.File
	stderr    *piLog
	done      chan struct{}

	mu       sync.Mutex
	waitErr  error
	force    sync.Once
	clean    sync.Once
	cleanErr error
}

type piLog struct {
	mu   sync.Mutex
	data []byte
}

func (log *piLog) Write(body []byte) (int, error) {
	log.mu.Lock()
	defer log.mu.Unlock()
	n := len(body)
	if left := (32 << 10) - len(log.data); left > 0 {
		if len(body) > left {
			body = body[:left]
		}
		log.data = append(log.data, body...)
	}
	return n, nil
}

func (log *piLog) String() string {
	log.mu.Lock()
	defer log.mu.Unlock()
	return string(append([]byte(nil), log.data...))
}

func startPiProcess(socket, provisional, executable, cwd string, arguments []string) (*piProcess, error) {
	lock, err := host.AcquireSessionLock(socket, "pi", provisional)
	if err != nil {
		return nil, err
	}
	failed := true
	defer func() {
		if failed {
			_ = lock.Close()
		}
	}()
	parent, err := filepath.EvalSymlinks(filepath.Dir(socket))
	if err != nil {
		return nil, err
	}
	directory, err := os.MkdirTemp(parent, ".pi-")
	if err != nil {
		return nil, err
	}
	defer func() {
		if failed {
			_ = os.RemoveAll(directory)
		}
	}()
	bridgePath := filepath.Join(directory, "bridge.sock")
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: bridgePath, Net: "unix"})
	if err != nil {
		return nil, err
	}
	defer func() {
		if failed {
			_ = listener.Close()
		}
	}()
	if err = os.Chmod(bridgePath, 0o600); err != nil {
		return nil, err
	}
	launchBody, err := json.Marshal(piLaunch{Directory: directory, OwnerPID: os.Getpid(), Socket: bridgePath, Topology: "lane"})
	if err != nil {
		return nil, err
	}
	stdin, input, err := os.Pipe()
	if err != nil {
		return nil, err
	}
	defer func() {
		if failed {
			_ = stdin.Close()
			_ = input.Close()
		}
	}()
	output, stdout, err := os.Pipe()
	if err != nil {
		return nil, err
	}
	defer func() {
		if failed {
			_ = output.Close()
			_ = stdout.Close()
		}
	}()
	log := &piLog{}
	command := piCommand(executable, arguments...)
	command.Dir = cwd
	command.Stdin, command.Stdout, command.Stderr = stdin, stdout, log
	command.ExtraFiles = append(command.ExtraFiles, lock.File())
	command.Env = scrubPiEnvironment(os.Environ(),
		host.SocketEnv, host.LocalKeyEnv, host.TokenEnv, host.SessionIDEnv,
		host.NameEnv, host.GroupsEnv, launchEnvironmentName, "SESSIONBUS_OMP_LAUNCH")
	command.Env = append(command.Env, launchEnvironmentName+"="+string(launchBody))
	if err = command.Start(); err != nil {
		return nil, err
	}
	_ = stdin.Close()
	_ = stdout.Close()
	process := &piProcess{
		command: command, lock: lock, listener: listener, directory: directory,
		socket: bridgePath, input: input, output: output, stderr: log,
		done: make(chan struct{}),
	}
	go func() {
		process.mu.Lock()
		process.waitErr = command.Wait()
		process.mu.Unlock()
		close(process.done)
	}()
	failed = false
	return process, nil
}

func scrubPiEnvironment(environment []string, names ...string) []string {
	result := make([]string, 0, len(environment))
	for _, entry := range environment {
		name, _, _ := strings.Cut(entry, "=")
		if !slices.Contains(names, name) {
			result = append(result, entry)
		}
	}
	return result
}

func (process *piProcess) accept(ctx context.Context) (net.Conn, error) {
	stop := context.AfterFunc(ctx, func() { _ = process.listener.Close() })
	connection, err := process.listener.AcceptUnix()
	stop()
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, err
	}
	_ = process.listener.Close()
	_ = os.Remove(process.socket)
	return connection, nil
}

func (process *piProcess) Wait() error {
	<-process.done
	process.mu.Lock()
	defer process.mu.Unlock()
	return process.waitErr
}

func (process *piProcess) Force() {
	process.force.Do(func() {
		if process.command.Process != nil {
			_ = process.command.Process.Kill()
		}
		_ = process.listener.Close()
		_ = process.input.Close()
		_ = process.output.Close()
	})
}

func (process *piProcess) Cleanup() error {
	process.clean.Do(func() {
		_ = process.listener.Close()
		_ = process.input.Close()
		_ = process.output.Close()
		process.cleanErr = errors.Join(process.lock.Close(), os.RemoveAll(process.directory))
	})
	return process.cleanErr
}
