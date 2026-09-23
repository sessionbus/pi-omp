// SPDX-License-Identifier: MIT

package omp

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

const launchEnvironmentName = "SESSIONBUS_OMP_LAUNCH"

var ompCommand = exec.Command
var ompStartProcess = startOMPProcess

type ompLaunch struct {
	Directory string `json:"directory"`
	OwnerPID  int    `json:"owner_pid"`
	Socket    string `json:"socket"`
	Topology  string `json:"topology"`
}

type ompProcess struct {
	command   *exec.Cmd
	lock      *host.SessionLock
	listener  *net.UnixListener
	directory string
	socket    string
	input     *os.File
	output    *os.File
	stderr    *ompLog
	done      chan struct{}

	mu       sync.Mutex
	waitErr  error
	force    sync.Once
	clean    sync.Once
	cleanErr error
}

type ompLog struct {
	mu   sync.Mutex
	data []byte
}

func (log *ompLog) Write(body []byte) (int, error) {
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

func (log *ompLog) String() string {
	log.mu.Lock()
	defer log.mu.Unlock()
	return string(append([]byte(nil), log.data...))
}

func startOMPProcess(socket, provisional, topology string, native NativeExecutable, cwd string, arguments []string) (*ompProcess, error) {
	if !filepath.IsAbs(native.RuntimePath) || !filepath.IsAbs(native.EntryPath) {
		return nil, errors.New("OMP native runtime and entry must be absolute")
	}
	if topology != ownerTopologyLane && topology != ownerTopologyInteractive {
		return nil, errors.New("OMP native topology is invalid")
	}
	lock, err := host.AcquireSessionLock(socket, "omp", provisional)
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
	directory, err := os.MkdirTemp(parent, ".omp-")
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
	launchBody, err := json.Marshal(ompLaunch{
		Directory: directory,
		OwnerPID:  os.Getpid(),
		Socket:    bridgePath,
		Topology:  topology,
	})
	if err != nil {
		return nil, err
	}
	log := &ompLog{}
	commandArguments := make([]string, 0, len(arguments)+1)
	commandArguments = append(commandArguments, native.EntryPath)
	commandArguments = append(commandArguments, arguments...)
	command := ompCommand(native.RuntimePath, commandArguments...)
	command.Dir = cwd
	var input, output *os.File
	var childInput, childOutput *os.File
	if topology == ownerTopologyLane {
		childInput, input, err = os.Pipe()
		if err != nil {
			return nil, err
		}
		defer func() {
			if failed {
				_ = childInput.Close()
				_ = input.Close()
			}
		}()
		output, childOutput, err = os.Pipe()
		if err != nil {
			return nil, err
		}
		defer func() {
			if failed {
				_ = output.Close()
				_ = childOutput.Close()
			}
		}()
		command.Stdin, command.Stdout, command.Stderr = childInput, childOutput, log
	} else {
		command.Stdin, command.Stdout, command.Stderr = os.Stdin, os.Stdout, os.Stderr
	}
	command.ExtraFiles = append(command.ExtraFiles, lock.File())
	command.Env = scrubOMPEnvironment(os.Environ(),
		host.SocketEnv, host.LocalKeyEnv, host.TokenEnv, host.SessionIDEnv,
		host.NameEnv, host.GroupsEnv, launchEnvironmentName, "SESSIONBUS_PI_LAUNCH")
	command.Env = append(command.Env, launchEnvironmentName+"="+string(launchBody))
	if err = command.Start(); err != nil {
		return nil, err
	}
	if childInput != nil {
		_ = childInput.Close()
	}
	if childOutput != nil {
		_ = childOutput.Close()
	}
	process := &ompProcess{
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

func scrubOMPEnvironment(environment []string, names ...string) []string {
	result := make([]string, 0, len(environment))
	for _, entry := range environment {
		name, _, _ := strings.Cut(entry, "=")
		if !slices.Contains(names, name) {
			result = append(result, entry)
		}
	}
	return result
}

func (process *ompProcess) accept(ctx context.Context) (net.Conn, error) {
	callbackDone := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		_ = process.listener.Close()
		close(callbackDone)
	})
	connection, err := process.listener.AcceptUnix()
	if !stop() {
		<-callbackDone
	}
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, err
	}
	_ = process.listener.Close()
	_ = os.Remove(process.socket)
	if err = ctx.Err(); err != nil {
		_ = connection.Close()
		return nil, err
	}
	return connection, nil
}

func (process *ompProcess) Wait() error {
	<-process.done
	process.mu.Lock()
	defer process.mu.Unlock()
	return process.waitErr
}

func (process *ompProcess) Force() {
	process.force.Do(func() {
		if process.command.Process != nil {
			_ = process.command.Process.Kill()
		}
		_ = process.listener.Close()
		closeOMPFile(process.input)
		closeOMPFile(process.output)
	})
}

func (process *ompProcess) Cleanup() error {
	process.clean.Do(func() {
		_ = process.listener.Close()
		closeOMPFile(process.input)
		closeOMPFile(process.output)
		process.cleanErr = errors.Join(process.lock.Close(), os.RemoveAll(process.directory))
	})
	return process.cleanErr
}

func closeOMPFile(file *os.File) {
	if file != nil {
		_ = file.Close()
	}
}
