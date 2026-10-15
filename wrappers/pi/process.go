// SPDX-License-Identifier: MIT

package pi

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"slices"
	"strings"
	"sync"

	"github.com/sessionbus/peer-common/host"
	"github.com/sessionbus/pi-omp/wrappers/pifamily"
)

const launchEnvironmentName = "SESSIONBUS_PI_LAUNCH"

var piCommand = exec.Command
var piAttachInheritedBridge = pifamily.AttachPiInheritedBridge
var piStartProcess = startPiProcess

type piLaunch struct {
	BridgeFD int    `json:"bridge_fd"`
	Topology string `json:"topology"`
}

type piProcess struct {
	command *exec.Cmd
	lock    *host.SessionLock
	bridge  io.ReadWriteCloser
	input   *os.File
	output  io.ReadCloser
	stderr  *piLog
	done    chan struct{}

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
	transport, err := piAttachInheritedBridge(command)
	if err != nil {
		return nil, err
	}
	defer func() {
		if failed {
			_ = transport.Close()
		}
	}()
	launchBody, err := json.Marshal(piLaunch{BridgeFD: transport.ChildReadFD(), Topology: "lane"})
	if err != nil {
		return nil, err
	}
	command.Env = scrubPiEnvironment(os.Environ(),
		host.SocketEnv, host.LocalKeyEnv, host.TokenEnv, host.SessionIDEnv,
		host.NameEnv, host.GroupsEnv, launchEnvironmentName, "SESSIONBUS_OMP_LAUNCH")
	command.Env = append(command.Env, launchEnvironmentName+"="+string(launchBody))
	startErr := command.Start()
	childCloseErr := transport.CloseChildCopies()
	if startErr != nil {
		return nil, errors.Join(startErr, childCloseErr)
	}
	if childCloseErr != nil {
		_ = command.Process.Kill()
		_ = command.Wait()
		return nil, childCloseErr
	}
	_ = stdin.Close()
	_ = stdout.Close()
	process := &piProcess{
		command: command, lock: lock, bridge: transport.Parent,
		input: input, output: output, stderr: log,
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
		_ = process.bridge.Close()
		_ = process.input.Close()
		_ = process.output.Close()
	})
}

func (process *piProcess) Cleanup() error {
	process.clean.Do(func() {
		_ = process.bridge.Close()
		_ = process.input.Close()
		_ = process.output.Close()
		process.cleanErr = process.lock.Close()
	})
	return process.cleanErr
}
