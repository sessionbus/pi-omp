// SPDX-License-Identifier: MIT

package pifamily

import (
	"errors"
	"io"
	"net"
	"os"
	"os/exec"
	"sync"

	"golang.org/x/sys/unix"
)

// InheritedBridgeTransport owns the parent endpoint of an anonymous bridge
// and the child copies appended to an exec.Cmd. Attach at most one bridge to a
// command, and never retry or reuse that command. Arrange Close before Start;
// immediately call CloseChildCopies after every Start result, whether Start
// succeeds or fails, and Close the transport on failure or shutdown.
type InheritedBridgeTransport struct {
	Parent io.ReadWriteCloser

	childReadFD  int
	childWriteFD int
	childFiles   []*os.File
	childOnce    sync.Once
	childErr     error
}

func (transport *InheritedBridgeTransport) ChildReadFD() int  { return transport.childReadFD }
func (transport *InheritedBridgeTransport) ChildWriteFD() int { return transport.childWriteFD }

func (transport *InheritedBridgeTransport) CloseChildCopies() error {
	transport.childOnce.Do(func() {
		for _, file := range transport.childFiles {
			transport.childErr = errors.Join(transport.childErr, file.Close())
		}
	})
	return transport.childErr
}

func (transport *InheritedBridgeTransport) Close() error {
	return errors.Join(transport.CloseChildCopies(), transport.Parent.Close())
}

// AttachPiInheritedBridge appends one AF_UNIX socket endpoint to command and
// returns the other endpoint for the Go host.
func AttachPiInheritedBridge(command *exec.Cmd) (*InheritedBridgeTransport, error) {
	if command == nil {
		return nil, errors.New("Pi inherited bridge command is nil")
	}
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM, 0)
	if err != nil {
		return nil, err
	}
	unix.CloseOnExec(fds[0])
	unix.CloseOnExec(fds[1])
	parentFile := os.NewFile(uintptr(fds[0]), "sessionbus-pi-bridge-parent")
	childFile := os.NewFile(uintptr(fds[1]), "sessionbus-pi-bridge-child")
	parent, err := net.FileConn(parentFile)
	_ = parentFile.Close()
	if err != nil {
		_ = childFile.Close()
		return nil, err
	}
	childFD := 3 + len(command.ExtraFiles)
	command.ExtraFiles = append(command.ExtraFiles, childFile)
	return &InheritedBridgeTransport{
		Parent: parent, childReadFD: childFD, childWriteFD: childFD,
		childFiles: []*os.File{childFile},
	}, nil
}

type pipeBridge struct {
	reader *os.File
	writer *os.File
	once   sync.Once
	err    error
}

func (bridge *pipeBridge) Read(body []byte) (int, error)  { return bridge.reader.Read(body) }
func (bridge *pipeBridge) Write(body []byte) (int, error) { return bridge.writer.Write(body) }
func (bridge *pipeBridge) Close() error {
	bridge.once.Do(func() {
		bridge.err = errors.Join(bridge.reader.Close(), bridge.writer.Close())
	})
	return bridge.err
}

// AttachOMPInheritedBridge appends a read pipe and a write pipe to command.
// The Bun child reads the first descriptor and writes the second.
func AttachOMPInheritedBridge(command *exec.Cmd) (*InheritedBridgeTransport, error) {
	if command == nil {
		return nil, errors.New("OMP inherited bridge command is nil")
	}
	parentRead, childWrite, err := os.Pipe()
	if err != nil {
		return nil, err
	}
	childRead, parentWrite, err := os.Pipe()
	if err != nil {
		_ = parentRead.Close()
		_ = childWrite.Close()
		return nil, err
	}
	baseFD := 3 + len(command.ExtraFiles)
	command.ExtraFiles = append(command.ExtraFiles, childRead, childWrite)
	return &InheritedBridgeTransport{
		Parent:      &pipeBridge{reader: parentRead, writer: parentWrite},
		childReadFD: baseFD, childWriteFD: baseFD + 1,
		childFiles: []*os.File{childRead, childWrite},
	}, nil
}
