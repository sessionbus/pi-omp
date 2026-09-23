// SPDX-License-Identifier: MIT

package omp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/sessionbus/peer-common/host"

	"github.com/sessionbus/peer-common/testsocket"
)

const (
	ompProcessHelperEnv   = "OMP_SESSIONBUS_PROCESS_HELPER"
	ompProcessTopologyEnv = "OMP_SESSIONBUS_PROCESS_TOPOLOGY"
)

type ompProcessCapture struct {
	Launch ompLaunch `json:"launch"`
	Args   []string  `json:"args"`
}

type ompCanceledAcceptContext struct{}

func (ompCanceledAcceptContext) Deadline() (time.Time, bool) { return time.Time{}, false }
func (ompCanceledAcceptContext) Done() <-chan struct{}       { return nil }
func (ompCanceledAcceptContext) Err() error                  { return context.Canceled }
func (ompCanceledAcceptContext) Value(any) any               { return nil }

func TestOMPProcessHelper(t *testing.T) {
	if os.Getenv(ompProcessHelperEnv) == "" {
		return
	}
	var launch ompLaunch
	wantTopology := os.Getenv(ompProcessTopologyEnv)
	if wantTopology == "" {
		wantTopology = ownerTopologyLane
	}
	if json.Unmarshal([]byte(os.Getenv(launchEnvironmentName)), &launch) != nil ||
		launch.OwnerPID != os.Getppid() || launch.Topology != wantTopology {
		_, _ = fmt.Fprintln(os.Stderr, "invalid OMP launch descriptor")
		os.Exit(9)
	}
	for _, name := range []string{
		host.SocketEnv, host.LocalKeyEnv, host.TokenEnv, host.SessionIDEnv,
		host.NameEnv, host.GroupsEnv, "SESSIONBUS_PI_LAUNCH",
	} {
		if _, present := os.LookupEnv(name); present {
			_, _ = fmt.Fprintf(os.Stderr, "helper inherited %s\n", name)
			os.Exit(9)
		}
	}
	connection, err := net.Dial("unix", launch.Socket)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(9)
	}
	arguments := os.Args[slices.Index(os.Args, "--")+1:]
	if err = json.NewEncoder(connection).Encode(ompProcessCapture{Launch: launch, Args: arguments}); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(9)
	}
	_ = connection.Close()
	if launch.Topology == ownerTopologyInteractive {
		return
	}
	_, _ = io.Copy(io.Discard, os.Stdin)
	os.Exit(0)
}

func TestOMPInteractiveProcessPreservesTopology(t *testing.T) {
	t.Setenv(ompProcessHelperEnv, "1")
	t.Setenv(ompProcessTopologyEnv, ownerTopologyInteractive)
	previous := ompCommand
	ompCommand = ompProcessHelperCommand
	t.Cleanup(func() { ompCommand = previous })
	directory := testsocket.Directory(t)
	process, err := startOMPProcess(filepath.Join(directory, "daemon.sock"), "provisional", ownerTopologyInteractive, NativeExecutable{
		RuntimePath: filepath.Join(directory, "runtime"), EntryPath: filepath.Join(directory, "entry.js"),
	}, directory, []string{"--extension", "/owned/extension.mjs", "--", "prompt"})
	if err != nil {
		t.Fatal(err)
	}
	connection, err := process.accept(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var capture ompProcessCapture
	if err = json.NewDecoder(connection).Decode(&capture); err != nil {
		t.Fatal(err)
	}
	_ = connection.Close()
	if capture.Launch.Topology != ownerTopologyInteractive {
		t.Fatalf("interactive topology = %q", capture.Launch.Topology)
	}
	if !slices.Equal(capture.Args, []string{
		filepath.Join(directory, "runtime"), filepath.Join(directory, "entry.js"),
		"--extension", "/owned/extension.mjs", "--", "prompt",
	}) {
		t.Fatalf("interactive argv = %#v", capture.Args)
	}
	if process.input != nil || process.output != nil || process.command.Stdin != os.Stdin ||
		process.command.Stdout != os.Stdout || process.command.Stderr != os.Stderr {
		t.Fatal("interactive process did not inherit the parent terminal streams")
	}
	if err = process.Wait(); err != nil {
		t.Fatal(err)
	}
	if err = process.Cleanup(); err != nil {
		t.Fatal(err)
	}
}

func ompProcessHelperCommand(name string, arguments ...string) *exec.Cmd {
	owned := []string{"-test.run=^TestOMPProcessHelper$", "--", name}
	owned = append(owned, arguments...)
	return exec.Command(os.Args[0], owned...)
}

func TestOMPOwnedProcessUsesDirectRuntimeAndPrivateDescriptor(t *testing.T) {
	t.Setenv(ompProcessHelperEnv, "1")
	for _, name := range []string{
		host.SocketEnv, host.LocalKeyEnv, host.TokenEnv, host.SessionIDEnv,
		host.NameEnv, host.GroupsEnv, "SESSIONBUS_PI_LAUNCH",
	} {
		t.Setenv(name, "must-not-leak")
	}
	previous := ompCommand
	ompCommand = ompProcessHelperCommand
	t.Cleanup(func() { ompCommand = previous })
	directory := testsocket.Directory(t)
	native := NativeExecutable{
		RuntimePath: filepath.Join(directory, "runtime"),
		EntryPath:   filepath.Join(directory, "package", "dist", "cli.js"),
	}
	process, err := startOMPProcess(
		filepath.Join(directory, "daemon.sock"), "provisional", ownerTopologyLane, native, directory,
		[]string{"--mode", "rpc-ui", "--allow-home"},
	)
	if err != nil {
		t.Fatal(err)
	}
	connection, err := process.accept(context.Background())
	if err != nil {
		process.Force()
		_ = process.Wait()
		t.Fatal(err)
	}
	var capture ompProcessCapture
	if err = json.NewDecoder(connection).Decode(&capture); err != nil {
		t.Fatal(err)
	}
	_ = connection.Close()
	wantArgs := []string{native.RuntimePath, native.EntryPath, "--mode", "rpc-ui", "--allow-home"}
	if !slices.Equal(capture.Args, wantArgs) {
		t.Fatalf("direct native argv = %#v, want %#v", capture.Args, wantArgs)
	}
	if capture.Launch.Directory != process.directory || capture.Launch.Socket != process.socket ||
		capture.Launch.OwnerPID != os.Getpid() || capture.Launch.Topology != "lane" {
		t.Fatalf("launch = %+v, process directory/socket = %q/%q", capture.Launch, process.directory, process.socket)
	}
	info, err := os.Stat(process.directory)
	if err != nil || info.Mode().Perm() != 0o700 {
		t.Fatalf("launch directory mode = %v, %v", info, err)
	}
	if _, err = os.Stat(process.socket); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("accepted private socket still exists: %v", err)
	}
	if err = process.input.Close(); err != nil {
		t.Fatal(err)
	}
	if err = process.Wait(); err != nil {
		t.Fatal(err)
	}
	if err = process.Cleanup(); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(process.directory); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("launch directory survived cleanup: %v", err)
	}
}

func TestOMPProcessForceJoinsAndCleans(t *testing.T) {
	t.Setenv(ompProcessHelperEnv, "1")
	previous := ompCommand
	ompCommand = ompProcessHelperCommand
	t.Cleanup(func() { ompCommand = previous })
	directory := testsocket.Directory(t)
	native := NativeExecutable{
		RuntimePath: filepath.Join(directory, "runtime"),
		EntryPath:   filepath.Join(directory, "entry.js"),
	}
	process, err := startOMPProcess(filepath.Join(directory, "daemon.sock"), "provisional", ownerTopologyLane, native, directory, nil)
	if err != nil {
		t.Fatal(err)
	}
	connection, err := process.accept(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var capture ompProcessCapture
	if err = json.NewDecoder(connection).Decode(&capture); err != nil {
		t.Fatal(err)
	}
	_ = connection.Close()
	process.Force()
	done := make(chan error, 1)
	go func() { done <- process.Wait() }()
	select {
	case err = <-done:
		if err == nil {
			t.Fatal("forced helper exited successfully")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("forced OMP child did not join")
	}
	if err = process.Cleanup(); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(process.directory); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("launch directory survived force cleanup: %v", err)
	}
}

func TestOMPProcessRejectsRelativeNativePaths(t *testing.T) {
	_, err := startOMPProcess(filepath.Join(testsocket.Directory(t), "daemon.sock"), "provisional", ownerTopologyLane, NativeExecutable{
		RuntimePath: "bun", EntryPath: "/entry.js",
	}, testsocket.Directory(t), nil)
	if err == nil {
		t.Fatal("relative OMP runtime was accepted")
	}
}

func TestOMPProcessAcceptCancellationJoinsAndOwnsResources(t *testing.T) {
	t.Setenv(ompProcessHelperEnv, "1")
	previous := ompCommand
	ompCommand = ompProcessHelperCommand
	t.Cleanup(func() { ompCommand = previous })
	directory := testsocket.Directory(t)
	process, err := startOMPProcess(filepath.Join(directory, "daemon.sock"), "provisional", ownerTopologyLane, NativeExecutable{
		RuntimePath: filepath.Join(directory, "runtime"), EntryPath: filepath.Join(directory, "entry.js"),
	}, directory, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		connection, err := process.accept(ctx)
		if connection != nil {
			_ = connection.Close()
		}
		result <- err
	}()
	cancel()
	if err = <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("accept cancellation = %v", err)
	}
	process.Force()
	if err = process.Wait(); err == nil {
		t.Fatal("forced helper exited successfully")
	}
	if err = process.Cleanup(); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(process.directory); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("launch directory survived cancellation cleanup: %v", err)
	}
}

func TestOMPAcceptClosesConnectionWhenCancellationWinsHandoff(t *testing.T) {
	directory := testsocket.Directory(t)
	socket := filepath.Join(directory, "bridge.sock")
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: socket, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	process := &ompProcess{listener: listener, socket: socket}
	result := make(chan error, 1)
	go func() {
		connection, err := process.accept(ompCanceledAcceptContext{})
		if connection != nil {
			_ = connection.Close()
		}
		result <- err
	}()
	client, err := net.Dial("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	if err = <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("handoff cancellation = %v", err)
	}
	if _, err = client.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
		t.Fatalf("accepted connection after canceled handoff = %v", err)
	}
	_ = client.Close()
	if _, err = os.Stat(socket); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("accepted socket survived canceled handoff: %v", err)
	}
}
