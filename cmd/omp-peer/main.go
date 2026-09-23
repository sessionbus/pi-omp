// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"syscall"

	sessionkit "github.com/antst/sessionbus/bus/sdk/go"
	"github.com/sessionbus/peer-common/host"
	"github.com/sessionbus/peer-common/peerversion"
	"github.com/sessionbus/pi-omp/wrappers/omp"
)

var (
	ompResolveNative    = omp.ResolveNativeExecutable
	ompResolveExtension = omp.ResolveManagedExtension
	ompRunInteractive   = omp.RunInteractive
	ompExecNative       = syscall.Exec
	ompExecutable       = os.Executable
)

type ompCaughtSignal struct{ signal os.Signal }

func (s ompCaughtSignal) Error() string           { return "caught " + s.signal.String() }
func (s ompCaughtSignal) CaughtSignal() os.Signal { return s.signal }

func main() {
	arguments, report, handled := peerversion.Resolve("omp-peer", filepath.Base(os.Args[0]), os.Args[1:])
	if handled {
		fmt.Fprintln(os.Stdout, report)
		return
	}
	ctx, stop := ompProcessContext(host.LaneMode())
	defer stop()
	if err := run(ctx, arguments, ompTerminal(os.Stdin), ompTerminal(os.Stdout)); err != nil {
		fmt.Fprintln(os.Stderr, err)
		code := 1
		var native *exec.ExitError
		if errors.As(err, &native) {
			if native.ExitCode() >= 0 {
				code = native.ExitCode()
			} else if status, ok := native.Sys().(syscall.WaitStatus); ok && status.Signaled() {
				code = 128 + int(status.Signal())
			}
		}
		os.Exit(code)
	}
}

func ompProcessContext(lane bool) (context.Context, func()) {
	ctx, cancel := context.WithCancelCause(context.Background())
	signals := []os.Signal{syscall.SIGTERM, syscall.SIGHUP}
	if lane {
		signals = append(signals, os.Interrupt)
	}
	controlled := make(chan os.Signal, 1)
	signal.Notify(controlled, signals...)
	// In an interactive foreground process group, native OMP consumes terminal
	// Ctrl-C. Keep the launcher alive without translating it to another signal.
	var interrupts chan os.Signal
	if !lane {
		interrupts = make(chan os.Signal, 1)
		signal.Notify(interrupts, os.Interrupt)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		select {
		case caught := <-controlled:
			cancel(ompCaughtSignal{caught})
		case <-ctx.Done():
		}
	}()
	return ctx, func() {
		signal.Stop(controlled)
		if interrupts != nil {
			signal.Stop(interrupts)
		}
		cancel(context.Canceled)
		<-done
	}
}

func run(ctx context.Context, arguments []string, stdinTTY, stdoutTTY bool) error {
	if len(arguments) > 0 && arguments[0] == "--sessionbus-install" {
		return omp.InstallPlugin(arguments[1:])
	}
	// Worker stdin/stdout are pipes. Lane ownership must therefore be selected
	// before the interactive non-terminal passthrough rule.
	if host.LaneMode() {
		if len(arguments) != 0 {
			return errors.New("lane mode accepts no arguments")
		}
		native, err := ompResolveNative("omp")
		if err != nil {
			return err
		}
		peer, err := ompExecutable()
		if err != nil {
			return err
		}
		extension, err := ompResolveExtension(peer)
		if err != nil {
			return err
		}
		product := omp.New(os.Getenv(host.SocketEnv), host.LaunchTokenDigest(os.Getenv(host.TokenEnv)), native, extension)
		worker := sessionkit.NewWorker(product)
		product.SetShutdown(worker.Shutdown)
		product.SetCaller(worker.Caller())
		return worker.Serve(ctx)
	}

	native, err := ompResolveNative("omp")
	if err != nil {
		return err
	}
	if !stdinTTY || !stdoutTTY {
		return execOMPNative(native, arguments, os.Environ())
	}
	plan, passthrough, err := omp.InteractivePlan(native, arguments, os.Environ())
	if err != nil {
		return err
	}
	if passthrough {
		return execOMPPlan(plan)
	}
	peer, err := ompExecutable()
	if err != nil {
		return err
	}
	extension, err := ompResolveExtension(peer)
	if err != nil {
		return err
	}
	return ompRunInteractive(ctx, plan, native, extension)
}

func execOMPNative(native omp.NativeExecutable, arguments, environment []string) error {
	args := make([]string, 0, len(arguments)+2)
	args = append(args, native.RuntimePath, native.EntryPath)
	args = append(args, arguments...)
	return ompExecNative(native.RuntimePath, args, environment)
}

func execOMPPlan(plan host.ExecPlan) error {
	return ompExecNative(plan.Path, append([]string{plan.Path}, plan.Args...), plan.Env)
}
