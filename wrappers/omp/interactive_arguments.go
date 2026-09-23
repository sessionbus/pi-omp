// SPDX-License-Identifier: MIT

package omp

import (
	"encoding/json"
	"errors"
	"path/filepath"
	"slices"
	"strings"

	sessionkit "github.com/antst/sessionbus/bus/sdk/go"
	"github.com/sessionbus/peer-common/host"
)

// Native 3b3a6dc cli/flag-tables.ts. Keep one traversal for native value
// ownership and wrapper identity projection so flag-shaped values remain data.
var ompStringValueFlags = map[string]bool{
	"--cwd": true, "--config": true, "--add-dir": true, "--mode": true,
	"--fork": true, "--provider": true, "--model": true, "--smol": true,
	"--slow": true, "--plan": true, "--prewalk-into": true,
	"--plan-yolo-into": true, "--max-time": true, "--service-tier": true,
	"--api-key": true, "--system-prompt": true, "--append-system-prompt": true,
	"--provider-session-id": true, "--prompt-cache-key": true,
	"--session-dir": true, "--models": true, "--tools": true,
	"--thinking": true, "--export": true, "--hook": true,
	"--extension": true, "-e": true, "--trusted-extension": true,
	"--plugin-dir": true, "--skills": true, "--approval-mode": true,
}

var ompOptionalValueFlags = map[string]bool{"--resume": true, "-r": true, "--session": true}

var ompValuelessFlags = map[string]bool{
	"--help": true, "--version": true, "--allow-home": true,
	"--continue": true, "--from-claude": true, "--from-codex": true,
	"--no-session": true, "--no-tools": true, "--no-lsp": true,
	"--no-pty": true, "--hide-thinking": true, "--advisor": true,
	"--external-thinking": true, "--prewalk": true, "--no-prewalk": true,
	"--plan-yolo": true, "--print": true, "--print-thoughts": true,
	"--no-extensions": true, "--no-skills": true, "--no-rules": true,
	"--no-title": true, "--auto-approve": true, "--yolo": true,
}

var ompCommands = map[string]bool{
	"launch": true, "acp": true, "auth-broker": true, "auth-gateway": true,
	"agents": true, "bench": true, "browser-relay": true, "cleanse": true,
	"commit": true, "completions": true, "__complete": true, "compress": true,
	"config": true, "dry-balance": true, "gc": true, "grep": true,
	"gallery": true, "git": true, "grievances": true, "images": true,
	"img": true, "if-bench": true, "install": true, "join": true,
	"models": true, "plugin": true, "ps": true, "say": true, "share": true,
	"setup": true, "shell": true, "read": true, "render": true, "ssh": true,
	"stats": true, "update": true, "usage": true, "tiny-models": true,
	"token": true, "ttsr": true, "worktree": true, "wt": true,
	"search": true, "q": true,
}

var ompReservedWords = map[string]bool{
	"extensions": true, "list": true, "remove": true, "uninstall": true,
	"marketplace": true, "discover": true, "upgrade": true,
	"enable": true, "disable": true,
}

// InteractivePlan keeps native command behavior byte-for-byte on passthrough
// and projects only wrapper group/name flags for a managed terminal launch.
func InteractivePlan(native NativeExecutable, arguments, environment []string) (host.ExecPlan, bool, error) {
	if !filepath.IsAbs(native.RuntimePath) || !filepath.IsAbs(native.EntryPath) ||
		strings.ContainsRune(native.RuntimePath, 0) || strings.ContainsRune(native.EntryPath, 0) {
		return host.ExecPlan{}, false, errors.New("OMP native runtime and entry must be absolute")
	}
	for _, argument := range arguments {
		if strings.ContainsRune(argument, 0) {
			return host.ExecPlan{}, false, errors.New("OMP argument contains NUL")
		}
	}
	projected := ompProjectWrapperValues(arguments)
	if passthrough, boundary := ompNativePassthrough(projected.arguments); passthrough {
		if projected.err != nil {
			if boundary < 0 || boundary >= len(projected.original) || projected.original[boundary] >= projected.errorIndex {
				return host.ExecPlan{}, false, projected.err
			}
		}
		nativeArguments := slices.Clone(projected.arguments)
		if boundary >= 0 && boundary < len(projected.original) {
			nativeArguments = append(slices.Clone(projected.arguments[:boundary]), arguments[projected.original[boundary]:]...)
		}
		return ompNativeExecPlan(native, nativeArguments, environment), true, nil
	}
	if projected.err != nil {
		return host.ExecPlan{}, false, projected.err
	}

	forwarded := make([]string, 0, len(projected.arguments)+1)
	forwarded = append(forwarded, native.EntryPath)
	for index := 0; index < len(projected.arguments); index++ {
		argument := projected.arguments[index]
		if argument == "--" {
			forwarded = append(forwarded, projected.arguments[index:]...)
			break
		}
		if ompFlagName(argument) == "--trusted-extension" {
			return host.ExecPlan{}, false, errors.New("argument conflicts with managed OMP extension: --trusted-extension")
		}
		if ompConsumesNativeValue(projected.arguments, index) {
			forwarded = append(forwarded, argument, projected.arguments[index+1])
			index++
			continue
		}
		forwarded = append(forwarded, argument)
	}
	encoded, err := json.Marshal(projected.groups)
	if err != nil {
		return host.ExecPlan{}, false, err
	}
	environment = ompSetEnvironment(environment, host.GroupsEnv, string(encoded))
	environment = ompSetEnvironment(environment, host.SessionIDEnv, "")
	environment = ompSetEnvironment(environment, host.NameEnv, projected.name)
	if ompLastEnvironmentValue(environment, host.SocketEnv) == "" {
		environment = ompSetEnvironment(environment, host.SocketEnv, sessionkit.Socket())
	}
	return host.ExecPlan{Path: native.RuntimePath, Args: forwarded, Env: environment}, false, nil
}

type ompProjectedArguments struct {
	arguments  []string
	original   []int
	groups     []string
	name       string
	err        error
	errorIndex int
}

// ompProjectWrapperValues applies wrapper ownership before native routing.
// Tokens consumed by native options remain native data. The native boundary is
// retained and can never be consumed as a wrapper value.
func ompProjectWrapperValues(arguments []string) ompProjectedArguments {
	projected := ompProjectedArguments{
		arguments:  make([]string, 0, len(arguments)),
		original:   make([]int, 0, len(arguments)),
		groups:     []string{},
		errorIndex: -1,
	}
	appendNative := func(index int) {
		projected.arguments = append(projected.arguments, arguments[index])
		projected.original = append(projected.original, index)
	}
	for index := 0; index < len(arguments); index++ {
		argument := arguments[index]
		if argument == "--" {
			for ; index < len(arguments); index++ {
				appendNative(index)
			}
			break
		}
		if wrapper, value, attached := ompAttachedWrapperValue(argument); wrapper != "" && attached {
			if strings.TrimSpace(value) == "" {
				if projected.err == nil {
					projected.err, projected.errorIndex = ompWrapperValueError(wrapper), index
				}
				appendNative(index)
				continue
			}
			if wrapper == "-g" || wrapper == "--group" {
				projected.groups = append(projected.groups, value)
			} else {
				projected.name = value
			}
			continue
		}
		if argument == "-g" || argument == "--group" || argument == "-n" || argument == "--peer-name" {
			if index+1 == len(arguments) || arguments[index+1] == "--" || strings.TrimSpace(arguments[index+1]) == "" {
				if projected.err == nil {
					projected.err, projected.errorIndex = ompWrapperValueError(argument), index
				}
				appendNative(index)
				continue
			}
			value := arguments[index+1]
			if argument == "-g" || argument == "--group" {
				projected.groups = append(projected.groups, value)
			} else {
				projected.name = value
			}
			index++
			continue
		}
		appendNative(index)
		if ompConsumesNativeValue(arguments, index) {
			appendNative(index + 1)
			index++
		}
	}
	return projected
}

func ompNativeExecPlan(native NativeExecutable, arguments, environment []string) host.ExecPlan {
	args := make([]string, 0, len(arguments)+1)
	args = append(args, native.EntryPath)
	args = append(args, arguments...)
	return host.ExecPlan{Path: native.RuntimePath, Args: args, Env: environment}
}

func ompNativePassthrough(arguments []string) (bool, int) {
	residual, sources, oneShot, oneShotSource := ompProfileRoute(arguments)
	if oneShot || len(residual) == 0 {
		return oneShot, oneShotSource
	}
	first := residual[0]
	if first == "--help" || first == "-h" || first == "--version" || first == "-v" ||
		first == "help" || first == "--smoke-test" || first == "--license" || ompReservedWord(residual) {
		return true, sources[0]
	}
	launchArguments := residual
	launchSources := sources
	if ompCommands[first] {
		if first != "launch" {
			return true, sources[0]
		}
		launchArguments = residual[1:]
		launchSources = sources[1:]
	} else if index := ompLeadingCommand(residual); index >= 0 {
		if residual[index] != "launch" {
			return true, sources[index]
		}
		launchArguments = append(slices.Clone(residual[:index]), residual[index+1:]...)
		launchSources = append(slices.Clone(sources[:index]), sources[index+1:]...)
	}
	if index := ompLaunchNonInteractive(launchArguments); index >= 0 {
		return true, launchSources[index]
	}
	return false, -1
}

// ompProfileResidual mirrors native profile-bootstrap ownership only as far as
// routing needs it. Invalid/missing global values and every alias are one-shot
// native outcomes and therefore passthrough.
func ompProfileResidual(arguments []string) ([]string, bool) {
	residual, _, oneShot, _ := ompProfileRoute(arguments)
	return residual, oneShot
}

func ompProfileRoute(arguments []string) ([]string, []int, bool, int) {
	residual := make([]string, 0, len(arguments))
	sources := make([]int, 0, len(arguments))
	passThrough, sawSubcommand, canDispatch, insertBoundary := false, false, true, false
	for index := 0; index < len(arguments); index++ {
		argument := arguments[index]
		if passThrough || sawSubcommand {
			residual = append(residual, argument)
			sources = append(sources, index)
			continue
		}
		if insertBoundary {
			if !strings.HasPrefix(argument, "-") {
				residual = append(residual, ompProfileBoundary)
				sources = append(sources, -1)
			}
			insertBoundary = false
		}
		if argument == "--" {
			passThrough = true
			residual = append(residual, argument)
			sources = append(sources, index)
			continue
		}
		name, value, attached := strings.Cut(argument, "=")
		if name == "--profile" || name == "--alias" {
			if attached {
				if value == "" {
					return residual, sources, true, index
				}
				if name == "--alias" {
					return residual, sources, true, index
				}
				insertBoundary = ompNeedsProfileBoundary(residual)
				continue
			}
			if index+1 == len(arguments) || strings.HasPrefix(arguments[index+1], "-") || arguments[index+1] == "" {
				return residual, sources, true, index
			}
			index++
			if name == "--alias" {
				return residual, sources, true, index - 1
			}
			insertBoundary = ompNeedsProfileBoundary(residual)
			continue
		}
		residual = append(residual, argument)
		sources = append(sources, index)
		if ompProfileConsumesValue(arguments, index) {
			residual = append(residual, arguments[index+1])
			sources = append(sources, index+1)
			index++
			canDispatch = false
			continue
		}
		if canDispatch && ompCommands[argument] && argument != "launch" && argument != "acp" {
			sawSubcommand = true
		}
		canDispatch = false
	}
	return residual, sources, false, -1
}

func ompLeadingCommand(arguments []string) int {
	for index := 0; index < len(arguments); index++ {
		argument := arguments[index]
		if argument == "--" {
			return -1
		}
		if !strings.HasPrefix(argument, "-") {
			if ompCommands[argument] {
				return index
			}
			return -1
		}
		if ompConsumesNativeValue(arguments, index) {
			index++
		}
	}
	return -1
}

func ompLaunchNonInteractive(arguments []string) int {
	for index := 0; index < len(arguments); index++ {
		argument := arguments[index]
		if argument == "--" {
			return -1
		}
		name, value, attached := strings.Cut(argument, "=")
		switch name {
		case "--help", "-h", "--version", "-v", "--print", "-p":
			return index
		case "--mode":
			if attached || index+1 < len(arguments) {
				return index
			}
		case "--export":
			if attached && value != "" || !attached && index+1 < len(arguments) && arguments[index+1] != "" {
				return index
			}
		}
		if ompConsumesNativeValue(arguments, index) {
			index++
		}
	}
	return -1
}

const ompProfileBoundary = "--omp-profile-boundary"

func ompNeedsProfileBoundary(arguments []string) bool {
	if len(arguments) == 0 {
		return false
	}
	previous := arguments[len(arguments)-1]
	name, _, attached := strings.Cut(previous, "=")
	return !attached && (name == "--plan" || ompOptionalValueFlags[name] || ompUnknownLongValue(name))
}

func ompProfileConsumesValue(arguments []string, index int) bool {
	argument := arguments[index]
	name, _, attached := strings.Cut(argument, "=")
	if attached || index+1 >= len(arguments) {
		return false
	}
	next := arguments[index+1]
	if name == "--plan" {
		return !strings.HasPrefix(next, "-")
	}
	if ompStringValueFlags[name] {
		return true
	}
	return (ompOptionalValueFlags[name] && next != "" && !strings.HasPrefix(next, "-")) ||
		(ompUnknownLongValue(name) && !strings.HasPrefix(next, "-"))
}

func ompConsumesNativeValue(arguments []string, index int) bool {
	argument := arguments[index]
	name, _, attached := strings.Cut(argument, "=")
	if attached || index+1 >= len(arguments) {
		return false
	}
	next := arguments[index+1]
	if ompStringValueFlags[name] {
		return true
	}
	return (ompOptionalValueFlags[name] && next != "" && !strings.HasPrefix(next, "-")) ||
		(ompUnknownLongValue(name) && !strings.HasPrefix(next, "-"))
}

func ompUnknownLongValue(name string) bool {
	return strings.HasPrefix(name, "--") && !ompStringValueFlags[name] && !ompOptionalValueFlags[name] && !ompValuelessFlags[name]
}

func ompReservedWord(arguments []string) bool {
	first := arguments[0]
	if !ompReservedWords[first] {
		return false
	}
	if len(arguments) == 1 {
		return true
	}
	if first == "marketplace" && slices.Contains([]string{"add", "remove", "rm", "update", "list"}, arguments[1]) {
		return true
	}
	return slices.ContainsFunc(arguments[1:], func(argument string) bool {
		return !strings.HasPrefix(argument, "-") && strings.Contains(argument, "@")
	})
}

func ompFlagName(argument string) string {
	name, _, _ := strings.Cut(argument, "=")
	return name
}

func ompAttachedWrapperValue(argument string) (string, string, bool) {
	name, value, attached := strings.Cut(argument, "=")
	if name == "-g" || name == "--group" || name == "-n" || name == "--peer-name" {
		return name, value, attached
	}
	return "", "", false
}

func ompWrapperValueError(name string) error {
	if name == "-g" || name == "--group" {
		return errors.New("-g/--group requires a non-empty value")
	}
	return errors.New("-n/--peer-name requires a non-empty value")
}

func ompSetEnvironment(environment []string, name, value string) []string {
	prefix := name + "="
	result := slices.DeleteFunc(slices.Clone(environment), func(entry string) bool {
		return strings.HasPrefix(entry, prefix)
	})
	if value != "" {
		result = append(result, prefix+value)
	}
	return result
}
