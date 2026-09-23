// SPDX-License-Identifier: MIT

package pi

import (
	"errors"
	"slices"
	"strings"

	sessionkit "github.com/antst/sessionbus/bus/sdk/go"
	"github.com/sessionbus/peer-common/host"
)

const InteractiveLaunchEnv = "SESSIONBUS_PI_LAUNCH"

// Native d981de1 cli/args.ts and command dispatcher. This list exists only so
// wrapper flags are not parsed out of a native option's following value.
var interactiveValueOptions = []string{
	"--provider", "--model", "--api-key", "--system-prompt", "--append-system-prompt",
	"--mode", "--session", "--session-id", "--fork", "--session-dir", "--name",
	"--models", "--tools", "-t", "--exclude-tools", "-xt", "--thinking", "--extension", "-e",
	"--skill", "--prompt-template", "--theme", "--use-theme", "--export",
	"--tui-mode",
}

var passthroughCommands = []string{"install", "remove", "uninstall", "update", "list", "config", "auth"}

// InteractivePlan classifies native maintenance/help before adding managed
// identity. Passthrough retains the original argv and environment byte values.
func InteractivePlan(arguments, environment []string) (host.ExecPlan, bool, error) {
	native, err := nativeNonTUI(arguments)
	if err != nil {
		return host.ExecPlan{}, false, err
	}
	if native {
		return host.ExecPlan{Path: "pi", Args: arguments, Env: environment}, true, nil
	}
	plan, _, err := host.ClassifiedInteractivePlan("pi", arguments, environment, host.PeerIdentity{}, func(value string) bool {
		if strings.Contains(value, "=") {
			return false
		}
		return slices.Contains(interactiveValueOptions, value)
	}, nil)
	if err != nil {
		return host.ExecPlan{}, false, err
	}
	for index := 0; index < len(plan.Args); index++ {
		argument := plan.Args[index]
		if argument == "--" {
			break
		}
		if argument == "--yolo" {
			plan.Args[index] = "--approve"
			continue
		}
		name, _, attached := strings.Cut(argument, "=")
		if name == "--mode" {
			return host.ExecPlan{}, false, errors.New("argument conflicts with managed Pi topology: --mode")
		}
		if slices.Contains(interactiveValueOptions, name) && !attached {
			index++
		}
	}
	if err = validateManagedToolArguments(plan.Args); err != nil {
		return host.ExecPlan{}, false, err
	}
	if interactiveEnvironmentValue(plan.Env, host.SocketEnv) == "" {
		plan.Env = setInteractiveEnvironment(plan.Env, host.SocketEnv, sessionkit.Socket())
	}
	return plan, false, nil
}

// validateManagedToolArguments mirrors native d981de1 cli/args.ts tool-list
// parsing. Pi's explicit lists are comma-separated, whitespace-trimmed,
// case-sensitive names; repeated list options use the last supplied value.
// Attached long values are not part of that native grammar.
func validateManagedToolArguments(arguments []string) error {
	var allowed, excluded []string
	var noTools, allowedSet, excludedSet bool
	for index := 0; index < len(arguments); index++ {
		argument := arguments[index]
		if argument == "--" {
			break
		}
		switch argument {
		case "--no-tools", "-nt":
			noTools = true
		case "--tools", "-t":
			if index+1 < len(arguments) {
				index++
				allowed = nativeToolList(arguments[index])
				allowedSet = true
			}
			continue
		case "--exclude-tools", "-xt":
			if index+1 < len(arguments) {
				index++
				excluded = nativeToolList(arguments[index])
				excludedSet = true
			}
			continue
		}
		if slices.Contains(interactiveValueOptions, argument) && index+1 < len(arguments) {
			index++
		}
	}
	if allowedSet && !slices.Contains(allowed, "sessionbus") {
		return errors.New("argument disables managed Pi Sessionbus tool: --tools must include sessionbus")
	}
	if excludedSet && slices.Contains(excluded, "sessionbus") {
		return errors.New("argument disables managed Pi Sessionbus tool: --exclude-tools contains sessionbus")
	}
	if noTools && !allowedSet {
		return errors.New("argument disables managed Pi Sessionbus tool: --no-tools")
	}
	return nil
}

func nativeToolList(value string) []string {
	result := make([]string, 0, strings.Count(value, ",")+1)
	for item := range strings.SplitSeq(value, ",") {
		if item = strings.TrimSpace(item); item != "" {
			result = append(result, item)
		}
	}
	return result
}

func nativeNonTUI(arguments []string) (bool, error) {
	if len(arguments) > 0 && slices.Contains(passthroughCommands, arguments[0]) {
		return true, nil
	}
	for index := 0; index < len(arguments); index++ {
		argument := arguments[index]
		if argument == "--" {
			return false, nil
		}
		name, value, attached := strings.Cut(argument, "=")
		if name == "-g" || name == "--group" || name == "-n" || name == "--peer-name" {
			if attached {
				if strings.TrimSpace(value) == "" {
					return false, wrapperValueError(name)
				}
				continue
			}
			if index+1 == len(arguments) || arguments[index+1] == "--" || strings.TrimSpace(arguments[index+1]) == "" {
				return false, wrapperValueError(name)
			}
			index++
			continue
		}
		switch argument {
		case "-h", "--help", "-v", "--version", "-p", "--print", "--list-models":
			return true, nil
		case "--export":
			// A value is required before Pi selects its one-shot export path.
			if index+1 < len(arguments) {
				return true, nil
			}
		}
		if slices.Contains(interactiveValueOptions, argument) {
			if index+1 < len(arguments) {
				index++
			}
			continue
		}
		// Extension flags are long options. Pi consumes their following plain
		// value, so a word such as "install" there is not a native command.
		if strings.HasPrefix(argument, "--") && !strings.Contains(argument, "=") && index+1 < len(arguments) &&
			!strings.HasPrefix(arguments[index+1], "-") && !strings.HasPrefix(arguments[index+1], "@") {
			index++
		}
	}
	return false, nil
}

func wrapperValueError(name string) error {
	if name == "-g" || name == "--group" {
		return errors.New("-g/--group requires a non-empty value")
	}
	return errors.New("-n/--peer-name requires a non-empty value")
}

func interactiveEnvironmentValue(environment []string, name string) string {
	prefix := name + "="
	for index := len(environment) - 1; index >= 0; index-- {
		if strings.HasPrefix(environment[index], prefix) {
			return strings.TrimPrefix(environment[index], prefix)
		}
	}
	return ""
}

func setInteractiveEnvironment(environment []string, name, value string) []string {
	prefix := name + "="
	result := slices.DeleteFunc(slices.Clone(environment), func(entry string) bool {
		return strings.HasPrefix(entry, prefix)
	})
	if value != "" {
		result = append(result, prefix+value)
	}
	return result
}
