// SPDX-License-Identifier: MIT

package pi

import (
	"encoding/json"
	"errors"
	"path/filepath"
	"slices"
	"strings"

	"github.com/sessionbus/peer-common/host"
)

const interactiveTopology = "interactive"

type interactiveLaunchDescriptor struct {
	Socket   string   `json:"socket"`
	Name     string   `json:"name"`
	Groups   []string `json:"groups"`
	Topology string   `json:"topology"`
}

// InteractiveExecPlan validates the installed native and extension, captures
// the immutable Sessionbus bootstrap, and returns the exact native exec plan.
// The native Node process owns the public Peer through the fixed extension.
func InteractiveExecPlan(plan host.ExecPlan, extension string) (host.ExecPlan, error) {
	native, err := ResolveNativeExecutable(plan.Path)
	if err != nil {
		return host.ExecPlan{}, err
	}
	if err = validateManagedExtension(extension); err != nil {
		return host.ExecPlan{}, err
	}
	return interactiveExecPlanResolved(plan, native.Path, extension)
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

func interactiveExecPlanResolved(plan host.ExecPlan, native, extension string) (host.ExecPlan, error) {
	if !filepath.IsAbs(native) || !filepath.IsAbs(extension) {
		return host.ExecPlan{}, errors.New("managed Pi executable and extension must be absolute")
	}
	socket := interactiveEnvironmentValue(plan.Env, host.SocketEnv)
	if !filepath.IsAbs(socket) {
		return host.ExecPlan{}, errors.New("managed Sessionbus socket must be absolute")
	}
	if interactiveEnvironmentValue(plan.Env, host.LocalKeyEnv) != "" || interactiveEnvironmentValue(plan.Env, host.TokenEnv) != "" {
		return host.ExecPlan{}, errors.New("managed Pi interactive transport is invalid")
	}
	var groups []string
	if err := json.Unmarshal([]byte(interactiveEnvironmentValue(plan.Env, host.GroupsEnv)), &groups); err != nil || groups == nil {
		return host.ExecPlan{}, errors.New("managed Pi groups are invalid")
	}
	for _, group := range groups {
		if !validInteractiveBootstrapText(group, 256, false) {
			return host.ExecPlan{}, errors.New("managed Pi groups are invalid")
		}
	}
	name := interactiveEnvironmentValue(plan.Env, host.NameEnv)
	if !validInteractiveBootstrapText(name, 4096, true) {
		return host.ExecPlan{}, errors.New("managed Pi initial name is invalid")
	}
	descriptor, err := json.Marshal(interactiveLaunchDescriptor{
		Socket: socket, Name: name, Groups: slices.Clone(groups), Topology: interactiveTopology,
	})
	if err != nil || len(descriptor) > 64<<10 {
		return host.ExecPlan{}, errors.New("managed Pi launch descriptor is invalid")
	}
	environment := slices.DeleteFunc(slices.Clone(plan.Env), func(entry string) bool {
		key, _, _ := strings.Cut(entry, "=")
		return strings.HasPrefix(key, "SESSIONBUS_")
	})
	environment = append(environment, InteractiveLaunchEnv+"="+string(descriptor))
	// Pi loads explicit CLI --extension paths first, in CLI order, then
	// discovered project and global extensions. Putting the managed extension
	// ahead of every user CLI extension makes it capture and scrub this
	// descriptor before any other extension module is evaluated.
	return host.ExecPlan{
		Path: native,
		Args: append([]string{"--extension", extension}, plan.Args...),
		Env:  environment,
	}, nil
}

func validInteractiveBootstrapText(value string, limit int, empty bool) bool {
	return (empty || value != "") && len(value) <= limit && !strings.ContainsRune(value, 0)
}
