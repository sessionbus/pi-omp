// SPDX-License-Identifier: MIT
package pi

import (
	"errors"
	"fmt"
	"path/filepath"
	"regexp"
	"slices"
	"strings"

	sessionkit "github.com/antst/sessionbus/bus/sdk/go"
	"github.com/sessionbus/peer-common/host"
)

const Product = "pi-peer"

// Native d981de1 cli/args.ts. Lifecycle, identity and policy switches cannot
// override their typed owners through the generic arguments array.
var laneRules = []host.ArgumentRule{
	{Name: "--system-prompt", TakesValue: true},
	{Name: "--append-system-prompt", TakesValue: true},
	{Name: "--verbose"},
	{Name: "--offline"},
	{Name: "--provider", TakesValue: true, ConflictField: "model"},
	{Name: "--model", TakesValue: true, ConflictField: "model"},
	{Name: "--thinking", TakesValue: true, ConflictField: "reasoning_effort"},
	{Name: "--session", TakesValue: true, ConflictField: "session_id"},
	{Name: "--session-id", TakesValue: true, ConflictField: "session_id"},
	{Name: "--session-dir", TakesValue: true, ConflictField: "session_id"},
	{Name: "--resume", ConflictField: "session_id"},
	{Name: "--continue", ConflictField: "session_id"},
	{Name: "--fork", TakesValue: true, ConflictField: "session_id"},
	{Name: "--name", TakesValue: true, ConflictField: "name"},
	{Name: "--mode", TakesValue: true, ConflictField: "topology"},
	{Name: "--extension", TakesValue: true, ConflictField: "topology"},
	{Name: "--no-extensions", ConflictField: "topology"},
	{Name: "--tools", TakesValue: true, ConflictField: "permission_mode"},
	{Name: "--exclude-tools", TakesValue: true, ConflictField: "permission_mode"},
	{Name: "--no-tools", ConflictField: "permission_mode"},
	{Name: "--approve", ConflictField: "permission_mode"},
	{Name: "--no-approve", ConflictField: "permission_mode"},
}

var nativeSessionID = regexp.MustCompile(`^[A-Za-z0-9](?:[A-Za-z0-9._-]*[A-Za-z0-9])?$`)

func launchArguments(open sessionkit.OpenOptions, extension, resume string) ([]string, error) {
	if !filepath.IsAbs(extension) || strings.ContainsRune(extension, 0) {
		return nil, errors.New("Pi requires an absolute managed extension path")
	}
	if open.PermissionMode != "" && open.PermissionMode != "default" {
		return nil, fmt.Errorf("unsupported value permission_mode=%s", open.PermissionMode)
	}
	if resume != "" && (len(resume) > 256 || !nativeSessionID.MatchString(resume)) {
		return nil, errors.New("invalid Pi native resume identity")
	}
	if open.Model != "" && (len(open.Model) > 256 || strings.TrimSpace(open.Model) != open.Model || strings.ContainsAny(open.Model, "\x00\r\n")) {
		return nil, errors.New("invalid Pi native model selection")
	}
	if open.ReasoningEffort != "" && !slices.Contains([]string{"off", "minimal", "low", "medium", "high", "xhigh", "max"}, open.ReasoningEffort) {
		return nil, fmt.Errorf("unsupported value reasoning_effort=%s", open.ReasoningEffort)
	}
	for _, argument := range open.Arguments {
		if strings.ContainsRune(argument, 0) {
			return nil, errors.New("Pi argument contains NUL")
		}
	}
	forwarded, err := host.BuildArguments(open.Arguments, laneRules)
	if err != nil {
		return nil, err
	}
	args := []string{"--extension", extension, "--mode", "rpc"}
	if resume != "" {
		args = append(args, "--session", resume)
	}
	if open.Model != "" {
		args = append(args, "--model", open.Model)
	}
	if open.ReasoningEffort != "" {
		args = append(args, "--thinking", open.ReasoningEffort)
	}
	// Pi's built-in option parser recognizes separate values, not --name=value.
	// Skip each consumed value so strings that resemble flags remain data.
	for i := 0; i < len(forwarded); i++ {
		name, value, attached := strings.Cut(forwarded[i], "=")
		args = append(args, name)
		if name == "--system-prompt" || name == "--append-system-prompt" {
			if !attached {
				i++
				value = forwarded[i]
			}
			args = append(args, value)
		}
	}
	return args, nil
}
