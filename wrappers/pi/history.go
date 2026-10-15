// SPDX-License-Identifier: MIT

// Package pi binds the native Pi session protocol to Sessionbus.
package pi

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	sessionkit "github.com/antst/sessionbus/bus/sdk/go"
)

const maxHistoryFrame = 8 << 20

type nativeEntry struct {
	ID, Parent, Type string
	Message          json.RawMessage
}

type nativeHistory struct {
	Entries []nativeEntry
	Leaf    string // An empty string represents the native null root.
}

// decodeHistory reads get_entries response data. It does not manufacture a
// session ID, infer completion, or interpret IDs as sortable timestamps.
func decodeHistory(body []byte) (nativeHistory, error) {
	var raw struct {
		Entries []json.RawMessage `json:"entries"`
		Leaf    json.RawMessage   `json:"leafId"`
	}
	var result nativeHistory
	if len(body) > maxHistoryFrame || json.Unmarshal(body, &raw) != nil || raw.Entries == nil {
		return result, errors.New("invalid Pi history response")
	}
	var err error
	if result.Leaf, err = entryReference(raw.Leaf); err != nil {
		return result, err
	}
	seen := make(map[string]bool, len(raw.Entries))
	for _, body := range raw.Entries {
		var entry struct {
			ID      string          `json:"id"`
			Parent  json.RawMessage `json:"parentId"`
			Type    string          `json:"type"`
			Message json.RawMessage `json:"message"`
		}
		if json.Unmarshal(body, &entry) != nil || !validEntryID(entry.ID) || entry.Type == "" || seen[entry.ID] {
			return result, errors.New("invalid or duplicate Pi history entry")
		}
		parent, err := entryReference(entry.Parent)
		if err != nil || parent == entry.ID {
			return result, errors.New("invalid Pi history parent")
		}
		seen[entry.ID] = true
		result.Entries = append(result.Entries, nativeEntry{entry.ID, parent, entry.Type, entry.Message})
	}
	return result, nil
}

func entryReference(raw json.RawMessage) (string, error) {
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return "", nil
	}
	var id string
	if json.Unmarshal(raw, &id) != nil || !validEntryID(id) {
		return "", errors.New("invalid Pi history reference")
	}
	return id, nil
}

func validEntryID(id string) bool {
	return id != "" && len(id) <= 256 && !strings.ContainsAny(id, "\x00\r\n\t ")
}

// ownedHistory keeps the owned run's entries: those after start, the leaf at
// its preflight, up to and including end, the leaf at its settle. Entries of
// work Pi ran before or after the run are not part of its result. A start
// that precedes the delta keeps it from its first entry.
func ownedHistory(delta nativeHistory, start, end string) (nativeHistory, error) {
	entries := delta.Entries
	for index, entry := range entries {
		if entry.ID == start {
			entries = entries[index+1:]
			break
		}
	}
	for index, entry := range entries {
		if entry.ID == end {
			return nativeHistory{Entries: entries[:index+1], Leaf: end}, nil
		}
	}
	return nativeHistory{}, errors.New("Pi history has no entry where the owned run settled")
}

// historyResult is called only after the controller has joined the owned
// native terminal. delta is get_entries(since: pre-submit append cursor),
// cut to the owned run, while parent is the leaf at its preflight; those IDs
// need not be equal.
// A branch switch during the Run is unavailable, not another branch's answer.
func historyResult(parent string, delta nativeHistory, prompt string) (sessionkit.TurnResult, error) {
	var result sessionkit.TurnResult
	userSeen, assistantSeen := false, false
	for _, entry := range delta.Entries {
		if entry.Parent != parent {
			return result, errors.New("Pi history changed branch during the Run")
		}
		parent = entry.ID
		if entry.Type != "message" {
			continue
		}
		var message struct {
			Role       string          `json:"role"`
			Content    json.RawMessage `json:"content"`
			StopReason string          `json:"stopReason"`
		}
		if json.Unmarshal(entry.Message, &message) != nil || message.Role == "" {
			return result, errors.New("invalid Pi native message")
		}
		if message.Role != "user" && message.Role != "assistant" {
			continue
		}
		text, err := messageText(message.Content)
		if err != nil {
			return result, err
		}
		if message.Role == "user" {
			if !userSeen && text != prompt {
				return result, errors.New("Pi native user does not match the owned preflight")
			}
			userSeen = true
			assistantSeen = false
			continue
		}
		if !userSeen {
			return result, errors.New("Pi assistant precedes the owned native user")
		}
		assistantSeen = true
		result.Result, result.NativeStopReason = text, message.StopReason
	}
	if parent != delta.Leaf || !userSeen || !assistantSeen {
		return result, errors.New("Pi terminal has no matching current assistant history")
	}
	switch result.NativeStopReason {
	case "stop":
		result.Outcome = "completed"
	case "aborted":
		result.Outcome = "interrupted"
	case "error", "length", "toolUse":
		result.Outcome = "failed"
	default:
		return result, fmt.Errorf("unknown Pi native stop reason %q", result.NativeStopReason)
	}
	return result, nil
}

func messageText(raw json.RawMessage) (string, error) {
	var text string
	if json.Unmarshal(raw, &text) == nil {
		return text, nil
	}
	var parts []struct {
		Type string          `json:"type"`
		Text json.RawMessage `json:"text"`
	}
	if json.Unmarshal(raw, &parts) != nil || parts == nil {
		return "", errors.New("invalid Pi message content")
	}
	var output strings.Builder
	for _, part := range parts {
		if part.Type == "" {
			return "", errors.New("invalid Pi message content part")
		}
		if part.Type == "text" {
			if json.Unmarshal(part.Text, &text) != nil {
				return "", errors.New("invalid Pi text part")
			}
			output.WriteString(text)
		}
	}
	return output.String(), nil
}
