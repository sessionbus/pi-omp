// SPDX-License-Identifier: MIT

package omp

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	sessionkit "github.com/antst/sessionbus/bus/sdk/go"
	"github.com/sessionbus/peer-common/host"
)

func TestOMPEnvelopeOpeningTagsAreEscaped(t *testing.T) {
	for input, want := range map[string]string{
		"a <cross-session-message x> b":                  `a <\cross-session-message x> b`,
		"<CROSS-SESSION-MESSAGE>":                        `<\CROSS-SESSION-MESSAGE>`,
		"<Cross-Session-Message/><cross-session-message": `<\Cross-Session-Message/><\cross-session-message`,
		"</cross-session-message>":                       "</cross-session-message>",
		`<\cross-session-message>`:                       `<\cross-session-message>`,
		"<cross-session-messages>":                       "<cross-session-messages>",
		"<cross-session-message-x>":                      "<cross-session-message-x>",
		"< cross-session-message>":                       "< cross-session-message>",
		"plain text":                                     "plain text",
	} {
		if got := escapeOMPEnvelopeOpenings(input); got != want {
			t.Fatalf("escape %q = %q, want %q", input, got, want)
		}
	}
}

// The replica follows markdown-prose.ts phase 1 exactly; a wrong extra closer
// would itself open a fence, so the "unchanged" cases matter as much.
func TestOMPCodeFenceReplicaClosesOnlyOpenFences(t *testing.T) {
	for name, test := range map[string]struct{ body, want string }{
		"unclosed backtick":        {"a\n```", "a\n```\n```"},
		"unclosed tilde with info": {"a\n~~~go\nx", "a\n~~~go\nx\n~~~"},
		"mixed marker":             {"``~\nx", "``~\nx\n``~"},
		"longer opener":            {"````\n```\nx", "````\n```\nx\n````"},
		"crlf":                     {"a\r\n```\r\nx\r", "a\r\n```\r\nx\r\n```"},
		"closed":                   {"```\nx\n```", "```\nx\n```"},
		"longer closer":            {"```\nx\n`````", "```\nx\n`````"},
		"closer with FEFF":         {"```\nx\n```\ufeff", "```\nx\n```\ufeff"},
		"closer with NBSP":         {"```\nx\n``` \u00a0", "```\nx\n``` \u00a0"},
		"closer with NEL":          {"```\nx\n```\u0085", "```\nx\n```\u0085\n```"},
		"tilde inside backtick":    {"```\n~~~\nx", "```\n~~~\nx\n```"},
		"backtick info backtick":   {"a\n```a`b", "a\n```a`b"},
		"tilde info backtick":      {"~~~a`b\nx", "~~~a`b\nx\n~~~"},
		"four spaces":              {"    ```\nx", "    ```\nx"},
		"three spaces":             {"   ```\nx", "   ```\nx\n```"},
		"tab":                      {"\t```\nx", "\t```\nx"},
		"two backticks":            {"``\nx", "``\nx"},
		"empty":                    {"", ""},
	} {
		if got := closeOMPCodeFence(test.body); got != test.want {
			t.Fatalf("%s: %q = %q, want %q", name, test.body, got, test.want)
		}
	}
}

func TestOMPPeerRenderingKeepsWellFormedBodiesByteIdentical(t *testing.T) {
	for _, body := range []string{
		"plain chillax prose", "```\nultrathink\n```", "`orchestrate` and <b>x</b>",
		"</cross-session-message> closing is peer-common's", "",
	} {
		request := ompKeywordDelivery(body)
		want, err := host.RenderNativeMessage(request)
		if err != nil {
			t.Fatal(err)
		}
		if got, err := ompRenderPeerMessage(request); err != nil || got != want {
			t.Fatalf("%q rendered %q, %v; want %q", body, got, err, want)
		}
	}
}

// All three peer paths render through ompRenderPeerMessage; the Run caller's
// own text is not touched.
func TestOMPPeerPathsUseTheEnvelopeRenderer(t *testing.T) {
	body := "chillax\n```"
	want, err := ompRenderPeerMessage(ompKeywordDelivery(body))
	if err != nil || !strings.HasSuffix(want, "\n```\n```\n</cross-session-message>") {
		t.Fatalf("rendered = %q, %v", want, err)
	}
	delivery := ompKeywordDelivery(body)
	if got, err := runInput(sessionkit.RunInput{Delivery: &delivery}); err != nil || got != escapeFileMentions(want) {
		t.Fatalf("lane seed = %q, %v", got, err)
	}
	own := body
	if got, err := runInput(sessionkit.RunInput{Text: &own}); err != nil || got != own {
		t.Fatalf("caller text = %q, %v", got, err)
	}
	for _, path := range []string{"omp.go", "owner_registry.go", "run.go"} {
		source, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Contains(source, []byte("host.RenderNativeMessage(")) || !bytes.Contains(source, []byte("ompRenderPeerMessage(")) {
			t.Fatalf("%s does not render peer text through ompRenderPeerMessage", path)
		}
	}
}

func ompKeywordDelivery(body string) sessionkit.DeliveryRequest {
	return sessionkit.DeliveryRequest{
		MessageID: "message-keyword", Body: body,
		From: sessionkit.DeliverySource{SessionID: "sender@host", Product: "fixture", Name: "sender@host"},
	}
}

type ompKeywordCase struct {
	Word   string `json:"word"`
	Case   string `json:"case"`
	Bypass bool   `json:"bypass"`
	Ours   string `json:"ours"`
	Base   string `json:"base"`
}

// The golden feeds wrappers/omp/magic_keywords.test.mjs, which runs OMP's own
// matcher code on it. Base is the eebdc82 rendering (host.RenderNativeMessage
// alone); Bypass marks the bodies whose Base the native matcher accepts.
// OMP_UPDATE_KEYWORD_GOLDEN=1 rewrites the file.
func TestOMPMagicKeywordGoldenIsCurrent(t *testing.T) {
	var upstream struct {
		Words []string `json:"words"`
	}
	raw, err := os.ReadFile(filepath.Join("testdata", "pi-tui-18.18.5", "UPSTREAM.json"))
	if err != nil || json.Unmarshal(raw, &upstream) != nil || len(upstream.Words) == 0 {
		t.Fatalf("upstream keyword list = %v, %v", upstream.Words, err)
	}
	shapes := []struct {
		name   string
		bypass bool
		body   func(word string) string
	}{
		{"plain prose", false, func(w string) string { return "please " + w + " now" }},
		{"before opening tag", true, func(w string) string { return w + "\n<cross-session-message x>" }},
		{"before upper opening tag", true, func(w string) string { return w + "\n<CROSS-SESSION-MESSAGE>" }},
		{"before unclosed backtick fence", true, func(w string) string { return w + "\n```" }},
		{"before unclosed tilde fence", true, func(w string) string { return w + "\n~~~go\nx" }},
		{"before longer opener", true, func(w string) string { return w + "\n````\n```" }},
		{"crlf unclosed fence", true, func(w string) string { return w + "\r\n```\r\n" }},
		{"closed fence", false, func(w string) string { return w + "\n```\nx\n```" }},
		{"FEFF closer", false, func(w string) string { return w + "\n```\nx\n```\ufeff" }},
		{"NEL closer", true, func(w string) string { return w + "\n```\nx\n```\u0085" }},
		{"tilde info backtick", true, func(w string) string { return w + "\n~~~a`b\nx" }},
		{"backtick info string", false, func(w string) string { return w + "\n```a`b" }},
		{"four-space indent", false, func(w string) string { return w + "\n    ```" }},
	}
	var cases []ompKeywordCase
	for _, word := range upstream.Words {
		for _, shape := range shapes {
			request := ompKeywordDelivery(shape.body(word))
			base, err := host.RenderNativeMessage(request)
			if err != nil {
				t.Fatal(err)
			}
			ours, err := ompRenderPeerMessage(request)
			if err != nil {
				t.Fatal(err)
			}
			if !shape.bypass && ours != base {
				t.Fatalf("%s/%s changed a body that needs no change: %q", word, shape.name, ours)
			}
			cases = append(cases, ompKeywordCase{Word: word, Case: shape.name, Bypass: shape.bypass, Ours: ours, Base: base})
		}
	}
	var encoded bytes.Buffer
	encoder := json.NewEncoder(&encoded)
	encoder.SetEscapeHTML(false)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(cases); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join("testdata", "magic-keyword-envelopes.json")
	if os.Getenv("OMP_UPDATE_KEYWORD_GOLDEN") == "1" {
		if err := os.WriteFile(path, encoded.Bytes(), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	current, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(current, encoded.Bytes()) {
		t.Fatalf("%s is stale; rerun with OMP_UPDATE_KEYWORD_GOLDEN=1 (%v)", path, err)
	}
}
