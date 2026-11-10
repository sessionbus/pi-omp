// SPDX-License-Identifier: MIT

package omp

import (
	"strings"

	sessionkit "github.com/antst/sessionbus/bus/sdk/go"
	"github.com/sessionbus/peer-common/host"
)

// OMP's magic keywords (ultrathink, chillax and others) act only on standalone prose:
// the matcher skips code and XML/HTML elements with their content (pi-tui
// 18.18.5 prompt/markdown-prose.ts). A rendered peer message is one balanced
// <cross-session-message> element, so its body is skipped unless the body
// unbalances it. Two constructs can: an opening tag of the same name nests,
// and a code fence left open masks the closing tag first. ompRenderPeerMessage
// neutralises both before rendering; the caller's own text never passes here.
const ompEnvelopeName = "cross-session-message"

func ompRenderPeerMessage(request sessionkit.DeliveryRequest) (string, error) {
	request.Body = closeOMPCodeFence(escapeOMPEnvelopeOpenings(request.Body))
	return host.RenderNativeMessage(request)
}

// escapeOMPEnvelopeOpenings writes `<\` for each `<` that opens a tag named
// cross-session-message in any letter case, as peer-common writes `<\/` for
// its closings. OMP compares tag names case-insensitively, and only ASCII
// letters, digits and hyphens form a name (markdown-prose.ts:18, :106).
func escapeOMPEnvelopeOpenings(body string) string {
	var output strings.Builder
	start := 0
	for index := 0; index+1+len(ompEnvelopeName) <= len(body); index++ {
		if body[index] != '<' || !asciiEqualFold(body[index+1:index+1+len(ompEnvelopeName)], ompEnvelopeName) {
			continue
		}
		if end := index + 1 + len(ompEnvelopeName); end < len(body) && ompTagNameByte(body[end]) {
			continue
		}
		output.WriteString(body[start : index+1])
		output.WriteByte('\\')
		start = index + 1
	}
	if start == 0 {
		return body
	}
	output.WriteString(body[start:])
	return output.String()
}

func asciiEqualFold(text, lower string) bool {
	for index := 0; index < len(lower); index++ {
		character := text[index]
		if 'A' <= character && character <= 'Z' {
			character += 'a' - 'A'
		}
		if character != lower[index] {
			return false
		}
	}
	return true
}

func ompTagNameByte(character byte) bool {
	return 'a' <= character && character <= 'z' || 'A' <= character && character <= 'Z' ||
		'0' <= character && character <= '9' || character == '-'
}

// closeOMPCodeFence appends the opening marker on its own line when the body
// leaves a fenced code block open by OMP's line rules (markdown-prose.ts
// phase 1, FENCE_RE /^( {0,3})([`~]{3,})/ in render/render-utils.ts:124).
// The replica must be exact: a closer appended where OMP already sees the
// fence closed would itself open a new one.
func closeOMPCodeFence(body string) string {
	open, length, marker := byte(0), 0, ""
	for _, line := range strings.Split(body, "\n") {
		run, rest, ok := ompFenceLine(line)
		if open != 0 {
			if ok && run[0] == open && len(run) >= length && ompJSTrimEmpty(rest) {
				open, length, marker = 0, 0, ""
			}
			continue
		}
		if ok && !(run[0] == '`' && strings.Contains(rest, "`")) {
			open, length, marker = run[0], len(run), run
		}
	}
	if open == 0 {
		return body
	}
	return body + "\n" + marker
}

// ompFenceLine matches FENCE_RE: up to three spaces, then a run of at least
// three backticks or tildes, mixed allowed; rest is the remainder of the line.
func ompFenceLine(line string) (string, string, bool) {
	indent := 0
	for indent < len(line) && line[indent] == ' ' {
		indent++
	}
	if indent > 3 {
		return "", "", false
	}
	end := indent
	for end < len(line) && (line[end] == '`' || line[end] == '~') {
		end++
	}
	if end-indent < 3 {
		return "", "", false
	}
	return line[indent:end], line[end:], true
}

// ompJSTrimEmpty reports whether JavaScript's String.prototype.trim leaves
// text empty: ECMAScript WhiteSpace and LineTerminator, which differ from
// Go's unicode.IsSpace (U+FEFF counts, U+0085 does not).
func ompJSTrimEmpty(text string) bool {
	for _, character := range text {
		switch {
		case character == '\t', character == '\n', character == '\v', character == '\f', character == '\r',
			character == ' ', character == 0x00A0, character == 0x1680, character == 0x2028, character == 0x2029,
			character == 0x202F, character == 0x205F, character == 0x3000, character == 0xFEFF,
			0x2000 <= character && character <= 0x200A:
		default:
			return false
		}
	}
	return true
}
