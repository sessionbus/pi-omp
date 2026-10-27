// SPDX-License-Identifier: MIT

package omp

import (
	"strings"
	"unicode/utf8"
)

// wordJoiner is U+2060 WORD JOINER: invisible, and neither whitespace nor an
// @ to OMP's file-mention rule.
const wordJoiner = "\u2060"

// escapeFileMentions inserts a word joiner right after each @ that OMP would
// read as the start of a file mention. OMP 18.12.2 reads the files that every
// "@path" in a prompt names (agent-session.ts #promptWithMessage), with no
// option to turn that off. An @ starts a mention at the start of the text or
// after whitespace or one of ([{<"'` (utils/file-mentions.ts
// isMentionBoundary). After the joiner, any mention OMP extracts there begins
// with U+2060 and names no file, and the visible text is unchanged.
func escapeFileMentions(text string) string {
	var output strings.Builder
	last := 0
	// An @ byte never occurs inside a multi-byte UTF-8 sequence.
	for index := 0; index < len(text); index++ {
		if text[index] != '@' || !fileMentionBoundary(text[:index]) {
			continue
		}
		output.WriteString(text[last : index+1])
		output.WriteString(wordJoiner)
		last = index + 1
	}
	if last == 0 {
		return text
	}
	output.WriteString(text[last:])
	return output.String()
}

// fileMentionBoundary reports whether an @ that follows before starts an OMP
// file mention: OMP's MENTION_BOUNDARY_REGEX, /[\s([{<"'`]/, on the character
// before it.
func fileMentionBoundary(before string) bool {
	if before == "" {
		return true
	}
	previous, _ := utf8.DecodeLastRuneInString(before)
	switch previous {
	case '(', '[', '{', '<', '"', '\'', '`':
		return true
	}
	return javaScriptSpace(previous)
}

// javaScriptSpace is JavaScript's \s: its WhiteSpace and LineTerminator
// characters. It differs from unicode.IsSpace at U+0085 and U+FEFF.
func javaScriptSpace(character rune) bool {
	switch character {
	case '\t', '\n', '\v', '\f', '\r', ' ', '\u00a0', '\u1680', '\u2028', '\u2029', '\u202f', '\u205f', '\u3000', '\ufeff':
		return true
	}
	return character >= '\u2000' && character <= '\u200a'
}
