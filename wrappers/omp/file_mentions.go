// SPDX-License-Identifier: MIT

package omp

import (
	"strings"
	"unicode/utf8"
)

// wordJoiner is U+2060 WORD JOINER: invisible, and neither whitespace nor one
// of the characters after which OMP reads an @ as a file mention.
const wordJoiner = "\u2060"

// escapeFileMentions inserts a word joiner right before each @ that OMP would
// read as the start of a file mention. OMP 18.12.2 reads the files that every
// "@path" in a prompt names (agent-session.ts #promptWithMessage), with no
// option to turn that off. An @ starts a mention only at the start of the text
// or after whitespace or one of ([{<"'` (utils/file-mentions.ts
// isMentionBoundary, checked by extractFileMentions for every match). After
// the joiner the @ is at neither, so OMP extracts no mention there and
// resolves no path; the visible text is unchanged. A joiner after the @ would
// not do: the mention would still be extracted, and path resolution cancels
// its first segment against a following "..".
func escapeFileMentions(text string) string {
	var output strings.Builder
	last := 0
	// An @ byte never occurs inside a multi-byte UTF-8 sequence.
	for index := 0; index < len(text); index++ {
		if text[index] != '@' || !fileMentionBoundary(text[:index]) {
			continue
		}
		output.WriteString(text[last:index])
		output.WriteString(wordJoiner)
		last = index
	}
	if output.Len() == 0 {
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
