// SPDX-License-Identifier: MIT

package omp

import (
	"bufio"
	"context"
	"encoding/json"
	"net"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	kit "github.com/antst/sessionbus/bus/sdk/go"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
	"github.com/sessionbus/peer-common/host"
	"github.com/sessionbus/peer-common/testsocket"
)

// JavaScript's \s, which OMP's patterns use.
const ompJavaScriptSpaces = `\t\n\x{0b}\f\r \x{a0}\x{1680}\x{2000}-\x{200a}\x{2028}\x{2029}\x{202f}\x{205f}\x{3000}\x{feff}`

// OMP 18.12.2's FILE_MENTION_REGEX and MENTION_BOUNDARY_REGEX
// (utils/file-mentions.ts:33, 36), written out independently of the code
// under test.
var (
	ompMentionPattern  = regexp.MustCompile(`@(?:"([^"]+)"|'([^']+)'|([^` + ompJavaScriptSpaces + `@]+))`)
	ompMentionBoundary = regexp.MustCompile("^[" + ompJavaScriptSpaces + "(\\[{<\"'`]$")
)

// ompMentionPaths applies OMP's extractFileMentions match and boundary rule
// (file-mentions.ts:168-178) and returns each mention's path before OMP trims
// punctuation from it.
func ompMentionPaths(text string) []string {
	var paths []string
	for _, match := range ompMentionPattern.FindAllStringSubmatchIndex(text, -1) {
		if match[0] > 0 {
			previous, _ := utf8.DecodeLastRuneInString(text[:match[0]])
			if !ompMentionBoundary.MatchString(string(previous)) {
				continue
			}
		}
		for group := 1; group <= 3; group++ {
			if match[2*group] >= 0 {
				paths = append(paths, text[match[2*group]:match[2*group+1]])
				break
			}
		}
	}
	return paths
}

func TestEscapeFileMentionsMarksEachMentionBoundaryAt(t *testing.T) {
	for _, test := range []struct{ text, want string }{
		{"", ""},
		{"no mention here", "no mention here"},
		{"@notes.txt", "@\u2060notes.txt"},
		{"read @notes.txt", "read @\u2060notes.txt"},
		{"(@a) [@b] {@c} <@d> \"@e\" '@f' `@g`", "(@\u2060a) [@\u2060b] {@\u2060c} <@\u2060d> \"@\u2060e\" '@\u2060f' `@\u2060g`"},
		{"\t@a\n@b\r@c\v@d\f@e", "\t@\u2060a\n@\u2060b\r@\u2060c\v@\u2060d\f@\u2060e"},
		{"\u00a0@a\u1680@b\u2000@c\u200a@d\u2028@e\u2029@f\u202f@g\u205f@h\u3000@i\ufeff@j",
			"\u00a0@\u2060a\u1680@\u2060b\u2000@\u2060c\u200a@\u2060d\u2028@\u2060e\u2029@\u2060f\u202f@\u2060g\u205f@\u2060h\u3000@\u2060i\ufeff@\u2060j"},
		{"@\"two words.txt\"", "@\u2060\"two words.txt\""},
		// The second @ is inside OMP's quoted mention; once the first is
		// escaped OMP no longer reads the quotes, so it is escaped too.
		{"@\"a @b\"", "@\u2060\"a @\u2060b\""},
		{"x@y.z user@host a@@b @@c", "x@y.z user@host a@@b @\u2060@c"},
		// Not JavaScript whitespace, so no mention boundary.
		{"\u0085@a \u2060@b \u00e9@c", "\u0085@a \u2060@b \u00e9@c"},
	} {
		got := escapeFileMentions(test.text)
		if got != test.want {
			t.Errorf("escapeFileMentions(%q) = %q, want %q", test.text, got, test.want)
		}
		for _, path := range ompMentionPaths(got) {
			if !strings.HasPrefix(path, wordJoiner) {
				t.Errorf("escapeFileMentions(%q) leaves OMP mention %q", test.text, path)
			}
		}
	}
}

// A peer message that starts a lane Run reaches OMP with every file mention
// escaped and its other text unchanged; the owner's own Run input is sent as
// given.
func TestRunInputEscapesOnlyThePeerSeed(t *testing.T) {
	delivery := kit.DeliveryRequest{
		MessageID: "mention", Body: "read @notes.txt, then @\"plan b.md\" and mail x@y.z",
		From: kit.DeliverySource{SessionID: "sender@local", Product: "fixture", Groups: []string{}},
	}
	rendered, err := host.RenderNativeMessage(delivery)
	if err != nil {
		t.Fatal(err)
	}
	if got := ompMentionPaths(rendered); !slices.Equal(got, []string{"notes.txt,", "plan b.md"}) {
		t.Fatalf("OMP mentions in the rendered message = %q", got)
	}
	seed, err := runInput(kit.RunInput{Delivery: &delivery})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(seed, wordJoiner) != 2 || strings.ReplaceAll(seed, wordJoiner, "") != rendered {
		t.Fatalf("seed = %q, rendered %q", seed, rendered)
	}
	if got := ompMentionPaths(seed); len(got) != 2 || !strings.HasPrefix(got[0], wordJoiner) || !strings.HasPrefix(got[1], wordJoiner) {
		t.Fatalf("OMP mentions in the seed = %q", got)
	}

	owner := "read @notes.txt, then @\"plan b.md\""
	input, err := runInput(kit.RunInput{Text: &owner})
	if err != nil || input != owner {
		t.Fatalf("owner input = %q, %v", input, err)
	}
}

// The Worker's idle delivery reaches OMP's prompt with its mentions escaped.
func TestOMPWorkerDeliverySeedReachesOMPWithItsMentionsEscaped(t *testing.T) {
	fixture := newOMPRunFixture(t)
	listener, err := net.Listen("unix", filepath.Join(testsocket.Directory(t), "bus.sock"))
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv(host.SocketEnv, listener.Addr().String())
	t.Setenv(host.TokenEnv, "omp-mention-worker-token")
	t.Setenv(host.LocalKeyEnv, "")
	worker := kit.NewWorker(&ompWorkerProduct{fixture.wrapper})
	fixture.wrapper.SetCaller(worker.Caller())
	served := make(chan error, 1)
	go func() { served <- worker.Serve(context.Background()) }()
	connection, err := listener.Accept()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = connection.Close()
		_ = listener.Close()
		worker.Shutdown()
		<-worker.Closed()
		<-served
	})
	reader := bufio.NewReader(connection)
	read := func() protocol.Frame {
		t.Helper()
		if err := connection.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
			t.Fatal(err)
		}
		line, readErr := reader.ReadBytes('\n')
		if readErr != nil {
			t.Fatal(readErr)
		}
		frame, decodeErr := protocol.DecodeFrame(line[:len(line)-1])
		if decodeErr != nil {
			t.Fatal(decodeErr)
		}
		return frame
	}
	write := func(body []byte, err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		if _, writeErr := connection.Write(body); writeErr != nil {
			t.Fatal(writeErr)
		}
	}
	hello := read()
	write(protocol.ResultBytes(hello.ID, hello.Method, struct{}{}))
	write(protocol.RequestBytes(1, "session.open", kit.OpenRequest{Name: "managed@local", Groups: []string{}}))
	if frame := read(); frame.ID != 1 || frame.Error != nil {
		t.Fatalf("Open response = %+v", frame)
	}
	delivery := kit.DeliveryRequest{
		RunID: "g/1", MessageID: "delivery-mention", Body: "read @notes.txt",
		From: kit.DeliverySource{SessionID: "sender@local", Product: "fixture", Groups: []string{}},
	}
	write(protocol.RequestBytes(2, "message.deliver", delivery))
	command := fixture.peer.read(t)
	message := nativeRPCField(t, command, "message")
	if nativeRPCField(t, command, "type") != "prompt" || !strings.Contains(message, "read @\u2060notes.txt") ||
		strings.Contains(message, "@notes.txt") {
		t.Fatalf("seed prompt = %q", message)
	}
	// End the Run through OMP's refusal, whose frames are known.
	fixture.refuse(t, nativeRPCField(t, command, "id"))
	receiptSeen, readySeen := false, false
	for !receiptSeen || !readySeen {
		frame := read()
		switch {
		case !frame.Request && frame.ID == 2:
			receiptSeen = true
		case frame.Request && frame.Method == "turn.ready":
			var ready protocol.TurnReady
			if json.Unmarshal(frame.Params, &ready) != nil || ready.State != "unavailable" {
				t.Fatalf("refused terminal = %s", frame.Params)
			}
			write(protocol.ResultBytes(frame.ID, frame.Method, struct{}{}))
			readySeen = true
		default:
			t.Fatalf("unexpected Worker frame = %+v", frame)
		}
	}
	_ = connection.SetReadDeadline(time.Time{})
	fixture.completeOwnedRun(t, 1, "next prompt", "next answer")
	fixture.requireLane(t)
}
