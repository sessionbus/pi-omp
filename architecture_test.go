// SPDX-License-Identifier: MIT

package sessionbus_peers_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
)

const (
	productModule           = "github.com/sessionbus/pi-omp"
	sdkModule               = "github.com/antst/sessionbus/bus/sdk/go"
	commonVersion           = "github.com/sessionbus/peer-common v0.0.0-20260922143100-eb655f686e44"
	citationCount           = 116
	citationReachabilitySHA = "70a0acc5246af380d8fb221bdd9e1ab8e6b8afa3a48e51c0c99ca58adb6a4550"
	factsHeader             = "> Historical source note: citations to pre-split Sessionbus paths resolve in\n> the Forgejo `ai/sessionbus` repository through its `legacy-*` branches.\n> Citations to product source resolve in the external repository and full\n> commit recorded by the split archive manifest. Host evidence paths are\n> immutable external artifacts, not repository paths."
)

func TestRepositoryBoundary(t *testing.T) {
	allowed := map[string]bool{
		".forgejo": true, ".git": true, ".github": true, ".gitignore": true,
		".golangci.yml": true, "LICENSE": true, "README.md": true, "RELEASE_VERSION": true,
		"architecture_test.go": true, "version_test.go": true, "cmd": true,
		"docs": true, "go.mod": true, "go.sum": true, "pi": true, "omp": true,
		"scripts": true, "wrappers": true,
	}
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if !allowed[entry.Name()] {
			t.Errorf("path outside Pi/OMP boundary: %s", entry.Name())
		}
	}
	for _, path := range []string{"internal", "wrappers/host", "wrappers/mcp", "scripts/cleanup-legacy", "go.work", "go.work.sum", "Makefile"} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Errorf("removed path remains: %s", path)
		}
	}
	if got := directoryNames(t, "cmd"); !equalStrings(got, []string{"omp-peer", "pi-peer"}) {
		t.Errorf("commands = %v", got)
	}
	if got := directoryNames(t, "wrappers"); !equalStrings(got, []string{"omp", "pi", "pifamily"}) {
		t.Errorf("wrappers = %v", got)
	}
	if _, err := os.Stat(".github/workflows/release.yml"); !os.IsNotExist(err) {
		t.Fatal("unreviewed release workflow remains")
	}
}

func TestModuleAndImportBoundary(t *testing.T) {
	module := read(t, "go.mod")
	for _, expected := range []string{"module " + productModule + "\n", sdkModule + " v0.5.7", commonVersion} {
		if !bytes.Contains(module, []byte(expected)) {
			t.Errorf("go.mod lacks %q", expected)
		}
	}
	if bytes.Contains(module, []byte("replace ")) {
		t.Fatal("local module replacement")
	}
	checkGoImports(t, ".", func(path, imported string) {
		if strings.HasPrefix(imported, "github.com/antst/sessionbus-peers") ||
			strings.HasPrefix(imported, "github.com/antst/sessionbus/wrappers/") ||
			strings.HasPrefix(imported, "github.com/antst/sessionbus/bus/internal/") {
			t.Errorf("%s retains combined-tree import %s", path, imported)
		}
		if strings.HasPrefix(imported, "github.com/antst/sessionbus/") &&
			imported != sdkModule && !strings.HasPrefix(imported, sdkModule+"/") {
			t.Errorf("%s imports non-SDK daemon package %s", path, imported)
		}
	})
	command := exec.Command("go", "list", "-m", "all")
	command.Env = append(os.Environ(), "GOWORK=off")
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("module graph: %v\n%s", err, output)
	}
	if bytes.Contains(output, []byte("github.com/antst/sessionbus\n")) {
		t.Fatal("daemon module in graph")
	}
}

func TestFactsHeadersAndReachabilityAudit(t *testing.T) {
	entries, err := os.ReadDir("docs/products")
	if err != nil {
		t.Fatal(err)
	}
	citationPattern := regexp.MustCompile("`([0-9a-f]{7,40}:[^`\\s]+)`")
	citations := make([]string, 0, citationCount)
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".md" {
			continue
		}
		path := filepath.Join("docs/products", entry.Name())
		body := read(t, path)
		lines := bytes.Split(body, []byte{'\n'})
		if len(lines) < 8 || !bytes.Equal(bytes.Join(lines[2:7], []byte{'\n'}), []byte(factsHeader)) {
			t.Errorf("facts header absent or misplaced: %s", path)
		}
		for _, match := range citationPattern.FindAllSubmatch(body, -1) {
			citations = append(citations, string(match[1]))
		}
	}
	sort.Strings(citations)
	digest := sha256.Sum256([]byte(strings.Join(citations, "\n") + "\n"))
	if len(citations) != citationCount || hex.EncodeToString(digest[:]) != citationReachabilitySHA {
		t.Fatalf("citation audit changed: count=%d SHA=%s", len(citations), hex.EncodeToString(digest[:]))
	}
}

func TestFormerBrandGuardForProductFacts(t *testing.T) {
	err := filepath.WalkDir("docs/products", func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() || filepath.Ext(path) != ".md" {
			return err
		}
		cleaned, cleanErr := removeHistoricalExceptions(read(t, path))
		if cleanErr != nil {
			return cleanErr
		}
		if containsFormerBrand(cleaned) {
			t.Errorf("former brand in facts: %s", path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestSPDXAndLicenseCoverage(t *testing.T) {
	if !bytes.Contains(read(t, "LICENSE"), []byte("MIT License")) {
		t.Fatal("root MIT license missing")
	}
	err := filepath.WalkDir(".", func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if ignoredDirectory(path) {
				return filepath.SkipDir
			}
			return nil
		}
		body := read(t, path)
		if bytes.Contains(body, []byte("SPDX-License-Identifier: "+"GPL")) {
			t.Errorf("GPL SPDX: %s", path)
		}
		switch filepath.Ext(path) {
		case ".go", ".mjs":
			if firstOrSecondLine(body) != "// SPDX-License-Identifier: MIT" {
				t.Errorf("missing MIT SPDX: %s", path)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"scripts/package-product", "scripts/install-pi.sh", "scripts/install-omp.sh", "scripts/release/install-product", "scripts/release/version.sh"} {
		if firstOrSecondLine(read(t, path)) != "# SPDX-License-Identifier: MIT" {
			t.Errorf("missing shell MIT SPDX: %s", path)
		}
	}
}

func TestPackageAndInstallerBoundary(t *testing.T) {
	pack := read(t, "scripts/package-product")
	installer := read(t, "scripts/release/install-product")
	if !bytes.Contains(pack, []byte("case \"$product\" in pi|omp)")) ||
		!bytes.Contains(installer, []byte("case \"$product\" in pi|omp)")) {
		t.Fatal("package or installer accepts unsupported product")
	}
	for _, expected := range []string{
		"wrappers/pi/extension.mjs wrappers/pi/native.mjs",
		"wrappers/omp/extension.mjs",
		"wrappers/pifamily/extension/bridge.mjs",
		"CGO_ENABLED=0",
	} {
		if !bytes.Contains(pack, []byte(expected)) {
			t.Errorf("package lacks %q", expected)
		}
	}
	for _, expected := range []string{
		"\"$root/pi-peer\" --sessionbus-install --plugin-dir \"$root/plugin\"",
		"\"$root/omp-peer\" --sessionbus-install --plugin-dir \"$root/plugin\"",
	} {
		if !bytes.Contains(installer, []byte(expected)) {
			t.Errorf("installer lacks %q", expected)
		}
	}
	for _, removed := range []string{"npm ", "node ", "grok", "qwen", "kilo", "opencode", "claude", "codex", "-peer-mcp"} {
		if bytes.Contains(pack, []byte(removed)) || bytes.Contains(installer, []byte(removed)) {
			t.Errorf("package/installer references removed or unnecessary target %q", removed)
		}
	}
	for _, product := range []string{"pi", "omp"} {
		path := "scripts/install-" + product + ".sh"
		if !bytes.Contains(read(t, path), []byte("https://github.com/sessionbus/pi-omp/releases/")) {
			t.Errorf("%s does not use destination releases", path)
		}
	}
}

func TestReadmeAndPreviewLinks(t *testing.T) {
	readme := read(t, "README.md")
	for _, exact := range []string{"scripts/package-product pi ./dist", "scripts/package-product omp ./dist",
		"docs/migration/FUNCTIONALITY-CHECKLIST.md", "preview", "node --test", "pi/README.md", "omp/README.md"} {
		if !bytes.Contains(readme, []byte(exact)) {
			t.Errorf("README lacks %q", exact)
		}
	}
	for _, path := range []string{"docs/designs/pi-omp-0.5.0/ACCEPTANCE.md", "docs/designs/pi-omp-0.5.0/DESIGN.md"} {
		if _, err := os.Stat(path); err != nil {
			t.Errorf("local acceptance link missing: %s", path)
		}
	}
}

func checkGoImports(t *testing.T, root string, check func(string, string)) {
	t.Helper()
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if ignoredDirectory(path) {
				return filepath.SkipDir
			}
			return nil
		}
		if filepath.Ext(path) != ".go" {
			return nil
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		for _, specification := range file.Imports {
			imported, err := strconv.Unquote(specification.Path.Value)
			if err != nil {
				return err
			}
			check(path, imported)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
func ignoredDirectory(path string) bool {
	base := filepath.Base(path)
	return base == ".git" || base == "node_modules" || base == "dist" || base == "bin"
}
func removeHistoricalExceptions(body []byte) ([]byte, error) {
	evidence := regexp.MustCompile(`/home/antst/agentbus-evidence/[^\x60\s]+`)
	citation := regexp.MustCompile("`[0-9a-f]{7,40}:[^`]+`")
	cleaned := make([]byte, 0, len(body))
	fenced := false
	for _, line := range bytes.Split(body, []byte{'\n'}) {
		if bytes.HasPrefix(bytes.TrimSpace(line), []byte("```")) {
			fenced = !fenced
			continue
		}
		if fenced {
			continue
		}
		line = evidence.ReplaceAll(line, nil)
		line = citation.ReplaceAll(line, nil)
		cleaned = append(cleaned, line...)
		cleaned = append(cleaned, '\n')
	}
	if fenced {
		return nil, os.ErrInvalid
	}
	return cleaned, nil
}
func containsFormerBrand(body []byte) bool {
	lower := bytes.ToLower(body)
	for _, former := range [][]byte{[]byte("agent" + "bus"), []byte("agent" + "_sessions"), []byte("agent" + "-sessions")} {
		if bytes.Contains(lower, former) {
			return true
		}
	}
	words := bytes.Join(bytes.Fields(body), []byte{' '})
	return bytes.Contains(words, []byte("Agent "+"Sessions"))
}
func directoryNames(t *testing.T, path string) []string {
	t.Helper()
	entries, err := os.ReadDir(path)
	if err != nil {
		t.Fatal(err)
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			names = append(names, entry.Name())
		}
	}
	sort.Strings(names)
	return names
}
func firstOrSecondLine(body []byte) string {
	lines := bytes.SplitN(body, []byte{'\n'}, 3)
	if len(lines) > 0 && bytes.HasPrefix(lines[0], []byte("#!")) && len(lines) > 1 {
		return string(lines[1])
	}
	if len(lines) > 0 {
		return string(lines[0])
	}
	return ""
}
func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
func read(t *testing.T, path string) []byte {
	t.Helper()
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return body
}
