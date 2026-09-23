// SPDX-License-Identifier: MIT
package release_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestBootstrapReleaseSelection(t *testing.T) {
	for _, role := range []string{"pi", "omp"} {
		for _, tc := range []struct{ name, version, mirror, base, status string }{
			{"default", "", "", "https://github.com/sessionbus/pi-omp/releases/download/v0.5.0", "200"},
			{"latest", "latest", "", "https://github.com/sessionbus/pi-omp/releases/download/v0.5.0", "200"},
			{"new latest release", "", "", "https://github.com/sessionbus/pi-omp/releases/download/v12.34.56", "newrelease"},
			{"default prerelease", "", "", "https://github.com/sessionbus/pi-omp/releases/download/development", "none"},
			{"latest prerelease", "latest", "", "https://github.com/sessionbus/pi-omp/releases/download/development", "none"},
			{"release page unavailable", "", "", "https://github.com/sessionbus/pi-omp/releases/download/v0.5.0", "page504"},
			{"lookup gateway timeout", "", "", "", "504"},
			{"unexpected origin", "", "", "", "foreign"},
			{"prerelease redirect", "", "", "", "prerelease"},
			{"malformed tag", "", "", "", "badtag"},
			{"missing redirect", "", "", "", "missing"},
			{"lookup denied", "", "", "", "403"},
			{"lookup unavailable", "", "", "", "503"},
			{"lookup transport failure", "", "", "", "transport"},
			{"development", "development", "", "https://github.com/sessionbus/pi-omp/releases/download/development", ""},
			{"tag", "v0.5.0", "", "https://github.com/sessionbus/pi-omp/releases/download/v0.5.0", ""},
			{"mirror", "latest", "file:///offline/releases", "file:///offline/releases", ""},
		} {
			t.Run(role+"/"+tc.name, func(t *testing.T) {
				dir := t.TempDir()
				log := filepath.Join(dir, "curl-args")
				if err := os.WriteFile(filepath.Join(dir, "curl"), []byte(`#!/bin/sh
printf '%s\n' "$@" >> "$INSTALL_TEST_CURL_ARGS"
for arg do
 if [ "$arg" = https://github.com/sessionbus/pi-omp/releases/latest ]; then
  [ "$INSTALL_TEST_HTTP_STATUS" != transport ] || exit 7
  # A release-page outage must be irrelevant: following the redirect fails.
  for option do
   case "$option" in
    -L*|-[!-]*L*|--location|--location-trusted) printf '504 https://github.com/sessionbus/pi-omp/releases/tag/v0.5.0'; exit 0;;
   esac
  done
  case "$INSTALL_TEST_HTTP_STATUS" in
   200|page504) printf '302 https://github.com/sessionbus/pi-omp/releases/tag/v0.5.0';;
   newrelease) printf '302 https://github.com/sessionbus/pi-omp/releases/tag/v12.34.56';;
   none) printf '302 https://github.com/sessionbus/pi-omp/releases';;
   foreign) printf '302 https://example.com/releases/tag/v0.5.0';;
   prerelease) printf '302 https://github.com/sessionbus/pi-omp/releases/tag/v0.5.1-rc.1';;
   badtag) printf '302 https://github.com/sessionbus/pi-omp/releases/tag/v0.5.0/extra';;
   missing) printf '200 ';;
   *) printf '%s https://github.com/sessionbus/pi-omp/releases/latest' "$INSTALL_TEST_HTTP_STATUS";;
  esac
  exit 0
 fi
done
exit 39
`), 0700); err != nil {
					t.Fatal(err)
				}
				cmd := exec.Command("sh", filepath.Join("..", "install-"+role+".sh"))
				for _, env := range os.Environ() {
					if !strings.HasPrefix(env, "SESSIONBUS_VERSION=") && !strings.HasPrefix(env, "SESSIONBUS_DOWNLOAD_ROOT=") && !strings.HasPrefix(env, "PATH=") && !strings.HasPrefix(env, "INSTALL_TEST_CURL_ARGS=") {
						cmd.Env = append(cmd.Env, env)
					}
				}
				cmd.Env = append(cmd.Env, "PATH="+dir+string(os.PathListSeparator)+os.Getenv("PATH"), "INSTALL_TEST_CURL_ARGS="+log, "INSTALL_TEST_HTTP_STATUS="+tc.status)
				if tc.version != "" {
					cmd.Env = append(cmd.Env, "SESSIONBUS_VERSION="+tc.version)
				}
				if tc.mirror != "" {
					cmd.Env = append(cmd.Env, "SESSIONBUS_DOWNLOAD_ROOT="+tc.mirror)
				}
				if out, err := cmd.CombinedOutput(); err == nil {
					t.Fatalf("installer ignored failed download: %s", out)
				}
				args, err := os.ReadFile(log)
				if err != nil {
					t.Fatal(err)
				}
				metadata := strings.Contains(string(args), "https://github.com/sessionbus/pi-omp/releases/latest\n")
				if metadata != (tc.status != "") {
					t.Fatalf("release lookup=%t, status=%q: %s", metadata, tc.status, args)
				}
				if metadata && !strings.Contains(string(args), "%{http_code} %{redirect_url}\n") {
					t.Fatalf("lookup did not request redirect target: %s", args)
				}
				if tc.base == "" {
					if strings.Contains(string(args), ".tar.gz") {
						t.Fatalf("lookup failure attempted archive download: %s", args)
					}
					return
				}
				want := tc.base + "/" + role + "-peer-" + runtime.GOOS + "-" + runtime.GOARCH + ".tar.gz"
				if !strings.Contains("\n"+string(args), "\n"+want+"\n") {
					t.Fatalf("download arguments %q do not contain exact URL %q", args, want)
				}
			})
		}
	}
}

func TestPublishVersionValidation(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", ".github", "workflows", "binary-release.yml"))
	if err != nil {
		t.Fatal(err)
	}
	_, block, ok := strings.Cut(string(data), "      - name: Publish checksummed archives\n")
	if !ok {
		t.Fatal("publish step absent")
	}
	_, block, ok = strings.Cut(block, "        run: |\n")
	if !ok {
		t.Fatal("publish shell absent")
	}
	validation, _, ok := strings.Cut(block, "          cd dist\n")
	if !ok {
		t.Fatal("validation boundary absent")
	}
	for _, version := range []string{"development", "v0.5.0", "v12.30.400", "v1x.2.3", "v1.2.3-rc1", "v1.2.3junk", "v1.2.3.4", "v1.2", "latest", "", "v1.2.3\nv4.5.6"} {
		t.Run(version, func(t *testing.T) {
			cmd := exec.Command("sh", "-eu", "-c", validation)
			cmd.Env = append(os.Environ(), "VERSION="+version)
			out, err := cmd.CombinedOutput()
			wantOK := version == "development" || version == "v0.5.0" || version == "v12.30.400"
			if (err == nil) != wantOK {
				t.Fatalf("version %q accepted=%v, want %v: %s", version, err == nil, wantOK, out)
			}
		})
	}
}
