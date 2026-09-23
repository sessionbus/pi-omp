// SPDX-License-Identifier: MIT

package sessionbus_peers_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestPublicVersionFlagsNeverStartNativeProducts(t *testing.T) {
	const revision = "0123456789abcdef0123456789abcdef01234567"
	products := []struct {
		name  string
		short string
	}{
		{"pi", "-v"},
		{"omp", "-v"},
	}
	root := t.TempDir()
	bin := filepath.Join(root, "bin")
	native := filepath.Join(root, "native")
	if err := os.MkdirAll(bin, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(native, 0o700); err != nil {
		t.Fatal(err)
	}
	for _, product := range products {
		peer := product.name + "-peer"
		build := exec.Command("go", "build", "-trimpath", "-ldflags=-X github.com/sessionbus/peer-common/peerversion.Release=v0.5.2 -X github.com/sessionbus/peer-common/peerversion.Revision="+revision, "-o", filepath.Join(bin, peer), "./cmd/"+peer)
		build.Env = append(os.Environ(), "GOWORK=off")
		if output, err := build.CombinedOutput(); err != nil {
			t.Fatalf("build %s: %v\n%s", peer, err, output)
		}
		fake := "#!/bin/sh\nprintf invoked > \"$PEER_NATIVE_MARKER\"\nexit 97\n"
		if err := os.WriteFile(filepath.Join(native, product.name), []byte(fake), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	for _, product := range products {
		peer := product.name + "-peer"
		for _, flag := range []string{"--version", product.short} {
			t.Run(peer+"/"+flag, func(t *testing.T) {
				marker := filepath.Join(root, peer+"-"+strings.TrimLeft(flag, "-")+"-native")
				command := exec.Command(filepath.Join(bin, peer), flag)
				command.Env = append(os.Environ(), "PATH="+native, "PEER_NATIVE_MARKER="+marker)
				output, err := command.CombinedOutput()
				if err != nil {
					t.Fatalf("%s %s: %v\n%s", peer, flag, err, output)
				}
				want := peer + " v0.5.2 (" + revision + ")\n"
				if string(output) != want {
					t.Fatalf("output=%q want=%q", output, want)
				}
				if _, err := os.Stat(marker); !os.IsNotExist(err) {
					t.Fatalf("native product was invoked: %v", err)
				}
			})
		}
		t.Run(peer+"/lane-native-version", func(t *testing.T) {
			marker := filepath.Join(root, peer+"-lane-native")
			command := exec.Command(filepath.Join(bin, peer), "--native-version")
			command.Env = append(os.Environ(), "PATH="+native, "PEER_NATIVE_MARKER="+marker, "SESSIONBUS_LAUNCH_TOKEN=fixture")
			if output, err := command.CombinedOutput(); err == nil {
				t.Fatalf("lane native escape succeeded: %s", output)
			}
			if _, err := os.Stat(marker); !os.IsNotExist(err) {
				t.Fatalf("lane native product was invoked: %v", err)
			}
		})
	}
}
