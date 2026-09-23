#!/bin/sh
# SPDX-License-Identifier: MIT
# Download a checksummed release; the archive owns the installation recipe.
set -eu
role=omp
version=${SESSIONBUS_VERSION:-latest}
case "$version" in ''|*[!A-Za-z0-9._-]*) echo 'Invalid SESSIONBUS_VERSION' >&2; exit 1;; esac
case "$(uname -s)" in Linux) platform=linux;; Darwin) platform=darwin;; *) echo 'Supported systems: Linux and macOS' >&2; exit 1;; esac
case "$(uname -m)" in x86_64|amd64) arch=amd64;; aarch64|arm64) arch=arm64;; *) echo 'Supported architectures: amd64 and arm64' >&2; exit 1;; esac
for cmd in curl tar awk mktemp; do command -v "$cmd" >/dev/null || { echo "Required command: $cmd" >&2; exit 1; }; done
asset="$role-peer-$platform-$arch.tar.gz"
if [ -n "${SESSIONBUS_DOWNLOAD_ROOT:-}" ]; then
 base=$SESSIONBUS_DOWNLOAD_ROOT
elif [ "$version" = latest ]; then
 # Resolve the redirect without fetching the release page, which may fail
 # independently of downloads. Pin both downloads to the same stable tag.
 release=$(curl -IsS --retry 2 --connect-timeout 10 --max-time 30 -o /dev/null -w '%{http_code} %{redirect_url}' https://github.com/sessionbus/pi-omp/releases/latest)
 case "$release" in
  30[12378]' https://github.com/sessionbus/pi-omp/releases/tag/'*)
   tag=${release#*https://github.com/sessionbus/pi-omp/releases/tag/}
   awk -v tag="$tag" 'BEGIN { exit(tag !~ /^v[0-9]+[.][0-9]+[.][0-9]+$/) }' || { echo "Invalid stable peer release redirect ($release)" >&2; exit 1; }
   base=https://github.com/sessionbus/pi-omp/releases/download/$tag;;
  30[12378]' https://github.com/sessionbus/pi-omp/releases')
   echo 'No stable peer release yet; installing the published development build.' >&2
   base=https://github.com/sessionbus/pi-omp/releases/download/development;;
  *) echo "Cannot determine latest peer release ($release)" >&2; exit 1;;
 esac
else
 base=https://github.com/sessionbus/pi-omp/releases/download/$version
fi
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' 0
trap 'exit 1' HUP INT TERM
curl -fLsS --retry 2 "$base/$asset" -o "$tmp/$asset"
curl -fLsS --retry 2 "$base/SHA256SUMS" -o "$tmp/SHA256SUMS"
expected=$(awk -v file="$asset" '$2 == file {print $1}' "$tmp/SHA256SUMS")
case "$expected" in ''|*[!0-9a-f]*) echo 'Missing or invalid checksum' >&2; exit 1;; esac
[ "${#expected}" -eq 64 ] || { echo 'Ambiguous checksum' >&2; exit 1; }
if command -v sha256sum >/dev/null; then actual=$(sha256sum "$tmp/$asset" | awk '{print $1}'); else actual=$(shasum -a 256 "$tmp/$asset" | awk '{print $1}'); fi
[ "$actual" = "$expected" ] || { echo 'Release checksum mismatch' >&2; exit 1; }
mkdir "$tmp/payload"
tar -tzf "$tmp/$asset" > "$tmp/members"
if awk '/^\// || /(^|\/)\.\.(\/|$)/ {bad=1} END {exit !bad}' "$tmp/members"; then echo 'Unsafe archive member' >&2; exit 1; fi
tar -xzf "$tmp/$asset" -C "$tmp/payload"
[ "$(cat "$tmp/payload/ROLE")" = "$role" ] || { echo 'Wrong release role' >&2; exit 1; }
sh "$tmp/payload/install" </dev/null
