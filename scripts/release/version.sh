#!/bin/sh
# SPDX-License-Identifier: MIT

# Sourced by archive builders. Call peer_version_init from the repository root
# before peer_go_build.
peer_version_init() {
	peer_version_root=$1
	peer_base_version=$(cat "$peer_version_root/RELEASE_VERSION")
	if ! printf '%s\n' "$peer_base_version" | grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+$'; then
		echo "Invalid RELEASE_VERSION: $peer_base_version" >&2
		return 1
	fi
	peer_release=${SESSIONBUS_PEERS_RELEASE:-development}
	if [ "$peer_release" != development ] && [ "$peer_release" != "v$peer_base_version" ]; then
		echo "Stable peer release $peer_release does not match RELEASE_VERSION v$peer_base_version" >&2
		return 1
	fi
	peer_revision=${SESSIONBUS_PEERS_REVISION:-$(git -C "$peer_version_root" rev-parse HEAD)}
	case "$peer_revision" in
		*[!0-9a-f]*) echo "Invalid peer revision: $peer_revision" >&2; return 1 ;;
	esac
	if [ "${#peer_revision}" -ne 40 ]; then
		echo "Invalid peer revision length: $peer_revision" >&2
		return 1
	fi
}

peer_go_build() {
	peer_build_output=$1
	peer_build_package=$2
	go build -trimpath \
		-ldflags="-s -w -X github.com/sessionbus/peer-common/peerversion.Release=$peer_release -X github.com/sessionbus/peer-common/peerversion.Revision=$peer_revision" \
		-o "$peer_build_output" "$peer_build_package"
}
