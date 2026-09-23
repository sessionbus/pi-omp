# Sessionbus Pi and OMP peers

Connect native Pi and Oh My Pi (OMP) sessions through [Sessionbus](https://github.com/antst/sessionbus). `pi-peer` and `omp-peer` are preview integrations. Each provides an interactive peer and a managed lane while retaining its native tools, permissions, and history. They share a Go `pifamily` bridge and package their existing native extensions.

## Install

Install Sessionbus and the native product first. Use the real login home and PATH.

```sh
curl -fsSL https://raw.githubusercontent.com/sessionbus/pi-omp/main/scripts/install-pi.sh | sh
curl -fsSL https://raw.githubusercontent.com/sessionbus/pi-omp/main/scripts/install-omp.sh | sh
```

No independent release of this extraction has been published yet. Until then, these bootstrap scripts stop before installation. A reviewed archive built from source can be installed through its packaged `install` script. Downloads verify `SHA256SUMS` and the archive's Pi or OMP role before installation. The installed Go peer and maintenance path do not require Node. Pi's extension runs inside Pi's existing Node process; OMP's runs inside OMP's existing Bun process. This repository adds no Node sidecar, npm runtime dependency, or global extension registration.

The [Pi guide](pi/README.md) and [OMP guide](omp/README.md) cover native launch options and permanent installation. Source and test obligations are tracked in the [stable functionality checklist](docs/migration/FUNCTIONALITY-CHECKLIST.md) and [preservation inventory](docs/migration/PRESERVED-FILES.json). Historical scope and outstanding preview work remain in the [design](docs/designs/pi-omp-0.5.0/DESIGN.md) and [acceptance record](docs/designs/pi-omp-0.5.0/ACCEPTANCE.md). Source preservation is separate from fresh installed acceptance.

## Build and test

```sh
git clone https://github.com/sessionbus/pi-omp.git
cd pi-omp
GOWORK=off go mod download
GOWORK=off go test ./...
GOWORK=off go test -race ./...
GOWORK=off go vet ./...
node --test wrappers/pifamily/extension/bridge.test.mjs wrappers/pi/native.test.mjs wrappers/pi/extension.test.mjs wrappers/omp/extension.test.mjs
scripts/package-product pi ./dist
scripts/package-product omp ./dist
```

Building requires Go 1.24 or newer. Node is used only to execute the four existing native-extension test files; packaging copies their `.mjs` payloads directly and has no npm build step. Shared Go support is pinned to the reviewed `peer-common` module in `go.mod`/`go.sum`. Archives target Linux and macOS on amd64 and arm64. The Pi and OMP peers are preview integrations even when packaged with a stable release; publication remains held during validation.

`pi-peer --version` and `omp-peer --version` report the peer release and source revision without invoking native Pi or OMP. Native versions are recorded as test provenance, not pinned by this extraction.
