# Provisa Deployment Plan

## Overview

Provisa ships as three distinct packages per platform. (REQ-630) The split is driven by
GitHub Actions' 2 GB artifact limit — container image tarballs alone exceed it. (REQ-630)
The three packages map directly to the three docker-compose layers: (REQ-630)

| Package | Services | docker-compose file |
|---------|----------|---------------------|
| **Core** | postgres, pgbouncer, redis, trino, zaychik + Python backend + UI | `docker-compose.core.yml` |
| **Observability (Obs)** | minio, otlp2parquet, otel-collector, prometheus, tempo, grafana | `docker-compose.observability.yml` |
| **Demo** | petstore-mock, graphql-demo | `docker-compose.demo.yml` |

**Dependency chain**: Core ← Obs ← Demo (demo requires obs; obs requires core). (REQ-631)

---

## Platform Matrix

| Package | macOS | Windows | Linux |
|---------|-------|---------|-------|
| Core | DMG (Lima + nerdctl) | NSIS .exe (VirtualBox OVA) | AppImage (rootless Docker) |
| Obs | DMG (image load into Lima) | NSIS .exe (image load into VirtualBox VM) | **bundled into Core AppImage** |
| Demo | DMG (image load into Lima) | NSIS .exe (image load into VirtualBox VM) | **not included** |

Linux rationale: Linux users are typically server/technical installs. (REQ-227) OTel
observability is useful in production; petstore/graphql demo services are not.
One self-contained AppImage is simpler to distribute. (REQ-632)

---

## Extension Model (macOS + Windows)

Core is the only installer that creates the VM runtime (Lima / VirtualBox). (REQ-633)
Obs and Demo are **extension packages** — they load images into the existing VM
and drop a compose file into a well-known extensions directory. (REQ-633) Core's launcher
detects installed extensions at startup and composes the service set dynamically. (REQ-633)

### Extension directory

| Platform | Path |
|----------|------|
| macOS | `~/.provisa/extensions/` |
| Windows | `%USERPROFILE%\.provisa\extensions\` |

Each extension drops:
```
extensions/
  observability/
    docker-compose.observability.yml
    (obs-specific configs already in core's ~/.provisa/observability/)
  demo/
    docker-compose.demo.yml
    (demo server source already in core's ~/.provisa/demo/)
```

### Compose file assembly at launch

The launcher builds the compose file list dynamically: (REQ-633)
```
core:  docker-compose.core.yml + docker-compose.app.yml + docker-compose.airgap.yml
+ obs:  + extensions/observability/docker-compose.observability.yml
+ demo: + extensions/demo/docker-compose.demo.yml
```

When an extension is installed, the launcher restarts all services together with
the expanded file list. (REQ-633) Trino picks up the OTel `JAVA_TOOL_OPTIONS` override from
`docker-compose.observability.yml` on that restart. (REQ-633)

---

## macOS Packages

### Core DMG (`Provisa-<version>.dmg`)

**Current state**: `packaging/macos/build-dmg.sh` — fully implemented, builds
everything into one DMG.

**Target state**: Core only.

**Contents of DMG**:
- `Provisa.app` — signed + notarized SwiftUI launcher (ProvisaLauncher) (REQ-227)
- `images/` — core image tarballs (hidden from Finder):
  - `python-3.12-slim.tar.gz`
  - `postgres-16.tar.gz`
  - `pgbouncer-latest.tar.gz`
  - `redis-7-alpine.tar.gz`
  - `trino-480.tar.gz`
  - `zaychik-local.tar.gz`
- `nerdctl/` — `nerdctl-full-2.2.2-linux-arm64.tar.gz` (hidden) (REQ-228)
- `vm-image/` — `provisa-vm.img` Ubuntu 24.04 arm64 (hidden) (REQ-228)

**`Provisa.app/Contents/Resources/` embeds**: (REQ-294)
- `docker-compose.core.yml`, `docker-compose.app.yml`, `docker-compose.airgap.yml`
- `config/`, `db/`, `trino/`, `observability/` (trino-otel dir + OTel Java agent jar)
- `provisa-source/` (Dockerfile, main.py, pyproject.toml, provisa/, static UI, wheels)

**`first-launch.sh` changes needed**:
- Copy `observability/` configs but NOT start obs services (no obs images yet)
- Copy `demo/` source but NOT start demo services

### Obs DMG (`Provisa-Obs-<version>.dmg`)

**New package**.

**Contents**:
- `install-obs.sh` — installer script (no `.app`, just a shell script run via
  a minimal DMG or a signed pkg)
- `images/` (hidden):
  - `minio-latest.tar.gz`
  - `otlp2parquet-latest.tar.gz`
  - `otel-collector-contrib-0.99.0.tar.gz`
  - `prometheus-v2.51.2.tar.gz`
  - `tempo-2.4.1.tar.gz`
  - `grafana-10.4.2.tar.gz`

**`install-obs.sh` steps**:
1. Check Lima VM `provisa` exists (core must be installed). (REQ-633)
2. Start Lima VM if not running. (REQ-228)
3. `limactl shell provisa sudo ctr images import` for each image tarball. (REQ-294)
4. Write `~/.provisa/extensions/observability/docker-compose.observability.yml`. (REQ-633)
5. Print: "Observability installed. Restart Provisa to activate."

**Build script**: `packaging/macos/build-dmg-obs.sh`
- Pulls + saves obs images (`--platform linux/arm64`, gzip compressed) (REQ-294)
- Embeds `install-obs.sh` + images into a minimal DMG
- Signs + notarizes `install-obs.sh` (REQ-227)

### Demo DMG (`Provisa-Demo-<version>.dmg`)

**New package**. Requires Obs to be installed. (REQ-631)

**Contents**:
- `install-demo.sh`
- `images/` (hidden):
  - `petstore3-unstable.tar.gz`
  - `graphql-demo-local.tar.gz`

**`install-demo.sh` steps**:
1. Check `~/.provisa/extensions/observability/` exists (obs must be installed). (REQ-631)
2. Start Lima VM if not running. (REQ-228)
3. Import demo image tarballs into Lima. (REQ-294)
4. Write `~/.provisa/extensions/demo/docker-compose.demo.yml`. (REQ-633)
5. Print: "Demo installed. Restart Provisa to activate."

**Build script**: `packaging/macos/build-dmg-demo.sh`

### ProvisaLauncher changes (`ServiceStatus.swift` / `ScriptRunner.swift`)

The launcher's `provisa start` path needs to:
1. Enumerate `~/.provisa/extensions/*/docker-compose.*.yml` at startup. (REQ-633)
2. Append each found file to the compose file list. (REQ-633)
3. Set `PROVISA_REDIRECT_ENABLED`, MinIO, and OTel env vars only when obs
   extension is present. (REQ-633)

---

## Windows Packages

Container runtime: VirtualBox OVA (not Lima). (REQ-633) Images are loaded into the OVA's
Docker daemon by `first-launch.ps1` post-VM-boot. (REQ-228)

### Core Installer (`Provisa-Setup-<version>.exe`)

**Current state**: `packaging/windows/build-installer.ps1` + `installer.nsi` —
builds one installer. Needs obs + demo images removed.

**Target images** (same set as macOS core, minus obs/demo).

### Obs Installer (`Provisa-Obs-Setup-<version>.exe`)

**New package**.

**`install-obs.ps1` steps**:
1. Check VM `Provisa` exists and is running. (REQ-633)
2. `docker load` each obs image tarball inside the VM via `VBoxManage guestcontrol`. (REQ-633)
3. Write `%USERPROFILE%\.provisa\extensions\observability\docker-compose.observability.yml`. (REQ-633)
4. Prompt user to restart Provisa.

**Build script**: `packaging/windows/build-installer-obs.ps1`

### Demo Installer (`Provisa-Demo-Setup-<version>.exe`)

**New package**. Requires Obs installer. (REQ-631)

Same pattern as obs — loads demo images, writes extension compose file. (REQ-633)

**Build script**: `packaging/windows/build-installer-demo.ps1`

### `provisa.ps1` changes

Same extension detection as ProvisaLauncher: enumerate
`$env:USERPROFILE\.provisa\extensions\*/docker-compose.*.yml` and append to
compose file list. (REQ-633)

---

## Linux AppImage

**Current state**: `packaging/linux/build-appimage.sh` — bundles core images
only (postgres, pgbouncer, minio, redis, trino). Minio is currently in this
list but should move to obs.

**Target state**: Bundle core + obs images. No demo. (REQ-632)

### `save_images()` target list

```bash
# Core
"postgres:16"
"edoburu/pgbouncer:latest"
"redis:7-alpine"
"trinodb/trino:480"
"provisa/zaychik:local"   # built from source

# Obs (bundled directly — no separate download on Linux)
"minio/minio:latest"
"ghcr.io/smithclay/otlp2parquet:latest"
"otel/opentelemetry-collector-contrib:0.99.0"
"prom/prometheus:v2.51.2"
"grafana/tempo:2.4.1"
"grafana/grafana:10.4.2"
```

### `build_appdir()` changes

- Copy `docker-compose.core.yml` + `docker-compose.observability.yml` into
  `${APPDIR}/compose/`
- `AppRun` / `first-launch.sh` always starts core + obs (no flag needed) (REQ-632)
- Remove demo compose file from bundle entirely (REQ-632)

### `first-launch.sh` (Linux) changes

Start command becomes:
```bash
docker compose \
  -f compose/docker-compose.core.yml \
  -f compose/docker-compose.observability.yml \
  -f compose/docker-compose.app.yml \
  -f compose/docker-compose.airgap.yml \
  up -d
```

---

## CI / GitHub Actions

Three parallel build jobs per platform. (REQ-630) Each uploads its artifact separately,
staying under the 2 GB GitHub artifact limit. (REQ-630)

```yaml
jobs:
  build-macos-core:
    outputs: Provisa-<version>.dmg

  build-macos-obs:
    outputs: Provisa-Obs-<version>.dmg

  build-macos-demo:
    outputs: Provisa-Demo-<version>.dmg

  build-windows-core:
    outputs: Provisa-Setup-<version>.exe

  build-windows-obs:
    outputs: Provisa-Obs-Setup-<version>.exe

  build-windows-demo:
    outputs: Provisa-Demo-Setup-<version>.exe

  build-linux:
    outputs: Provisa-<version>.AppImage   # core + obs, no demo
```

All jobs are independent and run in parallel. (REQ-630) Demo jobs have a logical
dependency on obs (checked at install time by the installer script, not
enforced by CI). (REQ-631)

---

## Dev Environment

The dev environment mirrors the packaged product's compose layers but with the
Python backend and UI running on the **host** (uvicorn + vite), not in
containers. (REQ-634) This means `docker-compose.app.yml` is **never used in dev** — it
would bind ports 8000 and 3000 to containerized services, conflicting with the
local processes. (REQ-634)

### Compose stacks

| Mode | Compose files used |
|------|--------------------|
| Core only | `core.yml` + `dev-install.yml` |
| Core + Obs | `core.yml` + `dev-install.yml` + `observability.yml` |
| Core + Obs + Demo | `core.yml` + `dev-install.yml` + `observability.yml` + `demo.yml` |

`docker-compose.app.yml` and `docker-compose.airgap.yml` are **packaged-product
only** — never included in dev. (REQ-634)

### Port map

All service ports are exposed to the host by `dev-install.yml` (core services)
or `observability.yml` (obs services). The local backend connects to everything
via `localhost`. (REQ-634)

| Port | Service | Who binds it |
|------|---------|--------------|
| 5432 | postgres | `dev-install.yml` |
| 6432 | pgbouncer | `dev-install.yml` |
| 6379 | redis | `dev-install.yml` |
| 8080 | trino | `dev-install.yml` |
| 8480 | zaychik (Flight) | `dev-install.yml` |
| 9000 | minio S3 | `observability.yml` |
| 9001 | minio console | `observability.yml` |
| 4317 | otel-collector gRPC | `observability.yml` |
| 4318 | otel-collector HTTP | `observability.yml` |
| 4319 | otlp2parquet HTTP | `observability.yml` |
| 9090 | prometheus | `observability.yml` |
| 3100 | grafana | `observability.yml` |
| 18080 | petstore-mock | `demo.yml` |
| 4000 | graphql-demo | `demo.yml` |
| **8000** | **Python backend (uvicorn)** | **host process — never containerised in dev** |
| **3000** | **UI (vite dev server)** | **host process — never containerised in dev** |

Ports 8000 and 3000 must never appear in any dev compose file. (REQ-634) Any future
compose overlay that adds a service binding those ports would silently break the
dev environment.

### Backend OTel endpoint in dev

`docker-compose.app.yml` points the containerised backend to
`http://otel-collector:4317` (Docker-internal gRPC). The local backend cannot
reach that hostname. (REQ-634)

When obs is active in dev, `start-ui-install.sh` sets: (REQ-330)
```bash
OTEL_EXPORTER_OTLP_ENDPOINT="http://localhost:4319"   # OTLP HTTP to otlp2parquet, host-exposed
OTEL_SERVICE_NAME="provisa"
```

When obs is not active, these vars are unset (spans are dropped). (REQ-330)

### `start-ui-install.sh` modes

```
./start-ui-install.sh              # core only
./start-ui-install.sh --demo       # core + obs + demo  (--demo always implies obs)
```

There is intentionally no `--obs` flag without demo — in dev, running obs
without demo data produces an empty Grafana/Tempo dashboard, which is not
useful. (REQ-634) The flag may be added later if needed.

---

## Implementation Order

1. **`docker-compose.observability.yml`** — make self-contained (done)
2. **`docker-compose.dev-install.yml`** — remove minio ports (done)
3. **`start-ui-install.sh`** — dynamic compose assembly, demo-conditional env vars (done)
3a. **`start-ui-install.sh`** — add `OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4319` + `OTEL_SERVICE_NAME` to backend env when demo active
4. **`build-dmg.sh`** — strip obs + demo images; obs/demo configs stay in Resources
5. **`build-dmg-obs.sh`** — new script: pull obs images, build obs DMG
6. **`build-dmg-demo.sh`** — new script: pull demo images, build demo DMG
7. **ProvisaLauncher** — extension detection in `ServiceStatus.swift` / compose assembly
8. **`first-launch.sh` (macOS)** — copy obs/demo configs but don't start services
9. **`build-installer.ps1`** — strip obs + demo images
10. **`build-installer-obs.ps1`** — new script: Windows obs installer
11. **`build-installer-demo.ps1`** — new script: Windows demo installer
12. **`provisa.ps1`** — extension detection
13. **`build-appimage.sh`** — add obs images, always-on obs compose, remove demo
14. **CI workflow** — split into parallel jobs
