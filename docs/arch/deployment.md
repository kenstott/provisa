# Provisa Deployment Plan

## Overview

Provisa ships as three distinct packages per platform. (REQ-630) The split is driven by
GitHub Actions' 2 GB artifact limit — container image tarballs alone exceed it. (REQ-630)
The three packages map directly to the three docker-compose layers: (REQ-630)

| Package | Services | docker-compose file |
|---------|----------|---------------------|
| **Core** | postgres, pgbouncer, redis, minio, trino, zaychik + Python backend + UI | `docker-compose.core.yml` |
| **Observability (Obs)** | otlp2parquet, otel-collector, prometheus, tempo, grafana | `docker-compose.observability.yml` |
| **Demo** | petstore-mock, graphql-demo | `docker-compose.demo.yml` |

**Dependency chain**: Core ← Obs ← Demo (demo requires obs; obs requires core). (REQ-631)

---

## Platform Matrix

| Package | macOS | Windows | Linux |
|---------|-------|---------|-------|
| Core | DMG (native venv, or user's own Docker) | NSIS .exe (native, or WSL2 + containerd) | AppImage (rootless Docker) |
| Obs | DMG (image load into user's Docker) | NSIS .exe (image load into WSL2 VM) | **bundled into Core AppImage** |
| Demo | DMG (image load into user's Docker) | NSIS .exe (image load into WSL2 VM) | **not included** |

Linux rationale: Linux users are typically server/technical installs. (REQ-227) OTel
observability is useful in production; petstore/graphql demo services are not.
One self-contained AppImage is simpler to distribute. (REQ-632)

---

## Extension Model (macOS + Windows)

On macOS the Core DMG's Docker tier runs on the user's own Docker (Docker
Desktop or colima); on Windows the Core installer's container tier provisions
WSL2 + containerd. (REQ-633) Obs and Demo are **extension packages** — they load
images into that runtime with `docker load` (or `nerdctl load` on Windows) and
drop a compose file into a well-known extensions directory. (REQ-633) Core's launcher
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

`packaging/macos/build-dmg.sh` builds the Core DMG (Core package only). There is
no separate Runtime DMG — the interpreter and images ship inside this DMG.

**Contents of DMG**:
- `Provisa.app` — signed + notarized SwiftUI launcher (ProvisaLauncher) (REQ-227)
- `python-base/` — a bare python-build-standalone CPython (macOS arm64), NOT
  pip-installed; first-launch builds `~/.provisa/venv` from it (native tier)
- `wheels/` — macOS arm64 wheelhouse (`provisa[embedded]` + uvicorn + mcp-proxy +
  deps) for the airgapped `pip install --no-index` path
- `images/` — service image tarballs for the Docker tier (hidden from Finder),
  `docker load`ed at first-launch:
  - `postgres-16.tar.gz`
  - `pgbouncer-latest.tar.gz`
  - `redis-7-alpine.tar.gz`
  - `minio-latest.tar.gz`
  - `trino-480.tar.gz`
  - `zaychik-local.tar.gz`
  - `provisa-local.tar.gz`, `provisa-ui-local.tar.gz` (built from source at
    DMG-build time)

**`Provisa.app/Contents/Resources/` embeds**: (REQ-294)
- `docker-compose.core.yml`, `docker-compose.app.yml`, `docker-compose.airgap.yml`
- `config/`, `db/`, `trino/`, `observability/` (trino-otel dir + OTel Java agent jar)
- `provisa-source/` (Dockerfile, main.py, pyproject.toml, provisa/, static UI, wheels)

**`first-launch.sh`** picks the tier from the deployment choices (REQ-976):
- **Native tier** (default, no Trino/Docker triggers): builds `~/.provisa/venv`
  from `python-base`, then `pip install provisa[embedded]` — from PyPI when
  online, or `--no-index --find-links` against the bundled `wheels/` when
  airgapped. Writes `runtime: native` to `~/.provisa/config.yaml`. No Docker.
- **Docker tier** (Trino engine, or obs/demo on Docker): `docker load`s the
  bundled image tarballs into the user's own Docker, writes `runtime: docker`
  and `image_source: tarball`, and runs `docker compose` (with the airgap
  overlay) on that Docker.
- Copies `observability/` configs but does not start obs services (no obs images yet)
- Copies `demo/` source but does not start demo services

### Obs DMG (`Provisa-Obs-<version>.dmg`)

**Contents**:
- `install-obs.sh` — installer script (no `.app`, just a shell script run via
  a minimal DMG or a signed pkg)
- `images/` (hidden):
  - `otlp2parquet-latest.tar.gz`
  - `otel-collector-contrib-0.99.0.tar.gz`
  - `prometheus-v2.51.2.tar.gz`
  - `tempo-2.4.1.tar.gz`
  - `grafana-10.4.2.tar.gz`

**`install-obs.sh` steps**:
1. Check core is installed (`~/.provisa/config.yaml` with `runtime: docker`). (REQ-633)
2. Check the user's Docker is running. (REQ-228)
3. `docker load` each obs image tarball. (REQ-294)
4. Write `~/.provisa/extensions/observability/docker-compose.observability.yml`. (REQ-633)
5. Print: "Observability installed. Restart Provisa to activate."

**Build script**: `packaging/macos/build-dmg-obs.sh`
- Pulls + saves obs images (`--platform linux/arm64`, gzip compressed) (REQ-294)
- Embeds `install-obs.sh` + images into a minimal DMG
- Signs + notarizes `install-obs.sh` (REQ-227)

### Demo DMG (`Provisa-Demo-<version>.dmg`)

Requires Obs to be installed. (REQ-631)

**Contents**:
- `install-demo.sh`
- `images/` (hidden):
  - `petstore3-unstable.tar.gz`
  - `graphql-demo-local.tar.gz`

**`install-demo.sh` steps**:
1. Check `~/.provisa/extensions/observability/` exists (obs must be installed). (REQ-631)
2. Check the user's Docker is running. (REQ-228)
3. `docker load` the demo image tarballs. (REQ-294)
4. Write `~/.provisa/extensions/demo/docker-compose.demo.yml`. (REQ-633)
5. Print: "Demo installed. Restart Provisa to activate."

**Build script**: `packaging/macos/build-dmg-demo.sh`

### ProvisaLauncher changes (`ServiceStatus.swift` / `ScriptRunner.swift`)

The launcher's `provisa start` path:

1. Enumerates `~/.provisa/extensions/*/docker-compose.*.yml` at startup. (REQ-633)
2. Appends each found file to the compose file list. (REQ-633)
3. Sets `PROVISA_REDIRECT_ENABLED`, MinIO, and OTel env vars only when the obs
   extension is present. (REQ-633)

---

## Windows Packages

### Core Installer — native tier (`Provisa-Setup-<version>.exe`) (REQ-979)

`packaging/windows/build-sfx.ps1` builds the Core installer with Inno Setup. It
bundles a standalone Python runtime (python-build-standalone for
x86_64-pc-windows-msvc) with the provisa wheel + uvicorn + duckdb/pg_duckdb +
aiosqlite, and stages the built UI at `<site-packages>\static`. The base
installer ships **no Docker, no VM, and no container images** — no VirtualBox,
no OVA, no Trino.

`first-launch-native.ps1` stages the runtime to `%USERPROFILE%\.provisa\runtime`
and writes config; `provisa-native.ps1` runs the two uvicorn processes (API
factory + ui_server). The macOS native tier is the parallel path — its
first-launch builds `~/.provisa/venv` via `setup_native_venv`.

### Container Tier — on-demand upgrade (`Provisa-Container-Setup-<version>.exe`) (REQ-889, REQ-633)

`packaging/windows/build-container.ps1` builds a separate installer (Inno Setup)
that adds the compute stack (Trino + services) via **WSL2 + containerd** — the
Windows equivalent of the macOS Docker tier. VirtualBox is never used. It bundles:

- the compose tree (core/app/airgap/observability/demo, config, db, trino config
  minus plugins),
- the core image tarballs (`docker-images-core-amd64` from CI),
- `nerdctl-full-<ver>-linux-amd64.tar.gz`,
- a WSL base rootfs (`rootfs.tar.gz`).

`install-container.ps1` steps:

1. Enable WSL2 (`wsl --install --no-distribution`, `--set-default-version 2`).
2. `wsl --import provisa %USERPROFILE%\.provisa\wsl\provisa rootfs.tar.gz --version 2`.
3. `wsl/provision-containerd.sh` installs nerdctl-full; `wsl/start-containerd.sh`
   starts containerd (no systemd in WSL2 by default).
4. `nerdctl load` each core image tarball.
5. Copy the compose tree to `/opt/provisa/compose` inside the distro.
6. Write config `runtime: container`, stop the native tier, start the stack.

`provisa-container.ps1` routes compose through
`wsl -d provisa -u root sh -c 'cd /opt/provisa/compose && nerdctl compose -f ... <cmd>'`,
mirroring the `RUNTIME=docker` routing in `scripts/provisa`. WSL2 forwards
`localhost` ports, so the UI/API are reachable at `http://localhost:3000`/`:8000`.
The tier is additive and reversible: switch back to the native tier with
`provisa-native.ps1`; `uninstall.ps1` unregisters the WSL distro.

### Obs Installer (`Provisa-Obs-Setup-<version>.exe`) — container tier

**`install-obs.ps1` steps**:
1. Check the container-tier runtime exists and is running. (REQ-633)
2. `docker load` each obs image tarball into the runtime. (REQ-633)
3. Write `%USERPROFILE%\.provisa\extensions\observability\docker-compose.observability.yml`. (REQ-633)
4. Prompt user to restart Provisa.

**Build script**: `packaging/windows/build-installer-obs.ps1`

### Demo Installer (`Provisa-Demo-Setup-<version>.exe`)

Requires Obs installer. (REQ-631)

Same pattern as obs — loads demo images, writes extension compose file. (REQ-633)

**Build script**: `packaging/windows/build-installer-demo.ps1`

### Container-tier CLI extension detection

The container-tier CLI uses the same extension detection as ProvisaLauncher:
enumerate `$env:USERPROFILE\.provisa\extensions\*/docker-compose.*.yml` and
append to the compose file list. (REQ-633)

---

## Linux AppImage

`packaging/linux/build-appimage.sh` bundles core images (postgres, pgbouncer,
minio, redis, trino, zaychik) plus obs images. MinIO is a core service
(REQ-561) and is bundled with the core image set. No demo. (REQ-632)

### `save_images()` target list

```bash
# Core
"postgres:16"
"edoburu/pgbouncer:latest"
"redis:7-alpine"
"minio/minio:latest"
"trinodb/trino:480"
"provisa/zaychik:local"   # built from source

# Obs (bundled directly — no separate download on Linux)
"ghcr.io/smithclay/otlp2parquet:latest"
"otel/opentelemetry-collector-contrib:0.99.0"
"prom/prometheus:v2.51.2"
"grafana/tempo:2.4.1"
"grafana/grafana:10.4.2"
```

### `build_appdir()`

- Copies `docker-compose.core.yml` + `docker-compose.observability.yml` into
  `${APPDIR}/compose/`
- `AppRun` / `first-launch.sh` always starts core + obs (no flag needed) (REQ-632)
- No demo compose file in the bundle (REQ-632)

### `first-launch.sh` (Linux)

Start command:
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
| 8480 | zaychik (Flight SQL proxy) | `dev-install.yml` |
| 9000 | minio S3 | `dev-install.yml` |
| 9001 | minio console | `dev-install.yml` |
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

## Shipped Components

The three-package split, the extension/detection model, the per-OS installers,
and the parallel CI jobs are shipped. (REQ-630, REQ-631, REQ-632, REQ-633)

- **`docker-compose.observability.yml`** is self-contained.
- **`docker-compose.dev-install.yml`** binds core service ports (including minio).
- **`start-ui-install.sh`** does dynamic compose assembly with demo-conditional env vars,
  and sets `OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4319` + `OTEL_SERVICE_NAME`
  on the backend env when demo is active.
- **`build-dmg.sh`** builds the Core DMG; obs/demo configs stay in Resources.
- **`build-dmg-obs.sh`** pulls obs images and builds the obs DMG.
- **`build-dmg-demo.sh`** pulls demo images and builds the demo DMG.
- **ProvisaLauncher** does extension detection in `ServiceStatus.swift` / compose assembly.
- **`first-launch.sh` (macOS)** copies obs/demo configs without starting their services.
- **`build-sfx.ps1`** builds the native Core installer (embedded Python, no images).
- **`build-installer-obs.ps1`** builds the Windows obs installer (container tier).
- **`build-installer-demo.ps1`** builds the Windows demo installer (container tier).
- **`provisa-native.ps1`** runs the native tier; the container-tier CLI does extension detection.
- **`build-appimage.sh`** bundles core + obs images with always-on obs compose, no demo.
- **CI workflow** runs three parallel build jobs per platform.
