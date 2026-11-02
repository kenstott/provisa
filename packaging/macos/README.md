# Provisa macOS DMG — Phase AF2a

macOS installer with two tiers and no VM:

- **Native tier (default, `Provisa.dmg`):** a python-build-standalone interpreter with
  provisa already installed. First launch copies it to `~/.provisa/venv`. No Docker needed.
- **Docker tier (`Provisa-Container.dmg`, `PROVISA_EDITION=container`):** runs
  `docker compose` on the user's own Docker (Docker Desktop or colima). Images are
  `docker load`-ed from the core-images tarball and resolved through `docker-compose.airgap.yml`.

## What's inside the DMG

```
Provisa.app/
  Contents/
    Info.plist                   macOS app metadata
    MacOS/
      ProvisaLauncher            Native Swift launcher (handles first-launch)
      first-launch.sh            Tier setup (native venv or Docker image load)
    Resources/
      provisa-cli                The provisa start/stop/status/... CLI
      VERSION                    Release version stamped into the installed tier
      docker-compose.*.yml       Container edition only: core, app, airgap, trino TLS
      config/ db/ trino/         Container edition only
      observability/             Container edition only
runtime/                         Hidden DMG content: prebuilt interpreter (core edition)
```

## Building

### Prerequisites (build host only)

- macOS with Docker (or Colima/OrbStack)
- `hdiutil` (built into macOS)
- `codesign` + `xcrun` (Xcode Command Line Tools)
- `curl`, `tar`

### Local build (no signing)

```bash
packaging/macos/build-dmg.sh
# Output: packaging/macos/dist/Provisa.dmg
```

Signing and notarization are skipped automatically when the env vars below are absent.

### Signed + notarized build (CI / release)

Set these env vars before running `build-dmg.sh`:

| Variable | Description |
|---|---|
| `APPLE_DEVELOPER_ID` | Developer ID Application cert name, e.g. `Developer ID Application: Acme Corp (TEAMID)` |
| `APPLE_NOTARYTOOL_APPLE_ID` | Apple ID used for notarization |
| `APPLE_NOTARYTOOL_PASSWORD` | App-specific password for the Apple ID |
| `APPLE_NOTARYTOOL_TEAM_ID` | 10-character Apple Team ID |

### Pinning image digests (recommended for airgap production builds)

Edit `build-dmg.sh` `IMAGES` array to use `image@sha256:...` instead of tags for reproducible, tamper-proof bundles.

## First-launch flow

1. User drags `Provisa.app` → `/Applications`, double-clicks.
2. `provisa-launcher` detects `~/.provisa/.first-launch-complete` is absent.
3. `first-launch.sh` runs:
   - Native tier: copies the bundled runtime to `~/.provisa/venv`
   - Docker tier: checks Docker is running, stages compose files to `~/.provisa/compose/`,
     acquires the core-images tarball and loads it with `docker load`
   - Writes `~/.provisa/config.yaml` (`runtime: native` or `runtime: docker`)
   - Installs `/usr/local/bin/provisa` CLI (prompts for password via osascript if needed)
   - Writes sentinel file `~/.provisa/.first-launch-complete`
4. Subsequent launches skip setup and delegate directly to the `provisa` CLI.

## CLI commands (unchanged from standard install)

```
provisa start       Start all services
provisa stop        Stop all services
provisa restart     Restart
provisa status      Service health
provisa open        Open UI in browser
provisa logs        Tail logs
provisa upgrade     Pull latest version
provisa uninstall   Remove Provisa
```

## GitHub Actions

`.github/workflows/build-dmg.yml` triggers on `v*` tag pushes, builds on `macos-14` (arm64), and uploads the DMG as a GitHub Release asset.

Required repository secrets: `APPLE_DEVELOPER_ID`, `APPLE_NOTARYTOOL_APPLE_ID`, `APPLE_NOTARYTOOL_PASSWORD`, `APPLE_NOTARYTOOL_TEAM_ID`.
