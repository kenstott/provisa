# Provisa macOS DMG — Phase AF2a

Airgapped macOS installer that bundles Lima VM + containerd. No Docker required on the end-user machine. No outbound network at install or runtime.

## What's inside the DMG

```
Provisa.app/
  Contents/
    Info.plist                   macOS app metadata
    MacOS/
      provisa-launcher           Entry point (handles first-launch)
      first-launch.sh            VM setup + image import
      bin/
        arm64/  limactl  ctr     Native arm64 binaries
        x86_64/ limactl  ctr     Native x86_64 binaries
    Resources/
      provisa-cli                The provisa start/stop/status/... CLI
      docker-compose.yml
      docker-compose.prod.yml
      config/                    pgbouncer config, etc.
      db/                        Init SQL + mongo scripts
      trino/                     Trino catalog + config
      images/
        postgres-16.tar
        mongo-7.tar
        ... (all service images)
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
   - Stages image tarballs to `~/.provisa/images/`
   - Creates + starts a Lima VM named `provisa` (uses Virtualization.framework on Apple Silicon)
   - Imports all tarballs via `ctr images import` inside the VM
   - Writes `~/.provisa/config.yaml`
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
