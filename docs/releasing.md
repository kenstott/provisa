# Releasing

Releases are triggered by pushing a git tag. The tag name determines the channel.

## Tag Conventions

| Tag format | Channel | GitHub Release type |
|-----------|---------|-------------------|
| `v1.2.3-alpha.1` | alpha | Pre-release |
| `v1.2.3-beta.1` | beta | Pre-release |
| `v1.2.3-rc.1` | rc | Pre-release |
| `v1.2.3` | stable | Latest release |

## Creating a Release

```bash
# Alpha
git tag v1.2.3-alpha.1 && git push origin v1.2.3-alpha.1

# Beta
git tag v1.2.3-beta.1 && git push origin v1.2.3-beta.1

# Release candidate
git tag v1.2.3-rc.1 && git push origin v1.2.3-rc.1

# Stable
git tag v1.2.3 && git push origin v1.2.3
```

The CI workflow (`build-dmg.yml`, named "Build Provisa Packages") triggers on any `v*` tag and runs these jobs, most in parallel:

1. **Resolve release metadata** — detects channel from tag suffix, derives PEP 440 version and asset names
2. **Download / package Trino plugins** — pulls Calcite Trino connectors and packages a tarball
3. **Pull core / obs / demo Docker images** — saves service image tarballs (arm64, plus amd64 core for the Windows container tier)
4. **Build macOS Core / Obs / Demo DMGs** — run on `macos-14` (Apple Silicon), airgapped
5. **Build Linux AppImage** — core, airgapped
6. **Build Windows Core installer** — native, embedded Python, no Docker
7. **Build Windows Container-tier installer** — WSL2 + Trino, fetches images on demand (no VirtualBox/OVA)
8. **Build JDBC driver** — Maven shaded JAR
9. **Build and test Python client**, then **Publish to PyPI**
10. **Publish GitHub Release** — uploads all assets, sets pre-release flag for alpha/beta/rc

## Release Assets

Each release publishes the following assets, all attached to the GitHub Release (the wheel also goes to PyPI):

| Asset | Platform / Use |
|-------|----------------|
| `Provisa-<tag>-macOS.dmg` | macOS Core (Apple Silicon, airgapped) |
| `Provisa-Runtime-<tag>-macOS.dmg` | macOS native Python runtime (mount alongside Core) |
| `Provisa-Obs-<tag>-macOS.dmg` | macOS Observability extension |
| `Provisa-Demo-<tag>-macOS.dmg` | macOS Demo extension (requires Obs) |
| `Provisa-<tag>-linux-x86_64.AppImage` | Linux x86_64 core (airgapped) |
| `Provisa-<tag>-windows-x64.exe` | Windows x64 native installer (embedded Python, no Docker) |
| `Provisa-Container-<tag>-windows-x64.exe` | Windows x64 container-tier upgrade (WSL2 + Trino) |
| `provisa-jdbc-<tag>.jar` | JDBC driver — Tableau, PowerBI, DBeaver |
| `provisa_client-<pep440>-py3-none-any.whl` | Python client (also PyPI) |
| `provisa-core-images-<tag>.tar.gz` | Core Services image tarballs (arm64, airgapped) |
| `provisa-core-images-amd64-<tag>.zip` | Core Services images (amd64, Windows container tier / airgap) |
| `provisa-obs-images-<tag>.tar.gz` | Observability Stack images (optional) |
| `provisa-demo-images-<tag>.tar.gz` | Demo Data Pack images (optional) |
| `provisa-trino-plugins-<tag>.tar.gz` | Coordination Engine connectors (SharePoint, Splunk, File) |

The Python client version is automatically converted to PEP 440 format:
`v0.1.0-alpha.1` → `0.1.0a1`, `v0.1.0-beta.1` → `0.1.0b1`, `v0.1.0-rc.1` → `0.1.0rc1`.

## PyPI Publishing Setup (one-time)

1. Copy your API token from `~/.pypirc` (the `pypi-...` value for `pypi.org`)
2. Add it as a repository secret named `PYPI_API_TOKEN` under **Settings → Secrets → Actions**

The `publish-pypi` job will then publish automatically on every tag.

## Required Repository Secrets

Configure these under **Settings → Secrets → Actions**:

| Secret | Required for | Description |
|--------|-------------|-------------|
| `PYPI_API_TOKEN` | PyPI publishing | API token from `~/.pypirc` (starts with `pypi-`) |
| `APPLE_CERT_P12_BASE64` | Signed builds | Base64-encoded `.p12` certificate file (see below) |
| `APPLE_CERT_P12_PASSWORD` | Signed builds | Password set when exporting the `.p12` from Keychain Access |
| `APPLE_DEVELOPER_ID` | Signed builds | Full cert name: `Developer ID Application: Your Name (TEAMID)` |
| `APPLE_NOTARYTOOL_APPLE_ID` | Notarized builds | Apple ID email |
| `APPLE_NOTARYTOOL_PASSWORD` | Notarized builds | App-specific password from appleid.apple.com (not your login password) |
| `APPLE_NOTARYTOOL_TEAM_ID` | Notarized builds | 10-character Apple Team ID |

Builds without these secrets succeed but produce an unsigned/unnotarized DMG (users will see a Gatekeeper warning).

## Exporting the .p12 Certificate

1. Open **Keychain Access** → login keychain → **My Certificates**
2. Find **Developer ID Application: Your Name (TEAMID)** — expand it to confirm the private key is nested underneath
3. Select both the certificate and its private key → right-click → **Export 2 Items** → save as `.p12` → set a strong password
4. Base64-encode and copy to clipboard:
   ```bash
   base64 -i YourCert.p12 | pbcopy
   ```
5. Paste as the value of `APPLE_CERT_P12_BASE64`; set `APPLE_CERT_P12_PASSWORD` to the password from step 3

## Finding Your Certificate Name

```bash
security find-identity -v -p codesigning | grep "Developer ID Application"
```

Copy the full string in quotes — that's the value for `APPLE_DEVELOPER_ID`.

## Deleting a Bad Tag

```bash
git tag -d v1.2.3-alpha.1
git push origin :refs/tags/v1.2.3-alpha.1
```

Then delete the corresponding GitHub Release in the UI before re-tagging.
