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

The CI workflow (`build-dmg.yml`) triggers on any `v*` tag and:
1. Detects the channel from the tag suffix
2. Builds the airgapped macOS DMG on `macos-14` (arm64)
3. Creates a GitHub Release — pre-release flag set for alpha/beta/rc, latest for stable
4. Uploads `Provisa-<tag>-macOS.dmg` as the release asset
5. Auto-generates release notes from commits since the previous tag

## Release Assets

Each release publishes three assets:

| Asset | Where |
|-------|-------|
| `Provisa-<tag>-macOS.dmg` | GitHub Release |
| `provisa-jdbc-<tag>.jar` | GitHub Release |
| `provisa_client-<pep440>-py3-none-any.whl` | GitHub Release + PyPI |

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
| `APPLE_DEVELOPER_ID` | Signed builds | Full cert name: `Developer ID Application: Your Name (TEAMID)` |
| `APPLE_NOTARYTOOL_APPLE_ID` | Notarized builds | Apple ID email |
| `APPLE_NOTARYTOOL_PASSWORD` | Notarized builds | App-specific password |
| `APPLE_NOTARYTOOL_TEAM_ID` | Notarized builds | 10-character Apple Team ID |

Builds without these secrets succeed but produce an unsigned/unnotarized DMG (users will see a Gatekeeper warning).

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
