# Releasing

Releases are triggered by pushing a git tag. The tag name determines the channel. For how a change reaches a release, and which stages are in use today, see [Release process](release-process.md).

## Tag Conventions

| Tag format | Channel | GitHub Release type |
| ----------- | --------- | ------------------- |
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

1. **Resolve release metadata.** Detects channel from tag suffix, derives PEP 440 version and asset names.
2. **Download / package Trino plugins.** Pulls Calcite Trino connectors and packages a tarball.
3. **Pull core / obs / demo Docker images.** Saves service image tarballs (arm64, plus amd64 core for the Windows container tier)
4. **Build macOS Core / Obs / Demo DMGs.** Run on `macos-15` (Apple Silicon), airgapped.
5. **Build Linux AppImage.** Core, airgapped.
6. **Build Windows Core installer.** Native, embedded Python, no Docker.
7. **Build Windows Container-tier installer.** WSL2 + Trino, fetches images on demand (no VirtualBox/OVA)
8. **Build JDBC driver.** Maven shaded JAR.
9. **Build and test Python client**, then **Publish to PyPI**
10. **Publish GitHub Release.** Checks that the draft release carries every asset, then undrafts it and sets the pre-release flag for beta/rc.

## How Assets Reach the Release

Each job that builds a release asset attaches it to the draft release itself, right after building it, with the `attach-release-assets` action (`.github/actions/attach-release-assets`). The action creates the draft for the tag when it does not exist yet (through `ensure-release`, which never undrafts) and runs `gh release upload <tag> <files> --clobber`. Every file is uploaded once. GitHub has no server-side copy from a workflow artifact to a release asset, and the earlier design, which downloaded every artifact in the publish job and uploaded it again, took more than 30 minutes for v0.1.0-alpha.510.

The attach step runs only on a real release: its `if: inputs.publish` is true only when the run was started on the tag it names with `publish` checked. A dry run attaches nothing. It uploads each asset to the run as a workflow artifact instead, and that upload fails when the file is missing, so a green dry run shows every asset was built. On a release, a job keeps a workflow artifact only when another job of the same run reads it: the PyPI jobs read the client and notebook wheels, and the installers read the UI build, the image tarballs and the Trino plugins.

The publish job ("Release: attach and publish", `release-publish.yml`) downloads nothing and attaches nothing. It reads the draft's asset list and refuses to undraft if any expected installer, image tarball, JAR or wheel (one `provisa` wheel per platform, the client and the notebook) is missing. Then it undrafts the release and regenerates the wheel index on GitHub Pages. The JDBC JAR's Sigstore attestation is made in the job that builds the JAR.

To publish a release whose publish job did not run, dispatch "Release: attach and publish" by hand with the release's `version` and `publish` checked. The assets are already on the draft, so the dispatch verifies them, undrafts the release, writes the index and pushes the engine and Flight proxy images.

## Release Assets

Each release publishes the following assets, all attached to the GitHub Release (the wheel also goes to PyPI):

| Asset | Platform / Use |
| ------- | ---------------- |
| `Provisa-<tag>-macOS.dmg` | macOS Core (Apple Silicon, airgapped) |
| `Provisa-Runtime-<tag>-macOS.dmg` | macOS native Python runtime (mount alongside Core) |
| `Provisa-Obs-<tag>-macOS.dmg` | macOS Observability extension |
| `Provisa-Demo-<tag>-macOS.dmg` | macOS Demo extension (requires Obs) |
| `Provisa-<tag>-linux-x86_64.AppImage` | Linux x86_64 core (airgapped) |
| `Provisa-<tag>-windows-x64.exe` | Windows x64 native installer (embedded Python, no Docker) |
| `Provisa-Container-<tag>-windows-x64.exe` | Windows x64 container-tier upgrade (WSL2 + Trino) |
| `provisa-jdbc-<tag>.jar` | JDBC driver for Tableau, PowerBI and DBeaver |
| `provisa_client-<pep440>-py3-none-any.whl` | Python client (also PyPI) |
| `provisa-core-images-<tag>.tar.gz` | Core Services image tarballs (arm64, airgapped) |
| `provisa-core-images-amd64-<tag>.zip` | Core Services images (amd64, Windows container tier / airgap) |
| `provisa-obs-images-<tag>.tar.gz` | Observability Stack images (optional) |
| `provisa-demo-images-<tag>.tar.gz` | Demo Data Pack images (optional) |
| `provisa-trino-plugins-<tag>.tar.gz` | Coordination Engine connectors (SharePoint, Splunk, File) |

The Python client version is automatically converted to PEP 440 format:
`v0.1.0-alpha.1` → `0.1.0a1`, `v0.1.0-beta.1` → `0.1.0b1`, `v0.1.0-rc.1` → `0.1.0rc1`.

## PyPI Publishing Setup (one-time)

The `publish-pypi` (`provisa-client`) and `publish-provisa-pypi` (`provisa`) jobs in `.github/workflows/build-dmg.yml` publish with PyPI trusted publishing. The job's GitHub OIDC token (`id-token: write`) authenticates it, and no API token is read. Register this repository and `build-dmg.yml` as a trusted publisher for each project on pypi.org (**Your projects → Manage → Publishing**). The publishes then run automatically on every tag.

## Signing and Supply Chain

What each release artifact carries, as the release workflows build it:

| Artifact | Signature |
| -------- | --------- |
| `provisa-client` and `provisa-notebook` on PyPI | Sigstore-signed PEP 740 attestation per file (`attestations: true`), verifiable on pypi.org |
| `provisa` wheels, attached to the GitHub release | Not signed; the index at `https://kenstott.github.io/provisa/simple/` carries each wheel's sha256, which pip checks |
| `provisa-pg-ext`, `provisa-duckdb-ext` on PyPI | Sigstore-signed PEP 740 attestation per file |
| JDBC driver `.jar` | Sigstore build-provenance attestation (`actions/attest-build-provenance`) |
| macOS `.dmg` installers | Developer ID code signature and Apple notarization |
| Windows `.exe` installers | Not signed |
| Linux `.AppImage` | Not signed |

The `provisa` server wheel left PyPI at v0.1.0-alpha.510, when the project reached PyPI's 10 GB size limit. Each platform's wheel job attaches its wheel to the release, and the "Release: attach and publish" workflow rebuilds a PEP 503 index of every published release's wheels on GitHub Pages; install with `pip install provisa --extra-index-url https://kenstott.github.io/provisa/simple/`. No signed install of the server exists on Linux or Windows now. The Windows build signs with Authenticode when a code-signing certificate is configured (`WINDOWS_CERT_PFX_BASE64`, `WINDOWS_CERT_PFX_PASSWORD`, `WINDOWS_CERT_TIMESTAMP_URL`, read by `packaging/windows/build-sfx.ps1`); none is configured, so it ships unsigned.

Dependencies and CVE gates:

- Python dependencies are locked with hashes in `uv.lock`; CI installs them with `uv sync --frozen`.
- The `CVE Audit` workflow (`.github/workflows/cve-audit.yml`) runs `pip-audit` over the locked set on every push to `main`, on every pull request and weekly.
- At release, the `provisa` and `provisa-client` builds each produce a CycloneDX SBOM and run `pip-audit` over what ships. The JDBC driver is scanned with `grype --fail-on medium`. A finding fails the job, and the release publishes nothing, because the PyPI uploads and the GitHub release both wait on these jobs. The SBOMs and CVE reports are attached to each release.
- The desktop installers build the `provisa` wheel from source on the release runner and resolve its dependencies with `pip` at build time, not from `uv.lock`; the release CVE gate scans the locked set, not the installers' bundled dependencies.
- `packaging/pypi/requirements-embedded.lock` is a hash-pinned resolution of `provisa[embedded]` for airgap mirrors (see `packaging/pypi/MIRROR-CHECKLIST.md`). It is regenerated by hand, not per release.

## Required Repository Secrets

Configure these under **Settings → Secrets → Actions**:

| Secret | Required for | Description |
| -------- | ------------- | ------------- |
| `APPLE_CERT_P12_BASE64` | Signed builds | Base64-encoded `.p12` certificate file (see below) |
| `APPLE_CERT_P12_PASSWORD` | Signed builds | Password set when exporting the `.p12` from Keychain Access |
| `APPLE_DEVELOPER_ID` | Signed builds | Full cert name: `Developer ID Application: Your Name (TEAMID)` |
| `APPLE_NOTARYTOOL_APPLE_ID` | Notarized builds | Apple ID email |
| `APPLE_NOTARYTOOL_PASSWORD` | Notarized builds | App-specific password from appleid.apple.com (not your login password) |
| `APPLE_NOTARYTOOL_TEAM_ID` | Notarized builds | 10-character Apple Team ID |

Builds without these secrets succeed but produce an unsigned/unnotarized DMG (users will see a Gatekeeper warning).

## Exporting the .p12 Certificate

1. Open **Keychain Access** → login keychain → **My Certificates**
2. Find **Developer ID Application: Your Name (TEAMID)** and expand it to confirm the private key is nested underneath
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

Copy the full string in quotes. That string is the value for `APPLE_DEVELOPER_ID`.

## Deleting a Bad Tag

```bash
git tag -d v1.2.3-alpha.1
git push origin :refs/tags/v1.2.3-alpha.1
```

Then delete the corresponding GitHub Release in the UI before re-tagging.
