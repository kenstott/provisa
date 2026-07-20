#!/usr/bin/env bash
# Phase AF2a — Build airgapped macOS DMG with Lima + containerd.
# Requires: docker (build host only), hdiutil, codesign, xcrun, python3
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
OUT_DIR="${SCRIPT_DIR}/dist"
APP_BUNDLE="${SCRIPT_DIR}/Provisa.app"
IMAGES_DIR="${SCRIPT_DIR}/images"
VM_IMAGES_DIR="${SCRIPT_DIR}/vm-images"
NERDCTL_DIR="${SCRIPT_DIR}/nerdctl"
BIN_DIR="${APP_BUNDLE}/Contents/MacOS/bin"
DMG_NAME="Provisa.dmg"
DMG_PATH="${OUT_DIR}/${DMG_NAME}"
# The native Python runtime ships in its own DMG so the core DMG stays under
# GitHub's 2 GB release-asset limit. first-launch.sh finds it via /Volumes/*/runtime.
RUNTIME_DMG_NAME="Provisa-Runtime.dmg"
RUNTIME_DMG_PATH="${OUT_DIR}/${RUNTIME_DMG_NAME}"

# Lima version
LIMA_VERSION="2.1.1"
# nerdctl-full version Lima 2.1.1 fetches (must match exactly)
NERDCTL_VERSION="2.2.2"
NERDCTL_ARCHIVE="nerdctl-full-${NERDCTL_VERSION}-linux-arm64.tar.gz"
# sha256 of the official nerdctl-full-2.2.2-linux-arm64.tar.gz
NERDCTL_DIGEST="sha256:55d68d2613b5f065021146bac21f620cde9e7fdd4bd3eff74cd324f5462e107a"

# Core service images only — obs images ship in the separate Obs DMG
IMAGES=(
  "python:3.12-slim"
  "postgres:16"
  "edoburu/pgbouncer:latest"
  "redis:7-alpine"
  "trinodb/trino:480"
)

RED='\033[0;31m'; GREEN='\033[0;32m'; CYAN='\033[0;36m'; BOLD='\033[1m'; NC='\033[0m'
info() { printf "${CYAN}[build-dmg]${NC} %s\n" "$*"; }
ok()   { printf "${GREEN}[build-dmg]${NC} %s\n" "$*"; }
err()  { printf "${RED}[build-dmg]${NC} %s\n" "$*" >&2; }

curl_retry() {
  local url="$1" out="$2"
  for attempt in 1 2 3 4 5; do
    if curl -fsSL --connect-timeout 30 --max-time 600 "$url" -o "$out"; then
      return 0
    fi
    info "Download attempt $attempt failed for $(basename "$url"), retrying in 15s..."
    sleep 15
  done
  err "Failed to download $url after 5 attempts"
  exit 1
}

# ── Prerequisites ─────────────────────────────────────────────────────────────
check_prereqs() {
  for cmd in curl hdiutil codesign python3; do
    if ! command -v "$cmd" &>/dev/null; then
      err "Required tool not found: ${cmd}"
      exit 1
    fi
  done
  # create-dmg — install via brew if absent
  if ! command -v create-dmg &>/dev/null; then
    info "Installing create-dmg..."
    brew install create-dmg --quiet
  fi
  ok "All prerequisites found."
}

# ── Generate app icon and DMG background ─────────────────────────────────────
generate_assets() {
  info "Generating icon and DMG background..."

  # Use an isolated venv to avoid PEP 668 restrictions on managed Python installs
  local venv="${SCRIPT_DIR}/.build-venv"
  if [ ! -x "${venv}/bin/python3" ]; then
    python3 -m venv "$venv"
  fi
  "${venv}/bin/pip" install pillow --quiet --upgrade

  "${venv}/bin/python3" "${SCRIPT_DIR}/generate-icon.py" "${SCRIPT_DIR}"
  "${venv}/bin/python3" "${SCRIPT_DIR}/generate-dmg-background.py" "${SCRIPT_DIR}"

  # Copy icon into app bundle
  local icns_src="${SCRIPT_DIR}/Provisa.icns"
  local icns_dst="${APP_BUNDLE}/Contents/Resources/AppIcon.icns"
  mkdir -p "$(dirname "$icns_dst")"
  cp "$icns_src" "$icns_dst"
  ok "Icon and background generated."
}

# ── Download Lima binaries (arm64 + x86_64) ──────────────────────────────────
download_lima() {
  info "Downloading Lima ${LIMA_VERSION}..."
  local base_url="https://github.com/lima-vm/lima/releases/download/v${LIMA_VERSION}"
  local arm64_tar="lima-${LIMA_VERSION}-Darwin-arm64.tar.gz"
  local x86_tar="lima-${LIMA_VERSION}-Darwin-x86_64.tar.gz"
  local tmp="${SCRIPT_DIR}/tmp-lima"
  mkdir -p "${tmp}/arm64" "${tmp}/x86_64" "${BIN_DIR}/arm64" "${BIN_DIR}/x86_64"

  curl_retry "${base_url}/${arm64_tar}" "${tmp}/lima-arm64.tar.gz"
  tar -xzf "${tmp}/lima-arm64.tar.gz" -C "${tmp}/arm64" --strip-components=1
  curl_retry "${base_url}/${x86_tar}" "${tmp}/lima-x86_64.tar.gz"
  tar -xzf "${tmp}/lima-x86_64.tar.gz" -C "${tmp}/x86_64" --strip-components=1

  for arch in arm64 x86_64; do
    cp "${tmp}/${arch}/bin/limactl" "${BIN_DIR}/${arch}/limactl"
    chmod +x "${BIN_DIR}/${arch}/limactl"
  done

  # Bundle Linux-aarch64 guest agent in Resources/ (NOT Contents/MacOS/).
  # Codesign rejects any file under Contents/MacOS/ as an unsigned code object.
  # first-launch.sh stages the gz to ~/.provisa/share/lima/ and creates a
  # symlink ~/.provisa/bin/limactl → real limactl so Lima's SelfDirs() resolves
  # share/lima/ relative to ~/.provisa/bin/ — outside the signed bundle.
  local guest_dir="${APP_BUNDLE}/Contents/Resources/lima-guest-agents"
  mkdir -p "$guest_dir"
  local guest_gz="${tmp}/arm64/share/lima/lima-guestagent.Linux-aarch64.gz"
  if [ -f "$guest_gz" ]; then
    cp "$guest_gz" "${guest_dir}/lima-guestagent.Linux-aarch64.gz"
    ok "Lima guest agent bundled (compressed) in Resources/."
  else
    err "lima-guestagent.Linux-aarch64.gz not found in Lima tarball"
    exit 1
  fi

  rm -rf "$tmp"
  ok "Lima binaries downloaded."
}

# ── Download nerdctl-full archive (bundled for airgapped containerd install) ──
# Lima uses nerdctl-full to provision containerd inside the VM. We pre-download
# it so the install is fully airgapped — no network needed at first launch.
# first-launch.sh stages the archive to ~/.provisa/nerdctl/ and the Lima YAML
# points containerd.archives to that local file:// URL.
download_nerdctl() {
  mkdir -p "$NERDCTL_DIR"
  if [ -f "${NERDCTL_DIR}/${NERDCTL_ARCHIVE}" ]; then
    info "nerdctl archive cached — skipping."
    return
  fi
  info "Downloading nerdctl-full ${NERDCTL_VERSION} (arm64, ~245MB)..."
  curl_retry \
    "https://github.com/containerd/nerdctl/releases/download/v${NERDCTL_VERSION}/${NERDCTL_ARCHIVE}" \
    "${NERDCTL_DIR}/${NERDCTL_ARCHIVE}"
  ok "nerdctl archive downloaded."
}

# ── containerd ────────────────────────────────────────────────────────────────
# containerd is installed inside the VM by Lima via the nerdctl-full archive.
# No macOS-side binary is needed.
download_containerd() {
  ok "containerd: installed in VM from bundled nerdctl-full archive."
}

# ── Download Lima base VM image (bundled for airgapped install) ───────────────
# Lima 2.x requires a base OS disk image to boot the VM. We bundle the
# Ubuntu 24.04 minimal arm64 image (~280MB) so Apple Silicon installs are
# fully airgapped. The minimal variant is used to stay under GitHub
# Releases' 2 GB per-asset limit.
download_vm_images() {
  mkdir -p "$VM_IMAGES_DIR"
  local base_url="https://cloud-images.ubuntu.com/minimal/releases/noble/release"
  local arm64_img="ubuntu-24.04-minimal-cloudimg-arm64.img"
  # Store with a fixed name so first-launch.sh never needs updating when the
  # upstream filename changes across releases.
  local fixed_name="provisa-vm.img"

  if [ -f "${VM_IMAGES_DIR}/${fixed_name}" ]; then
    info "  Skipping (cached): ${fixed_name}"
  else
    info "  Downloading base VM image: ${arm64_img} (~200MB)..."
    curl_retry "${base_url}/${arm64_img}" "${VM_IMAGES_DIR}/${fixed_name}"
    ok "  Saved: ${VM_IMAGES_DIR}/${fixed_name}"
  fi
  ok "Base VM image ready."
}

# ── Download OTel Java agent for Trino (bundled for airgapped install) ───────
# Downloaded at build time (network available on build host); bundled in Resources
# so first-launch.sh can copy it into ~/.provisa/compose/observability/trino-otel/
# without any network access at install time.
download_otel_agent() {
  local dest="${APP_BUNDLE}/Contents/Resources/observability/trino-otel"
  local jar="${dest}/opentelemetry-javaagent.jar"
  if [ -f "$jar" ]; then
    info "OTel Java agent cached — skipping."
    return
  fi
  mkdir -p "$dest"
  info "Downloading OTel Java agent for Trino..."
  curl_retry \
    "https://github.com/open-telemetry/opentelemetry-java-instrumentation/releases/latest/download/opentelemetry-javaagent.jar" \
    "$jar"
  ok "OTel Java agent bundled ($(du -sh "$jar" | cut -f1))."
}

# ── Pre-build provisa wheels for linux/arm64 (airgapped pip install) ─────────
# Wheels are built inside a linux/arm64 container on the build host (network
# available here, not at install time). Bundled into provisa-source/wheels/ so
# Dockerfile can use --no-index --find-links /wheels with no PyPI access.
build_provisa_wheels() {
  local wheels_dir="${SCRIPT_DIR}/tmp-provisa-wheels"
  # Skip if .whl files are already present (e.g. downloaded from CI artifact by pull-images job)
  if [ -d "$wheels_dir" ] && ls "${wheels_dir}"/*.whl &>/dev/null 2>&1; then
    info "Provisa wheels present ($(ls "${wheels_dir}"/*.whl | wc -l | tr -d ' ') wheels) — skipping build."
    return
  fi
  local stamp_file="${wheels_dir}/.pyproject_mtime"
  local current_mtime
  current_mtime=$(stat -f '%m' "${REPO_ROOT}/pyproject.toml" 2>/dev/null || echo "0")
  if [ -d "$wheels_dir" ] && [ "$(ls -A "$wheels_dir" 2>/dev/null)" ] \
     && [ -f "$stamp_file" ] && [ "$(cat "$stamp_file")" = "$current_mtime" ]; then
    info "Provisa wheels cached — skipping."
    return
  fi
  rm -rf "$wheels_dir"
  mkdir -p "$wheels_dir"
  info "Building provisa wheels for linux/arm64 (requires network on build host)..."
  # Copy source to writable tmpfs inside container — egg-info can't be written to :ro mount
  docker run --rm --platform linux/arm64 \
    -v "${REPO_ROOT}:/src:ro" \
    -v "${wheels_dir}:/wheels" \
    python:3.12-slim \
    bash -c "cp -r /src /tmp/src && pip wheel --no-cache-dir --wheel-dir /wheels /tmp/src"
  echo "$current_mtime" > "${wheels_dir}/.pyproject_mtime"
  ok "Provisa wheels built ($(ls "$wheels_dir" | wc -l | tr -d ' ') wheels)."
}

# ── Save service images as tarballs ──────────────────────────────────────────
# Images are saved as .tar.gz (gzip -9) to fit under GitHub's 2 GB per-asset
# limit while bundling the nerdctl archive. Trino:480 is ~1.5 GB uncompressed
# but ~600 MB gzipped. ctr images import handles gzip streams transparently.
save_images() {
  mkdir -p "$IMAGES_DIR"
  local count
  count=$(ls "${IMAGES_DIR}"/*.tar.gz 2>/dev/null | wc -l | tr -d ' ')
  if [ "$count" -ge 6 ]; then
    info "Images pre-populated (${count} tarballs) — skipping docker pull."
    return
  fi
  # Core: 5 images + zaychik = 6 tarballs expected
  if ! command -v docker &>/dev/null; then
    err "docker not found and images not pre-populated in ${IMAGES_DIR}"
    exit 1
  fi
  info "Saving service images (gzip compressed)..."
  for img in "${IMAGES[@]}"; do
    local tag="${img##*/}"
    tag="${tag//:/-}"
    tag="${tag//\//-}"
    local out="${IMAGES_DIR}/${tag}.tar.gz"
    if [ -f "$out" ]; then
      info "  Skipping (cached): ${img}"
      continue
    fi
    info "  Pulling + saving: ${img}"
    docker pull --platform linux/arm64 "$img"
    docker save "$img" | gzip -9 > "$out"
    ok "  Saved: ${out}"
  done
  # Build and save zaychik (custom image, arm64)
  info "  Building + saving zaychik..."
  docker build --platform linux/arm64 -t provisa/zaychik:local "${REPO_ROOT}/zaychik"
  docker save provisa/zaychik:local | gzip -9 > "${IMAGES_DIR}/zaychik-local.tar.gz"
  ok "  Saved zaychik."

  # provisa/provisa:local is built at first-launch from bundled source — not saved here.
}

# ── Embed compose files and config ───────────────────────────────────────────
embed_compose() {
  local res="${APP_BUNDLE}/Contents/Resources"
  mkdir -p "$res"
  cp "${REPO_ROOT}/docker-compose.core.yml" "${res}/docker-compose.core.yml"
  cp "${REPO_ROOT}/docker-compose.app.yml" "${res}/docker-compose.app.yml"
  cp "${REPO_ROOT}/docker-compose.airgap.yml" "${res}/docker-compose.airgap.yml"
  cp -r "${REPO_ROOT}/config" "${res}/config"
  cp -r "${REPO_ROOT}/db" "${res}/db"
  # Copy trino WITHOUT plugins/ — plugins ship as hidden DMG content (trino-plugins/)
  # to avoid pushing the app bundle over the 2 GB GitHub release-asset limit.
  mkdir -p "${res}/trino"
  rsync -a --exclude='plugins/' "${REPO_ROOT}/trino/" "${res}/trino/"
  cp -r "${REPO_ROOT}/observability" "${res}/observability"
  cp "${REPO_ROOT}/scripts/provisa" "${res}/provisa-cli"
  chmod +x "${res}/provisa-cli"
  # Bundle provisa source so first-launch can build provisa/provisa:local inside Lima
  local src_dst="${res}/provisa-source"
  mkdir -p "$src_dst"
  cp "${REPO_ROOT}/Dockerfile"    "$src_dst/"
  cp "${REPO_ROOT}/main.py"        "$src_dst/"
  cp "${REPO_ROOT}/pyproject.toml" "$src_dst/"
  cp -r "${REPO_ROOT}/provisa"    "${src_dst}/provisa"
  # Build React UI and embed static files so the provisa-ui container can serve them.
  # The UI prebuild builds the offline MkDocs docs site (public/docs-site/); point it
  # at the build venv's mkdocs so no global install is required.
  info "Building React UI..."
  local venv="${SCRIPT_DIR}/.build-venv"
  "${venv}/bin/pip" install mkdocs-material pymdown-extensions --quiet --upgrade
  (cd "${REPO_ROOT}/provisa-ui" \
    && MKDOCS_BIN="${venv}/bin/mkdocs" PYTHON_BIN="${venv}/bin/python3" \
       npm ci --silent && MKDOCS_BIN="${venv}/bin/mkdocs" PYTHON_BIN="${venv}/bin/python3" npm run build)
  mkdir -p "${src_dst}/static"
  cp -r "${REPO_ROOT}/provisa-ui/dist/." "${src_dst}/static/"
  ok "React UI built and embedded."
  # Embed pre-built wheels so Dockerfile pip install needs no network
  local wheels_src="${SCRIPT_DIR}/tmp-provisa-wheels"
  if [ ! -d "$wheels_src" ] || [ -z "$(ls -A "$wheels_src" 2>/dev/null)" ]; then
    err "No wheels found in ${wheels_src} — run build_provisa_wheels() first."
    exit 1
  fi
  cp -r "$wheels_src" "${src_dst}/wheels"
  ok "Compose files, config, and provisa source embedded."
}

# ── Bundle the standalone native Python runtime (REQ-979) ────────────────────
# The native (no-Docker) tier runs provisa on a self-contained interpreter shipped
# in the app bundle. We download python-build-standalone (a relocatable CPython for
# macOS arm64), pip-install provisa + its deps INTO it (macOS wheels from PyPI — the
# native runtime is macOS, so it cannot reuse the linux/arm64 wheels from
# build_provisa_wheels), and drop the built UI where ui_server expects it
# (STATIC_DIR = <site-packages>/static). first-launch.sh stages this to
# ~/.provisa/runtime and scripts/provisa runs uvicorn against it.
#
# Pins are overridable so the builder can bump CPython without editing this file.
PBS_RELEASE="${PBS_RELEASE:-20250612}"
PBS_PYTHON="${PBS_PYTHON:-3.12.11}"
RUNTIME_PAYLOAD_DIR="${SCRIPT_DIR}/runtime-payload"   # staged OUTSIDE the .app (hidden DMG content)
bundle_native_runtime() {
  # Ships as hidden DMG payload (not inside the notarized .app) — like images/ —
  # so the .app stays small and notarizes fast. first-launch.sh stages it to
  # ~/.provisa/runtime and ad-hoc signs + de-quarantines it so it runs.
  local dest="${RUNTIME_PAYLOAD_DIR}/runtime"
  if [ -x "${dest}/bin/python3" ] && [ -d "${dest}"/lib/python3.*/site-packages/provisa ]; then
    info "Native runtime already bundled — skipping."
    return
  fi
  rm -rf "$dest"
  mkdir -p "$(dirname "$dest")"

  local tarball="cpython-${PBS_PYTHON}+${PBS_RELEASE}-aarch64-apple-darwin-install_only.tar.gz"
  local url="https://github.com/astral-sh/python-build-standalone/releases/download/${PBS_RELEASE}/${tarball}"
  local tmp="${SCRIPT_DIR}/tmp-pbs"
  rm -rf "$tmp"; mkdir -p "$tmp"

  info "Downloading python-build-standalone ${PBS_PYTHON} (macOS arm64)..."
  curl_retry "$url" "${tmp}/${tarball}"
  tar -xzf "${tmp}/${tarball}" -C "$tmp"        # extracts to ${tmp}/python/
  if [ ! -x "${tmp}/python/bin/python3" ]; then
    err "python-build-standalone extraction failed (no bin/python3)"
    exit 1
  fi
  mv "${tmp}/python" "$dest"

  info "Installing provisa + deps into the native runtime (macOS wheels from PyPI)..."
  "${dest}/bin/python3" -m pip install --upgrade pip --quiet
  # mcp-proxy (REQ-1104): Node-free stdio<->Streamable-HTTP bridge for the Claude Desktop connector,
  # bundled so the runtime's own python is the config command (no npx, no user pip, airgapped).
  "${dest}/bin/python3" -m pip install --quiet "${REPO_ROOT}" uvicorn mcp-proxy
  "${dest}/bin/python3" -c "import mcp_proxy" || { err "mcp-proxy missing from the bundled native runtime"; exit 1; }

  # Place the built UI where ui_server resolves it: <site-packages>/static.
  # embed_compose builds provisa-ui/dist earlier in the pipeline.
  local site
  site="$("${dest}/bin/python3" -c 'import sysconfig; print(sysconfig.get_paths()["purelib"])')"
  if [ ! -d "${REPO_ROOT}/provisa-ui/dist" ]; then
    err "provisa-ui/dist not found — embed_compose must build the UI before bundle_native_runtime."
    exit 1
  fi
  mkdir -p "${site}/static"
  cp -r "${REPO_ROOT}/provisa-ui/dist/." "${site}/static/"

  rm -rf "$tmp"
  ok "Native runtime bundled ($(du -sh "$dest" | cut -f1))."
}

# ── Build SwiftUI launcher and embed binary ───────────────────────────────────
build_launcher() {
  info "Building ProvisaLauncher (Swift)..."
  local launcher_dir="${SCRIPT_DIR}/ProvisaLauncher"
  swift build --package-path "$launcher_dir" -c release 2>&1 | grep -v "^Build complete"
  local binary="${launcher_dir}/.build/release/ProvisaLauncher"
  if [ ! -f "$binary" ]; then
    err "ProvisaLauncher binary not found after build: ${binary}"
    exit 1
  fi
  # Replace old shell launcher with native Swift binary
  rm -f "${APP_BUNDLE}/Contents/MacOS/provisa-launcher"
  cp "$binary" "${APP_BUNDLE}/Contents/MacOS/ProvisaLauncher"
  chmod +x "${APP_BUNDLE}/Contents/MacOS/ProvisaLauncher"
  ok "ProvisaLauncher built and embedded."
}

# ── Copy first-launch script ──────────────────────────────────────────────────
embed_scripts() {
  cp "${SCRIPT_DIR}/first-launch.sh" "${APP_BUNDLE}/Contents/MacOS/first-launch.sh"
  chmod +x "${APP_BUNDLE}/Contents/MacOS/first-launch.sh"
  ok "Scripts embedded."
}

# ── Sign macOS native binaries embedded inside JARs ──────────────────────────
# Apple notarization rejects unsigned .dylib/.jnilib/.so files even when they
# are nested inside JARs inside the app bundle. We extract, sign, and repack.
sign_jar_natives() {
  if [ -z "${APPLE_DEVELOPER_ID:-}" ]; then
    return
  fi

  local id="${APPLE_DEVELOPER_ID}"
  local sign_flags=(--force --sign "$id" --options runtime --timestamp)
  # Plugins are now in REPO_ROOT/trino/plugins (not inside app bundle)
  # They are bundled as hidden DMG content, outside the notarized .app,
  # so Apple notarytool will not scan them. Signing is skipped.
  info "Trino plugins are outside the app bundle (hidden DMG content) — JAR native signing not required."
  return
  # (unreachable — kept for reference if plugins are moved back inside bundle)
  local plugins_dir="${APP_BUNDLE}/Contents/Resources/trino/plugins"

  if [ ! -d "$plugins_dir" ]; then
    info "No trino/plugins directory — skipping JAR native signing."
    return
  fi

  info "Signing macOS native binaries inside Trino plugin JARs..."
  local tmp_jar_dir
  tmp_jar_dir=$(mktemp -d)

  local jar_count=0
  local signed_count=0

  while IFS= read -r -d '' jar; do
    local jar_tmp="${tmp_jar_dir}/$(basename "$jar" .jar)-$$"
    mkdir -p "$jar_tmp"

    # Extract only macOS native files
    local natives
    natives=$(unzip -l "$jar" 2>/dev/null \
      | awk '{print $NF}' \
      | grep -E '\.(dylib|jnilib)$|/osx[_/]|/mac[_/]|/darwin[_/]|/Mac[_/]|so_osx' \
      | grep -v '^$' || true)

    if [ -z "$natives" ]; then
      rm -rf "$jar_tmp"
      continue
    fi

    # Extract those files
    local extracted=0
    while IFS= read -r entry; do
      [ -z "$entry" ] && continue
      unzip -q "$jar" "$entry" -d "$jar_tmp" 2>/dev/null && extracted=$((extracted + 1))
    done <<< "$natives"

    if [ "$extracted" -eq 0 ]; then
      rm -rf "$jar_tmp"
      continue
    fi

    # Sign each extracted native binary
    local jar_signed=0
    while IFS= read -r -d '' native; do
      codesign "${sign_flags[@]}" "$native" 2>/dev/null && jar_signed=$((jar_signed + 1))
    done < <(find "$jar_tmp" -type f \( -name "*.dylib" -o -name "*.jnilib" -o -name "*.so_osx*" \) -print0)

    if [ "$jar_signed" -gt 0 ]; then
      # Repack — update jar in place with signed binaries
      (cd "$jar_tmp" && zip -u "$jar" $(find . -type f \( -name "*.dylib" -o -name "*.jnilib" -o -name "*.so_osx*" \) | sed 's|^\./||') 2>/dev/null)
      signed_count=$((signed_count + jar_signed))
      jar_count=$((jar_count + 1))
      info "  Signed ${jar_signed} native(s) in $(basename "$jar")"
    fi

    rm -rf "$jar_tmp"
  done < <(find "$plugins_dir" -name "*.jar" -print0)

  rm -rf "$tmp_jar_dir"
  ok "JAR native signing complete: ${signed_count} binaries in ${jar_count} JARs."
}

# ── Code signing ──────────────────────────────────────────────────────────────
sign_app() {
  if [ -z "${APPLE_DEVELOPER_ID:-}" ]; then
    info "APPLE_DEVELOPER_ID not set — skipping signing."
    return
  fi

  local id="${APPLE_DEVELOPER_ID}"
  local sign_flags=(--force --sign "$id" --options runtime --timestamp)

  # Apple requires every code object inside the bundle to be signed with
  # Developer ID + secure timestamp before the outer bundle is signed.
  # --deep is NOT used: it doesn't reliably propagate --timestamp.
  # Sign innermost files first, then the outer bundle.
  # Sign all code objects in explicit dependency order (innermost first).
  # find is not used because it doesn't guarantee order, and codesign requires
  # every subcomponent to be signed before the file that contains/calls it.
  info "Signing bundled executables (inner → outer)..."

  # limactl needs com.apple.security.virtualization to use Apple's VZ framework
  local limactl_entitlements="${SCRIPT_DIR}/entitlements-limactl.plist"
  for arch in arm64 x86_64; do
    local lc="${APP_BUNDLE}/Contents/MacOS/bin/${arch}/limactl"
    [ -f "$lc" ] || continue
    codesign "${sign_flags[@]}" --entitlements "$limactl_entitlements" "$lc"
    info "  Signed (with virtualization entitlement): bin/${arch}/limactl"
  done

  local sign_targets=(
    "${APP_BUNDLE}/Contents/MacOS/first-launch.sh"
    "${APP_BUNDLE}/Contents/MacOS/ProvisaLauncher"
  )
  for f in "${sign_targets[@]}"; do
    [ -f "$f" ] || continue
    codesign "${sign_flags[@]}" "$f"
    info "  Signed: ${f#"${APP_BUNDLE}/"}"
  done

  info "Signing app bundle..."
  codesign "${sign_flags[@]}" --verbose \
    --entitlements "${SCRIPT_DIR}/entitlements.plist" \
    "${APP_BUNDLE}"

  # Diagnostic: show what cert was actually used for limactl
  info "Verifying limactl signature (certificate details):"
  codesign -dvvv "${APP_BUNDLE}/Contents/MacOS/bin/arm64/limactl" 2>&1 | grep -E "Authority|TeamIdent|Signature" || true

  ok "App bundle signed."
}

# ── Notarization (targets the .app bundle, NOT the DMG) ──────────────────────
# Images are kept outside the .app so the bundle is small and notarizes in
# seconds rather than the 45-60 min needed to scan a 1.4 GB DMG.
notarize_app() {
  if [ -z "${APPLE_NOTARYTOOL_APPLE_ID:-}" ]; then
    info "APPLE_NOTARYTOOL_APPLE_ID not set — skipping notarization."
    return
  fi

  local notary_args=(
    --apple-id "${APPLE_NOTARYTOOL_APPLE_ID}"
    --password "${APPLE_NOTARYTOOL_PASSWORD}"
    --team-id  "${APPLE_NOTARYTOOL_TEAM_ID}"
  )

  # notarytool requires a zip, pkg, or dmg — zip the .app with ditto
  local zip_path="${OUT_DIR}/Provisa-notarize.zip"
  info "Zipping app bundle for notarization submission..."
  ditto -c -k --keepParent "${APP_BUNDLE}" "$zip_path"

  info "Submitting app bundle for notarization..."
  local submit_out submit_err submit_rc
  submit_out=$(xcrun notarytool submit "$zip_path" "${notary_args[@]}" --output-format json 2>/tmp/notary-submit-err) \
    && submit_rc=0 || submit_rc=$?
  submit_err=$(cat /tmp/notary-submit-err 2>/dev/null || true)
  rm -f "$zip_path" /tmp/notary-submit-err
  if [ $submit_rc -ne 0 ]; then
    if printf '%s\n%s' "$submit_out" "$submit_err" | grep -qi "required agreement\|403"; then
      info "WARNING: Notarization skipped — Apple Developer agreement missing or expired (HTTP 403). DMG will be unsigned."
      return
    fi
    err "notarytool submit failed (exit $submit_rc): $submit_err"
    exit 1
  fi
  local submission_id
  submission_id=$(printf '%s' "$submit_out" | python3 -c "import sys,json; print(json.load(sys.stdin)['id'])")
  ok "Submission ID: ${submission_id}"

  # Poll with retry — notarytool --wait has no retry on transient network errors
  local max_polls=45   # 45 × 40 s = 30 min ceiling (small app should be fast)
  local poll=0
  local status=""
  while [ $poll -lt $max_polls ]; do
    status=$(xcrun notarytool info "$submission_id" "${notary_args[@]}" \
               --output-format json 2>/dev/null \
             | python3 -c "import sys,json; print(json.load(sys.stdin).get('status',''))" \
             2>/dev/null || echo "network-error")

    case "$status" in
      Accepted)
        ok "Notarization accepted."
        break ;;
      Invalid|Rejected)
        err "Notarization ${status}:"
        xcrun notarytool log "$submission_id" "${notary_args[@]}" >&2 || true
        exit 1 ;;
      network-error)
        info "  Network error — retrying in 40 s (poll $((poll+1))/${max_polls})..." ;;
      *)
        info "  Status: ${status:-unknown} (poll $((poll+1))/${max_polls})" ;;
    esac

    sleep 40
    poll=$((poll + 1))
  done

  if [ "$status" != "Accepted" ]; then
    err "Notarization timed out after $((max_polls * 40 / 60)) minutes."
    exit 1
  fi

  xcrun stapler staple "${APP_BUNDLE}"
  ok "App bundle notarized and stapled."
}

# ── Create DMG ────────────────────────────────────────────────────────────────
create_dmg() {
  info "Creating DMG..."
  mkdir -p "$OUT_DIR"
  local tmp_dmg="${OUT_DIR}/tmp-provisa"
  rm -rf "$tmp_dmg"
  mkdir -p "$tmp_dmg"
  cp -r "${APP_BUNDLE}" "${tmp_dmg}/Provisa.app"

  # Slim base (REQ-979): the core container images are NOT bundled in the DMG — that
  # kept the default (native) install under GitHub's 2 GB asset limit once the native
  # runtime is bundled. The Docker/Trino tier fetches them via first-launch's
  # acquire_addon from the published provisa-core-images-<version>.tar.gz (or a copy
  # pre-staged beside the installer for airgapped installs).

  mkdir -p "${tmp_dmg}/nerdctl"
  cp "${NERDCTL_DIR}/${NERDCTL_ARCHIVE}" "${tmp_dmg}/nerdctl/"
  chflags hidden "${tmp_dmg}/nerdctl"

  mkdir -p "${tmp_dmg}/vm-image"
  cp "${VM_IMAGES_DIR}"/*.img "${tmp_dmg}/vm-image/"
  chflags hidden "${tmp_dmg}/vm-image"

  # The native Python runtime is NOT bundled here — it ships in a separate DMG
  # (create_runtime_dmg) to keep this core DMG under GitHub's 2 GB asset limit.
  # first-launch.sh stages it from the mounted runtime DMG (/Volumes/*/runtime).

  # Remove any existing DMG so create-dmg doesn't complain
  rm -f "${DMG_PATH}"

  create-dmg \
    --volname "Provisa" \
    --volicon "${SCRIPT_DIR}/Provisa.icns" \
    --background "${SCRIPT_DIR}/dmg-background.png" \
    --window-pos 200 120 \
    --window-size 660 400 \
    --icon-size 128 \
    --icon "Provisa.app" 165 230 \
    --hide-extension "Provisa.app" \
    --app-drop-link 495 230 \
    "${DMG_PATH}" \
    "${tmp_dmg}/"

  rm -rf "$tmp_dmg"
  ok "DMG created: ${DMG_PATH}"
}

# ── Native runtime DMG (separate asset, <2 GB) ────────────────────────────────
# Ships the standalone Python runtime for the no-Docker tier as its own DMG so the
# core DMG stays under GitHub's 2 GB release-asset limit. first-launch.sh's
# stage_native_runtime() already searches /Volumes/*/runtime, so mounting this DMG
# (airgap: the user downloads both) is enough for it to be found.
create_runtime_dmg() {
  if [ ! -d "${RUNTIME_PAYLOAD_DIR}/runtime" ]; then
    err "Native runtime payload missing — bundle_native_runtime must run first."
    exit 1
  fi
  info "Creating native runtime DMG..."
  local tmp_dmg="${OUT_DIR}/tmp-runtime"
  rm -rf "$tmp_dmg"; mkdir -p "$tmp_dmg"
  cp -R "${RUNTIME_PAYLOAD_DIR}/runtime" "${tmp_dmg}/runtime"
  rm -f "${RUNTIME_DMG_PATH}"
  hdiutil create -volname "Provisa Runtime" -srcfolder "${tmp_dmg}" \
    -ov -format UDZO "${RUNTIME_DMG_PATH}"
  rm -rf "$tmp_dmg"
  ok "Runtime DMG created: ${RUNTIME_DMG_PATH}"
}

# ── Main ──────────────────────────────────────────────────────────────────────
main() {
  printf "\n${BOLD}Provisa DMG Builder — Phase AF2a${NC}\n"
  printf "═══════════════════════════════════════════\n\n"

  check_prereqs
  mkdir -p "${BIN_DIR}/arm64" "${BIN_DIR}/x86_64"

  generate_assets
  download_lima
  download_nerdctl
  download_containerd
  download_vm_images
  save_images
  build_provisa_wheels
  embed_compose        # copies observability/ from repo; builds provisa-ui/dist; before download_otel_agent
  download_otel_agent  # adds opentelemetry-javaagent.jar into Resources/observability/trino-otel/
  bundle_native_runtime # standalone Python for the native tier (uses provisa-ui/dist from embed_compose)
  build_launcher       # compile SwiftUI launcher and embed binary
  embed_scripts
  sign_jar_natives  # sign macOS natives inside Trino plugin JARs before outer bundle signing
  sign_app
  notarize_app   # notarize the small .app before images are added
  create_dmg     # DMG bundles Provisa.app (notarized) + images/ alongside
  create_runtime_dmg  # native Python runtime in its own DMG (2 GB asset limit)

  printf "\n${GREEN}${BOLD}Build complete.${NC}\n"
  printf "DMG: %s\n" "${DMG_PATH}"
  printf "Runtime DMG: %s\n" "${RUNTIME_DMG_PATH}"
}

main "$@"
