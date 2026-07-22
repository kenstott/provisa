#!/usr/bin/env bash
# Build airgapped Linux AppImage.
# Images must be pre-populated in packaging/linux/images/ (6+ tarballs).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
IMAGES_DIR="${SCRIPT_DIR}/images"
APPDIR="${SCRIPT_DIR}/Provisa.AppDir"
OUT_DIR="${SCRIPT_DIR}/dist"
DOCKER_BIN_CACHE="${SCRIPT_DIR}/.docker-bin-cache"
APPIMAGETOOL_URL="https://github.com/AppImage/AppImageKit/releases/download/continuous/appimagetool-x86_64.AppImage"
APPIMAGETOOL="${SCRIPT_DIR}/appimagetool-x86_64.AppImage"

# Pin Docker version — update here to upgrade bundled runtime
DOCKER_VERSION="${DOCKER_VERSION:-27.5.1}"
DOCKER_ARCH="x86_64"
DOCKER_BASE_URL="https://download.docker.com/linux/static/stable/${DOCKER_ARCH}"

# Native tier: bare python-build-standalone interpreter for Linux x86_64. Keep the
# SAME pins as packaging/macos/build-dmg.sh (only the platform triple differs).
PBS_RELEASE="${PBS_RELEASE:-20250612}"
PBS_PYTHON="${PBS_PYTHON:-3.12.11}"
NATIVE_PAYLOAD_DIR="${SCRIPT_DIR}/.native-payload-cache"

RED='\033[0;31m'; GREEN='\033[0;32m'; CYAN='\033[0;36m'; BOLD='\033[1m'; NC='\033[0m'
info() { printf "${CYAN}[build-appimage]${NC} %s\n" "$*"; }
ok()   { printf "${GREEN}[build-appimage]${NC} %s\n" "$*"; }
err()  { printf "${RED}[build-appimage]${NC} %s\n" "$*" >&2; }

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

# ── Pre-build provisa image for linux/amd64 and save as tarball ───────────────
# Source is never bundled into the AppImage — only the compiled image ships.
build_provisa_image() {
  local out="${IMAGES_DIR}/provisa-local.tar.gz"
  local stamp_file="${IMAGES_DIR}/.provisa_mtime"
  local current_mtime
  current_mtime=$(stat -c '%Y' "${REPO_ROOT}/pyproject.toml" 2>/dev/null || echo "0")

  if [ -f "$out" ] && [ -f "$stamp_file" ] && [ "$(cat "$stamp_file")" = "$current_mtime" ]; then
    info "provisa-local.tar.gz cached — skipping build."
    return
  fi

  if ! command -v docker &>/dev/null; then
    err "docker not found — required to pre-build provisa image on packaging host"
    exit 1
  fi

  info "Building provisa/provisa:local for linux/amd64 (source stays on build host)..."
  local retries=3
  local attempt=0
  until docker build \
    --platform linux/amd64 \
    --tag provisa/provisa:local \
    "${REPO_ROOT}"; do
    attempt=$((attempt + 1))
    if [ "$attempt" -ge "$retries" ]; then
      err "docker build failed after ${retries} attempts"
      exit 1
    fi
    info "docker build failed (attempt ${attempt}/${retries}), retrying in 30s..."
    sleep 30
  done

  info "Saving provisa/provisa:local → $(basename "$out")..."
  mkdir -p "$IMAGES_DIR"
  docker save provisa/provisa:local | gzip -9 > "$out"
  echo "$current_mtime" > "$stamp_file"
  ok "provisa image saved: ${out}"
}

# ── Prerequisites ──────────────────────────────────────────────────────────────
check_prereqs() {
  if ! command -v curl &>/dev/null; then
    err "Required tool not found: curl"
    exit 1
  fi
  if ! dpkg -s libfuse2 &>/dev/null 2>&1; then
    info "Installing libfuse2 (required by appimagetool)..."
    sudo apt-get update -qq
    sudo apt-get install -y libfuse2
  fi
  ok "Prerequisites satisfied."
}

# ── Download and cache Docker static binaries ──────────────────────────────────
bundle_docker() {
  local cache_marker="${DOCKER_BIN_CACHE}/.version-${DOCKER_VERSION}"
  if [ -f "$cache_marker" ]; then
    info "Docker ${DOCKER_VERSION} binaries already cached."
    return
  fi

  info "Downloading Docker ${DOCKER_VERSION} static binaries..."
  mkdir -p "$DOCKER_BIN_CACHE"

  # Core daemon + runtime binaries
  curl -fsSL "${DOCKER_BASE_URL}/docker-${DOCKER_VERSION}.tgz" \
    | tar -xz -C "$DOCKER_BIN_CACHE" --strip-components=1 \
        docker/dockerd \
        docker/docker-proxy \
        docker/docker-init \
        docker/containerd \
        docker/containerd-shim-runc-v2 \
        docker/runc

  # Rootless extras (no sudo/root required)
  curl -fsSL "${DOCKER_BASE_URL}/docker-rootless-extras-${DOCKER_VERSION}.tgz" \
    | tar -xz -C "$DOCKER_BIN_CACHE" --strip-components=1 \
        docker-rootless-extras/dockerd-rootless.sh \
        docker-rootless-extras/rootlesskit \
        docker-rootless-extras/rootlesskit-docker-proxy \
        docker-rootless-extras/vpnkit

  chmod +x "${DOCKER_BIN_CACHE}"/*
  touch "$cache_marker"
  ok "Docker ${DOCKER_VERSION} binaries cached at ${DOCKER_BIN_CACHE}/"
}

# ── Save service images as tarballs ───────────────────────────────────────────
save_images() {
  mkdir -p "$IMAGES_DIR"
  local count
  count=$(find "${IMAGES_DIR}" -maxdepth 1 -name "*.tar.gz" 2>/dev/null | wc -l | tr -d ' ')
  if [ "$count" -ge 5 ]; then
    info "Images pre-populated (${count} tarballs) — skipping docker pull."
    return
  fi
  if ! command -v docker &>/dev/null; then
    err "docker not found and images not pre-populated in ${IMAGES_DIR}"
    exit 1
  fi
  # Core images only — the obs stack (minio/otel/prometheus/tempo/grafana) is NOT bundled,
  # keeping the AppImage under GitHub's 2 GB asset limit. Obs ships as the downloadable
  # provisa-obs-images-<version>.tar.gz add-on.
  info "Saving service images (core)..."
  local images=(
    "postgres:16"
    "edoburu/pgbouncer:latest"
    "redis:7-alpine"
    "trinodb/trino:480"
  )
  for img in "${images[@]}"; do
    local tag="${img##*/}"
    tag="${tag//:/-}"
    tag="${tag//\//-}"
    local out="${IMAGES_DIR}/${tag}.tar.gz"
    if [ -f "$out" ]; then
      info "  Skipping (cached): ${img}"
      continue
    fi
    info "  Pulling + saving: ${img}"
    docker pull "$img"
    docker save "$img" | gzip -9 > "$out"
    ok "  Saved: ${out}"
  done
}

# ── Build AppDir ───────────────────────────────────────────────────────────────
build_appdir() {
  info "Building AppDir..."
  rm -rf "$APPDIR"
  mkdir -p "${APPDIR}/compose" "${APPDIR}/bin"

  # Bundled Docker runtime
  cp "${DOCKER_BIN_CACHE}"/dockerd \
     "${DOCKER_BIN_CACHE}"/docker-proxy \
     "${DOCKER_BIN_CACHE}"/docker-init \
     "${DOCKER_BIN_CACHE}"/containerd \
     "${DOCKER_BIN_CACHE}"/containerd-shim-runc-v2 \
     "${DOCKER_BIN_CACHE}"/runc \
     "${DOCKER_BIN_CACHE}"/dockerd-rootless.sh \
     "${DOCKER_BIN_CACHE}"/rootlesskit \
     "${DOCKER_BIN_CACHE}"/rootlesskit-docker-proxy \
     "${DOCKER_BIN_CACHE}"/vpnkit \
     "${APPDIR}/bin/"
  chmod +x "${APPDIR}/bin/"*

  # Slim AppImage: images are NOT bundled (native payload + images would exceed the
  # 2 GB asset limit). The Docker tier acquires provisa-core-images-amd64-<ver>.zip
  # on demand at first launch (local-first beside the AppImage, else download).

  # Copy compose files and config (core only; obs images are not bundled, so the obs
  # overlay is intentionally omitted — the CLI won't auto-start obs without it. Demo
  # excluded on Linux.)
  cp "${REPO_ROOT}/docker-compose.core.yml"        "${APPDIR}/compose/"
  cp "${REPO_ROOT}/docker-compose.app.yml"         "${APPDIR}/compose/"
  cp "${REPO_ROOT}/docker-compose.airgap.yml"      "${APPDIR}/compose/"
  cp -r "${REPO_ROOT}/config"                      "${APPDIR}/compose/config"
  cp -r "${REPO_ROOT}/db"                          "${APPDIR}/compose/db"
  # Copy trino WITHOUT plugins/ — plugins ship as a separate release asset
  # (provisa-trino-plugins-*.tar.gz) to keep the AppImage under the 2 GB limit.
  mkdir -p "${APPDIR}/compose/trino"
  rsync -a --exclude='plugins/' "${REPO_ROOT}/trino/" "${APPDIR}/compose/trino/"
  cp -r "${REPO_ROOT}/observability"               "${APPDIR}/compose/observability"

  # Copy CLI and launch scripts
  cp "${REPO_ROOT}/scripts/provisa"         "${APPDIR}/provisa-cli"
  chmod +x "${APPDIR}/provisa-cli"
  cp "${SCRIPT_DIR}/first-launch.sh"        "${APPDIR}/first-launch.sh"
  chmod +x "${APPDIR}/first-launch.sh"
  cp "${SCRIPT_DIR}/AppRun"                 "${APPDIR}/AppRun"
  chmod +x "${APPDIR}/AppRun"
  cp "${SCRIPT_DIR}/Provisa.desktop"        "${APPDIR}/Provisa.desktop"

  # Bake the release version so first-launch.sh can pin the online native pip
  # install to the matching release (parity with macOS Resources/VERSION).
  printf '%s' "${VERSION:-dev}" > "${APPDIR}/VERSION"

  # Brand icon (graphite/emerald P mark).
  if [ -f "${SCRIPT_DIR}/Provisa.png" ]; then
    cp "${SCRIPT_DIR}/Provisa.png" "${APPDIR}/Provisa.png"
  else
    echo "ERROR: ${SCRIPT_DIR}/Provisa.png not found — run packaging/macos/generate-icon.py" >&2
    exit 1
  fi

  ok "AppDir built at ${APPDIR}"
}

# ── Stage the native-tier payload into the AppDir (parity with macOS) ─────────
# The native (no-Docker) tier builds its own Python venv at first launch from a
# bundled bare interpreter + a Linux x86_64 wheelhouse. Three dirs are staged
# inside the AppDir (no hidden DMG content on Linux — first-launch reads
# ${APPDIR}/{python-base,wheels,ui-dist}):
#   python-base/  bare python-build-standalone CPython (NOT pip-installed)
#   wheels/       Linux x86_64 wheelhouse (provisa[embedded] + uvicorn + mcp-proxy + deps)
#   ui-dist/      built provisa-ui/dist (ui_server resolves STATIC_DIR from it)
# Downloads/builds are cached in NATIVE_PAYLOAD_DIR so re-runs skip network work,
# then copied into the (freshly-wiped) AppDir. Must run AFTER build_appdir.
bundle_native_payload() {
  local base="${NATIVE_PAYLOAD_DIR}/python-base"
  local wheels="${NATIVE_PAYLOAD_DIR}/wheels"
  local ui="${NATIVE_PAYLOAD_DIR}/ui-dist"

  # ── 1. Bare python-build-standalone interpreter (no provisa install) ──
  if [ -x "${base}/bin/python3" ]; then
    info "python-base already staged — skipping download."
  else
    rm -rf "$base"; mkdir -p "$(dirname "$base")"
    local tarball="cpython-${PBS_PYTHON}+${PBS_RELEASE}-x86_64-unknown-linux-gnu-install_only.tar.gz"
    local url="https://github.com/astral-sh/python-build-standalone/releases/download/${PBS_RELEASE}/${tarball}"
    local tmp="${SCRIPT_DIR}/tmp-pbs"
    rm -rf "$tmp"; mkdir -p "$tmp"
    info "Downloading python-build-standalone ${PBS_PYTHON} (Linux x86_64)..."
    curl_retry "$url" "${tmp}/${tarball}"
    tar -xzf "${tmp}/${tarball}" -C "$tmp"        # extracts to ${tmp}/python/
    if [ ! -x "${tmp}/python/bin/python3" ]; then
      err "python-build-standalone extraction failed (no bin/python3)"
      exit 1
    fi
    mv "${tmp}/python" "$base"
    rm -rf "$tmp"
    ok "python-base staged (bare interpreter, $(du -sh "$base" | cut -f1))."
  fi

  # ── 2. Linux x86_64 wheelhouse ──
  info "Building the provisa wheel (Linux)..."
  # Build with the bundled python-base (the runner's default python may lack `build`;
  # the provisa wheel is pure-python so the interpreter version doesn't matter).
  "${base}/bin/python3" -m pip install --quiet build
  if [ -x "${REPO_ROOT}/scripts/build-wheel.sh" ]; then
    PROVISA_SKIP_UI_BUILD=1 PYTHON="${base}/bin/python3" "${REPO_ROOT}/scripts/build-wheel.sh" --wheel
  else
    ( cd "$REPO_ROOT" && "${base}/bin/python3" -m build --wheel )
  fi
  local built_wheel
  built_wheel="$(ls -t "${REPO_ROOT}/dist"/provisa-*.whl 2>/dev/null | head -1)"
  if [ -z "$built_wheel" ] || [ ! -f "$built_wheel" ]; then
    err "provisa wheel not found in ${REPO_ROOT}/dist after build."
    exit 1
  fi
  rm -rf "$wheels"; mkdir -p "$wheels"
  info "Downloading Linux x86_64 wheelhouse (provisa[embedded] + uvicorn + mcp-proxy + deps)..."
  # mcp-proxy (REQ-1104): Node-free stdio<->Streamable-HTTP bridge for the Claude Desktop connector.
  "${base}/bin/python3" -m pip download --dest "$wheels" "${built_wheel}[embedded]" uvicorn mcp-proxy
  ok "Wheelhouse staged ($(ls "$wheels" | wc -l | tr -d ' ') wheels)."

  # ── 3. Built UI (build provisa-ui/dist if not already present) ──
  if [ ! -d "${REPO_ROOT}/provisa-ui/dist" ]; then
    info "Building React UI..."
    ( cd "${REPO_ROOT}/provisa-ui" && npm ci --silent && npm run build )
  fi
  if [ ! -d "${REPO_ROOT}/provisa-ui/dist" ]; then
    err "provisa-ui/dist not found after build."
    exit 1
  fi
  rm -rf "$ui"; mkdir -p "$ui"
  cp -r "${REPO_ROOT}/provisa-ui/dist/." "$ui/"
  ok "ui-dist staged."

  # ── Copy the cached payload into the (freshly-built) AppDir ──
  info "Copying native payload into AppDir..."
  rm -rf "${APPDIR}/python-base" "${APPDIR}/wheels" "${APPDIR}/ui-dist"
  cp -R "$base"   "${APPDIR}/python-base"
  cp -R "$wheels" "${APPDIR}/wheels"
  cp -R "$ui"     "${APPDIR}/ui-dist"
  chmod -R u+rwX "${APPDIR}/python-base"
  ok "Native payload bundled into AppDir."
}

# ── Create AppImage ────────────────────────────────────────────────────────────
create_appimage() {
  info "Fetching appimagetool..."
  if [ ! -f "$APPIMAGETOOL" ]; then
    curl -fsSL "$APPIMAGETOOL_URL" -o "$APPIMAGETOOL"
    chmod +x "$APPIMAGETOOL"
  fi

  mkdir -p "$OUT_DIR"
  info "Packing AppImage..."
  APPIMAGE_EXTRACT_AND_RUN=1 ARCH=x86_64 \
    "$APPIMAGETOOL" "$APPDIR" "${OUT_DIR}/Provisa.AppImage"
  ok "AppImage created: ${OUT_DIR}/Provisa.AppImage"
}

# ── Main ───────────────────────────────────────────────────────────────────────
main() {
  printf "\n${BOLD}Provisa AppImage Builder${NC}\n"
  printf "═══════════════════════════════════════════\n\n"
  info "Bundling Docker ${DOCKER_VERSION} runtime..."

  check_prereqs
  bundle_docker           # rootless docker binaries (Docker tier runtime)
  build_appdir            # compose + CLI + first-launch (no bundled images — slim)
  bundle_native_payload   # bare interpreter + Linux wheelhouse + ui-dist → AppDir (native tier)
  create_appimage

  printf "\n${GREEN}${BOLD}Build complete.${NC}\n"
  printf "AppImage: %s\n" "${OUT_DIR}/Provisa.AppImage"
}

main "$@"
