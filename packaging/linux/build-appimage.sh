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

RED='\033[0;31m'; GREEN='\033[0;32m'; CYAN='\033[0;36m'; BOLD='\033[1m'; NC='\033[0m'
info() { printf "${CYAN}[build-appimage]${NC} %s\n" "$*"; }
ok()   { printf "${GREEN}[build-appimage]${NC} %s\n" "$*"; }
err()  { printf "${RED}[build-appimage]${NC} %s\n" "$*" >&2; }

# ── Pre-build provisa wheels for linux/amd64 (airgapped pip install) ──────────
build_provisa_wheels() {
  local wheels_dir="${SCRIPT_DIR}/tmp-provisa-wheels"
  local stamp_file="${wheels_dir}/.pyproject_mtime"
  local current_mtime
  current_mtime=$(stat -c '%Y' "${REPO_ROOT}/pyproject.toml" 2>/dev/null || echo "0")
  if [ -d "$wheels_dir" ] && [ "$(ls -A "$wheels_dir" 2>/dev/null)" ] \
     && [ -f "$stamp_file" ] && [ "$(cat "$stamp_file")" = "$current_mtime" ]; then
    info "Provisa wheels cached — skipping."
    return
  fi
  rm -rf "$wheels_dir"
  mkdir -p "$wheels_dir"
  info "Building provisa wheels for linux/amd64 (requires network on build host)..."
  docker run --rm --platform linux/amd64 \
    -v "${REPO_ROOT}:/src:ro" \
    -v "${wheels_dir}:/wheels" \
    python:3.12-slim \
    pip wheel --no-cache-dir --wheel-dir /wheels /src
  echo "$current_mtime" > "${wheels_dir}/.pyproject_mtime"
  ok "Provisa wheels built ($(ls "$wheels_dir" | wc -l | tr -d ' ') wheels)."
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
  if [ "$count" -ge 6 ]; then
    info "Images pre-populated (${count} tarballs) — skipping docker pull."
    return
  fi
  if ! command -v docker &>/dev/null; then
    err "docker not found and images not pre-populated in ${IMAGES_DIR}"
    exit 1
  fi
  info "Saving service images..."
  local images=(
    "postgres:16"
    "edoburu/pgbouncer:latest"
    "minio/minio:latest"
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
  info "  Building + saving zaychik..."
  docker build -t provisa/zaychik:local "${REPO_ROOT}/zaychik"
  docker save provisa/zaychik:local | gzip -9 > "${IMAGES_DIR}/zaychik-local.tar.gz"
  ok "  Saved zaychik."
}

# ── Build AppDir ───────────────────────────────────────────────────────────────
build_appdir() {
  info "Building AppDir..."
  rm -rf "$APPDIR"
  mkdir -p "${APPDIR}/images" "${APPDIR}/compose" "${APPDIR}/bin"

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

  # Copy image tarballs — exclude provisa-local.tar.gz (built at first-launch)
  for f in "${IMAGES_DIR}"/*.tar.gz; do
    [[ "$(basename "$f")" == "provisa-local.tar.gz" ]] && continue
    cp "$f" "${APPDIR}/images/"
  done

  # Copy compose files and config
  cp "${REPO_ROOT}/docker-compose.yml"       "${APPDIR}/compose/"
  cp "${REPO_ROOT}/docker-compose.prod.yml"  "${APPDIR}/compose/"
  cp "${REPO_ROOT}/docker-compose.airgap.yml" "${APPDIR}/compose/"
  cp -r "${REPO_ROOT}/config"                "${APPDIR}/compose/config"
  cp -r "${REPO_ROOT}/db"                    "${APPDIR}/compose/db"
  cp -r "${REPO_ROOT}/trino"                 "${APPDIR}/compose/trino"

  # Bundle provisa source for first-launch image build
  mkdir -p "${APPDIR}/provisa-source"
  cp "${REPO_ROOT}/Dockerfile"    "${APPDIR}/provisa-source/"
  cp "${REPO_ROOT}/main.py"        "${APPDIR}/provisa-source/"
  cp "${REPO_ROOT}/pyproject.toml" "${APPDIR}/provisa-source/"
  cp -r "${REPO_ROOT}/provisa"    "${APPDIR}/provisa-source/provisa"
  # Embed pre-built wheels so Dockerfile pip install needs no network
  local wheels_src="${SCRIPT_DIR}/tmp-provisa-wheels"
  if [ ! -d "$wheels_src" ] || [ -z "$(ls -A "$wheels_src" 2>/dev/null)" ]; then
    err "No wheels found in ${wheels_src} — run build_provisa_wheels() first."
    exit 1
  fi
  cp -r "$wheels_src" "${APPDIR}/provisa-source/wheels"

  # Copy CLI and launch scripts
  cp "${REPO_ROOT}/scripts/provisa"         "${APPDIR}/provisa-cli"
  chmod +x "${APPDIR}/provisa-cli"
  cp "${SCRIPT_DIR}/first-launch.sh"        "${APPDIR}/first-launch.sh"
  chmod +x "${APPDIR}/first-launch.sh"
  cp "${SCRIPT_DIR}/AppRun"                 "${APPDIR}/AppRun"
  chmod +x "${APPDIR}/AppRun"
  cp "${SCRIPT_DIR}/Provisa.desktop"        "${APPDIR}/Provisa.desktop"

  # Generate icon
  if command -v convert &>/dev/null; then
    convert -size 256x256 xc:'#1a1a2e' \
      -fill white -gravity Center -pointsize 80 -annotate 0 'P' \
      "${APPDIR}/Provisa.png"
  else
    touch "${APPDIR}/Provisa.png"
  fi

  ok "AppDir built at ${APPDIR}"
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
  bundle_docker
  build_provisa_wheels
  save_images
  build_appdir
  create_appimage

  printf "\n${GREEN}${BOLD}Build complete.${NC}\n"
  printf "AppImage: %s\n" "${OUT_DIR}/Provisa.AppImage"
}

main "$@"
