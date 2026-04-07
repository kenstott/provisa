#!/usr/bin/env bash
# Phase AF2a — Build airgapped macOS DMG with Lima + containerd.
# Requires: docker (build host only), hdiutil, codesign, xcrun, python3
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
OUT_DIR="${SCRIPT_DIR}/dist"
APP_BUNDLE="${SCRIPT_DIR}/Provisa.app"
IMAGES_DIR="${SCRIPT_DIR}/images"
BIN_DIR="${APP_BUNDLE}/Contents/MacOS/bin"
DMG_NAME="Provisa.dmg"
DMG_PATH="${OUT_DIR}/${DMG_NAME}"

# Lima + containerd versions
LIMA_VERSION="2.1.1"
CONTAINERD_VERSION="2.2.2"

# Service images from docker-compose (use digest-pinning in production)
IMAGES=(
  "postgres:16"
  "edoburu/pgbouncer:latest"
  "minio/minio:latest"
  "redis:7-alpine"
  "trinodb/trino:480"
)

RED='\033[0;31m'; GREEN='\033[0;32m'; CYAN='\033[0;36m'; BOLD='\033[1m'; NC='\033[0m'
info() { printf "${CYAN}[build-dmg]${NC} %s\n" "$*"; }
ok()   { printf "${GREEN}[build-dmg]${NC} %s\n" "$*"; }
err()  { printf "${RED}[build-dmg]${NC} %s\n" "$*" >&2; }

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

  curl -fsSL "${base_url}/${arm64_tar}" -o "${tmp}/lima-arm64.tar.gz"
  tar -xzf "${tmp}/lima-arm64.tar.gz" -C "${tmp}/arm64" --strip-components=1
  curl -fsSL "${base_url}/${x86_tar}" -o "${tmp}/lima-x86_64.tar.gz"
  tar -xzf "${tmp}/lima-x86_64.tar.gz" -C "${tmp}/x86_64" --strip-components=1

  for arch in arm64 x86_64; do
    cp "${tmp}/${arch}/bin/limactl" "${BIN_DIR}/${arch}/limactl"
    chmod +x "${BIN_DIR}/${arch}/limactl"
  done
  rm -rf "$tmp"
  ok "Lima binaries downloaded."
}

# ── Download containerd binaries ──────────────────────────────────────────────
download_containerd() {
  info "Downloading containerd ${CONTAINERD_VERSION}..."
  local base_url="https://github.com/containerd/containerd/releases/download/v${CONTAINERD_VERSION}"
  local tmp="${SCRIPT_DIR}/tmp-containerd"
  mkdir -p "$tmp"

  for arch in arm64 x86_64; do
    local carch="$arch"
    [ "$arch" = "x86_64" ] && carch="amd64"
    local tar_name="containerd-${CONTAINERD_VERSION}-linux-${carch}.tar.gz"
    curl -fsSL "${base_url}/${tar_name}" -o "${tmp}/containerd-${arch}.tar.gz"
    mkdir -p "${tmp}/${arch}"
    tar -xzf "${tmp}/containerd-${arch}.tar.gz" -C "${tmp}/${arch}"
    cp "${tmp}/${arch}/bin/ctr" "${BIN_DIR}/${arch}/ctr"
    chmod +x "${BIN_DIR}/${arch}/ctr"
  done
  rm -rf "$tmp"
  ok "containerd binaries downloaded."
}

# ── Save service images as tarballs ──────────────────────────────────────────
save_images() {
  mkdir -p "$IMAGES_DIR"
  local count
  count=$(ls "${IMAGES_DIR}"/*.tar 2>/dev/null | wc -l | tr -d ' ')
  if [ "$count" -ge 6 ]; then
    info "Images pre-populated (${count} tarballs) — skipping docker pull."
    return
  fi
  if ! command -v docker &>/dev/null; then
    err "docker not found and images not pre-populated in ${IMAGES_DIR}"
    exit 1
  fi
  info "Saving service images..."
  for img in "${IMAGES[@]}"; do
    local tag="${img##*/}"
    tag="${tag//:/-}"
    tag="${tag//\//-}"
    local out="${IMAGES_DIR}/${tag}.tar"
    if [ -f "$out" ]; then
      info "  Skipping (cached): ${img}"
      continue
    fi
    info "  Pulling + saving: ${img}"
    docker pull "$img"
    docker save "$img" -o "$out"
    ok "  Saved: ${out}"
  done
  # Build and save zaychik (custom image)
  info "  Building + saving zaychik..."
  docker build -t provisa/zaychik:local "${REPO_ROOT}/zaychik"
  docker save provisa/zaychik:local -o "${IMAGES_DIR}/zaychik-local.tar"
  ok "  Saved zaychik."
}

# ── Embed compose files and config ───────────────────────────────────────────
embed_compose() {
  local res="${APP_BUNDLE}/Contents/Resources"
  mkdir -p "$res"
  cp "${REPO_ROOT}/docker-compose.yml" "${res}/docker-compose.yml"
  cp "${REPO_ROOT}/docker-compose.prod.yml" "${res}/docker-compose.prod.yml"
  cp -r "${REPO_ROOT}/config" "${res}/config"
  cp -r "${REPO_ROOT}/db" "${res}/db"
  cp -r "${REPO_ROOT}/trino" "${res}/trino"
  cp "${REPO_ROOT}/scripts/provisa" "${res}/provisa-cli"
  chmod +x "${res}/provisa-cli"
  ok "Compose files and config embedded."
}

# ── Copy first-launch + launcher scripts ─────────────────────────────────────
embed_scripts() {
  cp "${SCRIPT_DIR}/first-launch.sh" "${APP_BUNDLE}/Contents/MacOS/first-launch.sh"
  chmod +x "${APP_BUNDLE}/Contents/MacOS/first-launch.sh"
  chmod +x "${APP_BUNDLE}/Contents/MacOS/provisa-launcher"
  ok "Scripts embedded."
}

# ── Code signing ──────────────────────────────────────────────────────────────
sign_app() {
  if [ -z "${APPLE_DEVELOPER_ID:-}" ]; then
    info "APPLE_DEVELOPER_ID not set — skipping signing."
    return
  fi
  info "Signing app bundle..."
  codesign --deep --force --verify --verbose \
    --sign "${APPLE_DEVELOPER_ID}" \
    --options runtime \
    --entitlements "${SCRIPT_DIR}/entitlements.plist" \
    "${APP_BUNDLE}"
  ok "App bundle signed."
}

# ── Notarization ──────────────────────────────────────────────────────────────
notarize_dmg() {
  if [ -z "${APPLE_NOTARYTOOL_APPLE_ID:-}" ]; then
    info "APPLE_NOTARYTOOL_APPLE_ID not set — skipping notarization."
    return
  fi
  info "Submitting DMG for notarization..."
  xcrun notarytool submit "${DMG_PATH}" \
    --apple-id "${APPLE_NOTARYTOOL_APPLE_ID}" \
    --password "${APPLE_NOTARYTOOL_PASSWORD}" \
    --team-id "${APPLE_NOTARYTOOL_TEAM_ID}" \
    --wait
  xcrun stapler staple "${DMG_PATH}"
  ok "DMG notarized and stapled."
}

# ── Create DMG ────────────────────────────────────────────────────────────────
create_dmg() {
  info "Creating DMG..."
  mkdir -p "$OUT_DIR"
  local tmp_dmg="${OUT_DIR}/tmp-provisa"
  rm -rf "$tmp_dmg"
  mkdir -p "$tmp_dmg"
  cp -r "${APP_BUNDLE}" "${tmp_dmg}/Provisa.app"

  # Remove any existing DMG so create-dmg doesn't complain
  rm -f "${DMG_PATH}"

  create-dmg \
    --volname "Provisa" \
    --volicon "${SCRIPT_DIR}/Provisa.icns" \
    --background "${SCRIPT_DIR}/dmg-background.png" \
    --window-pos 200 120 \
    --window-size 660 400 \
    --icon-size 128 \
    --icon "Provisa.app" 165 185 \
    --hide-extension "Provisa.app" \
    --app-drop-link 495 185 \
    "${DMG_PATH}" \
    "${tmp_dmg}/"

  rm -rf "$tmp_dmg"
  ok "DMG created: ${DMG_PATH}"
}

# ── Main ──────────────────────────────────────────────────────────────────────
main() {
  printf "\n${BOLD}Provisa DMG Builder — Phase AF2a${NC}\n"
  printf "═══════════════════════════════════════════\n\n"

  check_prereqs
  mkdir -p "${BIN_DIR}/arm64" "${BIN_DIR}/x86_64"

  generate_assets
  download_lima
  download_containerd
  save_images

  # Copy images into app bundle
  mkdir -p "${APP_BUNDLE}/Contents/Resources/images"
  cp "${IMAGES_DIR}"/*.tar "${APP_BUNDLE}/Contents/Resources/images/"

  embed_compose
  embed_scripts
  sign_app
  create_dmg
  notarize_dmg

  printf "\n${GREEN}${BOLD}Build complete.${NC}\n"
  printf "DMG: %s\n" "${DMG_PATH}"
}

main "$@"
