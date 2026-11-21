#!/usr/bin/env bash
# Build Provisa Observability DMG — loads obs images into the user's Docker, writes extension compose file.
# Requires: docker (build host), hdiutil, codesign, python3
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUT_DIR="${SCRIPT_DIR}/dist"
IMAGES_DIR="${SCRIPT_DIR}/obs-images"
DMG_NAME="Provisa-Obs.dmg"
DMG_PATH="${OUT_DIR}/${DMG_NAME}"

OBS_IMAGES=(
  # Docker Hub's minio/minio repo now denies anonymous pulls ("pull access denied ... may
  # require 'docker login'") — verified live, and CI's "UI e2e (Trino lane)" has failed on this
  # exact error since at least 2026-09-12. quay.io/minio/minio is MinIO's own current public
  # distribution (verified pullable).
  "quay.io/minio/minio:latest"
  "ghcr.io/smithclay/otlp2parquet:0.12.0"
  "otel/opentelemetry-collector-contrib:0.99.0"
  "prom/prometheus:v2.51.2"
  "grafana/tempo:2.4.1"
  "grafana/grafana:10.4.2"
)

RED='\033[0;31m'; GREEN='\033[0;32m'; CYAN='\033[0;36m'; BOLD='\033[1m'; NC='\033[0m'
info() { printf "${CYAN}[build-dmg-obs]${NC} %s\n" "$*"; }
ok()   { printf "${GREEN}[build-dmg-obs]${NC} %s\n" "$*"; }
err()  { printf "${RED}[build-dmg-obs]${NC} %s\n" "$*" >&2; }

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

# codesign's --timestamp flag contacts Apple's secure timestamp server, and that wait has no
# bound of its own: on v0.1.0-alpha.370 the first call blocked for 39 minutes until the job's
# 45-minute limit cancelled the build, leaving an orphan codesign process and no release. The
# timestamp is required for notarization, so the answer is not to drop it but to cap how long a
# single attempt may wait and try again. macOS ships no coreutils `timeout`, hence the watchdog.
CODESIGN_TIMEOUT="${CODESIGN_TIMEOUT:-180}"
# `hdiutil create` on GitHub's macOS runners fails intermittently with "Resource busy": the
# runner's XProtectBehaviorService (macOS 13+) holds the image hdiutil is writing. The runner-images
# maintainers' documented workaround is to retry (actions/runner-images#7522); only that failure is
# retried, a bounded number of times, and any other is raised at once.
hdiutil_create_retry() {
  local attempt out
  for attempt in 1 2 3 4 5; do
    if out=$(hdiutil create "$@" 2>&1); then
      printf '%s\n' "$out"
      return 0
    fi
    printf '%s\n' "$out" >&2
    if ! printf '%s' "$out" | grep -q "Resource busy"; then
      err "hdiutil create failed: $*"
      exit 1
    fi
    info "hdiutil create attempt ${attempt}: resource busy (runner XProtect), retrying in 15s..."
    sleep 15
  done
  err "hdiutil create still resource busy after 5 attempts: $*"
  exit 1
}

codesign_retry() {
  local attempt pid waited
  for attempt in 1 2 3 4 5; do
    codesign "$@" &
    pid=$!
    waited=0
    while kill -0 "$pid" 2>/dev/null && [ "$waited" -lt "$CODESIGN_TIMEOUT" ]; do
      sleep 2
      waited=$((waited + 2))
    done
    if kill -0 "$pid" 2>/dev/null; then
      kill -9 "$pid" 2>/dev/null || true
      wait "$pid" 2>/dev/null || true
      info "codesign attempt ${attempt} exceeded ${CODESIGN_TIMEOUT}s (timestamp server unresponsive), retrying..."
      continue
    fi
    if wait "$pid"; then
      return 0
    fi
    info "codesign attempt ${attempt} failed, retrying in 10s..."
    sleep 10
  done
  err "codesign failed after 5 attempts: $*"
  exit 1
}

check_prereqs() {
  for cmd in curl hdiutil codesign; do
    if ! command -v "$cmd" &>/dev/null; then
      err "Required tool not found: ${cmd}"
      exit 1
    fi
  done
  if ! command -v create-dmg &>/dev/null; then
    info "Installing create-dmg..."
    brew install create-dmg --quiet
  fi
  ok "Prerequisites satisfied."
}

save_images() {
  mkdir -p "$IMAGES_DIR"
  local count
  count=$(find "$IMAGES_DIR" -maxdepth 1 -name '*.tar.gz' | wc -l | tr -d ' ')
  if [ "$count" -ge "${#OBS_IMAGES[@]}" ]; then
    info "Obs images pre-populated (${count} tarballs) — skipping docker pull."
    return
  fi
  if ! command -v docker &>/dev/null; then
    err "docker not found and images not pre-populated in ${IMAGES_DIR}"
    exit 1
  fi
  info "Saving obs images (gzip compressed, linux/arm64)..."
  for img in "${OBS_IMAGES[@]}"; do
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
}

# Writes install-obs.sh into a staging directory
write_installer_script() {
  local dest="$1"
  cat > "${dest}/install-obs.sh" <<'INSTALLER'
#!/usr/bin/env bash
# Provisa Observability Extension Installer
# Loads obs images into the user's Docker (docker load) and writes the extension compose file.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROVISA_HOME="${PROVISA_INSTALL_DIR:-${HOME}/.provisa}"
EXT_DIR="${PROVISA_HOME}/extensions/observability"
EXT_COMPOSE="${EXT_DIR}/docker-compose.observability.yml"

RED='\033[0;31m'; GREEN='\033[0;32m'; CYAN='\033[0;36m'; NC='\033[0m'
info() { printf "${CYAN}[provisa-obs]${NC} %s\n" "$*"; }
ok()   { printf "${GREEN}[provisa-obs]${NC} %s\n" "$*"; }
err()  { printf "${RED}[provisa-obs]${NC} %s\n" "$*" >&2; }

check_core() {
  if [ ! -f "${PROVISA_HOME}/config.yaml" ]; then
    err "Provisa Core is not installed. Install Provisa.dmg first."
    exit 1
  fi
}

require_docker() {
  if ! command -v docker >/dev/null 2>&1; then
    err "Docker is required for the observability extension."
    err "Install Docker Desktop or colima, start it, then run this installer again."
    exit 1
  fi
  if ! docker info >/dev/null 2>&1; then
    err "Docker is installed but not running. Start Docker Desktop (or colima) and run this installer again."
    exit 1
  fi
}

import_images() {
  info "Loading observability images into Docker (no network required)..."
  local count=0
  for gz_file in "${SCRIPT_DIR}/images"/*.tar.gz; do
    [ -f "$gz_file" ] || continue
    info "  docker load $(basename "$gz_file")"
    docker load -i "$gz_file" >/dev/null
    count=$((count + 1))
  done
  ok "Loaded ${count} images."
}

write_extension() {
  mkdir -p "$EXT_DIR"
  local compose_src="${PROVISA_HOME}/compose"
  # Extension compose file references the obs compose from core's staged compose dir
  if [ -f "${compose_src}/docker-compose.observability.yml" ]; then
    cp "${compose_src}/docker-compose.observability.yml" "$EXT_COMPOSE"
    ok "Extension compose file written: ${EXT_COMPOSE}"
  else
    # Fallback: write a minimal reference compose (obs configs are in compose_src/observability/)
    err "docker-compose.observability.yml not found in ${compose_src}."
    err "Reinstall Provisa Core, then retry."
    exit 1
  fi
}

main() {
  printf "\nProvisa Observability Extension Installer\n"
  printf "═══════════════════════════════════════════\n\n"
  check_core
  require_docker
  import_images
  write_extension
  ok "Observability installed."
  printf "\nRestart Provisa to activate the observability stack.\n"
  printf "Grafana: http://localhost:3100  |  Prometheus: http://localhost:9090\n\n"
}

main "$@"
INSTALLER
  chmod +x "${dest}/install-obs.sh"
}

sign_script() {
  local script="$1"
  if [ -z "${APPLE_DEVELOPER_ID:-}" ]; then
    info "APPLE_DEVELOPER_ID not set — skipping signing."
    return
  fi
  codesign_retry --force --sign "${APPLE_DEVELOPER_ID}" --options runtime --timestamp "$script"
  ok "Signed: $(basename "$script")"
}

create_dmg() {
  info "Creating obs DMG..."
  mkdir -p "$OUT_DIR"
  local tmp_dmg="${OUT_DIR}/tmp-provisa-obs"
  rm -rf "$tmp_dmg"
  mkdir -p "${tmp_dmg}/images"

  write_installer_script "$tmp_dmg"
  sign_script "${tmp_dmg}/install-obs.sh"

  for f in "${IMAGES_DIR}"/*.tar.gz; do
    cp "$f" "${tmp_dmg}/images/"
  done
  chflags hidden "${tmp_dmg}/images"

  rm -f "${DMG_PATH}"
  hdiutil_create_retry -volname "Provisa Observability" \
    -srcfolder "$tmp_dmg" \
    -ov -format UDZO \
    "$DMG_PATH"

  rm -rf "$tmp_dmg"
  ok "DMG created: ${DMG_PATH}"
}

main() {
  printf '\n%bProvisa Obs DMG Builder%b\n' "$BOLD" "$NC"
  printf "═══════════════════════════════════════════\n\n"
  check_prereqs
  save_images
  create_dmg
  printf '\n%b%bBuild complete.%b\n' "$GREEN" "$BOLD" "$NC"
  printf "DMG: %s\n" "${DMG_PATH}"
}

main "$@"
