#!/usr/bin/env bash
# Build Provisa Observability DMG — loads obs images into Lima VM, writes extension compose file.
# Requires: docker (build host), hdiutil, codesign, python3
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
OUT_DIR="${SCRIPT_DIR}/dist"
IMAGES_DIR="${SCRIPT_DIR}/obs-images"
DMG_NAME="Provisa-Obs.dmg"
DMG_PATH="${OUT_DIR}/${DMG_NAME}"

OBS_IMAGES=(
  "minio/minio:latest"
  "ghcr.io/smithclay/otlp2parquet:latest"
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
  count=$(ls "${IMAGES_DIR}"/*.tar.gz 2>/dev/null | wc -l | tr -d ' ')
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
# Loads obs images into the Lima VM and writes the extension compose file.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROVISA_HOME="${PROVISA_INSTALL_DIR:-${HOME}/.provisa}"
LIMA_VM_NAME="provisa"
EXT_DIR="${PROVISA_HOME}/extensions/observability"
EXT_COMPOSE="${EXT_DIR}/docker-compose.observability.yml"

ARCH="$(uname -m)"
case "$ARCH" in
  arm64)  BIN_ARCH="arm64" ;;
  x86_64) BIN_ARCH="x86_64" ;;
  *) echo "Unsupported architecture: $ARCH" >&2; exit 1 ;;
esac
LIMACTL="${PROVISA_HOME}/bin/limactl"

RED='\033[0;31m'; GREEN='\033[0;32m'; CYAN='\033[0;36m'; NC='\033[0m'
info() { printf "${CYAN}[provisa-obs]${NC} %s\n" "$*"; }
ok()   { printf "${GREEN}[provisa-obs]${NC} %s\n" "$*"; }
err()  { printf "${RED}[provisa-obs]${NC} %s\n" "$*" >&2; }

check_core() {
  if [ ! -f "${PROVISA_HOME}/config.yaml" ]; then
    err "Provisa Core is not installed. Install Provisa.dmg first."
    exit 1
  fi
  if [ ! -x "$LIMACTL" ] && [ ! -L "$LIMACTL" ]; then
    err "limactl not found at ${LIMACTL}. Reinstall Provisa Core."
    exit 1
  fi
}

ensure_vm_running() {
  local state
  state="$("$LIMACTL" list --format '{{.Status}}' "$LIMA_VM_NAME" 2>/dev/null || echo "missing")"
  if [ "$state" = "missing" ]; then
    err "Provisa VM not found. Run Provisa.app first to complete first-launch setup."
    exit 1
  fi
  if [ "$state" != "Running" ]; then
    info "Starting Provisa VM..."
    "$LIMACTL" start --yes "$LIMA_VM_NAME"
    ok "VM started."
  fi
}

import_images() {
  info "Importing observability images into Provisa VM (no network required)..."
  local count=0
  for gz_file in "${SCRIPT_DIR}/images"/*.tar.gz; do
    [ -f "$gz_file" ] || continue
    local name
    name="$(basename "$gz_file")"
    info "  Importing: ${name}"
    gunzip -c "$gz_file" | \
      "$LIMACTL" shell "$LIMA_VM_NAME" -- \
        sudo ctr --namespace=default images import -
    count=$((count + 1))
  done
  ok "Imported ${count} images."
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
  ensure_vm_running
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
  codesign --force --sign "${APPLE_DEVELOPER_ID}" --options runtime --timestamp "$script"
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
  hdiutil create -volname "Provisa Observability" \
    -srcfolder "$tmp_dmg" \
    -ov -format UDZO \
    "$DMG_PATH"

  rm -rf "$tmp_dmg"
  ok "DMG created: ${DMG_PATH}"
}

notarize_dmg() {
  if [ -z "${APPLE_NOTARYTOOL_APPLE_ID:-}" ]; then
    info "APPLE_NOTARYTOOL_APPLE_ID not set — skipping notarization."
    return
  fi
  local notary_args=(
    --apple-id "${APPLE_NOTARYTOOL_APPLE_ID}"
    --password "${APPLE_NOTARYTOOL_PASSWORD}"
    --team-id  "${APPLE_NOTARYTOOL_TEAM_ID}"
  )
  info "Submitting DMG for notarization..."
  local submit_out submit_err submit_rc
  submit_out=$(xcrun notarytool submit "$DMG_PATH" "${notary_args[@]}" --output-format json 2>/tmp/notary-submit-err) \
    && submit_rc=0 || submit_rc=$?
  submit_err=$(cat /tmp/notary-submit-err 2>/dev/null || true)
  rm -f /tmp/notary-submit-err
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
  local max_polls=30 poll=0 status=""
  while [ $poll -lt $max_polls ]; do
    status=$(xcrun notarytool info "$submission_id" "${notary_args[@]}" \
               --output-format json 2>/dev/null \
             | python3 -c "import sys,json; print(json.load(sys.stdin).get('status',''))" \
             2>/dev/null || echo "network-error")
    case "$status" in
      Accepted) ok "Notarization accepted."; break ;;
      Invalid|Rejected)
        err "Notarization ${status}:"
        xcrun notarytool log "$submission_id" "${notary_args[@]}" >&2 || true
        exit 1 ;;
      *) info "  Status: ${status:-unknown} (poll $((poll+1))/${max_polls})" ;;
    esac
    sleep 40
    poll=$((poll + 1))
  done
  [ "$status" = "Accepted" ] || { err "Notarization timed out."; exit 1; }
  xcrun stapler staple "$DMG_PATH"
  ok "DMG notarized and stapled."
}

main() {
  printf "\n${BOLD}Provisa Obs DMG Builder${NC}\n"
  printf "═══════════════════════════════════════════\n\n"
  check_prereqs
  save_images
  create_dmg
  notarize_dmg
  printf "\n${GREEN}${BOLD}Build complete.${NC}\n"
  printf "DMG: %s\n" "${DMG_PATH}"
}

main "$@"
