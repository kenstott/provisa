#!/usr/bin/env bash
# First-launch setup. Native tier: build a Python venv from the bundled interpreter
# + wheelhouse (online: PyPI; airgapped: bundled wheels). Docker tier: bring up the
# stack on the user's own Docker (Docker Desktop / colima) — no VM.
# Called by provisa-launcher on first run only.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUNDLE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RESOURCES="${BUNDLE_DIR}/Resources"
IMAGES_DIR="${RESOURCES}/images"
PROVISA_HOME="${PROVISA_INSTALL_DIR:-${HOME}/.provisa}"
SENTINEL="${PROVISA_HOME}/.first-launch-complete"

ARCH="$(uname -m)"
case "$ARCH" in
  arm64)  BIN_ARCH="arm64" ;;
  x86_64) BIN_ARCH="x86_64" ;;
  *)
    printf "[provisa] Unsupported architecture: %s\n" "$ARCH" >&2
    exit 1
    ;;
esac

# Release version baked into the bundle (Resources/VERSION), used to pin the online
# native pip install to the matching release.
PROVISA_VERSION="${PROVISA_VERSION:-$(cat "${RESOURCES}/VERSION" 2>/dev/null || true)}"

RED='\033[0;31m'; GREEN='\033[0;32m'; CYAN='\033[0;36m'; BOLD='\033[1m'; DIM='\033[2m'; NC='\033[0m'
info() { printf "${CYAN}[provisa]${NC} %s\n" "$*"; }
ok()   { printf "${GREEN}[provisa]${NC} %s\n" "$*"; }
err()  { printf "${RED}[provisa]${NC} %s\n" "$*" >&2; }
# macOS ships /bin/bash 3.2 (ScriptRunner invokes /bin/bash), which lacks the
# ${var,,} lowercase expansion — use this helper instead.
_lc() { printf '%s' "$1" | tr '[:upper:]' '[:lower:]'; }
# ── Copy images into provisa home for VM access ───────────────────────────────
stage_images() {
  local staged="${PROVISA_HOME}/images"
  if [ -d "$staged" ] && [ "$(ls -A "$staged" 2>/dev/null)" ]; then
    return 0
  fi
  mkdir -p "$staged"

  # 1. Sibling .images next to the .app (running directly from mounted DMG)
  local bundle_parent
  bundle_parent="$(dirname "$BUNDLE_DIR")"
  local src=""
  for candidate in "${bundle_parent}/images" "${bundle_parent}/.images"; do
    if [ -d "$candidate" ] && ls "$candidate"/*.tar.gz &>/dev/null 2>&1; then
      src="$candidate"
      break
    fi
  done

  # 2. Scan mounted DMG volumes (user dragged .app to Applications but DMG still open)
  if [ -z "$src" ]; then
    for vol_images in /Volumes/*/images /Volumes/*/.images; do
      if [ -d "$vol_images" ] && ls "$vol_images"/*.tar.gz &>/dev/null 2>&1; then
        src="$vol_images"
        break
      fi
    done
  fi

  # 3. Fallback: images embedded inside the bundle (not typical in Option C)
  if [ -z "$src" ] && [ -d "$IMAGES_DIR" ] && ls "$IMAGES_DIR"/*.tar.gz &>/dev/null 2>&1; then
    src="$IMAGES_DIR"
  fi

  if [ -z "$src" ]; then
    # Slim base: core images aren't bundled in the DMG. Acquire the core-images add-on
    # (local-first beside the installer for airgap, else download). This is the Trino/
    # Docker tier's image source; the native tier never reaches here.
    local version="${PROVISA_VERSION:-}"
    if [ -z "$version" ] && command -v provisa &>/dev/null; then
      version="$(provisa version 2>/dev/null | head -1 | awk '{print $NF}')" || version=""
    fi
    acquire_addon "Core images" "provisa-core-images-${version}.tar.gz" "$staged" "y"
    if [ ! "$(ls -A "$staged" 2>/dev/null)" ]; then
      err "Core images unavailable. Place provisa-core-images-*.tar.gz beside the installer and re-run."
      exit 1
    fi
    return 0
  fi

  info "Staging images to ${staged}..."
  cp "$src"/*.tar.gz "$staged/"
}
# ── Stage Trino plugins to ~/.provisa/trino/plugins/ ────────────────────────
# Plugins ship as a separate release asset (provisa-trino-plugins-*.tar.gz).
# Extract it and place at ~/.provisa/trino/plugins/ to enable Trino connectors.
stage_trino_plugins() {
  local staged="${PROVISA_HOME}/trino/plugins"
  if [ -d "$staged" ] && [ "$(ls -A "$staged" 2>/dev/null)" ]; then
    return 0
  fi
  info "Trino plugins not present — download provisa-trino-plugins-*.tar.gz from the release"
  info "and extract to ${staged} to enable SharePoint, Splunk, and File connectors."
}

# ── Self-install to /Applications when running from DMG ──────────────────────
install_to_applications() {
  local app_dst="/Applications/Provisa.app"
  # Only auto-install when launched directly from a mounted DMG volume
  if [[ "$BUNDLE_DIR" != /Volumes/* ]]; then
    return 0
  fi
  if [ -d "$app_dst" ]; then
    info "Updating existing installation at ${app_dst}..."
    rm -rf "$app_dst"
  else
    info "Installing Provisa to /Applications..."
  fi
  if cp -rp "$BUNDLE_DIR" "$app_dst" 2>/dev/null; then
    ok "Installed to /Applications/Provisa.app"
  else
    osascript -e "do shell script \"cp -rp '$BUNDLE_DIR' '$app_dst'\" with administrator privileges"
    ok "Installed to /Applications/Provisa.app"
  fi
}

# ── Prompt for hostname ───────────────────────────────────────────────────────
ask_hostname() {
  local default="localhost"
  local hostname=""

  hostname=$(osascript <<APPLESCRIPT 2>/dev/null
    set defaultHost to "localhost"
    set result to display dialog "What hostname should Provisa use?" ¬
      default answer defaultHost ¬
      with title "Provisa Setup" ¬
      buttons {"Cancel", "Continue"} ¬
      default button "Continue"
    return text returned of result
APPLESCRIPT
  ) || hostname="$default"

  hostname="${hostname//[[:space:]]/}"
  [ -z "$hostname" ] && hostname="$default"
  echo "$hostname"
}

# ── Check if a TCP port is free on the host ──────────────────────────────────
check_port_free() {
  local port="$1"
  ! lsof -iTCP:"$port" -sTCP:LISTEN -t &>/dev/null
}

# ── Generic port prompt with conflict check ───────────────────────────────────
_ask_port() {
  local label="$1"
  local default="$2"
  local dialog_text="$3"
  local port=""

  port=$(osascript <<APPLESCRIPT 2>/dev/null
    set result to display dialog "${dialog_text}" ¬
      default answer "${default}" ¬
      with title "Provisa Setup" ¬
      buttons {"Cancel", "Continue"} ¬
      default button "Continue"
    return text returned of result
APPLESCRIPT
  ) || port="$default"

  port="${port//[[:space:]]/}"

  if ! [[ "$port" =~ ^[0-9]+$ ]] || [ "$port" -lt 1024 ] || [ "$port" -gt 65535 ]; then
    info "Invalid port '${port}' — defaulting to ${default}."
    port="$default"
  fi

  if ! check_port_free "$port"; then
    info "Port ${port} is already in use — defaulting to ${default}."
    if ! check_port_free "$default"; then
      err "Default ${label} port ${default} is also in use. Edit ~/.provisa/config.yaml after setup."
    fi
    port="$default"
  fi

  echo "$port"
}

# ── Prompt for UI port ────────────────────────────────────────────────────────
ask_ui_port() {
  _ask_port "UI" "3000" "Which port should the Provisa web UI listen on?"
}

# ── Prompt for API port ───────────────────────────────────────────────────────
ask_api_port() {
  _ask_port "API" "8000" "Which port should the Provisa API listen on?"
}

# ── Prompt for Flight port ────────────────────────────────────────────────────
ask_flight_port() {
  _ask_port "Flight" "8815" "Which port should the Provisa Arrow Flight server listen on?"
}

# ── Stage provisa source for first-launch image build ─────────────────────────
stage_provisa_source() {
  local dest="${PROVISA_HOME}/provisa-source"
  if [ -d "$dest" ] && [ -f "${dest}/Dockerfile" ]; then
    return 0
  fi
  mkdir -p "$dest"
  local src="${RESOURCES}/provisa-source"
  if [ ! -d "$src" ]; then
    err "provisa-source not found in bundle. Reinstall Provisa."
    exit 1
  fi
  cp -r "$src"/. "$dest/"
  ok "Provisa source staged to ${dest}"
}
# ── Stage compose files into ~/.provisa/compose/ ─────────────────────────────
# The Docker tier runs `docker compose -f ~/.provisa/compose/...` on the user's
# own Docker; the compose files + observability/ config live here (project_dir).
stage_compose() {
  local dest="${PROVISA_HOME}/compose"
  mkdir -p "$dest"
  # Always overwrite compose YAMLs — never allow stale files from a prior install
  for f in \
    docker-compose.core.yml \
    docker-compose.app.yml \
    docker-compose.airgap.yml; do
    if [ -f "${RESOURCES}/${f}" ]; then
      cp "${RESOURCES}/${f}" "${dest}/${f}"
    fi
  done
  # Stage support dirs only if not present (preserve user edits)
  for d in config db trino observability; do
    if [ ! -d "${dest}/${d}" ] && [ -d "${RESOURCES}/${d}" ]; then
      cp -r "${RESOURCES}/${d}" "${dest}/${d}"
    fi
  done
  # dest_otel used below for OTel jar detection
  local dest_otel="${dest}/observability/trino-otel"
  mkdir -p "$dest_otel"
  # Write TRINO_JAVA_TOOL_OPTIONS to compose .env when the jar is present.
  # docker-compose.yml passes this to Trino: JAVA_TOOL_OPTIONS: "${TRINO_JAVA_TOOL_OPTIONS:-}"
  local env_file="${dest}/.env"
  local otel_jar="${dest_otel}/opentelemetry-javaagent.jar"
  if [ -f "$otel_jar" ]; then
    if ! grep -q "TRINO_JAVA_TOOL_OPTIONS" "$env_file" 2>/dev/null; then
      printf '\nTRINO_JAVA_TOOL_OPTIONS=-javaagent:/etc/trino/otel/opentelemetry-javaagent.jar -Dotel.service.name=trino\n' \
        >> "$env_file"
    fi
  fi

  # Write port variables so docker-compose.prod.yml can substitute them.
  # These are read from config.yaml (written before stage_compose is called).
  local cfg="${PROVISA_HOME}/config.yaml"
  if [ -f "$cfg" ]; then
    local _api _ui _flight
    _api="$(grep -E '^api_port:' "$cfg" | awk '{print $2}' | tr -d '[:space:]')"
    _ui="$(grep -E '^ui_port:' "$cfg" | awk '{print $2}' | tr -d '[:space:]')"
    _flight="$(grep -E '^flight_port:' "$cfg" | awk '{print $2}' | tr -d '[:space:]')"
    [ -n "$_api" ]    && { grep -q "^API_PORT="    "$env_file" 2>/dev/null || printf 'API_PORT=%s\n'    "$_api"    >> "$env_file"; }
    [ -n "$_ui" ]     && { grep -q "^UI_PORT="     "$env_file" 2>/dev/null || printf 'UI_PORT=%s\n'     "$_ui"     >> "$env_file"; }
    [ -n "$_flight" ] && { grep -q "^FLIGHT_PORT=" "$env_file" 2>/dev/null || printf 'FLIGHT_PORT=%s\n' "$_flight" >> "$env_file"; }
  fi

  ok "Compose files staged to ${dest}"
}

# ── Write config ──────────────────────────────────────────────────────────────
write_config() {
  if [ -f "${PROVISA_HOME}/config.yaml" ]; then
    return
  fi
  mkdir -p "$PROVISA_HOME"

  local hostname ui_port api_port flight_port

  # Non-interactive mode: read from env vars (set by SwiftUI wizard)
  if [[ -n "${PROVISA_NONINTERACTIVE:-}" ]]; then
    hostname="${PROVISA_HOSTNAME:-localhost}"
    ui_port="${PROVISA_UI_PORT:-3000}"
    api_port="${PROVISA_API_PORT:-8000}"
    flight_port="${PROVISA_FLIGHT_PORT:-8815}"
  else
    hostname="$(ask_hostname)"
    ui_port="$(ask_ui_port)"
    api_port="$(ask_api_port)"
    flight_port="$(ask_flight_port)"
  fi

  info "Hostname: ${hostname}  |  UI: ${ui_port}  |  API: ${api_port}  |  Flight: ${flight_port}"

  # Deployment fields (resolve_deployment ran first). Native tier runs the venv
  # directly; the Docker tier runs docker compose on the user's own Docker.
  # image_source records how the Docker tier's images were obtained: `build`
  # (host `docker compose build`) or `tarball` (airgapped `docker load` — the CLI
  # then adds docker-compose.airgap.yml so build: services resolve to loaded tags).
  local runtime demo_flag
  if [ "${NEEDS_DOCKER:-false}" = false ]; then
    runtime="native"
  else
    runtime="docker"
  fi
  [ "${INSTALL_DEMO:-n}" = "y" ] || [ "${INSTALL_DEMO:-n}" = "Y" ] && demo_flag=true || demo_flag=false

  cat > "${PROVISA_HOME}/config.yaml" <<YAML
# Provisa configuration — generated by installer
# project_dir holds the compose files for the Docker tier (docker compose -f ...).
project_dir: "${PROVISA_HOME}/compose"
hostname: ${hostname}
ui_port: ${ui_port}
api_port: ${api_port}
flight_port: ${flight_port}
auto_open_browser: true
runtime: ${runtime}
image_source: ${IMAGE_SOURCE:-build}
federation_workers: ${FED_WORKERS:-0}
# Deployment (REQ-972..979): the CLI starts the app with this engine env; when demo
# is true it opens the UI at ?tour=1 to auto-start the guided tour.
engine: ${DEPLOY_ENGINE:-duckdb}
engine_url: "${PROVISA_ENGINE_URL:-}"
materialize_url: "${PROVISA_MATERIALIZE_URL:-}"
trino_host: "${PROVISA_TRINO_HOST:-}"
trino_port: "${PROVISA_TRINO_PORT:-}"
obs_mode: ${OBS_MODE:-none}
otlp_endpoint: "${PROVISA_OTLP_ENDPOINT:-}"
demo: ${demo_flag}
YAML
  ok "Config written to ${PROVISA_HOME}/config.yaml"
}

# ── Resolve the deployment the wizard chose into install globals ─────────────
# Reads wizard env (SwiftUI wizard / non-interactive install). Everything defaults
# to the self-contained NATIVE tier; Docker is provisioned only when a choice needs it.
#   PROVISA_ENGINE       duckdb (default, native) | trino (Docker) | <external engine key>
#   PROVISA_OBS_MODE     none (default) | docker (bundled collector+prometheus+grafana
#                        integration demo) | collector (redirect OTLP to an existing collector)
#   PROVISA_INSTALL_DEMO n (default) | y
#   PROVISA_DEMO_MODE    native (default — host mock servers) | docker
# Sets globals: DEPLOY_ENGINE OBS_MODE INSTALL_DEMO DEMO_MODE NEEDS_DOCKER
resolve_deployment() {
  DEPLOY_ENGINE="${PROVISA_ENGINE:-duckdb}"
  OBS_MODE="${PROVISA_OBS_MODE:-none}"
  INSTALL_DEMO="${PROVISA_INSTALL_DEMO:-n}"
  DEMO_MODE="${PROVISA_DEMO_MODE:-native}"
  NEEDS_DOCKER=false
  [ "$DEPLOY_ENGINE" = "trino" ] && NEEDS_DOCKER=true
  [ "$OBS_MODE" = "docker" ] && NEEDS_DOCKER=true
  { [ "$(_lc "$INSTALL_DEMO")" = "y" ] && [ "$DEMO_MODE" = "docker" ]; } && NEEDS_DOCKER=true
  ok "Deployment: engine=${DEPLOY_ENGINE} obs=${OBS_MODE} demo=${INSTALL_DEMO}/${DEMO_MODE} docker=${NEEDS_DOCKER}"
}

# ── Acquire an add-on image set: local-first, download last (airgap seam) ─────
# Discovery order — the first four are OFFLINE. An enterprise builds a fully
# airgapped install by pre-staging the tarball beside the installer:
#   1. installer-adjacent dir (next to the .app / DMG root)  ← the airgap seam
#   2. this script's dir
#   3. ~/Downloads/
#   4. any mounted volume (/Volumes/*)
#   5. GitHub release download (only if nothing staged AND the add-on was selected)
# Usage: acquire_addon <label> <filename> <dest_dir> <selected: y|n>
acquire_addon() {
  local label="$1" filename="$2" dest_dir="$3" selected="$4"

  if [ -d "$dest_dir" ] && [ "$(ls -A "$dest_dir" 2>/dev/null)" ]; then
    info "${label} already present — skipping."
    return 0
  fi
  mkdir -p "$dest_dir"

  local src="" cand
  for cand in \
    "$(dirname "$BUNDLE_DIR")/${filename}" \
    "${SCRIPT_DIR}/${filename}" \
    "${HOME}/Downloads/${filename}"; do
    [ -f "$cand" ] && src="$cand" && break
  done
  if [ -z "$src" ]; then
    for cand in "/Volumes/"*"/${filename}"; do
      [ -f "$cand" ] && src="$cand" && break
    done
  fi

  if [ -n "$src" ]; then
    info "Extracting ${label} from ${src} (offline)..."
    tar -xzf "$src" -C "$dest_dir"
    ok "${label} extracted."
    return 0
  fi

  # Not staged locally — offer/download from GitHub only when selected.
  local version download_url=""
  version="${PROVISA_VERSION:-}"
  if [ -z "$version" ] && command -v provisa &>/dev/null; then
    version="$(provisa version 2>/dev/null | head -1 | awk '{print $NF}')" || version=""
  fi
  [ -n "$version" ] && download_url="https://github.com/kenstott/provisa/releases/download/${version}/${filename}"

  local answer=""
  if [[ -n "${PROVISA_NONINTERACTIVE:-}" ]]; then
    answer="$selected"
  elif [ -n "$download_url" ]; then
    printf "\n${BOLD}%s${NC}\nNot found locally. Download from GitHub? (~1–2 GB) [y/N]: " "$label"
    read -r answer
  else
    printf "(Place %s beside the installer and re-run to install %s offline.)\n" "$filename" "$label"
    answer="n"
  fi

  if [ "$(_lc "$answer")" = "y" ] && [ -n "$download_url" ]; then
    info "Downloading ${filename}..."
    local tmp="${PROVISA_HOME}/${filename}"
    if curl -fL --retry 3 --retry-delay 5 -o "$tmp" "$download_url"; then
      tar -xzf "$tmp" -C "$dest_dir"; rm -f "$tmp"
      ok "${label} downloaded and extracted."
    else
      err "Download failed. Place ${filename} beside the installer and re-run."
      rm -rf "$dest_dir"
    fi
  else
    info "Skipping ${label}. Install later: place ${filename} beside the installer and re-run."
    rm -d "$dest_dir" 2>/dev/null || true
  fi
}

# ── Acquire only the add-on image sets the chosen deployment needs ────────────
install_addons() {
  local version="${PROVISA_VERSION:-}"
  if [ -z "$version" ] && command -v provisa &>/dev/null; then
    version="$(provisa version 2>/dev/null | head -1 | awk '{print $NF}')" || version=""
  fi
  # Note: the core-images add-on (postgres/trino/zaychik/redis/pgbouncer + python base) is
  # acquired by stage_images for ANY Docker tier (slim base ships no images in the DMG).
  # Observability integration demo (collector+prometheus+grafana) — only in docker mode.
  if [ "$OBS_MODE" = "docker" ]; then
    acquire_addon "Observability integration demo" "provisa-obs-images-${version}.tar.gz" "${PROVISA_HOME}/obs-images" "y"
  fi
  # Demo images — only when installing the demo on Docker (native demo uses host mock servers).
  if [ "$(_lc "$INSTALL_DEMO")" = "y" ] && [ "$DEMO_MODE" = "docker" ]; then
    acquire_addon "Demo" "provisa-demo-images-${version}.tar.gz" "${PROVISA_HOME}/demo-images" "y"
  fi
}

# ── Network check (online vs airgapped) ──────────────────────────────────────
_online() { curl -fsI --max-time 8 https://pypi.org/simple/ >/dev/null 2>&1; }

# ── Locate a bundled hidden-DMG payload dir by name (next to .app or on a volume) ─
# The bare interpreter, wheelhouse, and UI ship as hidden DMG content (like images/):
# discovered beside the .app (running from the mounted DMG) or on any mounted volume.
_find_payload() {
  local name="$1" test_glob="$2" bundle_parent cand
  bundle_parent="$(dirname "$BUNDLE_DIR")"
  for cand in "${bundle_parent}/${name}" "${bundle_parent}/.${name}" \
              /Volumes/*/"${name}" /Volumes/*/".${name}"; do
    if [ -d "$cand" ] && ( [ -z "$test_glob" ] || ls "$cand"/$test_glob >/dev/null 2>&1 ); then
      printf '%s' "$cand"; return 0
    fi
  done
  return 1
}

# ── Native tier: build a Python venv from the bundled interpreter + wheelhouse ─
# Online → pip install provisa[embedded] from PyPI (pinned to the release). Airgapped →
# --no-index --find-links against the bundled wheelhouse (always pre-staged on disk).
# The bare interpreter (python-base/), wheelhouse (wheels/) and built UI (ui-dist/)
# ship as hidden DMG payload; the standalone runtime DMG is gone.
setup_native_venv() {
  local venv="${PROVISA_HOME}/venv"
  if [ -x "${venv}/bin/python3" ] && "${venv}/bin/python3" -c "import provisa" 2>/dev/null; then
    return 0
  fi

  local base_src
  base_src="$(_find_payload python-base bin/python3)" || {
    err "Bundled Python interpreter not found. Keep the Provisa DMG mounted and re-open Provisa.app."
    exit 1
  }

  # Stage + de-quarantine + ad-hoc sign the interpreter so Gatekeeper lets it run.
  local base="${PROVISA_HOME}/python-base"
  if [ ! -x "${base}/bin/python3" ]; then
    info "Staging Python interpreter..."
    mkdir -p "$base"; cp -R "$base_src"/. "$base/"
    xattr -dr com.apple.quarantine "$base" 2>/dev/null || true
    codesign --force --deep --sign - "${base}/bin/python3" 2>/dev/null || true
  fi

  info "Creating Python environment..."
  "${base}/bin/python3" -m venv "$venv"
  local pip="${venv}/bin/pip"
  "$pip" install --quiet --upgrade pip 2>/dev/null || true

  local pin=""
  [ -n "$PROVISA_VERSION" ] && pin="==${PROVISA_VERSION#v}"
  local wheels; wheels="$(_find_payload wheels '*.whl' || true)"

  if _online; then
    info "Installing Provisa from PyPI..."
    "$pip" install --quiet "provisa[embedded]${pin}" uvicorn mcp-proxy
  elif [ -n "$wheels" ]; then
    info "Installing Provisa from bundled wheels (offline)..."
    "$pip" install --quiet --no-index --find-links "$wheels" "provisa[embedded]" uvicorn mcp-proxy
  else
    err "No network and no bundled wheels found. Keep the Provisa DMG mounted and re-open Provisa.app."
    exit 1
  fi

  # Place the built UI where ui_server resolves it (<site-packages>/static).
  local ui_src; ui_src="$(_find_payload ui-dist '' || true)"
  if [ -n "$ui_src" ]; then
    local site; site="$("${venv}/bin/python3" -c 'import sysconfig;print(sysconfig.get_paths()["purelib"])')"
    mkdir -p "${site}/static"; cp -R "$ui_src"/. "${site}/static/"
  fi
  ok "Native environment ready."
}

# ── Docker tier helpers (user's own Docker — no VM) ──────────────────────────
require_docker() {
  if ! command -v docker >/dev/null 2>&1; then
    err "Docker is required for this deployment (Trino engine and/or Docker observability)."
    err "Install Docker Desktop or colima, start it, then re-open Provisa.app."
    exit 1
  fi
  if ! docker info >/dev/null 2>&1; then
    err "Docker is installed but not running. Start Docker Desktop (or colima) and re-open Provisa.app."
    exit 1
  fi
}

# docker load every saved-image tarball in a dir (docker load handles gzip streams).
load_images() {
  local dir="$1" f
  [ -d "$dir" ] || return 0
  for f in "$dir"/*.tar.gz "$dir"/*.tar; do
    [ -f "$f" ] || continue
    info "  docker load $(basename "$f")"
    docker load -i "$f" >/dev/null
  done
}

# ── Install CLI symlink ───────────────────────────────────────────────────────
install_cli() {
  local cli_src="${RESOURCES}/provisa-cli"
  local cli_dst="/usr/local/bin/provisa"
  info "Installing provisa CLI to /usr/local/bin/..."
  if [ -w /usr/local/bin ]; then
    cp "$cli_src" "$cli_dst"
    chmod +x "$cli_dst"
  else
    osascript -e "do shell script \"cp '${cli_src}' '${cli_dst}' && chmod +x '${cli_dst}'\" with administrator privileges"
  fi
  ok "CLI installed."
}

# ── Main ──────────────────────────────────────────────────────────────────────
main() {
  info "Install directory: ${PROVISA_HOME}"
  printf '%s' "${PROVISA_HOME}" > "${HOME}/.provisa_home"
  if [ -f "$SENTINEL" ]; then
    # Already set up — update bundle, CLI, and compose YAMLs to latest version
    echo "PROGRESS:staging"
    install_to_applications
    stage_compose
    echo "PROGRESS:finalize"
    install_cli
    exit 0
  fi

  FED_WORKERS="${PROVISA_WORKERS:-0}"

  printf "\n${BOLD}Provisa — First Launch Setup${NC}\n"
  printf "═══════════════════════════════════════════\n\n"

  mkdir -p "$PROVISA_HOME"
  resolve_deployment      # sets DEPLOY_ENGINE OBS_MODE INSTALL_DEMO DEMO_MODE NEEDS_DOCKER

  # ── Native tier (default): a Python venv, no Docker ──
  if [ "$NEEDS_DOCKER" = false ]; then
    IMAGE_SOURCE=build
    write_config
    echo "PROGRESS:staging"
    setup_native_venv       # bundled interpreter + venv + pip (online: PyPI, offline: wheelhouse)
    install_to_applications # self-installs to /Applications if running from DMG
    echo "PROGRESS:extensions"
    install_addons          # native demo (host mock servers) needs no images; no-op unless selected
    echo "PROGRESS:finalize"
    install_cli
    touch "$SENTINEL"
    ok "First-launch setup complete (native — no Docker)."
    printf "\n${GREEN}${BOLD}Provisa is ready.${NC}\n"
    if [[ "$BUNDLE_DIR" == /Volumes/* ]]; then
      printf "\nOpening Provisa from /Applications...\n"
      open /Applications/Provisa.app
    else
      printf "Run: ${BOLD}provisa start${NC}\n\n"
    fi
    return 0
  fi

  # ── Docker tier: Trino engine and/or Docker obs/demo, on the user's own Docker ──
  # The Provisa app images (provisa/provisa:local, provisa-ui, zaychik) are built from
  # source at DMG-build time and shipped in the core-images tarball; we docker load them
  # (image_source=tarball → the CLI adds docker-compose.airgap.yml so build: services
  # resolve to the loaded tags). Registry images (postgres/trino/redis/minio) are loaded
  # from the same tarball offline, or pulled by `docker compose up` online.
  require_docker
  IMAGE_SOURCE=tarball
  write_config

  echo "PROGRESS:staging"
  stage_trino_plugins     # Trino connector plugins → ~/.provisa/trino/plugins/
  stage_compose           # compose files + observability/ → ~/.provisa/compose/
  install_to_applications # self-installs to /Applications if running from DMG

  echo "PROGRESS:build"
  stage_images            # acquire provisa-core-images tarball (local-first, else download)

  echo "PROGRESS:images"
  load_images "${PROVISA_HOME}/images"   # docker load app + core images from the tarball

  echo "PROGRESS:extensions"
  install_addons          # acquire obs/demo image tarballs for the chosen deployment
  [ "$OBS_MODE" = "docker" ] && load_images "${PROVISA_HOME}/obs-images"
  { [ "$(_lc "$INSTALL_DEMO")" = "y" ] && [ "$DEMO_MODE" = "docker" ]; } && load_images "${PROVISA_HOME}/demo-images"

  echo "PROGRESS:finalize"
  install_cli

  touch "$SENTINEL"
  ok "First-launch setup complete."
  printf "\n${GREEN}${BOLD}Provisa is ready.${NC}\n"

  if [[ "$BUNDLE_DIR" == /Volumes/* ]]; then
    printf "\nOpening Provisa from /Applications...\n"
    open /Applications/Provisa.app
  else
    printf "Run: ${BOLD}provisa start${NC}\n\n"
  fi
}

main "$@"
