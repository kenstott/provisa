#!/usr/bin/env bash
# Phase AF2a — First-launch setup: start Lima VM, import images.
# Called by provisa-launcher on first run only.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUNDLE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RESOURCES="${BUNDLE_DIR}/Resources"
IMAGES_DIR="${RESOURCES}/images"
PROVISA_HOME="${PROVISA_INSTALL_DIR:-${HOME}/.provisa}"
SENTINEL="${PROVISA_HOME}/.first-launch-complete"
LIMA_VM_NAME="provisa"
LIMA_YAML="${PROVISA_HOME}/provisa-lima.yaml"

ARCH="$(uname -m)"
case "$ARCH" in
  arm64)  BIN_ARCH="arm64" ;;
  x86_64) BIN_ARCH="x86_64" ;;
  *)
    printf "[provisa] Unsupported architecture: %s\n" "$ARCH" >&2
    exit 1
    ;;
esac

# Real limactl inside the signed bundle
LIMACTL_REAL="${BUNDLE_DIR}/MacOS/bin/${BIN_ARCH}/limactl"
# Symlink at ~/.provisa/bin/limactl — Lima's SelfDirs() uses os.Args[0] (symlink-aware)
# so Lima resolves share/lima/ relative to ~/.provisa/bin/, not inside the bundle.
LIMACTL="${PROVISA_HOME}/bin/limactl"

RED='\033[0;31m'; GREEN='\033[0;32m'; CYAN='\033[0;36m'; BOLD='\033[1m'; DIM='\033[2m'; NC='\033[0m'
info() { printf "${CYAN}[provisa]${NC} %s\n" "$*"; }
ok()   { printf "${GREEN}[provisa]${NC} %s\n" "$*"; }
err()  { printf "${RED}[provisa]${NC} %s\n" "$*" >&2; }
# macOS ships /bin/bash 3.2 (ScriptRunner invokes /bin/bash), which lacks the
# ${var,,} lowercase expansion — use this helper instead.
_lc() { printf '%s' "$1" | tr '[:upper:]' '[:lower:]'; }

# ── Derive suggested federation worker count from RAM budget ─────────────────
_suggested_workers() {
  local gb="$1"
  if   [ "$gb" -ge 96 ]; then echo 4
  elif [ "$gb" -ge 48 ]; then echo 2
  elif [ "$gb" -ge 24 ]; then echo 1
  else echo 0
  fi
}

# ── Ask RAM, CPU, and federation worker budgets at first launch ──────────────
# Sets globals: BUDGET_GB, FED_WORKERS, LIMA_MEMORY, LIMA_CPUS
ask_ram_budget() {
  # Non-interactive mode: read from env vars (set by SwiftUI wizard)
  if [[ -n "${PROVISA_NONINTERACTIVE:-}" ]]; then
    BUDGET_GB="${PROVISA_RAM_GB:-8}"
    LIMA_CPUS="${PROVISA_CPU_COUNT:-4}"
    FED_WORKERS="${PROVISA_WORKERS:-0}"
    LIMA_MEMORY="${BUDGET_GB}GiB"
    ok "RAM: ${BUDGET_GB}GB | CPUs: ${LIMA_CPUS} | Federation workers: ${FED_WORKERS}"
    return
  fi

  local total_gb total_cores
  total_gb="$(sysctl -n hw.memsize | awk '{printf "%d", $1/1024/1024/1024}')"
  total_cores="$(sysctl -n hw.logicalcpu)"

  # ── RAM ──
  printf "\n${BOLD}RAM Budget${NC}\n"
  printf "How much RAM should Provisa use? (host total: %dGB)\n\n" "$total_gb"

  local ram_options=()
  for size in 4 8 16 32 64 128; do
    [ "$size" -le "$total_gb" ] && ram_options+=("${size}GB")
  done
  ram_options+=("All (${total_gb}GB)")

  local i=1
  for opt in "${ram_options[@]}"; do
    printf "  [%d] %s\n" "$i" "$opt"
    i=$((i + 1))
  done
  printf "\n"

  local choice
  while true; do
    printf "Enter choice [1-%d]: " "${#ram_options[@]}"
    read -r choice
    if [[ "$choice" =~ ^[0-9]+$ ]] && [ "$choice" -ge 1 ] && [ "$choice" -le "${#ram_options[@]}" ]; then
      break
    fi
    printf "Invalid choice. Try again.\n"
  done

  local selected="${ram_options[$((choice - 1))]}"
  if [[ "$selected" == All* ]]; then
    BUDGET_GB="$total_gb"
  else
    BUDGET_GB="${selected%GB}"
  fi
  LIMA_MEMORY="${BUDGET_GB}GiB"

  # ── CPUs ──
  printf "\n${BOLD}CPU Budget${NC}\n"
  printf "How many CPU cores should Provisa use? (host total: %d)\n" "$total_cores"
  printf "${DIM}The query engine uses 2 threads per vCPU. Leave cores for your other tools.${NC}\n\n"

  local default_cpus=$(( total_cores / 2 ))
  [ "$default_cpus" -lt 2 ] && default_cpus=2
  [ "$default_cpus" -gt 12 ] && default_cpus=12

  local cpu_options=()
  for n in 2 4 6 8 10 12; do
    [ "$n" -le "$total_cores" ] && cpu_options+=("$n")
  done
  cpu_options+=("All (${total_cores})")

  i=1
  local default_cpu_idx=1
  for opt in "${cpu_options[@]}"; do
    local marker=""
    local opt_val="${opt%% *}"
    [ "$opt_val" = "$default_cpus" ] && marker=" ${DIM}(recommended)${NC}"
    printf "  [%d] %s cores%b\n" "$i" "$opt" "$marker"
    [ "$opt_val" = "$default_cpus" ] && default_cpu_idx=$i
    i=$((i + 1))
  done
  printf "\n"

  while true; do
    printf "Enter choice [1-%d] (default %d): " "${#cpu_options[@]}" "$default_cpu_idx"
    read -r choice
    [ -z "$choice" ] && choice="$default_cpu_idx"
    if [[ "$choice" =~ ^[0-9]+$ ]] && [ "$choice" -ge 1 ] && [ "$choice" -le "${#cpu_options[@]}" ]; then
      break
    fi
    printf "Invalid choice. Try again.\n"
  done

  local cpu_selected="${cpu_options[$((choice - 1))]}"
  if [[ "$cpu_selected" == All* ]]; then
    LIMA_CPUS="$total_cores"
  else
    LIMA_CPUS="${cpu_selected%% *}"
  fi

  # ── Federation Workers ──
  local default_workers
  default_workers="$(_suggested_workers "$BUDGET_GB")"

  printf "\n${BOLD}Federation Workers${NC}\n"
  printf "How many additional query workers should Provisa run?\n"
  printf "${DIM}Workers parallelize queries across federated sources. Each needs ~4GB RAM.${NC}\n"
  printf "${DIM}0 workers = coordinator-only mode (fine for most single-machine installs).${NC}\n\n"

  local worker_options=(0 1 2 3 4)
  i=1
  local default_worker_idx=1
  for opt in "${worker_options[@]}"; do
    local marker=""
    [ "$opt" = "$default_workers" ] && marker=" ${DIM}(recommended)${NC}"
    printf "  [%d] %d%b\n" "$i" "$opt" "$marker"
    [ "$opt" = "$default_workers" ] && default_worker_idx=$i
    i=$((i + 1))
  done
  printf "\n"

  while true; do
    printf "Enter choice [1-%d] (default %d): " "${#worker_options[@]}" "$default_worker_idx"
    read -r choice
    [ -z "$choice" ] && choice="$default_worker_idx"
    if [[ "$choice" =~ ^[0-9]+$ ]] && [ "$choice" -ge 1 ] && [ "$choice" -le "${#worker_options[@]}" ]; then
      break
    fi
    printf "Invalid choice. Try again.\n"
  done

  FED_WORKERS="${worker_options[$((choice - 1))]}"

  ok "RAM: ${BUDGET_GB}GB | CPUs: ${LIMA_CPUS} | Federation workers: ${FED_WORKERS}"
}

# ── Write Lima VM config if not present ──────────────────────────────────────
write_lima_config() {
  if [ -f "$LIMA_YAML" ]; then
    return
  fi
  mkdir -p "$PROVISA_HOME"
  if [ "$ARCH" != "arm64" ]; then
    err "Provisa macOS requires Apple Silicon (arm64). Intel Macs are not supported."
    exit 1
  fi
  local arm64_local="${PROVISA_HOME}/vm-image/provisa-vm.img"

  local nerdctl_archive="${PROVISA_HOME}/nerdctl/nerdctl-full-2.2.2-linux-arm64.tar.gz"
  local nerdctl_digest="sha256:55d68d2613b5f065021146bac21f620cde9e7fdd4bd3eff74cd324f5462e107a"

  cat > "$LIMA_YAML" <<YAML
# Provisa Lima VM — Apple Silicon (arm64) only
vmType: vz
os: Linux
arch: "aarch64"
cpus: ${LIMA_CPUS}
memory: "${LIMA_MEMORY}"
disk: "60GiB"
images:
  - location: "file://${arm64_local}"
    arch: "aarch64"
vmOpts:
  vz: {}
containerd:
  system: true
  user: false
  archives:
    - location: "${nerdctl_archive}"
      arch: "aarch64"
      digest: "${nerdctl_digest}"
mounts:
  - location: "${PROVISA_HOME}"
    writable: true
networks: []
provision:
  - mode: system
    script: |
      #!/bin/bash
      # iptables is required by the CNI bridge plugin for container networking
      apt-get update -qq && apt-get install -y --no-install-recommends iptables
      systemctl enable --now containerd || true
YAML
}

# ── Install guest agent and create limactl symlink ────────────────────────────
# Lima 2.x (usrlocal.GuestAgentBinary) resolves the guest agent at:
#   {limactl_binary_dir}/../share/lima/
# SelfDirs() uses os.Args[0] (symlink-aware, NOT os.Executable()).
# So invoking limactl via ~/.provisa/bin/limactl (symlink) makes Lima look in
# ~/.provisa/share/lima/ — outside the codesign-protected bundle.
install_guest_agent() {
  local guest_agents_src="${RESOURCES}/lima-guest-agents"
  local lima_share="${PROVISA_HOME}/share/lima"
  local lima_bin="${PROVISA_HOME}/bin"

  mkdir -p "$lima_share" "$lima_bin"

  # Stage gz (Lima decompresses internally on first VM start)
  local gz_name="lima-guestagent.Linux-aarch64.gz"
  if [ ! -f "${lima_share}/${gz_name}" ]; then
    if [ ! -f "${guest_agents_src}/${gz_name}" ]; then
      err "Guest agent not found in bundle: ${guest_agents_src}/${gz_name}"
      exit 1
    fi
    cp "${guest_agents_src}/${gz_name}" "${lima_share}/${gz_name}"
    ok "Guest agent staged to ${lima_share}/${gz_name}"
  fi

  # Create symlink so Lima's SelfDirs() resolves to ~/.provisa/bin/
  if [ ! -L "${lima_bin}/limactl" ]; then
    ln -sf "$LIMACTL_REAL" "${lima_bin}/limactl"
    ok "limactl symlink created at ${lima_bin}/limactl"
  fi
}

# ── Start Lima VM ─────────────────────────────────────────────────────────────
start_lima() {
  info "Starting Provisa VM (first launch — this takes ~2 minutes)..."

  if "$LIMACTL" list --format '{{.Name}}' 2>/dev/null | grep -q "^${LIMA_VM_NAME}$"; then
    local state
    state="$("$LIMACTL" list --format '{{.Status}}' "$LIMA_VM_NAME" 2>/dev/null || echo "unknown")"
    if [ "$state" = "Running" ]; then
      ok "VM already running."
      return 0
    fi
    info "Resuming existing VM..."
    "$LIMACTL" start --yes "$LIMA_VM_NAME"
  else
    write_lima_config
    info "Creating VM from config..."
    "$LIMACTL" start --yes --name="$LIMA_VM_NAME" "$LIMA_YAML"
  fi
  ok "VM started."
}

# ── Import bundled images ─────────────────────────────────────────────────────
import_images() {
  info "Importing bundled container images (no network required)..."
  local count=0
  for gz_file in "${PROVISA_HOME}/images"/*.tar.gz; do
    [ -f "$gz_file" ] || continue
    local name
    name="$(basename "$gz_file")"
    info "  Importing: ${name}"
    # ctr images import handles gzip streams; pipe via gunzip for compatibility
    gunzip -c "$gz_file" | \
    "$LIMACTL" shell "$LIMA_VM_NAME" -- \
      sudo ctr --namespace=default images import -
    count=$((count + 1))
  done
  ok "Imported ${count} images."
}

# ── Stage base VM image from DMG to ~/.provisa/vm-image/ ─────────────────────
stage_vm_image() {
  local staged="${PROVISA_HOME}/vm-image"
  if [ -f "${staged}/provisa-vm.img" ]; then
    return 0
  fi
  mkdir -p "$staged"

  local bundle_parent
  bundle_parent="$(dirname "$BUNDLE_DIR")"
  local src=""
  for candidate in "${bundle_parent}/vm-image" "${bundle_parent}/.vm-image"; do
    if [ -d "$candidate" ] && ls "$candidate"/*.img &>/dev/null 2>&1; then
      src="$candidate"; break
    fi
  done

  if [ -z "$src" ]; then
    for vol_vm in /Volumes/*/vm-image /Volumes/*/.vm-image; do
      if [ -d "$vol_vm" ] && ls "$vol_vm"/*.img &>/dev/null 2>&1; then
        src="$vol_vm"; break
      fi
    done
  fi

  if [ -z "$src" ]; then
    err "Base VM image not found. Please keep the Provisa DMG mounted and re-open Provisa.app."
    exit 1
  fi

  info "Staging base VM image to ${staged}..."
  local src_img
  src_img=$(ls "$src"/*.img | head -1)
  cp "$src_img" "${staged}/provisa-vm.img"
  if [ ! -f "${staged}/provisa-vm.img" ]; then
    err "VM image not found after staging: ${staged}/provisa-vm.img"
    exit 1
  fi
}

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

# ── Stage nerdctl-full archive for airgapped containerd install ───────────────
stage_nerdctl() {
  local staged="${PROVISA_HOME}/nerdctl"
  local archive="nerdctl-full-2.2.2-linux-arm64.tar.gz"
  if [ -f "${staged}/${archive}" ]; then
    return 0
  fi
  mkdir -p "$staged"

  local bundle_parent
  bundle_parent="$(dirname "$BUNDLE_DIR")"
  local src=""
  for candidate in "${bundle_parent}/nerdctl" "${bundle_parent}/.nerdctl"; do
    if [ -d "$candidate" ] && [ -f "${candidate}/${archive}" ]; then
      src="$candidate"; break
    fi
  done

  if [ -z "$src" ]; then
    for vol_nerdctl in /Volumes/*/nerdctl /Volumes/*/.nerdctl; do
      if [ -d "$vol_nerdctl" ] && [ -f "${vol_nerdctl}/${archive}" ]; then
        src="$vol_nerdctl"; break
      fi
    done
  fi

  if [ -z "$src" ]; then
    err "nerdctl archive not found. Please keep the Provisa DMG mounted and re-open Provisa.app."
    exit 1
  fi

  info "Staging nerdctl archive to ${staged}..."
  cp "${src}/${archive}" "${staged}/"
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

# ── Build provisa/provisa:local inside Lima from staged source ─────────────────
build_provisa_image() {
  info "Building provisa/provisa:local inside Lima VM..."
  # --pull=false: use the bundled python:3.12-slim image, never pull from Docker Hub
  "$LIMACTL" shell "$LIMA_VM_NAME" -- \
    sudo nerdctl build --pull=false -t provisa/provisa:local "${PROVISA_HOME}/provisa-source"
  ok "provisa/provisa:local built."
}

# ── Stage compose files into ~/.provisa/compose/ (VM-accessible) ─────────────
# The Lima YAML mounts ~/.provisa writable. The app bundle's Resources dir is
# NOT mounted, so compose files must live under ~/.provisa for nerdctl compose
# to find them inside the VM.
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

  # Deployment fields (resolve_deployment ran first). Native tier runs the bundled
  # runtime directly; the Docker tier drives the Lima VM. Defaults keep this safe
  # under `set -u` on the native path, where the Lima globals are never set.
  local runtime demo_flag
  if [ "${NEEDS_DOCKER:-false}" = false ]; then
    runtime="native"
  else
    runtime="lima"
  fi
  [ "${INSTALL_DEMO:-n}" = "y" ] || [ "${INSTALL_DEMO:-n}" = "Y" ] && demo_flag=true || demo_flag=false

  cat > "${PROVISA_HOME}/config.yaml" <<YAML
# Provisa configuration — generated by installer
# project_dir points to ~/.provisa/compose/ which is mounted into the Lima VM (docker tier)
project_dir: "${PROVISA_HOME}/compose"
hostname: ${hostname}
ui_port: ${ui_port}
api_port: ${api_port}
flight_port: ${flight_port}
auto_open_browser: true
runtime: ${runtime}
lima_vm: ${LIMA_VM_NAME:-provisa}
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

# ── Stage the bundled standalone Python runtime for the native (no-Docker) tier ─
# The native tier runs provisa on a self-contained interpreter (python-build-standalone
# + the provisa wheel + duckdb/pg_duckdb + aiosqlite). It ships as HIDDEN DMG payload
# (like images/) — not inside the notarized .app — so we discover it next to the .app
# or on a mounted volume, then de-quarantine + ad-hoc sign so Gatekeeper lets it run.
stage_native_runtime() {
  local dest="${PROVISA_HOME}/runtime"
  if [ -d "$dest" ] && [ -x "${dest}/bin/python3" ]; then
    return 0
  fi

  local bundle_parent src=""
  bundle_parent="$(dirname "$BUNDLE_DIR")"
  for cand in "${bundle_parent}/runtime" "${bundle_parent}/.runtime"; do
    if [ -x "${cand}/bin/python3" ]; then src="$cand"; break; fi
  done
  if [ -z "$src" ]; then
    for cand in /Volumes/*/runtime /Volumes/*/.runtime; do
      if [ -x "${cand}/bin/python3" ]; then src="$cand"; break; fi
    done
  fi
  # The runtime ships as a separate DMG (2 GB asset limit). If it is not already
  # mounted, look for Provisa-Runtime*.dmg beside the app / core DMG / ~/Downloads
  # and auto-mount it, then re-search the mounted volumes.
  if [ -z "$src" ]; then
    local rt_dmg=""
    for dir in "$bundle_parent" "$(dirname "$bundle_parent")" "${HOME}/Downloads"; do
      for cand in "${dir}"/Provisa-Runtime*.dmg; do
        [ -f "$cand" ] && { rt_dmg="$cand"; break; }
      done
      [ -n "$rt_dmg" ] && break
    done
    if [ -n "$rt_dmg" ]; then
      info "Mounting native runtime DMG: ${rt_dmg}"
      hdiutil attach -nobrowse -quiet "$rt_dmg" || true
      for cand in /Volumes/*/runtime /Volumes/*/.runtime; do
        if [ -x "${cand}/bin/python3" ]; then src="$cand"; break; fi
      done
    fi
  fi
  if [ -z "$src" ]; then
    err "Native runtime not found. Download and mount Provisa-Runtime-<version>-macOS.dmg (ships beside the core DMG), then re-run."
    exit 1
  fi

  info "Staging native runtime to ${dest}..."
  mkdir -p "$dest"
  cp -R "$src"/. "$dest/"
  # Downloaded DMG content is quarantined; an unsigned interpreter would be blocked.
  xattr -dr com.apple.quarantine "$dest" 2>/dev/null || true
  codesign --force --deep --sign - "${dest}/bin/python3" 2>/dev/null || true
  ok "Native runtime staged."
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
    install_guest_agent
    stage_compose
    echo "PROGRESS:finalize"
    install_cli
    exit 0
  fi

  # Globals set by ask_ram_budget
  BUDGET_GB=8
  FED_WORKERS=0
  LIMA_MEMORY="8GiB"
  LIMA_CPUS=4

  printf "\n${BOLD}Provisa — First Launch Setup${NC}\n"
  printf "═══════════════════════════════════════════\n\n"
  info "Setting up Provisa (no internet required)..."

  mkdir -p "$PROVISA_HOME"
  resolve_deployment      # sets DEPLOY_ENGINE OBS_MODE INSTALL_DEMO DEMO_MODE NEEDS_DOCKER

  # ── Native tier (default): no Lima VM, no images, no build — just the bundled runtime ──
  if [ "$NEEDS_DOCKER" = false ]; then
    write_config
    echo "PROGRESS:staging"
    stage_native_runtime    # copies the standalone Python runtime from the bundle
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

  # ── Docker tier: Trino engine and/or Docker obs/demo need the Lima VM + images ──
  # Globals set by ask_ram_budget (only relevant when the VM runs)
  BUDGET_GB=8
  FED_WORKERS=0
  LIMA_MEMORY="8GiB"
  LIMA_CPUS=4
  ask_ram_budget
  write_config

  echo "PROGRESS:staging"
  stage_vm_image          # copies base VM image from DMG → ~/.provisa/vm-image
  stage_images            # copies container images from DMG → ~/.provisa/images
  stage_nerdctl           # copies nerdctl-full archive from DMG → ~/.provisa/nerdctl/
  stage_trino_plugins     # copies Trino plugins from DMG hidden content → ~/.provisa/trino/plugins/
  stage_provisa_source    # copies Dockerfile + source → ~/.provisa/provisa-source/ (VM-accessible)
  stage_compose           # copies compose files from bundle → ~/.provisa/compose/ (VM-accessible)
  install_to_applications # self-installs to /Applications if running from DMG
  install_guest_agent     # stages gz to ~/.provisa/share/lima/, creates limactl symlink

  echo "PROGRESS:vm_start"
  start_lima

  echo "PROGRESS:images"
  import_images

  echo "PROGRESS:build"
  build_provisa_image     # builds provisa/provisa:local inside Lima from bundled source

  echo "PROGRESS:extensions"
  install_addons          # only the add-on image sets the chosen deployment needs

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
