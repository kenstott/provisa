#!/usr/bin/env bash
# Phase AF2a — First-launch setup: start Lima VM, import images.
# Called by provisa-launcher on first run only.
set -euo pipefail

BUNDLE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RESOURCES="${BUNDLE_DIR}/Resources"
IMAGES_DIR="${RESOURCES}/images"
PROVISA_HOME="${HOME}/.provisa"
SENTINEL="${PROVISA_HOME}/.first-launch-complete"
LIMA_VM_NAME="provisa"
LIMA_YAML="${RESOURCES}/provisa-lima.yaml"

ARCH="$(uname -m)"
case "$ARCH" in
  arm64)  BIN_ARCH="arm64" ;;
  x86_64) BIN_ARCH="x86_64" ;;
  *)
    printf "[provisa] Unsupported architecture: %s\n" "$ARCH" >&2
    exit 1
    ;;
esac

LIMACTL="${BUNDLE_DIR}/MacOS/bin/${BIN_ARCH}/limactl"

RED='\033[0;31m'; GREEN='\033[0;32m'; CYAN='\033[0;36m'; BOLD='\033[1m'; NC='\033[0m'
info() { printf "${CYAN}[provisa]${NC} %s\n" "$*"; }
ok()   { printf "${GREEN}[provisa]${NC} %s\n" "$*"; }
err()  { printf "${RED}[provisa]${NC} %s\n" "$*" >&2; }

# ── Derive Trino worker count from RAM budget ─────────────────────────────────
_workers_from_budget() {
  local gb="$1"
  if   [ "$gb" -ge 96 ]; then echo 4
  elif [ "$gb" -ge 48 ]; then echo 2
  elif [ "$gb" -ge 24 ]; then echo 1
  else echo 0
  fi
}

# ── Ask RAM budget at first launch ───────────────────────────────────────────
# Sets globals: BUDGET_GB, TRINO_WORKERS, LIMA_MEMORY
ask_ram_budget() {
  local total_gb
  total_gb="$(sysctl -n hw.memsize | awk '{printf "%d", $1/1024/1024/1024}')"

  printf "\n${BOLD}RAM Budget${NC}\n"
  printf "How much RAM should Provisa use? (host total: %dGB)\n\n" "$total_gb"

  # Build option list: powers of 2 up to total, then All
  local options=()
  for size in 4 8 16 32 64 128; do
    [ "$size" -le "$total_gb" ] && options+=("${size}GB")
  done
  options+=("All (${total_gb}GB)")

  local i=1
  for opt in "${options[@]}"; do
    printf "  [%d] %s\n" "$i" "$opt"
    i=$((i + 1))
  done
  printf "\n"

  local choice
  while true; do
    printf "Enter choice [1-%d]: " "${#options[@]}"
    read -r choice
    if [[ "$choice" =~ ^[0-9]+$ ]] && [ "$choice" -ge 1 ] && [ "$choice" -le "${#options[@]}" ]; then
      break
    fi
    printf "Invalid choice. Try again.\n"
  done

  local selected="${options[$((choice - 1))]}"
  if [[ "$selected" == All* ]]; then
    BUDGET_GB="$total_gb"
  else
    BUDGET_GB="${selected%GB}"
  fi

  TRINO_WORKERS="$(_workers_from_budget "$BUDGET_GB")"
  LIMA_MEMORY="${BUDGET_GB}GiB"

  ok "RAM budget: ${BUDGET_GB}GB → Trino workers: ${TRINO_WORKERS}"
}

# ── Write Lima VM config if not present ──────────────────────────────────────
write_lima_config() {
  if [ -f "$LIMA_YAML" ]; then
    return
  fi
  cat > "$LIMA_YAML" <<YAML
# Provisa Lima VM — airgapped, no network pull required
vmType: vz
os: Linux
arch: host
cpus: 4
memory: "${LIMA_MEMORY}"
disk: "60GiB"
rosetta:
  enabled: true
  binfmt: true
containerd:
  system: true
  user: false
mounts:
  - location: "~/.provisa"
    writable: true
networks: []
provision:
  - mode: system
    script: |
      #!/bin/bash
      # containerd already managed by Lima containerd integration
      systemctl enable --now containerd || true
YAML
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
    "$LIMACTL" start "$LIMA_VM_NAME"
  else
    write_lima_config
    info "Creating VM from config..."
    "$LIMACTL" start --name="$LIMA_VM_NAME" "$LIMA_YAML"
  fi
  ok "VM started."
}

# ── Import bundled images ─────────────────────────────────────────────────────
import_images() {
  info "Importing bundled container images (no network required)..."
  local count=0
  for tar_file in "${IMAGES_DIR}"/*.tar; do
    [ -f "$tar_file" ] || continue
    local name
    name="$(basename "$tar_file")"
    info "  Importing: ${name}"
    "$LIMACTL" shell "$LIMA_VM_NAME" -- \
      sudo ctr --namespace=default images import "/mnt/lima.hostagent/Users/${USER}/.provisa/images/${name}" \
      2>/dev/null || \
    "$LIMACTL" shell "$LIMA_VM_NAME" -- \
      sudo ctr --namespace=default images import - < "$tar_file"
    count=$((count + 1))
  done
  ok "Imported ${count} images."
}

# ── Copy images into provisa home for VM access ───────────────────────────────
stage_images() {
  local staged="${PROVISA_HOME}/images"
  if [ -d "$staged" ] && [ "$(ls -A "$staged" 2>/dev/null)" ]; then
    return 0
  fi
  mkdir -p "$staged"

  # 1. Sibling images next to the .app (running directly from mounted DMG)
  local sibling_images
  sibling_images="$(dirname "$BUNDLE_DIR")/images"
  local src=""
  if [ -d "$sibling_images" ] && ls "$sibling_images"/*.tar &>/dev/null 2>&1; then
    src="$sibling_images"
  fi

  # 2. Scan mounted DMG volumes (user dragged .app to Applications but DMG still open)
  if [ -z "$src" ]; then
    for vol_images in /Volumes/*/images; do
      if [ -d "$vol_images" ] && ls "$vol_images"/*.tar &>/dev/null 2>&1; then
        src="$vol_images"
        break
      fi
    done
  fi

  # 3. Fallback: images embedded inside the bundle (not typical in Option C)
  if [ -z "$src" ] && [ -d "$IMAGES_DIR" ] && ls "$IMAGES_DIR"/*.tar &>/dev/null 2>&1; then
    src="$IMAGES_DIR"
  fi

  if [ -z "$src" ]; then
    err "Container images not found."
    err "Please keep the Provisa DMG mounted and re-open Provisa.app to complete setup."
    exit 1
  fi

  info "Staging images to ${staged}..."
  cp "$src"/*.tar "$staged/"
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

# ── Prompt for UI port ────────────────────────────────────────────────────────
ask_ui_port() {
  local default=3000
  local port=""

  port=$(osascript <<APPLESCRIPT 2>/dev/null
    set defaultPort to "3000"
    set result to display dialog "Which port should the Provisa web UI listen on?" ¬
      default answer defaultPort ¬
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

  echo "$port"
}

# ── Write config ──────────────────────────────────────────────────────────────
write_config() {
  if [ -f "${PROVISA_HOME}/config.yaml" ]; then
    return
  fi
  mkdir -p "$PROVISA_HOME"

  local hostname
  hostname="$(ask_hostname)"
  local ui_port
  ui_port="$(ask_ui_port)"
  local api_port=$(( ui_port + 1 ))

  info "Hostname: ${hostname}  |  UI port: ${ui_port}  |  API port: ${api_port}"

  cat > "${PROVISA_HOME}/config.yaml" <<YAML
# Provisa configuration — generated by DMG installer
project_dir: "${RESOURCES}"
hostname: ${hostname}
ui_port: ${ui_port}
api_port: ${api_port}
auto_open_browser: true
runtime: lima
lima_vm: ${LIMA_VM_NAME}
federation_workers: ${TRINO_WORKERS}
YAML
  ok "Config written to ${PROVISA_HOME}/config.yaml"
}

# ── Install CLI symlink ───────────────────────────────────────────────────────
install_cli() {
  local cli_src="${RESOURCES}/provisa-cli"
  local cli_dst="/usr/local/bin/provisa"
  if [ -f "$cli_dst" ]; then
    return 0
  fi
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
  if [ -f "$SENTINEL" ]; then
    # Already set up — if re-run from DMG, still update the /Applications copy
    install_to_applications
    exit 0
  fi

  # Globals set by ask_ram_budget
  BUDGET_GB=8
  TRINO_WORKERS=0
  LIMA_MEMORY="8GiB"

  printf "\n${BOLD}Provisa — First Launch Setup${NC}\n"
  printf "═══════════════════════════════════════════\n\n"
  info "Setting up Provisa (no internet required)..."

  mkdir -p "$PROVISA_HOME"
  ask_ram_budget
  write_config
  stage_images            # copies images/ from DMG sibling → ~/.provisa/images
  install_to_applications # self-installs to /Applications if running from DMG
  start_lima
  import_images
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
