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
cpus: 4
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
  - location: "~/.provisa"
    writable: true
networks: []
provision:
  - mode: system
    script: |
      #!/bin/bash
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
  for gz_file in "${IMAGES_DIR}"/*.tar.gz; do
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
    err "Container images not found."
    err "Please keep the Provisa DMG mounted and re-open Provisa.app to complete setup."
    exit 1
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
  "$LIMACTL" shell "$LIMA_VM_NAME" -- \
    sudo nerdctl build -t provisa/provisa:local "${PROVISA_HOME}/provisa-source"
  ok "provisa/provisa:local built."
}

# ── Stage compose files into ~/.provisa/compose/ (VM-accessible) ─────────────
# The Lima YAML mounts ~/.provisa writable. The app bundle's Resources dir is
# NOT mounted, so compose files must live under ~/.provisa for nerdctl compose
# to find them inside the VM.
stage_compose() {
  local dest="${PROVISA_HOME}/compose"
  if [ -d "$dest" ] && [ -f "${dest}/docker-compose.yml" ]; then
    return 0
  fi
  mkdir -p "$dest"
  for f in \
    docker-compose.yml \
    docker-compose.prod.yml \
    docker-compose.airgap.yml; do
    if [ -f "${RESOURCES}/${f}" ]; then
      cp "${RESOURCES}/${f}" "${dest}/${f}"
    fi
  done
  for d in config db trino; do
    if [ -d "${RESOURCES}/${d}" ]; then
      cp -r "${RESOURCES}/${d}" "${dest}/${d}"
    fi
  done
  ok "Compose files staged to ${dest}"
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
# project_dir points to ~/.provisa/compose/ which is mounted into the Lima VM
project_dir: "${PROVISA_HOME}/compose"
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
  stage_vm_image          # copies base VM image from DMG → ~/.provisa/vm-image
  stage_images            # copies container images from DMG → ~/.provisa/images
  stage_nerdctl           # copies nerdctl-full archive from DMG → ~/.provisa/nerdctl/
  stage_provisa_source    # copies Dockerfile + source → ~/.provisa/provisa-source/ (VM-accessible)
  stage_compose           # copies compose files from bundle → ~/.provisa/compose/ (VM-accessible)
  install_to_applications # self-installs to /Applications if running from DMG
  install_guest_agent     # stages gz to ~/.provisa/share/lima/, creates limactl symlink
  start_lima
  import_images
  build_provisa_image     # builds provisa/provisa:local inside Lima from bundled source
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
