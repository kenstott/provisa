#!/usr/bin/env bash
# Uninstall all Provisa DMG assets from this Mac.
# Removes: Lima VM, /Applications/Provisa.app, ~/.provisa, /usr/local/bin/provisa
set -euo pipefail

RED='\033[0;31m'; GREEN='\033[0;32m'; CYAN='\033[0;36m'; BOLD='\033[1m'; NC='\033[0m'
info() { printf "${CYAN}[uninstall]${NC} %s\n" "$*"; }
ok()   { printf "${GREEN}[uninstall]${NC} %s\n" "$*"; }
err()  { printf "${RED}[uninstall]${NC} %s\n" "$*" >&2; }

LIMA_VM_NAME="provisa"
APP_PATH="/Applications/Provisa.app"
PROVISA_HOME="${HOME}/.provisa"
LIMA_HOME="${HOME}/.lima/${LIMA_VM_NAME}"
CLI_PATH="/usr/local/bin/provisa"

ARCH="$(uname -m)"
case "$ARCH" in
  arm64)  BIN_ARCH="arm64" ;;
  x86_64) BIN_ARCH="x86_64" ;;
  *)      BIN_ARCH="arm64" ;;
esac

LIMACTL="${APP_PATH}/Contents/MacOS/bin/${BIN_ARCH}/limactl"

# ── Confirm ───────────────────────────────────────────────────────────────────
printf "\n${BOLD}Provisa Uninstaller${NC}\n"
printf "═══════════════════════════════════════════\n\n"
printf "This will remove:\n"
printf "  • Lima VM '%s'\n" "$LIMA_VM_NAME"
printf "  • %s\n" "$APP_PATH"
printf "  • %s\n" "$PROVISA_HOME"
printf "  • %s\n" "$CLI_PATH"
printf "\n"
printf "Continue? [y/N] "
read -r confirm
if [[ "$confirm" != "y" && "$confirm" != "Y" ]]; then
  info "Aborted."
  exit 0
fi

# ── Stop and delete Lima VM ───────────────────────────────────────────────────
if [ -x "$LIMACTL" ]; then
  if "$LIMACTL" list --format '{{.Name}}' 2>/dev/null | grep -q "^${LIMA_VM_NAME}$"; then
    state="$("$LIMACTL" list --format '{{.Status}}' "$LIMA_VM_NAME" 2>/dev/null || echo "unknown")"
    if [ "$state" = "Running" ]; then
      info "Stopping Lima VM '${LIMA_VM_NAME}'..."
      "$LIMACTL" stop "$LIMA_VM_NAME" || true
    fi
    info "Deleting Lima VM '${LIMA_VM_NAME}'..."
    "$LIMACTL" delete "$LIMA_VM_NAME" || true
    ok "Lima VM removed."
  else
    info "Lima VM '${LIMA_VM_NAME}' not found — skipping."
  fi
else
  # App already removed or limactl not present — wipe lima data dir directly
  if [ -d "$LIMA_HOME" ]; then
    info "Removing Lima data at ${LIMA_HOME}..."
    rm -rf "$LIMA_HOME"
    ok "Lima data removed."
  fi
fi

# ── Remove /Applications/Provisa.app ─────────────────────────────────────────
if [ -d "$APP_PATH" ]; then
  info "Removing ${APP_PATH}..."
  if rm -rf "$APP_PATH" 2>/dev/null; then
    ok "Removed ${APP_PATH}"
  else
    osascript -e "do shell script \"rm -rf '${APP_PATH}'\" with administrator privileges"
    ok "Removed ${APP_PATH}"
  fi
else
  info "${APP_PATH} not found — skipping."
fi

# ── Remove ~/.provisa ─────────────────────────────────────────────────────────
if [ -d "$PROVISA_HOME" ]; then
  info "Removing ${PROVISA_HOME}..."
  rm -rf "$PROVISA_HOME"
  ok "Removed ${PROVISA_HOME}"
else
  info "${PROVISA_HOME} not found — skipping."
fi

# ── Remove Lima VM data dir (if limactl delete left it) ───────────────────────
if [ -d "$LIMA_HOME" ]; then
  info "Removing residual Lima data at ${LIMA_HOME}..."
  rm -rf "$LIMA_HOME"
  ok "Removed ${LIMA_HOME}"
fi

# ── Remove CLI ────────────────────────────────────────────────────────────────
if [ -f "$CLI_PATH" ]; then
  info "Removing ${CLI_PATH}..."
  if rm -f "$CLI_PATH" 2>/dev/null; then
    ok "Removed ${CLI_PATH}"
  else
    osascript -e "do shell script \"rm -f '${CLI_PATH}'\" with administrator privileges"
    ok "Removed ${CLI_PATH}"
  fi
else
  info "${CLI_PATH} not found — skipping."
fi

printf "\n${GREEN}${BOLD}Provisa uninstalled.${NC}\n\n"
