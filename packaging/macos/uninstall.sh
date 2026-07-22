#!/usr/bin/env bash
# Uninstall all Provisa DMG assets from this Mac.
# Removes: /Applications/Provisa.app, ~/.provisa, /usr/local/bin/provisa
# (stops the Docker stack first if the Docker tier was in use).
set -euo pipefail

RED='\033[0;31m'; GREEN='\033[0;32m'; CYAN='\033[0;36m'; BOLD='\033[1m'; NC='\033[0m'
info() { printf "${CYAN}[uninstall]${NC} %s\n" "$*"; }
ok()   { printf "${GREEN}[uninstall]${NC} %s\n" "$*"; }
err()  { printf "${RED}[uninstall]${NC} %s\n" "$*" >&2; }

APP_PATH="/Applications/Provisa.app"
CLI_PATH="/usr/local/bin/provisa"

# Resolve actual install dir from UserDefaults or redirect file, fallback to default
PROVISA_HOME_CUSTOM="$(defaults read com.provisa.app provisaInstallDir 2>/dev/null || true)"
if [ -z "$PROVISA_HOME_CUSTOM" ] && [ -f "${HOME}/.provisa_home" ]; then
  PROVISA_HOME_CUSTOM="$(cat "${HOME}/.provisa_home")"
fi
PROVISA_HOME="${PROVISA_HOME_CUSTOM:-${HOME}/.provisa}"

# ── Confirm ───────────────────────────────────────────────────────────────────
printf "\n${BOLD}Provisa Uninstaller${NC}\n"
printf "═══════════════════════════════════════════\n\n"
printf "This will remove:\n"
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

# ── Stop services (Docker tier) — best effort ─────────────────────────────────
if [ -x "$CLI_PATH" ]; then
  info "Stopping Provisa services..."
  "$CLI_PATH" stop >/dev/null 2>&1 || true
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

# ── Kill running app ──────────────────────────────────────────────────────────
if pgrep -x "ProvisaLauncher" &>/dev/null; then
  info "Stopping ProvisaLauncher..."
  pkill -x "ProvisaLauncher" || true
  ok "ProvisaLauncher stopped."
fi

# ── Clear UserDefaults and redirect file ──────────────────────────────────────
defaults delete com.provisa.app provisaInstallDir 2>/dev/null || true
if [ -f "${HOME}/.provisa_home" ]; then
  rm -f "${HOME}/.provisa_home"
  ok "Removed ~/.provisa_home"
fi

printf "\n${GREEN}${BOLD}Provisa uninstalled.${NC}\n\n"
