#!/usr/bin/env bash
# Build and run ProvisaLauncher locally for testing — no CI, no DMG.
# Usage: ./test-local.sh [--reset]
#   --reset  wipe UserDefaults and sentinel so the setup wizard re-runs
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PKG_DIR="${SCRIPT_DIR}/ProvisaLauncher"
BUILD_DIR="${PKG_DIR}/.build/debug"
APP_BUNDLE="/Applications/Provisa.app"

RED='\033[0;31m'; GREEN='\033[0;32m'; CYAN='\033[0;36m'; BOLD='\033[1m'; NC='\033[0m'
info() { printf "${CYAN}[test-local]${NC} %s\n" "$*"; }
ok()   { printf "${GREEN}[test-local]${NC} %s\n" "$*"; }
err()  { printf "${RED}[test-local]${NC} %s\n" "$*" >&2; exit 1; }

if [[ "${1:-}" == "--reset" ]]; then
  info "Resetting install state..."
  defaults delete com.provisa.launcher provisaInstallDir 2>/dev/null || true
  rm -f "${HOME}/.provisa_home"
  INSTALL_DIR="$(defaults read com.provisa.launcher provisaInstallDir 2>/dev/null || echo "${HOME}/.provisa")"
  rm -f "${INSTALL_DIR}/.first-launch-complete" 2>/dev/null || true
  ok "State cleared — setup wizard will re-run."
fi

# Kill any running instance
if pgrep -x "ProvisaLauncher" &>/dev/null; then
  info "Killing existing ProvisaLauncher..."
  pkill -x "ProvisaLauncher" || true
  sleep 1
fi

info "Building ProvisaLauncher..."
cd "$PKG_DIR"
swift build 2>&1 | tail -5
ok "Build succeeded."

BINARY="${BUILD_DIR}/ProvisaLauncher"
[ -x "$BINARY" ] || err "Binary not found at ${BINARY}"

# Stub /usr/local/bin/provisa if missing (prevents startProvisa crash in DoneView)
if [ ! -f /usr/local/bin/provisa ]; then
  info "No /usr/local/bin/provisa found — installing stub."
  sudo tee /usr/local/bin/provisa >/dev/null <<'EOF'
#!/usr/bin/env bash
echo "[stub provisa] args: $*" >> /tmp/provisa-stub.log
EOF
  sudo chmod +x /usr/local/bin/provisa
  ok "Stub installed. Logs → /tmp/provisa-stub.log"
fi

info "Launching ProvisaLauncher..."
open -a /Applications/Provisa.app 2>/dev/null || "$BINARY" &

ok "Running. Check menu bar or setup window."
info "Tail stub log:  tail -f /tmp/provisa-stub.log"
info "Reset & rerun:  ./test-local.sh --reset"
