#!/usr/bin/env bash
# Build and run ProvisaLauncher locally for testing — no CI, no DMG.
# Copies /Applications/Provisa.app to ~/Applications/ and swaps in the
# fresh binary, preserving bundle identity (com.provisa.app) and Info.plist.
#
# Usage: ./test-local.sh [--reset]
#   --reset  wipe UserDefaults and sentinel so the setup wizard re-runs
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PKG_DIR="${SCRIPT_DIR}/ProvisaLauncher"
BUILD_DIR="${PKG_DIR}/.build/debug"
SRC_BUNDLE="/Applications/Provisa.app"
TEST_BUNDLE="${HOME}/Applications/ProvisaTest.app"
TEST_BINARY="${TEST_BUNDLE}/Contents/MacOS/ProvisaLauncher"
BUNDLE_ID="com.provisa.app"

RED='\033[0;31m'; GREEN='\033[0;32m'; CYAN='\033[0;36m'; BOLD='\033[1m'; NC='\033[0m'
info() { printf "${CYAN}[test-local]${NC} %s\n" "$*"; }
ok()   { printf "${GREEN}[test-local]${NC} %s\n" "$*"; }
err()  { printf "${RED}[test-local]${NC} %s\n" "$*" >&2; exit 1; }

[ -d "$SRC_BUNDLE" ] || err "${SRC_BUNDLE} not found. Install Provisa first."

if [[ "${1:-}" == "--reset" ]]; then
  info "Resetting install state..."
  INSTALL_DIR="$(defaults read "${BUNDLE_ID}" provisaInstallDir 2>/dev/null || echo "${HOME}/.provisa")"
  defaults delete "${BUNDLE_ID}" provisaInstallDir 2>/dev/null || true
  rm -f "${HOME}/.provisa_home"
  rm -f "${INSTALL_DIR}/.first-launch-complete" 2>/dev/null || true
  ok "State cleared — setup wizard will re-run."
fi

# Kill any running test instance
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

# Build test bundle in ~/Applications (user-writable, no SIP)
mkdir -p "${HOME}/Applications"
info "Syncing bundle to ${TEST_BUNDLE}..."
rsync -a --delete "${SRC_BUNDLE}/" "${TEST_BUNDLE}/"
cp "$BINARY" "$TEST_BINARY"
find "$TEST_BUNDLE" -exec xattr -c {} \; 2>/dev/null || true
ok "Test bundle ready."

info "Launching ${TEST_BUNDLE}..."
open "$TEST_BUNDLE"

ok "Running. Check menu bar or setup window."
info "Reset & rerun:  ./test-local.sh --reset"
