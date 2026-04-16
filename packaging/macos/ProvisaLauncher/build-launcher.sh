#!/usr/bin/env bash
# Build ProvisaLauncher.app from the Swift Package and embed it in Provisa.app.
#
# Usage:
#   ./build-launcher.sh [--debug] [--app-bundle /path/to/Provisa.app]
#
# Output: packaging/macos/ProvisaLauncher/build/ProvisaLauncher.app
#         (then copied into Provisa.app/Contents/MacOS/ when --app-bundle is given)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BUILD_CONFIG="release"
APP_BUNDLE=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --debug)       BUILD_CONFIG="debug";;
    --app-bundle)  APP_BUNDLE="$2"; shift;;
    *) echo "Unknown flag: $1" >&2; exit 1;;
  esac
  shift
done

BUILT_APP="${SCRIPT_DIR}/build/ProvisaLauncher.app"
CONTENTS="${BUILT_APP}/Contents"
MACOS="${CONTENTS}/MacOS"
RESOURCES="${CONTENTS}/Resources"

# ── Build Swift binary ─────────────────────────────────────────────────────────
echo "[launcher] Building ProvisaLauncher (${BUILD_CONFIG})..."
swift build --package-path "$SCRIPT_DIR" -c "$BUILD_CONFIG"
BINARY="${SCRIPT_DIR}/.build/${BUILD_CONFIG}/ProvisaLauncher"

# ── Assemble .app bundle ───────────────────────────────────────────────────────
rm -rf "$BUILT_APP"
mkdir -p "$MACOS" "$RESOURCES"

cp "$BINARY" "${MACOS}/ProvisaLauncher"

# Embed first-launch.sh so ScriptRunner can find it next to the executable
cp "${SCRIPT_DIR}/../first-launch.sh" "${MACOS}/first-launch.sh"
chmod +x "${MACOS}/first-launch.sh"

# Info.plist
cat > "${CONTENTS}/Info.plist" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>CFBundleExecutable</key>     <string>ProvisaLauncher</string>
    <key>CFBundleIdentifier</key>    <string>com.provisa.launcher</string>
    <key>CFBundleName</key>          <string>Provisa</string>
    <key>CFBundleDisplayName</key>   <string>Provisa</string>
    <key>CFBundleVersion</key>       <string>1</string>
    <key>CFBundleShortVersionString</key> <string>0.1.0</string>
    <key>CFBundlePackageType</key>   <string>APPL</string>
    <key>LSMinimumSystemVersion</key><string>13.0</string>
    <!-- LSUIElement: hide from Dock; show only in menu bar -->
    <key>LSUIElement</key>           <true/>
    <key>NSHighResolutionCapable</key><true/>
    <key>NSPrincipalClass</key>      <string>NSApplication</string>
    <key>NSHumanReadableCopyright</key>
        <string>Copyright © 2026 Kenneth Stott. All rights reserved.</string>
    <!-- Permissions -->
    <key>NSAppleEventsUsageDescription</key>
        <string>Provisa uses AppleScript to install the CLI tool.</string>
</dict>
</plist>
PLIST

echo "[launcher] Built: ${BUILT_APP}"

# ── Optionally integrate into Provisa.app ─────────────────────────────────────
if [[ -n "$APP_BUNDLE" ]]; then
  if [[ ! -d "$APP_BUNDLE" ]]; then
    echo "[launcher] Error: --app-bundle path not found: ${APP_BUNDLE}" >&2
    exit 1
  fi
  DST="${APP_BUNDLE}/Contents/MacOS/ProvisaLauncher.app"
  echo "[launcher] Embedding into ${DST}..."
  rm -rf "$DST"
  cp -rp "$BUILT_APP" "$DST"
  echo "[launcher] Done."
fi
