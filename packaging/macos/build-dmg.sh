#!/usr/bin/env bash
# Build the macOS DMG. One script, two editions — the same split Windows ships:
#
#   core      (build-sfx.ps1)      native tier only: a python-build-standalone interpreter
#                                  with provisa + deps ALREADY pip-installed into it.
#                                  No compose, no observability, no container images.
#   container (build-container.ps1) Docker tier only: the compose tree (compose YAMLs,
#                                  config, db, trino minus plugins, observability + the
#                                  OTel Java agent). Images are fetched on demand.
#
# Select with PROVISA_EDITION=core|container.
# Requires: hdiutil, codesign, xcrun, python3
set -euo pipefail

EDITION="${PROVISA_EDITION:-core}"
case "$EDITION" in
  core|container) ;;
  *) printf 'PROVISA_EDITION must be core or container (got %s)\n' "$EDITION" >&2; exit 1 ;;
esac

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
OUT_DIR="${SCRIPT_DIR}/dist"
APP_BUNDLE="${SCRIPT_DIR}/Provisa.app"
if [ "$EDITION" = "container" ]; then
  DMG_NAME="Provisa-Container.dmg"
else
  DMG_NAME="Provisa.dmg"
fi
DMG_PATH="${OUT_DIR}/${DMG_NAME}"
# LZMA-compressed DMG. The default UDZO (zlib) leaves ~40% on the table over a tree of
# many small Python files; ULMO is what gets the DMG to Inno Setup's lzma2/ultra64 class.
DMG_FORMAT="${DMG_FORMAT:-ULMO}"

RED='\033[0;31m'; GREEN='\033[0;32m'; CYAN='\033[0;36m'; BOLD='\033[1m'; NC='\033[0m'
info() { printf "${CYAN}[build-dmg]${NC} %s\n" "$*"; }
ok()   { printf "${GREEN}[build-dmg]${NC} %s\n" "$*"; }
err()  { printf "${RED}[build-dmg]${NC} %s\n" "$*" >&2; }

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

# codesign's --timestamp flag contacts Apple's secure timestamp server, and that wait has no
# bound of its own: on v0.1.0-alpha.370 the first call blocked for 39 minutes until the job's
# 45-minute limit cancelled the build, leaving an orphan codesign process and no release. The
# timestamp is required for notarization, so the answer is not to drop it but to cap how long a
# single attempt may wait and try again. macOS ships no coreutils `timeout`, hence the watchdog.
CODESIGN_TIMEOUT="${CODESIGN_TIMEOUT:-180}"
codesign_retry() {
  local attempt pid waited
  for attempt in 1 2 3 4 5; do
    codesign "$@" &
    pid=$!
    waited=0
    while kill -0 "$pid" 2>/dev/null && [ "$waited" -lt "$CODESIGN_TIMEOUT" ]; do
      sleep 2
      waited=$((waited + 2))
    done
    if kill -0 "$pid" 2>/dev/null; then
      kill -9 "$pid" 2>/dev/null || true
      wait "$pid" 2>/dev/null || true
      info "codesign attempt ${attempt} exceeded ${CODESIGN_TIMEOUT}s (timestamp server unresponsive), retrying..."
      continue
    fi
    if wait "$pid"; then
      return 0
    fi
    info "codesign attempt ${attempt} failed, retrying in 10s..."
    sleep 10
  done
  err "codesign failed after 5 attempts: $*"
  exit 1
}

# ── Prerequisites ─────────────────────────────────────────────────────────────
check_prereqs() {
  for cmd in curl hdiutil codesign python3; do
    if ! command -v "$cmd" &>/dev/null; then
      err "Required tool not found: ${cmd}"
      exit 1
    fi
  done
  # create-dmg — install via brew if absent
  if ! command -v create-dmg &>/dev/null; then
    info "Installing create-dmg..."
    brew install create-dmg --quiet
  fi
  ok "All prerequisites found."
}

# ── Generate app icon and DMG background ─────────────────────────────────────
generate_assets() {
  info "Generating icon and DMG background..."

  # Use an isolated venv to avoid PEP 668 restrictions on managed Python installs
  local venv="${SCRIPT_DIR}/.build-venv"
  if [ ! -x "${venv}/bin/python3" ]; then
    python3 -m venv "$venv"
  fi
  "${venv}/bin/pip" install pillow --quiet --upgrade

  "${venv}/bin/python3" "${SCRIPT_DIR}/generate-icon.py" "${SCRIPT_DIR}"
  "${venv}/bin/python3" "${SCRIPT_DIR}/generate-dmg-background.py" "${SCRIPT_DIR}"

  # Copy icon into app bundle
  local icns_src="${SCRIPT_DIR}/Provisa.icns"
  local icns_dst="${APP_BUNDLE}/Contents/Resources/AppIcon.icns"
  mkdir -p "$(dirname "$icns_dst")"
  cp "$icns_src" "$icns_dst"
  ok "Icon and background generated."
}

# ── Download OTel Java agent for Trino (container edition only) ──────────────
# Downloaded at build time (network available on build host); bundled in Resources
# so first-launch.sh can copy it into ~/.provisa/compose/observability/trino-otel/
# without any network access at install time. The core edition runs no Trino, so it
# ships no agent — same as the Windows native installer.
download_otel_agent() {
  local dest="${APP_BUNDLE}/Contents/Resources/observability/trino-otel"
  local jar="${dest}/opentelemetry-javaagent.jar"
  if [ -f "$jar" ]; then
    info "OTel Java agent cached — skipping."
    return
  fi
  mkdir -p "$dest"
  info "Downloading OTel Java agent for Trino..."
  curl_retry \
    "https://github.com/open-telemetry/opentelemetry-java-instrumentation/releases/latest/download/opentelemetry-javaagent.jar" \
    "$jar"
  ok "OTel Java agent bundled ($(du -sh "$jar" | cut -f1))."
}

# ── Embed the compose tree (container edition only) ──────────────────────────
# Mirrors packaging/windows/build-container.ps1: the Docker tier's compose YAMLs and
# config live in the installer; the images are fetched on demand. The core edition
# ships none of this — it has no Docker tier, exactly like build-sfx.ps1.
embed_compose() {
  local res="${APP_BUNDLE}/Contents/Resources"
  mkdir -p "$res"
  cp "${REPO_ROOT}/docker-compose.core.yml" "${res}/docker-compose.core.yml"
  cp "${REPO_ROOT}/docker-compose.app.yml" "${res}/docker-compose.app.yml"
  cp "${REPO_ROOT}/docker-compose.airgap.yml" "${res}/docker-compose.airgap.yml"
  cp -r "${REPO_ROOT}/config" "${res}/config"
  cp -r "${REPO_ROOT}/db" "${res}/db"
  # Copy trino WITHOUT plugins/ — plugins ship as a separate release asset
  # (provisa-trino-plugins-*.tar.gz), same as the Windows container installer.
  mkdir -p "${res}/trino"
  rsync -a --exclude='plugins/' "${REPO_ROOT}/trino/" "${res}/trino/"
  cp -r "${REPO_ROOT}/observability" "${res}/observability"
  ok "Compose files, config, and observability embedded."
}

# ── Build the React UI ───────────────────────────────────────────────────────
# bundle_native_payload stages provisa-ui/dist into the runtime's site-packages (the
# native tier's ui_server serves it). The prebuild renders the offline MkDocs docs
# site; point it at the build venv's mkdocs so no global install is needed.
build_ui() {
  local res="${APP_BUNDLE}/Contents/Resources"
  mkdir -p "$res"
  cp "${REPO_ROOT}/scripts/provisa" "${res}/provisa-cli"
  chmod +x "${res}/provisa-cli"
  info "Building React UI..."
  local venv="${SCRIPT_DIR}/.build-venv"
  "${venv}/bin/pip" install mkdocs-material pymdown-extensions mkdocs-static-i18n --quiet --upgrade
  (cd "${REPO_ROOT}/provisa-ui" \
    && MKDOCS_BIN="${venv}/bin/mkdocs" PYTHON_BIN="${venv}/bin/python3" \
       npm ci --silent && MKDOCS_BIN="${venv}/bin/mkdocs" PYTHON_BIN="${venv}/bin/python3" npm run build)
  ok "React UI built."
}

# ── Stage the native runtime payload (REQ-979) ───────────────────────────────
# The native (no-Docker) tier runs a python-build-standalone interpreter that already
# HAS provisa installed: we pip-install into it here, at build time, exactly like the
# Windows installer (build-sfx.ps1). first-launch.sh copies the tree to
# ~/.provisa/venv and runs it — no venv creation, no pip, no PyPI at install time.
#
# Shipping a wheelhouse instead cost the DMG roughly double: every dependency rode
# along twice (as a .whl AND as the interpreter that would unpack it), and wheels are
# already-deflated zips that neither UDZO nor LZMA can compress a second time.
#
# The tree ships as HIDDEN DMG content (create_dmg copies it to the DMG root, outside
# the notarized .app); first-launch.sh finds it at /Volumes/*/runtime.
#
# Pins are overridable so the builder can bump CPython without editing this file.
PBS_RELEASE="${PBS_RELEASE:-20250612}"
PBS_PYTHON="${PBS_PYTHON:-3.12.11}"
NATIVE_PAYLOAD_DIR="${SCRIPT_DIR}/native-payload"   # staged OUTSIDE the .app (hidden DMG content)
bundle_native_payload() {
  local runtime="${NATIVE_PAYLOAD_DIR}/runtime"

  # ── 1. python-build-standalone interpreter ──
  rm -rf "$runtime"; mkdir -p "$NATIVE_PAYLOAD_DIR"
  local tarball="cpython-${PBS_PYTHON}+${PBS_RELEASE}-aarch64-apple-darwin-install_only.tar.gz"
  local url="https://github.com/astral-sh/python-build-standalone/releases/download/${PBS_RELEASE}/${tarball}"
  local tmp="${SCRIPT_DIR}/tmp-pbs"
  local cached="${SCRIPT_DIR}/.pbs-cache/${tarball}"
  mkdir -p "$(dirname "$cached")"
  if [ ! -f "$cached" ]; then
    info "Downloading python-build-standalone ${PBS_PYTHON} (macOS arm64)..."
    curl_retry "$url" "$cached"
  fi
  rm -rf "$tmp"; mkdir -p "$tmp"
  tar -xzf "$cached" -C "$tmp"                    # extracts to ${tmp}/python/
  if [ ! -x "${tmp}/python/bin/python3" ]; then
    err "python-build-standalone extraction failed (no bin/python3)"
    exit 1
  fi
  mv "${tmp}/python" "$runtime"
  rm -rf "$tmp"
  local py="${runtime}/bin/python3"

  # ── 2. Build the provisa wheel and install it INTO the runtime ──
  info "Building the provisa wheel (macOS)..."
  "$py" -m pip install --quiet --upgrade pip
  "$py" -m pip install --quiet build
  # embed_compose already built provisa-ui/dist; reuse it (no re-run of vite).
  if [ -x "${REPO_ROOT}/scripts/build-wheel.sh" ]; then
    PROVISA_SKIP_UI_BUILD=1 PYTHON="$py" "${REPO_ROOT}/scripts/build-wheel.sh" --wheel
  else
    ( cd "$REPO_ROOT" && "$py" -m build --wheel )
  fi
  local built_wheel
  built_wheel="$(ls -t "${REPO_ROOT}/dist"/provisa-*.whl 2>/dev/null | head -1)"
  if [ -z "$built_wheel" ] || [ ! -f "$built_wheel" ]; then
    err "provisa wheel not found in ${REPO_ROOT}/dist after build."
    exit 1
  fi
  info "Installing provisa[embedded] + deps into the native runtime..."
  # `embedded` is the base extra set. The data-quality checker (REQ-1443) is NOT baked in:
  # soda-core is Elastic License 2.0 and is never vendored, so first-launch installs the
  # operator's chosen checker on top of this tree.
  # mcp-proxy (REQ-1104): Node-free stdio<->Streamable-HTTP bridge for the Claude Desktop connector.
  "$py" -m pip install --quiet "${built_wheel}[embedded]" uvicorn mcp-proxy
  # Fail the build loudly if a critical native-tier dep did not land (mirrors build-sfx.ps1).
  "$py" -c "import provisa, aiosqlite, mcp_proxy" || {
    err "native runtime is missing provisa/aiosqlite/mcp_proxy after install."
    exit 1
  }
  # `build` is a build-host tool — it has no business in the shipped tree.
  "$py" -m pip uninstall --quiet --yes build

  # ── 3. Built UI → <site-packages>/static, where ui_server resolves it ──
  if [ ! -d "${REPO_ROOT}/provisa-ui/dist" ]; then
    err "provisa-ui/dist not found — embed_compose must build the UI before bundle_native_payload."
    exit 1
  fi
  local site; site="$("$py" -c 'import sysconfig;print(sysconfig.get_paths()["purelib"])')"
  mkdir -p "${site}/static"
  cp -R "${REPO_ROOT}/provisa-ui/dist/." "${site}/static/"

  # Drop build-time noise that only inflates the DMG: bytecode is regenerated on first
  # import, and pip's http cache is not part of the runtime.
  find "$runtime" -name '__pycache__' -type d -prune -exec rm -rf {} + 2>/dev/null || true
  rm -rf "${runtime}/lib/python"*/site-packages/pip/_vendor/certifi/__pycache__ 2>/dev/null || true

  printf '%s' "${VERSION:-dev}" > "${runtime}/.runtime-version"
  ok "Native runtime staged ($(du -sh "$runtime" | cut -f1), provisa installed)."
}

# ── Build SwiftUI launcher and embed binary ───────────────────────────────────
build_launcher() {
  info "Building ProvisaLauncher (Swift)..."
  local launcher_dir="${SCRIPT_DIR}/ProvisaLauncher"
  swift build --package-path "$launcher_dir" -c release 2>&1 | grep -v "^Build complete"
  local binary="${launcher_dir}/.build/release/ProvisaLauncher"
  if [ ! -f "$binary" ]; then
    err "ProvisaLauncher binary not found after build: ${binary}"
    exit 1
  fi
  # Replace old shell launcher with native Swift binary. Ensure Contents/MacOS
  # exists — it used to be created implicitly by the (now-removed) BIN_DIR mkdir.
  mkdir -p "${APP_BUNDLE}/Contents/MacOS"
  rm -f "${APP_BUNDLE}/Contents/MacOS/provisa-launcher"
  cp "$binary" "${APP_BUNDLE}/Contents/MacOS/ProvisaLauncher"
  chmod +x "${APP_BUNDLE}/Contents/MacOS/ProvisaLauncher"
  ok "ProvisaLauncher built and embedded."
}

# ── Copy first-launch script ──────────────────────────────────────────────────
embed_scripts() {
  cp "${SCRIPT_DIR}/first-launch.sh" "${APP_BUNDLE}/Contents/MacOS/first-launch.sh"
  chmod +x "${APP_BUNDLE}/Contents/MacOS/first-launch.sh"
  ok "Scripts embedded."
}

# ── Bake the release version into the bundle ─────────────────────────────────
# first-launch.sh reads Contents/Resources/VERSION to stamp the installed tier.
# VERSION is the release tag (github.ref_name); falls back to 'dev' for local builds.
bake_version() {
  local res="${APP_BUNDLE}/Contents/Resources"
  mkdir -p "$res"
  printf '%s' "${VERSION:-dev}" > "${res}/VERSION"
  ok "Version baked: ${VERSION:-dev}"
}

# ── Sign macOS native binaries embedded inside JARs ──────────────────────────
# Apple notarization rejects unsigned .dylib/.jnilib/.so files even when they
# are nested inside JARs inside the app bundle. We extract, sign, and repack.
sign_jar_natives() {
  if [ -z "${APPLE_DEVELOPER_ID:-}" ]; then
    return
  fi

  local id="${APPLE_DEVELOPER_ID}"
  local sign_flags=(--force --sign "$id" --options runtime --timestamp)
  # Plugins are now in REPO_ROOT/trino/plugins (not inside app bundle)
  # They are bundled as hidden DMG content, outside the notarized .app,
  # so Apple notarytool will not scan them. Signing is skipped.
  info "Trino plugins are outside the app bundle (hidden DMG content) — JAR native signing not required."
  return
  # (unreachable — kept for reference if plugins are moved back inside bundle)
  local plugins_dir="${APP_BUNDLE}/Contents/Resources/trino/plugins"

  if [ ! -d "$plugins_dir" ]; then
    info "No trino/plugins directory — skipping JAR native signing."
    return
  fi

  info "Signing macOS native binaries inside Trino plugin JARs..."
  local tmp_jar_dir
  tmp_jar_dir=$(mktemp -d)

  local jar_count=0
  local signed_count=0

  while IFS= read -r -d '' jar; do
    local jar_tmp="${tmp_jar_dir}/$(basename "$jar" .jar)-$$"
    mkdir -p "$jar_tmp"

    # Extract only macOS native files
    local natives
    natives=$(unzip -l "$jar" 2>/dev/null \
      | awk '{print $NF}' \
      | grep -E '\.(dylib|jnilib)$|/osx[_/]|/mac[_/]|/darwin[_/]|/Mac[_/]|so_osx' \
      | grep -v '^$' || true)

    if [ -z "$natives" ]; then
      rm -rf "$jar_tmp"
      continue
    fi

    # Extract those files
    local extracted=0
    while IFS= read -r entry; do
      [ -z "$entry" ] && continue
      unzip -q "$jar" "$entry" -d "$jar_tmp" 2>/dev/null && extracted=$((extracted + 1))
    done <<< "$natives"

    if [ "$extracted" -eq 0 ]; then
      rm -rf "$jar_tmp"
      continue
    fi

    # Sign each extracted native binary
    local jar_signed=0
    while IFS= read -r -d '' native; do
      codesign "${sign_flags[@]}" "$native" 2>/dev/null && jar_signed=$((jar_signed + 1))
    done < <(find "$jar_tmp" -type f \( -name "*.dylib" -o -name "*.jnilib" -o -name "*.so_osx*" \) -print0)

    if [ "$jar_signed" -gt 0 ]; then
      # Repack — update jar in place with signed binaries
      (cd "$jar_tmp" && zip -u "$jar" $(find . -type f \( -name "*.dylib" -o -name "*.jnilib" -o -name "*.so_osx*" \) | sed 's|^\./||') 2>/dev/null)
      signed_count=$((signed_count + jar_signed))
      jar_count=$((jar_count + 1))
      info "  Signed ${jar_signed} native(s) in $(basename "$jar")"
    fi

    rm -rf "$jar_tmp"
  done < <(find "$plugins_dir" -name "*.jar" -print0)

  rm -rf "$tmp_jar_dir"
  ok "JAR native signing complete: ${signed_count} binaries in ${jar_count} JARs."
}

# ── Code signing ──────────────────────────────────────────────────────────────
sign_app() {
  if [ -z "${APPLE_DEVELOPER_ID:-}" ]; then
    info "APPLE_DEVELOPER_ID not set — skipping signing."
    return
  fi

  local id="${APPLE_DEVELOPER_ID}"
  local sign_flags=(--force --sign "$id" --options runtime --timestamp)

  # Apple requires every code object inside the bundle to be signed with
  # Developer ID + secure timestamp before the outer bundle is signed.
  # --deep is NOT used: it doesn't reliably propagate --timestamp.
  # Sign innermost files first, then the outer bundle.
  # Sign all code objects in explicit dependency order (innermost first).
  # find is not used because it doesn't guarantee order, and codesign requires
  # every subcomponent to be signed before the file that contains/calls it.
  info "Signing bundled executables (inner → outer)..."

  local sign_targets=(
    "${APP_BUNDLE}/Contents/MacOS/first-launch.sh"
    "${APP_BUNDLE}/Contents/MacOS/ProvisaLauncher"
  )
  for f in "${sign_targets[@]}"; do
    [ -f "$f" ] || continue
    codesign_retry "${sign_flags[@]}" "$f"
    info "  Signed: ${f#"${APP_BUNDLE}/"}"
  done

  info "Signing app bundle..."
  codesign_retry "${sign_flags[@]}" --verbose \
    --entitlements "${SCRIPT_DIR}/entitlements.plist" \
    "${APP_BUNDLE}"

  ok "App bundle signed."
}

# ── Notarization (targets the .app bundle, NOT the DMG) ──────────────────────
# Images are kept outside the .app so the bundle is small and notarizes in
# seconds rather than the 45-60 min needed to scan a 1.4 GB DMG.
notarize_app() {
  if [ -z "${APPLE_NOTARYTOOL_APPLE_ID:-}" ]; then
    info "APPLE_NOTARYTOOL_APPLE_ID not set — skipping notarization."
    return
  fi

  local notary_args=(
    --apple-id "${APPLE_NOTARYTOOL_APPLE_ID}"
    --password "${APPLE_NOTARYTOOL_PASSWORD}"
    --team-id  "${APPLE_NOTARYTOOL_TEAM_ID}"
  )

  # notarytool requires a zip, pkg, or dmg — zip the .app with ditto
  local zip_path="${OUT_DIR}/Provisa-notarize.zip"
  info "Zipping app bundle for notarization submission..."
  ditto -c -k --keepParent "${APP_BUNDLE}" "$zip_path"

  info "Submitting app bundle for notarization..."
  local submit_out submit_err submit_rc
  submit_out=$(xcrun notarytool submit "$zip_path" "${notary_args[@]}" --output-format json 2>/tmp/notary-submit-err) \
    && submit_rc=0 || submit_rc=$?
  submit_err=$(cat /tmp/notary-submit-err 2>/dev/null || true)
  rm -f "$zip_path" /tmp/notary-submit-err
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

  # Poll with retry — notarytool --wait has no retry on transient network errors
  local max_polls=45   # 45 × 40 s = 30 min ceiling (small app should be fast)
  local poll=0
  local status=""
  while [ $poll -lt $max_polls ]; do
    status=$(xcrun notarytool info "$submission_id" "${notary_args[@]}" \
               --output-format json 2>/dev/null \
             | python3 -c "import sys,json; print(json.load(sys.stdin).get('status',''))" \
             2>/dev/null || echo "network-error")

    case "$status" in
      Accepted)
        ok "Notarization accepted."
        break ;;
      Invalid|Rejected)
        err "Notarization ${status}:"
        xcrun notarytool log "$submission_id" "${notary_args[@]}" >&2 || true
        exit 1 ;;
      network-error)
        info "  Network error — retrying in 40 s (poll $((poll+1))/${max_polls})..." ;;
      *)
        info "  Status: ${status:-unknown} (poll $((poll+1))/${max_polls})" ;;
    esac

    sleep 40
    poll=$((poll + 1))
  done

  if [ "$status" != "Accepted" ]; then
    err "Notarization timed out after $((max_polls * 40 / 60)) minutes."
    exit 1
  fi

  xcrun stapler staple "${APP_BUNDLE}"
  ok "App bundle notarized and stapled."
}

# ── Create DMG ────────────────────────────────────────────────────────────────
create_dmg() {
  info "Creating DMG..."
  mkdir -p "$OUT_DIR"
  local tmp_dmg="${OUT_DIR}/tmp-provisa"
  rm -rf "$tmp_dmg"
  mkdir -p "$tmp_dmg"
  cp -r "${APP_BUNDLE}" "${tmp_dmg}/Provisa.app"

  # Slim base (REQ-979): the core container images are NOT bundled in the DMG — that
  # kept the default (native) install under GitHub's 2 GB asset limit once the native
  # runtime is bundled. The Docker/Trino tier fetches them via first-launch's
  # acquire_addon from the published provisa-core-images-<version>.tar.gz (or a copy
  # pre-staged beside the installer for airgapped installs).

  if [ "$EDITION" = "core" ]; then
    # Native tier payload (hidden): the interpreter with provisa already installed.
    # first-launch.sh copies it to ~/.provisa/venv — it neither builds a venv nor pips.
    if [ ! -x "${NATIVE_PAYLOAD_DIR}/runtime/bin/python3" ]; then
      err "Native runtime missing at ${NATIVE_PAYLOAD_DIR}/runtime — bundle_native_payload must run first."
      exit 1
    fi
    cp -R "${NATIVE_PAYLOAD_DIR}/runtime" "${tmp_dmg}/runtime"
    chflags hidden "${tmp_dmg}/runtime"
  fi

  # Remove any existing DMG so create-dmg doesn't complain
  rm -f "${DMG_PATH}"

  create-dmg \
    --volname "$([ "$EDITION" = "container" ] && echo "Provisa Container" || echo "Provisa")" \
    --volicon "${SCRIPT_DIR}/Provisa.icns" \
    --background "${SCRIPT_DIR}/dmg-background.png" \
    --window-pos 200 120 \
    --window-size 660 400 \
    --icon-size 128 \
    --icon "Provisa.app" 165 230 \
    --hide-extension "Provisa.app" \
    --app-drop-link 495 230 \
    --format "${DMG_FORMAT}" \
    "${DMG_PATH}" \
    "${tmp_dmg}/"

  rm -rf "$tmp_dmg"
  ok "DMG created: ${DMG_PATH}"
}

# ── Main ──────────────────────────────────────────────────────────────────────
main() {
  printf "\n${BOLD}Provisa DMG Builder${NC}\n"
  printf "═══════════════════════════════════════════\n\n"

  check_prereqs

  generate_assets
  build_ui             # provisa-ui/dist + provisa-cli (both editions)
  if [ "$EDITION" = "container" ]; then
    embed_compose        # compose YAMLs, config, db, trino, observability
    download_otel_agent  # opentelemetry-javaagent.jar into Resources/observability/trino-otel/
  else
    bundle_native_payload # interpreter with provisa[embedded] + UI installed
  fi
  build_launcher       # compile SwiftUI launcher and embed binary
  embed_scripts
  bake_version         # write Contents/Resources/VERSION before signing seals the bundle
  sign_jar_natives  # sign macOS natives inside Trino plugin JARs before outer bundle signing
  sign_app
  notarize_app   # notarize the small .app before images are added
  create_dmg     # DMG bundles Provisa.app (notarized) + hidden native payload

  printf "\n${GREEN}${BOLD}Build complete.${NC}\n"
  printf "DMG: %s\n" "${DMG_PATH}"
}

main "$@"
