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
  cp "${REPO_ROOT}/docker-compose.trino-internal-tls.yml" "${res}/docker-compose.trino-internal-tls.yml"  # REQ-2072
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
  # Wheel file names are [A-Za-z0-9._+-] only (PEP 427), so ls -t is safe for picking the newest.
  # shellcheck disable=SC2012
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
  # REQ-2071: provisa's pinned provisa-notebook, built here (PyPI has it only once released).
  local notebook_dir="${REPO_ROOT}/dist/notebook"
  rm -rf "$notebook_dir"
  "${REPO_ROOT}/scripts/build-notebook-wheel.sh" "$py" "$notebook_dir"
  "$py" -m pip install --quiet --find-links "$notebook_dir" "${built_wheel}[embedded]" uvicorn mcp-proxy
  # Fail the build loudly if a critical native-tier dep did not land (mirrors build-sfx.ps1).
  # REQ-2194: provisa's wheel carries its compiled encoders (provisa._encoders) and their notices.
  # -I: from the repository root, `-c` would import the checkout's provisa (no compiled module).
  "$py" -I -c "import provisa, sqlite3, mcp_proxy, provisa._encoders.pgwire, pathlib; \
assert (pathlib.Path(provisa.__file__).parent / '_encoders.THIRD_PARTY_NOTICES').is_file()" || {
    err "native runtime is missing provisa/sqlite3/mcp_proxy/provisa._encoders (or its notices) after install."
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

# ── Take the DuckDB extensions out of the native runtime (core edition) ──────
# DuckDB extensions are Mach-O with DuckDB's own signature appended after the image: codesign
# refuses them, re-signing would break the signature DuckDB loads them under, and Apple rejects the
# DMG that carries them unsigned ("The signature of the binary is invalid"). So the Core DMG ships
# none (maintainer decision 2026-11-11, "Separate release asset"): every extension file of the
# runtime (the blob, its .info, a repository's trust anchor) goes into DUCKDB_EXTENSIONS_ASSET at its
# path under the runtime, a release asset of its own. Its sha256 is baked into the .app before it is
# signed, and first-launch.sh restores the asset into the staged runtime only when it matches.
DUCKDB_EXTENSIONS_ASSET="${DUCKDB_EXTENSIONS_ASSET:-provisa-duckdb-extensions-${VERSION:-dev}-macOS-arm64.tar.gz}"
DUCKDB_EXTENSIONS_PIN="duckdb-extensions.sha256"   # in Contents/Resources: "<sha256>  <asset>"
split_duckdb_extensions() {
  local runtime="${NATIVE_PAYLOAD_DIR}/runtime" asset="${OUT_DIR}/${DUCKDB_EXTENSIONS_ASSET}"
  local files=() f
  while IFS= read -r f; do files+=("$f"); done \
    < <(cd "$runtime" && find . -type f -name '*.duckdb_extension*' | LC_ALL=C sort)
  if [ "${#files[@]}" -eq 0 ]; then
    err "No DuckDB extension in the native runtime: provisa[embedded] did not install them."
    exit 1
  fi
  mkdir -p "$OUT_DIR" "${APP_BUNDLE}/Contents/Resources"
  tar -czf "$asset" -C "$runtime" "${files[@]}"
  ( cd "$runtime" && rm -f "${files[@]}" )
  ( cd "$OUT_DIR" && shasum -a 256 "$DUCKDB_EXTENSIONS_ASSET" > "${DUCKDB_EXTENSIONS_ASSET}.sha256" )
  cp "${asset}.sha256" "${APP_BUNDLE}/Contents/Resources/${DUCKDB_EXTENSIONS_PIN}"
  ok "${#files[@]} DuckDB extension files moved to ${DUCKDB_EXTENSIONS_ASSET}."
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
# Apple notarization rejects unsigned .dylib/.jnilib/.so files nested inside JARs inside the
# app bundle. The Trino plugins are not in the bundle: they ship as hidden DMG content, outside
# the notarized .app, so notarytool never scans them and there is nothing to sign.
sign_jar_natives() {
  if [ -z "${APPLE_DEVELOPER_ID:-}" ]; then
    return
  fi
  info "Trino plugins are outside the app bundle (hidden DMG content) — JAR native signing not required."
}

# ── Code signing ──────────────────────────────────────────────────────────────
# The one way this script signs code: Developer ID, hardened runtime, Apple's secure timestamp,
# through codesign_retry. Arguments are extra codesign options, then the files to sign.
sign_code() {
  codesign_retry --force --sign "${APPLE_DEVELOPER_ID}" --options runtime --timestamp "$@"
}

# The Mach-O files under a directory, one per line, found by their magic number: the runtime's
# executables carry no extension, and a .so is not always one. A fat header shares Java's class-file
# magic (0xcafebabe); a fat binary's next word is its architecture count, a class file's its version
# (45 and up). Symlinks are skipped: codesign signs the file a link points at.
macho_files() {
  python3 -I - "$1" <<'PY'
import os, struct, sys
THIN = {b"\xcf\xfa\xed\xfe", b"\xce\xfa\xed\xfe", b"\xfe\xed\xfa\xcf", b"\xfe\xed\xfa\xce"}
for root, dirs, files in os.walk(sys.argv[1]):
    dirs.sort()
    for name in sorted(files):
        path = os.path.join(root, name)
        if os.path.islink(path):
            continue
        with open(path, "rb") as f:
            head = f.read(8)
        if head[:4] in THIN or (
            head[:4] in (b"\xca\xfe\xba\xbe", b"\xbe\xba\xfe\xca")
            and len(head) == 8
            and 0 < struct.unpack(">I" if head[0] == 0xCA else "<I", head[4:])[0] < 45
        ):
            print(path)
PY
}

sign_app() {
  if [ -z "${APPLE_DEVELOPER_ID:-}" ]; then
    info "APPLE_DEVELOPER_ID not set — skipping signing."
    return
  fi

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
    sign_code "$f"
    info "  Signed: ${f#"${APP_BUNDLE}/"}"
  done

  info "Signing app bundle..."
  sign_code --verbose --entitlements "${SCRIPT_DIR}/entitlements.plist" "${APP_BUNDLE}"

  ok "App bundle signed."
}

# ── Sign the native runtime payload (core edition) ───────────────────────────
# The runtime ships in the DMG beside the .app, and the DMG is notarized, so Apple checks every
# Mach-O in it: the interpreter, libpython, and each extension module pip installed. Each is signed
# like the .app's own code (sign_code); executables also take the entitlements the .app is signed
# with. Batches keep every codesign call under CODESIGN_TIMEOUT while not paying one process per file.
SIGN_BATCH="${SIGN_BATCH:-25}"
sign_native_payload() {
  if [ -z "${APPLE_DEVELOPER_ID:-}" ]; then
    info "APPLE_DEVELOPER_ID not set — skipping native runtime signing."
    return
  fi
  local runtime="${NATIVE_PAYLOAD_DIR}/runtime"
  local f kind libraries=() executables=() i
  while IFS= read -r f; do
    kind="$(file -b "$f")"
    if [[ "$kind" == *executable* ]]; then executables+=("$f"); else libraries+=("$f"); fi
  done < <(macho_files "$runtime")
  info "Signing the native runtime (${#libraries[@]} libraries, ${#executables[@]} executables)..."
  for ((i = 0; i < ${#libraries[@]}; i += SIGN_BATCH)); do
    sign_code "${libraries[@]:i:SIGN_BATCH}"
  done
  # Executables last: each loads the libraries above.
  for ((i = 0; i < ${#executables[@]}; i += SIGN_BATCH)); do
    sign_code --entitlements "${SCRIPT_DIR}/entitlements.plist" "${executables[@]:i:SIGN_BATCH}"
  done
  ok "Native runtime signed."
}

# ── Verify the handoff: the .app and every signed Mach-O of the payload ──────
# Run by the notarize job right after it unpacks stage-<edition>.tar, before anything goes to
# Apple: a signature the handoff broke, or a binary left unsigned, fails here and names the file.
# Each must verify strictly, chain to Apple (Developer ID, not ad hoc), and carry a secure timestamp.
verify_signed() {
  local f details bad=0
  if ! codesign --verify --deep --strict "${APP_BUNDLE}"; then
    err "${APP_BUNDLE} does not verify after the handoff."
    bad=1
  fi
  if [ "$EDITION" = "core" ]; then
    # The DuckDB extensions ship as their own release asset (split_duckdb_extensions), never here.
    while IFS= read -r f; do
      err "${f#"${SCRIPT_DIR}/"} is a DuckDB extension: it ships in ${DUCKDB_EXTENSIONS_ASSET}, not the DMG."
      bad=1
    done < <(find "${NATIVE_PAYLOAD_DIR}/runtime" -type f -name '*.duckdb_extension*')
    while IFS= read -r f; do
      [[ "$f" == *.duckdb_extension ]] && continue   # named above
      # codesign -dv prints "Timestamp=" for a secure timestamp, "Signed Time=" for a local clock.
      details="$(codesign -dv "$f" 2>&1)" || details=""
      if ! codesign --verify --strict -R='anchor apple generic' "$f" \
         || [[ "$details" != *$'\n'Timestamp=* ]]; then
        err "${f#"${SCRIPT_DIR}/"} is not Developer ID signed with a secure timestamp after the handoff."
        bad=1
      fi
    done < <(macho_files "${NATIVE_PAYLOAD_DIR}/runtime")
  fi
  if [ "$bad" -ne 0 ]; then exit 1; fi
  ok "The .app$([ "$EDITION" = "core" ] && echo " and the native runtime") verify."
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
    local ext
    ext="$(find "${NATIVE_PAYLOAD_DIR}/runtime" -type f -name '*.duckdb_extension*' | head -1)"
    if [ -n "$ext" ]; then
      err "${ext} is a DuckDB extension: it ships in ${DUCKDB_EXTENSIONS_ASSET}, not the DMG (split_duckdb_extensions)."
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

# ── Stage: hand the signed .app (and the native payload) to the notarize job ──
# The DMG is made from the .app AFTER notarize-dmg.sh --app has stapled it, so the .app carries
# its own ticket and launches offline once copied out of the DMG. tar, not the artifact zip,
# keeps the bundle's symlinks and modes, which its signature seals.
STAGE_TAR="${OUT_DIR}/stage-${EDITION}.tar"
stage_app() {
  mkdir -p "$OUT_DIR"
  rm -f "$STAGE_TAR"
  local members=(Provisa.app)
  if [ "$EDITION" = "core" ]; then members+=(native-payload/runtime); fi
  tar -cf "$STAGE_TAR" -C "$SCRIPT_DIR" "${members[@]}"
  ok "Staged signed app: ${STAGE_TAR}"
}

# ── Main ──────────────────────────────────────────────────────────────────────
# Steps (argument):
#   stage  build and sign Provisa.app, write dist/stage-<edition>.tar. CI's build job.
#   dmg    create the DMG from an already built (and, in CI, notarized + stapled) Provisa.app
#          unpacked beside this script. CI's notarize job.
#   verify check the unpacked handoff: the .app and the native runtime are Developer ID signed.
#          CI's notarize job, before it submits anything.
#   (none) both, for a local build; nothing is notarized.
main() {
  local step="${1:-all}"
  case "$step" in
    stage|dmg|all) ;;
    verify) verify_signed; return ;;
    *) err "usage: build-dmg.sh [stage|dmg|verify]"; exit 2 ;;
  esac
  printf '\n%bProvisa DMG Builder%b\n' "$BOLD" "$NC"
  printf "═══════════════════════════════════════════\n\n"

  check_prereqs
  generate_assets

  if [ "$step" != "dmg" ]; then
    build_ui             # provisa-ui/dist + provisa-cli (both editions)
    if [ "$EDITION" = "container" ]; then
      embed_compose        # compose YAMLs, config, db, trino, observability
      download_otel_agent  # opentelemetry-javaagent.jar into Resources/observability/trino-otel/
    else
      bundle_native_payload # interpreter with provisa[embedded] + UI installed
      split_duckdb_extensions # extensions -> their own release asset; its sha256 into Resources
    fi
    build_launcher       # compile SwiftUI launcher and embed binary
    embed_scripts
    bake_version         # write Contents/Resources/VERSION before signing seals the bundle
    sign_jar_natives  # sign macOS natives inside Trino plugin JARs before outer bundle signing
    sign_app
    if [ "$EDITION" = "core" ]; then
      sign_native_payload # every Mach-O of the hidden runtime, before the stage tar carries it
    fi
  fi
  if [ "$step" = "stage" ]; then
    stage_app
    printf '\n%b%bBuild complete.%b\n' "$GREEN" "$BOLD" "$NC"
    return
  fi
  # The DMG bundles Provisa.app + the hidden native payload; notarize-dmg.sh notarizes it.
  create_dmg

  printf '\n%b%bBuild complete.%b\n' "$GREEN" "$BOLD" "$NC"
  printf "DMG: %s\n" "${DMG_PATH}"
}

main "$@"
