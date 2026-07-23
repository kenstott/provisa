#!/usr/bin/env bash
# Build mongo_fdw + its bundled mongo-c-driver 1.30.2 / json-c against the cached embedded PG 16.2,
# into ~/.cache/provisa-fdw (restart-safe: a second run is a no-op once the artifacts exist).
#
# mongo_fdw is REQ-1177's named PG conformance target. This reproduces the artifacts the mongo E2E
# (tests/integration/test_custom_connectors_mongo_e2e.py) installs into the embedded pgserver, so the
# config-driven pg_fdw descriptor can federate a live MongoDB — no skip, no substitution.
#
# Two macOS/cmake-4 quirks the stock autogen.sh does not survive, patched here:
#   * autogen.sh hardcodes `wget`; we shim it to curl when wget is absent.
#   * cmake 4 removed CMP0042 OLD support; the bundled libbson pins it — we flip it to NEW.
set -euo pipefail
export PATH="$HOME/homebrew/bin:$PATH"

CACHE="${PROVISA_FDW_CACHE:-$HOME/.cache/provisa-fdw}"
PGCONFIG="$CACHE/pg162/bin/pg_config"
PREFIX="$CACHE/mongo_fdw_deps"     # mongo-c-driver + json-c install prefix
SRC="$CACHE/mongo_fdw_src"
SHIM="$CACHE/_shim"

# Already built? (both driver dylibs + the FDW module + its control file present)
if [ -f "$PREFIX/lib/libmongoc-1.0.0.dylib" ] \
   && [ -f "$CACHE/pg162/lib/postgresql/mongo_fdw.dylib" ] \
   && [ -f "$CACHE/pg162/share/postgresql/extension/mongo_fdw.control" ]; then
  echo "mongo_fdw artifacts already cached — nothing to build"
  exit 0
fi

[ -x "$PGCONFIG" ] || { echo "FAIL: cached PG 16.2 not built ($PGCONFIG missing); build it first"; exit 1; }
mkdir -p "$CACHE" "$SHIM" "$PREFIX"

# autogen.sh hardcodes wget; provide a curl-backed shim when wget is unavailable.
if ! command -v wget >/dev/null 2>&1; then
  cat > "$SHIM/wget" <<'EOF'
#!/bin/bash
# minimal wget->curl shim: the single-URL download form autogen.sh uses
url="${!#}"
exec curl -fsSL -O "$url"
EOF
  chmod +x "$SHIM/wget"
  export PATH="$SHIM:$PATH"
fi

[ -d "$SRC" ] || git clone --depth 1 https://github.com/EnterpriseDB/mongo_fdw.git "$SRC"
cd "$SRC"

# cmake 4 dropped CMP0042 OLD; the bundled libbson sets it OLD → flip to NEW so configure succeeds.
export MONGOC_INSTALL_DIR="$PREFIX" JSONC_INSTALL_DIR="$PREFIX"
export CMAKE_POLICY_VERSION_MINIMUM=3.5
[ -f mongo-c-driver/src/libbson/CMakeLists.txt ] && \
  sed -i '' 's/cmake_policy(SET CMP0042 OLD)/cmake_policy(SET CMP0042 NEW)/' \
    mongo-c-driver/src/libbson/CMakeLists.txt || true

echo "=== autogen: build+install mongo-c-driver 1.30.2 + json-c ==="
bash ./autogen.sh
# If the libbson CMP0042 line only appeared after checkout, patch + rebuild the driver once.
if [ ! -f "$PREFIX/lib/libmongoc-1.0.0.dylib" ]; then
  sed -i '' 's/cmake_policy(SET CMP0042 OLD)/cmake_policy(SET CMP0042 NEW)/' \
    mongo-c-driver/src/libbson/CMakeLists.txt
  rm -rf mongo-c-driver/CMakeCache.txt mongo-c-driver/CMakeFiles
  bash ./autogen.sh
fi

echo "=== build mongo_fdw against embedded PG 16.2 ==="
export PKG_CONFIG_PATH="$PREFIX/lib/pkgconfig:$PREFIX/lib64/pkgconfig:${PKG_CONFIG_PATH:-}"
make USE_PGXS=1 PG_CONFIG="$PGCONFIG" clean || true
make USE_PGXS=1 PG_CONFIG="$PGCONFIG"
make USE_PGXS=1 PG_CONFIG="$PGCONFIG" install

echo "=== artifacts ==="
ls -la "$($PGCONFIG --pkglibdir)"/mongo_fdw* "$($PGCONFIG --sharedir)/extension"/mongo_fdw*
ls -la "$PREFIX/lib"/libmongoc-1.0.0.dylib "$PREFIX/lib"/libbson-1.0.0.dylib
echo "BUILD_OK"
