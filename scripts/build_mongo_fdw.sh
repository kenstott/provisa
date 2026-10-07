#!/usr/bin/env bash
# Build mongo_fdw + its bundled mongo-c-driver 1.30.2 / json-c against the cached embedded PG 16.2,
# into ~/.cache/provisa-fdw (restart-safe: a second run is a no-op once the artifacts exist).
#
# mongo_fdw is REQ-1177's named PG conformance target. This reproduces the artifacts the mongo E2E
# (tests/integration/test_custom_connectors_mongo_e2e.py) installs into the embedded pgserver, so the
# config-driven pg_fdw descriptor can federate a live MongoDB — no skip, no substitution.
#
# macOS and Linux. The driver libraries are named by platform (libmongoc-1.0.0.dylib /
# libmongoc-1.0.so.0), and so is how mongo_fdw finds them once the test has put them beside it in
# the pgserver install: on macOS the driver's install name is @rpath-relative and the installer adds
# an @loader_path rpath; on Linux the module is linked here with an $ORIGIN DT_RPATH (not RUNPATH:
# DT_RPATH also resolves libmongoc's own dependency on libbson).
#
# Two cmake-4 quirks the stock autogen.sh does not survive, patched here:
#   * autogen.sh hardcodes `wget`; we shim it to curl when wget is absent.
#   * cmake 4 removed CMP0042 OLD support; the bundled libbson pins it — we flip it to NEW.
set -euo pipefail
export PATH="$HOME/homebrew/bin:$PATH"

case "$(uname -s)" in
  Darwin) MODULE_SUFFIX=dylib; MONGOC_LIB=libmongoc-1.0.0.dylib; BSON_LIB=libbson-1.0.0.dylib; RPATH_FLAGS= ;;
  Linux)  MODULE_SUFFIX=so; MONGOC_LIB=libmongoc-1.0.so.0; BSON_LIB=libbson-1.0.so.0
          RPATH_FLAGS='-Wl,--disable-new-dtags,-rpath,\$$ORIGIN' ;;
  *) echo "FAIL: mongo_fdw build has no target for $(uname -s)"; exit 1 ;;
esac

# In-place sed, portable across BSD (macOS) and GNU (Linux) sed.
flip_cmp0042() {
  sed -i.bak 's/cmake_policy(SET CMP0042 OLD)/cmake_policy(SET CMP0042 NEW)/' "$1" && rm -f "$1.bak"
}

CACHE="${PROVISA_FDW_CACHE:-$HOME/.cache/provisa-fdw}"
PGCONFIG="$CACHE/pg162/bin/pg_config"
PREFIX="$CACHE/mongo_fdw_deps"     # mongo-c-driver + json-c install prefix
SRC="$CACHE/mongo_fdw_src"
SHIM="$CACHE/_shim"

# Already built? (both driver dylibs + the FDW module + its control file present)
if [ -f "$PREFIX/lib/$MONGOC_LIB" ] \
   && [ -f "$CACHE/pg162/lib/postgresql/mongo_fdw.$MODULE_SUFFIX" ] \
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
if [ -f mongo-c-driver/src/libbson/CMakeLists.txt ]; then
  flip_cmp0042 mongo-c-driver/src/libbson/CMakeLists.txt
fi

echo "=== autogen: build+install mongo-c-driver 1.30.2 + json-c ==="
bash ./autogen.sh
# If the libbson CMP0042 line only appeared after checkout, patch + rebuild the driver once.
if [ ! -f "$PREFIX/lib/$MONGOC_LIB" ]; then
  flip_cmp0042 mongo-c-driver/src/libbson/CMakeLists.txt
  rm -rf mongo-c-driver/CMakeCache.txt mongo-c-driver/CMakeFiles
  bash ./autogen.sh
fi

echo "=== build mongo_fdw against embedded PG 16.2 ==="
export PKG_CONFIG_PATH="$PREFIX/lib/pkgconfig:$PREFIX/lib64/pkgconfig:${PKG_CONFIG_PATH:-}"
make USE_PGXS=1 PG_CONFIG="$PGCONFIG" clean || true
make USE_PGXS=1 PG_CONFIG="$PGCONFIG" LDFLAGS_SL="$RPATH_FLAGS"
make USE_PGXS=1 PG_CONFIG="$PGCONFIG" LDFLAGS_SL="$RPATH_FLAGS" install

echo "=== artifacts ==="
ls -la "$($PGCONFIG --pkglibdir)"/mongo_fdw* "$($PGCONFIG --sharedir)/extension"/mongo_fdw*
ls -la "$PREFIX/lib/$MONGOC_LIB" "$PREFIX/lib/$BSON_LIB"
echo "BUILD_OK"
