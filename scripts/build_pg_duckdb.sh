#!/usr/bin/env bash
# Build pg_duckdb (v1.1.1 embeds DuckDB v1.4.3) against our cached PG 16.11 source build,
# so the artifact drops into pgserver's embedded PG 16.11 (same major). Long build: compiles DuckDB.
# Proven install route is the same as file_fdw/postgres_fdw (scripts/prove_embedded_fdw.sh).
set -euo pipefail

CACHE="${PROVISA_FDW_CACHE:-$HOME/.cache/provisa-fdw}"
PREFIX="$CACHE/pg1611"
PGC="$PREFIX/bin/pg_config"
[ -x "$PGC" ] || { echo "FAIL: cached PG 16.11 not built at $PREFIX (run prove_embedded_fdw.sh first)"; exit 1; }

# pg_duckdb calls ExecCheckOneRelPerms from v1.1.0 on (src/pgduckdb_planner.cpp), which PostgreSQL
# declares from 16.10 on: v1.1.x needs pixeltable-pgserver's 16.11, not pgserver 0.1.4's 16.2 (REQ-901).
TAG="${PG_DUCKDB_TAG:-v1.1.1}"
# The extensions compiled in follow the tag's DuckDB: httpfs at the commit that pg_duckdb release
# pins (third_party/pg_duckdb_extensions.cmake), iceberg at the commit that DuckDB release pins
# (.github/config/extensions/iceberg.cmake), since pg_duckdb pins none.
case "$TAG" in
  v1.1.1) HTTPFS=9c7d34977b10346d0b4cbbde5df807d1dab0b2bf; ICEBERG=1c0c4c60818f58b603fc50d5267b1f1202fe5484 ;;  # DuckDB v1.4.3
  *) echo "FAIL: no extension pins for pg_duckdb $TAG"; exit 1 ;;
esac
SRC="$CACHE/pg_duckdb-$TAG"
echo "== 1. clone pg_duckdb $TAG + duckdb submodule (shallow) =="
if [ ! -d "$SRC/.git" ]; then
  git clone --depth 1 --branch "$TAG" --recurse-submodules --shallow-submodules \
    https://github.com/duckdb/pg_duckdb "$SRC"
fi

# Pin the DuckDB extensions compiled into pg_duckdb (reproducible builds): file readers + object
# storage (httpfs) + iceberg. iceberg's aws-sdk-cpp[sso,sts,identity-management]/avro-c/roaring deps
# come from vcpkg (see USE_MERGED_...).
cat > "$SRC/third_party/pg_duckdb_extensions.cmake" <<EOF
duckdb_extension_load(json)
duckdb_extension_load(icu)
duckdb_extension_load(httpfs
    GIT_URL https://github.com/duckdb/duckdb-httpfs
    GIT_TAG $HTTPFS
    INCLUDE_DIR src/include
)
duckdb_extension_load(iceberg
    GIT_URL https://github.com/duckdb/duckdb-iceberg
    GIT_TAG $ICEBERG
)
EOF

echo "== 2. build pg_duckdb against cached PG 16.11 ($("$PGC" --version)) =="
cd "$SRC"
export PG_CONFIG="$PGC"
# DuckDB's httpfs extension (S3/HTTP reach) needs OpenSSL; macOS ships only LibreSSL headers.
# CMake FindOpenSSL honors OPENSSL_ROOT_DIR from the environment.
for o in /opt/homebrew/opt/openssl@3 "$HOME/homebrew/opt/openssl@3" /usr/local/opt/openssl@3; do
  [ -e "$o/lib/libcrypto.dylib" ] && { export OPENSSL_ROOT_DIR="$o"; break; }
done
echo "OPENSSL_ROOT_DIR=${OPENSSL_ROOT_DIR:-<none found>}"
# CMake 4 removed compatibility with cmake_minimum_required(<3.5); some extensions (delta + vendored
# deps) still declare an old minimum. This flag tells CMake 4 to treat them as requesting 3.5 policies.
export CMAKE_POLICY_VERSION_MINIMUM=3.5
# Keep a contaminating conda out of CMake's package search.
export CMAKE_IGNORE_PREFIX_PATH=/opt/miniconda3
PATH=$(printf '%s' "$PATH" | tr ':' '\n' | grep -v miniconda | paste -sd: -)
export PATH
# vcpkg supplies out-of-tree extension deps in isolation (iceberg -> aws-sdk-cpp[sso,sts], avro-c,
# roaring, curl, openssl). The DuckDB Makefile turns VCPKG_TOOLCHAIN_PATH into -DCMAKE_TOOLCHAIN_FILE.
if [ -f "$HOME/vcpkg/scripts/buildsystems/vcpkg.cmake" ]; then
  export VCPKG_TOOLCHAIN_PATH="$HOME/vcpkg/scripts/buildsystems/vcpkg.cmake"
  export VCPKG_TARGET_TRIPLET="${VCPKG_TARGET_TRIPLET:-arm64-osx}"
  export VCPKG_ROOT="$HOME/vcpkg"
  # DuckDB only wires -DVCPKG_MANIFEST_DIR (merged extension deps -> vcpkg install) when this is set.
  export USE_MERGED_VCPKG_MANIFEST=1
  echo "VCPKG_TOOLCHAIN_PATH=$VCPKG_TOOLCHAIN_PATH ($VCPKG_TARGET_TRIPLET) USE_MERGED_VCPKG_MANIFEST=1"
fi
# pg_duckdb links liblz4/libzstd/libcrypto that macOS doesn't ship; clang/ld resolve -l via LIBRARY_PATH.
LP=""
for lib in lz4 zstd openssl@3; do
  for base in /opt/homebrew/opt "$HOME/homebrew/opt" /usr/local/opt; do
    [ -d "$base/$lib/lib" ] && { LP="$LP:$base/$lib/lib"; break; }
  done
done
export LIBRARY_PATH="${LP#:}${LIBRARY_PATH:+:$LIBRARY_PATH}"
echo "LIBRARY_PATH=$LIBRARY_PATH"
# -j: build DuckDB + pg_duckdb; this is the ~20-30 min step.
# The compiler output goes to a file because it is tens of thousands of lines. Under `set -e` a
# failing make then exited with the whole diagnosis still on the runner's disk and nothing in the
# CI log but the exit code — so every failure has to print its own tail before it propagates.
run_make() { "$@" >> "$CACHE/pgduckdb-build.log" 2>&1 || {
  echo "FAIL: $* (exit $?) — last 80 lines of $CACHE/pgduckdb-build.log:"; tail -80 "$CACHE/pgduckdb-build.log"; exit 1; }; }
: > "$CACHE/pgduckdb-build.log"
run_make make -j"$(sysctl -n hw.ncpu 2>/dev/null || nproc)" PG_CONFIG="$PGC"
run_make make install PG_CONFIG="$PGC"

echo "== 3. verify artifacts installed into the cached prefix =="
PKGLIB="$("$PGC" --pkglibdir)"; EXT="$("$PGC" --sharedir)/extension"
for e in dylib so; do [ -e "$PKGLIB/pg_duckdb.$e" ] && SO="$PKGLIB/pg_duckdb.$e"; done
[ -n "${SO:-}" ] || { echo "FAIL: pg_duckdb.{dylib,so} not found in $PKGLIB"; tail -30 "$CACHE/pgduckdb-build.log"; exit 1; }
file "$SO"
ls -la "$SO"; ls "$EXT"/pg_duckdb* 2>/dev/null
echo "== BUILD OK: $SO =="
