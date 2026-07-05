#!/usr/bin/env bash
# Build pg_duckdb (embeds DuckDB v1.5.4 inside Postgres) against our cached PG 16.2 source build,
# so the artifact drops into pgserver's embedded PG 16.2 (same major). Long build: compiles DuckDB.
# Proven install route is the same as file_fdw/postgres_fdw (scripts/prove_embedded_fdw.sh).
set -euo pipefail

CACHE="${PROVISA_FDW_CACHE:-$HOME/.cache/provisa-fdw}"
PREFIX="$CACHE/pg162"
PGC="$PREFIX/bin/pg_config"
[ -x "$PGC" ] || { echo "FAIL: cached PG 16.2 not built at $PREFIX (run prove_embedded_fdw.sh first)"; exit 1; }

# v1.0.0 supports PG 14-17 and avoids ExecCheckOneRelPerms (added post-v1.1.1), which pgserver's
# fixed PG 16.2 backend does not export. HEAD requires a newer 16.x minor than pgserver ships.
TAG="${PG_DUCKDB_TAG:-v1.0.0}"
SRC="$CACHE/pg_duckdb-$TAG"
echo "== 1. clone pg_duckdb $TAG + duckdb submodule (shallow) =="
if [ ! -d "$SRC/.git" ]; then
  git clone --depth 1 --branch "$TAG" --recurse-submodules --shallow-submodules \
    https://github.com/duckdb/pg_duckdb "$SRC"
fi

# Pin the DuckDB extensions compiled into pg_duckdb (reproducible builds): file readers + object
# storage (httpfs) + iceberg. iceberg's v1.3-ossivalis branch matches DuckDB v1.3.2 (pg_duckdb v1.0.0);
# its aws-sdk-cpp[sso,sts,identity-management]/avro-c/roaring deps come from vcpkg (see USE_MERGED_...).
cat > "$SRC/third_party/pg_duckdb_extensions.cmake" <<'EOF'
duckdb_extension_load(json)
duckdb_extension_load(icu)
duckdb_extension_load(httpfs
    GIT_URL https://github.com/duckdb/duckdb-httpfs
    GIT_TAG 7ce5308ed8fe48b593538dbd54344a2fc0695bc7
    INCLUDE_DIR extension/httpfs/include
)
duckdb_extension_load(iceberg
    GIT_URL https://github.com/duckdb/duckdb-iceberg
    GIT_TAG v1.3-ossivalis
)
EOF

echo "== 2. build pg_duckdb against cached PG 16.2 ($("$PGC" --version)) =="
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
make -j"$(sysctl -n hw.ncpu 2>/dev/null || nproc)" PG_CONFIG="$PGC" > "$CACHE/pgduckdb-build.log" 2>&1
make install PG_CONFIG="$PGC" >> "$CACHE/pgduckdb-build.log" 2>&1

echo "== 3. verify artifacts installed into the cached prefix =="
PKGLIB="$("$PGC" --pkglibdir)"; EXT="$("$PGC" --sharedir)/extension"
for e in dylib so; do [ -e "$PKGLIB/pg_duckdb.$e" ] && SO="$PKGLIB/pg_duckdb.$e"; done
[ -n "${SO:-}" ] || { echo "FAIL: pg_duckdb.{dylib,so} not found in $PKGLIB"; tail -30 "$CACHE/pgduckdb-build.log"; exit 1; }
file "$SO"
ls -la "$SO"; ls "$EXT"/pg_duckdb* 2>/dev/null
echo "== BUILD OK: $SO =="
