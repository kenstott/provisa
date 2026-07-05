#!/usr/bin/env bash
# Build the embedded-Postgres extension/FDW bundle for ONE platform and emit a checksummed manifest.
#
# Produces, into $OUT (default dist/pg-ext/<os>-<arch>):
#   lib/<name>.<so|dylib>            the extension binaries (relocated: @loader_path / $ORIGIN)
#   share/extension/<name>.control   + <name>--*.sql
#   manifest.json                    one row per artifact: {name,key,file,sha256,os,arch,pg_major,
#                                     runtime_deps,redistribution}
#
# Members (the OOTB set we build; each is smoke-tested by the caller/CI, not here):
#   core contrib : file_fdw, postgres_fdw            (no external runtime dep)
#   external fdw : sqlite_fdw (system libsqlite3), mysql_fdw (libmysqlclient/mariadb-connector-c)
#   pg_duckdb    : csv/parquet/json + httpfs + iceberg, via scripts/build_pg_duckdb.sh (vcpkg)
#
# macOS path is proven on this repo's dev machine; the Linux path is CI-targeted (patchelf/$ORIGIN,
# apt-provided client libs) — build it in an OLD-glibc container so the .so loads broadly.
set -euo pipefail

PG_VERSION="${PG_VERSION:-16.2}"                 # must match pgserver's bundled PG major
PGDUCKDB_TAG="${PG_DUCKDB_TAG:-v1.0.0}"
CACHE="${PROVISA_FDW_CACHE:-$HOME/.cache/provisa-fdw}"
PREFIX="$CACHE/pg${PG_VERSION//./}"              # cached PG-from-source install (headers + pg_config)
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"

case "$(uname -s)" in
  Darwin) OS=darwin; SUF=dylib ;;
  Linux)  OS=linux;  SUF=so ;;
  *) echo "unsupported OS $(uname -s)"; exit 1 ;;
esac
case "$(uname -m)" in
  arm64|aarch64) ARCH=arm64 ;;
  x86_64|amd64)  ARCH=x64 ;;
  *) echo "unsupported arch $(uname -m)"; exit 1 ;;
esac
OUT="${OUT:-$ROOT/dist/pg-ext/$OS-$ARCH}"
NPROC="$( (command -v nproc >/dev/null && nproc) || sysctl -n hw.ncpu )"

echo "== build PG $PG_VERSION from source (minimal) + core contrib (file_fdw, postgres_fdw) =="
if [ ! -x "$PREFIX/bin/pg_config" ]; then
  mkdir -p "$CACHE"; SRC="$CACHE/postgresql-$PG_VERSION"
  [ -d "$SRC" ] || { curl -fsSL "https://ftp.postgresql.org/pub/source/v$PG_VERSION/postgresql-$PG_VERSION.tar.bz2" | tar xj -C "$CACHE"; }
  ( cd "$SRC"
    ./configure --without-icu --without-readline --without-zlib --without-gssapi --prefix="$PREFIX" >/dev/null
    make -j"$NPROC" >/dev/null && make install >/dev/null
    make -C contrib/file_fdw install >/dev/null && make -C contrib/postgres_fdw install >/dev/null )
fi
PGC="$PREFIX/bin/pg_config"; PKGLIB="$("$PGC" --pkglibdir)"; EXTDIR="$("$PGC" --sharedir)/extension"

build_external_fdw() {  # $1 repo, $2 make-vars...
  local name="$1" repo="$2"; shift 2
  local src="$CACHE/$name"
  [ -d "$src/.git" ] || git clone --depth 1 "$repo" "$src"
  make -C "$src" USE_PGXS=1 PG_CONFIG="$PGC" "$@" >/dev/null
  make -C "$src" USE_PGXS=1 PG_CONFIG="$PGC" install "$@" >/dev/null
}
echo "== build sqlite_fdw (system libsqlite3) =="
SDK="$( (command -v xcrun >/dev/null && xcrun --show-sdk-path) || echo /usr )"
build_external_fdw sqlite_fdw https://github.com/pgspider/sqlite_fdw "SQLITE_INCLUDE=-I$SDK/usr/include" "SQLITE_LIB=-lsqlite3" || true
echo "== build mysql_fdw (mariadb-connector-c) =="
MYCFG="$( (command -v mariadb_config || command -v mysql_config) 2>/dev/null || true )"
[ -n "$MYCFG" ] && build_external_fdw mysql_fdw https://github.com/EnterpriseDB/mysql_fdw "MYSQL_CONFIG=$MYCFG" || echo "  (skip: no mysql client config found)"

echo "== build pg_duckdb (vcpkg: csv/parquet/json + httpfs + iceberg) =="
PG_DUCKDB_TAG="$PGDUCKDB_TAG" PROVISA_FDW_CACHE="$CACHE" bash "$ROOT/scripts/build_pg_duckdb.sh"

echo "== collect + relocate into $OUT =="
rm -rf "$OUT"; mkdir -p "$OUT/lib" "$OUT/share/extension"
relocate() {  # make a lib self-contained: @loader_path (macOS) / $ORIGIN (linux) for sibling deps
  local f="$1"
  if [ "$OS" = darwin ]; then
    install_name_tool -add_rpath "@loader_path" "$f" 2>/dev/null || true
    codesign -f -s - "$f" 2>/dev/null || true
  else
    patchelf --set-rpath '$ORIGIN' "$f" 2>/dev/null || true
  fi
}
# manifest rows: name key redistribution "runtime_deps..."
declare -a MEMBERS=(
  "file_fdw|file_fdw|bundled|"
  "postgres_fdw|postgres_fdw|bundled|"
  "sqlite_fdw|sqlite_fdw|bundled|libsqlite3 (system)"
  "mysql_fdw|mysql_fdw|bundled|libmysqlclient/mariadb-connector-c"
  "pg_duckdb|pg_duckdb|bundled|libduckdb; aws-sdk-cpp/avro-c/roaring (static)"
  "libduckdb|libduckdb|bundled|"
)
manifest="$OUT/manifest.json"; echo '{"os":"'$OS'","arch":"'$ARCH'","pg_major":"'${PG_VERSION%%.*}'","artifacts":[' > "$manifest"
first=1
for row in "${MEMBERS[@]}"; do
  IFS='|' read -r name key redis deps <<<"$row"
  src="$PKGLIB/$name.$SUF"; [ -e "$src" ] || { echo "  (missing $name.$SUF — skip)"; continue; }
  cp "$src" "$OUT/lib/"; relocate "$OUT/lib/$name.$SUF"
  [ -e "$EXTDIR/$key.control" ] && cp "$EXTDIR/$key.control" "$OUT/share/extension/" || true
  for s in "$EXTDIR/$key"--*.sql; do [ -e "$s" ] && cp "$s" "$OUT/share/extension/"; done
  sha="$( (command -v sha256sum >/dev/null && sha256sum "$OUT/lib/$name.$SUF" || shasum -a256 "$OUT/lib/$name.$SUF") | awk '{print $1}')"
  [ $first -eq 1 ] || echo ',' >> "$manifest"; first=0
  printf '  {"name":"%s","key":"%s","file":"lib/%s.%s","sha256":"%s","redistribution":"%s","runtime_deps":"%s"}' \
    "$name" "$key" "$name" "$SUF" "$sha" "$redis" "$deps" >> "$manifest"
done
echo '' >> "$manifest"; echo ']}' >> "$manifest"

echo "== package =="
TARBALL="$ROOT/dist/provisa-pg-ext-$OS-$ARCH.tar.gz"
tar -czf "$TARBALL" -C "$OUT" .
( cd "$(dirname "$TARBALL")" && { command -v sha256sum >/dev/null && sha256sum "$(basename "$TARBALL")" || shasum -a256 "$(basename "$TARBALL")"; } > "$TARBALL.sha256" )
echo "BUNDLE: $TARBALL"; cat "$manifest"
