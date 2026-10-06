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
#   external fdw : sqlite_fdw (system libsqlite3), mysql_fdw (with MariaDB Connector/C, built here)
#   pg_duckdb    : csv/parquet/json + httpfs + iceberg, via scripts/build_pg_duckdb.sh (vcpkg)
#   pg_clickhouse: built from source (github.com/ClickHouse/pg_clickhouse release zip — no apt/
#                  PGDG package exists, confirmed; live-verified working, REQ-1870)
#   wrappers     : NOT in the bundle. Supabase's prebuilt .deb needs GLIBCXX_3.4.32 (built for
#                  Ubuntu 24.04), above every other module's floor, so it never loaded where the
#                  bundle runs. It returns once built from source (pgrx) inside that floor (REQ-1871).
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
echo "== build MariaDB Connector/C (LGPL), the client library mysql_fdw loads =="
# mysql_fdw dlopens lib<client>.<suf> at load time; the bundle ships this build of it, with its auth
# plugins compiled in (a dynamic plugin is loaded from a directory fixed at build time) and nothing
# beyond OpenSSL linked: no remote_io (curl), no GSSAPI, no zstd, bundled zlib.
MCC_TAG="${MARIADB_CC_TAG:-v3.4.9}"
MCC_SRC="$CACHE/mariadb-connector-c-${MCC_TAG#v}"
MCC_PREFIX="$CACHE/mariadb-cc-${MCC_TAG#v}"
if [ ! -x "$MCC_PREFIX/bin/mariadb_config" ]; then
  [ -d "$MCC_SRC" ] || git clone -q --depth 1 --branch "$MCC_TAG" \
    https://github.com/mariadb-corporation/mariadb-connector-c "$MCC_SRC"
  MCC_SSL=()
  [ "$OS" = darwin ] && MCC_SSL=(-DOPENSSL_ROOT_DIR="$(brew --prefix openssl@3)")
  cmake -S "$MCC_SRC" -B "$MCC_SRC/build" -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX="$MCC_PREFIX" -DWITH_SSL=OPENSSL "${MCC_SSL[@]}" \
    -DWITH_UNIT_TESTS=OFF -DWITH_CURL=OFF -DWITH_EXTERNAL_ZLIB=OFF \
    -DCLIENT_PLUGIN_DIALOG=STATIC -DCLIENT_PLUGIN_CLIENT_ED25519=STATIC \
    -DCLIENT_PLUGIN_CACHING_SHA2_PASSWORD=STATIC -DCLIENT_PLUGIN_SHA256_PASSWORD=STATIC \
    -DCLIENT_PLUGIN_PARSEC=STATIC -DCLIENT_PLUGIN_MYSQL_CLEAR_PASSWORD=STATIC \
    -DCLIENT_PLUGIN_AUTH_GSSAPI_CLIENT=OFF -DCLIENT_PLUGIN_REMOTE_IO=OFF \
    -DCLIENT_PLUGIN_ZSTD=OFF -DCLIENT_PLUGIN_REPLICATION=OFF >/dev/null
  cmake --build "$MCC_SRC/build" -j"$NPROC" >/dev/null
  cmake --install "$MCC_SRC/build" >/dev/null
fi
echo "== build mysql_fdw (against that Connector/C) =="
# A cached tree may hold objects compiled against another client's headers; make cannot tell.
[ ! -d "$CACHE/mysql_fdw/.git" ] || make -C "$CACHE/mysql_fdw" USE_PGXS=1 PG_CONFIG="$PGC" clean >/dev/null
build_external_fdw mysql_fdw https://github.com/EnterpriseDB/mysql_fdw \
  "MYSQL_CONFIG=$MCC_PREFIX/bin/mariadb_config"

echo "== build pg_duckdb (vcpkg: csv/parquet/json + httpfs + iceberg) =="
PG_DUCKDB_TAG="$PGDUCKDB_TAG" PROVISA_FDW_CACHE="$CACHE" bash "$ROOT/scripts/build_pg_duckdb.sh"

PGCH_TAG="${PGCH_TAG:-v0.10.0}"
echo "== build pg_clickhouse (clickhouse-c client, live-verified REQ-1870) =="
PGCH_SRC="$CACHE/pg_clickhouse-${PGCH_TAG#v}"
if [ ! -d "$PGCH_SRC" ]; then
  curl -fsSL -o "$CACHE/pg_clickhouse.zip" \
    "https://github.com/ClickHouse/pg_clickhouse/releases/download/$PGCH_TAG/pg_clickhouse-${PGCH_TAG#v}.zip"
  unzip -q -d "$CACHE" "$CACHE/pg_clickhouse.zip"
fi
# pg_clickhouse names PostgreSQL's regex type pg_regex_t, the name later 16.x minors gave regex_t;
# 16.2's headers (pinned to pgserver's PG above) still call it regex_t. Same type, renamed only.
PGCH_CPP=""
grep -q pg_regex_t "$("$PGC" --includedir-server)/regex/regex.h" || PGCH_CPP="-Dpg_regex_t=regex_t"
PGCH_ENV=()
if [ "$OS" = darwin ]; then
  # Homebrew's lz4/zstd/openssl@3 are not on the compiler's default search path on arm64.
  inc=""; lib=""
  for f in lz4 zstd openssl@3; do p="$(brew --prefix "$f")"; inc="$inc:$p/include"; lib="$lib:$p/lib"; done
  PGCH_ENV=(CPATH="${inc#:}" LIBRARY_PATH="${lib#:}")
fi
( cd "$PGCH_SRC" && env "${PGCH_ENV[@]}" make PG_CONFIG="$PGC" COPT="$PGCH_CPP" >/dev/null \
  && env "${PGCH_ENV[@]}" make PG_CONFIG="$PGC" COPT="$PGCH_CPP" install >/dev/null )

echo "== collect + relocate into $OUT =="
rm -rf "$OUT"; mkdir -p "$OUT/lib" "$OUT/share/extension"
rpaths() { otool -l "$1" | awk '/cmd LC_RPATH/ {getline; getline; print $2}'; }
relocate() {  # make a lib self-contained: @loader_path (macOS) / $ORIGIN (linux) for sibling deps
  local f="$1"
  if [ "$OS" = darwin ]; then
    # Drop the run paths the build left pointing into its own tree; keep only @loader_path.
    for rp in $(rpaths "$f"); do
      [ "$rp" = "@loader_path" ] || install_name_tool -delete_rpath "$rp" "$f"
    done
    rpaths "$f" | grep -qx "@loader_path" || install_name_tool -add_rpath "@loader_path" "$f"
    codesign -f -s - "$f"
  else
    patchelf --set-rpath '$ORIGIN' "$f" 2>/dev/null || true
  fi
}
# manifest rows: name key redistribution "runtime_deps..."
declare -a MEMBERS=(
  "file_fdw|file_fdw|bundled|"
  "postgres_fdw|postgres_fdw|bundled|"
  "sqlite_fdw|sqlite_fdw|bundled|libsqlite3 (system)"
  "mysql_fdw|mysql_fdw|bundled|libmysqlclient (bundled MariaDB Connector/C)"
  "pg_duckdb|pg_duckdb|bundled|libduckdb; aws-sdk-cpp/avro-c/roaring (static)"
  "libduckdb|libduckdb|bundled|"
  "pg_clickhouse|pg_clickhouse|bundled|libssl/libcrypto; liblz4; libzstd; libcurl; libuuid"
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
if [ -e "$OUT/lib/postgres_fdw.$SUF" ]; then
  # postgres_fdw links the libpq this build made under $PREFIX: on macOS by that absolute path, on
  # Linux by soname, and neither exists on the machine that stages the bundle. Ship it beside the
  # modules, under a name the stager's *.$SUF glob copies, and point postgres_fdw at that copy.
  if [ "$OS" = darwin ]; then
    cp "$PREFIX/lib/libpq.5.dylib" "$OUT/lib/libpq.dylib"
    install_name_tool -id "@rpath/libpq.dylib" "$OUT/lib/libpq.dylib"
    old="$(otool -L "$OUT/lib/postgres_fdw.dylib" | awk '/libpq/ {print $1}')"
    install_name_tool -change "$old" "@rpath/libpq.dylib" "$OUT/lib/postgres_fdw.dylib"
    codesign -f -s - "$OUT/lib/libpq.dylib" "$OUT/lib/postgres_fdw.dylib"
  else
    cp -L "$PREFIX/lib/libpq.so.5" "$OUT/lib/libpq.so"
    patchelf --set-soname libpq.so "$OUT/lib/libpq.so"
    patchelf --set-rpath '$ORIGIN' "$OUT/lib/libpq.so"  # its build-tree run path means nothing here
    patchelf --replace-needed libpq.so.5 libpq.so "$OUT/lib/postgres_fdw.so"
  fi
  sha="$( (command -v sha256sum >/dev/null && sha256sum "$OUT/lib/libpq.$SUF" || shasum -a256 "$OUT/lib/libpq.$SUF") | awk '{print $1}')"
  echo ',' >> "$manifest"
  printf '  {"name":"libpq","key":"libpq","file":"lib/libpq.%s","sha256":"%s","redistribution":"bundled","runtime_deps":""}' \
    "$SUF" "$sha" >> "$manifest"
fi
if [ -e "$OUT/lib/mysql_fdw.$SUF" ]; then
  # mysql_fdw dlopens the client by the name its Makefile derived from the client's link flags
  # (libmysqlclient.<suf> for Connector/C's -lmariadb); the dlopen finds it beside the module
  # (the pkglibdir on macOS, mysql_fdw's $ORIGIN run path on Linux).
  if [ "$OS" = darwin ]; then
    cp -L "$MCC_PREFIX/lib/mariadb/libmariadb.3.dylib" "$OUT/lib/libmysqlclient.dylib"
    install_name_tool -id "@rpath/libmysqlclient.dylib" "$OUT/lib/libmysqlclient.dylib"
    relocate "$OUT/lib/libmysqlclient.dylib"
  else
    cp -L "$MCC_PREFIX/lib/mariadb/libmariadb.so.3" "$OUT/lib/libmysqlclient.so"
    patchelf --set-soname libmysqlclient.so "$OUT/lib/libmysqlclient.so"
    patchelf --set-rpath '$ORIGIN' "$OUT/lib/libmysqlclient.so"
  fi
  sha="$( (command -v sha256sum >/dev/null && sha256sum "$OUT/lib/libmysqlclient.$SUF" || shasum -a256 "$OUT/lib/libmysqlclient.$SUF") | awk '{print $1}')"
  echo ',' >> "$manifest"
  printf '  {"name":"libmysqlclient","key":"libmysqlclient","file":"lib/libmysqlclient.%s","sha256":"%s","redistribution":"bundled (MariaDB Connector/C, LGPL-2.1)","runtime_deps":""}' \
    "$SUF" "$sha" >> "$manifest"
fi
if [ "$OS" = darwin ]; then
  # A module may load only the OS's own libraries (/usr/lib, /System) and the bundle's. Anything
  # else it links by absolute path (Homebrew's libssl/libcrypto/lz4/zstd for pg_clickhouse) ships
  # beside it and loads through @rpath + @loader_path; repeat until the vendored libraries' own
  # dependencies are covered too.
  changed=1
  while [ "$changed" = 1 ]; do
    changed=0
    for f in "$OUT"/lib/*.dylib; do
      for d in $(otool -L "$f" | tail -n +2 | awk '{print $1}'); do
        case "$d" in /usr/lib/*|/System/*|@rpath/*|@loader_path/*) continue ;; esac
        name="$(basename "$d")"
        if [ ! -e "$OUT/lib/$name" ]; then
          cp -L "$d" "$OUT/lib/$name"; chmod 755 "$OUT/lib/$name"
          install_name_tool -id "@rpath/$name" "$OUT/lib/$name"
          for rp in $(rpaths "$OUT/lib/$name"); do  # its own install's run paths mean nothing here
            [ "$rp" = "@loader_path" ] || install_name_tool -delete_rpath "$rp" "$OUT/lib/$name"
          done
          rpaths "$OUT/lib/$name" | grep -qx "@loader_path" \
            || install_name_tool -add_rpath "@loader_path" "$OUT/lib/$name"
          codesign -f -s - "$OUT/lib/$name"
          sha="$(shasum -a256 "$OUT/lib/$name" | awk '{print $1}')"
          echo ',' >> "$manifest"
          printf '  {"name":"%s","key":"%s","file":"lib/%s","sha256":"%s","redistribution":"bundled","runtime_deps":""}' \
            "${name%%.*}" "${name%%.*}" "$name" "$sha" >> "$manifest"
        fi
        install_name_tool -change "$d" "@rpath/$name" "$f"
        rpaths "$f" | grep -qx "@loader_path" || install_name_tool -add_rpath "@loader_path" "$f"
        codesign -f -s - "$f"
        changed=1
      done
    done
  done
fi
echo '' >> "$manifest"; echo ']}' >> "$manifest"

echo "== verify: the bundle loads only the OS's libraries and its own =="
for f in "$OUT"/lib/*."$SUF"; do
  if [ "$OS" = darwin ]; then
    for d in $(otool -L "$f" | tail -n +2 | awk '{print $1}'); do
      case "$d" in
        /usr/lib/*|/System/*) ;;
        @rpath/*|@loader_path/*)
          [ -e "$OUT/lib/$(basename "$d")" ] \
            || { echo "FAIL: $(basename "$f") loads $d, which the bundle does not ship"; exit 1; } ;;
        *) echo "FAIL: $(basename "$f") loads $d (not the OS's, not the bundle's)"; exit 1 ;;
      esac
    done
    for rp in $(rpaths "$f"); do
      [ "$rp" = "@loader_path" ] || { echo "FAIL: $(basename "$f") has run path $rp"; exit 1; }
    done
  fi
  if [ "$OS" = darwin ]; then deps="$(otool -L "$f" | tail -n +2 | awk '{print $1}') $(rpaths "$f")"
  else deps="$(patchelf --print-needed "$f") $(patchelf --print-rpath "$f" | tr ':' ' ')"; fi
  for d in $deps; do
    # Library references and run paths both: neither may name the build tree or a home directory.
    case "$d" in
      "$CACHE"/*|"$PREFIX"/*|"$ROOT"/*|/Users/*|/home/*|/tmp/*|/private/*)
        echo "FAIL: $(basename "$f") references $d (builder path)"; exit 1 ;;
    esac
    # A Linux soname this build produced must ship in the bundle.
    if [ "$OS" = linux ] && [ -e "$PREFIX/lib/$d" ] && [ ! -e "$OUT/lib/$d" ]; then
      echo "FAIL: $(basename "$f") needs $d, which this build made and the bundle does not ship"; exit 1
    fi
  done
done

echo "== manifest checksums: of the files as they ship =="
# Relocation (install_name_tool, patchelf, codesign) rewrites a module after its row was written,
# so every checksum is taken again here, from the final file, and the manifest is valid JSON.
python3 - "$OUT" <<'PY'
import hashlib, json, pathlib, sys
out = pathlib.Path(sys.argv[1])
manifest = json.loads((out / "manifest.json").read_text())
for artifact in manifest["artifacts"]:
    artifact["sha256"] = hashlib.sha256((out / artifact["file"]).read_bytes()).hexdigest()
(out / "manifest.json").write_text(json.dumps(manifest, indent=1) + "\n")
PY

echo "== package =="
TARBALL="${TARBALL:-$ROOT/dist/provisa-pg-ext-$OS-$ARCH.tar.gz}"
tar -czf "$TARBALL" -C "$OUT" .
( cd "$(dirname "$TARBALL")" && { command -v sha256sum >/dev/null && sha256sum "$(basename "$TARBALL")" || shasum -a256 "$(basename "$TARBALL")"; } > "$TARBALL.sha256" )
echo "BUNDLE: $TARBALL"; cat "$manifest"
