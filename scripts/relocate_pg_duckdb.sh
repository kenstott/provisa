#!/usr/bin/env bash
# Make the pg_duckdb artifact self-contained: bundle libduckdb + OpenSSL next to pg_duckdb and rewrite
# their install names / rpaths to @loader_path, so the extension loads with NO Homebrew dependency.
# Proves the release-artifact shape. Re-signs ad-hoc (install_name_tool invalidates arm64 signatures).
set -euo pipefail

CACHE="${PROVISA_FDW_CACHE:-$HOME/.cache/provisa-fdw}/pg162"
SRC="$CACHE/lib/postgresql"
OUT="${1:-$CACHE/pg_duckdb_bundle}"   # self-contained staging dir (the future release payload)
rm -rf "$OUT"; mkdir -p "$OUT"

echo "== 1. gather the 4 dylibs into one dir =="
cp "$SRC/pg_duckdb.dylib" "$SRC/libduckdb.dylib" "$OUT/"
# resolve the OpenSSL deps libduckdb currently points at (absolute Homebrew paths)
SSL=()
while IFS= read -r line; do SSL+=("$line"); done < <(otool -L "$SRC/libduckdb.dylib" | awk '/libssl|libcrypto/{print $1}')
[ "${#SSL[@]}" -ge 2 ] || { echo "expected libssl+libcrypto in libduckdb deps"; otool -L "$SRC/libduckdb.dylib"; exit 1; }
for s in "${SSL[@]}"; do cp "$s" "$OUT/"; done
SSL_LIB=$(basename "$(printf '%s\n' "${SSL[@]}" | grep libssl)")
CRY_LIB=$(basename "$(printf '%s\n' "${SSL[@]}" | grep libcrypto)")
echo "bundled: pg_duckdb.dylib libduckdb.dylib $SSL_LIB $CRY_LIB"

echo "== 2. rewrite install names -> @loader_path =="
chmod u+w "$OUT"/*.dylib
# each dylib's own id
install_name_tool -id "@loader_path/libduckdb.dylib"  "$OUT/libduckdb.dylib"
install_name_tool -id "@loader_path/$SSL_LIB"         "$OUT/$SSL_LIB"
install_name_tool -id "@loader_path/$CRY_LIB"         "$OUT/$CRY_LIB"
# libduckdb -> openssl (were absolute Homebrew paths)
for s in "${SSL[@]}"; do
  install_name_tool -change "$s" "@loader_path/$(basename "$s")" "$OUT/libduckdb.dylib"
done
# libssl -> libcrypto (libssl also links libcrypto by absolute path)
CRY_ABS=$(otool -L "$OUT/$SSL_LIB" | awk '/libcrypto/{print $1; exit}')
[ -n "$CRY_ABS" ] && install_name_tool -change "$CRY_ABS" "@loader_path/$CRY_LIB" "$OUT/$SSL_LIB" || true
# pg_duckdb -> libduckdb is @rpath; ensure an @loader_path rpath so the sibling resolves
install_name_tool -add_rpath "@loader_path" "$OUT/pg_duckdb.dylib" 2>/dev/null || true

echo "== 3. re-sign ad-hoc (install_name_tool invalidates the arm64 signature) =="
for d in "$OUT"/*.dylib; do codesign -f -s - "$d"; done

echo "== 4. copy control/sql alongside =="
cp "$CACHE/share/postgresql/extension/pg_duckdb.control" "$OUT/"
cp "$CACHE"/share/postgresql/extension/pg_duckdb--*.sql "$OUT/"

echo "== 5. VERIFY: no absolute Homebrew/Cellar paths remain =="
LEAK=$(otool -L "$OUT"/*.dylib | grep -E "/opt/homebrew|/Cellar|$HOME/homebrew" || true)
if [ -n "$LEAK" ]; then echo "LEAK (still external):"; echo "$LEAK"; exit 1; fi
echo "clean — only @loader_path + /usr/lib remain:"
otool -L "$OUT/libduckdb.dylib" | grep -vE "^$OUT" | sed 's/^/    /'
echo "== BUNDLE OK: $OUT =="
