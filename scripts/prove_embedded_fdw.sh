#!/usr/bin/env bash
# Prove whether a CI-style source build of PG 16.2's file_fdw loads into pgserver's
# embedded PG 16.2 (same major/OS/arch) via CREATE EXTENSION + a real CSV read.
# Builds ONLY PG-from-source + contrib/file_fdw (no icu/krb5/readline) — the exact
# thing a CI matrix job would run. No Homebrew, no dev-machine artifact copying.
set -euo pipefail

WORK=/tmp/fdwtest
mkdir -p "$WORK"

echo "== 1. build PG 16.2 from source (minimal) + contrib/file_fdw =="
SRC="$WORK/postgresql-16.2"
PREFIX="$WORK/pg162"
if [ ! -x "$PREFIX/bin/pg_config" ]; then
  if [ ! -d "$SRC" ]; then
    curl -fsSL https://ftp.postgresql.org/pub/source/v16.2/postgresql-16.2.tar.bz2 \
      | tar xj -C "$WORK"
  fi
  cd "$SRC"
  ./configure --without-icu --without-readline --without-zlib --without-gssapi \
    --prefix="$PREFIX" >"$WORK/configure.log" 2>&1
  make -j"$(sysctl -n hw.ncpu)" >"$WORK/make.log" 2>&1
  make install >"$WORK/install.log" 2>&1
  make -C contrib/file_fdw install >>"$WORK/install.log" 2>&1
fi
PGC="$PREFIX/bin/pg_config"
# macOS PG builds extensions as .dylib; Linux as .so — pick whichever exists.
PKGLIB="$("$PGC" --pkglibdir)"; SO=""
for e in dylib so; do [ -e "$PKGLIB/file_fdw.$e" ] && { SO="$PKGLIB/file_fdw.$e"; break; }; done
[ -n "$SO" ] || { echo "FAIL: no built file_fdw.{dylib,so} in $PKGLIB"; exit 1; }
EXT="$("$PGC" --sharedir)/extension"
CTL="$EXT/file_fdw.control"
SQL="$(ls "$EXT"/file_fdw--*.sql | head -1)"
echo "built: $("$PGC" --version)"
file "$SO"                          # must say: Mach-O ... arm64
echo "artifacts: $SO / $CTL / $SQL"

echo "== 2. fresh embedded PG (pgserver 16.2) in a 3.12 venv =="
VENV="$WORK/venv"
[ -x "$VENV/bin/python" ] || python3.12 -m venv "$VENV"
"$VENV/bin/pip" -q install pgserver asyncpg
PGROOT="$WORK/pgdata"
rm -rf "$PGROOT"; mkdir -p "$PGROOT"
LIBDIR="$("$VENV/bin/python" -c 'import pgserver,os;print(os.path.join(os.path.dirname(pgserver.__file__),"pginstall","lib","postgresql"))')"
[ -d "$LIBDIR" ] || LIBDIR="$("$VENV/bin/python" -c 'import pgserver,os;print(os.path.join(os.path.dirname(pgserver.__file__),"pginstall","lib"))')"
EXTDIR="$("$VENV/bin/python" -c 'import pgserver,os;print(os.path.join(os.path.dirname(pgserver.__file__),"pginstall","share","postgresql","extension"))')"
# pgserver's server has its own compiled DLSUFFIX — match it (its plpgsql tells us .so vs .dylib).
PLP=""
for e in dylib so; do [ -e "$LIBDIR/plpgsql.$e" ] && { PLP="$LIBDIR/plpgsql.$e"; break; }; done
[ -n "$PLP" ] || { echo "FAIL: no plpgsql.{dylib,so} in $LIBDIR"; exit 1; }
SUFFIX="${PLP##*.}"
echo "pgserver extension suffix: .$SUFFIX (from $PLP)"

echo "== 3. drop the 3 artifacts into the embedded install =="
cp -v "$SO"  "$LIBDIR/file_fdw.$SUFFIX"   # rename to pgserver's expected suffix
cp -v "$CTL" "$EXTDIR/"
cp -v "$SQL" "$EXTDIR/"

echo "== 3b. (advisory) PG_MODULE_MAGIC — authoritative check is CREATE EXTENSION in step 4 =="
# A PG extension bundle can't be dlopen'd standalone (unresolved backend symbols like
# CurrentMemoryContext, provided by the postgres executable at load). So the real module-magic
# validation is step 4's CREATE EXTENSION: PG checks the magic block on load and refuses a mismatch.
echo "skipped standalone dlopen; step 4 CREATE EXTENSION is the authoritative magic/ABI check"

echo "== 4. start embedded PG, CREATE EXTENSION file_fdw, read a real CSV =="
printf 'id,first_name,state\n1,Alice,NY\n2,Bob,CA\n' > "$WORK/cust.csv"
"$VENV/bin/python" - "$PGROOT" "$WORK/cust.csv" <<'PY'
import sys, asyncio, pgserver
db = pgserver.get_server(sys.argv[1])
print("server:", db.psql("SHOW server_version").strip())
async def main():
    import asyncpg
    conn = await asyncpg.connect(dsn=db.get_uri())
    try:
        await conn.execute("CREATE EXTENSION file_fdw")
        await conn.execute("CREATE SERVER csv_srv FOREIGN DATA WRAPPER file_fdw")
        await conn.execute(
            "CREATE FOREIGN TABLE cust (id int, first_name text, state text) "
            f"SERVER csv_srv OPTIONS (filename '{sys.argv[2]}', format 'csv', header 'true')"
        )
        rows = await conn.fetch("SELECT * FROM cust ORDER BY id")
        print("ROWS:", [dict(r) for r in rows])
        print("RESULT: file_fdw LOADED AND QUERIED OK")
    finally:
        await conn.close()
asyncio.run(main())
PY
echo "== done =="
