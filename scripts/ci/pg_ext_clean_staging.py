#!/usr/bin/env python3
# Copyright (c) 2026 Kenneth Stott
# Canary: 104a5c7d-3000-4a0a-9ca3-8d0ec7d48378
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Prove a PUBLISHED provisa-pg-ext works on a clean install.

Run inside a throwaway virtualenv that holds only the published wheel, pgserver and psycopg: it
copies the wheel's bundle for ``<platform>`` into that venv's own pgserver (the layout the product
stages), starts the server with pg_duckdb preloaded, creates every extension the bundle carries,
and reads through postgres_fdw (libpq), sqlite_fdw and pg_duckdb. No build tree is on the machine,
so whatever loads came from the wheel.

Usage: pg_ext_clean_staging.py <darwin-arm64 | linux-x64>
"""

from __future__ import annotations

import json
import os
import shutil
import sqlite3
import sys
import tempfile
from pathlib import Path

import pgserver
import psycopg
from provisa_pg_ext import ext_root  # type: ignore[import-not-found]


def main(platform: str) -> int:
    src = ext_root() / platform
    pginstall = Path(pgserver.__file__).parent / "pginstall"
    suffix = "dylib" if platform.startswith("darwin") else "so"
    for f in (src / "lib").glob(f"*.{suffix}"):
        shutil.copy2(f, pginstall / "lib" / "postgresql" / f.name)
    for f in (src / "share" / "extension").iterdir():
        shutil.copy2(f, pginstall / "share" / "postgresql" / "extension" / f.name)
    manifest = json.loads((src / "manifest.json").read_text())
    extensions = sorted(
        a["key"]
        for a in manifest["artifacts"]
        if (src / "share" / "extension" / f"{a['key']}.control").exists()
    )
    print("extensions in the bundle:", extensions)

    base = tempfile.mkdtemp(prefix="pgext-clean-")
    db = pgserver.get_server(base)
    db.psql("ALTER SYSTEM SET shared_preload_libraries = 'pg_duckdb';")
    db.cleanup()
    db = pgserver.get_server(base)
    try:
        with psycopg.connect(db.get_uri(), autocommit=True) as c:
            for ext in extensions:
                c.execute(f"CREATE EXTENSION {ext}")  # type: ignore[arg-type]
                print("CREATE EXTENSION", ext, "ok")

            socket_dir = db.get_uri().split("host=")[1]
            c.execute("CREATE TABLE src(id int, name text)")
            c.execute("INSERT INTO src VALUES (1, 'a'), (2, 'b')")
            c.execute(
                "CREATE SERVER rem FOREIGN DATA WRAPPER postgres_fdw "  # type: ignore[arg-type]
                f"OPTIONS (host '{socket_dir}', dbname 'postgres')"
            )
            c.execute("CREATE USER MAPPING FOR CURRENT_USER SERVER rem OPTIONS (user 'postgres')")
            c.execute(
                "CREATE FOREIGN TABLE ft(id int, name text) SERVER rem OPTIONS (table_name 'src')"
            )
            read = c.execute("SELECT * FROM ft ORDER BY id").fetchall()
            assert read == [(1, "a"), (2, "b")], read
            print("postgres_fdw read:", read)

            path = os.path.join(base, "t.sqlite")
            with sqlite3.connect(path) as s:
                s.execute("CREATE TABLE t(id integer, v text)")
                s.execute("INSERT INTO t VALUES (7, 'x')")
            c.execute(
                "CREATE SERVER sq FOREIGN DATA WRAPPER sqlite_fdw "  # type: ignore[arg-type]
                f"OPTIONS (database '{path}')"
            )
            c.execute("CREATE FOREIGN TABLE sqt(id int, v text) SERVER sq OPTIONS (table 't')")
            read = c.execute("SELECT * FROM sqt").fetchall()
            assert read == [(7, "x")], read
            print("sqlite_fdw read:", read)

            read = c.execute("SELECT * FROM duckdb.query('SELECT 42 AS answer')").fetchall()
            assert read == [(42,)], read
            print("pg_duckdb read:", read)
    finally:
        db.cleanup()
    print("CLEAN STAGING OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1]))
