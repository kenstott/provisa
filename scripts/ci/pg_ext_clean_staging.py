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

Run inside a throwaway virtualenv that holds only the published wheel, pgserver, psycopg and what
the fake functions import (faker, pyyaml for the staging module's package): it stages the wheel's
bundle for ``<platform>`` into that venv's own pgserver through the product's own staging
(``provisa.pg_extensions.staging``, from this checkout) -- which links the venv interpreter's
libpython beside plpython3 (REQ-1494) -- starts the server with pg_duckdb preloaded and the
postmaster environment PL/Python needs, creates every extension the bundle carries, reads through
postgres_fdw (libpq), sqlite_fdw and pg_duckdb, and computes the fake functions in PL/Python
against the same functions computed here. No build tree is on the machine, so whatever loads came
from the wheel.

Usage: pg_ext_clean_staging.py <darwin-arm64 | linux-x64>
"""

from __future__ import annotations

import hashlib
import json
import os
import secrets
import sqlite3
import sys
import tempfile
from pathlib import Path

import pgserver
import psycopg
from provisa_pg_ext import ext_root  # type: ignore[import-not-found]

# The checkout's root: the staging and the fake functions are the product's own, not a copy.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from provisa.fakes.digest import definition_hash, digest, fingerprint  # noqa: E402
from provisa.fakes.duckdb_functions import fake_method  # noqa: E402
from provisa.fakes.pg_functions import FUNCTIONS_SQL  # noqa: E402
from provisa.pg_extensions.staging import (  # noqa: E402
    bundle_platform,
    plpython_environment,
    stage_bundled_pg_extensions,
)


def _fake_functions_compute(c: psycopg.Connection, key: bytes) -> None:
    """REQ-1494: the fake functions run on the wheel's PL/Python, import provisa.fakes from the
    postmaster's PYTHONPATH, and agree with the same functions computed in this interpreter."""
    c.execute(FUNCTIONS_SQL)  # type: ignore[arg-type]
    d = digest(key, "ann@example.com")
    h = definition_hash("email", {}, None, "varchar")
    got = c.execute(
        "SELECT provisa_digest(%s, %s), provisa_fake_method('email', '{}', %s, %s)",
        (fingerprint(key), "ann@example.com", d, h),
    ).fetchone()
    want = (d, fake_method("email", "{}", d, h))
    assert got == want, f"PL/Python fake functions computed {got}, this interpreter {want}"
    print("plpython3u fake functions:", got)


def main(platform: str) -> int:
    assert bundle_platform() == platform, f"this host is {bundle_platform()}, not {platform}"
    src = ext_root() / platform
    pginstall = Path(pgserver.__file__).parent / "pginstall"
    stage_bundled_pg_extensions(pginstall)
    os.environ.update(plpython_environment())
    key = secrets.token_bytes(32)
    key_dir = Path(tempfile.mkdtemp(prefix="pgext-clean-keys-"))
    (key_dir / f"{fingerprint(key)}.key").write_text(key.hex())
    os.environ["PROVISA_FAKE_KEY_DIR"] = str(key_dir)
    manifest = json.loads((src / "manifest.json").read_text())
    stale = [
        a["file"]
        for a in manifest["artifacts"]
        if hashlib.sha256((src / a["file"]).read_bytes()).hexdigest() != a["sha256"]
    ]
    assert not stale, f"manifest checksums do not match the published files: {stale}"
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

            _fake_functions_compute(c, key)
    finally:
        db.cleanup()
    print("CLEAN STAGING OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1]))
