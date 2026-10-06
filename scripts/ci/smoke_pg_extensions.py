#!/usr/bin/env python3
# Copyright (c) 2026 Kenneth Stott
# Canary: faa0f2fc-8a54-4474-86f0-6e1f8b109685
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Smoke-test a built PG-extension bundle: install it into a fresh pgserver and CREATE EXTENSION each.

Build-and-prove-load in the same CI job (the discipline used to build these by hand): a bundle that
compiles but does not LOAD is a failure. Reads <bundle>/manifest.json, copies lib/* + share/extension/*
into pgserver's pginstall, then loads each extension. Every member must load, and the required
ones must be present; mysql_fdw's client library ships in the bundle like every other library.

Usage: python smoke_pg_extensions.py <bundle-dir>
Exit non-zero if any REQUIRED extension fails to load.
"""

from __future__ import annotations

import hashlib
import json
import os
import secrets
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pgserver

REQUIRED = {"file_fdw", "postgres_fdw", "sqlite_fdw", "pg_duckdb", "pg_clickhouse", "mysql_fdw"}
if sys.platform != "win32":
    REQUIRED |= {"plpython3u"}  # REQ-1494: the pg engine's fake functions; not built for Windows

# The repository root: the smoke runs Provisa's own fake functions and interpreter linking.
_REPO = Path(__file__).resolve().parents[2]


def _link_plpython(dl: Path) -> str:
    """REQ-1494: link this interpreter beside plpython3 as staging does, and give the postmaster
    its environment and a fake key; returns the key's hex."""
    sys.path.insert(0, str(_REPO))
    from provisa.fakes.digest import fingerprint
    from provisa.pg_extensions.staging import link_interpreter, plpython_environment

    link_interpreter(dl, sys.platform)
    os.environ.update(plpython_environment())
    key = secrets.token_bytes(32)
    key_dir = Path(tempfile.mkdtemp(prefix="smoke_fake_keys_"))
    (key_dir / f"{fingerprint(key)}.key").write_text(key.hex())
    os.environ["PROVISA_FAKE_KEY_DIR"] = str(key_dir)
    return key.hex()


def _fake_functions_compute(db, key_hex: str) -> list[str]:
    """The fake functions run on the bundle's PL/Python and agree with the embedded engine."""
    from provisa.fakes.digest import digest, fingerprint
    from provisa.fakes.duckdb_functions import fake_method
    from provisa.fakes.pg_functions import FUNCTIONS_SQL

    key = bytes.fromhex(key_hex)
    db.psql(FUNCTIONS_SQL)
    d = digest(key, "ann@example.com")
    out = db.psql(
        f"SELECT provisa_digest('{fingerprint(key)}', 'ann@example.com') || '|' || "
        f"provisa_fake_method('email', '{{}}', {d})"
    )
    want = f"{d}|{fake_method('email', '{}', d)}"
    if want in out:
        print("  OK   plpython3u fake functions")
        return []
    print(f"  FAIL plpython3u fake functions: wanted {want}, got:\n{out}")
    return ["plpython3u: the fake functions did not compute as the embedded engine does"]


def _postgres_fdw_reads(db) -> list[str]:
    """A foreign table over this same server: postgres_fdw opens a libpq connection, so the read
    proves the libpq the bundle ships is the one that loads (the build's own libpq is moved away
    before this runs in CI)."""
    socket_dir = parse_qs(urlparse(db.get_uri()).query)["host"][0]
    sql = (
        "CREATE TABLE smoke_src(id int); INSERT INTO smoke_src VALUES (1),(2),(3);"
        f"CREATE SERVER smoke_rem FOREIGN DATA WRAPPER postgres_fdw "
        f"OPTIONS (host '{socket_dir}', dbname 'postgres');"
        "CREATE USER MAPPING FOR CURRENT_USER SERVER smoke_rem OPTIONS (user 'postgres');"
        "CREATE FOREIGN TABLE smoke_ft(id int) SERVER smoke_rem OPTIONS (table_name 'smoke_src');"
        "SELECT 'fdw-sum=' || sum(id) FROM smoke_ft;"
    )
    # psql directly rather than pgserver's psql(), which drops stderr: a failed read names why.
    psql = Path(pgserver.__file__).parent / "pginstall" / "bin" / "psql"
    done = subprocess.run(  # noqa: S603
        [str(psql), "-v", "ON_ERROR_STOP=1", db.get_uri()],
        input=sql,
        capture_output=True,
        text=True,
        check=False,
    )
    out = done.stdout + done.stderr
    if "fdw-sum=6" in out:
        print("  OK   postgres_fdw foreign-table read")
        return []
    print(f"  FAIL postgres_fdw foreign-table read:\n{out}")
    return ["postgres_fdw: foreign-table read failed"]


def main(bundle: Path) -> int:
    manifest = json.loads((bundle / "manifest.json").read_text())
    keys = {a["key"] for a in manifest["artifacts"]}
    # The external installer refuses a file whose checksum does not match its row, so a stale row
    # ships a bundle that cannot be installed (0.1.1: relocation rewrote files after their rows).
    stale = [
        a["file"]
        for a in manifest["artifacts"]
        if hashlib.sha256((bundle / a["file"]).read_bytes()).hexdigest() != a["sha256"]
    ]
    if stale:
        print("SMOKE FAILED: manifest checksums do not match the files:", *stale, sep="\n  ")
        return 1

    pg = Path(pgserver.__file__).parent / "pginstall"
    dl = pg / "lib" / "postgresql"
    de = pg / "share" / "postgresql" / "extension"
    suffix = "dylib" if (dl / "plpgsql.dylib").exists() else "so"

    for so in (bundle / "lib").glob(f"*.{suffix}"):
        shutil.copy(so, dl / so.name)
        if so.stem == "pg_duckdb" and sys.platform == "darwin":
            subprocess.run(
                ["install_name_tool", "-add_rpath", "@loader_path", str(dl / so.name)],
                stderr=subprocess.DEVNULL,
            )  # sibling libduckdb  # noqa: S603,S607
    for f in (bundle / "share" / "extension").glob("*"):
        shutil.copy(f, de / f.name)

    key_hex = _link_plpython(dl) if "plpython3u" in keys else None

    base = tempfile.mkdtemp(prefix="smoke_pg_ext_")
    db = pgserver.get_server(base)
    # pg_duckdb requires preloading before CREATE EXTENSION
    if "pg_duckdb" in keys:
        db.psql("ALTER SYSTEM SET shared_preload_libraries = 'pg_duckdb';")
        db.cleanup()
        db = pgserver.get_server(base)

    failures = []
    for key in sorted(keys):
        # Support libraries (libduckdb) ship in the bundle but are not CREATE-able extensions.
        if not (de / f"{key}.control").exists():
            continue
        # pgserver.psql does NOT raise on SQL error — verify the load via pg_extension, not the return.
        db.psql(f"CREATE EXTENSION IF NOT EXISTS {key};")
        loaded = key in db.psql(f"SELECT extname FROM pg_extension WHERE extname = '{key}'")
        extra = ""
        if loaded and key == "pg_duckdb":
            has_iceberg = "iceberg_scan" in db.psql(
                "SELECT proname FROM pg_proc WHERE proname = 'iceberg_scan'"
            )
            extra = f" (iceberg_scan: {'yes' if has_iceberg else 'NO'})"
            if not has_iceberg:
                loaded = False
        if loaded:
            print(f"  OK   {key}{extra}")
        else:
            print(f"  FAIL {key}: CREATE EXTENSION did not register it")
            failures.append(f"{key}: did not load")

    if key_hex is not None and not any(f.startswith("plpython3u") for f in failures):
        failures += _fake_functions_compute(db, key_hex)

    if "postgres_fdw" in keys and not any(f.startswith("postgres_fdw") for f in failures):
        failures += _postgres_fdw_reads(db)

    missing = REQUIRED - keys
    if missing:
        failures.append(f"required members missing from bundle: {sorted(missing)}")
    if failures:
        print("SMOKE FAILED:", *failures, sep="\n  ")
        return 1
    print("SMOKE OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(Path(sys.argv[1])))
