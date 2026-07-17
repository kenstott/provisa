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
into pgserver's pginstall, then loads each extension. Required members must load; mysql_fdw is
best-effort (its client lib must be discoverable to the PG process — a packaging detail).

Usage: python smoke_pg_extensions.py <bundle-dir>
Exit non-zero if any REQUIRED extension fails to load.
"""

from __future__ import annotations

import json
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

import pgserver

REQUIRED = {"file_fdw", "postgres_fdw", "sqlite_fdw", "pg_duckdb"}
BEST_EFFORT = {"mysql_fdw"}


def main(bundle: Path) -> int:
    manifest = json.loads((bundle / "manifest.json").read_text())
    keys = {a["key"] for a in manifest["artifacts"]}

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
        elif key in BEST_EFFORT:
            print(f"  WARN {key} (best-effort): did not load (client lib not discoverable to PG?)")
        else:
            print(f"  FAIL {key}: CREATE EXTENSION did not register it")
            failures.append(f"{key}: did not load")

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
