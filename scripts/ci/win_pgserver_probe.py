#!/usr/bin/env python3
# Copyright (c) 2026 Kenneth Stott
# Canary: 404862ab-1488-4e23-9925-f6488c1c7552
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Windows viability probe for the EMBEDDED Postgres path (FDW/extension *building* set aside):

  Q1  Does pgserver's embedded PG boot on Windows? (an earlier attempt failed with initdb
      "could not access directory ... Invalid argument" using an AppData\\Local\\Temp data dir —
      retry with a clean, short data dir to see if that was the cause.)
  Q2  Can we ACQUIRE the built-in contrib FDW DLLs (postgres_fdw, file_fdw) from a native Windows
      PostgreSQL 16 install and load them into pgserver's embedded PG? (These ship with every Windows
      PG distribution as .dll; the ABI match is major+OS+arch, same principle proven on macOS.)

Env: PGDATA_DIR (data dir to init), PG16_HOME (a native Windows PG16 root providing lib/ + share/).
"""

from __future__ import annotations

import glob
import os
import shutil
import sys
from pathlib import Path

import pgserver


def _pginstall_dirs():
    """Return (lib_dir, ext_dir) inside pgserver's bundled install, handling layout differences."""
    root = Path(pgserver.__file__).parent / "pginstall"
    lib = root / "lib" / "postgresql"
    if not lib.exists():
        lib = root / "lib"
    ext = root / "share" / "postgresql" / "extension"
    if not ext.exists():
        ext = root / "share" / "extension"
    return lib, ext


def main() -> int:
    suffix = "dll" if os.name == "nt" else ("dylib" if sys.platform == "darwin" else "so")

    # ---- Q1: does embedded pgserver boot with a clean, short data dir? ----
    default = "C:/pgd" if os.name == "nt" else "/tmp/pgd"
    base = Path(os.environ.get("PGDATA_DIR", default))
    shutil.rmtree(base, ignore_errors=True)
    base.mkdir(parents=True, exist_ok=True)
    try:
        srv = pgserver.get_server(str(base))
        ver = srv.psql("SELECT version()").strip().replace("\n", " ")
        print(f"Q1 BOOT OK: {ver[:90]}")
    except Exception as e:  # noqa: BLE001
        print("Q1 BOOT FAILED:", str(e).splitlines()[-1][:200])
        print("=> embedded pgserver does NOT boot on this OS")
        return 1
    print("Q1 => embedded pgserver boots: YES")

    # ---- Q2: acquire the built-in FDW DLLs from a native Windows PG16 and load them ----
    pg16 = os.environ.get("PG16_HOME")
    if not pg16:
        print("Q2 skipped (set PG16_HOME to a native Windows PostgreSQL 16 root)")
        return 0
    src_lib = Path(pg16) / "lib"
    src_ext = Path(pg16) / "share" / "extension"
    if not src_ext.exists():
        src_ext = Path(pg16) / "share" / "postgresql" / "extension"
    dl, de = _pginstall_dirs()
    print(f"Q2 acquiring from {src_lib} -> {dl}")

    results = []
    for fdw in ("postgres_fdw", "file_fdw"):
        so = src_lib / f"{fdw}.{suffix}"
        if not so.exists():
            print(f"  {fdw}.{suffix}: NOT FOUND in native PG16 lib")
            results.append((fdw, "missing-in-source"))
            continue
        shutil.copy(so, dl / so.name)
        for f in glob.glob(str(src_ext / f"{fdw}*")):
            shutil.copy(f, de)
        srv.psql(f"CREATE EXTENSION IF NOT EXISTS {fdw};")
        loaded = fdw in srv.psql(f"SELECT extname FROM pg_extension WHERE extname = '{fdw}'")
        print(
            f"  {fdw}: {'LOADS into embedded pgserver' if loaded else 'copied but FAILED to load'}"
        )
        results.append((fdw, "loads" if loaded else "load-failed"))

    ok = all(r[1] == "loads" for r in results) and results
    print("Q2 => built-in FDW DLLs acquirable + loadable:", "YES" if ok else "NO/partial", results)
    return 0 if ok else 2


if __name__ == "__main__":
    raise SystemExit(main())
