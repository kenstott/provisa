#!/usr/bin/env python3
# Copyright (c) 2026 Kenneth Stott
# Canary: 8c42c343-b5d8-48ba-b993-c36286147bda
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Mirror the DuckDB-proper extensions the standalone DuckDB engine needs, for THIS platform + the
pinned DuckDB version, into a self-hostable repo layout — and prove each LOADs.

Family A (contrast Family B, the PG extensions). The standalone DuckDB engine (build_duckdb_engine)
reaches its sources by INSTALL/LOADing DuckDB extensions. Rather than compile, we mirror the official
core + community builds pinned to our exact DuckDB version, per platform, so offline/air-gapped installs
work deterministically. The extension list is DERIVED from the connector definitions (single source of
truth) plus what no connector declares (duckdb_extensions.required_extensions).

Output (into $OUT, default dist/duckdb-ext/<platform>):
  <duckdb_version>/<platform>/<name>.duckdb_extension    the mirrored, signed extension binaries
  manifest.json    {duckdb_version, platform, extensions:[{name,repo:core|community,file,sha256,
                                                            loaded,required}]}

Every extension must install and load, and everything DuckDB puts in the directory is mirrored, so an
extension another one pulls in (avro for iceberg, motherduck_impl for motherduck) travels with it.
Usage: python mirror_duckdb_extensions.py [out-dir]
"""

from __future__ import annotations

import hashlib
import json
import shutil
import sys
from pathlib import Path

import duckdb


def _sha256(p: Path) -> str:
    h = hashlib.sha256()
    h.update(p.read_bytes())
    return h.hexdigest()


def main(out: Path) -> int:
    con = duckdb.connect()
    raw_version = con.execute("SELECT version()").fetchone()[
        0
    ]  # "v1.5.3" — DuckDB's on-disk dir name
    version = raw_version.lstrip("v")
    platform = con.execute("PRAGMA platform").fetchone()[0]
    shutil.rmtree(out, ignore_errors=True)
    ext_dir = (
        out / "_install"
    ).resolve()  # DuckDB drops <ext_dir>/<version>/<platform>/<real>.duckdb_extension
    ext_dir.mkdir(parents=True)
    con.execute(f"SET extension_directory = '{ext_dir}'")

    print(f"DuckDB {version} / {platform}")
    from provisa.federation.duckdb_extensions import required_extensions

    exts = required_extensions()
    failures = []
    for name in sorted(exts):
        repo = "community" if exts[name] else "core"
        try:
            con.execute(f"INSTALL {name}" + (" FROM community" if exts[name] else ""))
            con.execute(f"LOAD {name}")
            print(f"  OK   {name:12} ({repo})")
        except Exception as e:  # noqa: BLE001
            failures.append(f"{name}: {str(e).splitlines()[0][:90]}")
            print(f"  FAIL {name:12} ({repo}) -- not available/loadable for {platform}@{version}")

    # What is on disk, not what was asked for: a LOAD may bring in further files.
    installed = ext_dir / raw_version / platform
    dst_dir = out / raw_version / platform
    dst_dir.mkdir(parents=True, exist_ok=True)
    records = []
    for src in sorted(installed.glob("*.duckdb_extension")):
        shutil.copy(src, dst_dir / src.name)
        records.append(
            {
                "file": f"{raw_version}/{platform}/{src.name}",
                "sha256": _sha256(dst_dir / src.name),
            }
        )
    shutil.rmtree(ext_dir, ignore_errors=True)
    (out / "manifest.json").write_text(
        json.dumps(
            {"duckdb_version": version, "platform": platform, "extensions": records}, indent=2
        )
    )
    print(
        json.dumps(
            {
                "duckdb_version": version,
                "platform": platform,
                "mirrored": [r["file"] for r in records],
            },
            indent=2,
        )
    )
    if failures:
        print("MIRROR FAILED:", *failures, sep="\n  ")
        return 1
    print("MIRROR OK")
    return 0


if __name__ == "__main__":
    out = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("dist/duckdb-ext")
    raise SystemExit(main(out))
