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
truth) plus the core scanners the base ATTACH connectors need.

Output (into $OUT, default dist/duckdb-ext/<platform>):
  <duckdb_version>/<platform>/<name>.duckdb_extension    the mirrored, signed extension binaries
  manifest.json    {duckdb_version, platform, extensions:[{name,repo:core|community,file,sha256,
                                                            loaded,required}]}

Required members (core) must load; community members are best-effort (availability varies per version/
platform) and recorded but non-fatal. Usage: python mirror_duckdb_extensions.py [out-dir]
"""

from __future__ import annotations

import hashlib
import inspect
import json
import shutil
import sys
from pathlib import Path

import duckdb

# Core scanners the base DuckDB connectors (postgres/sqlite ATTACH) need but don't declare as an
# extension (DuckDB auto-loads them); mirrored so offline attach works. name -> required.
_CORE_SCANNERS = {"postgres": True, "sqlite": True}
_REQUIRED_CORE = {"postgres", "sqlite", "iceberg"}


def _extension_list() -> dict[str, bool]:
    """name -> from_community, derived from the DuckDB connector defs + the core scanners."""
    from provisa.federation import connector as c

    exts: dict[str, bool] = {}
    for name in dir(c):
        obj = getattr(c, name)
        if inspect.isclass(obj) and getattr(obj, "engine", None) == "duckdb":
            ext = getattr(obj, "extension", None)
            if ext:
                exts[ext] = bool(getattr(obj, "install_from_community", False))
    for ext in _CORE_SCANNERS:
        exts.setdefault(ext, False)
    return exts


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
    exts = _extension_list()
    records, failures = [], []
    for name in sorted(exts):
        community = exts[name]
        required = name in _REQUIRED_CORE
        repo = "community" if community else "core"
        real_name = name  # the INSTALL alias may differ from the real extension name (sqlite->sqlite_scanner)
        try:
            con.execute(f"INSTALL {name}" + (" FROM community" if community else ""))
            con.execute(f"LOAD {name}")
            row = con.execute(
                "SELECT extension_name, loaded FROM duckdb_extensions() "
                "WHERE extension_name = ? OR list_contains(aliases, ?)",
                [name, name],
            ).fetchone()
            if row:
                real_name = row[0]
            ok = bool(row and row[1])
        except Exception as e:  # noqa: BLE001
            ok = False
            if required:
                failures.append(f"{name}: {str(e).splitlines()[0][:90]}")
        src = ext_dir / raw_version / platform / f"{real_name}.duckdb_extension"
        if ok and src.exists():
            dst_dir = out / raw_version / platform
            dst_dir.mkdir(parents=True, exist_ok=True)
            shutil.copy(src, dst_dir / src.name)
            rel = f"{raw_version}/{platform}/{src.name}"
            records.append({"name": name, "repo": repo, "file": rel,
                            "sha256": _sha256(dst_dir / src.name), "loaded": True,
                            "required": required})  # fmt: skip
            print(f"  OK   {name:12} ({repo})")
        else:
            records.append({"name": name, "repo": repo, "file": None,
                            "loaded": False, "required": required})  # fmt: skip
            tag = "FAIL" if required else "SKIP"
            print(f"  {tag} {name:12} ({repo}) — not available/loadable for {platform}@{version}")
            if required and f"{name}:" not in " ".join(failures):
                failures.append(f"{name}: not available for {platform}@{version}")

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
                "mirrored": [r["name"] for r in records if r["loaded"]],
            },
            indent=2,
        )
    )
    if failures:
        print("MIRROR FAILED (required):", *failures, sep="\n  ")
        return 1
    print("MIRROR OK")
    return 0


if __name__ == "__main__":
    out = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("dist/duckdb-ext")
    raise SystemExit(main(out))
