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
truth) plus the core scanners the base ATTACH connectors need, and the extensions DuckDB's own
repositories do not carry (provisa.federation.duckdb_extensions.EXTENSION_REPOSITORIES: crypto, from
Query.Farm's repository), installed from the named repository their publisher documents.

Output (into $OUT, default dist/duckdb-ext/<platform>):
  <duckdb_version>/<platform>/<name>.duckdb_extension    the mirrored, signed extension binaries
  <duckdb_version>/<platform>/repositories/<repo>/<name>.duckdb_extension(.info)
                   a named repository's extension, in DuckDB's own layout (LOAD <name> FROM <repo>)
  manifest.json    {duckdb_version, platform, extensions:[{name,repo:core|community|<repo>,file,
                                                            sha256,loaded,required}]}

Required members (core) must load; community members are best-effort (availability varies per version/
platform) and recorded but non-fatal. Usage: python mirror_duckdb_extensions.py [out-dir]
"""

from __future__ import annotations

import hashlib
import json
import shutil
import sys
from pathlib import Path

import duckdb

# Core scanners the base DuckDB connectors (postgres/sqlite ATTACH) need but don't declare as an
# extension (DuckDB auto-loads them); mirrored so offline attach works. name -> required.
_CORE_SCANNERS = {"postgres": True, "sqlite": True}
# REQ-1997: what a file source's location needs (connector_duckdb.file_location_details): httpfs
# for https, s3 and gcs, azure for Azure -- loaded by the runtime, declared by no connector.
_LOCATION_EXTENSIONS = ("httpfs", "azure")
# crypto: sha224/sha384/sha512 (REQ-2171) -- the maintainer's decision (2026-11-05) is that it ships.
_REQUIRED = {"postgres", "sqlite", "iceberg", "crypto"}


def _extension_list() -> dict[str, bool]:
    """name -> from_community: every extension the DuckDB engine's own connectors declare (the
    engine is built as the runtime builds it, so a connector moved between modules is still
    found), the core scanners, and the file-location extensions."""
    from provisa.federation.engine import build_duckdb_engine

    exts: dict[str, bool] = {}
    for connector in build_duckdb_engine().connectors.values():
        ext = getattr(connector, "extension", None)
        if ext:
            exts[ext] = bool(getattr(connector, "install_from_community", False))
    for ext in (*_CORE_SCANNERS, *_LOCATION_EXTENSIONS):
        exts.setdefault(ext, False)
    return exts


def _sources() -> dict[str, str]:
    """name -> the repository it is installed from: core, community, or the named repository
    (EXTENSION_REPOSITORIES) its publisher documents."""
    from provisa.federation.duckdb_extensions import EXTENSION_REPOSITORIES

    sources = {name: "community" if c else "core" for name, c in _extension_list().items()}
    sources.update({name: repo for name, (repo, _prefix) in EXTENSION_REPOSITORIES.items()})
    return sources


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
    from provisa.federation.duckdb_extensions import EXTENSION_REPOSITORIES

    if EXTENSION_REPOSITORIES:
        con.execute("SET allow_extension_repositories = 'allowed'")
    for repo, prefix in set(EXTENSION_REPOSITORIES.values()):
        con.execute(f"CREATE EXTENSION REPOSITORY IF NOT EXISTS {repo} WITH PREFIX '{prefix}'")

    print(f"DuckDB {version} / {platform}")
    sources = _sources()
    records, failures = [], []
    for name in sorted(sources):
        repo = sources[name]
        required = name in _REQUIRED
        named = repo not in ("core", "community")
        real_name = name  # the INSTALL alias may differ from the real extension name (sqlite->sqlite_scanner)
        try:
            con.execute(f"INSTALL {name}" + ("" if repo == "core" else f" FROM {repo}"))
            con.execute(f"LOAD {name}" + (f" FROM {repo}" if named else ""))
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
        # DuckDB keeps a named repository's extension apart, with the .info it is loaded by.
        sub = Path(raw_version, platform, *(("repositories", repo) if named else ()))
        src = ext_dir / sub / f"{real_name}.duckdb_extension"
        if ok and src.exists():
            dst_dir = out / sub
            dst_dir.mkdir(parents=True, exist_ok=True)
            shutil.copy(src, dst_dir / src.name)
            if named:
                shutil.copy(src.with_name(f"{src.name}.info"), dst_dir / f"{src.name}.info")
            rel = (sub / src.name).as_posix()
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
