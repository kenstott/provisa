# Copyright (c) 2026 Kenneth Stott
# Canary: 5f3c9e1a-7b2d-4e8f-9a61-2c4d8b0e7f13
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
"""packaging/duckdb-ext/collect.py gathers every platform's mirror into the wheel's _ext tree in the
layout DuckDB loads from: <version>/<platform>/<name>.duckdb_extension, and a named repository's
extension (crypto, from Query.Farm's, on DuckDB 2.0) under repositories/<repo>/ with its .info --
without which ``LOAD crypto FROM query_farm`` finds nothing."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]


def _collect():
    spec = importlib.util.spec_from_file_location(
        "duckdb_ext_collect", REPO / "packaging/duckdb-ext/collect.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["duckdb_ext_collect"] = module
    spec.loader.exec_module(module)
    return module


def _mirrored(root: Path, version: str) -> Path:
    plat = root / "osx-arm64" / f"v{version}" / "osx_arm64"
    (plat / "repositories" / "query_farm").mkdir(parents=True)
    (plat / "sqlite_scanner.duckdb_extension").write_bytes(b"SQLITE")
    (plat / "repositories" / "query_farm" / "crypto.duckdb_extension").write_bytes(b"CRYPTO")
    (plat / "repositories" / "query_farm" / "crypto.duckdb_extension.info").write_bytes(b"INFO")
    (root / "osx-arm64" / "manifest.json").write_text("{}")
    return root


def test_collect_keeps_a_named_repositorys_layout_and_info(tmp_path, monkeypatch):
    collect = _collect()
    dest = tmp_path / "_ext"
    monkeypatch.setattr(collect, "_DEST", dest)
    version = collect._pinned_duckdb_version()

    assert collect.main(_mirrored(tmp_path / "mirror", version)) == 0

    plat = dest / f"v{version}" / "osx_arm64"
    assert (plat / "sqlite_scanner.duckdb_extension").read_bytes() == b"SQLITE"
    assert (plat / "repositories/query_farm/crypto.duckdb_extension").read_bytes() == b"CRYPTO"
    assert (plat / "repositories/query_farm/crypto.duckdb_extension.info").read_bytes() == b"INFO"
    assert not (dest / "manifest.json").exists()


def test_collect_refuses_blobs_of_another_duckdb(tmp_path, monkeypatch):
    collect = _collect()
    monkeypatch.setattr(collect, "_DEST", tmp_path / "_ext")

    assert collect.main(_mirrored(tmp_path / "mirror", "1.5.5")) == 1
