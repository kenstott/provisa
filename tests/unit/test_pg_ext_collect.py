# Copyright (c) 2026 Kenneth Stott
# Canary: 78af5d09-3912-4240-a0a9-d5dd894e6629
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
"""packaging/pg-ext/collect.py refuses a platform tree whose manifest does not match its files.

The universal provisa-pg-ext wheel is built from what collect.py gathers, and a version on PyPI
cannot be replaced: 0.1.1 shipped five manifest rows that matched nothing, which every installer
that checks them refuses. So the tree is checked here, before the wheel exists."""

from __future__ import annotations

import hashlib
import importlib.util
import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]


def _collect():
    spec = importlib.util.spec_from_file_location(
        "pg_ext_collect", REPO / "packaging/pg-ext/collect.py"
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules["pg_ext_collect"] = module
    spec.loader.exec_module(module)
    return module


def _tree(root: Path, platform: str, body: bytes, recorded: bytes) -> Path:
    plat = root / platform
    (plat / "lib").mkdir(parents=True)
    (plat / "lib" / "postgres_fdw.so").write_bytes(body)
    row = {"name": "postgres_fdw", "file": "lib/postgres_fdw.so"}
    row["sha256"] = hashlib.sha256(recorded).hexdigest()
    (plat / "manifest.json").write_text(json.dumps({"artifacts": [row]}))
    return plat


def test_a_matching_tree_has_no_stale_rows(tmp_path):
    plat = _tree(tmp_path, "linux-x64", b"SHIPPED", recorded=b"SHIPPED")
    assert _collect().stale_rows(plat) == []


def test_a_row_hashed_before_the_file_changed_is_stale(tmp_path):
    plat = _tree(tmp_path, "linux-x64", b"RELOCATED", recorded=b"BEFORE-PATCHELF")
    assert _collect().stale_rows(plat) == [
        "lib/postgres_fdw.so: sha256 does not match its manifest row"
    ]


def test_collect_refuses_to_package_a_stale_tree(tmp_path, monkeypatch, capsys):
    collect = _collect()
    monkeypatch.setattr(collect, "_DEST", tmp_path / "dest")
    _tree(tmp_path / "artifacts", "linux-x64", b"RELOCATED", recorded=b"BEFORE-PATCHELF")

    assert collect.main(tmp_path / "artifacts") == 1
    assert "linux-x64: lib/postgres_fdw.so: sha256 does not match" in capsys.readouterr().err
    assert not (tmp_path / "dest").exists()
