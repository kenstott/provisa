# Copyright (c) 2026 Kenneth Stott
# Canary: 6eaa1a8b-264d-4bdc-b494-06d5d32f2b52
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A demo's declared org files (REQ-2101, amended 2026-10-21): files it ships beside its config
(org_files/) for its organisations' own folder, copied there once when the org's model is built,
so a source reads them in place ({files}/<name>) and nothing is fetched -- starthere's tour
builds its live source from supplies.parquet so in a sandbox, which may reach no address its demo
does not declare."""

# Requirements: REQ-2101

from __future__ import annotations

from pathlib import Path

import pytest

from provisa.core import declared_org_files as dof

pytestmark = pytest.mark.unit

_REPO = Path(__file__).resolve().parents[2]


@pytest.fixture
def org_folder(monkeypatch, tmp_path):
    folder = tmp_path / "orgs" / "sbx-1"
    monkeypatch.setattr("provisa.core.org_files.folder", lambda _org: folder)
    return folder


def _demo(tmp_path, files: dict[str, bytes]) -> Path:
    demo = tmp_path / "demo"
    for name, data in files.items():
        (demo / "org_files" / name).parent.mkdir(parents=True, exist_ok=True)
        (demo / "org_files" / name).write_bytes(data)
    return demo


def test_each_declared_file_is_copied_into_the_orgs_files(tmp_path, org_folder):
    demo = _demo(tmp_path, {"supplies.parquet": b"PAR1", "more/notes.csv": b"a,b"})
    assert dof.adopt_declared_org_files("sbx-1", demo) == ["more/notes.csv", "supplies.parquet"]
    assert (org_folder / "files" / "supplies.parquet").read_bytes() == b"PAR1"
    assert (org_folder / "files" / "more" / "notes.csv").read_bytes() == b"a,b"


def test_a_file_already_there_with_the_same_bytes_is_left_as_it_is(tmp_path, org_folder):
    demo = _demo(tmp_path, {"supplies.parquet": b"PAR1"})
    dof.adopt_declared_org_files("sbx-1", demo)
    assert dof.adopt_declared_org_files("sbx-1", demo) == []
    (demo / "org_files" / "supplies.parquet").write_bytes(b"PAR2")
    assert dof.adopt_declared_org_files("sbx-1", demo) == ["supplies.parquet"]
    assert (org_folder / "files" / "supplies.parquet").read_bytes() == b"PAR2"


def test_a_directory_without_org_files_declares_none(tmp_path, org_folder):
    assert dof.declared_org_files(tmp_path) == []
    assert dof.adopt_declared_org_files("sbx-1", tmp_path) == []
    assert not (org_folder / "files").exists()


def test_starthere_ships_the_supplies_file_provisa_publishes():
    starthere = _REPO / "demo" / "named" / "starthere"
    shipped = starthere / "org_files" / "supplies.parquet"
    assert [p.name for p in dof.declared_org_files(starthere)] == ["supplies.parquet"]
    assert shipped.read_bytes() == (_REPO / "site" / "demo-data" / "supplies.parquet").read_bytes()


def test_the_status_names_the_demos_org_files_for_the_tour():
    from provisa.api.setup_router import demo_org_files

    assert demo_org_files(_REPO / "demo" / "named" / "starthere") == ["supplies.parquet"]
    assert demo_org_files(None) == []
