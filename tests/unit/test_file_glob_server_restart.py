# Copyright (c) 2026 Kenneth Stott
# Canary: 3b0d6c1e-8f41-4a5e-9c2d-7e5a1f0b9d43
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A files-source glob table registered after its source's server started is served (REQ-788).

The server's file adapter merges a glob table from every file its glob matches, under the table's
registered name, in the model the server is started with. The source's discovery starts the
server before any glob table is registered, so a glob table registered then (the Register Table
form's "Or files matching") was in no model: a read of it named a relation the server never
served ("Table with name complaints does not exist! Did you mean _src_test.test.complaints_2026_09").
Each schema build gives the server the glob tables now registered, and starts it again where they
changed, the engine letting go of its attach of the source."""

# Requirements: REQ-788

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from provisa.federation import served_tables


@pytest.fixture(autouse=True)
def _fresh_record(monkeypatch):
    monkeypatch.setattr(served_tables, "_GLOB_TABLES", {})
    monkeypatch.setattr(served_tables, "_SERVED_TABLES", {})


def _files_source(path: str) -> SimpleNamespace:
    return SimpleNamespace(
        id="test", type=SimpleNamespace(value="files"), path=path, password=None, mapping={}
    )


def _glob_row(glob: str = "complaints_2026_*.csv", column: str | None = None) -> dict:
    """A registered-table row as the model store holds it (registry_view)."""
    return {
        "source_id": "test",
        "schema_name": "test",
        "table_name": "complaints",
        "file_glob": glob,
        "source_file_column": column,
    }


def test_a_registered_glob_table_is_one_relation_under_its_registered_name(tmp_path: Path):
    from provisa.federation.pgwire_replica import _files_operand

    (tmp_path / "complaints_2026_09.csv").write_text("id\n1\n")
    (tmp_path / "complaints_2026_10.csv").write_text("id\n2\n")
    specs = served_tables.file_glob_specs([_glob_row(column="source_file")])
    assert specs == {
        "test": [
            {
                "name": "complaints",
                "file_glob": "complaints_2026_*.csv",
                "source_file_column": "source_file",
            }
        ]
    }
    source = _files_source(str(tmp_path))
    source.file_glob_tables = specs["test"]
    operand = _files_operand(source)
    assert operand["tables"] == [
        {
            "name": "complaints",
            "url": str(tmp_path.resolve() / "complaints_2026_*.csv"),
            "sourceFileColumn": "source_file",
        }
    ]


def test_a_changed_glob_registration_is_a_change_and_the_same_one_is_not():
    assert served_tables.serve_glob_tables("test", []) is False
    spec = {"name": "complaints", "file_glob": "complaints_2026_*.csv"}
    assert served_tables.serve_glob_tables("test", [spec]) is True
    assert served_tables.serve_glob_tables("test", [spec]) is False
    assert served_tables.serve_glob_tables("test", [{**spec, "source_file_column": "f"}]) is True
    assert served_tables.serve_glob_tables("test", []) is True


@pytest.mark.asyncio
async def test_a_glob_table_registered_later_restarts_the_server_and_reattaches_the_source(
    monkeypatch, tmp_path: Path
):
    from provisa.api import app_rebuild
    from provisa.api.admin import schema_common

    calls: list[tuple[str, str]] = []

    async def restart(model):
        calls.append(("restart", model.id))

    monkeypatch.setattr(schema_common, "restart_source_server", restart)
    engine = SimpleNamespace(
        drop_source=lambda source_id, catalog_name: calls.append(("drop", catalog_name)),
        register_source=lambda source, password, catalog_name: calls.append(
            ("register", catalog_name)
        ),
    )
    tables: list = []
    state = SimpleNamespace(
        config=SimpleNamespace(sources=[_files_source(str(tmp_path))], tables=tables),
        federation_engine=engine,
        source_catalogs={"test": "_src_test"},
    )
    # The source discovered, its per-file tables only: nothing to start again.
    tables.append(SimpleNamespace(**{**_glob_row(), "table_name": "other", "file_glob": None}))
    await app_rebuild.serve_registered_tables(state)
    assert calls == []
    # A glob table registered: the server is started again with it, and the source attached anew.
    tables.append(SimpleNamespace(**_glob_row()))
    await app_rebuild.serve_registered_tables(state)
    assert calls == [("restart", "test"), ("drop", "_src_test"), ("register", "_src_test")]
    # Unchanged: left running.
    await app_rebuild.serve_registered_tables(state)
    assert len(calls) == 3
