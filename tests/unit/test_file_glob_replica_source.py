# Copyright (c) 2026 Kenneth Stott
# Canary: 59e4a007-7771-4010-9d2a-352ce9c6a4ad

"""A files-glob table reads as one Arrow stream over its matched files (REQ-788).

The replicator reads the glob with an in-process DuckDB on every engine, so the replica carries
every matched file's rows as one logical table, with _source_file when declared. A file that
differs from the rest is refused by name before any row is read."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from provisa.events.source_loader import SourceRowLoader
from provisa.file_source.files_glob import FileColumnsDiffer

pytestmark = pytest.mark.unit


def _source(path):
    return SimpleNamespace(id="files", type=SimpleNamespace(value="files"), path=path)


def _table(glob, source_file_column=None):
    return SimpleNamespace(
        source_id="files", schema_name="main", table_name="orders",
        file_glob=glob, source_file_column=source_file_column,
    )  # fmt: skip


COLUMNS = [("id", "integer"), ("name", "varchar")]


def _collect(source, table):
    loader = SourceRowLoader(engine=None)
    src = loader.replica_source(SimpleNamespace(source_pools=None), source, table, COLUMNS)

    async def _go():
        rows = []
        async for batch in src.batches(65536):
            rows.extend(batch.to_pylist())
        return rows

    return asyncio.run(_go())


def test_a_glob_reads_every_matched_file_as_one_table(tmp_path):
    (tmp_path / "a.csv").write_text("id,name\n1,x\n2,y\n")
    (tmp_path / "b.csv").write_text("id,name\n3,z\n")
    rows = _collect(_source(str(tmp_path / "a.csv")), _table("*.csv"))
    assert sorted(r["id"] for r in rows) == [1, 2, 3]
    assert set(rows[0]) == {"id", "name"}  # only the declared columns


def test_source_file_column_carries_each_rows_file(tmp_path):
    (tmp_path / "a.csv").write_text("id,name\n1,x\n")
    (tmp_path / "b.csv").write_text("id,name\n2,y\n")
    rows = _collect(_source(str(tmp_path / "a.csv")), _table("*.csv", source_file_column="_sf"))
    by_id = {r["id"]: r["_sf"] for r in rows}
    assert by_id[1].endswith("a.csv") and by_id[2].endswith("b.csv")


def test_a_differing_file_is_refused_by_name_before_any_row(tmp_path):
    (tmp_path / "a.csv").write_text("id,name\n1,x\n")
    (tmp_path / "b.csv").write_text("id,name,extra\n2,y,z\n")
    with pytest.raises(FileColumnsDiffer):
        _collect(_source(str(tmp_path / "a.csv")), _table("*.csv"))
