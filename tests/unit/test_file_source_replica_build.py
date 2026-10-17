# Copyright (c) 2026 Kenneth Stott
# Canary: 23df973e-d35d-4722-b193-b51001d54792
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""#255: a csv or parquet source floored to its replica (``replicate: 0``) has no live attach on
any engine (REQ-1912), so its replica is built from the file by its own reader -- not by an engine
read of the attach the floor removed (``Catalog "<id>" does not exist``)."""

from __future__ import annotations

from types import SimpleNamespace

import duckdb
import pytest

import provisa.api.app  # noqa: F401 -- the API layer says whether the deployment holds many orgs, which reading a source's secrets asks

from provisa.core.operator_floor import floor_setting
from provisa.events import source_loader as sl
from provisa.federation.replica_source import BlockingCursorSource, EngineTableSource

COLUMNS = [("id", "bigint"), ("name", "text")]


def _table():
    return SimpleNamespace(
        schema_name="main",
        table_name="orders",
        columns=[SimpleNamespace(name=c, native_filter_type=None) for c, _ in COLUMNS],
        landing_filter=None,
    )


def _file(tmp_path, kind: str) -> str:
    path = str(tmp_path / f"orders.{kind}")
    fmt = "(HEADER)" if kind == "csv" else "(FORMAT parquet)"
    con = duckdb.connect()
    con.execute(
        f"COPY (SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) t(id, name)) TO '{path}' {fmt}"
    )
    con.close()
    return path


def _floored(kind: str, path: str):
    source = SimpleNamespace(id="shop_orders", type=kind, path=path, replicate=0)
    assert floor_setting(source) is not None  # replicate: 0 floors every read to the replica
    return source


def _loader():
    """The build's row loader as the app wires it: the registered adapters, and an engine whose
    read of the floored source's attach would fail."""
    from provisa.events.app_wiring import build_adapter_loaders

    adapters = build_adapter_loaders(SimpleNamespace(config=SimpleNamespace()), object())
    return sl.SourceRowLoader(object(), adapter_loaders=adapters)


async def _rows(reader, batch_rows=2):
    return [row async for b in reader.batches(batch_rows) for row in b.to_pylist()]


@pytest.mark.parametrize("kind", ["csv", "parquet"])
async def test_a_floored_file_source_builds_its_replica_from_the_file(tmp_path, kind):
    source = _floored(kind, _file(tmp_path, kind))
    reader = _loader().replica_source(SimpleNamespace(source_pools=None), source, _table(), COLUMNS)
    assert not isinstance(reader, EngineTableSource)  # not the attach the floor removed
    assert isinstance(reader, BlockingCursorSource)
    assert await _rows(reader) == [
        {"id": 1, "name": "a"},
        {"id": 2, "name": "b"},
        {"id": 3, "name": "c"},
    ]


async def test_a_file_path_kept_as_written_is_resolved_where_it_is_read(tmp_path, monkeypatch):
    """REQ-1919: the store keeps ``${env:...}`` in the path; it is resolved at the read."""
    _file(tmp_path, "csv")
    monkeypatch.setenv("IT_FILES", str(tmp_path))
    source = _floored("csv", "${env:IT_FILES}/orders.csv")
    reader = sl.make_file_loader("csv").replica_source(source, _table(), COLUMNS)
    assert len(await _rows(reader, 10)) == 3


async def test_a_file_source_read_whole_lands_the_registered_columns(tmp_path):
    source = _floored("parquet", _file(tmp_path, "parquet"))
    rows = await sl.make_file_loader("parquet")(source, _table())
    assert rows == [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}, {"id": 3, "name": "c"}]


@pytest.mark.parametrize("kind", ["csv", "parquet"])
def test_a_file_source_not_floored_keeps_the_engines_in_place_copy(tmp_path, kind):
    """Its attach exists: the engine copies the replica in place, its rows never passing through
    Provisa."""
    source = SimpleNamespace(
        id="shop_orders", type=kind, path=_file(tmp_path, kind), replicate=None
    )
    assert floor_setting(source) is None
    reader = _loader().replica_source(SimpleNamespace(source_pools=None), source, _table(), COLUMNS)
    assert isinstance(reader, EngineTableSource)


async def test_a_floored_file_source_read_by_the_event_loop_is_read_from_the_file(tmp_path):
    source = _floored("csv", _file(tmp_path, "csv"))
    assert len(await _loader().load(source, _table())) == 3


def test_a_file_source_with_no_path_is_refused_by_name():
    source = SimpleNamespace(id="shop_orders", type="csv", path=None, replicate=0)
    with pytest.raises(ValueError, match="csv source 'shop_orders' has no path"):
        sl.make_file_loader("csv").replica_source(source, _table(), COLUMNS)
