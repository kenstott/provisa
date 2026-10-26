# Copyright (c) 2026 Kenneth Stott
# Canary: e099bdf1-6d48-4ae0-9edf-5deefba11d5d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1915: a row-level table read through a filter that is not a key. The filter is pushed to
the source as a WHERE; its rows are spooled in bounded batches, admitted by the region's row rule,
and replaced in the replica in one transaction with the filter's fetch time; the same filter is
served from the replica until its TTL passes; a source that cannot apply it is refused by name."""

from __future__ import annotations

import asyncio
import os
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import duckdb
import pyarrow as pa
import pytest

from provisa.api.errors import RowLevelFilterNotPushable
from provisa.compiler.pk_bounds import FilterBound
from provisa.federation import row_filter
from provisa.federation.store_connection import (
    filter_fetched_at_duckdb_native,
    filter_record_table,
    replace_filtered_rows_duckdb_native,
)

_COLUMNS = [("id", "bigint"), ("status", "text"), ("amount", "double")]
_TABLE = SimpleNamespace(table_name="orders", schema_name="sales", source_id="pg")


# -- the statement pushed to the source -----------------------------------------------------------


@pytest.mark.parametrize(
    ("dialect", "expected"),
    [
        (
            "postgres",
            'SELECT "id", "status" FROM "sales"."orders" WHERE "amount" > 100 AND "status" IN (\'a\')',
        ),
        (
            "tsql",
            "SELECT [id], [status] FROM [sales].[orders] WHERE [amount] > 100 AND [status] IN ('a')",
        ),
        (
            "mysql",
            "SELECT `id`, `status` FROM `sales`.`orders` WHERE `amount` > 100 AND `status` IN ('a')",
        ),
    ],
)
def test_the_filter_is_pushed_as_the_source_s_own_where(dialect, expected):
    sql = row_filter.source_sql(
        _TABLE, ["id", "status"], '"amount" > 100 AND "status" IN (\'a\')', dialect
    )
    assert sql == expected


# -- the replace, on a DuckDB store ---------------------------------------------------------------


def _spool(tmp_path, rows: list[dict]) -> str:
    from provisa.federation.materialize_exec import _ROW_CACHED_AT, _ROW_EXPIRES_AT

    now = datetime(2026, 10, 5, 12, 0, 0)
    stamped = [{**r, _ROW_CACHED_AT: now, _ROW_EXPIRES_AT: now + timedelta(hours=1)} for r in rows]
    schema = pa.schema(
        [
            ("id", pa.int64()),
            ("status", pa.string()),
            ("amount", pa.float64()),
            (_ROW_CACHED_AT, pa.timestamp("us")),
            (_ROW_EXPIRES_AT, pa.timestamp("us")),
        ]
    )
    path = str(tmp_path / "spool.arrow")
    with pa.ipc.new_file(path, schema) as writer:
        if stamped:
            writer.write_batch(pa.RecordBatch.from_pylist(stamped, schema=schema))
    return path


def _full_columns():
    from provisa.federation.materialize_exec import _ROW_CACHED_AT, _ROW_EXPIRES_AT

    return _COLUMNS + [(_ROW_CACHED_AT, "timestamp"), (_ROW_EXPIRES_AT, "timestamp")]


def _replace(con, path, where, fetched_at=datetime(2026, 10, 5, 12, 0, 0)):
    return replace_filtered_rows_duckdb_native(
        con,
        catalog="memory",
        schema="r",
        table="orders",
        columns=_full_columns(),
        pk_columns=["id"],
        where_sql=where,
        spool_path=path,
        filter_hash="h1",
        filter_sql='"amount" > 100',
        fetched_at=fetched_at,
    )


def test_a_filter_s_rows_are_replaced_and_its_fetch_recorded_in_one_transaction(tmp_path):
    con = duckdb.connect()
    con.execute("CREATE SCHEMA r")
    con.execute(
        "CREATE TABLE r.orders (id BIGINT, status VARCHAR, amount DOUBLE, "
        "_row_cached_at TIMESTAMP, _row_expires_at TIMESTAMP)"
    )
    # 1: matched the filter last time, deleted at the source since. 2: still matches, changed.
    # 3: never matched (landed by a key read) and is left alone.
    con.execute(
        "INSERT INTO r.orders VALUES (1, 'old', 500, NULL, NULL), (2, 'old', 200, NULL, NULL), "
        "(3, 'key', 5, NULL, NULL)"
    )
    path = _spool(
        tmp_path,
        [{"id": 2, "status": "new", "amount": 250.0}, {"id": 4, "status": "new", "amount": 900.0}],
    )

    assert _replace(con, path, "amount > 100") == 2

    rows = con.execute("SELECT id, status FROM r.orders ORDER BY id").fetchall()
    assert rows == [(2, "new"), (3, "key"), (4, "new")]
    at = filter_fetched_at_duckdb_native(
        con, catalog="memory", schema="r", table="orders", filter_hash="h1"
    )
    assert at == datetime(2026, 10, 5, 12, 0, 0)
    assert filter_record_table("orders") == "orders__filters"


def test_a_filter_no_row_matches_any_more_clears_its_rows(tmp_path):
    con = duckdb.connect()
    con.execute("CREATE SCHEMA r")
    con.execute(
        "CREATE TABLE r.orders (id BIGINT, status VARCHAR, amount DOUBLE, "
        "_row_cached_at TIMESTAMP, _row_expires_at TIMESTAMP)"
    )
    con.execute("INSERT INTO r.orders VALUES (1, 'old', 500, NULL, NULL)")
    assert _replace(con, _spool(tmp_path, []), "amount > 100") == 0
    assert con.execute("SELECT count(*) FROM r.orders").fetchone() == (0,)


def test_a_failed_replace_leaves_the_replica_and_the_record_as_they_were(tmp_path):
    con = duckdb.connect()
    con.execute("CREATE SCHEMA r")
    con.execute(
        "CREATE TABLE r.orders (id BIGINT, status VARCHAR, amount DOUBLE, "
        "_row_cached_at TIMESTAMP, _row_expires_at TIMESTAMP)"
    )
    con.execute("INSERT INTO r.orders VALUES (1, 'old', 500, NULL, NULL)")
    with pytest.raises(duckdb.Error):
        _replace(con, _spool(tmp_path, []), "no_such_column > 1")
    assert con.execute("SELECT id FROM r.orders").fetchall() == [(1,)]
    assert (
        filter_fetched_at_duckdb_native(
            con, catalog="memory", schema="r", table="orders", filter_hash="h1"
        )
        is None
    )


# -- the spool: bounded batches, region admission, refusals by name -------------------------------


class _Pools:
    def __init__(self, *, stream: bool = True) -> None:
        self._stream = stream

    def has(self, source_id: str) -> bool:
        return True

    def supports_stream(self, source_id: str) -> bool:
        return self._stream


def _state(**overrides):
    base = dict(source_pools=_Pools(), source_dialects={"pg": "postgres"})
    base.update(overrides)
    return SimpleNamespace(**base)


def _source(stype: str = "postgresql"):
    return SimpleNamespace(id="pg", type=stype)


def _fake_stream(monkeypatch, batches: list[list[dict]], seen_sql: list[str]):
    from provisa.core.ir_arrow import arrow_schema

    class _Reader:
        def __init__(self, open_stream, columns):
            self._columns = columns

        async def batches(self, batch_rows):
            for rows in batches:
                yield pa.RecordBatch.from_pylist(rows, schema=arrow_schema(self._columns))

    def _direct(open_stream, columns):
        return _Reader(open_stream, columns)

    monkeypatch.setattr("provisa.federation.replica_source.DirectTableSource", _direct)
    monkeypatch.setattr(
        "provisa.executor.direct.open_direct_stream",
        lambda pools, source_id, sql, params: seen_sql.append(sql),
    )


def _spool_rows(state, source=None):
    return asyncio.run(
        row_filter.spool_filtered_rows(
            state,
            source or _source(),
            _TABLE,
            '"amount" > 100',
            _COLUMNS,
            fetched_at=datetime(2026, 10, 5, 12, tzinfo=UTC),
            reap=3600,
        )
    )


def test_the_spool_holds_the_admitted_rows_stamped(monkeypatch):
    seen: list[str] = []
    _fake_stream(
        monkeypatch,
        [[{"id": 1, "status": "a", "amount": 150.0}], [{"id": 2, "status": "eu", "amount": 300.0}]],
        seen,
    )
    # the region keeps only status 'a' (G3b admission)
    monkeypatch.setattr(
        "provisa.federation.region_rows.admit_rows",
        lambda state, table, data: data.filter(pa.compute.equal(data["status"], "a")),
    )
    path, rows = _spool_rows(_state())
    try:
        table = pa.ipc.open_file(pa.memory_map(path)).read_all()
        assert rows == 1 and table.column("id").to_pylist() == [1]
        assert table.schema.names == ["id", "status", "amount", "_row_cached_at", "_row_expires_at"]
    finally:
        os.unlink(path)


def test_the_regions_row_rule_is_pushed_with_the_filter(monkeypatch):
    """REQ-1922: a rule on the table's own columns travels in the source's WHERE, so a row this
    region may not keep never leaves the source; the fetched rows are still judged."""
    _fake_stream(monkeypatch, [], [])
    monkeypatch.setattr("provisa.federation.region_rows.admit_rows", lambda s, t, d: d)
    monkeypatch.setattr(
        "provisa.federation.region_rows.fetch_predicate", lambda state, table: "\"status\" = 'eu'"
    )
    pushed: list[str] = []
    source_sql = row_filter.source_sql
    monkeypatch.setattr(
        row_filter,
        "source_sql",
        lambda table, names, predicate, dialect: (
            pushed.append(predicate) or source_sql(table, names, predicate, dialect)
        ),
    )
    path, _rows = _spool_rows(_state())
    os.unlink(path)
    assert pushed == ['("amount" > 100) AND ("status" = \'eu\')']


@pytest.mark.parametrize(
    ("state", "source", "why"),
    [
        (_state(source_pools=_Pools(stream=False)), None, "no server-side cursor"),
        (_state(), _source("mongodb"), "not pushed to this source type yet"),
    ],
)
def test_a_source_that_cannot_apply_the_filter_is_refused_by_name(monkeypatch, state, source, why):
    _fake_stream(monkeypatch, [], [])
    with pytest.raises(RowLevelFilterNotPushable, match=why) as refused:
        _spool_rows(state, source)
    assert refused.value.code == "data.row_level_filter_not_pushable"
    assert refused.value.params["table"] == "orders"


def test_a_fetch_the_deadline_cuts_leaves_no_spool(monkeypatch, tmp_path):
    from provisa.core import request_deadline

    _fake_stream(monkeypatch, [[{"id": 1, "status": "a", "amount": 150.0}]], [])
    monkeypatch.setattr("provisa.federation.region_rows.admit_rows", lambda s, t, d: d)
    monkeypatch.setattr(request_deadline, "check", lambda: (_ for _ in ()).throw(TimeoutError()))
    monkeypatch.setattr("tempfile.tempdir", str(tmp_path))
    with pytest.raises(TimeoutError):
        _spool_rows(_state())
    assert list(tmp_path.iterdir()) == []


# -- served from the replica while fresh ----------------------------------------------------------


def test_a_fresh_filter_is_served_from_the_replica_and_a_stale_one_is_fetched_once(monkeypatch):
    from contextlib import asynccontextmanager

    bound = FilterBound("pg", "sales", "orders", ("id",), '"amount" > 100')
    table = SimpleNamespace(
        table_name="orders", schema_name="sales", source_id="pg", row_materialize=True
    )
    source = SimpleNamespace(id="pg", type="postgresql")
    backend = SimpleNamespace(
        dialect="postgres",
        replica_address=lambda state, **k: SimpleNamespace(schema="r", table="orders"),
    )
    state = SimpleNamespace(
        federation_engine=SimpleNamespace(engine=SimpleNamespace(backend=backend))
    )

    async def _sources(state):
        return [source]

    async def _tables(state):
        return [table]

    @asynccontextmanager
    async def _lock(node, key):
        yield

    record: dict = {"at": None}
    fetched: list = []

    async def _fetched_at(*args):
        return record["at"]

    async def _spool(*args, fetched_at, reap, **kwargs):
        fetched.append(fetched_at)
        return "/nonexistent", 0

    async def _replace(*args, fetched_at, **kwargs):
        record["at"] = fetched_at
        return 3

    monkeypatch.setattr("provisa.federation.registry_view.registered_sources", _sources)
    monkeypatch.setattr("provisa.federation.registry_view.registered_tables", _tables)
    monkeypatch.setattr(
        "provisa.federation.residency.resolve_landing_args",
        lambda s, t, platform: SimpleNamespace(columns=_COLUMNS),
    )
    monkeypatch.setattr(
        "provisa.federation.query_residency._row_cache_ttls", lambda t, s, r: (300, 3600)
    )
    monkeypatch.setattr("provisa.federation.query_residency._node", lambda t: "pg/sales.orders")

    async def _ensure_table(*args):
        return None

    monkeypatch.setattr("provisa.federation.query_residency._ensure_row_cache_table", _ensure_table)
    monkeypatch.setattr("provisa.events.row_lock.row_lock", _lock)
    monkeypatch.setattr(row_filter, "filter_fetched_at", _fetched_at)
    monkeypatch.setattr(row_filter, "spool_filtered_rows", _spool)
    monkeypatch.setattr(row_filter, "replace_filtered_rows", _replace)
    monkeypatch.setattr(os, "unlink", lambda path: None)

    first = asyncio.run(row_filter.ensure_filtered_rows_resident(state, [bound], reader_role=None))
    again = asyncio.run(row_filter.ensure_filtered_rows_resident(state, [bound], reader_role=None))
    assert first == [("pg", "orders", 3)] and again == [("pg", "orders", 0)]
    assert len(fetched) == 1

    record["at"] = datetime.now(UTC) - timedelta(seconds=301)  # past the reader's TTL
    asyncio.run(row_filter.ensure_filtered_rows_resident(state, [bound], reader_role=None))
    assert len(fetched) == 2


def test_the_record_key_is_the_canonical_filter_s_digest():
    assert row_filter.filter_hash('"amount" > 100') == row_filter.filter_hash('"amount" > 100')
    assert row_filter.filter_hash('"amount" > 100') != row_filter.filter_hash('"amount" > 101')


async def test_a_filter_recorded_nowhere_was_never_fetched_and_asking_creates_nothing(tmp_path):
    """Asked before the replica's first fetch, the store holds no record of it (on Postgres, not
    even the replica's schema): the filter was never fetched, and the read creates nothing -- it
    created the record in a schema that did not exist, and the read failed."""
    import sqlite3

    store = tmp_path / "store.sqlite"
    sqlite3.connect(store).close()
    engine = SimpleNamespace(engine=SimpleNamespace(materialize_store=lambda: f"sqlite:///{store}"))
    at = await row_filter.filter_fetched_at(
        SimpleNamespace(), SimpleNamespace(dialect="postgres"), engine, "", "orders", "abc"
    )
    assert at is None
    tables = sqlite3.connect(store).execute("SELECT name FROM sqlite_master").fetchall()
    assert tables == []
