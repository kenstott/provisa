# Copyright (c) 2026 Kenneth Stott
# Canary: 1043dcbd-2d6d-4bf5-afed-8b37ac4a5ccd
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1865: SourceRowLoader.load_keys — the keyed IN-predicate builder, and the explicit
registration-gap error for a query-API (adapter-fetch-only) source type."""

from __future__ import annotations

import types

import pytest

from provisa.events.source_loader import (
    SourceRowLoader,
    UnsupportedSourceFetch,
    _pk_in_clause,
    _sql_literal,
)


def test_sql_literal_quotes_strings_and_escapes():
    assert _sql_literal(5) == "5"
    assert _sql_literal(5.5) == "5.5"
    assert _sql_literal(None) == "NULL"
    assert _sql_literal(True) == "TRUE"
    assert _sql_literal("o'brien") == "'o''brien'"


def test_pk_in_clause_single_column():
    clause = _pk_in_clause(["id"], [(1,), (2,)])
    assert clause == '"id" IN (1, 2)'


def test_pk_in_clause_composite():
    clause = _pk_in_clause(["id", "region"], [(1, "us"), (2, "eu")])
    assert clause == "(\"id\", \"region\") IN ((1, 'us'), (2, 'eu'))"


@pytest.mark.asyncio
async def test_load_keys_empty_returns_empty_without_touching_engine():
    loader = SourceRowLoader(engine=None)
    result = await loader.load_keys(types.SimpleNamespace(type="postgresql"), None, ["id"], [])
    assert result == []


@pytest.mark.asyncio
async def test_load_keys_raises_for_adapter_fetch_only_source():
    loader = SourceRowLoader(engine=None)
    source = types.SimpleNamespace(type="neo4j", id="neo")
    with pytest.raises(UnsupportedSourceFetch, match="no keyed-fetch translation"):
        await loader.load_keys(source, None, ["id"], [(1,)])


@pytest.mark.asyncio
async def test_load_keys_runs_bounded_select_through_engine_terminal():
    calls = []

    class _FakeResult:
        column_names = ["id", "status"]
        rows = [(1, "new")]

    class _FakeEngine:
        def address_replicas(self, sql):
            return sql  # this stand-in's tables are all read where the statement names them

        async def execute_engine(self, sql):
            calls.append(sql)
            return _FakeResult()

    loader = SourceRowLoader(engine=_FakeEngine())
    source = types.SimpleNamespace(type="postgresql", id="pg1")
    table = types.SimpleNamespace(schema_name="public", table_name="orders")
    rows = await loader.load_keys(source, table, ["id"], [(1,)])
    assert rows == [{"id": 1, "status": "new"}]
    assert len(calls) == 1
    assert "IN (1)" in calls[0]
    assert "public" in calls[0] and "orders" in calls[0]


def test_pk_in_clauses_within_splits_under_the_limit_and_names_every_key_once():
    from provisa.events.source_loader import _pk_in_clauses_within

    keys = [(i,) for i in range(2, 200_001, 2)]  # no consecutive run: every key in an IN list
    clauses = _pk_in_clauses_within(["order_id"], keys, 200_000)
    assert len(clauses) > 1
    assert all(len(c) <= 200_000 for c in clauses)
    named = [int(v) for c in clauses for v in c.split("IN (", 1)[1].rstrip(")").split(", ")]
    assert named == list(range(2, 200_001, 2))


def test_pk_in_clauses_within_names_a_consecutive_integer_run_as_one_between():
    from provisa.events.source_loader import _pk_in_clauses_within

    keys = [(i,) for i in range(1, 1_000_001)] + [(2_000_000,), (2_000_002,)]
    assert _pk_in_clauses_within(["order_id"], keys, 200_000) == [
        '("order_id" BETWEEN 1 AND 1000000)',
        '"order_id" IN (2000000, 2000002)',
    ]


def test_pk_in_clauses_within_keeps_short_runs_and_non_integer_keys_in_in_lists():
    from provisa.events.source_loader import _pk_in_clauses_within

    assert _pk_in_clauses_within(["id"], [(1,), (2,), (5,)], 200_000) == ['"id" IN (1, 2, 5)']
    assert _pk_in_clauses_within(["id"], [("a",), ("b",)], 200_000) == ["\"id\" IN ('a', 'b')"]
    assert _pk_in_clauses_within(["id", "r"], [(1, "x"), (2, "x"), (3, "x")], 200_000) == [
        "(\"id\", \"r\") IN ((1, 'x'), (2, 'x'), (3, 'x'))"
    ]


def test_pk_in_clauses_within_keeps_a_small_key_set_in_one_clause():
    from provisa.events.source_loader import _pk_in_clauses_within

    assert _pk_in_clauses_within(["id"], [(1,), (2,)], 200_000) == ['"id" IN (1, 2)']


@pytest.mark.asyncio
async def test_clickhouse_keyed_loader_stays_under_max_query_size(monkeypatch):
    """Confirmed live: one IN list over large_federated_join's 1..1M keys exceeded ClickHouse's
    262144-byte max_query_size. The keyed loader batches so no statement exceeds it, and returns
    every batch's rows."""
    from provisa.core.models import Column, Table
    from provisa.events.source_loader import make_clickhouse_keyed_loader

    statements: list[str] = []

    class _FakeDriver:
        def configure(self, hints):
            pass

        async def connect(self, *a):
            pass

        async def close(self):
            pass

        async def execute_arrow(self, sql):
            import pyarrow as pa

            assert len(sql) <= 262_144, "exceeds ClickHouse max_query_size"
            statements.append(sql)
            import re

            where = sql.split(" WHERE ", 1)[1]
            ids = [
                i
                for lo, hi in re.findall(r"BETWEEN (\d+) AND (\d+)", where)
                for i in range(int(lo), int(hi) + 1)
            ]
            if " IN (" in where:
                ids += [int(v) for v in where.split(" IN (", 1)[1].rstrip(")").split(", ")]
            return pa.table({"order_id": pa.array(ids, pa.int64())})

    monkeypatch.setattr("provisa.executor.drivers.clickhouse.ClickHouseDriver", _FakeDriver)
    table = Table(
        source_id="ch",
        domain_id="d",
        schema_name="default",
        table_name="order_events",
        columns=[Column(name="order_id", visible_to=["org_admin"], data_type="integer")],
    )
    source = types.SimpleNamespace(
        id="ch", host="h", port=8123, database=None, username="u", password="p"
    )
    evens = [(i,) for i in range(2, 400_001, 2)]  # IN batches
    block = [(i,) for i in range(1_000_000, 2_000_000)]  # one BETWEEN
    rows = await make_clickhouse_keyed_loader()(source, table, ["order_id"], evens + block)
    assert len(statements) > 2
    assert sorted(r["order_id"] for r in rows) == sorted(k[0] for k in evens + block)


@pytest.mark.asyncio
async def test_clickhouse_keyed_arrow_loader_returns_one_columnar_table(monkeypatch):
    """REQ-1865: the keyed fetch stays Arrow end to end -- every batch's table, concatenated."""
    import pyarrow as pa

    from provisa.core.models import Column, Table
    from provisa.events.source_loader import make_clickhouse_keyed_arrow_loader

    class _FakeDriver:
        def configure(self, hints):
            pass

        async def connect(self, *a):
            pass

        async def close(self):
            pass

        async def execute_arrow(self, sql):
            assert '"order_id", "event_type"' in sql
            n = 3 if "BETWEEN" in sql else 1
            return pa.table({"order_id": list(range(n)), "event_type": ["x"] * n})

    monkeypatch.setattr("provisa.executor.drivers.clickhouse.ClickHouseDriver", _FakeDriver)
    table = Table(
        source_id="ch",
        domain_id="d",
        schema_name="default",
        table_name="order_events",
        columns=[
            Column(name="order_id", visible_to=["org_admin"], data_type="integer"),
            Column(name="event_type", visible_to=["org_admin"], data_type="varchar"),
        ],
    )
    source = types.SimpleNamespace(
        id="ch", host="h", port=8123, database=None, username="u", password="p"
    )
    got = await make_clickhouse_keyed_arrow_loader()(
        source, table, ["order_id"], [(1,), (2,), (3,), (9,)]
    )
    assert isinstance(got, pa.Table)
    assert got.num_rows == 4 and got.column_names == ["order_id", "event_type"]


@pytest.mark.asyncio
async def test_load_keys_arrow_converts_a_row_loader_without_an_arrow_fetch():
    import pyarrow as pa

    async def _rows(source, table, pk, keys):
        return [{"id": k[0], "v": "a"} for k in keys]

    loader = SourceRowLoader(engine=None, keyed_adapter_loaders={"neo4j": _rows})
    got = await loader.load_keys_arrow(
        types.SimpleNamespace(type="neo4j", id="n"), None, ["id"], [(1,), (2,)]
    )
    assert got.equals(pa.table({"id": [1, 2], "v": ["a", "a"]}))


def test_clickhouse_arrow_datetime_and_date_are_retyped_to_arrow_temporals():
    """ClickHouse's Arrow output sends DateTime as uint32 seconds and Date as uint16 days
    (confirmed live: landing order_events.event_ts failed "UINTEGER -> TIMESTAMP")."""
    from datetime import date, datetime

    import pyarrow as pa

    from provisa.events.source_loader import _clickhouse_arrow_temporals

    data = pa.table(
        {
            "ts": pa.array([0, 86_400], pa.uint32()),
            "d": pa.array([1, 2], pa.uint16()),
            "n": pa.array([5, 6], pa.uint32()),
        }
    )
    cols = [
        types.SimpleNamespace(name="ts", data_type="timestamp"),
        types.SimpleNamespace(name="d", data_type="date"),
        types.SimpleNamespace(name="n", data_type="integer"),
    ]
    got = _clickhouse_arrow_temporals(data, cols)
    assert got.schema.types == [pa.timestamp("s"), pa.date32(), pa.uint32()]
    assert got.to_pylist()[1] == {
        "ts": datetime(1970, 1, 2),
        "d": date(1970, 1, 3),
        "n": 6,
    }
