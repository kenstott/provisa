# Copyright (c) 2026 Kenneth Stott
# Canary: 3f6a1c2e-7d84-4b19-a0e5-92c6d1b47e38
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration: a nested GraphQL read over a ClickHouse table on the pg engine, read once.

The bench's federated_join took 10 s over 1000 root rows: the field compiled to a correlated
subquery and PostgreSQL ran its foreign scan once per row. Through the real compile, governance
and transpile, on an embedded Postgres attached to a real ClickHouse server, the two statements
answer the same and ClickHouse's own query log counts the reads.
"""

# Requirements: REQ-1880, REQ-1911

from __future__ import annotations

import json
import os
import time
from types import SimpleNamespace

import psycopg2
import pytest

pytestmark = [pytest.mark.integration, pytest.mark.requires_clickhouse]

clickhouse_connect = pytest.importorskip("clickhouse_connect")

from provisa.compiler.context import build_context  # noqa: E402
from provisa.compiler.introspect import ColumnMetadata  # noqa: E402
from provisa.compiler.naming import live_view_schema  # noqa: E402
from provisa.compiler.parser import parse_query  # noqa: E402
from provisa.compiler.rls import RLSContext  # noqa: E402
from provisa.compiler.schema_gen import SchemaInput, generate_schema  # noqa: E402
from provisa.compiler.sql_gen import compile_query  # noqa: E402
from provisa.compiler.sql_rewrite import (  # noqa: E402
    propagate_literal_join_predicates,
    rewrite_semantic_to_physical,
)
from provisa.compiler.stage2 import apply_governance, build_governance_context  # noqa: E402
from provisa.federation.pg_runtime import PgFederationRuntime  # noqa: E402
from provisa.transpiler.transpile import transpile  # noqa: E402
from tests.embedded_pg import start_local_postgres  # noqa: E402
from tests.helpers import (  # noqa: E402
    ALL_DATA_CAPABILITIES,
    registry_write_ops,
    registry_write_returns_rows,
)

_EVENTS = "itest_pg_nested_events"
_STATUS = "itest_pg_nested_status"
_LOCAL = "itest_nested_local"
_ORDERS = 1000  # the bound the query reads
_ALL_ORDERS = 5000  # ClickHouse's rows beyond it must never leave ClickHouse

_ADMIN = {
    "description": "Test role.",
    "id": "admin",
    "capabilities": ALL_DATA_CAPABILITIES,
    "domain_access": ["*"],
}

_QUERY = (
    """
{
  orders(where: {order_id: {gte: 1, lte: %d}}) {
    order_id
    events { event_type }
    status { state }
  }
}
"""
    % _ORDERS
)


def _client():
    return clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST", "localhost"),
        port=int(os.environ["CLICKHOUSE_PORT"]),
        username=os.environ.get("CLICKHOUSE_USER", "default"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
    )


def _source(table: str) -> SimpleNamespace:
    return SimpleNamespace(
        id="ch-nested-src",
        catalog="ch_nested_src",
        type=SimpleNamespace(value="clickhouse"),
        host=os.environ.get("CLICKHOUSE_HOST", "localhost"),
        port=int(os.environ["CLICKHOUSE_PORT"]),
        username=os.environ.get("CLICKHOUSE_USER", "default"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
        database="default",
        federation_hints={},
        schema_name="default",
        table_name=table,
    )


@pytest.fixture
def clickhouse():
    client = _client()
    for table in (_EVENTS, _STATUS):
        client.command(f"DROP TABLE IF EXISTS {table}")
    client.command(
        f"CREATE TABLE {_EVENTS} (order_id Int32, event_type String) "
        "ENGINE = MergeTree ORDER BY order_id"
    )
    client.command(
        f"CREATE TABLE {_STATUS} (order_id Int32, state String) "
        "ENGINE = MergeTree ORDER BY order_id"
    )
    # Every order has two equal events and one its row rule hides; the last order has none.
    events = [
        [order, kind]
        for order in range(1, _ALL_ORDERS + 1)
        if order != _ORDERS
        for kind in ("placed", "placed", "hidden")
    ]
    client.insert(_EVENTS, events, column_names=["order_id", "event_type"])
    client.insert(
        _STATUS,
        [[order, "open"] for order in range(1, _ALL_ORDERS + 1)],
        column_names=["order_id", "state"],
    )
    yield client
    for table in (_EVENTS, _STATUS):
        client.command(f"DROP TABLE IF EXISTS {table}")
    client.close()


@pytest.fixture
def engine(clickhouse):
    pg = start_local_postgres(fdws=True)
    conn = psycopg2.connect(pg.dsn())
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA {_LOCAL}")
        cur.execute(f"CREATE TABLE {_LOCAL}.orders (order_id int PRIMARY KEY)")
        cur.execute(f"INSERT INTO {_LOCAL}.orders SELECT generate_series(1, %s)", (_ALL_ORDERS,))
        cur.execute(f"ANALYZE {_LOCAL}.orders")
    conn.close()
    rt = PgFederationRuntime(engine_dsn=pg.dsn())
    rt.attach_source(_source(_EVENTS))
    rt.attach_source(_source(_STATUS))
    yield rt
    rt.close()
    pg.stop()


def _table(table_id: int, schema: str, name: str, columns: tuple[str, ...]) -> dict:
    return {
        "id": table_id,
        "source_id": f"s{table_id}",
        "domain_id": "sales",
        "schema_name": schema,
        "table_name": name,
        "write_ops": registry_write_ops("postgresql"),
        "write_returns_rows": registry_write_returns_rows("postgresql"),
        "write_refused_forms": [],
        "columns": [{"column_name": c, "visible_to": ["admin"]} for c in columns],
    }


def _relationship(rel: str, target: int, cardinality: str) -> dict:
    return {
        "id": rel,
        "source_table_id": 1,
        "target_table_id": target,
        "source_column": "order_id",
        "target_column": "order_id",
        "cardinality": cardinality,
    }


def _statement(attached: str) -> tuple[str, list]:
    """The query's governed physical SQL, the row rule on the events it reads, and its params."""

    def col(name: str, data_type: str = "varchar") -> ColumnMetadata:
        return ColumnMetadata(column_name=name, data_type=data_type, is_nullable=True)

    si = SchemaInput(
        tables=[
            _table(1, _LOCAL, "orders", ("order_id",)),
            _table(2, attached, _EVENTS, ("order_id", "event_type")),
            _table(3, attached, _STATUS, ("order_id", "state")),
        ],
        relationships=[
            _relationship("events", 2, "one-to-many"),
            _relationship("status", 3, "one-to-one"),
        ],
        column_types={
            1: [col("order_id", "integer")],
            2: [col("order_id", "integer"), col("event_type")],
            3: [col("order_id", "integer"), col("state")],
        },
        naming_rules=[],
        role=_ADMIN,
    )
    schema = generate_schema(si)
    ctx = build_context(si)
    compiled = compile_query(parse_query(schema, _QUERY, {}, ctx=ctx), ctx)[0]
    rls = RLSContext(rules={2: "event_type <> 'hidden'"}, domain_rules={})
    gov = build_governance_context("admin", rls, {}, ctx, si.tables, role=_ADMIN)
    governed = apply_governance(compiled.sql, gov)
    return rewrite_semantic_to_physical(governed, ctx), list(compiled.params)


def _run(
    runtime: PgFederationRuntime,
    client,
    attached: str,
    sql: str,
    params: list,
    read_once: bool,
) -> tuple[list, str, float, int]:
    """The answer, the statement that gave it, its milliseconds, and ClickHouse's reads for it."""
    column_types = {
        (_LOCAL, "orders", "order_id"): "integer",
        (attached, _EVENTS, "order_id"): "integer",
        (attached, _STATUS, "order_id"): "integer",
    }
    eligible = {(attached, _EVENTS), (attached, _STATUS)}
    bound = propagate_literal_join_predicates(
        sql, "postgres", eligible, column_types, allow_params=True, read_once=read_once
    )
    physical = transpile(bound, "postgres")
    mark = client.command("SELECT now64(6)")
    started = time.perf_counter()
    rows = runtime.run_sync(physical, params).rows
    elapsed = (time.perf_counter() - started) * 1000
    client.command("SYSTEM FLUSH LOGS")
    reads = int(
        client.command(
            "SELECT count() FROM system.query_log WHERE type = 'QueryFinish' "
            f"AND event_time_microseconds >= toDateTime64('{mark}', 6) "
            f"AND query LIKE '%{_EVENTS}%' AND query NOT LIKE '%system.query_log%'"
        )
    )
    return rows, physical, elapsed, reads


def _answer(rows: list) -> dict:
    def parsed(value):
        return json.loads(value) if isinstance(value, str) else value

    return {row[0]: (parsed(row[1]), parsed(row[2])) for row in rows}


def test_a_nested_read_reaches_clickhouse_once_and_answers_the_same(engine, clickhouse, capsys):
    attached = live_view_schema("ch_nested_src", "default").lower()
    sql, params = _statement(attached)

    before, before_sql, before_ms, before_reads = _run(
        engine, clickhouse, attached, sql, params, read_once=False
    )
    after, after_sql, after_ms, after_reads = _run(
        engine, clickhouse, attached, sql, params, read_once=True
    )

    with capsys.disabled():
        print(f"\nbefore: {before_ms:.0f} ms, {before_reads} ClickHouse reads: {before_sql}")
        print(f"after: {after_ms:.0f} ms, {after_reads} ClickHouse reads: {after_sql}")

    assert before_reads >= _ORDERS
    assert after_reads == 1
    assert len(after) == _ORDERS
    assert _answer(after) == _answer(before)
    answer = _answer(after)
    # Equal elements of a list stay, the row rule still holds, and no match is still [].
    assert answer[1] == ([{"event_type": "placed"}, {"event_type": "placed"}], {"state": "open"})
    assert answer[_ORDERS] == ([], {"state": "open"})
