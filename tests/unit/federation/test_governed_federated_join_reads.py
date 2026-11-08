# Copyright (c) 2026 Kenneth Stott
# Canary: 5b7e2c90-4d1a-4f83-9e6b-a2c8d0f3e517
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The perf bench's federated_join, governed by stage 2, reads its live sources as narrowly as the
same statement over bare tables (REQ-039, REQ-1880, REQ-899).

Stage 2 puts the role's governed relation in the place of every registered table, so the
statement's conditions sit around derived tables. Two engine-side passes read only a bare table:
the literal propagation onto the far tables' join columns, and the DuckDB ClickHouse scan's
pushed predicates and projection. The governed statement then asked ClickHouse for its whole
order_events table, every served column, and the Mongo scan saw no condition: federated_join went
from 95 to 459 ms on DuckDB (J670 mini bench, every transport alike)."""

# Requirements: REQ-039, REQ-1880, REQ-899

from __future__ import annotations

import re
from urllib.parse import parse_qs, urlparse

import pytest
import sqlglot

from provisa.compiler.compiled_query_cache import CompiledQueryCache
from provisa.compiler.rls import RLSContext
from provisa.compiler.sql_gen import CompilationContext, TableMeta
from provisa.compiler.sql_rewrite import propagate_literal_join_predicates
from provisa.federation.clickhouse_http_scan import ClickHouseRelation, rewrite
from provisa.federation.replica_address import ReplicaRoutes
from tests.role_state import RoleState

pytestmark = pytest.mark.asyncio

_SQL = (
    "SELECT o.order_id, o.amount, e.event_type, d.status FROM perf_bench.orders o "
    "JOIN perf_bench.order_events e ON e.order_id = o.order_id "
    "JOIN perf_bench.order_docs d ON d.order_id = o.order_id "
    "WHERE o.order_id BETWEEN 1 AND 1000"
)
_TABLES = {
    "orders": (1, "pg", ["order_id", "customer_id", "status", "amount"]),
    "order_events": (2, "ch", ["event_id", "order_id", "customer_id", "event_type", "payload"]),
    "order_docs": (3, "mongo", ["order_id", "status", "channel"]),
}
_ELIGIBLE = {("perf_bench", name) for name in _TABLES}
_TYPES = {("perf_bench", name, "order_id"): "integer" for name in _TABLES}
_CH = ClickHouseRelation(
    base_url="http://ch.example:8123",
    database="default",
    table="order_events",
    columns=(
        ("event_id", "UInt64"),
        ("order_id", "UInt64"),
        ("customer_id", "UInt32"),
        ("event_type", "String"),
        ("payload", "String"),
    ),
)


def _state() -> RoleState:
    ctx = CompilationContext()
    tables = []
    for name, (tid, source, columns) in _TABLES.items():
        ctx.tables[name] = TableMeta(
            table_id=tid,
            field_name=name,
            type_name=name.title(),
            source_id=source,
            catalog_name=source,
            schema_name="perf_bench",
            table_name=name,
            domain_id="perf-bench",
        )
        tables.append(
            {
                "id": tid,
                "source_id": source,
                "schema_name": "perf_bench",
                "table_name": name,
                "domain_id": "perf-bench",
                "columns": [
                    {"column_name": c, "data_type": "integer", "visible_to": ["analyst"]}
                    for c in columns
                ],
            }
        )
    return RoleState(
        admin_db=None,
        contexts={"analyst": ctx},
        rls_contexts={"analyst": RLSContext.empty()},
        roles={
            "analyst": {
                "id": "analyst",
                "capabilities": ["ignore_relationships"],
                "domain_access": ["*"],
            }
        },
        masking_rules={},
        tables=tables,
        source_types={"pg": "postgresql", "ch": "clickhouse", "mongo": "mongodb"},
        source_catalogs={},
        relationships=[],
        metrics={},
        federation_engine=None,
        view_sql_map=None,
        security_high=False,
        schema_boot_id="test-boot",
        schema_version=1,
        compiled_query_cache=CompiledQueryCache(),
        routing_cache=CompiledQueryCache(),
        replica_routes=ReplicaRoutes(),
        model_stamp=1,
    )


@pytest.fixture
async def governed(monkeypatch) -> str:
    import provisa.api.app as app_mod
    from provisa.pgwire import _pipeline, governed_plan

    monkeypatch.setattr(governed_plan, "_rebuild_in_progress", lambda: False)
    monkeypatch.setattr(app_mod, "state", _state(), raising=False)
    statement = await _pipeline.govern_statement(_SQL, "analyst")
    return statement.governed_semantic


async def test_the_governed_statement_reads_each_table_through_its_relation(governed):
    tree = sqlglot.parse_one(governed, read="postgres")
    assert [type(j.this).__name__ for j in tree.find_all(sqlglot.exp.Join)] == [
        "Subquery",
        "Subquery",
    ]


async def test_the_far_tables_receive_the_order_range(governed):
    out = propagate_literal_join_predicates(governed, "postgres", _ELIGIBLE, _TYPES)
    where = sqlglot.parse_one(out, read="postgres").args["where"].this.sql(dialect="postgres")
    assert "e.order_id BETWEEN 1 AND 1000" in where
    assert "d.order_id BETWEEN 1 AND 1000" in where


async def test_clickhouse_is_asked_for_the_rows_and_columns_the_statement_reads(governed):
    propagated = propagate_literal_join_predicates(governed, "postgres", _ELIGIBLE, _TYPES)
    engine_sql = sqlglot.parse_one(propagated, read="postgres").sql(dialect="duckdb")
    out = rewrite(engine_sql, None, {("", "perf_bench", "order_events"): _CH}, deadline_s=None)
    assert out is not None
    [url] = re.findall(r"read_parquet\('([^']+)'\)", out, flags=re.I)
    assert parse_qs(urlparse(url).query)["query"][0] == (
        'SELECT "order_id", "event_type" FROM "default"."order_events" '
        'WHERE "order_id" BETWEEN 1 AND 1000 FORMAT Parquet'
    )
