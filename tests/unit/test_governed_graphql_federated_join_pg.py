# Copyright (c) 2026 Kenneth Stott
# Canary: 2f6c8d14-7b3e-4a59-b1e0-8d4c2a9e6f71
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The perf bench's federated_join over GraphQL, governed, on the pg engine: the order range
reaches the far tables' reads (REQ-039, REQ-1880).

The GraphQL compile reads each nested relationship in a correlated subquery. On PostgreSQL a
correlated subquery runs once per outer row, so the literal propagation copies the outer range
into it and reads a one-row relationship once, as a LEFT JOIN (``read_once``). Before the
propagation read through governed relations, the governed statement got neither: the far tables
were read per order with no range, and federated_join over GraphQL on the pg engine went from
238 to 8355 ms (J670 mini bench, both rounds). The engine statement is pinned here: main's, with
each table read through its governed relation."""

# Requirements: REQ-039, REQ-1880

from __future__ import annotations

from provisa.compiler.context import build_context
from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.parser import parse_query
from provisa.compiler.rls import RLSContext
from provisa.compiler.schema_gen import SchemaInput, generate_schema
from provisa.compiler.sql_gen import compile_query
from provisa.compiler.sql_rewrite import (
    lower_to_catalog_physical,
    propagate_literal_join_predicates,
)
from provisa.compiler.stage2 import apply_governance, build_governance_context

_ROLE = {"id": "analyst", "capabilities": ["query_development"], "domain_access": ["*"]}
_INTEGER = {"order_id", "customer_id", "amount", "event_id"}


def _table(tid: int, source: str, name: str, columns: list[str]) -> dict:
    return {
        "id": tid,
        "source_id": source,
        "domain_id": "pb",
        "schema_name": "perf_bench",
        "table_name": name,
        "columns": [{"column_name": c, "visible_to": ["analyst"]} for c in columns],
        "write_ops": [],
        "write_returns_rows": False,
        "write_refused_forms": [],
    }


_TABLES = [
    _table(1, "pg", "orders", ["order_id", "customer_id", "amount", "status"]),
    _table(2, "ch", "order_events", ["event_id", "order_id", "event_type"]),
    _table(3, "mongo", "order_docs", ["order_id", "status", "channel"]),
]
_RELATIONSHIPS = [
    {
        "id": 1,
        "source_table_id": 1,
        "target_table_id": 2,
        "source_column": "order_id",
        "target_column": "order_id",
        "cardinality": "one-to-many",
        "alias": "orderEvents",
    },
    {
        "id": 2,
        "source_table_id": 1,
        "target_table_id": 3,
        "source_column": "order_id",
        "target_column": "order_id",
        "cardinality": "one-to-one",
        "alias": "orderDoc",
    },
]
# The bench's GraphQL text (demo/named/perf/bench/queries.py, federated_join), its range bound.
_QUERY = (
    "query { orders(where: {orderId: {gte: 1, lte: 1000}}) "
    "{ orderId amount orderEvents { eventType } orderDoc { status } } }"
)


def _engine_sql() -> str:
    """The governed statement as the pg engine route propagates it
    (``_pipeline.propagate_literal_predicates``: the far tables' connectors push a literal
    predicate but no join; PostgreSQL binds parameters by number and reads a correlated subquery
    once per row)."""
    types = {
        t["id"]: [
            ColumnMetadata(
                column_name=c["column_name"],
                data_type="integer" if c["column_name"] in _INTEGER else "varchar",
                is_nullable=True,
            )
            for c in t["columns"]
        ]
        for t in _TABLES
    }
    si = SchemaInput(
        tables=_TABLES,
        relationships=_RELATIONSHIPS,
        column_types=types,
        naming_rules=[],
        role=_ROLE,
        domains=[{"id": "pb", "description": "perf bench"}],
    )
    schema, ctx = generate_schema(si), build_context(si)
    gov = build_governance_context(
        "analyst", RLSContext(rules={}), {}, ctx, _TABLES, _ROLE, _RELATIONSHIPS
    )
    (compiled,) = compile_query(parse_query(schema, _QUERY, ctx=ctx), ctx)
    governed = lower_to_catalog_physical(apply_governance(compiled.sql, gov), ctx)
    return propagate_literal_join_predicates(
        governed,
        "postgres",
        {("perf_bench", "order_events"), ("perf_bench", "order_docs")},
        {
            ("perf_bench", n, "order_id"): "integer"
            for n in ("orders", "order_events", "order_docs")
        },
        allow_params=True,
        read_once=True,
    )


def test_the_pg_engine_statement_is_mains_read_through_governed_relations():
    assert _engine_sql() == (
        'SELECT "t0"."order_id", "t0"."amount", '
        "COALESCE((SELECT JSON_AGG(_t) FROM ("
        'SELECT JSON_OBJECT(\'eventType\': "t1"."event_type") AS _t FROM ('
        'SELECT "t1"."order_id" AS "order_id", "t1"."event_type" AS "event_type" '
        'FROM "ch"."perf_bench"."order_events" AS "t1") AS "t1" '
        'WHERE ("t1"."order_id" = "t0"."order_id" AND "t1"."order_id" >= $1) '
        'AND "t1"."order_id" <= $2 LIMIT 100) AS _sub), CAST(\'[]\' AS JSON)) AS "orderEvents", '
        '"_read_0"."_value" AS "orderDoc" '
        'FROM (SELECT "t0"."order_id" AS "order_id", "t0"."amount" AS "amount" '
        'FROM "pg"."perf_bench"."orders" AS "t0") AS "t0" '
        'LEFT JOIN (SELECT DISTINCT ON ("t2"."order_id") "t2"."order_id" AS "_key", '
        'JSON_OBJECT(\'status\': "t2"."status") AS "_value" FROM ('
        'SELECT "t2"."order_id" AS "order_id", "t2"."status" AS "status" '
        'FROM "mongo"."perf_bench"."order_docs" AS "t2") AS "t2" '
        'WHERE "t2"."order_id" >= $1 AND "t2"."order_id" <= $2) AS "_read_0" '
        'ON "_read_0"."_key" = "t0"."order_id" '
        'WHERE "t0"."order_id" >= $1 AND "t0"."order_id" <= $2 LIMIT 100'
    )


def test_the_order_range_reaches_each_far_table():
    sql = _engine_sql()
    assert '"t1"."order_id" >= $1' in sql  # the events, in the correlated subquery
    assert '"t2"."order_id" >= $1' in sql  # the document, read once
    assert 'LEFT JOIN (SELECT DISTINCT ON ("t2"."order_id")' in sql
