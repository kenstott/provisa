# Copyright (c) 2026 Kenneth Stott
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit: a GraphQL where's text value compared with a typed column compares as that column's type.

A uuid, a timestamp with time zone written with a short offset (``+00``), a date: each arrives as
text, and the compiler binds it and casts it to the column's registered type in the semantic
statement (``sql_where.text_cast``). The native stage spells the cast for its engine, so the
comparison holds on DuckDB and reaches Trino — which compares no varchar with a uuid, timestamp or
date — as a typed positional bind. Every GraphQL-shaped surface (HTTP GraphQL, REST, JSON:API,
gRPC) compiles its where here."""

# Requirements: REQ-1860, REQ-1877

from __future__ import annotations

import duckdb
import sqlglot

from provisa.compiler.params import bind_positionally
from provisa.compiler.parser import parse_query
from provisa.compiler.rls import RLSContext
from provisa.compiler.sql_gen import compile_query
from provisa.compiler.sql_rewrite import lower_to_catalog_physical
from provisa.compiler.stage2 import apply_governance, build_governance_context
from provisa.transpiler.transpile import transpile
from tests.grpc_model import grpc_model

_COLUMNS = [
    ("order_id", "integer"),
    ("seen_at", "timestamp with time zone"),
    ("placed_at", "timestamp(6)"),
    ("ship_date", "date"),
    ("ref", "uuid"),
    ("region", "varchar"),
]
_REF_1 = "6f1c2a34-0000-4000-8000-000000000001"
_REF_2 = "6f1c2a34-0000-4000-8000-000000000002"


def _compiled(where: str, variables: dict | None = None):
    schema, ctx = model = grpc_model(_COLUMNS)
    declared = "query($w: OrdersWhere) " if variables is not None else ""
    text = f"{declared}{{ orders(where: {where}) {{ orderId }} }}"
    (compiled,) = compile_query(parse_query(schema, text, variables, ctx=ctx), ctx, variables)
    return model, compiled


def _where(sql: str) -> str:
    return sql.split(" WHERE ", 1)[1]


def _rows(model, compiled) -> list[tuple]:
    """The compiled statement governed, lowered and run on DuckDB with its bound values."""
    _schema, ctx = model
    tables = [{"id": 1, "domain_id": "sales", "columns": []}]
    role = {"id": "admin", "domain_access": ["*"], "capabilities": []}
    gov = build_governance_context("admin", RLSContext(rules={}), {}, ctx, tables, role, [])
    lowered = lower_to_catalog_physical(apply_governance(compiled.sql, gov), ctx)
    con = duckdb.connect()
    try:
        con.execute("ATTACH ':memory:' AS pg1")
        con.execute("CREATE SCHEMA pg1.public")
        con.execute(
            "CREATE TABLE pg1.public.orders (order_id INTEGER, seen_at TIMESTAMPTZ, "
            "placed_at TIMESTAMP, ship_date DATE, ref UUID, region VARCHAR)"
        )
        con.execute(
            "INSERT INTO pg1.public.orders VALUES "
            f"(1, '2026-01-01 08:30:00+00', '2026-01-01 08:30:00', '2026-01-05', '{_REF_1}', 'e'),"
            f"(2, '2026-01-01 09:30:00+00', '2026-01-01 09:30:00', '2026-01-06', '{_REF_2}', 'w')"
        )
        statement = sqlglot.transpile(lowered, read="postgres", write="duckdb")[0]
        return con.execute(statement, list(compiled.params)).fetchall()
    finally:
        con.close()


def test_a_uuid_value_is_bound_and_cast_and_finds_its_row_on_duckdb():
    model, compiled = _compiled(f'{{ ref: {{ eq: "{_REF_2}" }} }}')
    assert _where(compiled.sql) == '"ref" = CAST($1 AS UUID)'
    assert compiled.params == [_REF_2]
    assert _rows(model, compiled) == [(2,)]


def test_a_short_offset_timestamptz_value_is_bound_and_cast_and_finds_its_row_on_duckdb():
    model, compiled = _compiled('{ seenAt: { eq: "2026-01-01 08:30:00+00" } }')
    assert _where(compiled.sql) == '"seen_at" = CAST($1 AS TIMESTAMPTZ)'
    assert compiled.params == ["2026-01-01 08:30:00+00"]
    assert _rows(model, compiled) == [(1,)]


def test_an_iso_timestamp_is_bound_with_its_separator_normalized():
    """A precision in the registered type (``timestamp(6)``) is the same type; the ISO 'T'
    separator is not accepted by every engine's text-to-timestamp cast."""
    model, compiled = _compiled('{ placedAt: { gte: "2026-01-01T09:00:00" } }')
    assert _where(compiled.sql) == '"placed_at" >= CAST($1 AS TIMESTAMP)'
    assert compiled.params == ["2026-01-01 09:00:00"]
    assert _rows(model, compiled) == [(2,)]


def test_a_date_and_each_value_of_an_in_list_are_cast():
    model, compiled = _compiled(
        f'{{ shipDate: {{ eq: "2026-01-05" }}, ref: {{ in: ["{_REF_1}", "{_REF_2}"] }} }}'
    )
    assert _where(compiled.sql) == (
        '"ship_date" = CAST($1 AS DATE) AND "ref" IN (CAST($2 AS UUID), CAST($3 AS UUID))'
    )
    assert _rows(model, compiled) == [(1,)]


def test_a_value_given_as_a_variable_is_cast_alike():
    _model, literal = _compiled(f'{{ ref: {{ eq: "{_REF_1}" }} }}')
    _model, variable = _compiled("$w", {"w": {"ref": {"eq": _REF_1}}})
    assert (variable.sql, variable.params) == (literal.sql, literal.params)


def test_an_untyped_column_value_is_bound_as_it_is():
    _model, compiled = _compiled('{ region: { eq: "e" } }')
    assert _where(compiled.sql) == '"region" = $1'


def test_typed_values_reach_trino_as_typed_positional_binds():
    """Trino compares no varchar with a uuid, timestamp with time zone or date: the cast is in
    the statement Trino receives, spelled for Trino, and the values bind positionally in order."""
    _model, compiled = _compiled(
        f'{{ ref: {{ eq: "{_REF_1}" }}, seenAt: {{ eq: "2026-01-01 08:30:00+00" }}, '
        'shipDate: { eq: "2026-01-05" } }'
    )
    statement, bound = bind_positionally(
        transpile(f"SELECT order_id FROM orders WHERE {_where(compiled.sql)}", "trino"),
        compiled.params,
        "?",
    )
    assert statement == (
        'SELECT order_id FROM orders WHERE "ref" = CAST(? AS UUID) '
        'AND "seen_at" = CAST(? AS TIMESTAMP WITH TIME ZONE) AND "ship_date" = CAST(? AS DATE)'
    )
    assert bound == [_REF_1, "2026-01-01 08:30:00+00", "2026-01-05"]
