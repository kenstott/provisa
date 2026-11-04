# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A name a request gives the compiler is one identifier of the SQL it becomes.

A GraphQL variable's value is not coerced to its declared type before compilation, so the keys of a
``where``, ``order_by``, ``having`` or mutation ``input``/``set`` and the items of ``distinct_on``,
``by`` or ``on_conflict`` arrive as any text the request holds. Each such name is quoted as one
identifier: a quote, a comment marker or a semicolon in it never ends the identifier and adds a
clause, a predicate or a statement. The engine then refuses the name as an unknown column."""

from __future__ import annotations

from dataclasses import replace

import pytest
import sqlglot
import sqlglot.expressions as exp
from graphql import parse

from provisa.compiler.mutation_gen import compile_mutation
from provisa.compiler.sql_gen import compile_query
from tests.unit.test_sql_gen import _build_schema_and_ctx

HOSTILE = [
    'id" FROM x; --',
    'a"" OR 1=1 --',
    "a'b; DROP TABLE t; --",
    'c/* x */"d',
    "q”＂ r",
]


@pytest.fixture(scope="module")
def ctx():
    return _build_schema_and_ctx()[1]


def _held_as_one_identifier(sql: str, name: str) -> None:
    statements = sqlglot.parse(sql, read="postgres")
    assert len(statements) == 1, sql
    (statement,) = statements
    assert statement is not None, sql
    assert name in {i.this for i in statement.find_all(exp.Identifier)}, sql


READS = {
    "where": ("query($v: OrdersWhere) { orders(where: $v) { id } }", lambda h: {h: {"eq": 1}}),
    "order_by": (
        "query($v: [OrdersOrderBy!]) { orders(order_by: $v) { id } }",
        lambda h: [{h: "asc"}],
    ),
    "nested order_by": (
        "query($v: [OrdersOrderBy!]) { orders(order_by: $v) { id } }",
        lambda h: [{"customer": {h: "asc"}}],
    ),
    "distinct_on": (
        "query($v: [OrdersDistinctOnColumn!]) { orders(distinct_on: $v) { id } }",
        lambda h: [h],
    ),
    "lateral order_by": (
        "query($v: [CustomersOrderBy!]) { orders { id customer(order_by: $v, limit: 1) { name } } }",
        lambda h: [{h: "asc"}],
    ),
    "connection order_by": (
        'query($v: [X!]) { orders_connection(first: 2, after: "WzFd", order_by: $v) '
        "{ edges { node { id } } } }",
        lambda h: [{h: "asc"}],
    ),
    "aggregate where": (
        "query($v: X) { orders_aggregate(where: $v) { aggregate { count } } }",
        lambda h: {h: {"eq": 1}},
    ),
    "group_by by": ("query($v: [X!]) { orders_group_by(by: $v) { count } }", lambda h: [h]),
    "group_by having": (
        "query($v: X) { orders_group_by(by: [region], having: $v) { count } }",
        lambda h: {"sum": {h: {"gt": 1}}},
    ),
}


@pytest.mark.parametrize("hostile", HOSTILE)
@pytest.mark.parametrize("form", list(READS))
def test_a_read_names_a_requested_column_as_one_identifier(ctx, form, hostile):
    query, value = READS[form]
    (compiled,) = compile_query(parse(query), ctx, {"v": value(hostile)})
    _held_as_one_identifier(compiled.sql, hostile)


def test_a_distinct_on_wrapped_for_an_engine_names_the_column_as_one_identifier():
    """A source with no DISTINCT ON partitions a window by the columns instead."""
    _, ctx = _build_schema_and_ctx()
    ctx.tables["orders"] = replace(ctx.tables["orders"], source_type="iceberg")
    hostile = HOSTILE[0]
    (compiled,) = compile_query(
        parse("query($v: [OrdersDistinctOnColumn!]) { orders(distinct_on: $v) { id } }"),
        ctx,
        {"v": [hostile]},
    )
    _held_as_one_identifier(compiled.sql, hostile)


WRITES = {
    "insert input": (
        "mutation($v: X) { insert_orders(input: $v) { affected_rows } }",
        lambda h: {h: 1},
    ),
    "update set": (
        "mutation($v: X) { update_orders(set: $v, where: {id: {eq: 1}}) { affected_rows } }",
        lambda h: {h: 1},
    ),
    "upsert on_conflict": (
        "mutation($v: X) { upsert_orders(input: {id: 1}, on_conflict: $v) { affected_rows } }",
        lambda h: [h],
    ),
    "delete where": (
        "mutation($v: X) { delete_orders(where: $v) { affected_rows } }",
        lambda h: {h: {"eq": 1}},
    ),
}


@pytest.mark.parametrize("hostile", HOSTILE)
@pytest.mark.parametrize("form", list(WRITES))
def test_a_mutation_names_a_requested_column_as_one_identifier(ctx, form, hostile):
    query, value = WRITES[form]
    (compiled,) = compile_mutation(
        parse(query), ctx, {"sales-pg": "postgresql"}, {"v": value(hostile)}
    )
    _held_as_one_identifier(compiled.sql, hostile)


def test_a_name_without_a_quote_is_quoted_as_before(ctx):
    """The quoting of an ordinary name is unchanged: the SQL goldens hold."""
    (compiled,) = compile_query(
        parse('{ orders(where: {region: {eq: "w"}}, order_by: [{id: asc}]) { id region } }'),
        ctx,
    )
    assert compiled.sql == (
        'SELECT "id", "region" FROM "public"."orders" WHERE "region" = $1 ORDER BY "id" ASC'
    )
