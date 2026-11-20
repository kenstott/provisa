# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A statement's lowering reads its context's lookups, derived once per context.

Every pgwire statement is lowered (sql_rewrite.lower_to_catalog_physical) against its role's
CompilationContext. The lookups it resolves names by -- table names, replacements, published
columns -- were derived from every table of the context on every statement: on the conformance
model, most of the server time of ``select 1`` (~9 ms of it on a laptop), and psycopg's pool
tests (test_queue[NullConnectionPool]) missed their 20% timing window on a slow runner.

The latency test compares two timings taken on the same machine in the same run: lowering one
simple statement against a context of one table and against a context of thousands. Derived
per statement, the second is hundreds of times the first; derived once, they are the same work.
"""

from __future__ import annotations

import statistics
import time

import pytest

from provisa.compiler import naming
from provisa.compiler.sql_rewrite import (
    lower_to_catalog_physical,
    normalize_table_refs,
    published_columns,
)
from provisa.compiler.sql_types import CompilationContext, TableMeta

_BIG = 3000
# The large context may cost at most this many times the one-table context per statement. The
# lookups' stamp is a tuple of the context's tables (tens of microseconds at 3000), well inside it;
# a per-statement derivation of the lookups is two orders of magnitude past it.
_RATIO_BUDGET = 2.0


def _meta(i: int) -> TableMeta:
    return TableMeta(
        table_id=i,
        field_name=f"sales__orders_{i}",
        type_name=f"SalesOrders{i}",
        source_id="sales-pg",
        catalog_name="sales_pg",
        schema_name="public",
        table_name=f"orders_{i}",
        domain_id="sales",
        source_type="postgresql",
    )


def _context(n: int) -> CompilationContext:
    ctx = CompilationContext()
    for i in range(1, n + 1):
        meta = _meta(i)
        ctx.tables[meta.field_name] = meta
        ctx.physical_to_sql[(i, "OrderId")] = "order_id"
    return ctx


def _median_seconds(ctx: CompilationContext, sql: str, rounds: int) -> float:
    lower_to_catalog_physical(sql, ctx)  # the first statement derives the lookups
    samples = []
    for _ in range(rounds):
        start = time.perf_counter()
        lower_to_catalog_physical(sql, ctx)
        samples.append(time.perf_counter() - start)
    return statistics.median(samples)


def test_simple_statement_lowering_does_not_grow_with_the_model():
    small, big = _context(1), _context(_BIG)
    sql = "SELECT 1"
    # Interleaved, so a burst of load on the machine lands on both sides alike.
    small_t, big_t = [], []
    for _ in range(5):
        small_t.append(_median_seconds(small, sql, 40))
        big_t.append(_median_seconds(big, sql, 40))
    ratio = statistics.median(big_t) / statistics.median(small_t)
    assert ratio <= _RATIO_BUDGET, (
        f"lowering SELECT 1 against {_BIG} tables took {ratio:.1f}x the time against one "
        f"(budget {_RATIO_BUDGET}x): the context's lookups are being derived per statement"
    )


def test_a_memoized_lowering_answers_as_a_fresh_one():
    ctx = _context(50)
    sql = 'SELECT o.order_id FROM orders_7 o JOIN "sales"."orders_9" p ON true'
    first = lower_to_catalog_physical(sql, ctx)
    again = lower_to_catalog_physical(sql, ctx)
    fresh = lower_to_catalog_physical(sql, _context(50))
    assert first == again == fresh
    assert '"sales_pg"."public"."orders_9"' in first
    assert '"o"."OrderId" AS "order_id"' in first


def test_a_table_added_to_the_context_is_resolved_on_the_next_statement():
    ctx = _context(3)
    sql = "SELECT * FROM orders_4"
    # Unknown: left as written.
    assert normalize_table_refs(sql, ctx) == "SELECT * FROM orders_4"
    meta = _meta(4)
    ctx.tables[meta.field_name] = meta
    assert '"public"."orders_4"' in normalize_table_refs(sql, ctx)


def test_a_column_rename_added_to_the_context_is_published_on_the_next_read():
    ctx = _context(2)
    assert published_columns(ctx) == {1: {"order_id": "OrderId"}, 2: {"order_id": "OrderId"}}
    ctx.physical_to_sql[(2, "Placed")] = "placed"
    assert published_columns(ctx)[2] == {"order_id": "OrderId", "placed": "Placed"}


@pytest.fixture
def camel_sql_convention():
    naming.configure(sql="apollo_graphql")
    yield
    naming.configure()


@pytest.mark.usefixtures("camel_sql_convention")
def test_the_sql_convention_in_force_names_the_tables():
    """An org's own convention names its tables otherwise (REQ-1327): the lookups are kept per
    convention, never one convention's served to another."""
    ctx = _context(1)
    meta = TableMeta(
        table_id=99,
        field_name="sales__order_lines",
        type_name="SalesOrderLines",
        source_id="sales-pg",
        catalog_name="sales_pg",
        schema_name="public",
        table_name="order_lines",
        domain_id="sales",
        source_type="postgresql",
    )
    ctx.tables[meta.field_name] = meta
    camel = normalize_table_refs('SELECT * FROM "sales"."orderLines"', ctx)
    naming.configure(sql="snake")
    snake = normalize_table_refs('SELECT * FROM "sales"."order_lines"', ctx)
    assert '"public"."order_lines"' in camel
    assert '"public"."order_lines"' in snake
