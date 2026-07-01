# Copyright (c) 2026 Kenneth Stott
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""pytest-bdd step implementations for REQ-810: Materialized Views partial join-pattern matching."""

# Requirements: REQ-810

from __future__ import annotations

import time

import pytest
from pytest_bdd import given, scenario, then, when

from provisa.compiler.sql_gen import ColumnRef, CompiledQuery
from provisa.mv.models import JoinPattern, MVDefinition, MVStatus
from provisa.mv.rewriter import rewrite_if_mv_match


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def shared_data() -> dict:
    return {}


# ---------------------------------------------------------------------------
# Scenario
# ---------------------------------------------------------------------------


@scenario(
    "../features/REQ-810.feature",
    "REQ-810 default behaviour",
)
def test_req_810_default_behaviour():
    pass


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_fresh_mv(
    mv_id: str,
    left_table: str,
    left_column: str,
    right_table: str,
    right_column: str,
    target_table: str = "mv_orders_customers",
    target_schema: str = "mv_cache",
    target_catalog: str = "iceberg",
) -> MVDefinition:
    jp = JoinPattern(
        left_table=left_table,
        left_column=left_column,
        right_table=right_table,
        right_column=right_column,
        join_type="left",
    )
    mv = MVDefinition(
        id=mv_id,
        source_tables=[left_table, right_table],
        target_catalog=target_catalog,
        target_schema=target_schema,
        target_table=target_table,
        join_pattern=jp,
        refresh_interval=300,
    )
    mv.status = MVStatus.FRESH
    mv.last_refresh_at = time.time() - 5
    return mv


def _make_compiled(sql: str, sources: set[str] | None = None) -> CompiledQuery:
    return CompiledQuery(
        sql=sql,
        params=[],
        root_field="orders",
        columns=[
            ColumnRef(alias="t0", column="id", field_name="id", nested_in=None),
            ColumnRef(alias="t1", column="name", field_name="name", nested_in="customers"),
            ColumnRef(alias="t2", column="title", field_name="title", nested_in="products"),
        ],
        sources=sources or {"sales-pg"},
    )


# ---------------------------------------------------------------------------
# Given
# ---------------------------------------------------------------------------


@given("a materialized view covering a subset of a query's joins")
def given_mv_covering_subset_of_joins(shared_data):
    """Set up a query with two JOINs and an MV that covers only the first JOIN.

    The query joins orders → customers → products.
    The MV covers only orders ⟶ customers (the first JOIN).
    The second JOIN (orders → products) is NOT covered and must be preserved.
    """
    # Query with two JOINs
    sql = (
        'SELECT "t0"."id", "t1"."name", "t2"."title" '
        'FROM "public"."orders" "t0" '
        'LEFT JOIN "public"."customers" "t1" ON "t0"."customer_id" = "t1"."id" '
        'LEFT JOIN "public"."products" "t2" ON "t0"."product_id" = "t2"."id"'
    )
    shared_data["original_sql"] = sql
    shared_data["compiled"] = _make_compiled(sql)

    # MV covers only orders → customers (first JOIN only)
    mv = _make_fresh_mv(
        mv_id="mv-orders-customers",
        left_table="orders",
        left_column="customer_id",
        right_table="customers",
        right_column="id",
        target_table="mv_orders_customers",
        target_schema="mv_cache",
        target_catalog="iceberg",
    )
    shared_data["mv"] = mv
    shared_data["covered_join_table"] = "customers"
    shared_data["uncovered_join_table"] = "products"


# ---------------------------------------------------------------------------
# When
# ---------------------------------------------------------------------------


@when("the query is compiled")
def when_query_is_compiled(shared_data):
    """Run the MV rewriter against the compiled query and the partial-match MV."""
    compiled = shared_data["compiled"]
    mv = shared_data["mv"]

    result = rewrite_if_mv_match(compiled, [mv])
    shared_data["result"] = result


# ---------------------------------------------------------------------------
# Then
# ---------------------------------------------------------------------------


@then("the joins covered by the MV are rewritten to read the MV")
def then_covered_joins_rewritten_to_mv(shared_data):
    """Assert that the rewritten SQL references the MV target table, not the covered source table."""
    result = shared_data["result"]
    mv = shared_data["mv"]
    covered_table = shared_data["covered_join_table"]

    rewritten_sql = result.sql

    # The MV target table must appear in the rewritten SQL
    assert mv.target_table in rewritten_sql, (
        f"Expected MV target table '{mv.target_table}' in rewritten SQL, got:\n{rewritten_sql}"
    )

    # The MV target schema must appear in the rewritten SQL
    assert mv.target_schema in rewritten_sql, (
        f"Expected MV schema '{mv.target_schema}' in rewritten SQL, got:\n{rewritten_sql}"
    )

    # Verify the rewritten SQL is actually different from the original
    original_sql = shared_data["original_sql"]
    assert rewritten_sql != original_sql, (
        "Rewritten SQL should differ from original SQL when partial MV match occurs"
    )

    # The covered source table should not appear as a JOIN target any more
    # (it may appear embedded in MV table name, so check for the JOIN clause specifically)
    # The MV replaces the JOIN to 'covered_table', so there should be no
    # 'JOIN ... "customers"' remaining in the rewritten SQL
    import re

    covered_join_pattern = rf'JOIN\s+"[^"]*"\."(?:{covered_table})"\s+"t\d+"'
    assert not re.search(covered_join_pattern, rewritten_sql, re.IGNORECASE), (
        f"JOIN to covered table '{covered_table}' should be removed from rewritten SQL, "
        f"got:\n{rewritten_sql}"
    )


@then("the joins not covered by the MV are preserved and executed live")
def then_uncovered_joins_preserved(shared_data):
    """Assert that the JOIN for the uncovered table is still present in the rewritten SQL."""
    result = shared_data["result"]
    uncovered_table = shared_data["uncovered_join_table"]

    rewritten_sql = result.sql

    # The uncovered table's JOIN must still be present
    import re

    uncovered_join_pattern = rf'JOIN\s+"[^"]*"\."(?:{uncovered_table})"\s+"t\d+"'
    assert re.search(uncovered_join_pattern, rewritten_sql, re.IGNORECASE), (
        f"JOIN to uncovered table '{uncovered_table}' should be preserved in rewritten SQL, "
        f"got:\n{rewritten_sql}"
    )

    # Confirm the SQL still contains a JOIN keyword (for the preserved join)
    assert "JOIN" in rewritten_sql.upper(), (
        f"Rewritten SQL should still contain JOIN for uncovered table, got:\n{rewritten_sql}"
    )

    # The uncovered table itself must be referenced in the SQL
    assert uncovered_table in rewritten_sql, (
        f"Uncovered table '{uncovered_table}' must appear in rewritten SQL, got:\n{rewritten_sql}"
    )
