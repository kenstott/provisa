# Copyright (c) 2026 Kenneth Stott
# Canary: f27ff6ef-d1c7-4647-8e6e-8ceef795f283
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1

"""Step definitions for REQ-196 / REQ-197 / REQ-198 / REQ-199 — aggregate root fields, per-role gating, aggregate MV routing, and view auto-materialization."""

from __future__ import annotations

import time

import pytest

from graphql import GraphQLObjectType
from pytest_bdd import given, when, then, scenarios

from tests.helpers import registry_write_ops, registry_write_returns_rows
from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler import naming as _naming
from provisa.compiler.schema_gen import SchemaInput, generate_schema
from provisa.compiler.context import build_context
from provisa.mv.models import MVDefinition, MVStatus


scenarios("../features/REQ-196.feature")
scenarios("../features/REQ-197.feature")
scenarios("../features/REQ-198.feature")
scenarios("../features/REQ-199.feature")


@pytest.fixture
def shared_data() -> dict:
    return {}


def _col(name: str, data_type: str = "varchar(100)", nullable: bool = False) -> ColumnMetadata:
    return ColumnMetadata(column_name=name, data_type=data_type, is_nullable=nullable)


@given("a registered table with numeric and comparable columns")
def _registered_table(shared_data):
    _naming.configure(gql="snake")
    tables = [
        {
            "id": 1,
            "source_id": "sales-pg",
            "domain_id": "sales",
            "schema_name": "public",
            "table_name": "orders",
            "write_ops": registry_write_ops("postgresql"),
            "write_returns_rows": registry_write_returns_rows("postgresql"),
            "write_refused_forms": [],
            "enable_aggregates": True,
            "columns": [
                {"column_name": "id", "visible_to": ["admin"]},
                {"column_name": "amount", "visible_to": ["admin"]},
                {"column_name": "qty", "visible_to": ["admin"]},
                {"column_name": "region", "visible_to": ["admin"]},
                {"column_name": "created_at", "visible_to": ["admin"]},
            ],
        }
    ]
    column_types = {
        1: [
            _col("id", "integer"),
            _col("amount", "decimal(10,2)"),
            _col("qty", "integer"),
            _col("region", "varchar(20)"),
            _col("created_at", "timestamp"),
        ],
    }
    role = {"description": "Test role.", "id": "admin", "capabilities": [], "domain_access": ["*"]}
    domains = [{"id": "sales", "description": "Sales"}]
    si = SchemaInput(
        tables=tables,
        relationships=[],
        column_types=column_types,
        naming_rules=[],
        role=role,
        domains=domains,
    )
    shared_data["schema_input"] = si
    # numeric columns (sum/avg/stddev/variance eligible)
    shared_data["numeric_cols"] = {"id", "amount", "qty"}
    # comparable columns (min/max eligible) — numeric + ordered types
    shared_data["comparable_cols"] = {"id", "amount", "qty", "region", "created_at"}
    shared_data["all_cols"] = {"id", "amount", "qty", "region", "created_at"}


@when("the schema compiler runs")
def _compile_schema(shared_data):
    si = shared_data["schema_input"]
    schema = generate_schema(si)
    ctx = build_context(si)
    assert schema is not None, "schema generation returned None"
    assert ctx is not None, "build_context returned None"
    shared_data["schema"] = schema
    shared_data["ctx"] = ctx


@then(
    "a <table>_aggregate root field is generated with sum/avg/stddev/variance on numeric columns, min/max on comparable columns, and count on all columns"
)
def _verify_aggregate_field(shared_data):
    schema = shared_data["schema"]
    query_type = schema.query_type
    assert query_type is not None, "schema has no query type"

    # 1. The orders_aggregate root field must exist
    fields = query_type.fields
    assert "orders_aggregate" in fields, (
        f"orders_aggregate root field missing; got {sorted(fields)}"
    )

    agg_field = fields["orders_aggregate"]
    agg_type = agg_field.type
    # unwrap NonNull/List wrappers down to the named object type
    while hasattr(agg_type, "of_type"):
        agg_type = agg_type.of_type
    assert isinstance(agg_type, GraphQLObjectType), (
        f"aggregate root field type is not an object: {agg_type}"
    )

    # 2. The aggregate type exposes an 'aggregate' sub-field (Hasura v2 pattern)
    assert "aggregate" in agg_type.fields, (
        f"aggregate object missing 'aggregate' field; got {sorted(agg_type.fields)}"
    )
    inner = agg_type.fields["aggregate"].type
    while hasattr(inner, "of_type"):
        inner = inner.of_type
    assert isinstance(inner, GraphQLObjectType), f"inner aggregate type is not an object: {inner}"
    agg_fields = inner.fields

    # 3. count must be present on all columns (root-level count field)
    assert "count" in agg_fields, f"aggregate type missing 'count'; got {sorted(agg_fields)}"

    def _agg_op_columns(op_name: str) -> set[str]:
        assert op_name in agg_fields, (
            f"aggregate type missing '{op_name}'; got {sorted(agg_fields)}"
        )
        op_type = agg_fields[op_name].type
        while hasattr(op_type, "of_type"):
            op_type = op_type.of_type
        assert isinstance(op_type, GraphQLObjectType), (
            f"'{op_name}' field type is not an object: {op_type}"
        )
        return set(op_type.fields)

    # 4. sum/avg/stddev/variance must expose exactly the numeric columns
    numeric = shared_data["numeric_cols"]
    for op in ("sum", "avg", "stddev", "variance"):
        cols = _agg_op_columns(op)
        assert numeric.issubset(cols), (
            f"'{op}' missing numeric columns: expected {numeric}, got {cols}"
        )
        # non-numeric columns must not appear under numeric ops
        assert "region" not in cols, f"'{op}' wrongly includes non-numeric column 'region'"
        assert "created_at" not in cols, f"'{op}' wrongly includes non-numeric column 'created_at'"

    # 5. min/max must expose all comparable columns (numeric + ordered types)
    comparable = shared_data["comparable_cols"]
    for op in ("min", "max"):
        cols = _agg_op_columns(op)
        assert comparable.issubset(cols), (
            f"'{op}' missing comparable columns: expected {comparable}, got {cols}"
        )

    # 6. No steward configuration was supplied beyond enable_aggregates — default behaviour
    si = shared_data["schema_input"]
    orders_table = next(t for t in si.tables if t["table_name"] == "orders")
    assert "aggregate_config" not in orders_table, (
        "default behaviour must not require explicit aggregate_config"
    )


# ---------------------------------------------------------------------------
# REQ-197 — per-role aggregate gating via allow_aggregations
# ---------------------------------------------------------------------------


@given("a role without allow_aggregations permission")
def _role_without_aggregations(shared_data):
    _naming.configure(gql="snake")
    tables = [
        {
            "id": 1,
            "source_id": "sales-pg",
            "domain_id": "sales",
            "schema_name": "public",
            "table_name": "orders",
            "write_ops": registry_write_ops("postgresql"),
            "write_returns_rows": registry_write_returns_rows("postgresql"),
            "write_refused_forms": [],
            # Table-level aggregates auto-detection is enabled; gating must be per-role.
            "enable_aggregates": True,
            "columns": [
                {"column_name": "id", "visible_to": ["analyst"]},
                {"column_name": "amount", "visible_to": ["analyst"]},
                {"column_name": "region", "visible_to": ["analyst"]},
            ],
        }
    ]
    column_types = {
        1: [
            _col("id", "integer"),
            _col("amount", "decimal(10,2)"),
            _col("region", "varchar(20)"),
        ],
    }
    # The analyst role can see all columns (so the base table IS exposed) but has
    # allow_aggregations explicitly withheld (default v2 behaviour).
    role = {
        "description": "Test role.",
        "id": "analyst",
        "capabilities": [],
        "domain_access": ["*"],
        "allow_aggregations": False,
    }
    domains = [{"id": "sales", "description": "Sales"}]
    si = SchemaInput(
        tables=tables,
        relationships=[],
        column_types=column_types,
        naming_rules=[],
        role=role,
        domains=domains,
    )
    shared_data["schema_input"] = si


@when("the schema is generated")
def _generate_schema_for_role(shared_data):
    si = shared_data["schema_input"]
    schema = generate_schema(si)
    assert schema is not None, "schema generation returned None"
    shared_data["schema"] = schema


@then("aggregate root fields are not exposed to that role")
def _verify_no_aggregate_for_role(shared_data):
    schema = shared_data["schema"]
    query_type = schema.query_type
    assert query_type is not None, "schema has no query type"

    fields = query_type.fields

    # The base table must still be exposed — the role can read the columns.
    assert "orders" in fields, (
        f"base 'orders' root field should be exposed to the role; got {sorted(fields)}"
    )

    # The aggregate root field must NOT be exposed to a role lacking allow_aggregations.
    assert "orders_aggregate" not in fields, (
        "aggregate root field 'orders_aggregate' must not be exposed to a role "
        f"without allow_aggregations; got {sorted(fields)}"
    )

    # No aggregate-style root field of any kind should leak through.
    agg_leaks = [name for name in fields if name.endswith("_aggregate")]
    assert not agg_leaks, f"unexpected aggregate root fields exposed to gated role: {agg_leaks}"


# ---------------------------------------------------------------------------
# REQ-198 — aggregate MV routing (query rewrite to use a covering MV)
# ---------------------------------------------------------------------------


@given("an aggregate query whose pattern matches a materialized view")
def _aggregate_query_matching_mv(shared_data):
    # A fresh view pre-aggregating sales.public.orders by region; its id names no base table so
    # base-table absence can be checked after the rewrite.
    shared_data["mv"] = MVDefinition(
        id="aggmv1",
        source_tables=["orders"],
        target_catalog="mat",
        target_schema="sales_mv",
        sql=(
            'SELECT "region", SUM("amount") AS "total_amount", SUM("qty") AS "total_qty" '
            'FROM "sales_pg"."public"."orders" GROUP BY "region"'
        ),
        status=MVStatus.FRESH,
    )
    # The statement as the pipeline's optimize stage sees it: governed, catalog-physical.
    shared_data["base_sql"] = (
        'SELECT "region", SUM("amount"), SUM("qty") FROM "sales_pg"."public"."orders" '
        'GROUP BY "region"'
    )


@when("the compiler processes the query")
def _compiler_processes_query(shared_data):
    from provisa.compiler.sql_types import CompilationContext
    from provisa.mv.view_reinject import reinject

    shared_data["routed"] = reinject(
        shared_data["base_sql"], [shared_data["mv"]], CompilationContext()
    )


@then("it rewrites the query to use the MV instead of the base table")
def _verify_query_rewritten_to_mv(shared_data):
    routed = shared_data["routed"]
    mv: MVDefinition = shared_data["mv"]
    assert routed is not None and routed.mv.id == mv.id, "no view answered"
    assert mv.target_table in routed.sql, routed.sql
    assert '"orders"' not in routed.sql, f"still reads the base table: {routed.sql}"
    # Each aggregate is re-aggregated over the view's stored sums, grouped as asked.
    assert 'SUM("total_amount")' in routed.sql and 'SUM("total_qty")' in routed.sql, routed.sql
    assert "GROUP BY" in routed.sql.upper(), routed.sql


# ---------------------------------------------------------------------------
# REQ-199 — view auto-materialization with TTL-aware staleness fallback
# ---------------------------------------------------------------------------


@given("an expensive view eligible for auto-materialization")
def _expensive_view_eligible(shared_data):
    # An expensive computed view registered with a configurable TTL (refresh_interval). It
    # starts STALE -- never refreshed -- so the registry offers it to no query yet.
    from provisa.mv.registry import MVRegistry

    registry = MVRegistry()
    ttl_seconds = 3600  # default TTL per materialized_views.default_ttl
    mv = MVDefinition(
        id="autoview1",
        source_tables=["expensive_view"],
        target_catalog="iceberg",
        target_schema="analytics_mv",
        refresh_interval=ttl_seconds,
        status=MVStatus.STALE,
        last_refresh_at=None,
    )
    registry.register(mv)
    assert registry.get_fresh() == [], "a never-refreshed view must not be offered to queries"
    assert mv.refresh_interval == ttl_seconds
    assert mv.is_fresh_at(time.time()) is False, "a never-refreshed MV must not be considered fresh"

    shared_data["registry"] = registry
    shared_data["mv"] = mv
    shared_data["ttl_seconds"] = ttl_seconds


@when("the background loop refreshes it and a query targets that view")
def _background_loop_refresh_and_query(shared_data):
    mv: MVDefinition = shared_data["mv"]

    # Background loop refreshes the MV: status -> FRESH, last_refresh_at = now.
    refresh_time = time.time()
    mv.status = MVStatus.FRESH
    mv.last_refresh_at = refresh_time
    mv.row_count = 1000

    # A query right after the refresh is offered the view.
    fresh = shared_data["registry"].get_fresh()
    assert [m.id for m in fresh] == [mv.id], f"the refreshed view is not offered: {fresh}"
    matched = fresh[0]

    # Freshness evaluated within the TTL window -- the MV is serveable.
    shared_data["uses_mv_when_fresh"] = matched.is_fresh_at(refresh_time + 10)
    # A later query past the TTL boundary.
    shared_data["uses_mv_when_stale"] = matched.is_fresh_at(
        refresh_time + shared_data["ttl_seconds"] + 1
    )
    shared_data["matched_mv"] = matched
    shared_data["refresh_time"] = refresh_time


@then("the query uses the MV; if the MV is stale, it falls back to live execution")
def _verify_mv_use_and_stale_fallback(shared_data):
    mv: MVDefinition = shared_data["matched_mv"]

    # 1. After a fresh refresh and within TTL, the query uses the MV.
    assert shared_data["uses_mv_when_fresh"] is True, (
        "query should use the MV when it is fresh and within its TTL"
    )

    # 2. Once the TTL elapses, the MV is no longer serveable — the query must
    #    fall back to live execution (designed REQ-199 exception to fail-fast).
    assert shared_data["uses_mv_when_stale"] is False, (
        "a stale MV (TTL elapsed) must fall back to live execution, not be served"
    )

    # 3. The status enum and TTL gate together drive serveability — a DISABLED MV
    #    is never served regardless of recency.
    mv.status = MVStatus.DISABLED
    assert mv.is_fresh_at(shared_data["refresh_time"] + 1) is False, (
        "a DISABLED MV must never be served, even within its TTL window"
    )

    # 4. Restoring FRESH status within TTL makes it serveable again.
    mv.status = MVStatus.FRESH
    assert mv.is_fresh_at(shared_data["refresh_time"] + 5) is True, (
        "a FRESH MV within its TTL must be serveable"
    )
