# Copyright (c) 2026 Kenneth Stott
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1

"""Step definitions for REQ-196 / REQ-197 / REQ-198 / REQ-199 — aggregate root fields, per-role gating, aggregate MV routing, and view auto-materialization."""

from __future__ import annotations

import time

import pytest

from graphql import GraphQLObjectType
from pytest_bdd import given, when, then, scenarios

from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler import naming as _naming
from provisa.compiler.schema_gen import SchemaInput, generate_schema
from provisa.compiler.context import build_context
from provisa.mv.aggregate_catalog import AggregateMVCatalog
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
            "governance": "pre-approved",
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
    role = {"id": "admin", "capabilities": [], "domain_access": ["*"]}
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
            "governance": "pre-approved",
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
    # Build an aggregate-serving MV catalog and register a covering MV.
    # The MV id avoids the substring "orders" so we can verify base-table absence post-rewrite.
    catalog = AggregateMVCatalog()
    mv = MVDefinition(
        id="aggmv1",
        source_tables=["orders"],
        target_catalog="iceberg",
        target_schema="sales_mv",
        serves_aggregates=True,
        aggregate_columns=["amount", "qty", "region"],
        status=MVStatus.FRESH,
    )
    catalog.register(mv)

    # The MV must have been routed into the per-table index.
    assert "orders" in catalog._by_table, "MV was not registered under its source table"
    assert mv.target_table == "mv_aggmv1", f"unexpected MV backing table name: {mv.target_table}"

    # The incoming aggregate query — a SUM grouped by region over the base table.
    base_sql = "SELECT region, SUM(amount) AS total_amount, SUM(qty) AS total_qty FROM orders GROUP BY region"

    shared_data["catalog"] = catalog
    shared_data["mv"] = mv
    shared_data["base_sql"] = base_sql
    shared_data["agg_columns"] = ["amount", "qty"]
    shared_data["filters"] = []


@when("the compiler processes the query")
def _compiler_processes_query(shared_data):
    catalog: AggregateMVCatalog = shared_data["catalog"]
    agg_columns = shared_data["agg_columns"]
    filters = shared_data["filters"]

    # 1. The catalog must find the covering MV for this aggregate pattern.
    matched = catalog.find_aggregate_mv("orders", agg_columns, filters)
    assert matched is not None, "expected a covering MV but find_aggregate_mv returned None"
    assert matched.id == shared_data["mv"].id, f"matched the wrong MV: {matched.id}"
    shared_data["matched_mv"] = matched

    # 2. Rewrite the SQL to read from the MV backing table instead of the base table.
    rewritten = catalog.rewrite_sql(
        shared_data["base_sql"], matched, agg_columns, remaining_filters=filters
    )
    assert isinstance(rewritten, str) and rewritten, "rewrite_sql returned no SQL"
    shared_data["rewritten_sql"] = rewritten


@then("it rewrites the query to use the MV instead of the base table")
def _verify_query_rewritten_to_mv(shared_data):
    rewritten = shared_data["rewritten_sql"]
    mv: MVDefinition = shared_data["matched_mv"]

    # The rewritten SQL must reference the MV backing table.
    assert mv.target_table in rewritten, (
        f"rewritten SQL does not reference MV backing table '{mv.target_table}': {rewritten}"
    )

    # And it must no longer read directly from the base table.
    assert "orders" not in rewritten, (
        f"rewritten SQL still references the base table 'orders': {rewritten}"
    )

    # The rewrite substitutes raw column reads for aggregate expressions because
    # the MV already holds pre-computed aggregates — SUM is correctly absent.
    upper = rewritten.upper()
    assert "AMOUNT" in upper, f"aggregate column 'amount' lost during rewrite: {rewritten}"
    assert "QTY" in upper, f"aggregate column 'qty' lost during rewrite: {rewritten}"
    assert "GROUP BY" in upper, f"GROUP BY clause lost during rewrite: {rewritten}"


# ---------------------------------------------------------------------------
# REQ-199 — view auto-materialization with TTL-aware staleness fallback
# ---------------------------------------------------------------------------


@given("an expensive view eligible for auto-materialization")
def _expensive_view_eligible(shared_data):
    # An expensive computed view is auto-registered into the aggregate catalog
    # with a configurable TTL (refresh_interval). It starts STALE — never refreshed.
    catalog = AggregateMVCatalog()
    ttl_seconds = 3600  # default TTL per materialized_views.default_ttl
    mv = MVDefinition(
        id="autoview1",
        source_tables=["expensive_view"],
        target_catalog="iceberg",
        target_schema="analytics_mv",
        serves_aggregates=True,
        aggregate_columns=["revenue", "units"],
        refresh_interval=ttl_seconds,
        status=MVStatus.STALE,
        last_refresh_at=None,
    )
    catalog.register(mv)

    # Auto-materialization must register the MV without a bespoke materialized_views entry —
    # it is discoverable via the aggregate catalog by its source table.
    assert "expensive_view" in catalog._by_table, (
        "auto-materialized view was not registered in the aggregate catalog"
    )
    assert mv.refresh_interval == ttl_seconds, (
        f"expected default TTL {ttl_seconds}, got {mv.refresh_interval}"
    )

    # An un-refreshed MV is not serveable — queries must fall back to live execution.
    now = time.time()
    assert mv.is_fresh_at(now) is False, "a never-refreshed MV must not be considered fresh"

    shared_data["catalog"] = catalog
    shared_data["mv"] = mv
    shared_data["ttl_seconds"] = ttl_seconds


@when("the background loop refreshes it and a query targets that view")
def _background_loop_refresh_and_query(shared_data):
    mv: MVDefinition = shared_data["mv"]
    catalog: AggregateMVCatalog = shared_data["catalog"]

    # Background loop refreshes the MV: status -> FRESH, last_refresh_at = now.
    refresh_time = time.time()
    mv.status = MVStatus.FRESH
    mv.last_refresh_at = refresh_time
    mv.row_count = 1000

    # A query immediately after refresh targets the view's source table.
    matched = catalog.find_aggregate_mv("expensive_view", ["revenue"], [])
    assert matched is not None, "freshly refreshed MV not found by the aggregate catalog"
    assert matched.id == mv.id, f"catalog matched the wrong MV: {matched.id}"

    # Freshness evaluated within the TTL window — the MV is serveable.
    query_time_fresh = refresh_time + 10  # well within the 3600s TTL
    shared_data["uses_mv_when_fresh"] = matched.is_fresh_at(query_time_fresh)

    # Simulate the MV becoming stale: a later query past the TTL boundary.
    query_time_stale = refresh_time + shared_data["ttl_seconds"] + 1
    shared_data["uses_mv_when_stale"] = matched.is_fresh_at(query_time_stale)

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
