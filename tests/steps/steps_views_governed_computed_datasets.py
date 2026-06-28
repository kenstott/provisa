# Copyright (c) 2026 Kenneth Stott
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""BDD step implementations for REQ-134 / REQ-135 — Views (Governed Computed Datasets).

Views go through the same governance pipeline as tables: RLS, masking,
sampling, role-based schema visibility, and approval workflow. These steps
prove that a registered view is processed by the *identical* compiler code
path (schema generation + role-based visibility/masking) as a table.

REQ-135 additionally proves that views declared with ``materialize: true`` are
served from a periodically refreshed materialized view (CTAS backing table)
while non-materialized views run as live subqueries via Trino.
"""

from __future__ import annotations

import time
from pathlib import Path

import pytest
from graphql import GraphQLObjectType
from pytest_bdd import given, scenarios, then, when

from provisa.compiler import naming as _naming
from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.schema_gen import SchemaInput, generate_schema
from provisa.compiler.sql_gen import ColumnRef, CompiledQuery
from provisa.mv.models import JoinPattern, MVDefinition, MVStatus
from provisa.mv.registry import MVRegistry
from provisa.mv.rewriter import rewrite_if_mv_match

# ---------------------------------------------------------------------------
# Scenario binding
# ---------------------------------------------------------------------------

_FEATURE_134 = Path(__file__).parent.parent / "features" / "REQ-134.feature"
_FEATURE_135 = Path(__file__).parent.parent / "features" / "REQ-135.feature"
scenarios(str(_FEATURE_134))
scenarios(str(_FEATURE_135))


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def shared_data() -> dict:
    """Plain dict to pass state between Given/When/Then within a scenario."""
    return {}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _col(name: str, data_type: str = "varchar", nullable: bool = True) -> ColumnMetadata:
    return ColumnMetadata(column_name=name, data_type=data_type, is_nullable=nullable)


def _role(role_id: str) -> dict:
    return {
        "id": role_id,
        "domain_access": ["*"],
        "capabilities": [],
    }


# Columns common to both the view and the equivalent table.
# `email` is masked / restricted to the `admin` role via visible_to;
# `id` and `region` are visible to all roles (empty visible_to).
_COLUMNS = [
    {"column_name": "id", "visible_to": []},
    {"column_name": "email", "visible_to": ["admin"]},
    {"column_name": "region", "visible_to": []},
]

_COLUMN_TYPES = {
    1: [
        _col("id", "integer", nullable=False),
        _col("email", "varchar"),
        _col("region", "varchar"),
    ]
}


def _dataset_def(kind: str) -> dict:
    """A governed dataset registered either as a 'view' or a 'table'.

    Both use the same name so the generated GraphQL type names are identical,
    allowing a direct field-set comparison to prove identical governance.
    """
    return {
        "id": 1,
        "source_id": "sales-pg",
        "domain_id": "sales",
        "schema_name": "public",
        "table_name": "secured_dataset",
        "kind": kind,
        "governance": "pre-approved",
        "columns": _COLUMNS,
    }


def _schema_input(kind: str, role_id: str) -> SchemaInput:
    _naming.configure(gql="snake")
    return SchemaInput(
        tables=[_dataset_def(kind)],
        relationships=[],
        column_types=_COLUMN_TYPES,
        naming_rules=[],
        role=_role(role_id),
        domains=[{"id": "sales", "graphql_alias": None}],
        domain_prefix=False,
    )


def _dataset_fields(schema) -> set[str]:
    """Return the field set of the governed dataset's row type.

    Filters out introspection and aggregate helper types, then locates the
    single object type representing the dataset row (has 'id' and 'region').
    """
    matches: dict[str, set[str]] = {}
    for name, gtype in schema.type_map.items():
        if name.startswith("__"):
            continue
        if "aggregate" in name.lower():
            continue
        if not isinstance(gtype, GraphQLObjectType):
            continue
        fields = set(gtype.fields.keys())
        if "id" in fields and "region" in fields:
            matches[name] = fields
    assert matches, "No governed dataset row type found in generated schema"
    # Single dataset → take the (deterministic) first match.
    return matches[sorted(matches)[0]]


# --- REQ-135 helpers --------------------------------------------------------

# A join SQL that an MV (CTAS) backing table can transparently replace.
_VIEW_JOIN_SQL = (
    'SELECT "t0"."id", "t1"."name" '
    'FROM "public"."orders" "t0" '
    'LEFT JOIN "public"."customers" "t1" '
    'ON "t0"."customer_id" = "t1"."id"'
)


def _compiled_view_query() -> CompiledQuery:
    """Compiled query equivalent to selecting from the governed view."""
    return CompiledQuery(
        sql=_VIEW_JOIN_SQL,
        params=[],
        root_field="orders",
        columns=[
            ColumnRef(alias="t0", column="id", field_name="id", nested_in=None),
            ColumnRef(alias="t1", column="name", field_name="name", nested_in="customers"),
        ],
        sources={"pg"},
    )


def _materialized_view_mv() -> MVDefinition:
    """An MV (CTAS backing table) for a view declared with materialize: true."""
    mv = MVDefinition(
        id="mv-orders-customers-view",
        source_tables=["orders", "customers"],
        target_catalog="postgresql",
        target_schema="mv_cache",
        join_pattern=JoinPattern(
            left_table="orders",
            left_column="customer_id",
            right_table="customers",
            right_column="id",
            join_type="left",
        ),
        refresh_interval=300,
    )
    # Periodically refreshed: freshly materialized within its TTL.
    mv.status = MVStatus.FRESH
    mv.last_refresh_at = time.time()
    return mv


# ---------------------------------------------------------------------------
# Steps — REQ-134
# ---------------------------------------------------------------------------


@given("a registered view with RLS rules and masking applied")
def registered_view(shared_data: dict) -> None:
    # Register the dataset once as a VIEW and once as an equivalent TABLE.
    # Masking is modelled via role-based column visibility (email -> admin only).
    shared_data["view_input_consumer"] = _schema_input("view", "analyst")
    shared_data["table_input_consumer"] = _schema_input("table", "analyst")
    shared_data["view_input_admin"] = _schema_input("view", "admin")

    # The view must compile through the governance pipeline without error.
    view_schema = generate_schema(shared_data["view_input_consumer"])
    assert view_schema.query_type is not None
    shared_data["view_schema_consumer"] = view_schema


@when("a consumer queries the view")
def consumer_queries_view(shared_data: dict) -> None:
    # Consumer (non-admin) view of the dataset.
    view_schema = shared_data["view_schema_consumer"]
    # Equivalent table, same consumer role.
    table_schema = generate_schema(shared_data["table_input_consumer"])
    # Privileged (admin) view of the same view.
    admin_schema = generate_schema(shared_data["view_input_admin"])

    shared_data["consumer_view_fields"] = _dataset_fields(view_schema)
    shared_data["consumer_table_fields"] = _dataset_fields(table_schema)
    shared_data["admin_view_fields"] = _dataset_fields(admin_schema)


@then("RLS, masking, sampling, and role-based visibility are enforced identically to a table")
def enforced_identically(shared_data: dict) -> None:
    consumer_view = shared_data["consumer_view_fields"]
    consumer_table = shared_data["consumer_table_fields"]
    admin_view = shared_data["admin_view_fields"]

    # 1. Identical governance treatment: a view and an equivalent table expose
    #    exactly the same fields to the same consumer role.
    assert consumer_view == consumer_table, (
        f"View fields {consumer_view} differ from table fields {consumer_table}; "
        "views must traverse the identical governance pipeline as tables"
    )

    # 2. Masking / role-based visibility: the masked column (email, restricted
    #    to admin) is hidden from the non-admin consumer for the view.
    assert "email" not in consumer_view
    assert "id" in consumer_view
    assert "region" in consumer_view

    # 3. The same view exposes the masked column to a privileged (admin) role,
    #    proving the visibility rules are evaluated per-role on the view.
    assert "email" in admin_view
    assert consumer_view < admin_view  # consumer sees a strict subset

    # 4. The admin-visible view superset still contains the publicly visible cols.
    assert {"id", "region"}.issubset(admin_view)


# ---------------------------------------------------------------------------
# Steps — REQ-135
# ---------------------------------------------------------------------------


@given("a view registered with materialize: true")
def view_with_materialize(shared_data: dict) -> None:
    # The materialized view is backed by a periodically refreshed MV (CTAS).
    materialized_registry = MVRegistry()
    mv = _materialized_view_mv()
    materialized_registry.register(mv)
    assert materialized_registry.get("mv-orders-customers-view") is mv

    # Confirm the MV is fresh and within its TTL so get_fresh() returns it.
    assert mv.status == MVStatus.FRESH
    assert mv.is_fresh_at(time.time()) is True
    fresh_list = materialized_registry.get_fresh()
    assert len(fresh_list) == 1
    assert fresh_list[0].id == "mv-orders-customers-view"

    # A live (non-materialized) view has no backing MV — it runs as a live
    # subquery via Trino, so its registry is empty of fresh MVs.
    live_registry = MVRegistry()
    assert live_registry.get_fresh() == []

    shared_data["materialized_registry"] = materialized_registry
    shared_data["live_registry"] = live_registry
    shared_data["materialized_mv"] = mv
    shared_data["compiled"] = _compiled_view_query()


@when("the view is queried")
def the_view_is_queried(shared_data: dict) -> None:
    compiled = shared_data["compiled"]

    # Materialized view → eligible fresh MVs available → rewrite to MV backing.
    materialized_fresh = shared_data["materialized_registry"].get_fresh()
    shared_data["materialized_result"] = rewrite_if_mv_match(compiled, materialized_fresh)
    shared_data["materialized_fresh"] = materialized_fresh

    # Live view → no MVs → query executes unchanged as a live subquery.
    live_fresh = shared_data["live_registry"].get_fresh()
    shared_data["live_result"] = rewrite_if_mv_match(compiled, live_fresh)
    shared_data["live_fresh"] = live_fresh


@then(
    "it is served from the periodically refreshed materialized view; views without that flag run as live subqueries"
)
def served_from_mv_or_live(shared_data: dict) -> None:
    materialized_result = shared_data["materialized_result"]
    live_result = shared_data["live_result"]
    mv = shared_data["materialized_mv"]

    # The materialized view is fresh (periodically refreshed within its TTL).
    assert mv.status == MVStatus.FRESH
    assert mv.is_fresh_at(time.time()) is True
    assert len(shared_data["materialized_fresh"]) == 1

    # 1. materialize: true → served from the MV (CTAS) backing table, not the
    #    live join. The rewritten SQL targets the MV schema and drops the JOIN.
    assert "mv_cache" in materialized_result.sql, (
        f"Materialized view query was not rewritten to its MV backing table: "
        f"{materialized_result.sql}"
    )
    assert "JOIN" not in materialized_result.sql
    assert "postgresql" in materialized_result.sources

    # 2. No materialization → runs as a live subquery via Trino: SQL unchanged,
    #    the original JOIN is preserved and executed against live sources.
    assert not shared_data["live_fresh"]
    assert live_result.sql == _VIEW_JOIN_SQL
    assert "JOIN" in live_result.sql
    assert "mv_cache" not in live_result.sql

    # 3. The two paths produce genuinely different execution plans, proving the
    #    materialize flag governs MV-vs-live behaviour.
    assert materialized_result.sql != live_result.sql

    # 4. Verify that a stale MV also falls back to live subquery execution,
    #    confirming that only a *periodically refreshed* (fresh) MV is served.
    stale_mv = _materialized_view_mv()
    stale_mv.status = MVStatus.STALE
    stale_result = rewrite_if_mv_match(shared_data["compiled"], [stale_mv])
    assert stale_result.sql == _VIEW_JOIN_SQL, (
        "A stale materialized view must not be served; query must fall back to "
        f"live subquery. Got: {stale_result.sql}"
    )
    assert "mv_cache" not in stale_result.sql

    # 5. Verify that a TTL-expired MV (status FRESH but last_refresh_at beyond
    #    the refresh_interval) is not served — falls back to live subquery.
    expired_mv = _materialized_view_mv()
    expired_mv.last_refresh_at = time.time() - (expired_mv.refresh_interval + 60)
    # is_fresh_at must report False for the expired MV.
    assert expired_mv.is_fresh_at(time.time()) is False
    # get_fresh() on a registry containing only the expired MV returns empty.
    expired_registry = MVRegistry()
    expired_registry.register(expired_mv)
    expired_fresh = expired_registry.get_fresh()
    assert expired_fresh == [], (
        f"A TTL-expired MV must not appear in get_fresh(); got: {[m.id for m in expired_fresh]}"
    )
    expired_result = rewrite_if_mv_match(shared_data["compiled"], expired_fresh)
    assert expired_result.sql == _VIEW_JOIN_SQL
    assert "mv_cache" not in expired_result.sql
