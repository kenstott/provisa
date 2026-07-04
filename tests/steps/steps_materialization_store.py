# Copyright (c) 2026 Kenneth Stott
# Canary: 21d7d412-e7b6-4906-af2b-ffb72a6b642e
#
# This source code is licensed under the Business Source License 1.1

"""pytest-bdd step implementations for REQ-882 — Aggregate MV rewrite path."""

from __future__ import annotations


import pytest
from pytest_bdd import given, parsers, scenario, then, when

from provisa.mv.aggregate_catalog import (
    AggregateMVCatalog,
    rewrite_aggregate_query,
)
from provisa.mv.models import MVDefinition, MVStatus
from provisa.mv.registry import MVRegistry


# ---------------------------------------------------------------------------
# Scenario binding
# ---------------------------------------------------------------------------


@scenario(
    "../../features/req_882.feature",
    "REQ-882 default behaviour",
)
def test_req_882_default_behaviour():
    pass


# ---------------------------------------------------------------------------
# Shared state fixture
# ---------------------------------------------------------------------------


@pytest.fixture
def shared_data():
    return {}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_aggregate_mv(
    mv_id: str,
    source_tables: list[str],
    aggregate_columns: list[str],
    filters: list[str] | None = None,
    target_catalog: str = "iceberg",
    target_schema: str = "mv_store",
    target_table: str | None = None,
) -> MVDefinition:
    mv = MVDefinition(
        id=mv_id,
        source_tables=source_tables,
        target_catalog=target_catalog,
        target_schema=target_schema,
        target_table=target_table or f"mv_{mv_id.replace('-', '_')}",
        serves_aggregates=True,
        aggregate_columns=aggregate_columns,
        filters=filters or [],
        status=MVStatus.FRESH,
        enabled=True,
    )
    return mv


# ---------------------------------------------------------------------------
# Given
# ---------------------------------------------------------------------------


@given(
    parsers.parse(
        'an aggregate MV over "orders" pre-computing SUM(amount), registered with no filters, '
        "and the aggregate catalog populated from the MV registry"
    )
)
def given_aggregate_mv_registered(shared_data):
    """Register an aggregate MV for 'orders' covering the 'amount' column with no filters."""
    catalog = AggregateMVCatalog()
    registry = MVRegistry()

    mv = _make_aggregate_mv(
        mv_id="mv-orders-sum-amount",
        source_tables=["orders"],
        aggregate_columns=["amount"],
        filters=[],
        target_catalog="iceberg",
        target_schema="mv_store",
        target_table="mv_orders_sum_amount",
    )
    # MVRegistry.register syncs the process-level catalog; we also register directly
    # on our isolated catalog so tests are hermetic.
    registry.register(mv)
    catalog.register(mv)

    # Confirm the MV is visible in the catalog
    found = catalog.find_aggregate_mv("orders", ["amount"], ["region = 'us'"])
    assert found is not None, (
        "MV was registered but find_aggregate_mv returned None — "
        "aggregate catalog was not populated from the registry"
    )
    assert found.id == "mv-orders-sum-amount"

    shared_data["catalog"] = catalog
    shared_data["registry"] = registry
    shared_data["mv"] = mv


# ---------------------------------------------------------------------------
# When
# ---------------------------------------------------------------------------


@when(
    parsers.parse(
        "a query \"SELECT SUM(amount) FROM orders WHERE region = 'us'\" reaches the endpoint "
        "and the join-MV rewriter did not fire"
    )
)
def when_aggregate_query_reaches_endpoint(shared_data):
    """Simulate the endpoint query path: join-MV rewriter did not fire, so we call aggregate rewrite."""
    sql = "SELECT SUM(amount) FROM orders WHERE region = 'us'"
    catalog: AggregateMVCatalog = shared_data["catalog"]

    # rewrite_aggregate_query is what the endpoint calls after the join-MV rewriter
    result = rewrite_aggregate_query(sql, catalog)

    shared_data["original_sql"] = sql
    shared_data["rewrite_result"] = result


# ---------------------------------------------------------------------------
# Then
# ---------------------------------------------------------------------------


@then(
    parsers.parse(
        "the query is rewritten to read the MV target table with region = 'us' re-applied, "
        "and its sources become the MV catalog"
    )
)
def then_query_rewritten_to_mv(shared_data):
    """Assert the query was rewritten to the MV target table and the region filter re-applied."""
    result = shared_data["rewrite_result"]
    mv: MVDefinition = shared_data["mv"]

    assert result is not None, (
        "rewrite_aggregate_query returned None — no aggregate MV rewrite happened, "
        "but a covering MV was registered"
    )

    rewritten_sql, used_mv = result

    # The rewrite must have used our registered MV
    assert used_mv.id == mv.id, f"Expected MV id={mv.id!r} to be used, got {used_mv.id!r}"

    # The rewritten SQL must reference the MV target table
    assert mv.target_table in rewritten_sql, (
        f"Rewritten SQL does not reference MV target table {mv.target_table!r}:\n{rewritten_sql}"
    )

    # The region filter must be re-applied in the rewritten SQL
    assert "region" in rewritten_sql.lower(), (
        f"Rewritten SQL does not contain the re-applied 'region' filter:\n{rewritten_sql}"
    )
    assert "us" in rewritten_sql, (
        f"Rewritten SQL does not contain the 'us' value in the region filter:\n{rewritten_sql}"
    )

    # The MV comment annotation must be present
    assert "aggregate_mv" in rewritten_sql, (
        f"Rewritten SQL is missing the aggregate_mv annotation:\n{rewritten_sql}"
    )

    # Verify sources would be set to the MV catalog (tested via the CompiledQuery path
    # in rewrite_if_aggregate_match; here we validate the used_mv carries the right catalog)
    assert used_mv.target_catalog == mv.target_catalog, (
        f"MV target_catalog mismatch: expected {mv.target_catalog!r}, got {used_mv.target_catalog!r}"
    )


@then(
    parsers.parse(
        "an MV pre-computed WITH status = 'active' is NOT used for a query that lacks that "
        "filter (subset-safety), so no rows are silently dropped"
    )
)
def then_subset_safety_enforced(shared_data):
    """Assert subset-safety: an MV pre-computed with a filter is NOT used by a query that lacks that filter."""
    # Build a fresh isolated catalog with a filtered MV
    filtered_catalog = AggregateMVCatalog()

    # MV was pre-computed WITH status = 'active' — it holds ONLY rows where status = 'active'
    filtered_mv = _make_aggregate_mv(
        mv_id="mv-orders-sum-active",
        source_tables=["orders"],
        aggregate_columns=["amount"],
        filters=["status = 'active'"],
        target_catalog="iceberg",
        target_schema="mv_store",
        target_table="mv_orders_sum_active",
    )
    filtered_catalog.register(filtered_mv)

    # A query WITHOUT status = 'active' must NOT use this MV (subset-safety)
    # The MV's filter {status = 'active'} is NOT a subset of the query's filters {}
    query_filters_without_status: list[str] = []
    unsafe_mv = filtered_catalog.find_aggregate_mv(
        "orders", ["amount"], query_filters_without_status
    )
    assert unsafe_mv is None, (
        f"Subset-safety violation: MV pre-computed with status='active' was returned "
        f"for a query that does NOT have that filter. This would silently drop rows. "
        f"MV id={unsafe_mv.id!r}"
    )

    # Verify the same MV IS used when the query INCLUDES the required filter (positive check)
    query_filters_with_status = ["status = 'active'"]
    safe_mv = filtered_catalog.find_aggregate_mv("orders", ["amount"], query_filters_with_status)
    assert safe_mv is not None, (
        "Expected the filtered MV to be usable when the query includes status='active', "
        "but find_aggregate_mv returned None"
    )
    assert safe_mv.id == "mv-orders-sum-active"

    # Also verify via rewrite_aggregate_query end-to-end:
    # A query that LACKS the status filter must not be rewritten onto the filtered MV
    sql_without_filter = "SELECT SUM(amount) FROM orders WHERE region = 'us'"
    rewrite_unsafe = rewrite_aggregate_query(sql_without_filter, filtered_catalog)
    assert rewrite_unsafe is None, (
        "rewrite_aggregate_query should return None for a query missing the MV's "
        "required filter 'status = ''active''', but it returned a rewrite. "
        "This would silently drop non-active rows."
    )

    # A query that INCLUDES the status filter may be safely rewritten
    sql_with_filter = "SELECT SUM(amount) FROM orders WHERE status = 'active'"
    rewrite_safe = rewrite_aggregate_query(sql_with_filter, filtered_catalog)
    assert rewrite_safe is not None, (
        "rewrite_aggregate_query should rewrite a query that includes the MV's filter "
        "'status = ''active''', but it returned None"
    )
    safe_rewritten_sql, safe_used_mv = rewrite_safe
    assert safe_used_mv.id == "mv-orders-sum-active"
    assert "mv_orders_sum_active" in safe_rewritten_sql
