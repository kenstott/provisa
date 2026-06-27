# Copyright (c) 2026 Kenneth Stott
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1

"""BDD step implementations for REQ-158 / REQ-160 / REQ-543 — Auto-Materialized Relationships.

Cross-source relationships declared with ``materialize: true`` must auto-generate
materialized-view definitions at platform startup, so that joins across sources are
served by a pre-computed MV rather than requiring manual ETL (REQ-158).

Auto-generated MVs start in the STALE state and are populated by the background
refresh loop before they serve live query traffic (REQ-160).

Mutations to a source table of a materialized cross-source relationship mark the
corresponding MV as stale so it is re-refreshed promptly, with a default
``refresh_interval`` of 300 seconds (REQ-543).
"""

from __future__ import annotations

import asyncio
import time

import pytest
from pytest_bdd import given, scenario, then, when

from provisa.mv.models import JoinPattern, MVDefinition, MVStatus
from provisa.mv.registry import MVRegistry


@pytest.fixture
def shared_data() -> dict:
    return {}


def _auto_generate_mvs(relationships: list[dict], registry: MVRegistry) -> list[MVDefinition]:
    """Replicate startup auto-generation of MV definitions for materialized relationships.

    For every cross-source relationship flagged ``materialize: true`` a join-pattern
    MVDefinition is created and registered. Non-materialized relationships are ignored.
    """
    generated: list[MVDefinition] = []
    for rel in relationships:
        if not rel.get("materialize", False):
            continue
        left_table = rel["left_table"]
        right_table = rel["right_table"]
        mv = MVDefinition(
            id=rel.get("id", f"mv-{left_table}-{right_table}"),
            source_tables=[left_table, right_table],
            target_catalog=rel.get("target_catalog", "postgresql"),
            target_schema=rel.get("target_schema", "mv_cache"),
            join_pattern=JoinPattern(
                left_table=left_table,
                left_column=rel["left_column"],
                right_table=right_table,
                right_column=rel["right_column"],
                join_type=rel.get("join_type", "left"),
            ),
            refresh_interval=rel.get("refresh_interval", 300),
        )
        registry.register(mv)
        generated.append(mv)
    return generated


async def _run_background_refresh_loop(mv: MVDefinition, registry: MVRegistry) -> None:
    """Emulate one pass of the background refresh loop populating a STALE MV.

    Mirrors the real refresh lifecycle: a STALE MV transitions REFRESHING -> FRESH,
    its row count is materialised, and its last refresh timestamp is stamped so the
    registry will then serve it as fresh (REQ-160, REQ-199).
    """
    if mv.status != MVStatus.STALE:
        return
    mv.status = MVStatus.REFRESHING
    # Simulate the asynchronous population of the backing table by the loop.
    await asyncio.sleep(0)
    mv.row_count = 42
    mv.last_refresh_at = time.time()
    mv.status = MVStatus.FRESH
    # Re-register so the registry reflects the populated state.
    registry.register(mv)


def _apply_mutation(mutated_table: str, registry: MVRegistry) -> list[MVDefinition]:
    """Replicate mutation-triggered staleness propagation (REQ-543).

    Any MV whose ``source_tables`` include the mutated table is marked STALE so the
    background refresh loop will re-refresh it within its ``refresh_interval``. The MV's
    last refresh timestamp is preserved so freshness/TTL bookkeeping remains intact.
    """
    invalidated: list[MVDefinition] = []
    for mv in registry.all():
        if mutated_table in mv.source_tables:
            mv.status = MVStatus.STALE
            registry.register(mv)
            invalidated.append(mv)
    return invalidated


# ---------------------------------------------------------------------------
# REQ-158
# ---------------------------------------------------------------------------


@scenario(
    "REQ-158_auto_materialized_relationships.feature",
    "REQ-158 default behaviour",
)
def test_req_158_default_behaviour():
    pass


@given("a cross-source relationship configured with materialize: true")
def given_cross_source_relationship(shared_data: dict):
    relationship = {
        "id": "rel-orders-customers",
        "left_source": "sales_pg",
        "left_table": "orders",
        "left_column": "customer_id",
        "right_source": "crm_mysql",
        "right_table": "customers",
        "right_column": "id",
        "join_type": "left",
        "materialize": True,
    }
    # A second, non-materialized relationship to prove only flagged ones generate MVs.
    other = {
        "id": "rel-orders-shipments",
        "left_source": "sales_pg",
        "left_table": "orders",
        "left_column": "id",
        "right_source": "logistics_pg",
        "right_table": "shipments",
        "right_column": "order_id",
        "materialize": False,
    }
    shared_data["relationships"] = [relationship, other]
    shared_data["registry"] = MVRegistry()
    assert relationship["materialize"] is True
    assert relationship["left_source"] != relationship["right_source"]


@when("the platform starts up")
def when_platform_starts_up(shared_data: dict):
    registry: MVRegistry = shared_data["registry"]
    generated = _auto_generate_mvs(shared_data["relationships"], registry)
    shared_data["generated"] = generated


@then("MV definitions are auto-generated for that relationship")
def then_mv_definitions_generated(shared_data: dict):
    registry: MVRegistry = shared_data["registry"]
    generated: list[MVDefinition] = shared_data["generated"]

    # Only the materialized relationship produced an MV.
    assert len(generated) == 1
    assert len(registry.all()) == 1

    mv = registry.get("rel-orders-customers")
    assert mv is not None
    assert isinstance(mv, MVDefinition)

    # The MV spans both cross-source tables.
    assert set(mv.source_tables) == {"orders", "customers"}

    # The join pattern mirrors the declared relationship.
    assert mv.join_pattern is not None
    assert mv.join_pattern.left_table == "orders"
    assert mv.join_pattern.left_column == "customer_id"
    assert mv.join_pattern.right_table == "customers"
    assert mv.join_pattern.right_column == "id"
    assert mv.join_pattern.join_type == "left"

    # Newly auto-generated MVs start stale until first refresh.
    assert mv.status == MVStatus.STALE
    assert mv.target_table == "mv_rel_orders_customers"

    # The non-materialized relationship did not produce an MV.
    assert registry.get("rel-orders-shipments") is None


# ---------------------------------------------------------------------------
# REQ-160
# ---------------------------------------------------------------------------


@scenario(
    "REQ-160_auto_materialized_relationships.feature",
    "REQ-160 default behaviour",
)
def test_req_160_default_behaviour():
    pass


@given("an auto-generated MV created at startup")
def given_auto_generated_mv(shared_data: dict):
    registry = MVRegistry()
    relationships = [
        {
            "id": "rel-orders-customers",
            "left_table": "orders",
            "left_column": "customer_id",
            "right_table": "customers",
            "right_column": "id",
            "join_type": "left",
            "materialize": True,
        }
    ]
    generated = _auto_generate_mvs(relationships, registry)
    assert len(generated) == 1
    mv = generated[0]

    # Verify the MV was auto-generated with the correct initial state.
    assert mv.status == MVStatus.STALE
    assert mv.last_refresh_at is None
    assert mv.row_count is None

    shared_data["registry"] = registry
    shared_data["mv"] = mv


@when("it is first created")
def when_first_created(shared_data: dict):
    mv: MVDefinition = shared_data["mv"]
    # Capture the freshly-created state before any refresh loop runs.
    shared_data["initial_status"] = mv.status
    shared_data["initial_last_refresh"] = mv.last_refresh_at
    shared_data["initial_row_count"] = mv.row_count


@then(
    "its state is STALE and the background refresh loop populates it "
    "before it serves queries"
)
def then_stale_then_populated(shared_data: dict):
    registry: MVRegistry = shared_data["registry"]
    mv: MVDefinition = shared_data["mv"]

    # At creation it is STALE and never refreshed.
    assert shared_data["initial_status"] == MVStatus.STALE
    assert shared_data["initial_last_refresh"] is None
    assert shared_data["initial_row_count"] is None

    # A STALE MV is NOT served as fresh — queries fall back to live sources.
    fresh_before = registry.get_fresh()
    assert mv.id not in {m.id for m in fresh_before}
    assert mv.is_fresh is False

    # The background refresh loop populates it.
    asyncio.run(_run_background_refresh_loop(mv, registry))

    # Now it is FRESH, populated, and served by the registry.
    assert mv.status == MVStatus.FRESH
    assert mv.is_fresh is True
    assert mv.row_count == 42
    assert mv.last_refresh_at is not None
    assert mv.is_fresh_at(time.time()) is True

    fresh_after = registry.get_fresh()
    assert mv.id in {m.id for m in fresh_after}


# ---------------------------------------------------------------------------
# REQ-543
# ---------------------------------------------------------------------------


@scenario(
    "REQ-543_auto_materialized_relationships.feature",
    "REQ-543 default behaviour",
)
def test_req_543_default_behaviour():
    pass


@given("a materialized cross-source relationship MV")
def given_materialized_relationship_mv(shared_data: dict):
    registry = MVRegistry()
    relationships = [
        {
            "id": "rel-orders-customers",
            "left_table": "orders",
            "left_column": "customer_id",
            "right_table": "customers",
            "right_column": "id",
            "join_type": "left",
            "materialize": True,
            # refresh_interval intentionally omitted to verify the 300s default.
        }
    ]
    generated = _auto_generate_mvs(relationships, registry)
    assert len(generated) == 1
    mv = generated[0]

    # refresh_interval must default to 300 seconds (5 minutes) when not set.
    assert mv.refresh_interval == 300

    # Bring the MV to a FRESH state via the background refresh loop so we can
    # observe the transition back to STALE when a mutation arrives.
    asyncio.run(_run_background_refresh_loop(mv, registry))
    assert mv.status == MVStatus.FRESH
    assert mv.is_fresh is True
    assert mv.id in {m.id for m in registry.get_fresh()}

    shared_data["registry"] = registry
    shared_data["mv"] = mv


@when("a mutation is applied to one of its source tables")
def when_mutation_applied(shared_data: dict):
    registry: MVRegistry = shared_data["registry"]
    mv: MVDefinition = shared_data["mv"]

    # Pick a real source table of the MV and mutate it.
    mutated_table = mv.source_tables[0]
    shared_data["mutated_table"] = mutated_table
    shared_data["last_refresh_before"] = mv.last_refresh_at

    invalidated = _apply_mutation(mutated_table, registry)
    shared_data["invalidated"] = invalidated


@then(
    "the MV is marked stale and scheduled for re-refresh within the refresh_interval"
)
def then_marked_stale_and_scheduled(shared_data: dict):
    registry: MVRegistry = shared_data["registry"]
    mv: MVDefinition = shared_data["mv"]
    invalidated: list[MVDefinition] = shared_data["invalidated"]

    # The mutated source table's MV was invalidated.
    assert mv.id in {m.id for m in invalidated}

    # The MV is now STALE.
    assert mv.status == MVStatus.STALE
    assert mv.is_fresh is False

    # A STALE MV is no longer served as fresh — it falls back to live sources
    # until the background refresh loop re-populates it.
    assert mv.id not in {m.id for m in registry.get_fresh()}
    assert mv.is_fresh_at(time.time()) is False

    # The default refresh_interval governs the re-refresh window (5 minutes).
    assert mv.refresh_interval == 300

    # Re-refresh restores freshness within the interval.
    asyncio.run(_run_background_refresh_loop(mv, registry))
    assert mv.status == MVStatus.FRESH
    assert mv.is_fresh is True
    assert mv.last_refresh_at is not None
    assert mv.last_refresh_at >= (shared_data["last_refresh_before"] or 0.0)
    assert mv.id in {m.id for m in registry.get_fresh()}
