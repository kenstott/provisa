# Copyright (c) 2026 Kenneth Stott
# Canary: req-234-mv-storage-reclamation-steps
#
# This source code is licensed under the Business Source License 1.1

"""BDD step implementations for REQ-234 — Materialized View storage reclamation.

Covers: dropping backing MV tables when a view is removed/disabled or its
source is unregistered, and flagging orphaned MV tables for auto-drop.
"""

from __future__ import annotations

import asyncio
import inspect
import time
from unittest.mock import MagicMock

import pytest
from pytest_bdd import given, scenarios, then, when

from provisa.mv.models import JoinPattern, MVDefinition, MVStatus
from provisa.mv.registry import MVRegistry
from provisa.mv import refresh as mv_refresh

scenarios("../features/REQ-234.feature")


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def shared_data() -> dict:
    """Plain dict for passing state between Given/When/Then steps."""
    return {}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_mv(
    mv_id: str = "mv-reclaim",
    target_table: str = "mv_reclaim",
    enabled: bool = True,
    orphan_grace_period: int = 0,
) -> MVDefinition:
    return MVDefinition(
        id=mv_id,
        source_tables=["orders", "customers"],
        target_catalog="iceberg",
        target_schema="mv_cache",
        target_table=target_table,
        join_pattern=JoinPattern(
            left_table="orders",
            left_column="customer_id",
            right_table="customers",
            right_column="id",
            join_type="left",
        ),
        refresh_interval=300,
        enabled=enabled,
        orphan_grace_period=orphan_grace_period,
    )


def _make_mock_conn(schema_tables: list[str]) -> tuple[MagicMock, list[str]]:
    """Build a mock Trino connection.

    The cursor reports ``schema_tables`` when the backing schema is listed and
    records every executed SQL statement into the returned list so the test can
    assert that DROP TABLE statements were issued.
    """
    executed: list[str] = []

    cursor = MagicMock()

    def _execute(sql, *args, **kwargs):
        executed.append(sql)

    cursor.execute.side_effect = _execute
    cursor.fetchall.return_value = [(t,) for t in schema_tables]
    cursor.fetchone.return_value = (0,)

    conn = MagicMock()
    conn.cursor.return_value = cursor
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)

    return conn, executed


def _make_mock_engine(schema_tables: list[str], executed: list[str]) -> MagicMock:
    """A mock federation engine whose async ``execute_engine`` records every SQL into ``executed`` and
    reports ``schema_tables`` as result rows — the MV lifecycle uses ``engine.execute_engine`` (REQ-234)."""
    engine = MagicMock()

    async def _execute_engine(sql, *args, **kwargs):
        executed.append(sql)
        result = MagicMock()
        result.rows = [(t,) for t in schema_tables]
        return result

    engine.execute_engine = _execute_engine
    return engine


def _invoke(func, **available):
    """Bind only the parameters ``func`` declares, call it, await if needed."""
    if func is None:
        return None
    sig = inspect.signature(func)
    kwargs = {k: v for k, v in available.items() if k in sig.parameters}
    result = func(**kwargs)
    if inspect.isawaitable(result):
        result = asyncio.run(result)
    return result


# ---------------------------------------------------------------------------
# Given
# ---------------------------------------------------------------------------


@given("a materialized view that is removed from config or whose source is unregistered")
def given_removed_mv(shared_data: dict) -> None:
    registry = MVRegistry()

    # An active MV that remains in the registry/config.
    active_mv = _make_mv(mv_id="mv-active", target_table="mv_active")
    registry.register(active_mv)

    # An MV that has been disabled (config removed / source unregistered).
    removed_mv = _make_mv(
        mv_id="mv-removed",
        target_table="mv_removed",
        enabled=False,
        orphan_grace_period=0,
    )
    removed_mv.status = MVStatus.DISABLED
    removed_mv.last_refresh_at = time.time() - 10_000
    registry.register(removed_mv)

    # The target schema physically contains:
    #  - the active MV table (mv_active)
    #  - the disabled MV table (mv_removed) -- must be reclaimed
    #  - an orphan table (mv_ghost) present in schema but absent from registry
    schema_tables = ["mv_active", "mv_removed", "mv_ghost"]
    conn, executed = _make_mock_conn(schema_tables)
    engine = _make_mock_engine(schema_tables, executed)

    shared_data["registry"] = registry
    shared_data["active_mv"] = active_mv
    shared_data["removed_mv"] = removed_mv
    shared_data["conn"] = conn
    shared_data["engine"] = engine
    shared_data["executed_sql"] = executed
    shared_data["target_catalog"] = "iceberg"
    shared_data["target_schema"] = "mv_cache"
    shared_data["known_tables"] = {"mv_active"}


# ---------------------------------------------------------------------------
# When
# ---------------------------------------------------------------------------


@when("config is reloaded or the daily cleanup runs")
def when_cleanup_runs(shared_data: dict) -> None:
    registry: MVRegistry = shared_data["registry"]
    conn = shared_data["conn"]
    removed_mv: MVDefinition = shared_data["removed_mv"]
    now = time.time()

    # config_mv_ids: the set of MV IDs still present in config (active only).
    config_mv_ids = {shared_data["active_mv"].id}
    # Orphan tracker maps table name -> first-seen timestamp; ghost is already expired.
    orphan_tracker: dict[str, float] = {"mv_ghost": now - 10_000}
    orphan_tables = ["mv_ghost"]
    common = dict(
        conn=conn,
        connection=conn,
        trino_conn=conn,
        registry=registry,
        config_mv_ids=config_mv_ids,
        mv=removed_mv,
        mvs=[shared_data["active_mv"], removed_mv],
        definitions=[shared_data["active_mv"], removed_mv],
        now=now,
        grace_period=0,
        catalog=shared_data["target_catalog"],
        schema=shared_data["target_schema"],
        schema_name=shared_data["target_schema"],
        target_catalog=shared_data["target_catalog"],
        target_schema=shared_data["target_schema"],
        auto_drop=True,
        known_tables=shared_data["known_tables"],
        orphan_tracker=orphan_tracker,
        orphan_tables=orphan_tables,
        engine=shared_data["engine"],
    )

    # 1. Reclaim tables for MVs removed/disabled in config.
    shared_data["reclaimed"] = _invoke(getattr(mv_refresh, "reclaim_removed_mvs", None), **common)

    # 2. Detect orphan tables (present in schema, absent from registry).
    shared_data["orphans"] = _invoke(getattr(mv_refresh, "detect_orphans", None), **common)

    # 3. Auto-drop orphans past their grace period.
    shared_data["dropped_orphans"] = _invoke(
        getattr(mv_refresh, "drop_expired_orphans", None), **common
    )


# ---------------------------------------------------------------------------
# Then
# ---------------------------------------------------------------------------


@then("the backing MV table is dropped and any orphaned MV tables are flagged for auto-drop")
def then_table_dropped_and_orphans_flagged(shared_data: dict) -> None:
    executed_sql = shared_data["executed_sql"]
    upper = [s.upper() for s in executed_sql]

    # The reclamation / cleanup pipeline must have issued at least one DROP TABLE.
    drops = [s for s in upper if "DROP TABLE" in s]
    assert drops, f"expected a DROP TABLE statement, got: {executed_sql}"

    # The disabled MV's backing table must be the target of a drop.
    removed_table = shared_data["removed_mv"].target_table
    assert any(removed_table.upper() in s for s in drops), (
        f"removed MV table {removed_table} was not dropped: {executed_sql}"
    )

    # The active MV's backing table must NOT be dropped.
    active_table = shared_data["active_mv"].target_table
    assert not any(
        f"{active_table.upper()} " in s or s.rstrip().endswith(active_table.upper()) for s in drops
    ), f"active MV table {active_table} must not be dropped: {executed_sql}"

    # Orphan detection must surface the ghost table not present in the registry.
    orphans = shared_data["orphans"]
    assert orphans is not None, "detect_orphans returned None"
    orphan_str = " ".join(str(o) for o in orphans)
    assert "mv_ghost" in orphan_str, f"orphan mv_ghost not flagged: {orphans}"

    # The disabled MV must reflect a non-serveable lifecycle status.
    assert shared_data["removed_mv"].status in (
        MVStatus.DISABLED,
        MVStatus.STALE,
    ), f"unexpected status: {shared_data['removed_mv'].status}"
