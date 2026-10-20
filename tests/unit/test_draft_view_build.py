# Copyright (c) 2026 Kenneth Stott
# Canary: 22eca13e-693a-4e8e-a6a2-5fb53c50a2c1
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A draft view's build is kept and never scheduled; its steward's preview asks for it (REQ-1921).

A draft view is offered to no reader: no statement expands it. A materialized one stays
registered with its build disabled -- the scheduler and the rewriter skip a disabled build, and
the reclamation sweep keeps a registered build's stored table -- so a view set back to draft
keeps what it has built. Its steward's preview starts the build and does not wait for it."""

# Requirements: REQ-1921

from __future__ import annotations

import asyncio
import threading
from types import SimpleNamespace

import pytest

from provisa.api.admin import table_read
from provisa.mv.models import MVDefinition, MVStatus
from provisa.mv.registry import MVRegistry

pytestmark = pytest.mark.unit


# --- the rebuild: registered, disabled, expanded for nobody -------------------------------------


@pytest.fixture
def model(control_plane):
    from sqlalchemy import insert

    from provisa.core.schema_org import domains, sources

    with control_plane.engine().begin() as c:
        c.exec_driver_sql('SET LOCAL search_path TO "org_default"')
        c.execute(insert(domains).values(id="sales", description=""))
        c.execute(insert(sources).values(id="__derived__", type="derived"))
    return control_plane.org()


async def _save_view(conn, name: str, *, draft: bool, materialize: bool) -> None:
    from sqlalchemy import insert, update

    from provisa.core.schema_org import registered_tables

    existing = (
        await conn.execute_core(
            update(registered_tables)
            .where(registered_tables.c.table_name == name)
            .values(draft=draft, materialize=materialize)
            .returning(registered_tables.c.id)
        )
    ).fetchall()
    if not existing:
        await conn.execute_core(
            insert(registered_tables).values(
                source_id="__derived__",
                domain_id="sales",
                schema_name="views",
                table_name=name,
                view_sql="SELECT id FROM orders",
                draft=draft,
                materialize=materialize,
            )
        )


@pytest.fixture
def rebuilt(model, monkeypatch):
    """The real registration step of a rebuild, over the real registry and the model store."""
    import provisa.api.app as appmod
    from provisa.api import app_rebuild

    state = appmod.state
    runtime_engine = SimpleNamespace(
        materialize_store_target=lambda org: ("store", "views"),
        engine=SimpleNamespace(catalog_qualified=True),  # REQ-1730: it names catalogs
    )
    monkeypatch.setattr(state, "mv_registry", MVRegistry(), raising=False)
    monkeypatch.setattr(state, "view_sql_map", {}, raising=False)
    monkeypatch.setattr(state, "bitemporal_view_reads", {}, raising=False)
    monkeypatch.setattr(state, "federation_engine", runtime_engine, raising=False)
    monkeypatch.setattr(state, "view_context", object(), raising=False)
    monkeypatch.setattr(app_rebuild, "compile_view_sql_to_physical", lambda sql, ctx: sql)

    async def _rebuild(name: str, *, draft: bool, materialize: bool = True):
        async with model.acquire() as conn:
            await _save_view(conn, name, draft=draft, materialize=materialize)
            await app_rebuild._register_user_views_in_state(conn, None, [], [])
        return state

    return _rebuild


async def test_a_materialized_draft_view_stays_registered_with_its_build_disabled(rebuilt):
    state = await rebuilt("big_orders", draft=True)
    mv = state.mv_registry.get("view-big_orders")
    assert mv is not None and mv.enabled is False
    # No statement expands it, and nothing schedules or reads its build.
    assert "big_orders" not in state.view_sql_map
    assert state.mv_registry.get_enabled() == []
    assert state.mv_registry.get_due_for_refresh() == []
    assert state.mv_registry.get_fresh() == []
    # The reclamation sweep keeps the stored table of every registered build.
    assert [m.target_table for m in state.mv_registry.all()] == ["mv_big_orders"]


async def test_a_view_set_back_to_draft_keeps_its_build_and_its_release_enables_it(rebuilt):
    state = await rebuilt("big_orders", draft=False)
    released = state.mv_registry.get("view-big_orders")
    assert released.enabled is True and "big_orders" in state.view_sql_map
    released.status, released.last_refresh_at = MVStatus.FRESH, 1.0  # built

    state = await rebuilt("big_orders", draft=True)
    kept = state.mv_registry.get("view-big_orders")
    assert kept is released, "the same registration, with what it has built"
    assert kept.enabled is False and kept.last_refresh_at == 1.0
    assert "big_orders" not in state.view_sql_map
    assert state.mv_registry.get_fresh() == []

    state = await rebuilt("big_orders", draft=False)
    assert state.mv_registry.get("view-big_orders") is released and released.enabled is True
    assert "big_orders" in state.view_sql_map


async def test_a_draft_view_that_is_not_materialized_has_no_build(rebuilt):
    state = await rebuilt("big_orders", draft=True, materialize=False)
    assert state.mv_registry.get("view-big_orders") is None
    assert "big_orders" not in state.view_sql_map


# --- the preview: starts the build, waits for nothing it does not need ---------------------------


def _draft_view(**over) -> dict:
    return {"id": 10, "table_name": "big_orders", "view_sql": "SELECT id FROM orders;", **over}


def _registered(**over) -> MVDefinition:
    return MVDefinition(
        id="view-big_orders",
        source_tables=[],
        target_catalog="store",
        target_schema="views",
        target_table="mv_big_orders",
        enabled=False,
        status=MVStatus.STALE,
        **over,
    )


@pytest.fixture
def builds(monkeypatch) -> list:
    """The id of every view whose build a preview started."""
    started: list = []

    async def _refresh(engine, mv, registry, *, store, ledger):
        started.append(mv.id)

    def _detached(coro, *, name):
        # A worker of its own, as spawn_background's: the build runs to its end off this loop.
        worker = threading.Thread(target=asyncio.run, args=(coro,))
        worker.start()
        worker.join()

    monkeypatch.setattr("provisa.mv.refresh.refresh_mv", _refresh)
    monkeypatch.setattr("provisa.core.connection_loop.spawn_background", _detached)
    # The body is compiled to the engine's catalogs as a published view's is.
    monkeypatch.setattr(
        "provisa.api.app_rebuild.compile_view_sql_to_physical",
        lambda sql, ctx: f"/*physical*/ {sql}",
    )
    return started


def _state(mv: MVDefinition | None) -> SimpleNamespace:
    registry = MVRegistry()
    if mv is not None:
        registry.register(mv)
    return SimpleNamespace(
        mv_registry=registry,
        federation_engine=object(),
        model_db=object(),
        tenant_db=object(),
        view_context=object(),
    )


async def test_a_table_and_a_plain_view_start_no_build(builds):
    assert table_read._view_body(_state(None), {"id": 9, "table_name": "refunds"}) is None
    assert (
        table_read._view_body(_state(None), _draft_view()) == "/*physical*/ SELECT id FROM orders"
    )
    await asyncio.sleep(0)
    assert builds == []


async def test_a_materialized_draft_views_preview_starts_its_build_and_reads_live(builds):
    body = table_read._view_body(_state(_registered()), _draft_view())
    assert body == "/*physical*/ SELECT id FROM orders"  # answered now, from its inputs
    await asyncio.sleep(0)
    assert builds == ["view-big_orders"]


@pytest.mark.parametrize("status", [MVStatus.FRESH, MVStatus.REFRESHING])
async def test_a_build_that_is_fresh_or_running_is_not_started_again(builds, status):
    mv = _registered()
    mv.status = status
    assert table_read._view_body(_state(mv), _draft_view()) == "/*physical*/ SELECT id FROM orders"
    await asyncio.sleep(0)
    assert builds == []


async def test_a_bitemporal_draft_view_answers_as_building_until_its_log_exists(builds):
    from provisa.federation.replica_state import ReplicaBuilding
    from provisa.mv.bitemporal import BitemporalSpec, view_read_sql

    # REQ-2109: a bitemporal view's reads travel on its valid time.
    spec = BitemporalSpec(key=("id",), columns=("id", "day"), valid_column="day")
    mv = _registered(bitemporal=spec)
    state = _state(mv)
    with pytest.raises(ReplicaBuilding) as building:
        table_read._view_body(state, _draft_view())
    assert building.value.replica == "big_orders"
    await asyncio.sleep(0)
    assert builds == ["view-big_orders"]

    mv.status, mv.last_refresh_at = MVStatus.FRESH, 1.0  # the build the preview started, done
    stored = '"store"."views"."mv_big_orders"'
    assert table_read._view_body(state, _draft_view()) == view_read_sql(stored, spec)
    await asyncio.sleep(0)
    assert builds == ["view-big_orders"]  # and not started again
