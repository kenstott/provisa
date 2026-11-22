# Copyright (c) 2026 Kenneth Stott
# Canary: 22eca13e-693a-4e8e-a6a2-5fb53c50a2c1
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A withdrawn view's build is kept and never scheduled; its steward's preview asks for it
(REQ-1921).

A withdrawn view is offered to no reader: no statement expands it. A materialized one stays
registered with its build disabled -- the scheduler and the rewriter skip a disabled build, and
the reclamation sweep keeps a registered build's stored table -- so a withdrawn view keeps what it
has built. Its steward's preview starts the build and does not wait for it."""

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


async def _save_view(
    conn, name: str, *, withdrawn: bool, materialize: bool, view_sql: str = "SELECT id FROM orders"
) -> None:
    from sqlalchemy import insert, update

    from provisa.core.schema_org import registered_tables

    existing = (
        await conn.execute_core(
            update(registered_tables)
            .where(registered_tables.c.table_name == name)
            .values(withdrawn=withdrawn, materialize=materialize)
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
                view_sql=view_sql,
                withdrawn=withdrawn,
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
    monkeypatch.setattr(state, "stored_view_reads", set(), raising=False)
    monkeypatch.setattr(state, "federation_engine", runtime_engine, raising=False)
    monkeypatch.setattr(state, "view_context", object(), raising=False)
    monkeypatch.setattr(app_rebuild, "compile_view_sql_to_physical", lambda sql, ctx: sql)

    async def _rebuild(name: str, *, withdrawn: bool, materialize: bool = True, **saved):
        async with model.acquire() as conn:
            await _save_view(conn, name, withdrawn=withdrawn, materialize=materialize, **saved)
            await app_rebuild._register_user_views_in_state(conn, None, [], [])
        return state

    return _rebuild


async def test_a_materialized_withdrawn_view_stays_registered_with_its_build_disabled(rebuilt):
    state = await rebuilt("big_orders", withdrawn=True)
    mv = state.mv_registry.get("view-big_orders")
    assert mv is not None and mv.enabled is False
    # No statement expands it, and nothing schedules or reads its build.
    assert "views.big_orders" not in state.view_sql_map
    assert state.mv_registry.get_enabled() == []
    assert state.mv_registry.get_due_for_refresh() == []
    assert state.mv_registry.get_fresh() == []
    # The reclamation sweep keeps the stored table of every registered build.
    assert [m.target_table for m in state.mv_registry.all()] == ["mv_big_orders"]


async def test_a_withdrawn_view_keeps_its_build_and_putting_it_back_enables_it(rebuilt):
    state = await rebuilt("big_orders", withdrawn=False)
    put_back = state.mv_registry.get("view-big_orders")
    assert put_back.enabled is True and "views.big_orders" in state.view_sql_map
    put_back.status, put_back.last_refresh_at = MVStatus.FRESH, 1.0  # built

    state = await rebuilt("big_orders", withdrawn=True)
    kept = state.mv_registry.get("view-big_orders")
    assert kept is put_back, "the same registration, with what it has built"
    assert kept.enabled is False and kept.last_refresh_at == 1.0
    assert "views.big_orders" not in state.view_sql_map
    assert state.mv_registry.get_fresh() == []

    state = await rebuilt("big_orders", withdrawn=False)
    assert state.mv_registry.get("view-big_orders") is put_back and put_back.enabled is True
    assert "views.big_orders" in state.view_sql_map


async def test_a_view_is_keyed_by_its_schema_and_name(rebuilt):
    """A reference is the view only under the view's own schema: the view map keys it
    ``schema.table``, never by its name alone, which a table under another schema may have."""
    from provisa.compiler.view_expand import expand_view_refs

    state = await rebuilt("big_orders", withdrawn=False, materialize=False)
    assert set(state.view_sql_map) == {"views.big_orders"}
    assert "orders" in expand_view_refs("SELECT * FROM views.big_orders", state.view_sql_map)
    other = "SELECT * FROM otherschema.big_orders"
    assert expand_view_refs(other, state.view_sql_map) == other
    assert expand_view_refs(other, state.view_sql_map, mark=True) == other


async def test_a_deleted_view_leaves_no_build_registered(rebuilt, model):
    """REQ-1918: a view's build is a part of it and goes with it. A view over another, both
    deleted, leaves neither registered -- a build left reading a view that is gone failed the
    org's event wiring on every later change."""
    from sqlalchemy import delete

    from provisa.core.schema_org import registered_tables

    await rebuilt("e2e_mv_a", withdrawn=False)
    state = await rebuilt("e2e_mv_b", withdrawn=False, view_sql="SELECT id FROM views.e2e_mv_a")
    assert {m.id for m in state.mv_registry.all()} == {"view-e2e_mv_a", "view-e2e_mv_b"}
    async with model.acquire() as conn:
        await conn.execute_core(
            delete(registered_tables).where(registered_tables.c.table_name == "e2e_mv_b")
        )
    state = await rebuilt("e2e_mv_a", withdrawn=False)
    assert {m.id for m in state.mv_registry.all()} == {"view-e2e_mv_a"}
    async with model.acquire() as conn:
        await conn.execute_core(
            delete(registered_tables).where(registered_tables.c.table_name == "e2e_mv_a")
        )
        from provisa.api import app_rebuild

        await app_rebuild._register_user_views_in_state(conn, None, [], [])
    assert state.mv_registry.all() == []
    assert "views.e2e_mv_a" not in state.view_sql_map and "views.e2e_mv_b" not in state.view_sql_map


async def test_a_materialized_view_not_built_from_a_view_survives_whatever_its_id(rebuilt):
    """What is unregistered is decided by what a build was made from, never by its name: a
    materialized view declared in the config is no view's build, so a rebuild keeps it, even one
    whose id reads like a view's."""
    state = await rebuilt("big_orders", withdrawn=False)
    declared = MVDefinition(
        id="view-declared_in_config",
        source_tables=["orders"],
        target_catalog="store",
        target_schema="views",
        target_table="declared_in_config",
        sql="SELECT id FROM orders",
    )
    state.mv_registry.register(declared)
    state = await rebuilt("big_orders", withdrawn=False)
    assert state.mv_registry.get("view-declared_in_config") is declared
    assert state.mv_registry.get("view-big_orders").view_table == "big_orders"


async def test_a_view_no_longer_materialized_has_no_build(rebuilt):
    state = await rebuilt("big_orders", withdrawn=False)
    assert state.mv_registry.get("view-big_orders") is not None
    state = await rebuilt("big_orders", withdrawn=False, materialize=False)
    assert state.mv_registry.get("view-big_orders") is None
    assert "views.big_orders" in state.view_sql_map  # still read, through its SQL


async def test_a_withdrawn_view_that_is_not_materialized_has_no_build(rebuilt):
    state = await rebuilt("big_orders", withdrawn=True, materialize=False)
    assert state.mv_registry.get("view-big_orders") is None
    assert "views.big_orders" not in state.view_sql_map


# --- the preview: starts the build, waits for nothing it does not need ---------------------------


def _withdrawn_view(**over) -> dict:
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
        table_read._view_body(_state(None), _withdrawn_view())
        == "/*physical*/ SELECT id FROM orders"
    )
    await asyncio.sleep(0)
    assert builds == []


async def test_a_materialized_withdrawn_views_preview_starts_its_build_and_reads_live(builds):
    body = table_read._view_body(_state(_registered()), _withdrawn_view())
    assert body == "/*physical*/ SELECT id FROM orders"  # answered now, from its inputs
    await asyncio.sleep(0)
    assert builds == ["view-big_orders"]


@pytest.mark.parametrize("status", [MVStatus.FRESH, MVStatus.REFRESHING])
async def test_a_build_that_is_fresh_or_running_is_not_started_again(builds, status):
    mv = _registered()
    mv.status = status
    assert (
        table_read._view_body(_state(mv), _withdrawn_view()) == "/*physical*/ SELECT id FROM orders"
    )
    await asyncio.sleep(0)
    assert builds == []


async def test_a_bitemporal_withdrawn_view_answers_as_building_until_its_log_exists(builds):
    from provisa.federation.replica_state import ReplicaBuilding
    from provisa.mv.bitemporal import BitemporalSpec, view_read_sql

    # REQ-2109: a bitemporal view's reads travel on its valid time.
    spec = BitemporalSpec(key=("id",), columns=("id", "day"), valid_column="day")
    mv = _registered(bitemporal=spec)
    state = _state(mv)
    with pytest.raises(ReplicaBuilding) as building:
        table_read._view_body(state, _withdrawn_view())
    assert building.value.replica == "big_orders"
    await asyncio.sleep(0)
    assert builds == ["view-big_orders"]

    mv.status, mv.last_refresh_at = MVStatus.FRESH, 1.0  # the build the preview started, done
    stored = '"store"."views"."mv_big_orders"'
    assert table_read._view_body(state, _withdrawn_view()) == view_read_sql(stored, spec)
    await asyncio.sleep(0)
    assert builds == ["view-big_orders"]  # and not started again


async def test_a_view_applying_a_column_transform_is_landed_and_read_from_its_store(rebuilt, model):
    """REQ-2131: a view applying a column transform is built as a materialized view is, though not
    declared one, and is read only from its stored table: its outputs exist nowhere else."""
    from sqlalchemy import insert

    from provisa.core.schema_org import tracked_functions

    async with model.acquire() as conn:
        await conn.execute_core(
            insert(tracked_functions).values(
                name="clean_address",
                source_id="addresses",
                function_name="clean_address",
                kind="query",
                impl_kind="code_columns",
                arguments=[{"name": "raw_address", "type": "text"}],
                output_columns=[{"name": "street", "type": "text"}],
            )
        )
    state = await rebuilt(
        "clean_suppliers",
        withdrawn=False,
        materialize=False,
        view_sql="SELECT id, clean_address(raw_address) FROM suppliers",
    )
    mv = state.mv_registry.get("view-clean_suppliers")
    assert mv is not None and mv.enabled is True and mv.target_table == "mv_clean_suppliers"
    assert (
        state.view_sql_map["views.clean_suppliers"]
        == 'SELECT * FROM "store"."views"."mv_clean_suppliers"'
    )
    assert state.stored_view_reads == {"views.clean_suppliers"}


async def test_a_rebuild_publishes_new_maps_and_leaves_the_ones_a_request_walks(rebuilt):
    """A request on another thread walks the view map, the bitemporal reads and the stored reads
    it took while a rebuild runs. The rebuild builds new ones and publishes each in one
    assignment: the ones the request holds never change under it (a dict changed in place under a
    walk fails it with "dictionary changed size during iteration")."""
    state = await rebuilt("big_orders", withdrawn=False, materialize=False)
    held_map = state.view_sql_map
    held_bt = state.bitemporal_view_reads
    held_stored = state.stored_view_reads
    before = dict(held_map)

    state = await rebuilt("big_orders", withdrawn=True, materialize=False)

    assert held_map == before, "the map the request walks is not changed"
    assert "views.big_orders" in held_map
    assert "views.big_orders" not in state.view_sql_map
    assert state.view_sql_map is not held_map
    assert state.bitemporal_view_reads is not held_bt
    assert state.stored_view_reads is not held_stored
