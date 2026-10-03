# Copyright (c) 2026 Kenneth Stott
# Canary: 0c5e8a37-2b19-4f64-9d7e-a1f3b6c8e254
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What follows a write to a registered table, whoever made it.

A table is written by a mutation compiled against it, or by a command that writes it -- a
source's write operation registered with the table it writes (REQ-1924, REQ-871). Either way
the same things follow: what is held of the table's rows stops being served."""

# Requirements: REQ-080, REQ-084, REQ-172, REQ-176, REQ-1915, REQ-1924

from __future__ import annotations

from typing import Any

from provisa.core.connection_loop import spawn_background


async def after_table_written(
    state: Any,
    *,
    table_id: int,
    table_name: str,
    source_id: str,
) -> None:
    """Drop the table's cached responses (REQ-080), mark the materialized views over it stale
    (REQ-084), announce the change (REQ-172), run its sinks (REQ-176), ask for a build of its
    whole-table replica (REQ-1915), and reload it when it is held hot."""
    from provisa.cache.tenancy import invalidate_tables
    from provisa.kafka.change_events import emit_change_event
    from provisa.kafka.sink_executor import trigger_sinks_for_table

    # REQ-595: the acting org's entries — the tenant they were written under.
    await invalidate_tables(state, [table_id])
    state.mv_registry.mark_stale(table_name)
    emit_change_event(table_name, source_id)
    spawn_background(trigger_sinks_for_table(table_name, state))
    await _request_replica_build(state, table_id, source_id)
    if state.hot_manager is None:
        return
    from provisa.cache.hot_tables import HotTableManager

    hot_mgr = state.hot_manager
    assert isinstance(hot_mgr, HotTableManager)
    # Reloaded with the key and address it was hot under (a table not hot is left alone).
    await hot_mgr.refresh_after_write(state.federation_engine, table_id)


async def _request_replica_build(state: Any, table_id: int, source_id: str) -> None:
    """REQ-1915, REQ-1924: a table read from its whole-table replica is out of date once it is
    written, so a build of the replica is asked for. Readers keep the old replica until the new
    one swaps in. The table is judged as a read judges it (``query_residency``): it is served
    from a replica when the operator's settings put it there or the engine cannot read its
    source in place, and only a whole copy is built -- a table replicated row by row, or one
    with a parameter column, has no whole to rebuild."""
    from provisa.core.request_context import current_org
    from provisa.federation import replica_builds, replica_state
    from provisa.federation.registry_view import registered_sources, registered_tables
    from provisa.federation.replica_converge import whole_copy
    from provisa.federation.strategy import engine_attaches

    engine = state.federation_engine
    source = {s.id: s for s in await registered_sources(state)}.get(source_id)
    if source is None:
        return  # a built-in source (registry_view.registered_sources): never landed, no replica
    table = {t.id: t for t in await registered_tables(state)}[table_id]
    served_from_replica = table.id in state.replica_routes.floored or not engine_attaches(
        engine, source.type.value
    )
    if not (served_from_replica and whole_copy(source, table, engine)):
        return
    async with state.tenant_db.acquire() as conn:
        requested = await replica_state.request_build(
            conn,
            (source.id, table.schema_name, table.table_name),
            replica_state.REASON_WRITE,
        )
    if requested:
        replica_builds.kick(current_org.get(None))
