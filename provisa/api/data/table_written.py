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


async def after_table_written(
    state: Any,
    *,
    table_id: int,
    table_name: str,
    source_id: str,
) -> None:
    """Drop the table's cached responses (REQ-080), mark the materialized views over it stale
    (REQ-084), announce the change (REQ-172), ask for a build of its whole-table replica
    (REQ-1915), and reload it when it is held hot. A table publishes its new rows to Kafka through
    a live output (live.outputs, REQ-565), not here."""
    from provisa.cache.tenancy import invalidate_tables
    from provisa.kafka.change_events import emit_change_event

    # REQ-595: the acting org's entries — the tenant they were written under.
    await invalidate_tables(state, [table_id])
    state.mv_registry.mark_stale(table_name)
    emit_change_event(table_name, source_id)
    # REQ-1915, REQ-1924: a table read from its whole-table replica is out of date once it is
    # written, so a build of the replica is asked for. Readers keep the old replica until the new
    # one swaps in.
    from provisa.scheduler.table_triggers import source_changed
    from provisa.federation import replica_state

    # REQ-2054: and the commands that run when it changes run -- when its new replica lands, or
    # now for a table read live.
    await source_changed(state, table_id, source_id, replica_state.REASON_WRITE)
    if state.hot_manager is None:
        return
    from provisa.cache.hot_tables import HotTableManager

    hot_mgr = state.hot_manager
    assert isinstance(hot_mgr, HotTableManager)
    # Reloaded with the key and address it was hot under (a table not hot is left alone).
    await hot_mgr.refresh_after_write(state.federation_engine, table_id)
