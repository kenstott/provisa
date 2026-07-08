# Copyright (c) 2026 Kenneth Stott
# Canary: 8f3c2a17-6b40-4d19-9e52-1a7c0d3b8e64
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""CDC landing consumer (REQ-932): apply a provider's change stream to a landed table.

When a table's change_signal is a push signal (debezium/kafka) and its source is materialized,
the provider's ChangeEvents are applied to the landed copy by primary key — upsert on
insert/update, tombstone on delete. This is the streaming counterpart to the periodic
replace/append refresh, and the only landing path that carries hard deletes.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from provisa.federation.materialize_exec import apply_cdc_events_into_pg

log = logging.getLogger(__name__)


async def consume_cdc_into_store(
    provider,
    conn: Any,
    *,
    schema: str,
    table: str,
    columns: list[tuple[str, str]],
    pk_columns: list[str],
    disconnect: asyncio.Event,
) -> dict[str, int]:
    """Drain ``provider.watch(table)`` into the landed ``schema.table``, applying each event by PK.

    Returns cumulative {upsert, delete} counts. Stops when ``disconnect`` is set or the provider
    stream ends; always closes the provider. A primary key is required (enforced downstream)."""
    totals = {"upsert": 0, "delete": 0}
    try:
        async for event in provider.watch(table):
            if disconnect.is_set():
                break
            counts = await apply_cdc_events_into_pg(
                conn,
                schema=schema,
                table=table,
                columns=columns,
                pk_columns=pk_columns,
                events=[event],
            )
            totals["upsert"] += counts["upsert"]
            totals["delete"] += counts["delete"]
        log.info(
            "CDC landing %s.%s: applied %d upserts, %d deletes",
            schema,
            table,
            totals["upsert"],
            totals["delete"],
        )
    finally:
        await provider.close()
    return totals
