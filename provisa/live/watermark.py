# Copyright (c) 2026 Kenneth Stott
# Canary: b2c3d4e5-f6a7-8901-bcde-f12345678901
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Watermark persistence for live queries (Phase AM).

Each live query tracks the maximum value seen in its watermark column.
The state is persisted in the ``live_query_state`` PostgreSQL table so that
Provisa instances can resume polling after a restart without replaying rows.

Schema (created by ``provisa.core.db.init_schema``):

    CREATE TABLE IF NOT EXISTS live_query_state (
        query_id    TEXT PRIMARY KEY,
        watermark   TEXT,
        updated_at  TIMESTAMPTZ DEFAULT NOW()
    );
"""

from __future__ import annotations

import logging

log = logging.getLogger(__name__)


async def get_watermark(conn, query_id: str) -> str | None:
    """Return the persisted watermark value for *query_id*, or None if absent."""
    row = await conn.fetchrow(
        "SELECT watermark FROM live_query_state WHERE query_id = $1",
        query_id,
    )
    return row["watermark"] if row else None


async def set_watermark(conn, query_id: str, value: str) -> None:
    """Upsert the watermark for *query_id*."""
    await conn.execute(
        """
        INSERT INTO live_query_state (query_id, watermark, updated_at)
        VALUES ($1, $2, NOW())
        ON CONFLICT (query_id)
        DO UPDATE SET watermark = EXCLUDED.watermark, updated_at = NOW()
        """,
        query_id,
        value,
    )
    log.debug("[LIVE] watermark updated: query_id=%s value=%s", query_id, value)
