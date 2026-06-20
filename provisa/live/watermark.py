# Copyright (c) 2026 Kenneth Stott
# Canary: b2c3d4e5-f6a7-8901-bcde-f12345678901
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Watermark persistence for live queries (Phase AM, Phase AY).

Each live output tracks the maximum value seen in its watermark column.
State is persisted in live_query_state keyed by (source, output_type).

Schema:
    CREATE TABLE IF NOT EXISTS live_query_state (
        source          TEXT NOT NULL,
        output_type     TEXT NOT NULL,
        last_watermark  TEXT,
        last_polled_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        status          TEXT NOT NULL DEFAULT 'active',
        PRIMARY KEY (source, output_type)
    );
"""

from __future__ import annotations

import logging

log = logging.getLogger(__name__)


async def get_watermark(conn, source: str, output_type: str) -> str | None:
    row = await conn.fetchrow(
        "SELECT last_watermark FROM live_query_state WHERE source = $1 AND output_type = $2",
        source,
        output_type,
    )
    return row["last_watermark"] if row else None


async def set_watermark(
    conn, source: str, output_type: str, value: str, status: str = "active"
) -> None:
    await conn.execute(
        """
        INSERT INTO live_query_state (source, output_type, last_watermark, last_polled_at, status)
        VALUES ($1, $2, $3, NOW(), $4)
        ON CONFLICT (source, output_type)
        DO UPDATE SET last_watermark = EXCLUDED.last_watermark,
                      last_polled_at = NOW(),
                      status = EXCLUDED.status
        """,
        source,
        output_type,
        value,
        status,
    )
    log.debug(
        "[LIVE] watermark updated: source=%s output_type=%s value=%s",
        source,
        output_type,
        value,
    )
