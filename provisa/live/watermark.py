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
from typing import TYPE_CHECKING

from sqlalchemy import func, select

from provisa.core.schema_org import live_query_state

if TYPE_CHECKING:
    from provisa.core.database import Connection

# Requirements: REQ-283, REQ-286, REQ-287

log = logging.getLogger(__name__)


async def get_watermark(conn: "Connection", source: str, output_type: str) -> str | None:  # REQ-287
    result = await conn.execute_core(
        select(live_query_state.c.last_watermark).where(
            live_query_state.c.source == source,
            live_query_state.c.output_type == output_type,
        )
    )
    row = result.fetchone()
    return row._mapping["last_watermark"] if row is not None else None


async def set_watermark(  # REQ-283, REQ-286, REQ-287
    conn: "Connection", source: str, output_type: str, value: str, status: str = "active"
) -> None:
    await conn.upsert(
        live_query_state,
        {
            "source": source,
            "output_type": output_type,
            "last_watermark": value,
            "last_polled_at": func.now(),
            "status": status,
        },
        index_elements=["source", "output_type"],
        update_columns=["last_watermark", "status"],
        set_extra={"last_polled_at": func.now()},
    )
    log.debug(
        "[LIVE] watermark updated: source=%s output_type=%s value=%s",
        source,
        output_type,
        value,
    )
