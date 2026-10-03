# Copyright (c) 2026 Kenneth Stott
# Canary: b58fb9dd-314a-4def-9160-0f15853b7076
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The save-time refusal of a MongoDB change feed its server cannot serve (REQ-1861).

A table that follows its source's change feed (change signal ``native``, its own or its
source's) is saved only when the source's server serves change streams: a standalone server is
refused by name, and a server that cannot be reached is the ordinary connection failure. The
config load applies the same rule (``config_loader._check_change_feeds``).
"""

# Requirements: REQ-1861

from __future__ import annotations

import logging
from types import SimpleNamespace
from typing import Any

from sqlalchemy import select

from provisa.api.admin.types import MutationResult
from provisa.core.schema_org import registered_tables
from provisa.mongodb.change_feed import (
    ChangeStreamsUnavailable,
    follows_change_feed,
    require_change_feed,
)

log = logging.getLogger(__name__)


async def _check(source: Any) -> MutationResult | None:
    try:
        await require_change_feed(source)
    except ChangeStreamsUnavailable as exc:
        return MutationResult(
            success=False, message=str(exc), code=exc.code, params=dict(exc.params)
        )
    except Exception as exc:
        log.exception("source %r: its change-stream support could not be checked", source.id)
        return MutationResult(
            success=False,
            message=f"Source {source.id!r}: connection validation failed: {exc}",
            code="schema.source_connection_failed",
            params={"source": source.id, "error": str(exc)},
        )
    return None


async def source_change_feed_refusal(conn: Any, input: Any) -> MutationResult | None:
    """For a source save: refused when the source is MongoDB and it, or a table of it already
    registered, follows the change feed, and its server serves no change streams."""
    if input.type != "mongodb":
        return None
    rows = await conn.execute_core(
        select(registered_tables.c.change_signal).where(registered_tables.c.source_id == input.id)
    )
    signals = [row.change_signal for row in rows.fetchall()] or [None]
    if not any(follows_change_feed(input.type, s, input.change_signal) for s in signals):
        return None
    return await _check(
        SimpleNamespace(
            id=input.id,
            host=input.host,
            port=input.port,
            username=input.username,
            password=input.password,
        )
    )


async def table_change_feed_refusal(
    conn: Any, source_id: str, table_signal: str | None
) -> MutationResult | None:
    """For a table save: refused when the table follows its MongoDB source's change feed and the
    source's server serves no change streams."""
    from provisa.core.repositories import source as source_repo

    source = await source_repo.get(conn, source_id)
    # A table of a source that is not registered is refused by the save's own source check.
    if source is None or not follows_change_feed(
        source["type"], table_signal, source["change_signal"]
    ):
        return None
    return await _check(
        SimpleNamespace(
            id=source_id,
            host=source["host"],
            port=source["port"],
            username=source["username"],
            password=source["password_ref"],
        )
    )
