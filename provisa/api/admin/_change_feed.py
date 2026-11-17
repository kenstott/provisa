# Copyright (c) 2026 Kenneth Stott
# Canary: b58fb9dd-314a-4def-9160-0f15853b7076
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The save-time refusal of a change feed its source cannot serve (REQ-1861, REQ-929).

A table that follows its source's change feed (change signal ``native``, its own or its
source's) is saved only when the source can serve it: a standalone MongoDB server, or a
PostgreSQL database without logical replication for Provisa, is refused by name, and a server
that cannot be reached is the ordinary connection failure. The config load applies the same rule
(``config_loader._check_change_feeds``).
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
    CHANGE_FEED_TYPES,
    FEED_REFUSALS,
    follows_change_feed,
    require_change_feed,
)
from provisa.api.engine_error_names import client_text

log = logging.getLogger(__name__)


async def _check(source: Any) -> MutationResult | None:
    try:
        await require_change_feed(source)
    except FEED_REFUSALS as exc:
        return MutationResult(
            success=False, message=str(exc), code=exc.code, params=dict(exc.params)
        )
    except Exception as exc:
        log.exception("source %r: its change-stream support could not be checked", source.id)
        return MutationResult(
            success=False,
            message=f"Source {source.id!r}: connection validation failed: {exc}",
            code="schema.source_connection_failed",
            params={"source": source.id, "error": client_text(str(exc))},
        )
    return None


async def _inherited_kafka_refusal(conn: Any, input: Any) -> MutationResult | None:
    """A source saved on the ``kafka`` signal: each registered table of it that inherits the
    signal names the topic its changes are read from, or the save is refused, naming it."""
    from provisa.core.change_signal import kafka_topic_refusal

    rows = await conn.execute_core(
        select(registered_tables.c.table_name, registered_tables.c.live).where(
            registered_tables.c.source_id == input.id,
            registered_tables.c.change_signal.is_(None),
        )
    )
    for table_name, live in rows.fetchall():
        topic = ((live or {}).get("kafka") or {}).get("topic")
        refused = kafka_topic_refusal(input.type, input.change_signal, topic)
        if refused is not None:
            return MutationResult(
                success=False,
                message=f"Source {input.id!r}, table {table_name!r}: {refused}",
                code="schema.change_signal_no_topic",
                params={"source": input.id, "table": table_name},
            )
    return None


async def source_change_feed_refusal(conn: Any, input: Any) -> MutationResult | None:
    """For a source save: refused when a registered table that inherits the ``kafka`` signal
    names no topic, or when the source follows its own change feed -- itself or a table of it
    already registered -- and cannot serve it."""
    inherited = await _inherited_kafka_refusal(conn, input)
    if inherited is not None:
        return inherited
    if input.type not in CHANGE_FEED_TYPES:
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
            type=input.type,
            host=input.host,
            port=input.port,
            database=input.database,
            username=input.username,
            password=input.password,
        )
    )


async def table_change_feed_refusal(
    conn: Any,
    source_id: str,
    table_signal: str | None,
    kafka_topic: str | None = None,
    table_name: str = "",
) -> MutationResult | None:
    """For a table save: refused when its change signal is ``native`` on a source type whose
    change feed nothing receives, or when it follows its MongoDB source's change feed and the
    source's server serves no change streams."""
    from provisa.core.repositories import source as source_repo

    from provisa.core.change_signal import kafka_topic_refusal, native_signal_refusal

    source = await source_repo.get(conn, source_id)
    # A table of a source that is not registered is refused by the save's own source check.
    if source is None:
        return None
    signal = table_signal if table_signal is not None else source["change_signal"]
    refused = native_signal_refusal(source["type"], signal)
    if refused is not None:
        return MutationResult(
            success=False,
            message=f"Table of source {source_id!r}: {refused}",
            code="schema.change_signal_no_receiver",
            params={"source": source_id, "type": source["type"], "signal": signal},
        )
    no_topic = kafka_topic_refusal(source["type"], signal, kafka_topic)
    if no_topic is not None:
        return MutationResult(
            success=False,
            message=f"Source {source_id!r}, table {table_name!r}: {no_topic}",
            code="schema.change_signal_no_topic",
            params={"source": source_id, "table": table_name},
        )
    if not follows_change_feed(source["type"], table_signal, source["change_signal"]):
        return None
    return await _check(
        SimpleNamespace(
            id=source_id,
            type=source["type"],
            host=source["host"],
            port=source["port"],
            database=source["database"],
            username=source["username"],
            password=source["password_ref"],
        )
    )
