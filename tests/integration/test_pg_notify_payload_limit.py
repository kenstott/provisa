# Copyright (c) 2026 Kenneth Stott
# Canary: 8f2c1d64-9a3e-4c07-8a1b-2f5d6c9e10b4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A subscription trigger must never abort the write it observes (REQ-258)."""

# Requirements: REQ-258

from __future__ import annotations

import asyncio
import json
import os
import uuid

import asyncpg
import pytest

from provisa.subscriptions.pg_provider import CHANNEL_PREFIX
from provisa.subscriptions.pg_triggers import MAX_NOTIFY_BYTES, _trigger_sql

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


@pytest.fixture
async def notified_table(docker_postgres):
    """A throwaway table carrying the REQ-258 notify trigger."""
    conn = await asyncpg.connect(
        host=docker_postgres["host"],
        port=docker_postgres["port"],
        user="provisa",
        password=os.environ.get("PG_PASSWORD", "provisa"),
        database="provisa",
    )
    schema = f"notify_{uuid.uuid4().hex[:8]}"
    table = f"t_{uuid.uuid4().hex[:8]}"
    await conn.execute(f"CREATE SCHEMA {schema}")
    await conn.execute(f"CREATE TABLE {schema}.{table} (id serial primary key, body text)")
    await conn.execute(_trigger_sql(schema, table))
    try:
        yield conn, schema, table
    finally:
        await conn.execute(f"DROP SCHEMA {schema} CASCADE")
        await conn.close()


async def _listen(conn, table: str) -> asyncio.Queue:
    queue: asyncio.Queue = asyncio.Queue()
    await conn.add_listener(
        f"{CHANNEL_PREFIX}{table}", lambda _c, _pid, _ch, payload: queue.put_nowait(payload)
    )
    return queue


async def test_row_larger_than_the_notify_limit_is_truncated_not_refused(notified_table):
    # REQ-1515: NOTIFY refuses a payload of 8000 bytes or more. The trigger fires inside the
    # writer's transaction, so a refusal used to abort the INSERT — a query_audit_log row
    # holding a long statement failed the write and surfaced as an Internal Server Error.
    conn, schema, table = notified_table
    queue = await _listen(conn, table)

    body = "x" * (MAX_NOTIFY_BYTES * 2)
    await conn.execute(f"INSERT INTO {schema}.{table} (body) VALUES ($1)", body)

    stored = await conn.fetchval(f"SELECT body FROM {schema}.{table}")
    assert stored == body

    payload = await asyncio.wait_for(queue.get(), timeout=5)
    assert len(payload.encode()) <= MAX_NOTIFY_BYTES
    parsed = json.loads(payload)
    assert parsed["op"] == "insert"
    assert parsed["truncated"] is True
    # The leading columns survive: the truncated text is the head of the row's own JSON.
    assert parsed["row_text"].startswith('{"id"')
    assert "row" not in parsed


async def test_multibyte_row_is_truncated_under_the_byte_limit(notified_table):
    # REQ-1515: the limit is bytes, not characters. A row of four-byte characters must still
    # produce a payload PostgreSQL accepts.
    conn, schema, table = notified_table
    queue = await _listen(conn, table)

    await conn.execute(
        f"INSERT INTO {schema}.{table} (body) VALUES ($1)", "\U0001F600" * MAX_NOTIFY_BYTES
    )

    payload = await asyncio.wait_for(queue.get(), timeout=5)
    assert len(payload.encode()) <= MAX_NOTIFY_BYTES
    assert json.loads(payload)["truncated"] is True


async def test_row_within_the_limit_carries_its_body(notified_table):
    # REQ-258: the row travels with the notification whenever it fits — the oversized envelope
    # is the exception, not the shape every subscriber sees.
    conn, schema, table = notified_table
    queue = await _listen(conn, table)

    await conn.execute(f"INSERT INTO {schema}.{table} (body) VALUES ($1)", "small")

    parsed = json.loads(await asyncio.wait_for(queue.get(), timeout=5))
    assert parsed["op"] == "insert"
    assert parsed["row"]["body"] == "small"
