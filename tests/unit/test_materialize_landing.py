# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-932: the three landing shapes — replace, append, CDC (upsert + tombstone)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pytest

from provisa.federation.materialize_exec import (
    apply_cdc_events_into_pg,
    append_rows_into_pg,
    land_rows_into_pg,
)


@dataclass
class _Event:
    operation: str
    row: dict[str, Any]


class _RecordingConn:
    """Records every execute(sql, *args) call."""

    def __init__(self):
        self.calls: list[tuple[str, tuple]] = []

    async def execute(self, sql, *args):
        self.calls.append((sql, args))

    def sql_at(self, i):
        return self.calls[i][0]

    @property
    def sqls(self):
        return [c[0] for c in self.calls]


COLUMNS = [("id", "bigint"), ("status", "text")]


@pytest.mark.asyncio
async def test_replace_drops_creates_inserts():
    conn = _RecordingConn()
    await land_rows_into_pg(
        conn, schema="mat", table="pets", columns=COLUMNS, rows=[{"id": 1, "status": "new"}]
    )
    joined = " | ".join(conn.sqls)
    assert "DROP TABLE IF EXISTS" in joined  # replace = full refresh
    assert 'CREATE TABLE "mat"."pets"' in joined
    insert = conn.calls[-1]
    assert insert[0].startswith('INSERT INTO "mat"."pets" ("id", "status") VALUES ($1, $2)')
    assert insert[1] == (1, "new")


@pytest.mark.asyncio
async def test_append_does_not_drop():
    conn = _RecordingConn()
    await append_rows_into_pg(
        conn, schema="mat", table="pets", columns=COLUMNS, rows=[{"id": 2, "status": "sold"}]
    )
    joined = " | ".join(conn.sqls)
    assert "DROP TABLE" not in joined  # append never truncates
    assert "CREATE TABLE IF NOT EXISTS" in joined
    assert conn.calls[-1][1] == (2, "sold")


@pytest.mark.asyncio
async def test_append_empty_rows_creates_only():
    conn = _RecordingConn()
    await append_rows_into_pg(conn, schema="mat", table="pets", columns=COLUMNS, rows=[])
    assert not any(s.startswith("INSERT") for s in conn.sqls)


@pytest.mark.asyncio
async def test_cdc_requires_pk():
    conn = _RecordingConn()
    with pytest.raises(ValueError, match="requires primary key"):
        await apply_cdc_events_into_pg(
            conn, schema="mat", table="pets", columns=COLUMNS, pk_columns=[], events=[]
        )


@pytest.mark.asyncio
async def test_cdc_upsert_and_delete_by_pk():
    conn = _RecordingConn()
    events = [
        _Event("insert", {"id": 1, "status": "new"}),
        _Event("update", {"id": 1, "status": "pending"}),
        _Event("delete", {"id": 1, "status": None}),
    ]
    counts = await apply_cdc_events_into_pg(
        conn, schema="mat", table="pets", columns=COLUMNS, pk_columns=["id"], events=events
    )
    assert counts == {"upsert": 2, "delete": 1}
    # table created WITH a primary key (needed for ON CONFLICT)
    assert any('PRIMARY KEY ("id")' in s for s in conn.sqls)
    # insert/update → upsert on the PK, updating non-PK columns
    upsert_calls = [c for c in conn.calls if c[0].startswith("INSERT")]
    assert len(upsert_calls) == 2
    assert 'ON CONFLICT ("id") DO UPDATE SET "status" = EXCLUDED."status"' in upsert_calls[0][0]
    assert upsert_calls[1][1] == (1, "pending")
    # delete → tombstone by PK
    delete_call = next(c for c in conn.calls if c[0].startswith("DELETE"))
    assert delete_call[0] == 'DELETE FROM "mat"."pets" WHERE "id" = $1'
    assert delete_call[1] == (1,)


@pytest.mark.asyncio
async def test_cdc_all_pk_columns_do_nothing():
    conn = _RecordingConn()
    cols = [("id", "bigint")]
    await apply_cdc_events_into_pg(
        conn,
        schema="mat",
        table="k",
        columns=cols,
        pk_columns=["id"],
        events=[_Event("insert", {"id": 7})],
    )
    upsert = next(c for c in conn.calls if c[0].startswith("INSERT"))
    assert 'ON CONFLICT ("id") DO NOTHING' in upsert[0]


class _FakeProvider:
    def __init__(self, events):
        self._events = events
        self.closed = False

    async def watch(self, table, filter_expr=None):
        for ev in self._events:
            yield ev

    async def close(self):
        self.closed = True


@pytest.mark.asyncio
async def test_cdc_landing_consumer_drains_provider():
    import asyncio

    from provisa.subscriptions.cdc_landing import consume_cdc_into_store

    events = [
        _Event("insert", {"id": 1, "status": "new"}),
        _Event("update", {"id": 1, "status": "pending"}),
        _Event("delete", {"id": 1, "status": None}),
    ]
    provider = _FakeProvider(events)
    conn = _RecordingConn()

    totals = await consume_cdc_into_store(
        provider,
        conn,
        schema="mat",
        table="pets",
        columns=COLUMNS,
        pk_columns=["id"],
        disconnect=asyncio.Event(),
    )
    assert totals == {"upsert": 2, "delete": 1}
    assert provider.closed
    assert any(s.startswith("DELETE") for s in conn.sqls)
