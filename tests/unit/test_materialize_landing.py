# Copyright (c) 2026 Kenneth Stott
# Canary: 1304bea7-2c09-45ad-8bd6-6451659d3ac4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-932: the three landing shapes — replace, append, CDC (upsert + tombstone).

The ops run through the ``Connection`` abstraction with vanilla SQLAlchemy Core (no dialect SQL),
so the fake here records the Core statements executed and the dialect-agnostic ``upsert`` calls.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pytest

from provisa.core.database import Capabilities
from provisa.federation.materialize_exec import (
    _sa_type,
    apply_cdc,
    build_table,
    land_append,
    land_replace,
)


@dataclass
class _Event:
    operation: str
    row: dict[str, Any]


class _Result:
    rowcount = 0

    def fetchone(self):
        return None


class _FakeConn:
    """Records execute_core() Core statements and upsert() calls; renders SQL for assertions."""

    def __init__(self, dialect: str = "postgresql"):
        self.capabilities = Capabilities.for_dialect(dialect)
        self._dialect_name = dialect
        self.stmts: list[Any] = []
        self.upserts: list[tuple[Any, dict, list[str]]] = []
        self.bulk_copies: list[tuple[str, list[dict]]] = []  # (qualified table, rows) per bulk_copy

    async def execute_core(self, stmt):
        self.stmts.append(stmt)
        return _Result()

    async def bulk_copy(self, table, rows):
        # REQ-990: landing inserts go through the bulk-COPY face, not per-row execute_core.
        qualified = f"{table.schema}.{table.name}" if table.schema else table.name
        self.bulk_copies.append((qualified, list(rows)))
        return len(rows)

    async def upsert(self, table, values, *, index_elements, update_columns=None, set_extra=None):
        self.upserts.append((table, dict(values), list(index_elements)))

    def sql(self) -> list[str]:
        from sqlalchemy.dialects import postgresql

        out = []
        for s in self.stmts:
            try:
                out.append(str(s.compile(dialect=postgresql.dialect())))
            except Exception:
                out.append(str(s))
        return out


COLUMNS = [("id", "bigint"), ("status", "text")]


def test_sa_type_maps_and_raises():
    from sqlalchemy import BigInteger, Text

    assert build_table("mat", "t", COLUMNS).c["id"].type.__class__ is BigInteger
    assert build_table("mat", "t", COLUMNS).c["status"].type.__class__ is Text
    # a length qualifier is stripped
    assert _sa_type("varchar(255)").__name__ == "Text"
    with pytest.raises(ValueError, match="not in the IR vocabulary"):
        _sa_type("geography")


@pytest.mark.asyncio
async def test_replace_deletes_and_reinserts():
    conn = _FakeConn()
    table = build_table("mat", "pets", COLUMNS)
    await land_replace(conn, table, [{"id": 1, "status": "new"}])
    joined = " | ".join(conn.sql())
    assert (
        "DROP TABLE" not in joined
    )  # replace = DELETE+INSERT, never drops the reconcile-managed table
    assert "CREATE TABLE" in joined and "IF NOT EXISTS" in joined  # create-if-absent only
    assert "DELETE FROM" in joined  # full refresh of contents
    assert "INSERT INTO" not in joined  # REQ-990: rows land via bulk_copy, not per-row INSERT
    assert conn.bulk_copies == [("mat.pets", [{"id": 1, "status": "new"}])]


@pytest.mark.asyncio
async def test_append_does_not_drop():
    conn = _FakeConn()
    table = build_table("mat", "pets", COLUMNS)
    await land_append(conn, table, [{"id": 2, "status": "sold"}])
    joined = " | ".join(conn.sql())
    assert "DROP TABLE" not in joined  # append never truncates
    assert "CREATE TABLE" in joined and "IF NOT EXISTS" in joined
    assert "INSERT INTO" not in joined  # REQ-990: bulk_copy, not per-row INSERT
    assert conn.bulk_copies == [("mat.pets", [{"id": 2, "status": "sold"}])]


@pytest.mark.asyncio
async def test_append_empty_rows_creates_only():
    conn = _FakeConn()
    table = build_table("mat", "pets", COLUMNS)
    await land_append(conn, table, [])
    assert not any("INSERT INTO" in s for s in conn.sql())
    assert conn.bulk_copies == [("mat.pets", [])]  # bulk_copy called, but with no rows


@pytest.mark.asyncio
async def test_cdc_requires_pk():
    conn = _FakeConn()
    table = build_table("mat", "pets", COLUMNS)
    with pytest.raises(ValueError, match="requires primary key"):
        await apply_cdc(conn, table, [], [])


@pytest.mark.asyncio
async def test_cdc_upsert_and_delete_by_pk():
    conn = _FakeConn()
    table = build_table("mat", "pets", COLUMNS, ("id",))
    events = [
        _Event("insert", {"id": 1, "status": "new"}),
        _Event("update", {"id": 1, "status": "pending"}),
        _Event("delete", {"id": 1, "status": None}),
    ]
    counts = await apply_cdc(conn, table, ["id"], events)
    assert counts == {"upsert": 2, "delete": 1}
    # insert/update → dialect-agnostic upsert on the PK
    assert len(conn.upserts) == 2
    assert conn.upserts[0][2] == ["id"]
    assert conn.upserts[1][1] == {"id": 1, "status": "pending"}
    # delete → tombstone by PK (a Core DELETE … WHERE id = …)
    deletes = [s for s in conn.sql() if s.startswith("DELETE FROM")]
    assert len(deletes) == 1
    assert "WHERE" in deletes[0] and "id" in deletes[0]


@pytest.mark.asyncio
async def test_cdc_insert_routes_to_upsert():
    conn = _FakeConn()
    cols = [("id", "bigint")]
    table = build_table("mat", "k", cols, ("id",))
    await apply_cdc(conn, table, ["id"], [_Event("insert", {"id": 7})])
    assert conn.upserts == [(table, {"id": 7}, ["id"])]


class _FakeProvider:
    def __init__(self, events):
        self._events = events
        self.closed = False

    async def watch(self, table, filter_expr=None):
        for ev in self._events:
            yield ev

    async def ack(self, events):  # REQ-1734: no offset concept in this test double
        return

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
    landed_batches: list[list[_Event]] = []

    # REQ-1733/REQ-989: consume_cdc_into_store no longer holds a connection itself — it drains
    # the provider and applies each batch through the caller's own write face (land_fn), the same
    # way push_wiring.py's real _land closure wraps EngineRuntime.apply_cdc_events.
    async def _land(batch: list) -> dict[str, int]:
        landed_batches.append(batch)
        return {
            "upsert": sum(1 for e in batch if e.operation != "delete"),
            "delete": sum(1 for e in batch if e.operation == "delete"),
        }

    totals = await consume_cdc_into_store(
        provider,
        _land,
        schema="mat",
        table="pets",
        disconnect=asyncio.Event(),
    )
    assert totals == {"upsert": 2, "delete": 1}
    assert provider.closed
    assert sum(len(b) for b in landed_batches) == 3
