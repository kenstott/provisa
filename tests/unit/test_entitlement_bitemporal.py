# Copyright (c) 2026 Kenneth Stott
# Canary: 3f7c9e21-8a4d-4b6e-9c02-d5e1a7b8f640
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2219: valid time on the entitlement store. A row's valid-to is the next row's valid-from
for the same key, as recorded; rows are append-only; the store reads as of either timeline."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from sqlalchemy import insert, select

from provisa.core.schema_org import entitlement_expiries, entitlements
from provisa.entitlements import history, store

pytestmark = pytest.mark.asyncio

T1 = datetime(2026, 1, 1, tzinfo=UTC)
T2 = datetime(2026, 2, 1, tzinfo=UTC)
T3 = datetime(2026, 3, 1, tzinfo=UTC)
T4 = datetime(2026, 4, 1, tzinfo=UTC)


def _mid(a: datetime, b: datetime) -> datetime:
    return a + (b - a) / 2


@pytest.fixture
async def conn(control_plane):
    db = control_plane.org()
    async with db.acquire() as c:
        yield c


async def _put(conn, identifiers, *, valid_from, recorded_at, removed=False, principal="alice"):
    await conn.execute_core(
        insert(entitlements).values(
            source="provisa",
            principal=principal,
            role_id="viewer",
            kind="tables",
            identifiers=identifiers,
            removed=removed,
            set_by="ops",
            reason="r",
            valid_from=valid_from,
            recorded_at=recorded_at,
        )
    )


async def _put_rule(conn, ends_at, *, valid_from, recorded_at, removed=False):
    await conn.execute_core(
        insert(entitlement_expiries).values(
            rule_key="k1",
            principal="alice",
            role_id="viewer",
            ends_at=ends_at,
            ends={"*": ["a.b"]},
            list_kind="tables",
            removed=removed,
            set_by="ops",
            reason="r",
            valid_from=valid_from,
            recorded_at=recorded_at,
        )
    )


def _held(view: history.AsOf) -> dict[str, tuple[str, ...]]:
    return {p.row.principal: tuple(p.row.identifiers) for p in view.entitlements}


async def test_a_set_then_a_change_gives_two_periods_the_first_closed_at_the_seconds_start(conn):
    await _put(conn, ["a.b"], valid_from=T1, recorded_at=T1)
    await _put(conn, ["c.d"], valid_from=T2, recorded_at=T2)
    periods = await history.entitlement_periods(conn, principal="alice")
    assert [(tuple(p.row.identifiers), p.valid_from, p.valid_to) for p in periods] == [
        (("a.b",), T1, T2),
        (("c.d",), T2, None),
    ]


async def test_the_store_reads_as_of_valid_time(conn):
    await _put(conn, ["a.b"], valid_from=T1, recorded_at=T1)
    await _put(conn, ["c.d"], valid_from=T2, recorded_at=T2)
    assert _held(await history.as_of(conn, _mid(T1, T2), T4)) == {"alice": ("a.b",)}
    assert _held(await history.as_of(conn, _mid(T2, T3), T4)) == {"alice": ("c.d",)}
    assert _held(await history.as_of(conn, datetime(2025, 12, 1, tzinfo=UTC), T4)) == {}


async def test_as_recorded_before_the_change_the_first_row_is_open(conn):
    await _put(conn, ["a.b"], valid_from=T1, recorded_at=T1)
    await _put(conn, ["c.d"], valid_from=T2, recorded_at=T2)
    periods = await history.entitlement_periods(conn, principal="alice", recorded_at=_mid(T1, T2))
    assert [(tuple(p.row.identifiers), p.valid_to) for p in periods] == [(("a.b",), None)]
    assert _held(await history.as_of(conn, _mid(T2, T3), _mid(T1, T2))) == {"alice": ("a.b",)}


async def test_a_backdated_close_reads_both_ways(conn):
    """A row set at T1 and another at T3; at T4 a removal is recorded valid from T2."""
    await _put(conn, ["a.b"], valid_from=T1, recorded_at=T1)
    await _put(conn, ["c.d"], valid_from=T3, recorded_at=T3)
    await _put(conn, [], valid_from=T2, recorded_at=T4, removed=True)
    at = _mid(T2, T3)
    assert _held(await history.as_of(conn, at, _mid(T3, T4))) == {"alice": ("a.b",)}
    assert _held(await history.as_of(conn, at, T4)) == {}
    periods = await history.entitlement_periods(conn, principal="alice", recorded_at=T4)
    assert [(p.row.removed, p.valid_from, p.valid_to) for p in periods] == [
        (False, T1, T2),
        (True, T2, T3),
        (False, T3, None),
    ]
    assert _held(await history.as_of(conn, _mid(T3, T4), T4)) == {"alice": ("c.d",)}


async def test_keys_are_independent(conn):
    await _put(conn, ["a.b"], valid_from=T1, recorded_at=T1)
    await _put(conn, ["x.y"], valid_from=T2, recorded_at=T2, principal="bob")
    periods = await history.entitlement_periods(conn)
    assert [(p.row.principal, p.valid_to) for p in periods] == [("alice", None), ("bob", None)]


async def test_expiry_rules_read_as_of_either_timeline(conn):
    await _put_rule(conn, T4, valid_from=T1, recorded_at=T1)
    await _put_rule(conn, T4, valid_from=T2, recorded_at=T3, removed=True)
    rules = lambda view: [p.row.rule_key for p in view.expiries]  # noqa: E731
    assert rules(await history.as_of(conn, _mid(T2, T3), _mid(T2, T3))) == ["k1"]
    assert rules(await history.as_of(conn, _mid(T2, T3), T3)) == []
    assert rules(await history.as_of(conn, _mid(T1, T2), T3)) == ["k1"]
    periods = await history.expiry_periods(conn, principal="alice")
    assert [(p.valid_from, p.valid_to) for p in periods] == [(T1, T2), (T2, None)]


async def test_writes_append_and_never_change_a_stored_row(conn):
    await store.set_list(conn, "alice", "viewer", "tables", ["a.b"], set_by="ops", reason="r")
    first = (await conn.execute_core(select(entitlements))).fetchall()
    await store.set_list(conn, "alice", "viewer", "tables", ["c.d"], set_by="ops", reason="r2")
    await store.remove_list(conn, "alice", "viewer", "tables", set_by="ops", reason="gone")
    after = (await conn.execute_core(select(entitlements).order_by(entitlements.c.id))).fetchall()
    assert len(after) == 3
    assert after[0] == first[0]


async def test_the_current_reads_are_the_open_periods(conn):
    """load_held reads the store's open periods: a backdated close recorded later ends the row."""
    await _put(conn, ["a.b"], valid_from=T1, recorded_at=T1)
    await _put(conn, ["c.d"], valid_from=T3, recorded_at=T3)
    await _put(conn, [], valid_from=T2, recorded_at=T4, removed=True)
    held = (await store.load_held(conn))["alice"]
    assert [r.identifiers for r in held.lists] == [("c.d",)]
