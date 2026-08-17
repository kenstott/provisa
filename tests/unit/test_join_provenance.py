# Copyright (c) 2026 Kenneth Stott
# Canary: 91c4e7a2-0f36-4d58-b1a9-6e83d2c50f47
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1478: a membership records how it came about, and whether the member has been told.

A person can end up in an org by an email-rule match or by an administrator adding them —
neither is an act they performed — so the membership carries what to say the next time they
sign in, and a row is only acknowledged once they have seen it.
"""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from provisa.core.org_membership import (
    JOINED_VIA_ADMIN,
    JOINED_VIA_AUTO_JOIN,
    JOINED_VIA_CREATED,
    JOINED_VIA_INVITE,
    acknowledge_membership,
    grant_membership,
    membership_values,
)


class _Conn:
    def __init__(self):
        self.upserts: list[tuple] = []
        self.statements: list = []

    async def upsert(self, table, values, *, index_elements, update_columns):
        self.upserts.append((table.name, values, tuple(index_elements), tuple(update_columns)))

    async def execute_core(self, stmt):
        self.statements.append(stmt)
        return SimpleNamespace(fetchall=lambda: [], fetchone=lambda: None)


class _Db:
    def __init__(self):
        self.conn = _Conn()

    def acquire(self):
        conn = self.conn

        class _Ctx:
            async def __aenter__(self_inner):
                return conn

            async def __aexit__(self_inner, *exc):
                return False

        return _Ctx()


# ---------------------------------------------------------------------------
# membership_values
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("joined_via", [JOINED_VIA_INVITE, JOINED_VIA_AUTO_JOIN, JOINED_VIA_ADMIN])
def test_a_membership_the_user_did_not_ask_for_starts_unacknowledged(joined_via):
    row = membership_values("alice", "acme", joined_via)
    assert row["joined_via"] == joined_via
    assert "acknowledged_at" not in row


def test_creating_an_org_needs_no_announcement():
    row = membership_values("alice", "acme", JOINED_VIA_CREATED)
    assert row["joined_via"] == JOINED_VIA_CREATED
    assert isinstance(row["acknowledged_at"], datetime)


# ---------------------------------------------------------------------------
# grant_membership
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_grant_membership_records_the_provenance():
    db = _Db()
    await grant_membership(db, "alice", "acme", joined_via=JOINED_VIA_AUTO_JOIN)
    table, values, index_elements, update_columns = db.conn.upserts[0]
    assert table == "user_org_memberships"
    assert values == {"user_id": "alice", "org_id": "acme", "joined_via": JOINED_VIA_AUTO_JOIN}
    assert index_elements == ("user_id", "org_id")
    # Empty update list: a re-grant leaves the first way in — and its acknowledgement — standing,
    # so an admin re-adding a member does not resurrect a notice the member already dismissed.
    assert update_columns == ()


# ---------------------------------------------------------------------------
# acknowledge_membership
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_acknowledge_stamps_only_the_callers_own_membership():
    db = _Db()
    before = datetime.now(timezone.utc)
    await acknowledge_membership(db, "alice", "acme")
    stmt = db.conn.statements[0]
    compiled = stmt.compile()
    assert stmt.table.name == "user_org_memberships"
    assert set(compiled.params.values()) >= {"alice", "acme"}
    stamped = stmt.compile().params["acknowledged_at"]
    assert stamped >= before


# ---------------------------------------------------------------------------
# /auth/acknowledge-join
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_acknowledge_join_refuses_an_unauthenticated_caller():
    from provisa.api.auth_router import AcknowledgeJoinRequest, acknowledge_join
    from provisa.api.errors import ApiError

    request = SimpleNamespace(state=SimpleNamespace(identity=None))
    with pytest.raises(ApiError) as exc:
        await acknowledge_join(AcknowledgeJoinRequest(org_id="acme"), request)
    assert exc.value.status_code == 401


@pytest.mark.asyncio
async def test_acknowledge_join_stamps_the_caller(monkeypatch):
    from provisa.api import app as app_mod
    from provisa.api.auth_router import AcknowledgeJoinRequest, acknowledge_join

    db = _Db()
    monkeypatch.setattr(app_mod.state, "admin_db", db, raising=False)
    request = SimpleNamespace(state=SimpleNamespace(identity=SimpleNamespace(user_id="alice")))

    body = await acknowledge_join(AcknowledgeJoinRequest(org_id="acme"), request)

    assert body == {"org_id": "acme", "acknowledged": True}
    assert set(db.conn.statements[0].compile().params.values()) >= {"alice", "acme"}
