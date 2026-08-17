# Copyright (c) 2026 Kenneth Stott
# Canary: 3c58e02a-7b46-4d91-a0f8-15b7c9e2340d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1271 / REQ-1272: creating an org returns before the org exists, and says so.

Provisioning a tenant builds a schema, a PG role, a Redis ACL and a data-plane runtime — far
longer than a request should hold. So the create returns ``provisioning`` immediately with
membership already granted, and a background task flips the row to ``ready`` or to ``failed``
with the reason attached.

That split is where the interesting failures live: a creator who owns the org in the admin plane
but whose tenant-plane role assignment never lands, and a provisioning failure that disappears
into a log instead of onto the row the poller reads. Both are asserted here against a recorded
admin plane — a live Postgres would prove the SQL runs, not that the order is right.
"""

# Requirements: REQ-1266, REQ-1271, REQ-1272

from __future__ import annotations

import types

import pytest

from provisa.api.errors import ApiError


class _Result:
    def __init__(self, rows):
        self._rows = rows

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def scalar(self):
        return self._rows[0][0] if self._rows else 0


class _Row:
    def __init__(self, mapping):
        self._mapping = mapping


class _Conn:
    """Records every statement, and answers the two reads create_org makes."""

    def __init__(self, plane):
        self._plane = plane

    async def execute_core(self, stmt):
        text = str(stmt)
        self._plane.statements.append(text)
        if text.startswith("SELECT count"):
            return _Result([(self._plane.owned_count,)])
        if text.startswith("SELECT orgs.id"):
            if not self._plane.existing_id:
                return _Result([])
            return _Result([_Row(self._plane.existing_row)])
        if text.startswith("INSERT INTO orgs"):
            values = self._plane.inserted
            return _Result([_Row(values)])
        if text.startswith("UPDATE orgs"):
            self._plane.updates.append(text)
            self._plane.update_params.append(stmt.compile().params)
            if self._plane.updated_rows:
                row = self._plane.updated_rows.pop(0)
                return _Result([row] if row is not None else [])
        return _Result([])

    async def upsert(self, table, values, *, index_elements, update_columns):
        self._plane.memberships.append(values)


class _Pool:
    def __init__(self, plane):
        self._plane = plane

    def acquire(self):
        conn = _Conn(self._plane)

        class _Ctx:
            async def __aenter__(self_inner):
                return conn

            async def __aexit__(self_inner, *exc):
                return False

        return _Ctx()


@pytest.fixture(autouse=True)
def self_hosted(monkeypatch):
    """The default deployment shape for this suite: no commercial plugin, so create provisions.

    Pinned rather than inherited, because whether ``provisa_commercial`` happens to be importable is
    a property of the developer's PYTHONPATH, not of the behaviour under test.
    """
    import provisa.core.commerce as commerce

    monkeypatch.setattr(commerce, "_PLUGIN", None)
    monkeypatch.setattr(commerce, "_LOADED", True)


@pytest.fixture
def plane(monkeypatch):
    """A recorded admin plane, with the background provisioning task never started."""
    import provisa.api.admin.orgs_router as mod

    state = types.SimpleNamespace(
        statements=[],
        updates=[],
        update_params=[],
        updated_rows=[],
        memberships=[],
        owned_count=0,
        existing_id=None,
        existing_row={
            "id": "carolco",
            "name": "Carolco",
            "created_by": "someone-else",
            "provisioning_state": "ready",
        },
        inserted={
            "id": "carolco",
            "name": "Carolco",
            "created_by": "carol",
            "provisioning_state": "provisioning",
        },
        provisioned=[],
    )
    pool = _Pool(state)
    monkeypatch.setattr(mod, "_admin_pool", lambda: pool)
    monkeypatch.setattr(mod, "_pool", lambda: pool)

    # The task is the async half; create_org's contract is that it STARTS one and returns.
    def _spawn(coro):
        coro.close()
        state.provisioned.append("started")

        class _Task:
            def add_done_callback(self, cb):
                pass

        return _Task()

    monkeypatch.setattr(mod.asyncio, "create_task", _spawn)
    monkeypatch.setattr(mod, "_provisioning_tasks", set())
    return state


def _request(user_id: str | None):
    identity = None if user_id is None else types.SimpleNamespace(user_id=user_id)
    return types.SimpleNamespace(state=types.SimpleNamespace(identity=identity))


def _body(**kwargs):
    from provisa.api.admin.orgs_router import CreateOrgBody

    return CreateOrgBody(**{"id": "carolco", "name": "Carolco", **kwargs})


@pytest.mark.asyncio
async def test_any_authenticated_user_may_create_an_org(plane):
    from provisa.api.admin.orgs_router import create_org

    body = await create_org(_body(), _request("carol"))

    assert body["provisioning_state"] == "provisioning"
    assert plane.provisioned == ["started"]


@pytest.mark.asyncio
async def test_the_creator_owns_the_org_before_the_response_returns(plane):
    """Membership is admin-plane state and must be synchronous: the creator's very next request
    is the status poll, which has to route to an org they are a member of."""
    from provisa.api.admin.orgs_router import create_org

    await create_org(_body(), _request("carol"))

    assert len(plane.memberships) == 1
    row = plane.memberships[0]
    assert row["user_id"] == "carol"
    assert row["org_id"] == "carolco"
    # REQ-1478: creating the org is the creator's own act, so the membership is born acknowledged
    # and no sign-in notice is raised for it.
    assert row["joined_via"] == "created"
    assert row["acknowledged_at"] is not None


@pytest.mark.asyncio
async def test_an_anonymous_request_under_auth_is_refused(plane):
    """``anonymous`` is the dev principal. Under auth it must not end up owning a tenant."""
    from provisa.api.admin.orgs_router import create_org

    with pytest.raises(ApiError) as exc:
        await create_org(_body(), _request("anonymous"))

    assert exc.value.status_code == 401
    assert exc.value.code == "orgs.auth_required_create"
    assert plane.provisioned == []


@pytest.mark.asyncio
async def test_an_existing_id_is_refused_before_anything_is_provisioned(plane):
    from provisa.api.admin.orgs_router import create_org

    plane.existing_id = "carolco"

    with pytest.raises(ApiError) as exc:
        await create_org(_body(), _request("carol"))

    assert exc.value.status_code == 409
    assert exc.value.code == "orgs.already_exists"
    assert plane.provisioned == []


@pytest.mark.asyncio
async def test_the_per_user_org_limit_is_enforced_at_create(plane):
    from provisa.api.admin.orgs_router import _MAX_ORGS_PER_USER, create_org

    plane.owned_count = _MAX_ORGS_PER_USER

    with pytest.raises(ApiError) as exc:
        await create_org(_body(), _request("carol"))

    assert exc.value.status_code == 409
    assert exc.value.code == "orgs.limit_reached"


@pytest.mark.asyncio
async def test_a_failed_org_does_not_count_against_the_limit(plane):
    """A failed org holds no data; counting it would strand a user who hit a transient error."""
    from provisa.api.admin.orgs_router import create_org

    await create_org(_body(), _request("carol"))

    count_sql = next(s for s in plane.statements if s.startswith("SELECT count"))
    assert "provisioning_state != " in count_sql


@pytest.mark.asyncio
async def test_provisioning_failure_is_written_to_the_row_the_poller_reads(plane, monkeypatch):
    """A traceback in the log tells the operator; the row is what tells the user waiting on the
    spinner. Swallowing it leaves them polling ``provisioning`` forever."""
    import provisa.api.admin.orgs_router as mod

    async def _boom(*args, **kwargs):
        raise RuntimeError("schema create denied")

    monkeypatch.setattr("provisa.core.org_provisioning.provision_org", _boom)

    await mod._provision_org_task("carolco", False, "carol", False)

    assert plane.updates, "the failure never reached the orgs row"
    assert "provisioning_state" in plane.updates[-1]


# ---------------------------------------------------------------------------
# REQ-1476: where the org is sold, create reserves and the subscription builds
# ---------------------------------------------------------------------------


@pytest.fixture
def gated(monkeypatch, self_hosted):
    """A deployment with the commercial plugin present, without importing it."""
    import provisa.core.commerce as commerce

    monkeypatch.setattr(commerce, "_PLUGIN", types.SimpleNamespace(), raising=False)
    monkeypatch.setattr(commerce, "_LOADED", True, raising=False)


@pytest.mark.asyncio
async def test_a_gated_create_reserves_the_id_and_builds_nothing(plane, gated, monkeypatch):
    """The subscription is what pays for the schema, so nothing is built before it exists."""
    import provisa.core.commerce as commerce
    from provisa.api.admin.orgs_router import create_org

    async def _no_sweep(conn):
        return None

    monkeypatch.setattr(commerce, "sweep_org_reservations", _no_sweep)
    plane.inserted = {**plane.inserted, "provisioning_state": "awaiting_checkout"}

    body = await create_org(_body(), _request("carol"))

    assert body["provisioning_state"] == "awaiting_checkout"
    assert plane.provisioned == []
    insert_sql = next(s for s in plane.statements if s.startswith("INSERT INTO orgs"))
    assert "awaiting_checkout" not in insert_sql  # bound as a parameter, not inlined


@pytest.mark.asyncio
async def test_the_creator_returning_to_their_reservation_gets_it_back(plane, gated, monkeypatch):
    """The id is already theirs, so a second create is the resume of an abandoned checkout."""
    import provisa.core.commerce as commerce
    from provisa.api.admin.orgs_router import create_org

    async def _no_sweep(conn):
        return None

    monkeypatch.setattr(commerce, "sweep_org_reservations", _no_sweep)
    plane.existing_id = "carolco"
    plane.existing_row = {
        "id": "carolco",
        "name": "Carolco",
        "created_by": "carol",
        "provisioning_state": "awaiting_checkout",
    }

    body = await create_org(_body(), _request("carol"))

    assert body["provisioning_state"] == "awaiting_checkout"
    assert plane.provisioned == []
    assert not any(s.startswith("INSERT INTO orgs") for s in plane.statements)


@pytest.mark.asyncio
async def test_someone_elses_reservation_is_a_taken_id(plane, gated, monkeypatch):
    import provisa.core.commerce as commerce
    from provisa.api.admin.orgs_router import create_org

    async def _no_sweep(conn):
        return None

    monkeypatch.setattr(commerce, "sweep_org_reservations", _no_sweep)
    plane.existing_id = "carolco"
    plane.existing_row = {
        "id": "carolco",
        "name": "Carolco",
        "created_by": "dave",
        "provisioning_state": "awaiting_checkout",
    }

    with pytest.raises(ApiError) as exc:
        await create_org(_body(), _request("carol"))

    assert exc.value.code == "orgs.already_exists"


@pytest.mark.asyncio
async def test_begin_provisioning_builds_the_reservation_once(plane):
    """The webhook can be redelivered; the second delivery must not build the org twice."""
    from provisa.api.admin.orgs_router import begin_provisioning

    plane.updated_rows = [
        _Row({"seeded_demo": True, "created_by": "carol", "isolated_engine": False}),
        None,
    ]

    assert await begin_provisioning("carolco") is True
    assert plane.provisioned == ["started"]
    assert await begin_provisioning("carolco") is False
    assert plane.provisioned == ["started"]
    assert "awaiting_checkout" in plane.update_params[0].values()
