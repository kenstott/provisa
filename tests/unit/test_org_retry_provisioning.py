# Copyright (c) 2026 Kenneth Stott
# Canary: 7c2e9a14-5b60-4d3f-8e71-a4f0c6d93b28
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1315: a failed org provisioning is retried, not abandoned."""

from __future__ import annotations

import concurrent.futures
import types

import pytest
from starlette.requests import Request

from provisa.api.admin import orgs_router
from provisa.api.errors import ApiError


class _Result:
    def __init__(self, row):
        self._row = row

    def fetchone(self):
        return self._row


class _Conn:
    def __init__(self, plane):
        self._plane = plane

    async def execute_core(self, stmt):
        text = str(stmt).lstrip().upper()
        if text.startswith("SELECT"):
            return _Result(self._plane.row)
        self._plane.updates.append(stmt.compile().params)
        return _Result(None)


class _Db:
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


def _request(user_id="creator") -> Request:
    req = Request({"type": "http", "headers": []})
    req.state.identity = types.SimpleNamespace(user_id=user_id)
    return req


@pytest.fixture
def plane(monkeypatch):
    p = types.SimpleNamespace(
        row=("creator", "failed", True, False),
        updates=[],
        deprovisioned=[],
        spawned=[],
        invalidated=[],
    )
    monkeypatch.setattr(orgs_router, "_admin_pool", lambda: _Db(p))
    monkeypatch.setattr(orgs_router, "_pool", lambda: "tenant-pool")
    monkeypatch.setattr(orgs_router, "_redis_url", lambda: "redis://none")
    monkeypatch.setattr(
        "provisa.api.app.state",
        types.SimpleNamespace(
            org_registry=types.SimpleNamespace(invalidate_org=p.invalidated.append)
        ),
        raising=False,
    )

    async def _deprovision(pool, org_id, redis_url=None):
        p.deprovisioned.append(org_id)

    monkeypatch.setattr("provisa.core.org_provisioning.deprovision_org", _deprovision)

    def _spawn(coro, name=None):
        coro.close()
        p.spawned.append(name)
        return concurrent.futures.Future()

    monkeypatch.setattr(orgs_router, "spawn_background", _spawn)
    return p


async def test_a_failed_org_is_deprovisioned_and_rebuilt_from_the_start(plane):
    out = await orgs_router.retry_provisioning("acme", _request())
    assert out == {"id": "acme", "provisioning_state": "provisioning"}
    assert plane.deprovisioned == ["acme"]
    assert plane.invalidated == ["acme"]
    assert plane.spawned == ["org-provision:acme"]
    assert plane.updates[0]["provisioning_state"] == "provisioning"
    assert plane.updates[0]["provisioning_error"] is None


@pytest.mark.parametrize("state", ["ready", "provisioning"])
async def test_only_a_failed_org_may_be_retried(plane, state):
    plane.row = ("creator", state, True, False)
    with pytest.raises(ApiError) as err:
        await orgs_router.retry_provisioning("acme", _request())
    assert err.value.status_code == 409
    assert err.value.code == "orgs.retry_not_failed"
    assert plane.deprovisioned == []
    assert plane.spawned == []


async def test_an_unknown_org_is_not_found(plane):
    plane.row = None
    with pytest.raises(ApiError) as err:
        await orgs_router.retry_provisioning("ghost", _request())
    assert err.value.status_code == 404
