# Copyright (c) 2026 Kenneth Stott
# Canary: 0e5c7a92-4b1d-4f38-9a6e-d2f8c3b1e754
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2216: an approve or decide call carrying an Idempotency-Key applies once.

A repeat of the same key, by the same caller on the same endpoint, is answered with the first
result and carries nothing out again. A failed call keeps nothing, so its retry runs. Every one of
the five decision endpoints is held to it: creation requests' approve and reject, environment
merge decisions, glossary term approval and profiler constraint decisions.
"""

from __future__ import annotations

import types
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock

import pytest

from provisa.api import idempotency
from provisa.api.errors import ApiError


class _Db:
    """The kept results, by (scope, actor, key), and the advisory locks taken."""

    def __init__(self) -> None:
        self.kept: dict[tuple[str, str, str], object] = {}
        self.locks: list[int] = []

    @asynccontextmanager
    async def acquire(self):
        db = self

        class _Conn:
            @asynccontextmanager
            async def advisory_lock(self, key: int):
                db.locks.append(key)
                yield self

            async def execute_core(self, stmt):
                params = stmt.compile().params
                found = db.kept.get((params["scope_1"], params["actor_1"], params["call_key_1"]))
                result = MagicMock()
                result.scalar.return_value = found
                return result

            async def insert_returning(self, table, values, returning):
                db.kept[(values["scope"], values["actor"], values["call_key"])] = values["response"]
                return values[returning]

        yield _Conn()


def _request(key: str | None, *, user: str = "alice", path: str = "/admin/x/1/approve"):
    headers = {} if key is None else {idempotency.HEADER: key}
    return types.SimpleNamespace(
        headers=headers,
        method="POST",
        url=types.SimpleNamespace(path=path),
        state=types.SimpleNamespace(identity=types.SimpleNamespace(user_id=user)),
    )


async def test_a_repeated_key_applies_once_and_returns_the_first_result():
    db = _Db()
    run = AsyncMock(side_effect=[{"status": "executed", "n": 1}, {"status": "executed", "n": 2}])
    first = await idempotency.once(db, _request("k1"), run)  # type: ignore[arg-type]
    second = await idempotency.once(db, _request("k1"), run)  # type: ignore[arg-type]
    assert run.await_count == 1
    assert first == second == {"status": "executed", "n": 1}
    assert db.locks[0] == db.locks[1]


async def test_no_key_runs_every_call():
    db = _Db()
    run = AsyncMock(return_value={"ok": True})
    await idempotency.once(db, _request(None), run)  # type: ignore[arg-type]
    await idempotency.once(db, _request(None), run)  # type: ignore[arg-type]
    assert run.await_count == 2
    assert db.kept == {}


async def test_the_key_is_the_callers_and_the_endpoints():
    db = _Db()
    run = AsyncMock(return_value={"ok": True})
    await idempotency.once(db, _request("k1"), run)  # type: ignore[arg-type]
    await idempotency.once(db, _request("k1", user="bob"), run)  # type: ignore[arg-type]
    await idempotency.once(db, _request("k1", path="/admin/x/2/approve"), run)  # type: ignore[arg-type]
    assert run.await_count == 3


async def test_a_failed_call_keeps_nothing_so_its_retry_runs():
    db = _Db()
    run = AsyncMock(side_effect=[ApiError(409, "x.busy", "busy"), {"ok": True}])
    with pytest.raises(ApiError):
        await idempotency.once(db, _request("k1"), run)  # type: ignore[arg-type]
    assert await idempotency.once(db, _request("k1"), run) == {"ok": True}  # type: ignore[arg-type]
    assert run.await_count == 2


async def test_an_empty_key_is_refused_by_name():
    with pytest.raises(ApiError) as caught:
        await idempotency.once(_Db(), _request("  "), AsyncMock())  # type: ignore[arg-type]
    assert caught.value.code == "idempotency.key_empty"


# --- the five decision endpoints ---------------------------------------------------------------


@pytest.fixture
def db():
    """One kept-results store every endpoint's ``once`` reads and writes."""
    return _Db()


async def _twice(call) -> tuple:
    return await call(), await call()


async def test_creation_request_approve_and_reject_apply_once(monkeypatch, db):
    import provisa.api.admin.creation_requests_router as mod

    monkeypatch.setattr(mod, "_get_pool", lambda: db)
    approve = AsyncMock(side_effect=[{"id": 1, "status": "executed"}, {"id": 1, "status": "x"}])
    reject = AsyncMock(side_effect=[{"id": 2, "status": "rejected"}, {"id": 2, "status": "x"}])
    monkeypatch.setattr(mod, "_approve", approve)
    monkeypatch.setattr(mod, "_reject", reject)
    req = _request("a", path="/admin/creation-requests/1/approve")
    a1, a2 = await _twice(lambda: mod.approve_request(1, req))  # type: ignore[arg-type]
    assert a1 == a2 == {"id": 1, "status": "executed"} and approve.await_count == 1
    body = mod.RejectBody(reason="duplicate")
    req = _request("r", path="/admin/creation-requests/2/reject")
    r1, r2 = await _twice(lambda: mod.reject_request(2, body, req))  # type: ignore[arg-type]
    assert r1 == r2 == {"id": 2, "status": "rejected"} and reject.await_count == 1


async def test_environment_merge_decision_applies_once(monkeypatch, db):
    import provisa.api.admin.environments_router as mod
    import provisa.api.admin.orgs_router as orgs

    async def _org_db(_org_id):
        return db

    monkeypatch.setattr(orgs, "_org_model_db", _org_db)
    decide = AsyncMock(side_effect=[{"state": "applied"}, {"state": "again"}])
    monkeypatch.setattr(mod, "_decide_merge_request", decide)
    req = _request("m", path="/admin/orgs/acme/environments/-/merge-requests/3/decide")
    body = mod.DecideBody(approve=True)
    inner = getattr(mod.decide_merge_request, "__wrapped__", mod.decide_merge_request)
    d1, d2 = await _twice(lambda: inner(req, "acme", 3, body))  # type: ignore[arg-type]
    assert d1 == d2 == {"state": "applied"} and decide.await_count == 1


async def test_glossary_term_approval_applies_once(monkeypatch, db):
    import provisa.api.admin.glossary_router as mod

    async def _pool():
        return db

    monkeypatch.setattr(mod, "_pool", _pool)
    monkeypatch.setattr(mod, "_require_glossary_rw", lambda request: None)
    status = AsyncMock(side_effect=[{"ok": True, "status": "approved"}, {"ok": False}])
    monkeypatch.setattr(mod, "_status_route", status)
    req = _request("g", path="/admin/glossary/terms/4/approve")
    g1, g2 = await _twice(lambda: mod.approve_term(req, 4))  # type: ignore[arg-type]
    assert g1 == g2 == {"ok": True, "status": "approved"} and status.await_count == 1


async def test_profiler_constraint_decision_applies_once(monkeypatch, db):
    import provisa.api.admin.profiler_checks_router as mod

    monkeypatch.setattr(mod, "_db", lambda: db)
    monkeypatch.setattr(mod, "require_capability_request", lambda request, cap: None)
    decide = AsyncMock(side_effect=[{"id": 9}, {"id": 10}])
    monkeypatch.setattr(mod, "_decide_constraint", decide)
    req = _request("p", path="/admin/profiler/tables/5/profile-constraints")
    body = MagicMock()
    p1, p2 = await _twice(lambda: mod.decide_constraint(req, 5, body))  # type: ignore[arg-type]
    assert p1 == p2 == {"id": 9} and decide.await_count == 1
