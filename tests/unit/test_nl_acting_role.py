# Copyright (c) 2026 Kenneth Stott
# Canary: 1f4b7d92-8a3e-4c65-b2d0-9e6a5c3f7b18
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The natural-language job runs as the request's acting role (REQ-273, REQ-354).

``POST /query/nl`` carried a ``role`` field in its body and ran the job — the role-scoped schema
it prompts with, the compile and the execution preview — as THAT role, whatever role the auth
layer had established for the request. The acting role is the only role a request can run as; a
body role that differs from it is refused."""

# Requirements: REQ-273, REQ-354, REQ-356

from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from provisa.api.rest import nl_router

pytestmark = pytest.mark.asyncio


class _Jobs:
    """The job store, in memory."""

    def __init__(self) -> None:
        self.jobs: dict = {}

    async def put(self, job) -> None:
        self.jobs[job.job_id] = job

    async def get(self, job_id):
        return self.jobs.get(job_id)


@pytest.fixture
def nl(monkeypatch):
    """The NL router behind an auth layer that established ``analyst`` for the request — what
    AuthMiddleware does once it has validated the identity (``request.state.role``)."""
    import provisa.api.app as app_mod

    ran: list[str] = []
    jobs = _Jobs()

    async def _run_job(job_id, nl_query, role, app_state, llm, strict, delivery):
        ran.append(role)

    async def _llm(state):
        return object()

    monkeypatch.setattr(nl_router, "_run_job", _run_job)
    monkeypatch.setattr(nl_router, "_get_llm", _llm)
    monkeypatch.setattr(nl_router, "_job_store", jobs)
    monkeypatch.setattr(
        app_mod, "state", SimpleNamespace(rate_limiter=None, config=None), raising=False
    )

    app = FastAPI()

    @app.middleware("http")
    async def _authenticated_as_analyst(request, call_next):
        request.state.role = "analyst"
        return await call_next(request)

    app.include_router(nl_router.router)
    client = AsyncClient(transport=ASGITransport(app=app), base_url="http://test")
    return SimpleNamespace(client=client, ran=ran, jobs=jobs)


async def _settle() -> None:
    import asyncio

    for _ in range(20):
        await asyncio.sleep(0.01)


async def test_a_role_named_in_the_body_is_not_the_role_the_job_runs_as(nl):
    resp = await nl.client.post("/query/nl", json={"q": "count orders", "role": "org_admin"})
    await _settle()
    assert nl.ran != ["org_admin"], "the NL job ran as the role the request body picked"
    assert resp.status_code == 400, resp.text
    detail = resp.json()["detail"]
    assert "'org_admin'" in detail and "'analyst'" in detail
    assert nl.ran == [] and nl.jobs.jobs == {}, "a refused request still created a job"


async def test_the_job_runs_as_the_acting_role(nl):
    resp = await nl.client.post("/query/nl", json={"q": "count orders"})
    assert resp.status_code == 202, resp.text
    await _settle()
    job = await nl.jobs.get(resp.json()["job_id"])
    assert job.role == "analyst" and nl.ran == ["analyst"]


async def test_a_body_role_equal_to_the_acting_role_is_accepted(nl):
    resp = await nl.client.post("/query/nl", json={"q": "count orders", "role": "analyst"})
    assert resp.status_code == 202, resp.text
    await _settle()
    assert nl.ran == ["analyst"]


def test_the_acting_role_rule():
    from provisa.api.acting_role import acting_role
    from provisa.api.errors import ApiError

    secured = SimpleNamespace(state=SimpleNamespace(role="analyst"))
    assert acting_role(secured, None, None, "default") == "analyst"
    assert acting_role(secured, None, "analyst", "default") == "analyst"
    with pytest.raises(ApiError) as refused:
        acting_role(secured, None, "org_admin", "default")
    assert refused.value.status_code == 400 and refused.value.code == "data.role_mismatch"
    # no auth layer at all (a bare router): the body's role, else the field's default
    bare = SimpleNamespace(state=SimpleNamespace())
    assert acting_role(bare, None, "analyst", "default") == "analyst"
    assert acting_role(bare, None, None, "default") == "default"
