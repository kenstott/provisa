# Copyright (c) 2026 Kenneth Stott
# Canary: 7c2e5a91-4b8d-4f36-a1e0-6d9b3f8c2e47
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A forced redirect asked for over NL (REQ-1194, REQ-1224 amended 2026-10-04).

``POST /query/nl`` takes ``redirect`` / ``redirect_format``; the job runs every branch's statement
with that delivery, and a delivered branch answers the delivery's handle instead of rows. A format
that cannot be read is refused before a job is created."""

# Requirements: REQ-1194, REQ-1224, REQ-354

from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from provisa.api.rest import nl_router
from provisa.nl import executor

pytestmark = pytest.mark.asyncio


class _Jobs:
    def __init__(self) -> None:
        self.jobs: dict = {}

    async def put(self, job) -> None:
        self.jobs[job.job_id] = job

    async def get(self, job_id):
        return self.jobs.get(job_id)


@pytest.fixture
def nl(monkeypatch):
    import provisa.api.app as app_mod

    ran: list = []
    jobs = _Jobs()

    async def _run_job(job_id, nl_query, role, app_state, llm, strict, delivery):
        ran.append(delivery)

    async def _llm(state):
        return object()

    monkeypatch.setattr(nl_router, "_run_job", _run_job)
    monkeypatch.setattr(nl_router, "_get_llm", _llm)
    monkeypatch.setattr(nl_router, "_job_store", jobs)
    monkeypatch.setattr(
        app_mod, "state", SimpleNamespace(rate_limiter=None, config=None), raising=False
    )
    monkeypatch.setattr(
        "provisa.executor.redirect.request_redirect_config",
        lambda threshold: SimpleNamespace(default_format=None, threshold=threshold),
    )
    app = FastAPI()
    app.include_router(nl_router.router)
    client = AsyncClient(transport=ASGITransport(app=app), base_url="http://test")
    return SimpleNamespace(client=client, ran=ran, jobs=jobs)


async def _settle() -> None:
    import asyncio

    for _ in range(20):
        await asyncio.sleep(0.01)


async def test_a_forced_redirect_runs_the_job_with_that_delivery(nl):
    resp = await nl.client.post(
        "/query/nl", json={"q": "count orders", "redirect": True, "redirect_format": "parquet"}
    )
    assert resp.status_code == 202, resp.text
    await _settle()
    (delivery,) = nl.ran
    assert delivery is not None and delivery.output_format == "parquet"


async def test_without_the_option_the_job_runs_with_no_delivery(nl):
    resp = await nl.client.post("/query/nl", json={"q": "count orders"})
    assert resp.status_code == 202, resp.text
    await _settle()
    assert nl.ran == [None]


async def test_a_format_that_cannot_be_read_is_refused_before_a_job(nl):
    resp = await nl.client.post(
        "/query/nl", json={"q": "count orders", "redirect": True, "redirect_format": "xlsx"}
    )
    assert resp.status_code == 400, resp.text
    assert resp.json()["error"] == "invalid_redirect_format"
    await _settle()
    assert nl.ran == [] and nl.jobs.jobs == {}


async def test_a_delivered_branch_answers_the_handle(monkeypatch):
    handle = {"sink": "s3", "redirect_url": "http://store/x", "row_count": 2}
    seen: dict = {}

    async def _batch(sql, role, state, *, deliver, buffered):
        seen.update(deliver=deliver, buffered=buffered)
        return SimpleNamespace(redirect=handle, rows=[], column_names=[])

    monkeypatch.setattr("provisa.pgwire._pipeline.execute_sql_batch", _batch)
    delivery = object()
    out = await executor.execute(
        "SELECT id FROM sales.orders", "sql", "analyst", object(), deliver=delivery
    )
    assert out == {"redirect": handle}
    assert seen == {"deliver": delivery, "buffered": True}


async def test_an_undelivered_branch_answers_its_rows(monkeypatch):
    async def _batch(sql, role, state, *, deliver, buffered):
        return SimpleNamespace(redirect=None, rows=[(1,), (2,)], column_names=["id"])

    monkeypatch.setattr("provisa.pgwire._pipeline.execute_sql_batch", _batch)
    out = await executor.execute(
        "SELECT id FROM sales.orders", "sql", "analyst", object(), deliver=None
    )
    assert out == {"columns": ["id"], "rows": [{"id": 1}, {"id": 2}]}
