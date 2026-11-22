# Copyright (c) 2026 Kenneth Stott
# Canary: 41cd4da3-c930-4591-b50b-ba985a37a889
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A role's limits, held by a real server, asked as a client asks (REQ-369, REQ-370, REQ-354,
REQ-1174).

One server over the harness's ``sales.orders``, its sql_generation model pointed at an
OpenAI-compatible endpoint this test serves. Each limit is a role of the test's own, so one case
never spends another's budget:

- ``burst`` may send two requests a second: a burst past that is answered 429 with a
  ``Retry-After``, and the role with no limit is never refused (REQ-369);
- ``streamer`` may hold one SSE subscription: a second is answered 429 while the first is open,
  and the slot is free again once it closes (REQ-369);
- ``nl.rate_limit`` is one NL question a minute per role: the second is answered 429 before the
  model is asked anything (REQ-370);
- an NL question is answered 202 with a job id at once, while the model has not answered; the
  job is polled and streamed to its end (REQ-354);
- ``limited`` carries ``max_query_complexity``: a statement over it is answered 413 with its
  score and the limit, one within it runs, and the role with no limit runs the larger one
  (REQ-1174).

Lands on the TEST instance only: one real server over a database the harness creates."""

from __future__ import annotations

import os
import time

import httpx
import pytest

from tests.integration.test_code_source_llm import _MODEL, _Endpoint
from tests.integration.worker_boot_harness import WorkerBoot, _config

pytestmark = [pytest.mark.integration]

_PG_HOST = os.environ.get("PG_HOST", "localhost")
_PG_PORT = int(os.environ.get("PG_PORT", "5432"))
_ADMIN = "org_admin"
_BURST, _STREAMER, _LIMITED = "burst", "streamer", "limited"
_NL_CAPPED, _NL_ASKER, _NL_OTHER = "nl_capped", "nl_asker", "nl_other"
_ROLES = [_ADMIN, "analyst", _BURST, _STREAMER, _LIMITED, _NL_CAPPED, _NL_ASKER, _NL_OTHER]
_RPS = 2
_COMPLEXITY = 4
_NARROW = "SELECT id, region FROM sales.orders"
# 2 relations + 3 columns + 1 nested query = 6: over the limit of 4.
_WIDE = "SELECT id, region FROM sales.orders WHERE id IN (SELECT id FROM sales.orders)"
_TIMEOUT = 120.0


def _role(role_id: str, rate_limit: dict | None) -> dict:
    role: dict = {
        "id": role_id,
        "description": "Test role.",
        "capabilities": ["query_development", "full_results"],
        "domain_access": ["*"],
    }
    if rate_limit is not None:
        role["rate_limit"] = rate_limit
    return role


@pytest.fixture(scope="module")
def endpoint():
    served = _Endpoint()
    yield served
    served.stop()


@pytest.fixture(scope="module")
def server(endpoint):
    orders = _config(_PG_HOST, _PG_PORT, "unused")["tables"][0]
    orders["columns"] = [
        {"name": "id", "data_type": "integer", "visible_to": _ROLES, "is_primary_key": True},
        {"name": "region", "data_type": "varchar", "visible_to": _ROLES},
    ]
    boot = WorkerBoot(
        1,
        pg_host=_PG_HOST,
        pg_port=_PG_PORT,
        extra_config={
            "tables": [orders],
            "roles": [
                _role(_BURST, {"requests_per_second": _RPS}),
                _role(_STREAMER, {"max_sse_subscriptions": 1}),
                _role(_LIMITED, {"max_query_complexity": _COMPLEXITY}),
                _role(_NL_CAPPED, None),
                _role(_NL_ASKER, None),
                _role(_NL_OTHER, None),
            ],
            "nl": {"rate_limit": 1},
            "ai_endpoints": [{"id": "test-endpoint", "style": "openai", "base_url": endpoint.url}],
            "ai_models": {"sql_generation": {"vendor": "test-endpoint", "model": _MODEL}},
        },
        env={"PROVISA_REDIRECT_ENABLED": "false"},
    )
    boot.create_database()
    try:
        boot.start()
        boot.wait_all_ready(timeout=300)
        yield boot
    finally:
        boot.cleanup()


def _url(boot, path: str) -> str:
    return f"http://127.0.0.1:{boot.ports['http']}{path}"


def _sql(boot, sql: str, role: str) -> httpx.Response:
    return httpx.post(
        _url(boot, "/data/sql"),
        json={"sql": sql},
        headers={"X-Provisa-Role": role},
        timeout=_TIMEOUT,
    )


def _ask(boot, role: str) -> httpx.Response:
    return httpx.post(
        _url(boot, "/query/nl"),
        json={"q": "How many orders are there per region?"},
        headers={"X-Provisa-Role": role},
        timeout=_TIMEOUT,
    )


def _job(boot, job_id: str, role: str) -> dict:
    resp = httpx.get(
        _url(boot, f"/query/nl/{job_id}"), headers={"X-Provisa-Role": role}, timeout=_TIMEOUT
    )
    assert resp.status_code == 200, resp.text
    return resp.json()


def _until_done(boot, job_id: str, role: str, deadline_s: float = 180.0) -> dict:
    deadline = time.monotonic() + deadline_s
    while True:
        job = _job(boot, job_id, role)
        if job["state"] in ("complete", "failed"):
            return job
        assert time.monotonic() < deadline, job
        time.sleep(0.5)


# --- REQ-369: request rate ---------------------------------------------------------------------


def test_a_burst_past_the_roles_request_rate_is_refused_with_retry_after(server):
    answers = [_sql(server, _NARROW, _BURST) for _ in range(_RPS * 4)]
    refused = [a for a in answers if a.status_code == 429]
    assert refused, [a.status_code for a in answers]
    assert all(a.status_code in (200, 429) for a in answers), [a.text for a in answers]
    first = refused[0]
    assert first.json()["error"] == "rate_limited", first.text
    assert int(first.headers["Retry-After"]) >= 1, first.headers

    # Waited out as asked, the role is served again.
    time.sleep(int(first.headers["Retry-After"]) + 1)
    again = _sql(server, _NARROW, _BURST)
    assert again.status_code == 200, again.text


def test_a_role_with_no_request_rate_is_never_refused(server):
    answers = [_sql(server, _NARROW, _ADMIN) for _ in range(_RPS * 4)]
    assert [a.status_code for a in answers] == [200] * len(answers), [a.text for a in answers]


# --- REQ-369: concurrent SSE subscriptions -------------------------------------------------------


def _subscribe(boot, role: str) -> httpx.Response:
    return httpx.get(
        _url(boot, "/data/subscribe/orders"),
        headers={"X-Provisa-Role": role, "Accept": "text/event-stream"},
        timeout=_TIMEOUT,
    )


def test_a_second_subscription_past_the_roles_cap_is_refused_until_the_first_closes(server):
    with httpx.Client(timeout=_TIMEOUT) as client:
        with client.stream(
            "GET",
            _url(server, "/data/subscribe/orders"),
            headers={"X-Provisa-Role": _STREAMER, "Accept": "text/event-stream"},
        ) as first:
            assert first.status_code == 200, first.read()
            assert next(first.iter_lines()) == ": connected"
            second = _subscribe(server, _STREAMER)
            assert second.status_code == 429, second.text
            assert "max concurrent SSE subscriptions" in second.text, second.text

    # The server sees the close within its disconnect poll and releases the slot.
    deadline = time.monotonic() + 30
    with httpx.Client(timeout=_TIMEOUT) as client:
        while True:
            with client.stream(
                "GET",
                _url(server, "/data/subscribe/orders"),
                headers={"X-Provisa-Role": _STREAMER, "Accept": "text/event-stream"},
            ) as third:
                if third.status_code == 200:
                    assert next(third.iter_lines()) == ": connected"
                    break
                assert third.status_code == 429, third.read()
            assert time.monotonic() < deadline, "the closed subscription's slot was never released"
            time.sleep(1)


# --- REQ-354: the NL job ------------------------------------------------------------------------


def test_an_nl_question_is_answered_with_a_job_at_once_and_polled_and_streamed_to_its_end(
    server, endpoint
):
    endpoint.answering.clear()  # the model holds every request until released
    try:
        submitted = _ask(server, _NL_ASKER)
        assert submitted.status_code == 202, submitted.text
        job_id = submitted.json()["job_id"]
        # Answered while the model has not answered anything: the job runs after the reply.
        held = _job(server, job_id, _NL_ASKER)
        assert held["job_id"] == job_id
        assert held["state"] in ("pending", "running"), held
    finally:
        endpoint.answering.set()

    job = _until_done(server, job_id, _NL_ASKER)
    assert job["job_id"] == job_id
    assert job["role"] == _NL_ASKER
    assert endpoint.received, "the job never asked the model"

    streamed = httpx.get(
        _url(server, f"/query/nl/{job_id}/stream"),
        headers={"X-Provisa-Role": _NL_ASKER},
        timeout=_TIMEOUT,
    )
    assert streamed.status_code == 200, streamed.text
    assert streamed.headers["content-type"].startswith("text/event-stream")
    assert f'event: done\ndata: {{"state": "{job["state"]}"}}' in streamed.text, streamed.text

    unknown = httpx.get(
        _url(server, "/query/nl/no-such-job"), headers={"X-Provisa-Role": _NL_ASKER}, timeout=30
    )
    assert unknown.status_code == 404, unknown.text


# --- REQ-370: the NL rate ------------------------------------------------------------------------


def test_an_nl_question_past_the_rate_is_refused_before_the_model_is_asked(server, endpoint):
    first = _ask(server, _NL_CAPPED)
    assert first.status_code == 202, first.text
    _until_done(server, first.json()["job_id"], _NL_CAPPED)
    asked = len(endpoint.received)

    second = _ask(server, _NL_CAPPED)
    assert second.status_code == 429, second.text
    assert second.json() == {"error": "rate_limited", "detail": "NL query rate limit exceeded"}
    assert int(second.headers["Retry-After"]) >= 1, second.headers
    time.sleep(3)
    assert len(endpoint.received) == asked, "a refused NL question reached the model"

    # The limit is the role's: another role is not refused.
    other = _ask(server, _NL_OTHER)
    assert other.status_code == 202, other.text


# --- REQ-1174: the query complexity limit --------------------------------------------------------


def test_a_statement_over_the_roles_complexity_is_refused_413_and_one_within_runs(server):
    refused = _sql(server, _WIDE, _LIMITED)
    assert refused.status_code == 413, refused.text
    assert f"query complexity 6 exceeds the role limit of {_COMPLEXITY}" in refused.text

    within = _sql(server, _NARROW, _LIMITED)
    assert within.status_code == 200, within.text
    assert sorted(r["id"] for r in within.json()["data"]["sql"]) == [1, 2]

    unlimited = _sql(server, _WIDE, _ADMIN)
    assert unlimited.status_code == 200, unlimited.text
    assert sorted(r["id"] for r in unlimited.json()["data"]["sql"]) == [1, 2]
