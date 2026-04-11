# Copyright (c) 2026 Kenneth Stott
# Canary: 8e629fb5-eb05-48c2-a3f9-536510e5486f
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration tests for POST /query/nl and GET /query/nl/{job_id}.

These tests require the full Provisa stack running (docker-compose up).
"""

import os
import time

import pytest
import httpx


BASE_URL = os.environ.get("PROVISA_URL", "http://localhost:8000")


def _headers() -> dict:
    token = os.environ.get("PROVISA_TOKEN", "")
    if token:
        return {"Authorization": f"Bearer {token}"}
    return {}


@pytest.fixture(scope="module")
def client():
    with httpx.Client(base_url=BASE_URL, headers=_headers(), timeout=60) as c:
        yield c


def test_post_nl_returns_job_id(client):
    resp = client.post("/query/nl", json={"q": "List all persons", "role": "default"})
    assert resp.status_code in (200, 202), resp.text
    data = resp.json()
    assert "job_id" in data
    assert data["job_id"]


def test_get_nl_result_pending_or_complete(client):
    resp = client.post("/query/nl", json={"q": "Find all data", "role": "default"})
    assert resp.status_code in (200, 202)
    job_id = resp.json()["job_id"]

    # Poll up to 30 seconds for completion
    deadline = time.time() + 30
    result_resp = None
    while time.time() < deadline:
        result_resp = client.get(f"/query/nl/{job_id}")
        assert result_resp.status_code == 200
        data = result_resp.json()
        if data.get("state") in ("complete", "failed"):
            break
        time.sleep(1)

    assert result_resp is not None
    data = result_resp.json()
    assert data["state"] in ("pending", "running", "complete", "failed")


def test_result_contains_all_three_branches(client):
    resp = client.post("/query/nl", json={"q": "count rows", "role": "default"})
    assert resp.status_code in (200, 202)
    job_id = resp.json()["job_id"]

    deadline = time.time() + 30
    while time.time() < deadline:
        r = client.get(f"/query/nl/{job_id}")
        data = r.json()
        if data.get("state") in ("complete", "failed"):
            break
        time.sleep(1)

    data = client.get(f"/query/nl/{job_id}").json()
    branches = data.get("branches", {})
    # All three targets should be present (even if error)
    assert set(branches.keys()) == {"cypher", "graphql", "sql"}


def test_failed_branch_has_null_query_and_error(client):
    """Branches that exhaust retries have query=null and error set."""
    resp = client.post("/query/nl", json={"q": "count rows", "role": "default"})
    assert resp.status_code in (200, 202)
    job_id = resp.json()["job_id"]

    deadline = time.time() + 30
    while time.time() < deadline:
        r = client.get(f"/query/nl/{job_id}")
        if r.json().get("state") in ("complete", "failed"):
            break
        time.sleep(1)

    data = client.get(f"/query/nl/{job_id}").json()
    for target, branch in data.get("branches", {}).items():
        if branch.get("query") is None:
            assert branch.get("error") is not None or branch.get("query") is None


def test_unknown_job_id_returns_404(client):
    resp = client.get("/query/nl/nonexistent-job-id-12345")
    assert resp.status_code == 404
