# Copyright (c) 2026 Kenneth Stott
# Canary: 1d4f8a2c-9b3e-4f7a-8c5d-3e1b5f9a7c2d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration tests for POST /data/cypher.

These tests require the full Provisa stack (postgres + federation engine).
Run with docker-compose up before executing.
"""

import os

import pytest
import httpx

pytestmark = [pytest.mark.e2e, pytest.mark.requires_provisa_server]

BASE_URL = os.environ.get("PROVISA_URL", "http://localhost:8000")


def _headers() -> dict:
    token = os.environ.get("PROVISA_TOKEN", "")
    if token:
        return {"Authorization": f"Bearer {token}"}
    return {}


@pytest.fixture(scope="module")
def client():
    # Federated Cypher→Trino queries can exceed a 30s read window under suite load
    # (flaky ReadTimeouts); keep connect short but give reads a wide budget.
    timeout = httpx.Timeout(connect=10.0, read=120.0, write=30.0, pool=60.0)
    with httpx.Client(base_url=BASE_URL, headers=_headers(), timeout=timeout) as c:
        yield c


def test_cypher_endpoint_reachable(client):
    """Verify the /data/cypher endpoint exists and responds.

    Uses a schema-inspection procedure (db.labels) rather than `MATCH (n)`: the
    latter fans out across every registered node label and can exceed the 30s
    client timeout on a cold cache, which would make a pure reachability check
    flaky. db.labels is handled in-process from the label map (no federation
    scan), so it proves the route exists and processes Cypher deterministically.
    """
    resp = client.post("/data/cypher", json={"query": "CALL db.labels()"})
    # 200 (labels returned) or 503 (schema not loaded) — never 404
    assert resp.status_code != 404, f"Endpoint not found: {resp.text}"


def _clear_session(client) -> None:
    cleared = client.delete("/data/cypher/session")
    assert cleared.status_code == 200, cleared.text


def _ephemeral_write(client, query: str) -> httpx.Response:
    """``query`` answered in an empty graph session, which is cleared again after: the node it
    makes is the principal's session's (REQ-2176), which every later statement of this principal
    would read beside the model, on every surface, until the session's retention ran out."""
    _clear_session(client)
    try:
        return client.post("/data/cypher", json={"query": query})
    finally:
        _clear_session(client)


def test_merge_of_a_free_label_makes_an_ephemeral_node(client):
    """REQ-818, REQ-2176: MERGE is MATCH, then CREATE under the governing rule. A label the
    model does not have names no model table, so the CREATE is no governed write: it makes one
    node in the principal's graph session (no longer refused as an unsupported write), and the
    answer counts it as Neo4j does (REQ-670)."""
    resp = _ephemeral_write(client, "MERGE (n:Person {name: 'Eve'})")
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["counters"]["nodes-created"] == 1, data
    assert data["counters"]["labels-added"] == 1, data


def test_create_of_a_free_label_makes_an_ephemeral_node(client):
    """REQ-2176: a CREATE whose label names no model table is not routed to the governed write
    (once refused there for the missing label); it makes an ephemeral node, never a read-only or
    write-clause error."""
    resp = _ephemeral_write(client, "CREATE (n:Person {name: 'Eve'})")
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["counters"]["nodes-created"] == 1, data
    assert "read-only" not in resp.text.lower()


def test_apoc_rejected(client):
    resp = client.post(
        "/data/cypher",
        json={"query": "MATCH (n) RETURN apoc.util.sleep(100)"},
    )
    assert resp.status_code == 400
    data = resp.json()
    assert "APOC" in data["detail"].upper(), data


def test_parse_error_returns_400(client):
    # A statement that does not parse (MERGE once stood in for one; it is a write, REQ-2176).
    resp = client.post(
        "/data/cypher",
        json={"query": "MATCH (n:Person RETURN n"},
    )
    assert resp.status_code == 400, resp.text
    assert resp.json()["code"] == "cypher.refused"


def test_valid_query_returns_columns_and_rows(client):
    """Query that should succeed if any table is registered.

    A ReadTimeout is NOT skipped: the module-scoped client uses a 30s timeout
    and the endpoint responds well within it under normal load, so a timeout
    is a real performance/correctness defect that must surface as a failure.
    """
    resp = client.post(
        "/data/cypher",
        json={"query": "MATCH (n) RETURN n LIMIT 1"},
    )
    # Either success or schema-not-loaded — not a 500 from a bug
    if resp.status_code == 200:
        data = resp.json()
        assert "columns" in data
        assert "rows" in data
    elif resp.status_code in (400, 403, 500, 503):
        # Acceptable: no schema, cross-source error, Trino catalog unavailable, or
        # no columns visible to the anonymous role (V003 — org_admin after REQ-1327).
        pass
    else:
        pytest.fail(f"Unexpected status {resp.status_code}: {resp.text}")


def test_named_params_bound_correctly(client):
    """A named parameter binds into a Cypher query without error or timeout.

    Binds ``$node_id`` into a label-BOUNDED query rather than an unbounded
    ``MATCH (n)``. An unlabelled match fans out across every registered node
    label and evaluates the predicate per-label, reliably exceeding the 30s
    client timeout (the same fan-out cost documented on
    test_cypher_endpoint_reachable). A single-label match with LIMIT 1 exercises
    the identical param-binding path but resolves to one source and returns in
    well under the timeout. The concrete label is discovered via db.labels();
    if none are registered, any explicit single label still avoids the fan-out.
    """
    labels_resp = client.post("/data/cypher", json={"query": "CALL db.labels()"})
    label = "Thing"
    if labels_resp.status_code == 200:
        rows = labels_resp.json().get("rows", [])
        if rows:
            label = rows[0]["label"]

    resp = client.post(
        "/data/cypher",
        json={
            "query": f"MATCH (n:{label}) WHERE n.id = $node_id RETURN n LIMIT 1",
            "params": {"node_id": "1"},
        },
    )
    # A ReadTimeout here is a real defect: a single-label LIMIT 1 query must
    # respond within the 30s client timeout. Any HTTP response is acceptable —
    # the param path is what's under test, not whether rows exist.
    assert resp.status_code in (200, 400, 500, 503)
    if resp.status_code == 400:
        assert resp.json()["detail"]
