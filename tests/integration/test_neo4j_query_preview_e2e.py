# Copyright (c) 2026 Kenneth Stott
# Canary: 7d3c5e21-8b4a-4f6e-a9d0-2c1e6b5f8a37
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A Neo4j table's Cypher is previewed against the live endpoint before it is registered --
on a real server over a real Neo4j (REQ-296).

Seven ``PreviewPet`` nodes are seeded. The steward's query is run with an implicit ``LIMIT 5``
and its rows are answered; a query that returns node objects instead of scalar projections is
refused, naming the rewrite (``RETURN n.name AS name``). Provisa does not flatten the nodes.

Lands on the TEST instance only: one real server, a Neo4j the CI job provides."""

from __future__ import annotations

import os

import httpx
import pytest
import yaml

pytestmark = [pytest.mark.integration, pytest.mark.requires_neo4j]

_ROLE = "org_admin"
_ORG = "neo4j_preview_e2e"
_SOURCE = "preview-neo4j"
_LABEL = "PreviewPet"
_SEED = [(i, f"pet {i}") for i in range(1, 8)]
_REPO = os.path.join(os.path.dirname(__file__), "..", "..")


def _neo4j_port() -> int:
    return int(os.environ["NEO4J_HTTP_PORT"])


@pytest.fixture(scope="module")
def seeded():
    tx = f"http://localhost:{_neo4j_port()}/db/neo4j/tx/commit"
    create = " ".join(f"CREATE (:{_LABEL} {{pet_id: {i}, name: '{n}'}})" for i, n in _SEED)
    with httpx.Client(timeout=60) as client:
        for stmt in (f"MATCH (p:{_LABEL}) DETACH DELETE p", create):
            resp = client.post(tx, json={"statements": [{"statement": stmt}]})
            assert resp.status_code == 200 and resp.json()["errors"] == [], resp.text
        yield
        client.post(tx, json={"statements": [{"statement": f"MATCH (p:{_LABEL}) DETACH DELETE p"}]})


@pytest.fixture(scope="module")
def server(seeded, tmp_path_factory):
    from tests.integration.isolated_server import IsolatedServer

    with open(os.path.join(_REPO, "tests/fixtures/sample_config.yaml")) as f:
        base = yaml.safe_load(f)
    cfg = {
        "naming": base["naming"],
        "roles": base["roles"],
        "relationships": [],
        "sources": [
            {
                "id": _SOURCE,
                "type": "neo4j",
                "host": "localhost",
                "port": _neo4j_port(),
                "database": "neo4j",
            }
        ],
        "domains": [{"id": "graph", "description": "neo4j preview e2e"}],
        "tables": [],
    }
    path = tmp_path_factory.mktemp("neo4j_preview") / "config.yaml"
    path.write_text(yaml.safe_dump(cfg))
    srv = IsolatedServer(_ORG, engine="duckdb", config=str(path), control_plane="own")
    srv.start(timeout=240)
    try:
        yield srv
    finally:
        srv.stop_process()


def _preview(srv, cypher: str) -> httpx.Response:
    return httpx.post(
        f"{srv.base_url}/admin/sources/neo4j/{_SOURCE}/preview",
        json={"cypher": cypher},
        headers={"X-Provisa-Role": _ROLE},
        timeout=srv.request_timeout + 10,
    )


def test_a_scalar_projection_is_previewed_with_an_implicit_limit_of_five(server):
    resp = _preview(
        server, f"MATCH (p:{_LABEL}) RETURN p.pet_id AS pet_id, p.name AS name ORDER BY p.pet_id"
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert [(r["pet_id"], r["name"]) for r in body["rows"]] == _SEED[:5]
    assert [c["name"] for c in body["columns"]] == ["pet_id", "name"]


def test_a_query_returning_nodes_is_refused_naming_the_scalar_rewrite(server):
    resp = _preview(server, f"MATCH (p:{_LABEL}) RETURN p")
    assert resp.status_code == 422, resp.text
    assert "returned a node object" in resp.text, resp.text
    assert "RETURN n.name AS name" in resp.text, resp.text
