# Copyright (c) 2026 Kenneth Stott
# Canary: 6e1a9d73-4c28-4b05-8f39-2d7b0c5e1a84
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E: a table whose source takes no writes refuses them by name, on every surface.

A real isolated server (DuckDB engine, SQLite control plane) over a real Neo4j. A Neo4j-backed
table takes no inserts, updates or deletes (executor/write_capability.py): an INSERT over SQL is
refused naming the table and the operation, before any right is checked, even for a role holding
the write right; GraphQL offers it no mutation field at all."""

from __future__ import annotations

import os

import httpx
import pytest
import yaml

pytestmark = [pytest.mark.integration, pytest.mark.requires_neo4j]

_ROLE = "org_admin"  # holds the write right in the sample config
_TABLE = "wc_person"
_REPO = os.path.join(os.path.dirname(__file__), "..", "..")


def _config() -> dict:
    with open(os.path.join(_REPO, "tests/fixtures/sample_config.yaml")) as f:
        base = yaml.safe_load(f)
    return {
        "naming": base["naming"],
        "roles": base["roles"],
        "relationships": [],
        "sources": [
            {
                "id": "wc-neo4j",
                "type": "neo4j",
                "host": "localhost",
                "port": int(os.environ["NEO4J_HTTP_PORT"]),
                "database": "neo4j",
            }
        ],
        "domains": [{"id": "graph", "description": "write capability e2e"}],
        "tables": [
            {
                "source_id": "wc-neo4j",
                "domain_id": "graph",
                "schema": "neo4j",
                "table": _TABLE,
                "row_materialize": True,
                "cache_ttl": 300,
                "query_template": (
                    "MATCH (p:WcPerson) RETURN p.person_id AS person_id, p.name AS name "
                    "ORDER BY p.person_id"
                ),
                "columns": [
                    {
                        "name": "person_id",
                        "data_type": "integer",
                        "is_primary_key": True,
                        "visible_to": [_ROLE],
                        "writable_by": [_ROLE],
                    },
                    {
                        "name": "name",
                        "data_type": "varchar",
                        "visible_to": [_ROLE],
                        "writable_by": [_ROLE],
                    },
                ],
            }
        ],
    }


@pytest.fixture(scope="module")
def server(tmp_path_factory):
    from tests.integration.isolated_server import IsolatedServer

    path = tmp_path_factory.mktemp("write_capability_neo4j") / "config.yaml"
    path.write_text(yaml.safe_dump(_config()))
    srv = IsolatedServer(
        "write_capability_neo4j", engine="duckdb", config=str(path), control_plane="sqlite"
    )
    srv.start(timeout=240)
    try:
        yield srv
    finally:
        srv.stop_process()


def _post(server, path: str, body: dict) -> httpx.Response:
    return httpx.post(
        f"{server.base_url}{path}", json=body, headers={"X-Provisa-Role": _ROLE}, timeout=120
    )


@pytest.mark.parametrize(
    "sql, operation",
    [
        (f"INSERT INTO graph.{_TABLE} (person_id, name) VALUES (1, 'ada')", "INSERT"),
        (f"UPDATE graph.{_TABLE} SET name = 'bea' WHERE person_id = 1", "UPDATE"),
        (f"DELETE FROM graph.{_TABLE} WHERE person_id = 1", "DELETE"),
    ],
)
def test_sql_refuses_a_write_its_source_cannot_take_naming_it(server, sql, operation):
    resp = _post(server, "/data/sql", {"sql": sql})
    assert resp.status_code == 400, resp.text
    body = resp.json()
    assert body["code"] == "data.write_not_supported", body
    assert body["params"] == {"table": _TABLE, "operation": operation}, body


def test_graphql_offers_no_mutation_for_the_table(server):
    resp = _post(
        server, "/data/graphql", {"query": "{ __schema { mutationType { fields { name } } } }"}
    )
    assert resp.status_code == 200, resp.text
    mutation_type = resp.json()["data"]["__schema"]["mutationType"]
    fields = [f["name"].lower() for f in (mutation_type or {}).get("fields") or []]
    assert not [f for f in fields if "wcperson" in f.replace("_", "")], fields


def test_graphql_names_the_insert_it_does_not_offer(server):
    resp = _post(
        server,
        "/data/graphql",
        {
            "query": 'mutation { g__insertWcPerson(input: {person_id: 1, name: "ada"}) { affected_rows } }'
        },
    )
    assert "g__insertWcPerson" in resp.text, resp.text
    assert resp.status_code != 200 or '"errors"' in resp.text, resp.text
