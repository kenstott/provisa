# Copyright (c) 2026 Kenneth Stott
# Canary: 6a1d9f38-2e7c-4b54-8f06-c3e5a7d1b920
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E: a row-level replica table of a Neo4j source is read by key or not at all (REQ-1915).

A real isolated server (DuckDB engine — no connector for neo4j, so row-level replication is the
table's reach; SQLite control plane) over a real Neo4j. The table is declared
``row_materialize`` in the config, as the perf fragment's ``bench_order_node`` is. An unfiltered
read used to copy every node of the label into the worker (Bolt; Neo4j ran out of memory) or fail
with ``relation … does not exist`` (pgwire). It is refused at planning, naming the table and its
key, over HTTP; a read that binds the key is answered from the source, by key."""

# Requirements: REQ-1915, REQ-1865

from __future__ import annotations

import os

import httpx
import pytest
import yaml

pytestmark = [pytest.mark.integration, pytest.mark.requires_neo4j]

_ROLE = "org_admin"
_ORG = "row_level_key_e2e"
_TABLE = "rl_order"
_SEED = [(1, "east"), (2, "west"), (3, "east")]
_REPO = os.path.join(os.path.dirname(__file__), "..", "..")


def _neo4j_port() -> int:
    return int(os.environ["NEO4J_HTTP_PORT"])


@pytest.fixture(scope="module")
def seeded():
    tx = f"http://localhost:{_neo4j_port()}/db/neo4j/tx/commit"
    create = " ".join(f"CREATE (:RlOrder {{order_id: {i}, region: '{r}'}})" for i, r in _SEED)
    with httpx.Client(timeout=60) as client:
        for stmt in ("MATCH (o:RlOrder) DETACH DELETE o", create):
            resp = client.post(tx, json={"statements": [{"statement": stmt}]})
            assert resp.status_code == 200 and resp.json()["errors"] == [], resp.text
        yield
        client.post(tx, json={"statements": [{"statement": "MATCH (o:RlOrder) DETACH DELETE o"}]})


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
                "id": "rl-neo4j",
                "type": "neo4j",
                "host": "localhost",
                "port": _neo4j_port(),
                "database": "neo4j",
            }
        ],
        "domains": [{"id": "rowlevel", "description": "row-level key e2e"}],
        "tables": [
            {
                "source_id": "rl-neo4j",
                "domain_id": "rowlevel",
                "schema": "neo4j",
                "table": _TABLE,
                "row_materialize": True,
                "cache_ttl": 300,
                "query_template": (
                    "MATCH (o:RlOrder) RETURN o.order_id AS order_id, o.region AS region "
                    "ORDER BY o.order_id"
                ),
                "columns": [
                    {
                        "name": "order_id",
                        "data_type": "integer",
                        "is_primary_key": True,
                        "visible_to": [_ROLE],
                    },
                    {"name": "region", "data_type": "varchar", "visible_to": [_ROLE]},
                ],
            }
        ],
    }
    path = tmp_path_factory.mktemp("row_level") / "config.yaml"
    path.write_text(yaml.safe_dump(cfg))
    srv = IsolatedServer(_ORG, engine="duckdb", config=str(path), control_plane="sqlite")
    srv.start(timeout=240)
    try:
        yield srv
    finally:
        srv.stop_process()


def _sql(server, sql: str) -> httpx.Response:
    return httpx.post(
        f"{server.base_url}/data/sql",
        json={"sql": sql},
        headers={"X-Provisa-Role": _ROLE},
        timeout=server.request_timeout + 30,
    )


def test_an_unfiltered_read_is_refused_over_http_naming_the_table_and_key(server):
    resp = _sql(server, f"SELECT order_id FROM rowlevel.{_TABLE} LIMIT 1")
    assert resp.status_code == 400, resp.text
    body = resp.json()
    assert body["code"] == "data.row_level_key_required", body
    assert body["params"] == {"table": _TABLE, "key": "order_id"}
    assert _TABLE in body["detail"] and '"order_id"' in body["detail"]


def test_a_keyed_read_is_answered_by_key(server):
    resp = _sql(server, f"SELECT order_id, region FROM rowlevel.{_TABLE} WHERE order_id = 2")
    assert resp.status_code == 200, resp.text
    assert resp.json()["data"]["sql"] == [{"order_id": 2, "region": "west"}]
    several = _sql(
        server, f"SELECT order_id FROM rowlevel.{_TABLE} WHERE order_id IN (1, 3) ORDER BY order_id"
    )
    assert several.status_code == 200, several.text
    assert [r["order_id"] for r in several.json()["data"]["sql"]] == [1, 3]


def test_the_refusal_did_not_copy_the_table(server):
    """After the refusals above the replica holds only the keys the keyed reads fetched: a key no
    read asked for is fetched now, and the unfiltered read is still refused."""
    again = _sql(server, f"SELECT count(*) AS n FROM rowlevel.{_TABLE}")
    assert again.status_code == 400 and again.json()["code"] == "data.row_level_key_required"
