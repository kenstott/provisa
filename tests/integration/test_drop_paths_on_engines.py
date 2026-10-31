# Copyright (c) 2026 Kenneth Stott
# Canary: 5e0b7c29-d146-48a3-9f72-a1c63d8e04b7
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What a drop on a graph frame writes (REQ-2135), run by a real server over real rows in the test
stack's Postgres on each engine: a node dropped on a node asks allShortestPaths with a hop limit and
a row cap, a class pill's schema path on Cypher, and a SPARQL frame's OPTIONAL path. Parsing is not
enough: each answers the rows it must, and the rows a path did not reach stay. No path goes through
a parameterized table, and a join declared from both ends answers once."""

# Requirements: REQ-2135, REQ-348

from __future__ import annotations

import json
import os

import httpx
import pytest
import pytest_asyncio

pytestmark = [
    pytest.mark.requires_trino,
    pytest.mark.integration,
    pytest.mark.asyncio(loop_scope="session"),
]

_ORG = "drop_paths_e2e"
_CONFIG = "tests/fixtures/node_paths_config.yaml"
_SCHEMA = "node_paths_e2e"


async def _pg():
    import asyncpg

    return await asyncpg.connect(
        host=os.environ.get("PG_HOST", "localhost"),
        port=int(os.environ.get("PG_PORT", "5432")),
        user=os.environ.get("PG_USER", "provisa"),
        password=os.environ.get("PG_PASSWORD", "provisa"),
        database=os.environ.get("PG_DATABASE", "provisa"),
        timeout=15,
    )


async def _seed() -> None:
    conn = await _pg()
    try:
        await conn.execute(f"DROP SCHEMA IF EXISTS {_SCHEMA} CASCADE")
        await conn.execute(f"CREATE SCHEMA {_SCHEMA}")
        await conn.execute(f"CREATE TABLE {_SCHEMA}.pets (id int PRIMARY KEY, name text)")
        await conn.execute(f"CREATE TABLE {_SCHEMA}.users (id int PRIMARY KEY, name text)")
        await conn.execute(
            f"CREATE TABLE {_SCHEMA}.inquiries (id int PRIMARY KEY, pet_id int, user_id int)"
        )
        await conn.execute(
            f"CREATE TABLE {_SCHEMA}.assignments_by_breed "
            "(id int PRIMARY KEY, pet_id int, user_id int, _nf_breed_name text)"
        )
        await conn.execute(f"INSERT INTO {_SCHEMA}.pets VALUES (1, 'Rex'), (2, 'Tom'), (3, 'Ivy')")
        await conn.execute(f"INSERT INTO {_SCHEMA}.users VALUES (1, 'Ann'), (2, 'Bo')")
        await conn.execute(
            f"INSERT INTO {_SCHEMA}.inquiries VALUES (10, 1, 1), (11, 2, 1), (12, NULL, 2)"
        )
        # Would join Bo to Ivy, whom no inquiry reaches, if a path read it.
        await conn.execute(
            f"INSERT INTO {_SCHEMA}.assignments_by_breed VALUES (20, 3, 2, 'beagle')"
        )
    finally:
        await conn.close()


async def _drop() -> None:
    conn = await _pg()
    try:
        await conn.execute(f"DROP SCHEMA IF EXISTS {_SCHEMA} CASCADE")
    finally:
        await conn.close()


@pytest_asyncio.fixture(scope="module", params=["duckdb", "pg", "trino"])
async def server(request):
    from tests.integration.isolated_server import IsolatedServer, drop_org_schema

    await _seed()
    org = f"{_ORG}_{request.param}"
    instance = IsolatedServer(
        org,
        engine=request.param,
        config=_CONFIG,
        env={"PROVISA_PUBLIC_URL": "https://paths.example"},  # a subject's IRI starts with it
    )
    instance.start()
    try:
        yield instance
    finally:
        instance.stop_process()
        await _drop()
        await drop_org_schema(org)


async def _cypher(server, query: str) -> dict:
    async with httpx.AsyncClient(base_url=server.base_url, timeout=120.0) as client:
        resp = await client.post(
            "/data/cypher",
            json={"query": query, "params": {}},
            headers={"X-Provisa-Role": "org_admin"},
        )
    assert resp.status_code == 200, resp.text
    return resp.json()


async def _labels(server) -> dict[str, str]:
    """Each table's label as the UI writes it: the sidebar's compound label."""
    async with httpx.AsyncClient(base_url=server.base_url, timeout=120.0) as client:
        resp = await client.get("/data/graph-schema", headers={"X-Provisa-Role": "org_admin"})
    assert resp.status_code == 200, resp.text
    return {
        n["table_label"].lower(): (
            f"{n['domain_label']}:{n['table_label']}" if n["domain_label"] else n["table_label"]
        )
        for n in resp.json()["node_labels"]
    }


def _path_nodes(row: dict) -> list[dict]:
    path = row["p"]
    return (json.loads(path) if isinstance(path, str) else path)["nodes"]


async def test_a_node_on_a_node_finds_the_path_through_the_class_between(server):
    labels = await _labels(server)
    body = await _cypher(
        server,
        f"MATCH p = allShortestPaths((a:{labels['users']})-[*..4]-(b:{labels['pets']}))\n"
        "WHERE a.id = 1 AND b.id = 2\nRETURN p\nLIMIT 25",
    )
    assert len(body["rows"]) == 1, body
    nodes = _path_nodes(body["rows"][0])
    assert [n["properties"]["id"] for n in nodes] == [1, 11, 2]
    # The frame finds each end's key by the label its nodes come back with.
    assert nodes[0]["label"] == labels["users"]


async def test_two_nodes_nothing_joins_have_no_path(server):
    labels = await _labels(server)
    body = await _cypher(
        server,
        f"MATCH p = shortestPath((a:{labels['users']})-[*..4]-(b:{labels['pets']}))\n"
        "WHERE a.id = 2 AND b.id = 3\nRETURN p\nLIMIT 25",
    )
    assert body["rows"] == []


async def test_a_class_pill_is_read_through_its_schema_path(server):
    labels = await _labels(server)
    body = await _cypher(
        server,
        f"MATCH (a:{labels['users']})\n"
        f"OPTIONAL MATCH (a)-[r1:SUBMITTED_BY]-(i:{labels['inquiries']})-[r2:ABOUT]-(b:{labels['pets']})\n"
        "RETURN a.name AS u, b.name AS pet ORDER BY u, pet",
    )
    assert [(r["u"], r["pet"]) for r in body["rows"]] == [
        ("Ann", "Rex"),
        ("Ann", "Tom"),
        ("Bo", None),
    ]


async def test_a_sparql_drop_keeps_the_rows_its_path_does_not_reach(server):
    async with httpx.AsyncClient(base_url=server.base_url, timeout=120.0) as client:
        vocab = await client.get("/data/sparql/vocabulary", headers={"X-Provisa-Role": "org_admin"})
        assert vocab.status_code == 200, vocab.text
        classes = {c["iri"].rsplit("/", 1)[1]: c["iri"] for c in vocab.json()["classes"]}
        rel = vocab.json()["prefixes"]["rel"]
        resp = await client.post(
            "/data/sparql",
            content=(
                f"SELECT * WHERE {{ ?u a <{classes['users']}> . "
                f"OPTIONAL {{ ?i <{rel}SUBMITTED_BY> ?u . ?i a <{classes['inquiries']}> . "
                f"?i <{rel}ABOUT> ?p . ?p a <{classes['pets']}> . }} }} LIMIT 250"
            ),
            headers={
                "Content-Type": "application/sparql-query",
                "Accept": "application/sparql-results+json",
                "X-Provisa-Role": "org_admin",
            },
        )
    assert resp.status_code == 200, resp.text
    bindings = resp.json()["results"]["bindings"]
    reached = sorted(b["p"]["value"].rsplit("/", 1)[1] for b in bindings if "p" in b)
    assert reached == ["1", "2"]
    assert len({b["u"]["value"] for b in bindings}) == 2


@pytest.mark.parametrize("func", ["shortestPath", "allShortestPaths"])
async def test_a_path_function_goes_round_a_parameterized_table(server, func):
    labels = await _labels(server)
    body = await _cypher(
        server,
        f"MATCH p = {func}((a:{labels['users']})-[*..4]-(b:{labels['pets']}))\n"
        "WHERE a.id = 2 AND b.id = 3\nRETURN p\nLIMIT 25",
    )
    assert body["rows"] == []


async def test_an_any_path_goes_round_a_parameterized_table(server):
    labels = await _labels(server)
    body = await _cypher(
        server,
        f"MATCH (a:{labels['users']})-[*1..4]-(b:{labels['pets']})\n"
        "RETURN a.name AS u, b.name AS pet ORDER BY u, pet",
    )
    assert ("Bo", "Ivy") not in [(r["u"], r["pet"]) for r in body["rows"]]
    assert ("Ann", "Rex") in [(r["u"], r["pet"]) for r in body["rows"]]


@pytest.mark.parametrize(
    "pattern",
    ["allShortestPaths((a:{users})-[*..4]-(b:{users}))", "(a:{users})-[*1..4]-(b:{users})"],
    ids=["all-shortest", "any-path"],
)
async def test_a_recursive_walk_goes_round_a_parameterized_table(server, pattern):
    labels = await _labels(server)
    body = await _cypher(
        server,
        f"MATCH p = {pattern.format(users=labels['users'])}\nWHERE a.id = 2\nRETURN b.name AS u",
    )
    assert "Ann" not in [r["u"] for r in body["rows"]]


async def test_a_walk_to_a_free_end_goes_round_a_parameterized_table(server):
    labels = await _labels(server)
    body = await _cypher(
        server,
        f"MATCH (a:{labels['users']})-[*1..2]-(b) WHERE a.id = 2 RETURN DISTINCT b.id AS id",
    )
    assert 20 not in [r["id"] for r in body["rows"]]
    assert 12 in [r["id"] for r in body["rows"]]


async def test_a_path_named_to_end_on_a_parameterized_table_is_refused_by_name(server):
    labels = await _labels(server)
    async with httpx.AsyncClient(base_url=server.base_url, timeout=120.0) as client:
        resp = await client.post(
            "/data/cypher",
            json={
                "query": (
                    f"MATCH p = shortestPath((a:{labels['users']})"
                    f"-[*..4]-(b:{labels['assignmentsbybreed']})) RETURN p"
                ),
                "params": {},
            },
            headers={"X-Provisa-Role": "org_admin"},
        )
    assert resp.status_code == 400, resp.text
    assert "a path cannot include" in resp.text


async def test_a_join_declared_from_both_ends_answers_once(server):
    labels = await _labels(server)
    body = await _cypher(
        server,
        f"MATCH p = allShortestPaths((a:{labels['users']})-[*..4]-(b:{labels['inquiries']}))\n"
        "WHERE a.id = 2 AND b.id = 12\nRETURN p\nLIMIT 25",
    )
    assert len(body["rows"]) == 1, body


async def test_the_vocabulary_and_the_schema_say_what_a_drop_needs(server):
    async with httpx.AsyncClient(base_url=server.base_url, timeout=120.0) as client:
        headers = {"X-Provisa-Role": "org_admin"}
        vocab = (await client.get("/data/sparql/vocabulary", headers=headers)).json()
        schema = (await client.get("/data/graph-schema", headers=headers)).json()
    classes = {c["iri"].rsplit("/", 1)[1]: c for c in vocab["classes"]}
    assert classes["assignments_by_breed"]["parameterized"] is True
    assert classes["users"]["parameterized"] is False
    joins = {e["type"]: e["join"] for e in vocab["edges"]}
    assert joins["SUBMITTED"] == joins["SUBMITTED_BY"]
    assert joins["SUBMITTED"] != joins["ABOUT"]
    rels = {r["type"]: r["join"] for r in schema["relationship_types"]}
    assert rels["SUBMITTED"] == rels["SUBMITTED_BY"]


async def test_a_drop_on_an_open_pattern_keeps_its_rows_and_fans_out(server):
    async with httpx.AsyncClient(base_url=server.base_url, timeout=120.0) as client:
        vocab = await client.get("/data/sparql/vocabulary", headers={"X-Provisa-Role": "org_admin"})
        classes = {c["iri"].rsplit("/", 1)[1]: c["iri"] for c in vocab.json()["classes"]}
        rel = vocab.json()["prefixes"]["rel"]
        resp = await client.post(
            "/data/sparql",
            content=(
                f"SELECT * WHERE {{ ?s ?p ?o . OPTIONAL {{ ?s <{rel}SUBMITTED_BY> ?u . "
                f"?u a <{classes['users']}> . }} }} LIMIT 250"
            ),
            headers={
                "Content-Type": "application/sparql-query",
                "Accept": "application/sparql-results+json",
                "X-Provisa-Role": "org_admin",
            },
        )
    assert resp.status_code == 200, resp.text
    bindings = resp.json()["results"]["bindings"]
    assert any("/users/" in b["s"]["value"] and "u" not in b for b in bindings)
    assert any("/inquiries/" in b["s"]["value"] and "u" in b for b in bindings)
