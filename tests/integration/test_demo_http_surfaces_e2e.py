# Copyright (c) 2026 Kenneth Stott
# Canary: 43a272c9-63df-4898-8735-1398c5ff50dc
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E: the default demo's HTTP surfaces, asked as a client asks them.

The ``test`` instance: the smoke check's isolated instance of the default demo (native DuckDB
engine, its own embedded PostgreSQL control plane, the pet store, the shelter's GraphQL API and the
gRPC demo on leased ports). These cases ran in the browser lane, which never opened a page for them
(test-overlap audit, maintainer 2026-11-20): Cypher reads across the demo's sources and what a
node, an edge and a path look like; Cypher writes and their counts; the schema version; the
unauthenticated endpoints; the REST and SSE reads of a table; the ingest refusals; the model's
relationships; a source's write operations registered as commands; and the export of the demo's
graph into a Neo4j server.
"""

# Requirements: REQ-027, REQ-219, REQ-222, REQ-331, REQ-345, REQ-414, REQ-536, REQ-537, REQ-539,
# Requirements: REQ-594, REQ-598, REQ-670, REQ-713, REQ-717, REQ-719, REQ-720, REQ-750, REQ-777,
# Requirements: REQ-797, REQ-1924

from __future__ import annotations

import os
import re
import time
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import httpx
import pytest

from tests.smoke import test_main_is_startable as smoke
from tests.smoke.test_main_is_startable import Instance

# isolated: the instance is the smoke check's, which writes the demo's files and owns its ports.
pytestmark = [pytest.mark.integration, pytest.mark.isolated]

_ADMIN = {"X-Provisa-Role": "org_admin"}
_TIMEOUT = 120.0


@pytest.fixture(scope="module")
def instance() -> Iterator[Instance]:
    yield from smoke.instance.__wrapped__()


def _cypher(inst: Instance, query: str, params: dict | None = None) -> httpx.Response:
    body: dict[str, Any] = {"query": query}
    if params is not None:
        body["params"] = params
    return httpx.post(f"{inst.url}/data/cypher", json=body, headers=_ADMIN, timeout=_TIMEOUT)


def _cypher_ok(inst: Instance, query: str) -> dict:
    resp = _cypher(inst, query)
    assert resp.status_code == 200, resp.text[:2000]
    body = resp.json()
    assert "error" not in body, body
    assert "columns" in body, body
    return body


def _admin_gql(inst: Instance, query: str) -> dict:
    resp = httpx.post(
        f"{inst.url}/admin/graphql", json={"query": query}, headers=_ADMIN, timeout=_TIMEOUT
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert "errors" not in body, body["errors"]
    return body["data"]


# --- Cypher reads across the demo's sources -----------------------------------------------------


def test_a_breed_filter_across_the_pet_store_and_the_shelter_resolves_breed_name(instance):
    """A pet reaches the assignments of its breed through animal_breeds (BREED_INFO, then
    ASSIGNMENT_BREED); every pattern hangs off the inquiry, so no pattern is a cross join (V002)."""
    _cypher_ok(
        instance,
        """
        MATCH (a:Inquiries)
        OPTIONAL MATCH (a:Inquiries)-[r1:HAS_PETS]->(b:Pets)
        OPTIONAL MATCH (b:Pets)-[r2:BREED_INFO]->(e:AnimalBreeds)
        OPTIONAL MATCH (c:Assignments)-[r4:ASSIGNMENT_BREED]->(e:AnimalBreeds)
        OPTIONAL MATCH (c:Assignments)-[r3:ASSIGNMENT_EMPLOYEE]->(d:Employees)
        OPTIONAL MATCH (a)-[rUsers:SUBMITTED_BY]-(mUsers:PetStore:Users)
        WHERE NOT b.id IN [2]
        AND NOT c.breedName IN ['Maine Coon']
        RETURN a, b, c, d, e, rUsers, mUsers, r1, r2, r3, r4
        """,
    )


def test_union_all_of_node_and_relationship_emails_runs(instance):
    """REQ-777: UNION ALL of an unlabelled node read and an unlabelled relationship read."""
    _cypher_ok(
        instance,
        """MATCH (n)
WHERE n.email IS NOT NULL
RETURN DISTINCT "node" AS entity, n.email AS email
LIMIT 25
UNION ALL
MATCH ()-[r]-()
WHERE r.email IS NOT NULL
RETURN DISTINCT "relationship" AS entity, r.email AS email
LIMIT 25""",
    )


def test_a_variable_length_cross_source_path_reaches_its_nodes(instance):
    """Inquiries -> Pets -> AnimalBreeds -> Assignments -> Employees, within five hops.

    65 unique nodes since J750 removed the pets<->assignments breed_name relationships: an
    inquiry's pet reaches the employees through animal_breeds (BREED_INFO, then ASSIGNMENT_BREED
    and ASSIGNMENT_EMPLOYEE). Observed alike in CI (run 42187604573) and a local core-lane run; not
    derived independently of the engine."""
    body = _cypher_ok(
        instance,
        "MATCH r = (n:PetStore:Inquiries)-[*..5]-(mEmployees:Shelter:Employees) RETURN r LIMIT 2500",
    )
    nodes = {
        f"{node['label']}:{node['id']}"
        for row in body["rows"]
        for node in (row.get("r") or {}).get("nodes", [])
    }
    assert len(nodes) == 65, sorted(nodes)


def test_a_variable_length_path_returning_its_ends_answers_nodes(instance):
    body = _cypher_ok(
        instance,
        "MATCH r = ((a:PetStore:Inquiries)-[*..5]-(b:Shelter:Employees)) RETURN a, b LIMIT 5",
    )
    assert "a" in body["columns"] and "b" in body["columns"], body["columns"]
    assert body["rows"], body
    first = body["rows"][0]
    for end in ("a", "b"):
        assert first[end]["label"], first
        assert isinstance(first[end]["properties"], dict), first


# --- What a node, an edge and a path look like (REQ-750) -----------------------------------------


def test_a_node_is_answered_with_id_label_table_label_and_properties(instance):
    body = _cypher_ok(instance, "MATCH (n:Pets) RETURN n LIMIT 1")
    assert body["rows"], body
    node = body["rows"][0]["n"]
    assert set(node) == {"id", "label", "tableLabel", "properties"}, node
    assert node["label"].endswith("Pets"), node
    assert isinstance(node["properties"], dict), node


def test_an_edge_is_answered_with_identity_start_end_type_and_properties(instance):
    body = _cypher_ok(instance, "MATCH (n:Inquiries)-[r]->(m) RETURN r LIMIT 1")
    assert body["rows"], body
    edge = body["rows"][0]["r"]
    for key in ("identity", "start", "end", "type", "properties", "startNode", "endNode"):
        assert key in edge, edge
    assert edge["start"] == edge["startNode"]["id"], edge
    assert edge["end"] == edge["endNode"]["id"], edge


def test_a_path_is_answered_with_its_nodes_and_edges(instance):
    body = _cypher_ok(
        instance, "MATCH p = (a:Inquiries)-[:HAS_PETS*1..2]->(b:Pets) RETURN p LIMIT 1"
    )
    assert body["rows"], body
    path = body["rows"][0]["p"]
    assert set(path) == {"nodes", "edges"}, path
    assert len(path["nodes"]) == len(path["edges"]) + 1, path
    assert all(e["type"] == "HAS_PETS" for e in path["edges"]), path


def test_a_cypher_answer_says_whether_it_came_from_the_cache(instance):
    """REQ-536: every data response names its cache status; a HIT also names its age."""
    resp = _cypher(instance, "MATCH (n:Meta:RegisteredTables) RETURN n LIMIT 1")
    assert resp.status_code == 200, resp.text
    status = resp.headers.get("x-provisa-cache")
    assert status in ("HIT", "MISS"), dict(resp.headers)
    if status == "HIT":
        assert int(resp.headers["x-provisa-cache-age"]) >= 0, dict(resp.headers)


# --- Cypher writes and their counts (REQ-670) ----------------------------------------------------


_SENTINEL = 99981


def test_cypher_create_set_and_delete_answer_the_rows_they_affected(instance):
    delete = f"MATCH (n:Users) WHERE n.id = {_SENTINEL} DELETE n"
    _cypher(instance, delete)  # a row an aborted earlier run left behind
    try:
        for statement in (
            f"CREATE (n:Users {{id: {_SENTINEL}, name: 'E2E Cypher', "
            "email: 'e2e-cypher@example.com'})",
            f"MATCH (n:Users) WHERE n.id = {_SENTINEL} SET n.name = 'E2E Cypher Updated'",
            delete,
        ):
            resp = _cypher(instance, statement)
            assert resp.status_code == 200, f"{statement}: {resp.text}"
            body = resp.json()
            assert "error" not in body, body
            assert body["type"] == "cypher", body
            assert body["affected_rows"] == 1, f"{statement}: {body}"
    finally:
        _cypher(instance, delete)


# --- Schema version (REQ-537) --------------------------------------------------------------------


def test_the_schema_version_is_the_boot_id_and_the_rebuild_counter(instance):
    first = httpx.get(f"{instance.url}/data/schema-version", timeout=_TIMEOUT)
    assert first.status_code == 200, first.text
    version = first.json()["version"]
    # The boot id is a UUID written as its 32 hex digits (uuid4().hex).
    assert re.fullmatch(r"[0-9a-f]{32}-\d+", version), version
    # No rebuild between the two reads: the same version.
    second = httpx.get(f"{instance.url}/data/schema-version", timeout=_TIMEOUT)
    assert second.json()["version"] == version


# --- Unauthenticated endpoints (REQ-539, REQ-594) ------------------------------------------------


def test_health_answers_with_no_credentials_on_get_and_head(instance):
    got = httpx.get(f"{instance.url}/health", timeout=_TIMEOUT)
    assert got.status_code == 200, got.text
    assert "status" in got.json(), got.json()
    assert httpx.head(f"{instance.url}/health", timeout=_TIMEOUT).status_code == 200


def test_setup_status_answers_with_no_credentials(instance):
    resp = httpx.get(f"{instance.url}/setup/status", timeout=_TIMEOUT)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert "needs_setup" in body and "demo_mode" in body, body


@pytest.mark.parametrize("path", ["/openapi.json", "/docs"])
def test_the_api_description_needs_no_credentials(instance, path):
    resp = httpx.get(f"{instance.url}{path}", timeout=_TIMEOUT)
    assert resp.status_code not in (401, 403), resp.text


# --- REST and SSE reads of a table (REQ-219, REQ-222) --------------------------------------------


def test_rest_reads_a_table_as_a_json_list_and_honours_limit(instance):
    every = httpx.get(f"{instance.url}/data/rest/pet-store/pets", timeout=_TIMEOUT)
    assert every.status_code == 200, every.text
    rows = every.json()["data"]
    assert rows and all(isinstance(r, dict) for r in rows), rows
    page = httpx.get(
        f"{instance.url}/data/rest/pet-store/pets",
        params={"limit": 2, "offset": 0},
        timeout=_TIMEOUT,
    )
    assert page.status_code == 200, page.text
    assert len(page.json()["data"]) == 2, page.json()


def test_a_table_subscription_is_an_event_stream(instance):
    """The stream never closes on its own (a keepalive loop): its first chunk is read and the
    connection dropped. It opens with a keepalive comment or an event."""
    with httpx.stream("GET", f"{instance.url}/data/subscribe/pets", timeout=_TIMEOUT) as resp:
        assert resp.status_code == 200, resp.read()
        assert "text/event-stream" in resp.headers["content-type"], dict(resp.headers)
        first = next(resp.iter_text())
    assert re.match(r"[:\s]|data:|event:", first), first


# --- Ingest refusals (REQ-331, REQ-335) ----------------------------------------------------------


@pytest.mark.parametrize(
    "path", ["/data/ingest/nonexistent/table", "/data/ingest/csv/missing_table"]
)
def test_an_ingest_to_an_unknown_source_or_table_is_not_found(instance, path):
    resp = httpx.post(f"{instance.url}{path}", json={"test": "data"}, timeout=_TIMEOUT)
    assert resp.status_code == 404, resp.text


# --- The model's relationships (REQ-414, REQ-598) ------------------------------------------------


def test_the_demo_declares_relationships_each_with_its_columns_and_cypher_flag(instance):
    data = _admin_gql(
        instance,
        "{ relationships { id sourceTableName targetTableName sourceColumn targetColumn "
        "disableCypher } }",
    )
    relationships = data["relationships"]
    assert relationships, data
    for rel in relationships:
        assert rel["sourceTableName"] and rel["targetTableName"] and rel["sourceColumn"], rel
        assert isinstance(rel["disableCypher"], bool), rel


# --- A source's write operations as commands (REQ-1924) ------------------------------------------


_STAMP = int(time.time())
_ORDER_ID = 900000 + (_STAMP % 99999)


def _petstore_base(inst: Instance) -> str:
    """The pet store's address, as it announced it when it started. The source's stored path
    cannot say it: a source's text fields are stored as written (``${env:PETSTORE_BASE_URL...}``)
    and resolved only where they are used."""
    log = (inst.log_dir / "petstore.log").read_text()
    found = re.search(r"Uvicorn running on (http://\S+)", log)
    assert found, log[-2000:]
    return f"{found.group(1)}/api/v3"


def _register(inst: Instance, name: str, operation: str) -> None:
    resp = httpx.post(
        f"{inst.url}/admin/actions/functions",
        headers=_ADMIN,
        json={
            "name": name,
            "sourceId": "petstore-api",
            "functionName": operation,
            "domainId": "pet-store",
            "writableBy": ["org_admin"],
        },
        timeout=_TIMEOUT,
    )
    assert resp.status_code == 200, resp.text


@pytest.fixture
def commands(instance) -> Iterator[list[str]]:
    """Names the test registers; each is removed when it ends."""
    names: list[str] = []
    yield names
    for name in names:
        httpx.delete(
            f"{instance.url}/admin/actions/functions/{name}", headers=_ADMIN, timeout=_TIMEOUT
        )


def _sql(inst: Instance, sql: str) -> httpx.Response:
    return httpx.post(f"{inst.url}/data/sql", json={"sql": sql}, headers=_ADMIN, timeout=_TIMEOUT)


def test_write_operations_are_offered_and_adding_the_source_registers_only_the_declared(instance):
    data = _admin_gql(
        instance,
        '{ availableFunctions(sourceId: "petstore-api", schemaName: "openapi") { name } }',
    )
    offered = {f["name"] for f in data["availableFunctions"]}
    assert {"placeOrder", "deleteOrder", "addPet"} <= offered, offered
    assert "getInventory" not in offered  # a GET is a table, not a command
    actions = httpx.get(f"{instance.url}/admin/actions", headers=_ADMIN, timeout=_TIMEOUT).json()
    # The install config declares one operation as a command, add_pet (addPet, REQ-2134).
    registered = [
        f["functionName"] for f in actions["functions"] if f["sourceId"] == "petstore-api"
    ]
    assert registered == ["addPet"], registered


def test_a_registered_operation_takes_a_json_object_and_reaches_the_remote_unchanged(
    instance, commands
):
    place, cancel = f"itest_place_order_{_STAMP}", f"itest_delete_order_{_STAMP}"
    for name, operation in ((place, "placeOrder"), (cancel, "deleteOrder")):
        _register(instance, name, operation)
        commands.append(name)
    actions = httpx.get(f"{instance.url}/admin/actions", headers=_ADMIN, timeout=_TIMEOUT).json()
    registered = next(f for f in actions["functions"] if f["name"] == place)
    assert registered["implKind"] == "source_operation", registered
    assert [(a["name"], a["type"]) for a in registered["arguments"]] == [("body", "json")]

    # GraphQL: the command is a mutation field (domain prefix plus the convention's casing of the
    # name, which keeps the underscore before a run of digits).
    schema = httpx.post(
        f"{instance.url}/data/graphql",
        json={"query": "{ __schema { mutationType { fields { name } } } }"},
        headers=_ADMIN,
        timeout=_TIMEOUT,
    ).json()
    fields = [
        f["name"]
        for f in schema["data"]["__schema"]["mutationType"]["fields"]
        if f["name"].lower().replace("_", "").endswith(f"placeorder{_STAMP}")
    ]
    assert len(fields) == 1, fields
    # A field the pet store's spec does not declare is passed through all the same.
    order = {"id": _ORDER_ID, "petId": 7, "quantity": 2, "status": "placed", "note": "itest"}
    called = httpx.post(
        f"{instance.url}/data/graphql",
        json={
            "query": f"mutation($o: JSON) {{ {fields[0]}(body: $o) }}",
            "variables": {"o": order},
        },
        headers=_ADMIN,
        timeout=_TIMEOUT,
    )
    assert called.status_code == 200, called.text
    answer = called.json()
    assert "errors" not in answer, answer
    assert answer["data"][fields[0]] == [order]
    petstore = _petstore_base(instance)
    assert httpx.get(f"{petstore}/store/order/{_ORDER_ID}", timeout=_TIMEOUT).json() == order

    # SQL: each argument is a JSON literal; the path parameter fills the path.
    deleted = _sql(instance, f"SELECT * FROM {cancel}('{_ORDER_ID}')")
    assert deleted.status_code == 200, deleted.text
    assert httpx.get(f"{petstore}/store/order/{_ORDER_ID}", timeout=_TIMEOUT).status_code == 404


def test_the_remotes_refusal_comes_back_as_the_remote_stated_it(instance, commands):
    refused = f"itest_refused_order_{_STAMP}"
    _register(instance, refused, "placeOrder")
    commands.append(refused)
    # No order at all: the pet store refuses with 400, "No Order provided".
    resp = _sql(instance, f"SELECT * FROM {refused}('null')")
    assert resp.status_code == 422, resp.text
    body = resp.json()
    assert body["code"] == "functions.remote_refused", body
    assert body["params"]["remote_status"] == 400, body
    assert "No Order provided" in body["params"]["answer"], body


def test_a_write_cannot_be_composed_into_a_larger_statement(instance, commands):
    composed = f"itest_composed_delete_{_STAMP}"
    _register(instance, composed, "deleteOrder")
    commands.append(composed)
    resp = _sql(
        instance, f"SELECT o.id FROM (SELECT 1 AS id) o JOIN {composed}('{_ORDER_ID}') c ON true"
    )
    assert resp.status_code >= 400, resp.text
    assert "cannot be composed in a query, a view or a materialized view" in resp.text


# --- Export of the demo's graph into Neo4j (REQ-713..720, REQ-797) -------------------------------

#: The backend serves four /data/cypher requests at a time; more only queue server-side.
_DISCOVERY_CONCURRENCY = 4
#: Rows read of each label and relationship type: every label and type reaching Neo4j is the
#: point, not every row (unbounded, the model's own catalog alone is tens of thousands of nodes).
_DISCOVERY_ROWS = 25
#: Relationship types queried per wave, until a wave yields an edge.
_REL_WAVE = 25
_EXPORT_BATCH = 200


def _neo4j_url() -> str:
    return f"http://localhost:{os.environ['NEO4J_HTTP_PORT']}"


def _neo4j_tx(statements: list[str]) -> dict:
    resp = httpx.post(
        f"{_neo4j_url()}/db/neo4j/tx/commit",
        json={"statements": [{"statement": s} for s in statements]},
        headers={"Accept": "application/json"},
        timeout=_TIMEOUT,
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["errors"] == [], body["errors"]
    return body


def _wait_for_neo4j(deadline_s: float = 90.0) -> None:
    """The transactional endpoint, not the discovery root: the root answers before the database
    accepts queries, and an export in that window is a 502."""
    deadline = time.monotonic() + deadline_s
    while True:
        try:
            if httpx.post(
                f"{_neo4j_url()}/db/neo4j/tx/commit", json={"statements": []}, timeout=10
            ).is_success:
                return
        except httpx.TransportError:
            pass  # not listening yet: the deadline below decides
        assert time.monotonic() < deadline, f"Neo4j at {_neo4j_url()} not ready in {deadline_s}s"
        time.sleep(1.5)


def _rows_or_none(inst: Instance, query: str) -> list[dict] | None:
    """A label or type whose read needs a parameter (a required native filter) is refused, and
    not exported."""
    resp = _cypher(inst, query)
    if resp.status_code != 200:
        return None
    body = resp.json()
    return None if "error" in body else body["rows"]


def _collect(rows: list[dict], nodes: dict[Any, dict], edges: dict[tuple, dict]) -> None:
    def node(n: dict) -> None:
        nodes.setdefault(
            n["id"], {"id": n["id"], "tableLabel": n["tableLabel"], "properties": n["properties"]}
        )

    for row in rows:
        for value in row.values():
            if not isinstance(value, dict):
                continue
            if {"identity", "startNode", "endNode"} <= value.keys():
                # Keyed as Neo4j's MERGE deduplicates: (start, end, type).
                edges[(value["start"], value["end"], value["type"])] = {
                    "start": value["start"],
                    "end": value["end"],
                    "type": value["type"],
                    "startNodeLabel": value["startNode"]["tableLabel"],
                    "endNodeLabel": value["endNode"]["tableLabel"],
                }
                node(value["startNode"])
                node(value["endNode"])
            elif "tableLabel" in value and isinstance(value.get("id"), int):
                node(value)


def _export(inst: Instance, nodes: list[dict], edges: list[dict]) -> list:
    resp = httpx.post(
        f"{inst.url}/data/neo4j-export",
        json={
            "url": _neo4j_url(),
            "username": "neo4j",
            "password": "neo4j",
            "database": "neo4j",
            "nodes": nodes,
            "edges": edges,
        },
        headers=_ADMIN,
        timeout=_TIMEOUT,
    )
    assert resp.status_code == 200, resp.text
    return resp.json()["errors"]


@pytest.mark.requires_neo4j
def test_the_demo_graph_exports_into_a_neo4j_server(instance):
    _wait_for_neo4j()
    _neo4j_tx(["MATCH (n) DETACH DELETE n"])

    schema = httpx.get(f"{instance.url}/data/graph-schema", headers=_ADMIN, timeout=_TIMEOUT)
    assert schema.status_code == 200, schema.text
    labels = [n["label"].split(":")[1] for n in schema.json()["node_labels"]]
    assert labels, schema.json()
    nodes: dict[Any, dict] = {}
    edges: dict[tuple, dict] = {}
    with ThreadPoolExecutor(_DISCOVERY_CONCURRENCY) as pool:
        for rows in pool.map(
            lambda label: _rows_or_none(
                instance, f"MATCH (n:{label}) RETURN n LIMIT {_DISCOVERY_ROWS}"
            ),
            labels,
        ):
            if rows is not None:
                _collect(rows, nodes, edges)
        rel_types = schema.json()["relationship_types"]
        for at in range(0, len(rel_types), _REL_WAVE):
            if edges:
                break
            for rows in pool.map(
                lambda rel: _rows_or_none(
                    instance,
                    f"MATCH (a:{rel['source']})-[r:{rel['type']}]->(b:{rel['target']}) "
                    f"RETURN a, r, b LIMIT {_DISCOVERY_ROWS}",
                ),
                rel_types[at : at + _REL_WAVE],
            ):
                if rows is not None:
                    _collect(rows, nodes, edges)
    exported_nodes, exported_edges = list(nodes.values()), list(edges.values())
    assert exported_nodes, "the demo's graph answered no nodes"
    assert exported_edges, "the demo's graph answered no edges"

    # Nodes in batches, then the edges alone (REQ-720, REQ-795): one transaction per batch.
    errors: list = []
    for i in range(0, len(exported_nodes), _EXPORT_BATCH):
        errors += _export(instance, exported_nodes[i : i + _EXPORT_BATCH], [])
    errors += _export(instance, [], exported_edges)
    assert errors == [], errors

    counts = _neo4j_tx(
        ["MATCH (n) RETURN count(n) AS nodeCount", "MATCH ()-[r]->() RETURN count(r) AS relCount"]
    )
    node_count = counts["results"][0]["data"][0]["row"][0]
    rel_count = counts["results"][1]["data"][0]["row"][0]
    # MERGE deduplicates on _provisa_id within a label, so Neo4j holds at most what was exported.
    assert 0 < node_count <= len(exported_nodes), (node_count, len(exported_nodes))
    assert rel_count == len(exported_edges), (rel_count, len(exported_edges))
