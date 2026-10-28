# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration: tests/unit/test_ephemeral_traversal_order.py's cases on the engines a session
graph is read by (REQ-2176, "SESSION GRAPHS ARE INDEXED AND COUNTED"): the session store on
PostgreSQL, read by the PostgreSQL engine and by Trino over the PostgreSQL scratch. Each case runs
over POST /data/cypher on a real server; the reordered read returns the rows the path written
either way holds, a path ending on a model node is read as written, and a read with no session,
an OPTIONAL MATCH and a named path are as written."""

# Requirements: REQ-2176

from __future__ import annotations

import pytest
import pytest_asyncio

pytestmark = [pytest.mark.integration]

_ROLE = "org_admin"


def _pg_config(source) -> dict:
    """A model of one orders table on ``source``, read by the pg engine."""
    from tests.embedded_pg import DATABASE, PASSWORD, ROLE

    return {
        "federation_engine": "pg",
        "auth": {
            "provider": "none",
            "assignments_source": "provisa",
            "default_assignments": [{"domain_id": "*", "role_id": _ROLE}],
        },
        "naming": {"domain_prefix": False, "rules": []},
        "cache": {"enabled": False},
        "domains": [{"id": "shop", "description": "Shop"}],
        "roles": [],
        "sources": [
            {
                "id": "shop_pg",
                "type": "postgresql",
                "host": "127.0.0.1",
                "port": source.port,
                "database": DATABASE,
                "username": ROLE,
                "password": PASSWORD,
            }
        ],
        "tables": [
            {
                "source_id": "shop_pg",
                "domain_id": "shop",
                "schema": "public",
                "table": "orders",
                "columns": [
                    {"name": "id", "data_type": "integer", "visible_to": [_ROLE]},
                    {"name": "region", "data_type": "varchar", "visible_to": [_ROLE]},
                ],
            }
        ],
    }


def _pg_server(org: str, tmp_path_factory):
    """The pg engine on a PostgreSQL of the test's own, which is the session store too, beside
    a source PostgreSQL holding orders 1 and 2."""
    import psycopg
    import yaml

    from tests.embedded_pg import start_local_postgres
    from tests.integration.isolated_server import IsolatedServer

    source = start_local_postgres()
    store = start_local_postgres(fdws=True)
    with psycopg.connect(source.dsn(), autocommit=True) as conn:
        conn.execute("CREATE TABLE orders (id integer PRIMARY KEY, region text)")
        conn.execute("INSERT INTO orders VALUES (1, 'east'), (2, 'west')")
    path = tmp_path_factory.mktemp("traversal_order_pg") / "config.yaml"
    path.write_text(yaml.safe_dump(_pg_config(source)))
    srv = IsolatedServer(
        org,
        engine="pg",
        config=str(path),
        control_plane="own",
        # The engine runs on the store, beside the source: left to the control plane, it would
        # run in the stack's container, where 127.0.0.1 is not them.
        env={"PROVISA_MATERIALIZE_URL": store.dsn(), "PROVISA_ENGINE_URL": store.dsn()},
    )
    return srv, (source, store)


def _trino_server(org: str):
    """Trino, the session store the stack's PostgreSQL (tests/conftest.py exports
    PROVISA_MATERIALIZE_URL), which Trino reads the session's scratch from."""
    from tests.integration.isolated_server import IsolatedServer

    srv = IsolatedServer(
        org,
        engine="trino",
        config="tests/fixtures/sample_config.yaml",
        control_plane="postgres",
        env={"PROVISA_REDIRECT_ENABLED": "false"},
    )
    return srv, ()


@pytest_asyncio.fixture(
    scope="module",
    params=[
        pytest.param("pg", id="pg", marks=pytest.mark.local_platform),
        pytest.param("trino", id="trino", marks=pytest.mark.requires_trino),
    ],
)
async def server(request, tmp_path_factory):
    from tests.integration.isolated_server import drop_org_schema

    org = f"traversal_order_{request.param}"
    if request.param == "pg":
        srv, owned = _pg_server(org, tmp_path_factory)
    else:
        srv, owned = _trino_server(org)
    try:
        srv.start()
        yield srv
    finally:
        srv.stop_process()
        if request.param == "trino":
            await drop_org_schema(org)
        for pg in owned:
            pg.stop()


def _call(srv, method: str, path: str, body: dict | None = None):
    import httpx

    resp = httpx.request(
        method,
        f"{srv.base_url}{path}",
        json=body,
        headers={"x-provisa-role": _ROLE},
        timeout=srv.request_timeout + 30,
    )
    assert resp.status_code == 200, resp.text
    return resp.json()


def _cypher(srv, statement: str) -> dict:
    return _call(srv, "POST", "/data/cypher", {"query": statement})


def _values(answer: dict) -> list[tuple]:
    columns = answer["columns"]
    rows = [
        tuple(r[c] for c in columns) if isinstance(r, dict) else tuple(r) for r in answer["rows"]
    ]
    return sorted(rows, key=repr)


def _orders_label(srv) -> str:
    schema = _call(srv, "GET", "/data/graph-schema")
    labels = [n["label"] for n in schema["node_labels"] if n["table_label"] == "Orders"]
    assert len(labels) == 1, schema["node_labels"]
    return labels[0]


@pytest.fixture
def graph(server):
    """The unit cases' session graph: 40 Big, 2 Small, Big k -> Small k; and Big 1 -> order 1."""
    assert _call(server, "DELETE", "/data/cypher/session") == {"cleared": True}
    _cypher(server, "UNWIND range(1, 40) AS i CREATE (:Big {k: i})")
    _cypher(server, "CREATE (:Small {k: 1}), (:Small {k: 2})")
    _cypher(server, "MATCH (b:Big), (s:Small) WHERE b.k = s.k CREATE (b)-[:R]->(s)")
    orders = _orders_label(server)
    _cypher(server, f"MATCH (b:Big), (o:{orders}) WHERE b.k = 1 AND o.id = 1 CREATE (b)-[:R]->(o)")
    yield server
    _call(server, "DELETE", "/data/cypher/session")


def test_a_path_read_from_its_more_selective_end_holds_its_rows(graph):
    answer = _cypher(graph, "MATCH (a:Big)-[:R]->(b:Small) RETURN a.k AS a, b.k AS b")
    assert _values(answer) == [(1, 1), (2, 2)]


def test_a_path_already_starting_there_holds_the_same_rows(graph):
    answer = _cypher(graph, "MATCH (b:Small)<-[:R]-(a:Big) RETURN a.k AS a, b.k AS b")
    assert _values(answer) == [(1, 1), (2, 2)]


def test_a_path_ending_on_a_model_node_holds_its_rows(graph):
    orders = _orders_label(graph)
    answer = _cypher(graph, f"MATCH (a:Big)-[:R]->(o:{orders}) RETURN a.k AS a, o.id AS id")
    assert _values(answer) == [(1, 1)]


def test_with_no_session_a_read_holds_the_models_rows(server):
    assert _call(server, "DELETE", "/data/cypher/session") == {"cleared": True}
    orders = _orders_label(server)
    answer = _cypher(server, f"MATCH (o:{orders}) WHERE o.id = 1 RETURN o.id AS id")
    assert _values(answer) == [(1,)]


def test_an_optional_or_named_path_holds_its_rows(graph):
    optional = _cypher(
        graph, "MATCH (x:Small) OPTIONAL MATCH (a:Big)-[:R]->(b:Small) RETURN a.k AS a"
    )
    assert _values(optional) == [(1,), (1,), (2,), (2,)]
    named = _cypher(graph, "MATCH p = (a:Big)-[:R]->(b:Small) RETURN a.k AS a, length(p) AS n")
    assert _values(named) == [(1, 1), (2, 1)]
