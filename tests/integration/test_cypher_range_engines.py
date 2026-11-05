# Copyright (c) 2026 Kenneth Stott
# Canary: 0b5f3c92-8e47-4a1d-b6c9-f2d7a1e04c58
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration: Cypher's range(start, end[, step]) on the engines a session graph is read by
(REQ-345, REQ-2176): the pg engine, which has no sequence() and computes the list with
generate_series, and Trino, whose sequence() it is. A session write UNWIND range(1, 40) AS i
CREATE (:Big {k: i}) was refused on the pg engine ("function sequence(integer, integer) does not
exist"); on each engine it writes 40 nodes, k 1 through 40, and range() returns its ends."""

# Requirements: REQ-345, REQ-2176

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
    path = tmp_path_factory.mktemp("cypher_range_pg") / "config.yaml"
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

    org = f"cypher_range_{request.param}"
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


def test_unwind_range_writes_one_node_per_value_inclusive(server):
    assert _call(server, "DELETE", "/data/cypher/session") == {"cleared": True}
    try:
        # The write reads its rows with the UNWIND (ephemeral.seam): the values it creates.
        rows = _cypher(server, "UNWIND range(1, 40) AS i RETURN i")
        assert [r[0] for r in _rows(rows)] == list(range(1, 41))
        made = _cypher(server, "UNWIND range(1, 40) AS i CREATE (:Big {k: i})")
        assert made["counters"]["nodes-created"] == 40, made
    finally:
        _call(server, "DELETE", "/data/cypher/session")


def test_a_range_returned_and_stepped_holds_both_ends(server):
    answer = _cypher(
        server, "RETURN range(1, 3) AS r, range(1, 10, 3) AS s, size(range(1, 40)) AS n"
    )
    assert _rows(answer) == [([1, 2, 3], [1, 4, 7, 10], 40)]


def _rows(answer: dict) -> list[tuple]:
    columns = answer["columns"]
    return [
        tuple(r[c] for c in columns) if isinstance(r, dict) else tuple(r) for r in answer["rows"]
    ]
