# Copyright (c) 2026 Kenneth Stott
# Canary: b3f8d2a6-7c1e-4e9b-a5d4-0f2c6e8b1d37
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A ``neo4j`` source in the config persists and serves (REQ-1668).

Config load writes the source's ``api_sources`` row and one ``api_endpoints`` row per table carrying
the Cypher; the startup loader reads them back; the openapi fetch chain (``make_openapi_loader`` →
``call_api`` → ``flatten_response``) runs the Cypher against a live Neo4j and returns the rows the
seed put there. The chain is the one the event loop lands a neo4j table with, so this is the
registration-to-rows path minus the SQL surface — the fragment demo/sources/neo4j/fragment.yaml
takes the same shape.
"""

from __future__ import annotations

import os
from pathlib import Path

import httpx
import pytest
import pytest_asyncio

from provisa.core.db import init_schema

pytestmark = [
    pytest.mark.integration,
    pytest.mark.requires_neo4j,
    pytest.mark.asyncio(loop_scope="session"),
]

_SCHEMA_SQL = (Path(__file__).parent.parent.parent / "provisa" / "core" / "schema.sql").read_text()
_SOURCE_ID = "neo4j_persist_it"
_TABLE = "persist_adopter"
_SEED = [(1, "Sara Kim", "Portland"), (2, "Tom Evans", "Portland"), (3, "Amy Zhao", "Seattle")]
_CYPHER = (
    "MATCH (a:PersistAdopter) RETURN a.adopter_id AS adopter_id, a.name AS name, a.city AS city "
    "ORDER BY a.adopter_id"
)


def _neo4j_base() -> str:
    return f"http://localhost:{os.environ['NEO4J_HTTP_PORT']}"


def _config() -> dict:
    return {
        "sources": [
            {
                "id": _SOURCE_ID,
                "type": "neo4j",
                "host": "localhost",
                "port": int(os.environ["NEO4J_HTTP_PORT"]),
                "database": "neo4j",
                "cache_ttl": 30,
            }
        ],
        "domains": [{"id": "graph_it", "description": "neo4j persistence"}],
        "naming": {"domain_prefix": False, "rules": []},
        "roles": [],
        "tables": [
            {
                "source_id": _SOURCE_ID,
                "domain_id": "graph_it",
                "schema": "neo4j",
                "table": _TABLE,
                "query_template": _CYPHER,
                "columns": [
                    {"name": "adopter_id", "data_type": "integer", "visible_to": ["org_admin"]},
                    {"name": "name", "data_type": "varchar", "visible_to": ["org_admin"]},
                    {"name": "city", "data_type": "varchar", "visible_to": ["org_admin"]},
                ],
            }
        ],
    }


@pytest.fixture(autouse=True)
def _seed_neo4j():
    """Land exactly the seed graph under a label no other module uses; remove it after."""
    tx = f"{_neo4j_base()}/db/neo4j/tx/commit"
    create = " ".join(
        f"CREATE (:PersistAdopter {{adopter_id: {i}, name: '{n}', city: '{c}'}})"
        for i, n, c in _SEED
    )
    with httpx.Client(timeout=60) as client:
        for stmt in ("MATCH (a:PersistAdopter) DETACH DELETE a", create):
            resp = client.post(tx, json={"statements": [{"statement": stmt}]})
            assert resp.status_code == 200, resp.text
            assert resp.json()["errors"] == [], resp.json()["errors"]
        yield
        client.post(
            tx, json={"statements": [{"statement": "MATCH (a:PersistAdopter) DETACH DELETE a"}]}
        )


@pytest_asyncio.fixture(scope="module")
async def pg_conn(tenant_db):
    await init_schema(tenant_db, _SCHEMA_SQL)
    async with tenant_db.acquire() as conn:
        await conn.execute("SET search_path TO org_default")
        yield conn
        await conn.execute(f"DELETE FROM api_endpoints WHERE source_id = '{_SOURCE_ID}'")
        await conn.execute(f"DELETE FROM api_sources WHERE id = '{_SOURCE_ID}'")
        await conn.execute(f"DELETE FROM registered_tables WHERE source_id = '{_SOURCE_ID}'")
        await conn.execute(f"DELETE FROM sources WHERE id = '{_SOURCE_ID}'")
        await conn.execute("DELETE FROM domains WHERE id = 'graph_it'")


async def _load(pg_conn) -> None:
    from provisa.core.config_loader import load_config, parse_config_dict

    await load_config(parse_config_dict(_config()), pg_conn, engine=None)


async def test_config_load_persists_source_and_cypher_endpoint(pg_conn):
    await _load(pg_conn)
    src = await pg_conn.fetch(f"SELECT type, base_url FROM api_sources WHERE id = '{_SOURCE_ID}'")
    assert [dict(r) for r in src] == [{"type": "neo4j", "base_url": _neo4j_base()}]
    eps = await pg_conn.fetch(
        "SELECT path, method, body_encoding, query_template, response_normalizer, ttl "
        f"FROM api_endpoints WHERE table_name = '{_TABLE}'"
    )
    assert [dict(r) for r in eps] == [
        {
            "path": "/db/neo4j/tx/commit",
            "method": "POST",
            "body_encoding": "neo4j_tx",
            "query_template": _CYPHER,
            "response_normalizer": "neo4j_tabular",
            "ttl": 30,
        }
    ]


async def test_config_load_is_idempotent(pg_conn):
    await _load(pg_conn)
    await _load(pg_conn)
    rows = await pg_conn.fetch(
        f"SELECT count(*) AS n FROM api_endpoints WHERE table_name = '{_TABLE}'"
    )
    assert rows[0]["n"] == 1


async def test_persisted_endpoint_hydrates_and_serves_rows(pg_conn):
    """Startup loader → the neo4j landing loader → rows: what a restart serves without the file."""
    from provisa.api_source.loader import load_api_sources
    from provisa.events.source_loader import make_openapi_loader

    await _load(pg_conn)
    endpoints, sources = await load_api_sources(pg_conn, [], {}, [], {})
    assert endpoints[_TABLE].query_template == _CYPHER

    class _Ref:
        def __init__(self, **kw):
            self.__dict__.update(kw)

    fetch = make_openapi_loader(endpoints, sources)
    rows = await fetch(_Ref(id=_SOURCE_ID), _Ref(table_name=_TABLE))
    assert [(r["adopter_id"], r["name"], r["city"]) for r in rows] == _SEED
