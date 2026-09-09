# Copyright (c) 2026 Kenneth Stott
# Canary: 5fc65a82-66ca-4364-a0b9-29bf90923351
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A ``sparql`` source in the config persists and serves (REQ-1683): config load writes the
api_sources/api_endpoints rows, the startup loader reads them back, and the openapi fetch chain runs
the SELECT against a live Fuseki and returns the seeded rows."""

from __future__ import annotations

import os
from pathlib import Path

import httpx
import pytest
import pytest_asyncio

from provisa.core.db import init_schema

pytestmark = [
    pytest.mark.integration,
    pytest.mark.requires_sparql,
    pytest.mark.asyncio(loop_scope="session"),
]

_SCHEMA_SQL = (Path(__file__).parent.parent.parent / "provisa" / "core" / "schema.sql").read_text()
_SOURCE_ID = "sparql_persist_it"
_TABLE = "persist_volunteer"
_PREFIX = "PREFIX s: <http://provisa.dev/persist#>"
_SEED = [("V-1", "Grace"), ("V-2", "Omar"), ("V-3", "Priya")]
_QUERY = (
    f"{_PREFIX} SELECT ?volunteer_id ?name WHERE {{ ?v a s:V ; s:id ?volunteer_id ; s:name ?name }} "
    "ORDER BY ?volunteer_id"
)


def _base() -> str:
    return f"http://localhost:{os.environ['FUSEKI_PORT']}/provisa"


def _config() -> dict:
    return {
        "sources": [
            {"id": _SOURCE_ID, "type": "sparql", "host": f"{_base()}/query", "cache_ttl": 30}
        ],
        "domains": [{"id": "graph_it", "description": "sparql persistence"}],
        "naming": {"domain_prefix": False, "rules": []},
        "roles": [],
        "tables": [
            {
                "source_id": _SOURCE_ID,
                "domain_id": "graph_it",
                "schema": "sparql",
                "table": _TABLE,
                "query_template": _QUERY,
                "columns": [
                    {"name": "volunteer_id", "data_type": "text", "visible_to": ["org_admin"]},
                    {"name": "name", "data_type": "text", "visible_to": ["org_admin"]},
                ],
            }
        ],
    }


@pytest.fixture(autouse=True)
def _seed():
    triples = " ".join(
        f'<http://provisa.dev/p/{vid}> a s:V ; s:id "{vid}" ; s:name "{name}" .'
        for vid, name in _SEED
    )
    with httpx.Client(timeout=60) as c:
        for update in (
            f"{_PREFIX} DELETE {{ ?s ?p ?o }} WHERE {{ ?s a s:V ; ?p ?o }}",
            f"{_PREFIX} INSERT DATA {{ {triples} }}",
        ):
            resp = c.post(f"{_base()}/update", data={"update": update})
            assert resp.status_code in (200, 204), resp.text
        yield
        c.post(
            f"{_base()}/update",
            data={"update": f"{_PREFIX} DELETE {{ ?s ?p ?o }} WHERE {{ ?s a s:V ; ?p ?o }}"},
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


async def test_config_load_persists_source_and_query_endpoint(pg_conn):
    await _load(pg_conn)
    src = await pg_conn.fetch(f"SELECT type, base_url FROM api_sources WHERE id = '{_SOURCE_ID}'")
    assert [dict(r) for r in src] == [
        {"type": "sparql", "base_url": f"http://localhost:{os.environ['FUSEKI_PORT']}"}
    ]
    eps = await pg_conn.fetch(
        "SELECT path, method, body_encoding, query_template, response_normalizer "
        f"FROM api_endpoints WHERE table_name = '{_TABLE}'"
    )
    assert [dict(r) for r in eps] == [
        {
            "path": "/provisa/query",
            "method": "POST",
            "body_encoding": "form",
            "query_template": _QUERY,
            "response_normalizer": "sparql_bindings",
        }
    ]


async def test_persisted_endpoint_hydrates_and_serves_rows(pg_conn):
    from provisa.api_source.loader import load_api_sources
    from provisa.events.source_loader import make_openapi_loader

    await _load(pg_conn)
    endpoints, sources = await load_api_sources(pg_conn, [], {}, [], {})

    class _Ref:
        def __init__(self, **kw):
            self.__dict__.update(kw)

    rows = await make_openapi_loader(endpoints, sources)(
        _Ref(id=_SOURCE_ID), _Ref(table_name=_TABLE)
    )
    assert [(r["volunteer_id"], r["name"]) for r in rows] == _SEED
