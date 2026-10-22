# Copyright (c) 2026 Kenneth Stott
# Canary: b3b89716-e5af-4510-b703-feaac3115e9a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A sparql source's reasoner, end to end (REQ-2138): a config with HermiT and OWL axioms is
loaded, its inferred tables are read through the openapi fetch chain, and the rows are what
HermiT infers from the slice a live Fuseki answers. The reasoner runs as its own program on the
pinned JDK and jars (provisa/sparql/reasoner_bundle.py), fetched on first use.

Also: a statement's conditions pushed into a SPARQL table's query are a query Fuseki answers.
"""

# Requirements: REQ-2138

from __future__ import annotations

import os
from pathlib import Path
from types import SimpleNamespace

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
_SOURCE_ID = "sparql_reasoner_it"
_KYC = "http://provisa.dev/kyc-it#"
_AXIOMS = f"""\
@prefix : <{_KYC}> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
<http://provisa.dev/kyc-it> a owl:Ontology .
:controls a owl:ObjectProperty, owl:TransitiveProperty .
:SanctionedParty a owl:Class .
:SanctionsExposed a owl:Class ;
  owl:equivalentClass [ a owl:Restriction ;
    owl:onProperty [ owl:inverseOf :controls ] ; owl:someValuesFrom :SanctionedParty ] .
"""
# A controls B controls C; A is sanctioned. X controls Y, neither sanctioned.
_FACTS = (
    ":A :controls :B . :B :controls :C . :X :controls :Y . :A a :SanctionedParty . "
    ':A :name "Alpha" .'
)
_PREFIX = f"PREFIX : <{_KYC}>"


def _base() -> str:
    return f"http://localhost:{os.environ['FUSEKI_PORT']}/provisa"


def _table(name: str, query: str, columns: list[str]) -> dict:
    return {
        "source_id": _SOURCE_ID,
        "domain_id": "kyc_it",
        "schema": "kyc",
        "table": name,
        "query_template": query,
        "columns": [{"name": c, "data_type": "text", "visible_to": ["org_admin"]} for c in columns],
    }


def _config() -> dict:
    return {
        "sources": [
            {
                "id": _SOURCE_ID,
                "type": "sparql",
                "host": f"{_base()}/query",
                "cache_ttl": 30,
                "mapping": {"reasoner": "hermit", "axioms": _AXIOMS},
            }
        ],
        "domains": [{"id": "kyc_it", "description": "sparql reasoner"}],
        "naming": {"domain_prefix": False, "rules": []},
        "roles": [],
        "tables": [
            _table("exposed", f"<{_KYC}SanctionsExposed>", ["party"]),
            _table("controls", f"<{_KYC}controls>", ["controller", "controlled"]),
            _table(
                "named",
                f"{_PREFIX} SELECT ?party ?name WHERE {{ ?party :name ?name }}",
                ["party", "name"],
            ),
        ],
    }


@pytest.fixture(autouse=True)
def _seed():
    clear = (
        f"{_PREFIX} DELETE {{ ?s ?p ?o }} WHERE {{ ?s ?p ?o FILTER(STRSTARTS(STR(?s), STR(:))) }}"
    )
    with httpx.Client(timeout=60) as c:
        for update in (clear, f"{_PREFIX} INSERT DATA {{ {_FACTS} }}"):
            resp = c.post(f"{_base()}/update", data={"update": update})
            assert resp.status_code in (200, 204), resp.text
        yield
        c.post(f"{_base()}/update", data={"update": clear})


@pytest_asyncio.fixture(scope="module")
async def pg_conn(tenant_db, platform_admin_db):
    await init_schema(tenant_db, _SCHEMA_SQL)
    async with tenant_db.acquire() as conn:
        await conn.execute("SET search_path TO org_default")
        yield conn
        await conn.execute(f"DELETE FROM api_endpoints WHERE source_id = '{_SOURCE_ID}'")
        await conn.execute(f"DELETE FROM api_sources WHERE id = '{_SOURCE_ID}'")
        await conn.execute(f"DELETE FROM registered_tables WHERE source_id = '{_SOURCE_ID}'")
        await conn.execute(f"DELETE FROM sources WHERE id = '{_SOURCE_ID}'")
        await conn.execute("DELETE FROM domains WHERE id = 'kyc_it'")


async def _endpoints(pg_conn):
    from provisa.api_source.loader import load_api_sources
    from provisa.core.config_loader import apply_config, parse_config_dict

    await apply_config(parse_config_dict(_config()), pg_conn, engine=None)
    return await load_api_sources(pg_conn, {})


async def _rows(endpoints, sources, table: str) -> list[dict]:
    from provisa.events.source_loader import make_openapi_loader

    load = make_openapi_loader(SimpleNamespace(api_endpoints=endpoints, api_sources=sources))
    return await load(SimpleNamespace(id=_SOURCE_ID), SimpleNamespace(table_name=table))


async def test_the_reasoners_inferred_tables_are_what_hermit_infers(pg_conn):
    endpoints, sources = await _endpoints(pg_conn)
    exposed = await _rows(endpoints, sources, "exposed")
    assert sorted(r["party"] for r in exposed) == [_KYC + "B", _KYC + "C"]
    controls = await _rows(endpoints, sources, "controls")
    assert sorted((r["controller"], r["controlled"]) for r in controls) == [
        (_KYC + "A", _KYC + "B"),
        (_KYC + "A", _KYC + "C"),  # transitive: inferred, never asserted
        (_KYC + "B", _KYC + "C"),
        (_KYC + "X", _KYC + "Y"),
    ]


async def test_a_pushed_down_query_is_answered_by_the_endpoint(pg_conn):
    from provisa.api_source.caller import answer_rows, call_api
    from provisa.sparql.pushdown import pushed_arguments

    endpoints, sources = await _endpoints(pg_conn)
    endpoint = endpoints[(_SOURCE_ID, "named")]
    args = pushed_arguments(
        endpoint, "SELECT party FROM named WHERE name = 'Alpha' LIMIT 1", "named"
    )
    assert "FILTER" in args["query"] and "LIMIT 1" in args["query"]
    source = sources[_SOURCE_ID]
    answer = await call_api(endpoint, args, base_url=source.base_url, auth=source.auth)
    assert answer_rows(endpoint, answer)[0] == [{"party": _KYC + "A", "name": "Alpha"}]
