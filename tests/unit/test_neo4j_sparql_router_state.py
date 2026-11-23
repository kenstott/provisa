# Copyright (c) 2026 Kenneth Stott
# Canary: 3f8b2d61-c4a7-4e95-b0d2-7a1e9c5f4b83
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The Neo4j and SPARQL admin routes run against Provisa's app state, not Starlette's (issue #345).

``request.app.state`` is Starlette's ``State``: it carries none of Provisa's attributes, so a
route that read the control plane from it always answered 503 ``database_not_connected``. Each
request here carries such a bare ``State``; the routes must reach the control plane bound on
Provisa's own app-state singleton.
"""

# Requirements: REQ-295, REQ-296, REQ-297, REQ-298, REQ-299

from __future__ import annotations

from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest
from starlette.applications import Starlette
from starlette.requests import Request

from provisa.api.admin import _neo4j_registration
from provisa.api.admin import neo4j_router, sparql_router
from provisa.api.app import state


class _Db:
    """A control plane whose one connection is recorded."""

    def __init__(self) -> None:
        self.conn = object()

    @asynccontextmanager
    async def acquire(self):
        yield self.conn


def _request() -> Request:
    """A request whose ``app.state`` is Starlette's bare State, as on a running server."""
    return Request({"type": "http", "method": "POST", "headers": [], "app": Starlette()})


@pytest.fixture
def db(monkeypatch) -> _Db:
    control_plane = _Db()
    monkeypatch.setattr(state, "model_db", control_plane)
    monkeypatch.setattr(state, "api_sources", {})
    for mod in (neo4j_router, sparql_router):
        monkeypatch.setattr(mod, "require_capability_request", lambda _r, _c: None)
    return control_plane


@pytest.mark.asyncio
async def test_neo4j_register_persists_into_the_provisa_control_plane(db, monkeypatch):
    persisted: list[tuple[object, str]] = []

    async def _persist(conn, api_source):
        persisted.append((conn, api_source.id))

    monkeypatch.setattr(neo4j_router, "persist_api_source", _persist)
    body = neo4j_router.Neo4jSourceRequest(source_id="graph", host="h")
    result = await neo4j_router.register_neo4j_source(body, _request())
    assert result == {"source_id": "graph", "base_url": "http://h:7474"}
    assert persisted == [(db.conn, "graph")]
    assert state.api_sources["graph"].base_url == "http://h:7474"


@pytest.mark.asyncio
async def test_neo4j_preview_runs_on_the_provisa_control_plane(db, monkeypatch):
    seen: list[tuple[object, str, str]] = []

    async def _preview(conn, source_id, cypher):
        seen.append((conn, source_id, cypher))
        return SimpleNamespace(
            error=None,
            rows=[{"name": "a"}],
            columns=[SimpleNamespace(name="name", data_type="text")],
        )

    monkeypatch.setattr(_neo4j_registration, "preview_neo4j", _preview)
    body = neo4j_router.Neo4jPreviewRequest(cypher="MATCH (p) RETURN p.name AS name")
    result = await neo4j_router.preview_neo4j_query("graph", body, _request())
    assert result == {"rows": [{"name": "a"}], "columns": [{"name": "name", "data_type": "text"}]}
    assert seen == [(db.conn, "graph", "MATCH (p) RETURN p.name AS name")]


@pytest.mark.asyncio
async def test_sparql_register_persists_into_the_provisa_control_plane(db, monkeypatch):
    persisted: list[tuple[object, str]] = []

    async def _persist(conn, api_source):
        persisted.append((conn, api_source.id))

    monkeypatch.setattr(sparql_router, "persist_api_source", _persist)
    body = sparql_router.SparqlSourceRequest(
        source_id="wiki", endpoint_url="https://query.example.org/sparql"
    )
    result = await sparql_router.register_sparql_source(body, _request())
    assert result["source_id"] == "wiki"
    assert persisted == [(db.conn, "wiki")]
    assert state.api_sources["wiki"].id == "wiki"
