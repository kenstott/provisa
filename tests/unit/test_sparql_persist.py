# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""SPARQL sources persist and preview (REQ-1683): config validation, the endpoint shape, the
preview resolver, and the shared registration hook."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import httpx
import pytest
import respx

from provisa.api.admin import _query_api_registration as qa
from provisa.api_source.models import ApiSourceType
from provisa.core.config_loader import _validate_neo4j_sources, parse_config_dict
from provisa.core.models import Column, Table
from provisa.sparql.persist import sparql_config_from_source

_EP = "http://fuseki.test:3030/provisa/query"
_VIS = ["org_admin"]


def _config(
    host: str = _EP, query: str | None = "SELECT ?name WHERE { ?a <urn:name> ?name }"
) -> dict:
    return {
        "sources": [{"id": "graph", "type": "sparql", "host": host}],
        "domains": [{"id": "d"}],
        "roles": [],
        "naming": {"domain_prefix": False, "rules": []},
        "tables": [
            {
                "source_id": "graph",
                "domain_id": "d",
                "schema": "sparql",
                "table": "adopter",
                "query_template": query,
                "columns": [{"name": "name", "data_type": "text", "visible_to": _VIS}],
            }
        ],
    }


class TestValidation:
    def test_valid_source_and_table_pass(self):
        _validate_neo4j_sources(parse_config_dict(_config()))

    def test_host_must_be_the_endpoint_url(self):
        with pytest.raises(ValueError, match="endpoint URL"):
            _validate_neo4j_sources(parse_config_dict(_config(host="fuseki.test")))

    def test_table_requires_the_query(self):
        with pytest.raises(ValueError, match="requires query_template"):
            _validate_neo4j_sources(parse_config_dict(_config(query=None)))


def test_endpoint_posts_the_query_form_encoded_to_the_endpoint_path():
    from provisa.sparql.source import build_endpoint

    cfg, api_source = sparql_config_from_source(source_id="graph", endpoint_url=_EP)
    assert api_source.type is ApiSourceType.sparql
    assert api_source.base_url == "http://fuseki.test:3030"
    ep = build_endpoint(cfg, "adopter", "SELECT ?name WHERE { ?a <urn:name> ?name }", [])
    assert (ep.path, ep.method, ep.body_encoding, ep.response_normalizer) == (
        "/provisa/query",
        "POST",
        "form",
        "sparql_bindings",
    )


class _Conn:
    def __init__(self, row: dict | None):
        self._row = row
        self.upserts: list[tuple[str, dict]] = []

    async def execute_core(self, stmt: Any):
        row = self._row

        class _Res:
            def fetchone(self_inner):
                return None if row is None else SimpleNamespace(_mapping=row)

        return _Res()

    async def upsert(self, table: Any, values: dict, **kw: Any) -> None:
        self.upserts.append((table.name, values))


_SRC = {"id": "graph", "type": "sparql", "host": _EP, "port": 0, "database": ""}


def _bindings(names: list[str], rows: list[list[str]]) -> dict:
    return {
        "head": {"vars": names},
        "results": {
            "bindings": [
                {n: {"type": "literal", "value": v} for n, v in zip(names, r)} for r in rows
            ]
        },
    }


@pytest.mark.asyncio
async def test_preview_returns_rows_and_text_columns():
    with respx.mock:
        respx.post(_EP).mock(
            return_value=httpx.Response(
                200, json=_bindings(["name", "city"], [["Sara", "Portland"]])
            )
        )
        out = await qa.preview_sparql(_Conn(_SRC), "graph", "SELECT ?name ?city WHERE { ?a ?p ?o }")  # type: ignore[arg-type]
    assert out.error is None
    assert out.rows == [{"name": "Sara", "city": "Portland"}]
    assert [(c.name, c.data_type) for c in out.columns] == [("name", "text"), ("city", "text")]


@pytest.mark.asyncio
async def test_preview_with_no_rows_falls_back_to_the_select_variables():
    with respx.mock:
        respx.post(_EP).mock(return_value=httpx.Response(200, json=_bindings(["name"], [])))
        out = await qa.preview_sparql(_Conn(_SRC), "graph", "SELECT ?name WHERE { ?a ?p ?o }")  # type: ignore[arg-type]
    assert out.error is None and [c.name for c in out.columns] == ["name"]
    with respx.mock:
        respx.post(_EP).mock(return_value=httpx.Response(200, json=_bindings([], [])))
        out = await qa.preview_sparql(_Conn(_SRC), "graph", "SELECT * WHERE { ?a ?p ?o }")  # type: ignore[arg-type]
    assert out.error is not None and "cannot be inferred" in out.error


@pytest.mark.asyncio
async def test_preview_reports_an_unreachable_endpoint_and_a_wrong_source_type():
    with respx.mock:
        respx.post(_EP).mock(side_effect=httpx.ConnectError("refused"))
        out = await qa.preview_sparql(_Conn(_SRC), "graph", "SELECT ?x WHERE {}")  # type: ignore[arg-type]
    assert out.error is not None and "ConnectError" in out.error
    out = await qa.preview_sparql(_Conn({**_SRC, "type": "neo4j"}), "graph", "SELECT ?x WHERE {}")  # type: ignore[arg-type]
    assert out.error == "'graph' is not a registered sparql source"


@pytest.mark.asyncio
async def test_registration_persists_the_sparql_endpoint_and_mirrors_live_state(monkeypatch):
    from provisa.api.app import state

    monkeypatch.setattr(state, "api_sources", {}, raising=False)
    monkeypatch.setattr(state, "api_endpoints", {}, raising=False)
    model = Table(
        source_id="graph",
        domain_id="d",
        schema="sparql",
        table="adopter",
        query_template="SELECT ?name WHERE { ?a <urn:name> ?name }",
        columns=[Column(name="name", data_type="text", visible_to=_VIS)],
    )
    conn = _Conn(_SRC)
    assert await qa.persist_query_api_registration(conn, model) is None  # type: ignore[arg-type]
    assert [t for t, _ in conn.upserts] == ["api_sources", "api_endpoints"]
    ep = conn.upserts[1][1]
    assert (ep["path"], ep["body_encoding"], ep["response_normalizer"]) == (
        "/provisa/query",
        "form",
        "sparql_bindings",
    )
    assert state.api_sources["graph"].type is ApiSourceType.sparql
    assert (
        state.api_endpoints["adopter"].query_template
        == "SELECT ?name WHERE { ?a <urn:name> ?name }"
    )
