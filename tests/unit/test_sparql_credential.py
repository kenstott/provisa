# Copyright (c) 2026 Kenneth Stott
# Canary: 5f3a8e21-7c4d-4b9a-a1e6-2d8c0f9b3e57
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2138: a sparql source reads its endpoint as the credential it declares (bearer or basic,
held as secret references), on every call -- its tables, its slice and the admin preview -- and
one pointing at Provisa's own endpoint with no credential is refused by name."""

from __future__ import annotations

import base64
from types import SimpleNamespace
from typing import Any

import httpx
import pytest
import respx

from provisa.api.admin import _query_api_registration as qa
from provisa.core.auth_models import ApiAuthBasic, ApiAuthBearer
from provisa.core.models import Column, Table
from provisa.sparql import source as sparql_source
from provisa.sparql.persist import persist_sparql_table
from provisa.sparql.source import (
    OwnEndpointNeedsCredential,
    SparqlCredentialInvalid,
    is_own_endpoint,
    sparql_auth,
)

_EP = "http://fuseki.test:3030/provisa/query"
_PUBLIC = "https://provisa.example.test"
_OWN = f"{_PUBLIC}/data/sparql"
_TOKEN_REF = "${env:SPARQL_CRED_TOKEN}"
_USER_REF = "${env:SPARQL_CRED_USER}"
_PASS_REF = "${env:SPARQL_CRED_PASS}"
_VIS = ["org_admin"]


@pytest.fixture(autouse=True)
def _credentials(monkeypatch):
    monkeypatch.setenv("SPARQL_CRED_TOKEN", "tok-123")
    monkeypatch.setenv("SPARQL_CRED_USER", "svc-reasoner")
    monkeypatch.setenv("SPARQL_CRED_PASS", "pw-456")


@pytest.fixture
def public_url(monkeypatch):
    """Provisa's own public address, so its own endpoint is ``_OWN``."""
    from provisa.core import settings_registry

    real = settings_registry.value

    def _value(key: str) -> Any:
        return _PUBLIC if key == "server.public_url" else real(key)

    monkeypatch.setattr(settings_registry, "value", _value)


def _row(mapping: dict, username: str = "", password: str = "", host: str = _EP) -> dict:
    return {
        "id": "graph",
        "type": "sparql",
        "host": host,
        "port": 0,
        "database": "",
        "mapping": mapping,
        "username": username,
        "password_ref": password,
    }


class _Conn:
    def __init__(self, row: dict):
        self._row = row
        self.upserts: list[tuple[str, dict]] = []

    async def execute_core(self, stmt: Any):
        row = self._row

        class _Res:
            def fetchone(self_inner):
                return SimpleNamespace(_mapping=row)

        return _Res()

    async def upsert(self, table: Any, values: dict, **kw: Any) -> None:
        self.upserts.append((table.name, values))


def _answer() -> dict:
    return {
        "head": {"vars": ["name"]},
        "results": {"bindings": [{"name": {"type": "literal", "value": "Sara"}}]},
    }


def _basic(user: str, password: str) -> str:
    return "Basic " + base64.b64encode(f"{user}:{password}".encode()).decode()


class TestDeclaredCredential:
    def test_no_kind_declares_none(self):
        assert sparql_auth("graph", {}, "", "") is None

    def test_bearer_is_the_password_field_kept_as_its_reference(self):
        auth = sparql_auth("graph", {"auth": "bearer"}, "", _TOKEN_REF)
        assert auth == ApiAuthBearer(token=_TOKEN_REF)

    def test_basic_is_username_and_password_kept_as_references(self):
        auth = sparql_auth("graph", {"auth": "basic"}, _USER_REF, _PASS_REF)
        assert auth == ApiAuthBasic(username=_USER_REF, password=_PASS_REF)

    @pytest.mark.parametrize(
        ("options", "username", "password", "said"),
        [
            ({"auth": "oauth"}, "", "x", "is not one of"),
            ({"auth": "bearer"}, "", "", "needs its token"),
            ({"auth": "basic"}, "u", "", "needs its password"),
            ({"auth": "basic"}, "", "p", "needs its username"),
        ],
    )
    def test_an_incomplete_or_unknown_credential_is_refused_by_name(
        self, options, username, password, said
    ):
        with pytest.raises(SparqlCredentialInvalid, match=said) as refused:
            sparql_auth("graph", options, username, password)
        assert refused.value.code == "sparql.credential_invalid"


@pytest.mark.asyncio
async def test_persist_keeps_the_credential_as_its_references():
    """The api source carries the references as written; nothing resolved is stored."""
    conn = _Conn(_row({}))
    api_source, _endpoint = await persist_sparql_table(
        conn,  # type: ignore[arg-type]
        source_id="graph",
        endpoint_url=_EP,
        table_name="adopter",
        query_template="SELECT ?name WHERE { ?a <urn:name> ?name }",
        columns=[],
        ttl=300,
        options={"auth": "basic"},
        username=_USER_REF,
        password=_PASS_REF,
    )
    assert api_source.auth == ApiAuthBasic(username=_USER_REF, password=_PASS_REF)
    stored = dict(conn.upserts)["api_sources"]
    assert stored["auth"] is None  # the credential lives in its Source row, never copied here
    assert "pw-456" not in repr(conn.upserts)


@pytest.mark.asyncio
async def test_preview_sends_the_bearer_token():
    with respx.mock:
        route = respx.post(_EP).mock(return_value=httpx.Response(200, json=_answer()))
        out = await qa.preview_sparql(
            _Conn(_row({"auth": "bearer"}, password=_TOKEN_REF)),  # type: ignore[arg-type]
            "graph",
            "SELECT ?name WHERE { ?a ?p ?name }",
        )
    assert out.error is None
    assert route.calls.last.request.headers["authorization"] == "Bearer tok-123"


@pytest.mark.asyncio
async def test_preview_sends_basic_credentials():
    with respx.mock:
        route = respx.post(_EP).mock(return_value=httpx.Response(200, json=_answer()))
        out = await qa.preview_sparql(
            _Conn(_row({"auth": "basic"}, _USER_REF, _PASS_REF)),  # type: ignore[arg-type]
            "graph",
            "SELECT ?name WHERE { ?a ?p ?name }",
        )
    assert out.error is None
    assert route.calls.last.request.headers["authorization"] == _basic("svc-reasoner", "pw-456")


@pytest.mark.asyncio
async def test_preview_without_a_credential_sends_no_authorization():
    with respx.mock:
        route = respx.post(_EP).mock(return_value=httpx.Response(200, json=_answer()))
        await qa.preview_sparql(_Conn(_row({})), "graph", "SELECT ?name WHERE { ?a ?p ?name }")  # type: ignore[arg-type]
    assert "authorization" not in route.calls.last.request.headers


@pytest.mark.asyncio
async def test_inferred_preview_reads_the_slice_under_the_credential(monkeypatch):
    """The admin preview of an inferred class reasons from a slice fetched with the source's
    credential, never with no headers."""
    from provisa.sparql import reasoning

    seen: dict[str, Any] = {}

    async def _classification(source_id, axioms, url, headers, ttl):
        seen.update(url=url, headers=headers)
        raise reasoning.ReasonerFailed("stop here")

    monkeypatch.setattr(reasoning, "classification", _classification)
    mapping = {"auth": "bearer", "reasoner": "hermit", "axioms": "<Ontology/>"}
    out = await qa.preview_sparql(
        _Conn(_row(mapping, password=_TOKEN_REF)),  # type: ignore[arg-type]
        "graph",
        "<urn:kyc#SanctionsExposed>",
    )
    assert out.error is not None and "stop here" in out.error
    assert seen["url"] == _EP
    assert seen["headers"]["Authorization"] == "Bearer tok-123"


@pytest.mark.asyncio
async def test_a_table_read_sends_the_credential():
    """The source's tables (and an inferred table's slice, built from the same call) carry the
    credential on every call to the endpoint."""
    from provisa.api_source.caller import call_api
    from provisa.sparql.source import SparqlSourceConfig, build_endpoint

    cfg = SparqlSourceConfig(
        source_id="graph", endpoint_url=_EP, auth=ApiAuthBearer(token=_TOKEN_REF)
    )
    api_source = sparql_source.build_api_source(cfg)
    endpoint = build_endpoint(cfg, "adopter", "SELECT ?name WHERE { ?a ?p ?name }", [])
    with respx.mock:
        route = respx.post(_EP).mock(return_value=httpx.Response(200, json=_answer()))
        await call_api(
            endpoint,
            {},
            api_source.base_url,
            api_source.auth,
            source_headers=api_source.headers,
        )
    assert route.calls.last.request.headers["authorization"] == "Bearer tok-123"


class TestOwnEndpoint:
    def test_own_endpoint_is_the_public_address_and_data_sparql(self, public_url):
        assert is_own_endpoint(_OWN)
        assert is_own_endpoint(_OWN + "/")
        assert not is_own_endpoint(_EP)
        assert not is_own_endpoint(f"{_PUBLIC}/data/graphql")

    def test_with_no_public_address_nothing_is_own(self, monkeypatch):
        from provisa.core import settings_registry

        real = settings_registry.value
        monkeypatch.setattr(
            settings_registry,
            "value",
            lambda key: "" if key == "server.public_url" else real(key),
        )
        assert not is_own_endpoint(_OWN)

    def test_save_with_no_credential_is_refused_by_name(self, public_url):
        refused = qa.sparql_source_refusal("graph", "sparql", {}, _OWN, "", "")
        assert refused is not None and not refused.success
        assert refused.code == "sparql.own_endpoint_needs_credential"
        assert refused.params == {"source": "graph"}
        assert "graph" in refused.message

    def test_save_with_a_credential_passes(self, public_url):
        assert (
            qa.sparql_source_refusal("graph", "sparql", {"auth": "bearer"}, _OWN, "", "t") is None
        )

    def test_save_of_an_incomplete_credential_is_refused(self, public_url):
        refused = qa.sparql_source_refusal("graph", "sparql", {"auth": "bearer"}, _OWN, "", "")
        assert refused is not None and refused.code == "sparql.credential_invalid"

    def test_another_type_is_not_judged(self, public_url):
        assert qa.sparql_source_refusal("graph", "neo4j", {}, _OWN, "", "") is None

    @pytest.mark.asyncio
    async def test_preview_with_no_credential_is_refused_before_any_call(self, public_url):
        with respx.mock:
            route = respx.post(_OWN).mock(return_value=httpx.Response(200, json=_answer()))
            out = await qa.preview_sparql(
                _Conn(_row({}, host=_OWN)),  # type: ignore[arg-type]
                "graph",
                "SELECT ?name WHERE { ?a ?p ?name }",
            )
        assert not route.called
        assert out.error is not None and "Provisa's own SPARQL endpoint" in out.error

    @pytest.mark.asyncio
    async def test_registration_with_no_credential_is_refused_by_name(
        self, public_url, monkeypatch
    ):
        from provisa.api.app import state

        monkeypatch.setattr(state, "api_sources", {}, raising=False)
        model = Table(
            source_id="graph",
            domain_id="d",
            schema_name="sparql",
            table_name="adopter",
            query_template="SELECT ?name WHERE { ?a <urn:name> ?name }",
            columns=[Column(name="name", data_type="text", visible_to=_VIS)],
        )
        conn = _Conn(_row({}, host=_OWN))
        refused = await qa.persist_query_api_registration(conn, model)  # type: ignore[arg-type]
        assert refused is not None and refused.code == "sparql.own_endpoint_needs_credential"
        assert conn.upserts == []

    @pytest.mark.asyncio
    async def test_a_table_read_with_no_credential_is_refused_before_any_call(self, public_url):
        from provisa.api_source.caller import call_api
        from provisa.sparql.source import SparqlSourceConfig, build_endpoint

        cfg = SparqlSourceConfig(source_id="graph", endpoint_url=_OWN)
        endpoint = build_endpoint(cfg, "adopter", "SELECT ?name WHERE { ?a ?p ?name }", [])
        with respx.mock:
            route = respx.post(_OWN).mock(return_value=httpx.Response(200, json=_answer()))
            with pytest.raises(OwnEndpointNeedsCredential):
                await call_api(endpoint, {}, sparql_source.build_api_source(cfg).base_url, None)
        assert not route.called


@pytest.mark.asyncio
async def test_loader_reads_the_credential_from_the_source_row():
    """At load the api source's credential is the one its Source row declares now."""
    from provisa.api_source.loader import load_api_sources

    class _LoadConn:
        async def fetch(self, sql: str, *args: Any) -> list[dict]:
            if "FROM api_sources" in sql:
                return [
                    {
                        "id": "graph",
                        "type": "sparql",
                        "base_url": "http://fuseki.test:3030",
                        "spec_url": None,
                        "auth": None,
                        "spec_path": None,
                        "source_mapping": '{"auth": "bearer"}',
                        "source_username": "",
                        "source_password": _TOKEN_REF,
                    }
                ]
            return []

    types: dict[str, str] = {}
    _endpoints, api_sources = await load_api_sources(_LoadConn(), types)  # type: ignore[arg-type]
    assert api_sources["graph"].auth == ApiAuthBearer(token=_TOKEN_REF)
