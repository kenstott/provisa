# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2006 (amended 2026-11-16): on a server of many organizations every outbound call a held
organization can aim -- a webhook, a scheduled trigger's webhook, an http-kind command, a Syncing
or metadata-export endpoint, the Neo4j export, a remote code host, an OAuth or service-account
token exchange, an OCI registry and its sign-in realm, an OpenAPI spec's server, and the clients
of Elasticsearch, Druid, Pinot, Neo4j preview, SPARQL, the Kafka schema registries, embedding
models and Qdrant -- reaches public destinations only, refused by name. Each test asserts no
request leaves at the transport. A single-organization install is not held."""

# Requirements: REQ-2006

from __future__ import annotations

import socket
from types import SimpleNamespace

import httpx
import pytest

# The API layer names the deployment's org at import (sandbox_egress.set_deployment_org_resolver);
# imported first, so each test's install below is the one in force.
import provisa.api.app as app_module
from provisa.core import secrets_providers
from provisa.core.request_context import reset_current_org, set_current_org
from provisa.security import sandbox_egress as egress
from provisa.security.sandbox_egress import PrivateDestinationRefused

pytestmark = pytest.mark.unit

_ORG = "acme"
_DEPLOYMENT = "operator"
_PRIVATE = "10.1.2.3"
_PUBLIC = "93.184.216.34"
_URL = f"http://{_PRIVATE}:8080/hook"


def _install(monkeypatch, *, many: bool) -> None:
    monkeypatch.setattr(secrets_providers, "_many_organisations", lambda: many)
    monkeypatch.setattr(egress, "_deployment_org", lambda: _DEPLOYMENT)


@pytest.fixture
def held_org(monkeypatch):
    """An ordinary org on a server of many organizations, bound as the current org."""
    _install(monkeypatch, many=True)
    token = set_current_org(_ORG)
    yield _ORG
    reset_current_org(token)


@pytest.fixture
def single_org(monkeypatch):
    """The org of a single-organization install, bound as the current org."""
    _install(monkeypatch, many=False)
    token = set_current_org(_ORG)
    yield _ORG
    reset_current_org(token)


@pytest.fixture
def wire(monkeypatch):
    """Every request that reaches httpx's transport, answered 200 with a JSON row; and every
    socket dialled."""
    sent: list[str] = []

    def _answer(request: httpx.Request) -> httpx.Response:
        sent.append(str(request.url))
        return httpx.Response(200, json=[{"ok": 1}], request=request)

    async def _ahandle(self, request):
        return _answer(request)

    def _handle(self, request):
        return _answer(request)

    def _dial(address, *args, **kwargs):
        sent.append(f"socket:{address[0]}:{address[1]}")
        raise ConnectionRefusedError("test: no host")

    monkeypatch.setattr(httpx.AsyncHTTPTransport, "handle_async_request", _ahandle)
    monkeypatch.setattr(httpx.HTTPTransport, "handle_request", _handle)
    monkeypatch.setattr(socket, "create_connection", _dial)
    return sent


# --- webhooks and triggers ----------------------------------------------------------------------


def _webhook_state() -> SimpleNamespace:
    hook = {
        "name": "hook",
        "url": _URL,
        "method": "POST",
        "timeout_ms": 1000,
        "requires_approval": False,
        "arguments": [],
    }
    return SimpleNamespace(tracked_webhooks={"hook": hook})


@pytest.fixture
def admitted(monkeypatch):
    """A webhook call admitted and recorded, its rows returned as given."""
    from provisa.api.data import action_exec

    async def _rows(rows, _wh, _role, _state):
        return rows, None

    monkeypatch.setattr(action_exec, "admit_command", lambda *a: {})
    monkeypatch.setattr(action_exec, "_record_call", lambda *a: None)
    monkeypatch.setattr(action_exec, "govern_action_rows", _rows)
    return action_exec


async def test_webhook_call_refused_for_a_held_org(held_org, wire, admitted):
    with pytest.raises(PrivateDestinationRefused):
        await admitted.invoke_tracked_webhook("hook", {}, _webhook_state(), "analyst")
    assert wire == []


async def test_webhook_call_reaches_an_internal_address_on_a_single_org_install(
    single_org, wire, admitted
):
    rows = await admitted.invoke_tracked_webhook("hook", {}, _webhook_state(), "analyst")
    assert rows == [{"ok": 1}]
    assert wire == [_URL]


@pytest.mark.parametrize("save", ["create_webhook", "update_webhook"])
async def test_webhook_save_refused_for_a_held_org(held_org, wire, monkeypatch, save):
    from provisa.api.admin import actions_router

    monkeypatch.setattr(actions_router, "require_capability_request", lambda *a: None)
    body = actions_router.WebhookInput(name="hook", url=_URL, domainId="sales")
    request = SimpleNamespace(state=SimpleNamespace())
    with pytest.raises(PrivateDestinationRefused):
        if save == "create_webhook":
            await actions_router.create_webhook(request, body)  # pyright: ignore[reportArgumentType]
        else:
            await actions_router.update_webhook(request, "hook", body)  # pyright: ignore[reportArgumentType]
    assert wire == []


async def test_trigger_webhook_refused_for_a_held_org(held_org, wire):
    from provisa.scheduler.trigger_runs import post_webhook

    with pytest.raises(PrivateDestinationRefused):
        await post_webhook(_URL, "nightly", {})
    assert wire == []


async def test_execute_webhook_refused_for_a_held_org(held_org, wire):
    from provisa.core.models import Webhook
    from provisa.webhooks.executor import execute_webhook

    with pytest.raises(PrivateDestinationRefused):
        await execute_webhook(Webhook(name="hook", url=_URL), {})
    assert wire == []


# --- http-kind commands -------------------------------------------------------------------------


def test_command_loopback_refused_for_a_held_org(held_org):
    from provisa.api.errors import ApiError
    from provisa.executor.function_dispatch import _check_egress

    with pytest.raises(ApiError) as refused:
        _check_egress(SimpleNamespace(udf_egress_allowlist=[]), "http://127.0.0.1:5439/f", "http")
    assert refused.value.code == "functions.egress_denied"


def test_command_allowlisted_private_host_refused_for_a_held_org(held_org):
    from provisa.executor.function_dispatch import _check_egress

    state = SimpleNamespace(udf_egress_allowlist=[_PRIVATE])
    with pytest.raises(PrivateDestinationRefused):
        _check_egress(state, _URL, "http")
    with pytest.raises(PrivateDestinationRefused):
        _check_egress(state, f"{_PRIVATE}:50051", "grpc")


def test_command_loopback_allowed_on_a_single_org_install(single_org):
    from provisa.executor.function_dispatch import _check_egress

    _check_egress(SimpleNamespace(udf_egress_allowlist=[]), "http://127.0.0.1:5439/f", "http")


async def test_command_http_call_refused_for_a_held_org(held_org, wire):
    from provisa.executor.function_dispatch import _http_call

    with pytest.raises(PrivateDestinationRefused):
        await _http_call("POST", _URL, {}, 5.0)
    assert wire == []


# --- Syncing and metadata export ----------------------------------------------------------------


async def test_syncing_save_refused_for_a_held_org(held_org, wire, monkeypatch):
    from provisa.api.admin import syncing_router

    async def _settings():
        return {}

    async def _domains():
        return {"sales"}

    monkeypatch.setattr(syncing_router, "require_org_settings", lambda r: None)
    monkeypatch.setattr(syncing_router, "require_active_org_id", lambda r: _ORG)
    monkeypatch.setattr(syncing_router, "stored_settings", _settings)
    monkeypatch.setattr(syncing_router, "_domain_ids", _domains)
    entry = {
        "id": "catalog-1",
        "type": "data_catalog",
        "direction": "export",
        "provider": "openlineage",
        "endpoint": f"http://{_PRIVATE}:5000",
        "domains": ["sales"],
    }

    class _Request:
        state = SimpleNamespace()

        async def json(self):
            return {"entries": [entry]}

    with pytest.raises(PrivateDestinationRefused):
        await syncing_router.set_syncing(_Request())  # pyright: ignore[reportArgumentType]
    assert wire == []


async def test_metadata_export_call_refused_for_a_held_org(held_org, wire):
    from provisa.api.metadata_export import metadata_export
    from provisa.core.models import MetadataExportTarget

    target = MetadataExportTarget(
        id="catalog-1", provider="openlineage", endpoint=f"http://{_PRIVATE}:5000"
    )
    with pytest.raises(PrivateDestinationRefused):
        await metadata_export(target).health()
    assert wire == []


# --- the Neo4j export and preview, SPARQL -------------------------------------------------------


async def test_neo4j_export_refused_for_a_held_org(held_org, wire):
    from provisa.api.rest.graph_tools_router import Neo4jExportRequest, neo4j_export

    body = Neo4jExportRequest(
        url=f"http://{_PRIVATE}:7474",
        username="neo4j",
        password="pw",
        nodes=[{"label": "Person", "id": 1}],
        edges=[],
    )
    with pytest.raises(PrivateDestinationRefused):
        await neo4j_export(body)
    assert wire == []


async def test_neo4j_preview_refused_for_a_held_org(held_org, wire):
    from provisa.neo4j.preview import preview_query

    with pytest.raises(PrivateDestinationRefused):
        await preview_query(f"http://{_PRIVATE}:7474", "neo4j", "MATCH (n) RETURN n")
    assert wire == []


async def test_sparql_probe_refused_for_a_held_org(held_org, wire):
    from provisa.sparql.source import SparqlSourceConfig, probe_endpoint

    cfg = SparqlSourceConfig(source_id="s", endpoint_url=f"http://{_PRIVATE}:3030/ds/sparql")
    with pytest.raises(PrivateDestinationRefused):
        await probe_endpoint(cfg, "SELECT * WHERE { ?s ?p ?o }")
    assert wire == []


# --- remote code hosts --------------------------------------------------------------------------


def test_code_host_pairing_refused_for_a_held_org(held_org, wire):
    from provisa.codesource import remote

    with pytest.raises(PrivateDestinationRefused):
        remote.pair(_PRIVATE, 9443, f"{'a' * 64}.secret", remote.new_identity(), {})
    assert wire == []


def test_code_host_worker_refused_for_a_held_org(held_org, wire):
    from provisa.codesource import remote

    worker = remote.RemoteWorker(
        "code1", "", address=_PRIVATE, port=9443, pin="a" * 64, identity=remote.new_identity()
    )
    with pytest.raises(PrivateDestinationRefused):
        worker.start()
    assert wire == []


def test_code_host_worker_dials_its_own_org(wire, monkeypatch):
    """A worker built for a held org is held on a thread that has no org bound."""
    from provisa.codesource import remote

    _install(monkeypatch, many=True)
    worker = remote.RemoteWorker(
        "code1",
        "",
        address=_PRIVATE,
        port=9443,
        pin="a" * 64,
        identity=remote.new_identity(),
        org_id=_ORG,
    )
    with pytest.raises(PrivateDestinationRefused):
        worker.start()
    assert wire == []


def test_code_host_attach_refused_for_a_held_org(held_org):
    from provisa.federation.codehost_connectors import _remote

    source = SimpleNamespace(id="code1", mapping={"remote": {"host": _PRIVATE, "port": 9443}})
    with pytest.raises(PrivateDestinationRefused):
        _remote(source)


# --- token exchanges and registries -------------------------------------------------------------


def test_oauth_token_exchange_refused_for_a_held_org(held_org, wire):
    from provisa.api_source import oauth_grants

    with pytest.raises(PrivateDestinationRefused):
        oauth_grants._exchange(f"http://{_PRIVATE}/token", {"grant_type": "x"}, "credential")
    assert wire == []


def test_oci_registry_refused_for_a_held_org(held_org, wire):
    from provisa.codesource import oci

    with oci.Registry(f"{_PRIVATE}:5000")._client() as client:
        with pytest.raises(PrivateDestinationRefused):
            client.get(f"https://{_PRIVATE}:5000/v2/")
    assert wire == []


def test_oci_token_realm_refused_for_a_held_org(held_org, wire):
    from provisa.codesource import oci

    registry = oci.Registry(_PUBLIC)
    with registry._client() as client:
        with pytest.raises(PrivateDestinationRefused):
            registry._token(
                client, f'Bearer realm="http://{_PRIVATE}/token",service="r"', "repository:a:pull"
            )
    assert wire == []


def test_oci_redirect_to_a_private_address_refused_for_a_held_org(held_org, monkeypatch):
    """Each redirect hop is a request of its own, checked before it leaves."""
    from provisa.codesource import oci

    sent: list[str] = []

    def _handle(self, request):
        sent.append(str(request.url))
        return httpx.Response(302, headers={"Location": f"http://{_PRIVATE}/blob"}, request=request)

    monkeypatch.setattr(httpx.HTTPTransport, "handle_request", _handle)
    with oci.Registry(_PUBLIC)._client() as client:
        with pytest.raises(PrivateDestinationRefused):
            client.get(f"https://{_PUBLIC}/v2/")
    assert sent == [f"https://{_PUBLIC}/v2/"]


# --- OpenAPI (M5) -------------------------------------------------------------------------------


async def test_openapi_spec_server_refused_for_a_held_org(held_org, wire, monkeypatch):
    from provisa.api.admin import openapi_router

    monkeypatch.setattr(openapi_router, "require_capability_request", lambda *a: None)
    monkeypatch.setattr(app_module, "state", SimpleNamespace(openapi_specs={}))
    spec = {
        "openapi": "3.0.0",
        "info": {"title": "t", "version": "1"},
        "servers": [{"url": f"http://{_PRIVATE}:8000"}],
        "paths": {},
    }

    class _Request:
        async def json(self):
            return spec

    with pytest.raises(PrivateDestinationRefused):
        await openapi_router.put_openapi_spec("api1", _Request())  # pyright: ignore[reportArgumentType]
    assert wire == []


async def test_openapi_column_probe_refused_for_a_held_org(held_org, wire):
    from provisa.api.admin.schema_helpers import _dynamic_openapi_columns

    query = SimpleNamespace(path="/items", query_params=[], response_schema={})
    with pytest.raises(PrivateDestinationRefused):
        await _dynamic_openapi_columns(f"http://{_PRIVATE}:8000", query)
    assert wire == []


# --- connect-time hooks (M6) --------------------------------------------------------------------


def test_elasticsearch_mapping_refused_for_a_held_org(held_org, wire):
    from provisa.elasticsearch.source import fetch_index_mapping

    with pytest.raises(PrivateDestinationRefused):
        fetch_index_mapping(_PRIVATE, 9200, "orders")
    assert wire == []


def test_elasticsearch_client_refused_for_a_held_org(held_org, wire):
    from provisa.elasticsearch.fetch import ESConnection, list_indices

    with pytest.raises(PrivateDestinationRefused):
        list_indices(ESConnection(base_url=f"http://{_PRIVATE}:9200"))
    assert wire == []


def test_druid_refused_for_a_held_org(held_org, wire):
    from provisa.druid.fetch import DruidConnection, _sql

    with pytest.raises(PrivateDestinationRefused):
        _sql(DruidConnection.build(_PRIVATE, 8082), "SELECT 1")
    assert wire == []


def test_pinot_refused_for_a_held_org(held_org, wire):
    from provisa.pinot.fetch import PinotConnection, list_tables

    with pytest.raises(PrivateDestinationRefused):
        list_tables(PinotConnection.build(_PRIVATE, 9000))
    assert wire == []


async def test_schema_registry_refused_for_a_held_org(held_org, wire):
    from provisa.kafka.schema_registry import SchemaRegistryClient

    with pytest.raises(PrivateDestinationRefused):
        await SchemaRegistryClient(f"http://{_PRIVATE}:8081").get_subjects()
    assert wire == []


async def test_avro_schema_registry_refused_for_a_held_org(held_org, wire):
    from provisa.kafka.avro_registry import RegistrySettings, SchemaRegistry

    registry = SchemaRegistry(RegistrySettings(url=f"http://{_PRIVATE}:8081"))
    with pytest.raises(PrivateDestinationRefused):
        await registry._get("/subjects")
    assert wire == []


async def test_embedding_model_refused_for_a_held_org(held_org, wire, monkeypatch):
    from provisa.vector import providers

    monkeypatch.setattr(providers, "_ollama_address", lambda m: f"http://{_PRIVATE}:11434")
    model = SimpleNamespace(id="nomic-embed-text")
    with pytest.raises(PrivateDestinationRefused):
        await providers.OllamaProvider().embed(["a"], model)  # pyright: ignore[reportArgumentType]
    assert wire == []


def test_qdrant_refused_for_a_held_org(held_org, monkeypatch):
    import qdrant_client

    from provisa.qdrant.fetch import QdrantConnection

    built: list[str] = []
    monkeypatch.setattr(qdrant_client, "QdrantClient", lambda **kw: built.append(kw["url"]))
    with pytest.raises(PrivateDestinationRefused):
        QdrantConnection(url=f"http://{_PRIVATE}:6333", api_key=None).client()
    assert built == []
