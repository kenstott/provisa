# Copyright (c) 2026 Kenneth Stott
# Canary: 6f1e2b7a-9c4d-4e58-8a31-2d7b0c9e5f14
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2006 (amended 2026-11-16), continued: on a server of many organizations,

- a configuration a held org's admin applies (or dry-runs) is refused, by name, when a webhook or
  a scheduled trigger in it calls a private destination -- as a save of either is;
- a destination that is not a source (a webhook, a trigger, a command, a Syncing or export
  endpoint) is refused in its own words (``egress.private_destination``), in every locale;
- a gRPC command, the Dataplex token exchange and the Qdrant client connect to the address the
  check resolved: a resolver that answers a public address once and a private one afterwards (DNS
  rebinding) cannot send them anywhere else."""

# Requirements: REQ-2006

from __future__ import annotations

import json
import socket
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
from pydantic import SecretStr

# The API layer names the deployment's org at import (sandbox_egress.set_deployment_org_resolver);
# imported first, so each test's install below is the one in force.
import provisa.api.app as app_module  # noqa: F401
from provisa.core import secrets_providers
from provisa.core.request_context import reset_current_org, set_current_org
from provisa.security import sandbox_egress as egress
from provisa.security.sandbox_egress import PrivateDestinationRefused, PrivateEndpointRefused

pytestmark = pytest.mark.unit

_ORG = "acme"
_DEPLOYMENT = "operator"
_PRIVATE = "10.1.2.3"
_PUBLIC = "93.184.216.34"
_URL = f"http://{_PRIVATE}:8080/hook"
_LOCALES = Path(__file__).resolve().parents[2] / "provisa-ui" / "src" / "i18n" / "locales"


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
def rebinding(monkeypatch):
    """A resolver that answers every name with a public address the first time it is asked and a
    private one every time after: whoever resolves the name again, after the check, is sent to a
    private address. Each answer is recorded."""
    answers: list[str] = []

    def _resolve(host, port, *args, **kwargs):
        address = _PUBLIC if not answers else _PRIVATE
        answers.append(address)
        return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", (address, port or 0))]

    monkeypatch.setattr(socket, "getaddrinfo", _resolve)
    return answers


@pytest.fixture
def public_dns(monkeypatch):
    """A resolver that answers every name with a public address."""

    def _resolve(host, port, *args, **kwargs):
        return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", (_PUBLIC, port or 0))]

    monkeypatch.setattr(socket, "getaddrinfo", _resolve)


@pytest.fixture
def wire(monkeypatch):
    """Every request that reaches httpx's transport -- its URL, Host header and TLS name --
    answered 200 with a token; and any request google-auth's own transport would send."""
    sent: list[tuple[str, str, str | None]] = []

    def _answer(request: httpx.Request) -> httpx.Response:
        sent.append(
            (str(request.url), request.headers["Host"], request.extensions.get("sni_hostname"))
        )
        return httpx.Response(
            200, json={"access_token": "minted", "expires_in": 3600}, request=request
        )

    def _handle(self, request):
        return _answer(request)

    def _requests_send(self, request, **kwargs):
        sent.append((request.url, "requests", None))
        raise ConnectionRefusedError("test: google-auth's own transport resolves the name again")

    import requests.adapters

    monkeypatch.setattr(httpx.HTTPTransport, "handle_request", _handle)
    monkeypatch.setattr(requests.adapters.HTTPAdapter, "send", _requests_send)
    return sent


# --- 1. a configuration's webhooks and triggers, at load ----------------------------------------


def _config(*, webhook_url: str | None = None, trigger_url: str | None = None):
    from provisa.core.config_loader import parse_config_dict

    data: dict = {"sources": [], "domains": [], "tables": [], "roles": []}
    if webhook_url is not None:
        data["webhooks"] = [{"name": "hook", "url": webhook_url}]
    if trigger_url is not None:
        data["scheduled_triggers"] = [
            {"id": "nightly", "name": "nightly", "cron": "0 * * * *", "url": trigger_url}
        ]
    return parse_config_dict(data)


class _Untouched:
    """A connection the apply must not reach: the refusal comes before any write."""

    def __getattr__(self, name):
        raise AssertionError(f"the apply reached the store ({name}) before refusing")


@pytest.mark.parametrize("field", ["webhook_url", "trigger_url"])
async def test_apply_refuses_a_private_destination_for_a_held_org(held_org, field):
    from provisa.core.config_loader import apply_config

    with pytest.raises(PrivateEndpointRefused) as refused:
        await apply_config(_config(**{field: _URL}), _Untouched())  # pyright: ignore[reportArgumentType]
    assert refused.value.params == {"host": _PRIVATE, "address": _PRIVATE}


@pytest.mark.parametrize("field", ["webhook_url", "trigger_url"])
async def test_dry_run_refuses_a_private_destination_for_a_held_org(held_org, field):
    from provisa.core.config_loader import dry_run_config

    async def _check_build(_conn):
        raise AssertionError("not reached")

    with pytest.raises(PrivateEndpointRefused):
        await dry_run_config(_config(**{field: _URL}), _Untouched(), None, _check_build)  # pyright: ignore[reportArgumentType]


def test_a_secret_reference_in_a_configured_url_is_judged_resolved(held_org, monkeypatch):
    from provisa.core import secrets
    from provisa.core.config_loader import _validate_destinations

    monkeypatch.setattr(
        secrets, "resolve_secrets", lambda v, **_kw: v.replace("${secret:H}", _PRIVATE)
    )
    config = SimpleNamespace(
        webhooks=[SimpleNamespace(url="http://${secret:H}/hook")], scheduled_triggers=[]
    )
    with pytest.raises(PrivateEndpointRefused):
        _validate_destinations(config)


def test_a_public_configured_destination_is_accepted_for_a_held_org(held_org, public_dns):
    from provisa.core.config_loader import _validate_destinations

    _validate_destinations(
        _config(webhook_url="https://hooks.example.com/a", trigger_url="https://t.example.com/b")
    )


def test_a_single_org_install_applies_a_private_destination(single_org):
    from provisa.core.config_loader import _validate_destinations

    _validate_destinations(_config(webhook_url=_URL, trigger_url=_URL))


# --- 2. a destination that is not a source is refused in its own words --------------------------


@pytest.mark.parametrize("save", ["create_webhook", "update_webhook"])
async def test_webhook_save_is_refused_as_a_destination(held_org, monkeypatch, save):
    from provisa.api.admin import actions_router

    monkeypatch.setattr(actions_router, "require_capability_request", lambda *a: None)
    body = actions_router.WebhookInput(name="hook", url=_URL, domainId="sales")
    request = SimpleNamespace(state=SimpleNamespace())
    with pytest.raises(PrivateEndpointRefused) as refused:
        if save == "create_webhook":
            await actions_router.create_webhook(request, body)  # pyright: ignore[reportArgumentType]
        else:
            await actions_router.update_webhook(request, "hook", body)  # pyright: ignore[reportArgumentType]
    assert refused.value.code == "egress.private_destination"
    assert "source" not in str(refused.value)


async def test_webhook_call_is_refused_as_a_destination(held_org):
    from provisa.core.models import Webhook
    from provisa.webhooks.executor import execute_webhook

    with pytest.raises(PrivateEndpointRefused):
        await execute_webhook(Webhook(name="hook", url=_URL), {})


async def test_trigger_call_is_refused_as_a_destination(held_org):
    from provisa.scheduler.trigger_runs import post_webhook

    with pytest.raises(PrivateEndpointRefused):
        await post_webhook(_URL, "nightly", {})


def test_command_is_refused_as_a_destination(held_org):
    from provisa.executor.function_dispatch import _check_egress

    with pytest.raises(PrivateEndpointRefused):
        _check_egress(SimpleNamespace(udf_egress_allowlist=[_PRIVATE]), _URL, "http")


def test_syncing_endpoint_is_refused_as_a_destination(held_org):
    with pytest.raises(PrivateEndpointRefused):
        egress.require_endpoint_url_allowed("https://10.0.0.9/sync")


def test_an_endpoint_name_that_does_not_resolve_is_refused_as_a_destination(held_org, monkeypatch):
    def _unknown(*_args, **_kwargs):
        raise socket.gaierror("test: no such name")

    monkeypatch.setattr(socket, "getaddrinfo", _unknown)
    with pytest.raises(PrivateEndpointRefused) as refused:
        egress.require_endpoint_url_allowed("https://hooks.invalid/a")
    assert refused.value.params == {
        "host": "hooks.invalid",
        "address": "a name that does not resolve",
    }


def test_a_source_is_still_refused_as_a_source(held_org):
    with pytest.raises(PrivateDestinationRefused) as refused:
        egress.require_url_allowed(f"http://{_PRIVATE}:6333")
    assert type(refused.value) is PrivateDestinationRefused
    assert refused.value.code == "sources.private_destination"


def _catalogs() -> list[Path]:
    return sorted(_LOCALES.glob("*/serverErrors.json"))


def test_every_locale_words_the_destination_refusal():
    catalogs = _catalogs()
    assert {p.parent.name for p in catalogs} >= {"en", "de", "fr", "ja", "zh", "zh-HK"}
    for path in catalogs:
        errors = json.loads(path.read_text())["serverErrors"]
        text = errors["egress"]["private_destination"]
        assert "{{host}}" in text and "{{address}}" in text, path
        assert "REQ-" not in text, path
        assert text != errors["sources"]["private_destination"], path


def test_english_destination_refusal_names_no_source():
    errors = json.loads((_LOCALES / "en" / "serverErrors.json").read_text())["serverErrors"]
    assert errors["egress"]["private_destination"] == (
        "This org may call only public addresses; {{host}} is {{address}}, which is not one."
    )


def test_every_locale_records_the_english_it_translated():
    for path in _catalogs():
        lng = path.parent.name
        if lng == "en":
            continue
        tm = json.loads((path.parent / ".tm.json").read_text())
        key = f"{lng}/serverErrors.json::serverErrors.egress.private_destination"
        assert tm[key] == (
            "This org may call only public addresses; {{host}} is {{address}}, which is not one."
        ), lng


# --- 3. connected to the address the check resolved ---------------------------------------------


class _Channel:
    def __init__(self, target: str) -> None:
        self.target = target

    def unary_unary(self, path, request_serializer, response_deserializer):
        async def _call(request, timeout):
            return b'{"ok": 1}'

        return _call

    async def close(self) -> None:
        return None


async def test_grpc_command_dials_the_checked_address(held_org, rebinding, monkeypatch):
    import grpc.aio

    from provisa.executor import function_dispatch as fd

    dialled: list[str] = []

    def _insecure(target, *args, **kwargs):
        dialled.append(target)
        return _Channel(target)

    monkeypatch.setattr(grpc.aio, "insecure_channel", _insecure)
    assert await fd._grpc_call("rpc.example.com:50051", "svc.Fn/Call", {}) == {"ok": 1}
    assert dialled == [f"{_PUBLIC}:50051"]
    assert rebinding == [_PUBLIC]


async def test_grpc_command_rebound_after_its_check_is_refused_as_a_destination(
    held_org, rebinding, monkeypatch
):
    import grpc.aio

    from provisa.executor import function_dispatch as fd

    dialled: list[str] = []
    monkeypatch.setattr(grpc.aio, "insecure_channel", lambda t, *a, **k: dialled.append(t))
    state = SimpleNamespace(udf_egress_allowlist=["rpc.example.com"])
    fd._check_egress(state, "rpc.example.com:50051", "grpc")
    with pytest.raises(PrivateEndpointRefused):
        await fd._grpc_call("rpc.example.com:50051", "svc.Fn/Call", {})
    assert dialled == []


def _service_account_key() -> str:
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import rsa

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    ).decode()
    return json.dumps(
        {
            "type": "service_account",
            "client_email": "exporter@proj-x.iam.gserviceaccount.com",
            "private_key": pem,
            "private_key_id": "k1",
            "token_uri": "https://oauth2.example.com/token",
        }
    )


def _dataplex():
    from provisa.api.metadata_export.dataplex import DataplexExport
    from provisa.core.models import MetadataExportTarget

    return DataplexExport(
        MetadataExportTarget(
            id="dataplex",
            provider="dataplex",
            endpoint="https://dataplex.example",
            auth_mode="google_service_account",
            google_service_account_json=SecretStr(_service_account_key()),
            gcp_project="proj-x",
            gcp_location="us",
            timeout_seconds=5,
        )
    )


async def test_dataplex_token_exchange_goes_to_the_checked_address(held_org, rebinding, wire):
    assert await _dataplex()._token() == "minted"
    assert wire == [
        (f"https://{_PUBLIC}/token", "oauth2.example.com", "oauth2.example.com"),
    ]
    assert rebinding == [_PUBLIC]


async def test_dataplex_token_exchange_to_a_private_address_is_refused(held_org, wire, monkeypatch):
    def _private(host, port, *args, **kwargs):
        return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", (_PRIVATE, port or 0))]

    monkeypatch.setattr(socket, "getaddrinfo", _private)
    with pytest.raises(PrivateEndpointRefused):
        await _dataplex()._token()
    assert wire == []


async def test_dataplex_token_exchange_on_a_single_org_install_is_by_name(single_org, wire):
    assert await _dataplex()._token() == "minted"
    assert wire == [("https://oauth2.example.com/token", "oauth2.example.com", None)]


@pytest.fixture
def qdrant_built(monkeypatch):
    import qdrant_client

    built: list[dict] = []
    monkeypatch.setattr(qdrant_client, "QdrantClient", lambda **kw: built.append(kw))
    return built


@pytest.mark.parametrize("grpc", [False, True])
def test_qdrant_connects_to_the_checked_address(held_org, rebinding, qdrant_built, grpc):
    from provisa.qdrant.fetch import QdrantConnection

    QdrantConnection(url="https://q.example.com:6333", api_key="k", prefer_grpc=grpc).client()
    (kwargs,) = qdrant_built
    assert kwargs["url"] == f"https://{_PUBLIC}:6333"
    assert kwargs["prefer_grpc"] is grpc
    assert kwargs["grpc_options"]["grpc.ssl_target_name_override"] == "q.example.com"
    assert rebinding == [_PUBLIC]


def test_qdrant_rest_requests_carry_the_name_to_the_checked_address(
    held_org, rebinding, qdrant_built
):
    from provisa.qdrant.fetch import QdrantConnection

    QdrantConnection(url="https://q.example.com:6333", api_key=None).client()
    (kwargs,) = qdrant_built
    (hook,) = kwargs["event_hooks"]["request"]
    request = httpx.Request("GET", f"https://{_PUBLIC}:6333/collections")
    hook(request)
    assert request.headers["Host"] == "q.example.com:6333"
    assert request.extensions["sni_hostname"] == "q.example.com"
    elsewhere = httpx.Request("GET", "https://other.example.com:6333/collections")
    with pytest.raises(PrivateDestinationRefused):
        hook(elsewhere)


def test_qdrant_of_a_single_org_install_is_by_name(single_org, qdrant_built):
    from provisa.qdrant.fetch import QdrantConnection

    QdrantConnection(url="http://qdrant:6333", api_key=None).client()
    assert qdrant_built == [{"url": "http://qdrant:6333", "api_key": None, "prefer_grpc": False}]
