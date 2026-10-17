# Copyright (c) 2026 Kenneth Stott
# Canary: b2e5d305-255e-4230-b41e-e4e866fe32d2
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2006: a sandbox org's sources reach only the destinations its demo declares and public
addresses -- checked when a source is saved, at each connection, and at every redirect hop."""

# Requirements: REQ-2006, REQ-1598

from __future__ import annotations

import asyncio

import httpx
import pytest

from provisa.core.request_context import reset_current_org, set_current_org
from provisa.security import sandbox_egress as egress

_SANDBOX = "sbx0123456789ab"
_PRIVATE = [
    "10.1.2.3",
    "172.16.0.9",
    "172.31.255.1",
    "192.168.4.4",
    "127.0.0.1",
    "169.254.169.254",
    "100.64.0.1",
    "0.0.0.0",
    "::1",
    "fc00::1",
    "fd00::5",
    "fe80::1",
    "::ffff:10.0.0.1",
]


@pytest.fixture
def in_sandbox():
    egress.set_demo_destinations(_SANDBOX, [{"host": "weblogs-db", "port": 5432}])
    token = set_current_org(_SANDBOX)
    yield
    reset_current_org(token)
    egress.forget(_SANDBOX)


@pytest.mark.parametrize("org", [_SANDBOX, "sbxtplweblogs", "sandbox"])
def test_every_kind_of_sandbox_org_is_confined(org):
    assert egress.confined(org)


@pytest.fixture
def organisations(monkeypatch):
    """Set whether the deployment holds many organizations, as the API layer says it."""
    from provisa.core import secrets_providers

    def _say(many: bool) -> None:
        monkeypatch.setattr(secrets_providers, "_many_organisations", lambda: many)

    return _say


def test_an_ordinary_org_on_a_deployment_of_one_reaches_a_private_address(organisations):
    organisations(False)
    assert not egress.confined("acme")
    assert not egress.held("acme")
    egress.require_allowed("10.0.0.1", 5432, "acme")


@pytest.mark.parametrize("address", ["10.10.0.4", "10.10.0.5", "127.0.0.1", "169.254.169.254"])
def test_an_ordinary_org_on_a_shared_deployment_is_refused_the_platforms_private_addresses(
    organisations, address
):
    organisations(True)
    assert egress.held("acme")
    with pytest.raises(egress.PrivateDestinationRefused) as refused:
        egress.require_connectable({"host": address, "port": 6379}, "redis", "acme")
    assert refused.value.code == "sources.private_destination"


def test_an_ordinary_orgs_unpinnable_public_source_on_a_shared_deployment_is_saved(
    organisations, monkeypatch
):
    organisations(True)
    monkeypatch.setattr(egress, "resolve", lambda host, port=None: ["93.184.216.34"])
    fields = {"host": "", "port": None, "base_url": "https://api.example.com/v1"}
    assert egress.require_connectable(fields, "openapi", "acme") is None


def test_an_ordinary_orgs_pinnable_source_on_a_shared_deployment_is_pinned(
    organisations, monkeypatch
):
    organisations(True)
    monkeypatch.setattr(egress, "resolve", lambda host, port=None: ["93.184.216.34"])
    fields = {"host": "db.example.com", "port": 5432}
    assert egress.require_connectable(fields, "postgresql", "acme") == "93.184.216.34"


@pytest.mark.parametrize("address", _PRIVATE)
def test_a_sandbox_source_on_a_private_address_is_refused(in_sandbox, address):
    with pytest.raises(egress.PrivateDestinationRefused) as refused:
        egress.require_source_allowed({"host": address, "port": 5432})
    assert refused.value.code == "sources.private_destination"


def test_a_name_resolving_to_a_private_address_is_refused(in_sandbox):
    with pytest.raises(egress.PrivateDestinationRefused):
        egress.require_allowed("localhost", 5432)


def test_a_source_with_a_port_and_no_host_is_localhost_and_refused(in_sandbox):
    with pytest.raises(egress.PrivateDestinationRefused):
        egress.require_source_allowed({"host": "", "port": 5432})


def test_urls_in_path_hints_and_mapping_are_checked(in_sandbox):
    for fields in (
        {"path": "http://169.254.169.254/latest/meta-data/"},
        {"base_url": "https://10.0.0.8/api"},
        {"federation_hints": {"endpoint": "http://192.168.1.1:9000"}},
        {"mapping": {"crawl": {"start": "http://127.0.0.1:8080/"}}},
    ):
        with pytest.raises(egress.PrivateDestinationRefused):
            egress.require_source_allowed(fields)


def test_the_demos_own_destination_and_a_public_one_are_reached(in_sandbox):
    egress.require_source_allowed({"host": "weblogs-db", "port": 5432})
    egress.require_source_allowed({"host": "8.8.8.8", "port": 443})
    egress.require_source_allowed({"path": "/data/orders.csv"})  # not a network destination
    with pytest.raises(egress.PrivateDestinationRefused):
        egress.require_allowed("weblogs-db", 5433)  # the demo's host, another port: not declared


def test_a_redirect_to_a_private_address_is_refused_before_it_is_sent(in_sandbox):
    sent: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        sent.append(str(request.url))
        return httpx.Response(302, headers={"location": "http://169.254.169.254/latest/"})

    async def fetch() -> None:
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(handler),
            follow_redirects=True,
            event_hooks=egress.http_event_hooks(),
        ) as client:
            await client.get("http://8.8.8.8/start")

    with pytest.raises(egress.PrivateDestinationRefused):
        asyncio.run(fetch())
    assert sent == ["http://8.8.8.8/start"]


def test_a_grpc_target_is_checked_and_pinned(in_sandbox, monkeypatch):
    with pytest.raises(egress.PrivateDestinationRefused):
        egress.pinned_address("10.0.0.2:50051")
    monkeypatch.setattr(egress, "resolve", lambda host, port=None: ["93.184.216.34"])
    assert egress.pinned_address("rpc.example.com:443") == ("93.184.216.34:443", "rpc.example.com")


def test_the_direct_pool_refuses_before_it_connects(in_sandbox):
    from provisa.executor.pool import SourcePool

    with pytest.raises(egress.PrivateDestinationRefused):
        asyncio.run(SourcePool().add("probe", "postgresql", "127.0.0.1", 5432, "x", "u", "p"))


# --- pinning: the address checked is the address connected to ----------------------------------

_PUBLIC = "93.184.216.34"


@pytest.fixture
def public_names(monkeypatch):
    """Names resolve to one public address, except ``rebind.example.com``: loopback."""

    def resolve(host, port=None):
        if host in ("rebind.example.com",):
            return ["127.0.0.1"]
        try:
            import ipaddress

            return [str(ipaddress.ip_address(host))]
        except ValueError:
            return [_PUBLIC]

    monkeypatch.setattr(egress, "resolve", resolve)


def _source(source_type: str, port: int, **extra):
    from provisa.core.models import Source

    return Source(
        id="probe",
        type=source_type,
        host="db.example.com",
        port=port,
        database="d",
        username="u",
        password="x",
        **extra,
    )


@pytest.mark.parametrize(
    ("source_type", "port", "extra", "pinned"),
    [
        ("postgresql", 5432, {}, f"host={_PUBLIC} port=5432"),
        ("sqlserver", 1433, {}, f"Server={_PUBLIC},1433"),
        ("firebird", 3050, {"path": "db.fdb"}, f"@{_PUBLIC}:3050/db.fdb"),
    ],
)
def test_each_pinnable_duckdb_connector_attaches_the_checked_address(
    in_sandbox, public_names, source_type, port, extra, pinned
):
    from provisa.federation.engine import build_engine

    attach = build_engine("duckdb").resolve(_source(source_type, port, **extra)).details["attach"]
    assert pinned in attach and "db.example.com" not in attach


def test_an_ordinary_orgs_source_is_attached_by_its_name(public_names, organisations):
    organisations(False)
    from provisa.federation.engine import build_engine

    attach = build_engine("duckdb").resolve(_source("postgresql", 5432)).details["attach"]
    assert "host=db.example.com" in attach


def test_the_demos_own_source_is_attached_by_its_name(in_sandbox, public_names):
    from provisa.core.models import Source
    from provisa.federation.engine import build_engine

    demo = Source(id="shop", type="postgresql", host="weblogs-db", port=5432, database="shop")
    assert "host=weblogs-db" in build_engine("duckdb").resolve(demo).details["attach"]


@pytest.mark.parametrize(
    "fields",
    [
        {"type": "csv", "path": "https://data.example.com/x.csv"},
        {"type": "clickhouse", "host": "ch.example.com", "port": 8123},
        {"type": "mongodb", "host": "mongodb://db.example.com:27017/?x=1"},
        {"type": "salesforce", "host": "acme.my.salesforce.com", "port": 443},
    ],
)
def test_a_destination_that_cannot_be_pinned_is_refused_by_name(in_sandbox, public_names, fields):
    source_type = fields.pop("type")
    with pytest.raises((egress.UnpinnableDestinationRefused, egress.PrivateDestinationRefused)):
        egress.require_connectable(fields, source_type)


def test_an_http_request_and_each_redirect_go_to_the_checked_address(in_sandbox, public_names):
    seen: list[tuple[str, str, str | None]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(
            (request.url.host, request.headers["host"], request.extensions.get("sni_hostname"))
        )
        if request.url.path == "/start":
            return httpx.Response(302, headers={"location": "http://next.example.com/end"})
        return httpx.Response(200)

    async def fetch() -> None:
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(handler),
            follow_redirects=True,
            event_hooks=egress.http_event_hooks(),
        ) as client:
            await client.get("http://api.example.com/start")

    asyncio.run(fetch())
    assert seen == [
        (_PUBLIC, "api.example.com", "api.example.com"),
        (_PUBLIC, "next.example.com", "next.example.com"),
    ]


def test_a_redirect_to_a_name_that_now_resolves_privately_is_refused(in_sandbox, public_names):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(302, headers={"location": "http://rebind.example.com/"})

    async def fetch() -> None:
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(handler),
            follow_redirects=True,
            event_hooks=egress.http_event_hooks(),
        ) as client:
            await client.get("http://api.example.com/")

    with pytest.raises(egress.PrivateDestinationRefused):
        asyncio.run(fetch())


def test_the_direct_pool_connects_to_the_checked_address(in_sandbox, public_names, monkeypatch):
    from provisa.executor import pool as pool_mod

    dialled: list[str] = []

    class _Driver:
        def bind_source(self, _sid):
            pass

        def configure(self, _extra):
            pass

        async def connect(self, host, *_args):
            dialled.append(host)

    monkeypatch.setattr(pool_mod, "create_driver", lambda *_a, **_k: _Driver())

    async def _no_vector(*_a):
        return None

    monkeypatch.setattr("provisa.vector.capability.detect_vector_capability", _no_vector)
    asyncio.run(
        pool_mod.SourcePool().add("probe", "postgresql", "db.example.com", 5432, "d", "u", "p")
    )
    assert dialled == [_PUBLIC]


# --- a host given as a URL: a Qdrant source's is its instance's URL ----------------------------

_QDRANT_URL = "http://provisa-demo-sonnets-202610161328-7d7b35b3a-qdrant-1:6333"


def _qdrant_pool(monkeypatch, host: str) -> list[str]:
    from provisa.executor import pool as pool_mod

    dialled: list[str] = []

    class _Driver:
        def bind_source(self, _sid):
            pass

        def configure(self, _extra):
            pass

        async def connect(self, host, *_args):
            dialled.append(host)

    monkeypatch.setattr(pool_mod, "create_driver", lambda *_a, **_k: _Driver())

    async def _no_vector(*_a):
        return None

    monkeypatch.setattr("provisa.vector.capability.detect_vector_capability", _no_vector)
    asyncio.run(pool_mod.SourcePool().add("vectors", "qdrant", host, 0, None, None, "k"))
    return dialled


def test_the_demos_own_qdrant_is_connected_by_its_url(monkeypatch):
    """The sonnets demo's Qdrant source names its store by URL, on the node's private network:
    the demo declares it, so each sandbox's direct pool connects to it by that URL."""
    egress.set_demo_destinations(_SANDBOX, [{"host": _QDRANT_URL, "port": 0}])
    token = set_current_org(_SANDBOX)
    try:
        assert _qdrant_pool(monkeypatch, _QDRANT_URL) == [_QDRANT_URL]
    finally:
        reset_current_org(token)
        egress.forget(_SANDBOX)


def test_a_url_host_the_demo_does_not_declare_is_refused_by_name(
    in_sandbox, public_names, monkeypatch
):
    """A URL is connected to by its name, which cannot be held to the address checked."""
    with pytest.raises(egress.UnpinnableDestinationRefused, match="qdrant.example.com"):
        _qdrant_pool(monkeypatch, "https://qdrant.example.com:6333")


def test_a_url_host_on_a_private_address_is_refused(in_sandbox, monkeypatch):
    with pytest.raises(egress.PrivateDestinationRefused):
        _qdrant_pool(monkeypatch, "http://127.0.0.1:6333")
