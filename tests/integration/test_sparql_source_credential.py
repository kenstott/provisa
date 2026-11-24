# Copyright (c) 2026 Kenneth Stott
# Canary: 3e9b6c14-8f2a-4d71-b5e0-9a4c2d7f1b68
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A sparql source reads Provisa's own endpoint as the credential it declares (REQ-2138).

One real server over a PostgreSQL source, ``orders(id, region)`` seeded east and west, with the
simple auth provider and two accounts:

* ``svc-reasoner`` -- kept for the service; its one role, ``east_reader``, reads only the east
  orders (a row rule);
* ``admin`` -- ``org_admin``, reads every order.

A sparql source points at that server's /data/sparql. The calls a source makes -- the reasoner's
slice (the CONSTRUCT its inferred tables are reasoned from), a SELECT table's read and the admin
preview -- are made with the headers the source's own call carries, and hold only what the
declared account may read. A source pointing there with no credential is refused by name before
any call leaves.
"""

# Requirements: REQ-2138, REQ-2135

from __future__ import annotations

import json
import os
import urllib.request

import bcrypt
import pytest
from rdflib import Graph, Literal, URIRef

from tests.integration.worker_boot_harness import WorkerBoot, _config

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

_PG_HOST = os.environ.get("PG_HOST", "localhost")
_PG_PORT = int(os.environ.get("PG_PORT", "5432"))
_PASSWORD = "svc-reasoner-pass"
_ADMIN_PASSWORD = "admin-pass"
_ROLES = ["org_admin", "east_reader"]


def _user(name: str, password: str, roles: list[str]) -> dict:
    hashed = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()
    return {"username": name, "password_hash": hashed, "roles": roles}


@pytest.fixture(scope="module")
def server():
    base = _config(_PG_HOST, _PG_PORT, "unused")
    orders = base["tables"][0]
    for column in orders["columns"]:
        column["visible_to"] = _ROLES
        if column["name"] == "id":
            column["is_primary_key"] = True
    boot = WorkerBoot(
        1,
        pg_host=_PG_HOST,
        pg_port=_PG_PORT,
        extra_config={
            "tables": [orders],
            "auth": {
                "provider": "simple",
                "allow_simple_auth": True,
                "jwt_secret": "sparql-source-credential-test-signing-key",
                # Each account acts as its own role (claims mode answers the deployment's
                # default role, analyst, for an account no rule maps).
                "role_mapping": [
                    {"claim": "username", "value": "svc-reasoner", "role": "east_reader"},
                    {"claim": "username", "value": "admin", "role": "org_admin"},
                ],
                "simple": {
                    "users": [
                        _user("svc-reasoner", _PASSWORD, ["east_reader"]),
                        _user("admin", _ADMIN_PASSWORD, ["org_admin"]),
                    ]
                },
            },
            "roles": [
                {
                    "id": "east_reader",
                    "description": "The reasoner service's role.",
                    "capabilities": ["query_development", "full_results"],
                    "domain_access": ["*"],
                },
            ],
            "rls_rules": [
                {
                    "description": "The service reads the east orders only.",
                    "table_id": "orders",
                    "role_id": "east_reader",
                    "filter": "region = 'east'",
                },
            ],
        },
        env={"PROVISA_REDIRECT_ENABLED": "false"},
    )
    # A subject's IRI starts with the deployment's public address (REQ-2135), and the source's
    # endpoint is that address's /data/sparql: the server's own.
    public = f"http://127.0.0.1:{boot.ports['http']}"
    boot._extra_env["PROVISA_PUBLIC_URL"] = public  # the port is known once leased, above
    boot.create_database()
    try:
        boot.start()
        boot.wait_all_ready(timeout=300)
        yield boot, public
    finally:
        boot.cleanup()


@pytest.fixture
def own(server, monkeypatch):
    """This process names the server's address as Provisa's public one, as the server does, and
    the accounts' credentials are secret references the source holds."""
    _boot, public = server
    monkeypatch.setenv("PROVISA_PUBLIC_URL", public)
    monkeypatch.setenv("SPARQL_SVC_TOKEN", _login(public, "svc-reasoner", _PASSWORD))
    monkeypatch.setenv("SPARQL_SVC_USER", "svc-reasoner")
    monkeypatch.setenv("SPARQL_SVC_PASS", _PASSWORD)
    monkeypatch.setenv("SPARQL_ADMIN_USER", "admin")
    monkeypatch.setenv("SPARQL_ADMIN_PASS", _ADMIN_PASSWORD)
    return public


def _login(public: str, user: str, password: str) -> str:
    req = urllib.request.Request(
        f"{public}/auth/login",
        data=json.dumps({"username": user, "password": password}).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read())["access_token"]


def _source(public: str, options: dict, username: str, password: str):
    from provisa.sparql.persist import sparql_config_from_source

    return sparql_config_from_source(
        source_id="kyc_graph",
        endpoint_url=f"{public}/data/sparql",
        options=options,
        username=username,
        password=password,
    )


_SERVICE_BEARER = ({"auth": "bearer"}, "", "${env:SPARQL_SVC_TOKEN}")
_SERVICE_BASIC = ({"auth": "basic"}, "${env:SPARQL_SVC_USER}", "${env:SPARQL_SVC_PASS}")
_ADMIN_BASIC = ({"auth": "basic"}, "${env:SPARQL_ADMIN_USER}", "${env:SPARQL_ADMIN_PASS}")


async def _slice_regions(public: str, credential: tuple) -> set[str]:
    """The regions in the slice an inferred table of the source is reasoned from: the CONSTRUCT
    fetched with the headers the table's own call carries (api_source.caller.prepare_call, the
    headers call_api hands the reasoner)."""
    from provisa.api_source.caller import prepare_call
    from provisa.sparql import reasoning
    from provisa.sparql.source import build_endpoint

    cfg, api_source = _source(public, *credential)
    endpoint = build_endpoint(
        cfg, "east_orders", f"<{public}/vocab/sales/orders>", [], reasoner=("hermit", "")
    )
    call = prepare_call(endpoint, {}, api_source.base_url, api_source.auth, api_source.headers)
    region = f"{public}/vocab/sales/orders#region"
    signature = reasoning.Signature((f"{public}/vocab/sales/orders",), (), (region,))
    graph = Graph().parse(
        data=await reasoning.fetch_slice(call.url, reasoning.slice_query(signature), call.headers),
        format="nt",
    )
    return {str(o) for o in graph.objects(None, URIRef(region)) if isinstance(o, Literal)}


async def test_the_slice_holds_only_the_service_accounts_rows(own):
    assert await _slice_regions(own, _SERVICE_BEARER) == {"east"}
    assert await _slice_regions(own, _SERVICE_BASIC) == {"east"}
    # The same slice under an account that reads every order holds them all: the service
    # account's roles, not the endpoint, bound what the source reads.
    assert await _slice_regions(own, _ADMIN_BASIC) == {"east", "west"}


async def test_a_select_table_reads_only_the_service_accounts_rows(own):
    from provisa.api_source.caller import answer_rows, call_api
    from provisa.api_source.models import ApiColumn, ApiColumnType
    from provisa.sparql.source import build_endpoint

    cfg, api_source = _source(own, *_SERVICE_BEARER)
    endpoint = build_endpoint(
        cfg,
        "orders",
        f"SELECT ?region WHERE {{ ?o <{own}/vocab/sales/orders#region> ?region }}",
        [ApiColumn(name="region", type=ApiColumnType.string)],
    )
    answer = await call_api(
        endpoint, {}, api_source.base_url, api_source.auth, source_headers=api_source.headers
    )
    rows, _cut = answer_rows(endpoint, answer)
    assert sorted(r["region"] for r in rows) == ["east"]


async def test_the_preview_reads_under_the_credential(own):
    from provisa.sparql.source import probe_endpoint

    query = f"SELECT ?region WHERE {{ ?o <{own}/vocab/sales/orders#region> ?region }}"
    cfg, _ = _source(own, *_SERVICE_BASIC)
    served = await probe_endpoint(cfg, query)
    assert [b["region"]["value"] for b in served["results"]["bindings"]] == ["east"]
    cfg, _ = _source(own, *_ADMIN_BASIC)
    served = await probe_endpoint(cfg, query)
    assert sorted(b["region"]["value"] for b in served["results"]["bindings"]) == [
        "east",
        "west",
    ]


async def test_a_source_on_the_own_endpoint_with_no_credential_is_refused_by_name(own):
    from provisa.api.admin._query_api_registration import sparql_source_refusal
    from provisa.api_source.caller import call_api
    from provisa.sparql.source import (
        OwnEndpointNeedsCredential,
        SparqlSourceConfig,
        build_api_source,
        build_endpoint,
        probe_endpoint,
    )

    url = f"{own}/data/sparql"
    refused = sparql_source_refusal("kyc_graph", "sparql", {}, url, "", "")
    assert refused is not None and refused.code == "sparql.own_endpoint_needs_credential"
    assert refused.params == {"source": "kyc_graph"}
    with pytest.raises(OwnEndpointNeedsCredential, match="kyc_graph"):
        _source(own, {}, "", "")
    cfg = SparqlSourceConfig(source_id="kyc_graph", endpoint_url=url)
    with pytest.raises(OwnEndpointNeedsCredential):
        await probe_endpoint(cfg, "SELECT ?s WHERE { ?s ?p ?o }")
    endpoint = build_endpoint(cfg, "orders", "SELECT ?s WHERE { ?s ?p ?o }", [])
    with pytest.raises(OwnEndpointNeedsCredential):
        await call_api(endpoint, {}, build_api_source(cfg).base_url, None)
