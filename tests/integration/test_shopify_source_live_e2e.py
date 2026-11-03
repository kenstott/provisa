# Copyright (c) 2026 Kenneth Stott
# Canary: 3f9b2c71-6e0d-4a58-b1c4-8d27e5a9f063
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Live Shopify source (REQ-1923): a real dev store added, registered and read through the admin
API the UI uses, on an in-process app.

What it proves that the mocked store of tests/unit/test_graphql_brand_shopify.py cannot: the store
accepts the token in Shopify's own header; a narrow ``shopify__products`` registers and its rows
are read; a wide ``shopify__orders`` is refused with the cost numbers Shopify itself puts on it;
and a read follows Shopify's cursor across more than one page.

The app runs in this process on the local platform (its own Postgres); the integration compose
stack is not started. Nothing is written to the store.

Credentials
-----------
There is no Shopify emulator: a real store and an Admin API access token are required. Read from
the environment (``.env`` through tests/env_creds.py):

  SHOPIFY_STORE_DOMAIN  the store: ``<handle>.myshopify.com`` or the bare handle
  SHOPIFY_API_TOKEN     an Admin API access token for it (read_products, read_orders)

Like the live Microsoft 365 test it is in no CI lane (the store is the maintainer's own; CI holds
no Shopify secrets), and a credential that is not set fails it by name.
"""

# Requirements: REQ-1923
from __future__ import annotations

import json
import os
import re
from typing import Any

import pytest
import pytest_asyncio
import respx
from httpx import ASGITransport, AsyncClient

from tests.env_creds import unset
from tests.helpers import registered_id, release_mutation

pytestmark = [
    pytest.mark.integration,
    pytest.mark.local_platform,
    pytest.mark.requires_warehouse,
    pytest.mark.requires_shopify,
    pytest.mark.asyncio(loop_scope="session"),
]

_ENV = ("SHOPIFY_STORE_DOMAIN", "SHOPIFY_API_TOKEN")

_SOURCE_ID = "shopify-live"
_DOMAIN = "shopify_live"
_ROLE = "org_admin"
_VISIBLE_TO = f'["{_ROLE}"]'
# One row a page, so any store with two products is read across pages.
_PAGE_SIZE = 1


def _store_handle(domain: str) -> str:
    """The store's handle, whether the domain was given as ``x.myshopify.com`` (with or without a
    scheme and a trailing slash) or as the bare handle ``x``."""
    host = re.sub(r"^https?://", "", domain.strip()).split("/", 1)[0].lower()
    return host.removesuffix(".myshopify.com")


def test_the_store_handle_is_read_from_either_form_of_the_domain():
    assert _store_handle("acme") == "acme"
    assert _store_handle("acme.myshopify.com") == "acme"
    assert _store_handle("https://Acme.myshopify.com/") == "acme"


@pytest.fixture(scope="module")
def creds() -> dict[str, str]:
    missing = unset(*_ENV)
    if missing:
        pytest.fail(f"not set: {missing}")
    return {
        "store": _store_handle(os.environ["SHOPIFY_STORE_DOMAIN"]),
        "token": os.environ["SHOPIFY_API_TOKEN"],
    }


@pytest_asyncio.fixture(loop_scope="session", scope="module")
async def client(creds):
    """An in-process app, its remote GraphQL reads a page of one row each."""
    import provisa.api.app as app_mod

    del creds
    app = app_mod.create_app()
    async with app.router.lifespan_context(app):
        gql = app_mod.state.config.graphql_remote
        prior_page = gql.max_list_items
        gql.max_list_items = _PAGE_SIZE
        try:
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test", timeout=300) as c:
                yield c
        finally:
            gql.max_list_items = prior_page


async def _mutation(client: AsyncClient, query: str, field: str) -> dict[str, Any]:
    """An admin GraphQL mutation's result, success or not."""
    resp = await client.post("/admin/graphql", json={"query": query})
    assert resp.status_code == 200, f"{field}: HTTP {resp.status_code}: {resp.text}"
    body = resp.json()
    assert "errors" not in body, f"{field}: GraphQL errors: {resp.text}"
    return body["data"][field]


def _register_table(table: str, alias: str, columns: list[str]) -> str:
    cols = ", ".join(f'{{ name: "{c}", visibleTo: {_VISIBLE_TO} }}' for c in columns)
    return f"""
        mutation {{
            registerTable(input: {{
                sourceId: "{_SOURCE_ID}", domainId: "{_DOMAIN}", schemaName: "graphql",
                tableName: "{table}", alias: "{alias}", columns: [{cols}]
            }}) {{ success message code }}
        }}
    """


async def _registered(client: AsyncClient, table: str, alias: str, columns: list[str]) -> None:
    """``table`` registered with ``columns`` and released, as a steward does."""
    result = await _mutation(client, _register_table(table, alias, columns), "registerTable")
    assert result["success"], result
    released = await _mutation(
        client, release_mutation(registered_id(result["message"])), "setTableDraft"
    )
    assert released["success"], released


async def _sql(client: AsyncClient, sql: str) -> list[dict[str, Any]]:
    resp = await client.post("/data/sql", json={"sql": sql, "role": _ROLE})
    assert resp.status_code == 200, f"{sql!r}: HTTP {resp.status_code}: {resp.text}"
    return resp.json()["data"]["sql"]


@pytest_asyncio.fixture(loop_scope="session", scope="module")
async def store(client, creds) -> str:
    """The store added as a Shopify source; its endpoint."""
    domain = await _mutation(
        client,
        f'mutation {{ createDomain(input: {{ id: "{_DOMAIN}", description: "Shopify live" }}) '
        "{ success message } }",
        "createDomain",
    )
    assert domain["success"], domain
    resp = await client.post(
        "/admin/sources/graphql-remote",
        json={
            "source_id": _SOURCE_ID,
            "brand": "shopify",
            "setup": {"store": creds["store"]},
            "domain_id": _DOMAIN,
            "auth": {"type": "bearer", "token": creds["token"]},
            # The clock its landed tables refresh on, as the form's Load Management sets it.
            "cache_ttl": 300,
        },
    )
    # The body of a refusal never carries the token (graphql_remote_router._auth_without_secret).
    assert resp.status_code == 200, resp.text
    added = resp.json()
    assert added["brand"] == "shopify" and added["table_names"] == []
    from provisa.api.app import state

    return state.graphql_remote_sources[_SOURCE_ID]["url"]


async def test_products_are_read_a_page_at_a_time_following_shopifys_cursor(client, store):
    await _registered(client, "shopify__products", "products", ["id", "title", "handle"])
    with respx.mock(assert_all_called=False) as mock:
        sent = mock.post(store).pass_through()
        mock.route().pass_through()
        rows = await _sql(client, f'SELECT "id", "title", "handle" FROM "{_DOMAIN}"."products"')
    reads = [json.loads(call.request.content) for call in sent.calls]
    pages = [r for r in reads if f"products(first: {_PAGE_SIZE}, after: $pageCursor)" in r["query"]]
    cursors = [(r.get("variables") or {}).get("pageCursor") for r in pages]
    print(f"\nshopify__products: {len(rows)} rows in {len(pages)} pages of {_PAGE_SIZE}")
    assert rows, "the store holds no products"
    assert all(r["id"].startswith("gid://shopify/Product/") for r in rows)
    assert all(r["title"] and r["handle"] for r in rows)
    assert len(rows) == len({r["id"] for r in rows})
    # The first page is asked for without a cursor, every later one after the last page's end.
    assert len(pages) >= 2, reads
    assert cursors[0] is None and all(cursors[1:]), cursors
    assert len(pages) == len(rows)


async def test_a_wide_orders_table_is_refused_with_shopifys_cost_numbers(client, store):
    del store
    # Every column the schema offers for an order: more than Shopify runs in one query.
    result = await _mutation(
        client, _register_table("shopify__orders", "orders_wide", []), "registerTable"
    )
    print(f"\nshopify__orders (wide): {result['message']}")
    assert result["success"] is False and result["code"] == "schema.table_too_complex", result
    assert result["message"].startswith("Shopify will not run shopify__orders"), result
    cost = re.search(
        r"Query cost is (\d+), which exceeds the single query max cost limit \((\d+)\)",
        result["message"],
    )
    assert cost, result["message"]
    assert int(cost.group(1)) > int(cost.group(2))


async def test_orders_registered_with_a_few_columns_say_what_the_store_holds(client, store):
    del store
    await _registered(client, "shopify__orders", "orders", ["id", "name"])
    rows = await _sql(client, f'SELECT "id", "name" FROM "{_DOMAIN}"."orders"')
    print(f"\nshopify__orders (narrow): {len(rows)} rows")
    assert all(r["id"].startswith("gid://shopify/Order/") and r["name"] for r in rows)
