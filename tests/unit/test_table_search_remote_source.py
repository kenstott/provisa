# Copyright (c) 2026 Kenneth Stott
# Canary: 5e2b9c41-7d3a-4f68-a1b0-c94e8d2f7a13
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-464: the Register Table form's "Find tables by description" searches the tables the form
offers for the source -- for a remote GraphQL source, the tables of its schema, a branded one's
the schema shipped with Provisa -- with their descriptions and columns."""

import time
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest

import provisa.api.app as app_module
from provisa.api.admin import table_search_router
from provisa.discovery import table_search
from provisa.graphql_remote.brands import BRANDS

SHOPIFY = BRANDS["shopify"]


@pytest.fixture(autouse=True)
def _one_organisation(monkeypatch):
    from provisa.core import secrets_providers

    monkeypatch.setattr(secrets_providers, "_many_organisations", lambda: False)


def _shopify_reg() -> dict:
    return {
        "source_id": "shop",
        "url": "https://acme.myshopify.com/admin/api/graphql.json",
        "namespace": "shopify",
        "auth": None,
        "brand": "shopify",
        "setup": {},
        "tables": [],
        "functions": [],
        "relationships": [],
    }


class _Pool:
    @asynccontextmanager
    async def acquire(self):
        yield SimpleNamespace()


@pytest.fixture
def shop_state(monkeypatch):
    """A process holding a Shopify source ``shop``, its catalog not yet indexed, no model key."""
    state = SimpleNamespace(
        source_types={"shop": "graphql_remote"},
        graphql_remote_sources={"shop": _shopify_reg()},
        source_pools=None,
        tenant_db=None,
        model_db=object(),
        config=SimpleNamespace(graphql_remote=SimpleNamespace(max_rows=10_000)),
    )
    monkeypatch.setattr(app_module, "state", state, raising=False)

    async def _cold(*_a, **_k):
        return None

    async def _pool():
        return _Pool()

    async def _no_keys(_db):
        return {}

    async def _no_model(*_a, **_k):
        return []  # no model to rank with: the text filter's answer is the answer

    monkeypatch.setattr("provisa.discovery.catalog_cache.read_cache", _cold)
    monkeypatch.setattr("provisa.api.admin.schema._get_pool", _pool)
    monkeypatch.setattr("provisa.core.org_secrets.read_org_api_keys", _no_keys)
    monkeypatch.setattr(table_search, "llm_rank", _no_model)
    return state


async def _search(q: str) -> list[str]:
    answer = await table_search_router.search_source_tables(
        SimpleNamespace(state=SimpleNamespace(identity=None)), "shop", q=q, schema_name="graphql"
    )
    return [c["table_name"] for c in answer["candidates"]]


async def test_a_customer_description_finds_the_shopify_customers(shop_state):
    assert "shopify__customers" in await _search("customer invoicing")


async def test_a_description_of_customer_orders_finds_the_customers_and_the_orders(shop_state):
    # Dozens of Shopify's tables are a kind of order (draft orders, a company's orders); the
    # orders table itself is among those the model is shown.
    found = await _search("customer orders")
    assert "shopify__customers" in found
    assert "shopify__orders" in found


async def test_the_search_is_given_every_table_the_register_form_offers(shop_state):
    from provisa.api.admin._graphql_table_registration import offered_tables, source_offer

    offered = {t["name"] for t in offered_tables(*await source_offer(shop_state, "shop"))}
    candidates = await table_search_router._candidates_live("shop", "graphql", shop_state)
    assert {c.name for c in candidates} == offered
    customers = next(c for c in candidates if c.name == "shopify__customers")
    assert customers.comment and "customers" in customers.comment
    assert "email" in " ".join(customers.columns) and "first_name" in customers.columns


async def test_a_catalog_indexed_before_the_source_was_read_does_not_narrow_the_search(
    shop_state, monkeypatch
):
    # The catalog cache holds only what an earlier walk listed -- here the one table registered
    # so far. The search is over what the source's schema offers now, as the form's list is.
    from provisa.discovery.catalog_cache import CachedTable

    async def _stale(*_a, **_k):
        return [
            CachedTable(
                schema_name="graphql", table_name="shopify__products", column_names=[], comment=None
            )
        ]

    monkeypatch.setattr("provisa.discovery.catalog_cache.read_cache", _stale)
    found = await _search("customer invoicing")
    assert "shopify__customers" in found


async def test_a_search_over_a_large_shipped_schema_answers_promptly(shop_state):
    started = time.monotonic()
    await _search("customers")
    assert time.monotonic() - started < 15.0
