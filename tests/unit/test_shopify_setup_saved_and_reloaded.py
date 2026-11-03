# Copyright (c) 2026 Kenneth Stott
# Canary: 3b9e72c4-1f6a-4d58-8e07-5c2a9d14f6b3
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A saved Shopify source loads with the org's model (REQ-1923). Its setup -- the store and the
API version -- was written into ``federation_hints``, a map of strings, as a dict: every load of the
model after the save failed validating the source, and the org's config reload with it. The setup
is the source's ``brand_setup``, saved, loaded into the model, and read back by the edit form."""

# Requirements: REQ-1923

from __future__ import annotations

from types import SimpleNamespace

import httpx
import pytest
import respx
from sqlalchemy import select

from provisa.core.schema_org import sources
from provisa.graphql_remote.brands import BRANDS, SHOPIFY_API_VERSION

SHOPIFY = BRANDS["shopify"]
STORE_URL = f"https://acme.myshopify.com/admin/api/{SHOPIFY_API_VERSION}/graphql.json"
SETUP = {"store": "acme", "api_version": SHOPIFY_API_VERSION}


@pytest.fixture
def org(control_plane, monkeypatch):
    """Org ``default``'s control plane as the process serves it."""
    import provisa.api.app as app

    async def no_rebuild():
        return None

    db = control_plane.org()
    state = SimpleNamespace(
        model_db=db,
        admin_db=control_plane.platform,
        active_org_id="default",
        graphql_remote_sources={},
        multitenancy=False,
    )
    monkeypatch.setattr(app, "state", state)
    monkeypatch.setattr(app, "_rebuild_schemas", no_rebuild)
    return SimpleNamespace(db=db, state=state)


@respx.mock
async def _save() -> None:
    from provisa.api.admin.graphql_remote_router import (
        GraphQLRemoteSourceRequest,
        _register_branded_source,
    )

    respx.post(STORE_URL).mock(
        return_value=httpx.Response(200, json={"data": {"shop": {"name": "Acme"}}})
    )
    body = GraphQLRemoteSourceRequest(
        source_id="my-shop",
        brand="shopify",
        setup={"store": "acme"},
        auth={"type": "bearer", "token": "shpat_x"},
    )
    await _register_branded_source(None, body)  # type: ignore[arg-type]


async def test_a_saved_shopify_source_loads_with_the_model(org):
    from provisa.core import store_config

    await _save()
    async with org.db.acquire() as conn:
        (written,) = [s for s in await store_config._sources(conn) if s["id"] == "my-shop"]
        row = (await conn.execute_core(select(sources).where(sources.c.id == "my-shop"))).one()
    assert written["brand_setup"] == SETUP
    assert written["federation_hints"]["brand"] == "shopify"
    assert all(isinstance(v, str) for v in row.federation_hints.values())
    assert row.brand_setup == SETUP


async def test_a_reloaded_shopify_source_shows_its_setup_to_the_edit_form(org):
    from provisa.api.admin.graphql_remote_router import list_graphql_remote_sources
    from provisa.api.app_loaders import _load_graphql_remote_sources_from_db

    await _save()
    org.state.graphql_remote_sources = {}  # a restarted process: nothing but the store
    await _load_graphql_remote_sources_from_db()

    reg = org.state.graphql_remote_sources["my-shop"]
    assert reg["url"] == STORE_URL and reg["brand"] == "shopify"
    assert reg["setup"] == SETUP
    request = SimpleNamespace(state=SimpleNamespace())  # no identity: the no-auth deployment
    (listed,) = await list_graphql_remote_sources(request)  # type: ignore[arg-type]
    assert listed["setup"] == SETUP
