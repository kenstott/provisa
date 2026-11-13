# Copyright (c) 2026 Kenneth Stott
# Canary: 65f24234-9fdc-4b46-ad96-919cbc6cc33c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Each organisation searches its own catalog (REQ-1008, REQ-1266).

One process serves every organisation. ``search_catalog`` embeds the bound organisation's catalog
into an index and filters the hits by what the role reaches: both belong to that organisation.
Through the real application state and the real org binding, with a bag-of-words embedder and the
catalog read per bound org (``_catalog`` needs a control plane; the routing is what is under test).
"""

# Requirements: REQ-1008, REQ-1266

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.api.app import AppState
from provisa.api.flight.catalog import CatalogColumn, CatalogTable
from provisa.api.mcp import tools
from provisa.api.org_runtime import OrgRegistry, OrgRuntime
from provisa.compiler.sql_types import CompilationContext
from tests.unit.mcp_described import metas_for, serve_whole
from provisa.core.request_context import (
    require_current_org,
    reset_current_org,
    set_current_org,
)

pytestmark = pytest.mark.unbound

_VOCAB = ["customer", "email", "product", "sku", "price", "employee", "salary", "supplier"]


class _Embedder:
    """Bag-of-words over a fixed vocabulary, counting the chunks it was asked to embed."""

    def __init__(self) -> None:
        self.embedded = 0

    async def embed(self, texts, model):
        self.embedded += len(texts)
        rows = []
        for text in texts:
            words = text.lower().replace(".", " ").replace(",", " ").split()
            rows.append([float(words.count(v)) + 0.01 for v in _VOCAB])
        return rows


def _table(domain: str, name: str, *columns: str) -> CatalogTable:
    return CatalogTable(
        domain_id=domain,
        table_name=name,
        description=f"{name} table",
        columns=[CatalogColumn(c, "varchar", True, f"{c} of {name}") for c in columns],
    )


def _runtime(org: str, roles: dict[str, list[str]]) -> OrgRuntime:
    runtime = OrgRuntime(org_id=org)
    runtime.roles = {r: {"id": r, "domain_access": d} for r, d in roles.items()}
    runtime.contexts = {r: CompilationContext() for r in roles}
    return runtime


@pytest.fixture
def world(monkeypatch):
    """A state serving two organisations, whose deployment configuration is ``first``'s."""
    state = AppState.__new__(AppState)
    state.org_registry = OrgRegistry()
    state.config = SimpleNamespace(
        roles=[SimpleNamespace(id="analyst", domain_access=["sales"])],
        vector_models=[
            SimpleNamespace(
                id="bow", provider="bow", dimensions=len(_VOCAB), base_url=None, enabled=True
            )
        ],
        domains=[],
    )
    catalogs: dict[str, list[CatalogTable]] = {}

    async def catalog_of_the_bound_org(_state, _role):
        return catalogs[require_current_org()]

    embedder = _Embedder()
    monkeypatch.setattr(tools, "_catalog", catalog_of_the_bound_org)
    monkeypatch.setattr("provisa.vector.providers.get_provider", lambda _name: embedder)
    serve_whole(monkeypatch)
    return SimpleNamespace(state=state, catalogs=catalogs, embedder=embedder)


def _add_org(world, org: str, roles: dict[str, list[str]], catalog: list[CatalogTable]) -> None:
    world.state.org_registry.set(org, _runtime(org, roles))
    _publish(world, org, catalog)


def _publish(world, org: str, catalog: list[CatalogTable]) -> None:
    """``org``'s catalog, and the tables its roles' contexts hold, as one build makes them."""
    runtime = world.state.org_registry.get(org)
    for ctx in runtime.contexts.values():
        ctx.tables = metas_for(catalog)
    world.catalogs[org] = catalog


async def _search(world, org: str, role: str, text: str) -> list[dict]:
    token = set_current_org(org)
    try:
        return await tools.search_catalog(world.state, role, text, k=5)
    finally:
        reset_current_org(token)


def _found(results: list[dict]) -> set[tuple[str, str]]:
    return {(r["schema"], r["table"]) for r in results}


@pytest.mark.asyncio
async def test_the_second_org_to_search_gets_its_own_catalog(world):
    """The first org's search must not build the index every later org reuses."""
    _add_org(
        world, "first", {"analyst": ["sales"]}, [_table("sales", "customers", "customer_email")]
    )
    _add_org(world, "second", {"analyst": ["sales"]}, [_table("sales", "products", "product_sku")])

    assert _found(await _search(world, "first", "analyst", "customer email")) == {
        ("sales", "customers")
    }
    assert _found(await _search(world, "second", "analyst", "product sku")) == {
        ("sales", "products")
    }
    # And the first is still served its own once the second has built.
    assert _found(await _search(world, "first", "analyst", "customer email")) == {
        ("sales", "customers")
    }


@pytest.mark.asyncio
async def test_a_table_name_two_orgs_share_matches_on_each_orgs_own_columns(world):
    """Names repeat across orgs, so a hit on the other org's column would still resolve."""
    _add_org(world, "first", {"analyst": ["sales"]}, [_table("sales", "items", "email")])
    _add_org(
        world, "second", {"analyst": ["sales"]}, [_table("sales", "items", "price", "supplier")]
    )

    await _search(world, "first", "analyst", "email")
    (hit, *_) = await _search(world, "second", "analyst", "price")

    assert hit["matched_on"]["column"] == "price"
    assert [c["name"] for c in hit["branch"]["columns"]] == ["price", "supplier"]


@pytest.mark.asyncio
async def test_a_role_only_the_org_declares_reaches_that_orgs_domains(world):
    """The role's domains are the bound org's, not the deployment configuration's."""
    _add_org(world, "first", {"analyst": ["sales"]}, [_table("sales", "customers", "email")])
    _add_org(
        world, "second", {"buyer": ["purchasing"]}, [_table("purchasing", "supplier", "price")]
    )

    assert _found(await _search(world, "second", "buyer", "supplier price")) == {
        ("purchasing", "supplier")
    }


@pytest.mark.asyncio
async def test_a_role_name_two_orgs_share_is_scoped_by_the_bound_org(world):
    """``analyst`` reaches sales in the deployment's configuration and hr in the second org."""
    _add_org(world, "first", {"analyst": ["sales"]}, [_table("sales", "customers", "email")])
    _add_org(
        world,
        "second",
        {"analyst": ["hr"]},
        [_table("hr", "employee", "salary"), _table("sales", "employee_sales", "salary")],
    )

    assert _found(await _search(world, "second", "analyst", "employee salary")) == {
        ("hr", "employee")
    }


@pytest.mark.asyncio
async def test_reloading_one_orgs_catalog_leaves_the_others_index(world):
    """A rebuild invalidates the index of the org it is rebuilding, and no other."""
    _add_org(world, "first", {"analyst": ["sales"]}, [_table("sales", "customers", "email")])
    _add_org(world, "second", {"analyst": ["sales"]}, [_table("sales", "products", "sku")])
    await _search(world, "first", "analyst", "email")
    await _search(world, "second", "analyst", "sku")
    embedded = world.embedder.embedded

    _publish(world, "second", [_table("sales", "offers", "price")])
    token = set_current_org("second")
    world.state.mcp_catalog_index = None  # what _rebuild_schemas_impl does for the org it rebuilds
    reset_current_org(token)

    await _search(world, "first", "analyst", "email")
    assert world.embedder.embedded - embedded == 1, "only the query was embedded: no rebuild"

    assert _found(await _search(world, "second", "analyst", "price")) == {("sales", "offers")}
