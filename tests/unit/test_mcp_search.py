# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""MCP semantic catalog search — the "explore" surface (REQ-1008 phase 2).

Uses a deterministic bag-of-words embedding provider so the vector math is exact
and offline: cosine similarity reduces to token overlap, so the nearest chunk for
a query is the one sharing the most words.
"""

from types import SimpleNamespace

import pytest

from provisa.api.flight.catalog import CatalogColumn, CatalogTable
from provisa.api.mcp import search as search_mod
from provisa.api.mcp import tools

_VOCAB = [
    "customer",
    "email",
    "address",
    "order",
    "amount",
    "total",
    "product",
    "sku",
    "price",
    "region",
    "id",
    "created",
]


class BowProvider:
    """Bag-of-words embedder over a fixed vocab. dimensions == len(_VOCAB)."""

    async def embed(self, texts, model):
        out = []
        for t in texts:
            toks = t.lower().replace(".", " ").replace(",", " ").split()
            out.append([float(sum(1 for w in toks if w == v)) for v in _VOCAB])
        return out


def _model():
    return SimpleNamespace(id="bow", provider="bow", dimensions=len(_VOCAB), base_url=None)


def _catalog():
    return [
        CatalogTable(
            domain_id="sales",
            table_name="customers",
            description="Customer master",
            columns=[
                CatalogColumn("id", "integer", False, "customer id"),
                CatalogColumn("email", "varchar", True, "customer email address"),
                CatalogColumn("region", "varchar", True, "sales region"),
            ],
        ),
        CatalogTable(
            domain_id="sales",
            table_name="orders",
            description="Purchase orders",
            columns=[
                CatalogColumn("id", "integer", False, "order id"),
                CatalogColumn("amount", "double", True, "order total amount"),
                CatalogColumn("product_sku", "varchar", True, "product sku"),
            ],
        ),
        CatalogTable(
            domain_id="hr",
            table_name="employees",
            description="Staff",
            columns=[CatalogColumn("id", "integer", False, "employee id")],
        ),
    ]


class TestChunking:
    def test_iter_entities_covers_schemas_tables_columns(self):
        cat = _catalog()
        addrs = search_mod.iter_entities(cat)
        levels = [a[0] for a in addrs]
        assert levels.count("schema") == 2  # sales, hr
        assert levels.count("table") == 3
        assert levels.count("column") == 7  # 3 + 3 + 1

    def test_get_chunk_column_tier_is_plain_prose_with_provenance(self):
        cat = _catalog()
        txt = search_mod.get_chunk(("column", "sales", "customers", "email"), cat)
        assert "email" in txt and "customers" in txt and "sales" in txt and "varchar" in txt
        assert "#" not in txt and "*" not in txt  # not markdown

    def test_get_chunk_schema_tier_includes_description_and_tables(self):
        cat = _catalog()
        txt = search_mod.get_chunk(("schema", "sales", None, None), cat, {"sales": "Sales data"})
        assert "sales" in txt and "Sales data" in txt and "customers" in txt and "orders" in txt


@pytest.mark.asyncio
class TestIndex:
    async def test_build_and_search_finds_nearest_chunk(self):
        idx = search_mod.CatalogSearchIndex(_model(), BowProvider())
        n = await idx.build(_catalog())
        assert n == len(search_mod.iter_entities(_catalog()))
        hits = await idx.search("customer email address", k=3)
        assert hits, "expected at least one hit"
        top = hits[0]
        # The email column chunk shares the most words with the query.
        assert top.schema == "sales" and top.table == "customers"
        assert top.column == "email"


def _state():
    """Fake AppState: config with roles + vector_models, and a role context for FK lookup."""
    roles = [
        SimpleNamespace(id="analyst", domain_access=["sales"]),
        SimpleNamespace(id="hr_only", domain_access=["hr"]),
        SimpleNamespace(id="admin", domain_access=["*"]),
    ]
    vms = [
        SimpleNamespace(
            id="bow", provider="bow", dimensions=len(_VOCAB), base_url=None, enabled=True
        )
    ]
    config = SimpleNamespace(roles=roles, vector_models=vms, domains=[])
    ctx = SimpleNamespace(tables={}, joins={})
    return SimpleNamespace(
        config=config,
        contexts={"analyst": ctx, "hr_only": ctx, "admin": ctx},
        mcp_catalog_index=None,
    )


@pytest.mark.asyncio
class TestSearchCatalogTool:
    async def _prime(self, monkeypatch, state):
        async def fake_catalog(_s):
            return _catalog()

        monkeypatch.setattr(tools, "_catalog", fake_catalog)
        await tools.build_catalog_index(state, BowProvider())

    async def test_resolves_up_to_table_branch_with_columns(self, monkeypatch):
        state = _state()
        await self._prime(monkeypatch, state)
        res = await tools.search_catalog(state, "analyst", "customer email address", k=3)
        assert res
        top = res[0]
        assert top["schema"] == "sales" and top["table"] == "customers"
        assert top["matched_on"]["column"] == "email"
        assert {c["name"] for c in top["branch"]["columns"]} == {"id", "email", "region"}
        assert top["breadcrumb"] == "sales > customers"

    async def test_role_domain_filter_hides_inaccessible_schemas(self, monkeypatch):
        state = _state()
        await self._prime(monkeypatch, state)
        # hr_only may not see the sales branch even though the query targets it.
        res = await tools.search_catalog(state, "hr_only", "customer email address", k=5)
        assert all(r["schema"] == "hr" for r in res)

    async def test_missing_embedding_model_fails_loud(self, monkeypatch):
        state = _state()
        state.config.vector_models = []  # none enabled/registered
        monkeypatch.setattr(tools, "_catalog", lambda _s: _catalog_async())
        with pytest.raises(ValueError, match="embedding model"):
            await tools.search_catalog(state, "analyst", "anything", k=3)

    async def test_blank_query_rejected(self, monkeypatch):
        state = _state()
        await self._prime(monkeypatch, state)
        with pytest.raises(ValueError, match="search text"):
            await tools.search_catalog(state, "analyst", "   ", k=3)


async def _catalog_async():
    return _catalog()


class TestSearchCatalogEndpoint:
    """The browser-facing REST wrapper /admin/mcp/search-catalog."""

    def _client(self, monkeypatch, fake_search):
        import provisa.api.app as app_mod
        from fastapi import FastAPI
        from fastapi.testclient import TestClient

        from provisa.api.mcp import tools as tools_mod
        from provisa.api.mcp.status import router

        monkeypatch.setattr(app_mod, "state", SimpleNamespace(), raising=False)
        monkeypatch.setattr(tools_mod, "search_catalog", fake_search)
        app = FastAPI()
        app.include_router(router)
        return TestClient(app)

    def test_passes_role_header_and_returns_results(self, monkeypatch):
        seen = {}

        async def fake_search(state, role, query, k=5):
            seen["role"] = role
            seen["query"] = query
            seen["k"] = k
            return [{"schema": "sales", "table": "customers"}]

        client = self._client(monkeypatch, fake_search)
        r = client.post(
            "/admin/mcp/search-catalog",
            json={"query": "customer email", "k": 3},
            headers={"x-provisa-role": "analyst"},
        )
        assert r.status_code == 200
        assert r.json()["results"][0]["table"] == "customers"
        assert seen == {"role": "analyst", "query": "customer email", "k": 3}

    def test_permission_error_maps_to_403(self, monkeypatch):
        async def fake_search(state, role, query, k=5):
            raise PermissionError("No schema for role 'ghost'")

        client = self._client(monkeypatch, fake_search)
        r = client.post(
            "/admin/mcp/search-catalog",
            json={"query": "x"},
            headers={"x-provisa-role": "ghost"},
        )
        assert r.status_code == 403

    def test_value_error_maps_to_400(self, monkeypatch):
        async def fake_search(state, role, query, k=5):
            raise ValueError("role is required for every MCP tool call")

        client = self._client(monkeypatch, fake_search)
        r = client.post("/admin/mcp/search-catalog", json={"query": "x"})
        assert r.status_code == 400
