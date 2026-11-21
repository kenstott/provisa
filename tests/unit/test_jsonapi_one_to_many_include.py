# Copyright (c) 2026 Kenneth Stott
# Canary: 3d8a1c47-5be2-4f09-a6e1-9c2f70b4d815
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1929 (amended 2026-11-18, a one-to-many include never collapses its resources).

An included resource reached by a one-to-many relationship is identified by its primary key where
one is declared, otherwise by its position among its parent's included resources, so every child
row stays a resource of its own. A position is scoped to its parent: the id is the parent's id and
the position, so children of different parents stay distinct.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import httpx
import pytest

from provisa.compiler import naming as _naming
from provisa.compiler.context import build_context
from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.rls import RLSContext
from provisa.compiler.schema_gen import SchemaInput, generate_schema
from tests.role_state import with_surfaces
from tests.unit.test_jsonapi_no_pk_include import _batches

_HEADERS = {"accept": "application/vnd.api+json", "X-Provisa-Role": "org_admin"}


def _items(order_id: int, skus: list[str]) -> list[dict]:
    return [{"lineNo": n + 1, "orderId": order_id, "sku": s} for n, s in enumerate(skus)]


def _orders() -> list[dict]:
    """Order 1 with three items, order 2 with two; the items share their order's order_id."""
    return [
        {"orderId": 1, "customer": "ann", "orderItems": _items(1, ["A", "B", "C"])},
        {"orderId": 2, "customer": "bob", "orderItems": _items(2, ["D", "E"])},
    ]


def _col(name: str, data_type: str = "varchar(100)") -> ColumnMetadata:
    return ColumnMetadata(column_name=name, data_type=data_type, is_nullable=True)


def _table(table_id: int, name: str, columns: list[str], pk: list[str]) -> dict:
    return {
        "id": table_id,
        "source_id": "shop-sqlite",
        "domain_id": "shop",
        "schema_name": "main",
        "table_name": name,
        "columns": [
            {"column_name": c, "visible_to": ["org_admin"], "is_primary_key": c in pk}
            for c in columns
        ],
        "write_ops": [],
        "write_returns_rows": False,
        "write_refused_forms": [],
    }


def _schema_input(item_pk: list[str]) -> SchemaInput:
    """orders (primary key order_id) -> order_items, one-to-many by order_items.order_id."""
    return SchemaInput(
        tables=[
            _table(1, "orders", ["order_id", "customer"], ["order_id"]),
            _table(2, "order_items", ["order_id", "line_no", "sku"], item_pk),
        ],
        relationships=[
            {
                "id": "order-items",
                "cardinality": "one-to-many",
                "source_table_id": 1,
                "source_column": "order_id",
                "target_table_id": 2,
                "target_column": "order_id",
                "graphql_alias": "orderItems",
            },
        ],
        column_types={
            1: [_col("order_id", "integer"), _col("customer")],
            2: [_col("order_id", "integer"), _col("line_no", "integer"), _col("sku")],
        },
        naming_rules=[],
        role={"id": "org_admin", "capabilities": ["full_results"], "domain_access": ["*"]},
        domains=[{"id": "shop", "description": "Shop"}],
    )


def _client(monkeypatch, item_pk: list[str]) -> httpx.AsyncClient:
    """The JSON:API router over a real apollo-convention schema and context.

    # unit: mock-justified — the engine is the external boundary; the router's include plan and
    # the Rust encoder, which are under test, run unmodified.
    """
    from fastapi import FastAPI

    from provisa.api.jsonapi.generator import create_jsonapi_router
    from provisa.auth.middleware import AuthMiddleware
    from provisa.executor import json_encoders

    real = json_encoders.jsonapi

    def encode(batches, columns, *args, **kwargs):
        return real(_batches(columns, _orders()), columns, *args, **kwargs)

    monkeypatch.setattr(json_encoders, "jsonapi", encode)

    _naming.configure(gql="apollo_graphql", sql="snake")
    si = _schema_input(item_pk)
    schema = generate_schema(si)
    ctx = build_context(si)

    state = MagicMock()
    state.schemas = {"org_admin": schema}
    state.contexts = {"org_admin": ctx}
    state.rls_contexts = {"org_admin": RLSContext.empty()}
    state.roles = {"org_admin": si.role}
    state.table_path_maps = {
        "org_admin": {
            gql: {
                "domain_id": "shop",
                "table_name": meta.table_name,
                "write_ops": [],
                "write_returns_rows": False,
                "write_refused_forms": [],
            }
            for gql, meta in ctx.tables.items()
        }
    }
    with_surfaces(state)
    state.masking_rules = {}
    state.source_types = {"shop-sqlite": "sqlite"}
    state.source_dialects = {"shop-sqlite": "sqlite"}

    async def _fake_govern(
        sql,
        role_id,
        exec_params=None,
        state=None,
        deliver=None,
        buffered=False,
        *,
        cache_hint,
        serve_cached,
        sdl_joins,
    ):
        return SimpleNamespace(_is_count="COUNT(*)" in sql)

    async def _fake_execute(plan, state):
        result = MagicMock()
        result.redirect = None
        result.cache_entry = None
        result.rows = [[2]] if plan._is_count else [[]]
        return result

    monkeypatch.setattr("provisa.pgwire._pipeline._govern_and_route_compiled", _fake_govern)
    monkeypatch.setattr("provisa.pgwire._pipeline._execute_plan", _fake_execute)
    monkeypatch.setattr("provisa.auth.middleware._loaded_derived_roles", lambda: [])

    app = FastAPI()
    app.add_middleware(AuthMiddleware)
    app.include_router(create_jsonapi_router(state))
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test")


@pytest.fixture(autouse=True)
def _reset_naming():
    yield
    _naming.configure()


async def _orders_with_items(client: httpx.AsyncClient, params: dict | None = None) -> dict:
    async with client as c:
        r = await c.get(
            "/data/jsonapi/shop/orders",
            params={"include": "order_items", **(params or {})},
            headers=_HEADERS,
        )
    assert r.status_code == 200, r.text
    return r.json()


def _linkage(body: dict) -> list[list[str]]:
    return [[d["id"] for d in o["relationships"]["order_items"]["data"]] for o in body["data"]]


@pytest.mark.anyio
async def test_one_to_many_without_a_primary_key_keeps_every_child(monkeypatch):
    """No primary key on order_items: each item is identified by its order and its position
    among that order's items; none collapses, and each order links to all of its own."""
    body = await _orders_with_items(_client(monkeypatch, item_pk=[]))
    assert [d["id"] for d in body["data"]] == ["1", "2"]
    ids = ["1:0", "1:1", "1:2", "2:0", "2:1"]
    assert [(i["type"], i["id"]) for i in body["included"]] == [("order_items", i) for i in ids]
    assert [i["attributes"]["sku"] for i in body["included"]] == ["A", "B", "C", "D", "E"]
    assert _linkage(body) == [["1:0", "1:1", "1:2"], ["2:0", "2:1"]]
    for d in body["data"]:
        assert all(x["type"] == "order_items" for x in d["relationships"]["order_items"]["data"])


@pytest.mark.anyio
async def test_one_to_many_with_a_primary_key_is_identified_by_it(monkeypatch):
    """A declared primary key on order_items is each item's id."""
    body = await _orders_with_items(_client(monkeypatch, item_pk=["order_id", "line_no"]))
    ids = ["1:1", "1:2", "1:3", "2:1", "2:2"]
    assert [i["id"] for i in body["included"]] == ids
    assert _linkage(body) == [["1:1", "1:2", "1:3"], ["2:1", "2:2"]]


@pytest.mark.anyio
async def test_a_primary_key_left_out_by_fields_is_still_read_for_the_id(monkeypatch):
    """fields[order_items] naming only sku still identifies each item by its primary key."""
    body = await _orders_with_items(
        _client(monkeypatch, item_pk=["order_id", "line_no"]), {"fields[order_items]": "sku"}
    )
    assert [i["id"] for i in body["included"]] == ["1:1", "1:2", "1:3", "2:1", "2:2"]
    assert [i["attributes"] for i in body["included"]][0] == {"sku": "A"}
