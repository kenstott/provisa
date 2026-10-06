# Copyright (c) 2026 Kenneth Stott
# Canary: 8f2a6d91-4c3e-4b07-a5d8-0e9c7b1f3a62
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A plain remote GraphQL source's tables are registered one at a time (REQ-308).

Adding the source registers nothing. What it offers comes from its own schema, read from its
endpoint; a table the steward registers is stored with the columns chosen and with how it is
read, so a process that starts later reads it without asking the remote for its schema."""

import json
from types import SimpleNamespace

import httpx
import pytest
import respx
from graphql import build_schema
from sqlalchemy import insert, select

from provisa.api.admin import _graphql_table_registration as registration
from provisa.core.database import Database, create_engine_from_url
from provisa.core.db import _init_schema_portable
from provisa.core.models import Column, GraphQLRemoteConfig, Table
from provisa.core.repositories import table as table_repo
from provisa.core.schema_org import domains, relationships, sources
from provisa.graphql_remote.brands import LiveSchema
from tests.unit.test_graphql_remote_relay import _type_ref

URL = "https://shop.example/graphql"

SDL = """
type Query {
  orders: [Order!]!
  order(id: Int!): Order
  customers: [Customer!]!
  shop(domain: String!): Shop
}
type Order { id: Int! total: Float customerId: Int customer: Customer }
type Customer { id: Int! name: String }
type Shop { name: String products(first: Int, after: String): ProductConnection! }
type Product { sku: String! title: String }
type PageInfo { hasNextPage: Boolean! endCursor: String }
type ProductConnection { nodes: [Product] pageInfo: PageInfo! }
"""


def _introspected() -> dict:
    schema = build_schema(SDL)
    types = []
    for name, t in schema.type_map.items():
        if name.startswith("__"):
            continue
        fields = None
        declared = getattr(t, "fields", None)
        if declared is not None:
            fields = [
                {
                    "name": fname,
                    "description": None,
                    "type": _type_ref(f.type),
                    "args": [
                        {"name": an, "defaultValue": None, "type": _type_ref(a.type)}
                        for an, a in f.args.items()
                    ],
                }
                for fname, f in declared.items()
            ]
        types.append({"kind": _type_ref(t)["kind"], "name": name, "fields": fields})
    return {"queryType": {"name": "Query"}, "mutationType": None, "types": types}


@pytest.fixture
async def state() -> SimpleNamespace:
    db = Database(create_engine_from_url("sqlite+pysqlite:///:memory:"), name="gql-register")
    await _init_schema_portable(db)
    async with db.acquire() as conn:
        await conn.execute_core(
            insert(sources).values(id="shop", type="graphql_remote", path=URL, mapping={})
        )
        await conn.execute_core(insert(domains).values(id="sales"))
    reg = {
        "source_id": "shop",
        "url": URL,
        "namespace": "shop",
        "domain_id": "",
        "auth": None,
        "tables": [],
    }
    return SimpleNamespace(
        model_db=db,
        tenant_db=db,
        graphql_remote_sources={"shop": reg},
        config=SimpleNamespace(graphql_remote=GraphQLRemoteConfig()),
    )


def _mock_schema() -> respx.Route:
    return respx.post(URL).mock(
        return_value=httpx.Response(200, json={"data": {"__schema": _introspected()}})
    )


async def _register(state, table_name: str, *columns: str, domain: str = "sales") -> dict:
    """What the registerTable mutation does for a remote GraphQL table: resolve its columns,
    record how it is read, store it."""
    offer, reg = await registration.source_offer(state, "shop")
    chosen = [Column(name=c, visible_to=["analyst"]) for c in columns]
    stored, omitted, fitted = await registration.columns_to_register(
        offer, reg, table_name, domain, chosen, 100
    )
    assert omitted == []
    await registration.remember_table(state, reg, table_name, fitted)
    async with state.tenant_db.acquire() as conn:
        await table_repo.upsert(
            conn,
            Table(
                source_id="shop",
                domain_id=domain,
                schema_name="graphql",
                table_name=table_name,
                columns=stored,
            ),
        )
    return fitted


# --- what a plain source offers ---


@respx.mock
async def test_the_schema_is_read_from_the_endpoint_once_and_kept(state):
    route = _mock_schema()
    offer, reg = await registration.source_offer(state, "shop")
    assert isinstance(offer, LiveSchema)
    await registration.source_offer(state, "shop")
    assert route.call_count == 1
    assert reg["schema"]["queryType"] == {"name": "Query"}


@respx.mock
async def test_every_table_is_offered_under_the_name_it_registers_with(state):
    _mock_schema()
    offered = await registration.source_offer(state, "shop")
    names = {t["name"] for t in registration.offered_tables(*offered)}
    assert names == {
        "shop__orders",
        "shop__order",
        "shop__customers",
        "shop__shop",
        "shop__shop_products",  # the connection under shop is a table of its own
    }


@respx.mock
async def test_a_table_not_yet_registered_offers_its_columns(state):
    _mock_schema()
    offered = await registration.source_offer(state, "shop")
    columns = {n: t for n, t, _ in registration.offered_columns(*offered, "shop__order")}
    assert columns == {
        "id": "integer",
        "total": "double",
        "customer_id": "integer",
        "customer": "json",
        "_nf_id": "integer",
    }


@respx.mock
async def test_a_plain_source_declares_nothing_to_check_so_the_remote_is_not_probed(state):
    route = _mock_schema()
    offer, reg = await registration.source_offer(state, "shop")
    await registration.columns_to_register(offer, reg, "shop__orders", "sales", [], 100)
    assert route.call_count == 1  # the schema read, and no query offered to the remote


# --- registering one ---


@respx.mock
async def test_only_the_table_registered_is_registered_with_the_columns_chosen(state):
    _mock_schema()
    await _register(state, "shop__orders", "id", "total")
    async with state.tenant_db.acquire() as conn:
        held = await table_repo.get_by_name(conn, "shop", "graphql", "shop__orders")
        other = await table_repo.get_by_name(conn, "shop", "graphql", "shop__customers")
    assert held is not None and other is None
    assert await registration.registered_column_names(state, "shop", "graphql", "shop__orders") == {
        "id",
        "total",
    }
    assert [t["sql_name"] for t in state.graphql_remote_sources["shop"]["tables"]] == [
        "shop__orders"
    ]


@respx.mock
async def test_how_a_registered_table_is_read_is_stored_with_the_source(state):
    _mock_schema()
    await _register(state, "shop__shop_products", "sku", "title")
    await _register(state, "shop__order", "id")
    async with state.tenant_db.acquire() as conn:
        row = (
            await conn.execute_core(select(sources.c.mapping).where(sources.c.id == "shop"))
        ).fetchone()
    specs = row.mapping["tables"]
    assert specs["shop__shop_products"]["field_name"] == "shop"
    assert specs["shop__shop_products"]["rows_path"] == ["products", "nodes"]
    assert [a["gql_type"] for a in specs["shop__shop_products"]["required_args"]] == ["String!"]
    assert specs["shop__order"]["field_name"] == "order"
    assert [a["gql_type"] for a in specs["shop__order"]["required_args"]] == ["Int!"]
    assert json.dumps(specs)  # stored as plain data


@respx.mock
async def test_registering_a_table_again_replaces_what_this_process_holds_for_it(state):
    _mock_schema()
    await _register(state, "shop__orders", "id")
    await _register(state, "shop__orders", "id", "total")
    tables = state.graphql_remote_sources["shop"]["tables"]
    assert len(tables) == 1
    assert {c["name"] for c in tables[0]["columns"]} == {"id", "total"}


@respx.mock
async def test_a_table_the_schema_does_not_offer_is_refused(state):
    _mock_schema()
    offer, reg = await registration.source_offer(state, "shop")
    with pytest.raises(KeyError, match="offers no table 'shop__invoices'"):
        await registration.columns_to_register(offer, reg, "shop__invoices", "sales", [], 100)


@respx.mock
async def test_a_table_named_as_the_remote_spells_it_is_found(state):
    _mock_schema()
    offer, reg = await registration.source_offer(state, "shop")
    # A hand-written model may name the field in the remote's own spelling.
    state.graphql_remote_sources["shop"]["namespace"] = ""
    offer, reg = await registration.source_offer(state, "shop")
    assert registration.offered_name(offer, reg, "shopProducts") == "shop_products"


# --- relationships between registered tables ---


@respx.mock
async def test_a_relationship_is_stored_once_both_of_its_tables_are_registered(state):
    _mock_schema()
    await _register(state, "shop__orders", "id", "customer_id", "customer")
    assert await registration.sync_detected_relationships(state, "shop") == 0
    await _register(state, "shop__customers", "id", "name")
    assert await registration.sync_detected_relationships(state, "shop") == 1
    async with state.tenant_db.acquire() as conn:
        rows = (
            await conn.execute_core(
                select(relationships.c.source_column, relationships.c.target_column)
            )
        ).fetchall()
    assert [(r.source_column, r.target_column) for r in rows] == [("customer_id", "id")]


@respx.mock
async def test_a_relationship_needs_the_column_it_rides_on_to_be_registered(state):
    _mock_schema()
    await _register(state, "shop__orders", "id", "total")  # customer not chosen
    await _register(state, "shop__customers", "id")
    assert await registration.sync_detected_relationships(state, "shop") == 0


# --- bringing registered tables up to date ---


@respx.mock
async def test_a_refresh_remaps_the_registered_tables_and_adds_no_column(state):
    _mock_schema()
    await _register(state, "shop__orders", "id")
    offer, reg = await registration.source_offer(state, "shop")
    tables = await registration.refreshed_registered_tables(state, offer, reg)
    assert [t["sql_name"] for t in tables] == ["shop__orders"]
    assert [c["name"] for c in tables[0]["columns"]] == ["id"]
    assert tables[0]["domain_id"] == "sales"
    assert tables[0]["registered_name"] == "shop__orders"
