# Copyright (c) 2026 Kenneth Stott
# Canary: 7e3f9a12-b841-4d5c-8c20-e6f1a3d07b94
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration tests for GraphQL Remote Schema Connector (REQ-307–313).

Boundary under test: introspect→mapper and mapper→ColumnMetadata synthesis.
No mocks at the boundary under test; HTTP mock is justified because the remote
GQL endpoint is an external third-party service not in the docker-compose stack.

# integration: mock-justified — mocking remote GQL endpoint;
# boundary under test is introspect→mapper
"""

from __future__ import annotations

import pytest
import httpx
import respx

from provisa.graphql_remote.introspect import introspect_schema
from provisa.graphql_remote.mapper import map_schema

pytestmark = [pytest.mark.integration]

REMOTE_URL = "https://shopify-test.example.com/graphql"

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _scalar(name: str) -> dict:
    return {"kind": "SCALAR", "name": name, "ofType": None}


def _object_ref(name: str) -> dict:
    return {"kind": "OBJECT", "name": name, "ofType": None}


def _list_of(inner: dict) -> dict:
    return {"kind": "LIST", "name": None, "ofType": inner}


def _non_null(inner: dict) -> dict:
    return {"kind": "NON_NULL", "name": None, "ofType": inner}


def _gql_introspection_response(schema: dict) -> dict:
    return {"data": {"__schema": schema}}


def _make_schema(
    query_fields: list[dict] | None = None,
    mutation_fields: list[dict] | None = None,
    extra_types: list[dict] | None = None,
) -> dict:
    """Build a minimal __schema dict."""
    types: list[dict] = []
    if query_fields is not None:
        types.append({"kind": "OBJECT", "name": "Query", "fields": query_fields})
    if mutation_fields is not None:
        types.append({"kind": "OBJECT", "name": "Mutation", "fields": mutation_fields})
    if extra_types:
        types.extend(extra_types)
    return {
        "queryType": {"name": "Query"} if query_fields is not None else None,
        "mutationType": {"name": "Mutation"} if mutation_fields is not None else None,
        "types": types,
    }


# ---------------------------------------------------------------------------
# Fixture: Shopify-like introspection payload
# ---------------------------------------------------------------------------

SHOPIFY_SCHEMA = _make_schema(
    query_fields=[
        {
            "name": "orders",
            "description": "List customer orders",
            "type": _list_of(_object_ref("Order")),
            "args": [
                {"name": "first", "type": _scalar("Int"), "defaultValue": None},
                {"name": "after", "type": _scalar("String"), "defaultValue": None},
            ],
        },
        {
            "name": "products",
            "description": "Product catalogue",
            "type": _list_of(_object_ref("Product")),
            "args": [],
        },
        {
            "name": "orderById",
            "description": "Single order",
            "type": _object_ref("Order"),
            "args": [
                {
                    "name": "id",
                    "type": _non_null(_scalar("ID")),
                    "defaultValue": None,
                }
            ],
        },
    ],
    mutation_fields=[
        {
            "name": "createOrder",
            "description": "Create a new order",
            "type": _object_ref("OrderResult"),
            "args": [
                {"name": "customerId", "type": _scalar("ID"), "defaultValue": None},
                {"name": "items", "type": _scalar("String"), "defaultValue": None},
            ],
        }
    ],
    extra_types=[
        {
            "kind": "OBJECT",
            "name": "Order",
            "description": "A purchase order",
            "fields": [
                {"name": "id", "type": _scalar("ID")},
                {"name": "total", "type": _scalar("Float")},
                {"name": "status", "type": _scalar("String")},
                {"name": "product", "type": _object_ref("Product")},
            ],
        },
        {
            "kind": "OBJECT",
            "name": "Product",
            "description": "A product",
            "fields": [
                {"name": "id", "type": _scalar("ID")},
                {"name": "name", "type": _scalar("String")},
                {"name": "price", "type": _scalar("Float")},
            ],
        },
        {
            "kind": "OBJECT",
            "name": "OrderResult",
            "description": "Result of createOrder",
            "fields": [
                {"name": "id", "type": _scalar("ID")},
                {"name": "success", "type": _scalar("Boolean")},
            ],
        },
    ],
)


# ---------------------------------------------------------------------------
# REQ-307 — introspect→mapper boundary
# ---------------------------------------------------------------------------


@pytest.mark.anyio
@respx.mock
async def test_introspect_then_map_produces_virtual_tables():
    # REQ-307: Provisa introspects via __schema query; result feeds into mapper.
    # integration: mock-justified — mocking remote GQL endpoint;
    # boundary under test is introspect→mapper
    respx.post(REMOTE_URL).mock(
        return_value=httpx.Response(200, json=_gql_introspection_response(SHOPIFY_SCHEMA))
    )

    schema = await introspect_schema(REMOTE_URL)
    tables, functions, relationships = map_schema(schema, "Shopify", "shopify-src")

    table_names = {t["name"] for t in tables}
    assert "Shopify__orders" in table_names
    assert "Shopify__products" in table_names


@pytest.mark.anyio
@respx.mock
async def test_introspect_with_bearer_auth():
    # REQ-307: optional auth (bearer) forwarded during introspection.
    # integration: mock-justified — mocking remote GQL endpoint;
    # boundary under test is introspect→mapper
    route = respx.post(REMOTE_URL).mock(
        return_value=httpx.Response(200, json=_gql_introspection_response(SHOPIFY_SCHEMA))
    )

    schema = await introspect_schema(REMOTE_URL, auth={"type": "bearer", "token": "secret-tok"})
    tables, _, _ = map_schema(schema, "Shopify", "shopify-src")

    req = route.calls[0].request
    assert req.headers["Authorization"] == "Bearer secret-tok"
    assert len(tables) > 0


# ---------------------------------------------------------------------------
# REQ-308 — Query fields → virtual tables; Mutation fields → tracked functions
# ---------------------------------------------------------------------------


def test_query_fields_become_virtual_tables():
    # REQ-308: Each Query field with OBJECT return type → virtual read-only table.
    tables, functions, _ = map_schema(SHOPIFY_SCHEMA, "Shopify", "shopify-src")

    table_names = {t["name"] for t in tables}
    assert "Shopify__orders" in table_names
    assert "Shopify__products" in table_names

    for t in tables:
        # virtual tables must carry source_id
        assert t["source_id"] == "shopify-src"
        # each table has a columns list
        assert isinstance(t["columns"], list)
        assert len(t["columns"]) > 0


def test_mutation_fields_become_tracked_functions():
    # REQ-308: Each Mutation field → tracked function with return_schema.
    _, functions, _ = map_schema(SHOPIFY_SCHEMA, "Shopify", "shopify-src")

    fn_names = {f["name"] for f in functions}
    assert "Shopify__createOrder" in fn_names

    fn = next(f for f in functions if f["name"] == "Shopify__createOrder")
    assert fn["source_id"] == "shopify-src"
    rs_names = {r["name"] for r in fn["return_schema"]}
    assert "id" in rs_names
    assert "success" in rs_names


def test_columns_have_correct_provisa_types():
    # REQ-308: column types inferred from GQL scalar → Provisa type mapping.
    tables, _, _ = map_schema(SHOPIFY_SCHEMA, "Shopify", "shopify-src")
    order_table = next(t for t in tables if t["name"] == "Shopify__orders")
    col_map = {c["name"]: c["type"] for c in order_table["columns"]}

    assert col_map["id"] == "text"  # ID → text
    assert col_map["total"] == "numeric"  # Float → numeric
    assert col_map["status"] == "text"  # String → text


def test_required_args_registered_on_table():
    # REQ-308: Required (NON_NULL) args on a query field are stored in required_args.
    tables, _, _ = map_schema(SHOPIFY_SCHEMA, "Shopify", "shopify-src")
    order_by_id = next(t for t in tables if t["name"] == "Shopify__orderById")
    req_arg_names = {a["name"] for a in order_by_id["required_args"]}
    assert "id" in req_arg_names


# ---------------------------------------------------------------------------
# REQ-309 — Column metadata synthesis (ColumnMetadata objects from mapper output)
# ---------------------------------------------------------------------------


def test_mapper_output_has_all_column_fields():
    # REQ-309: mapper synthesizes column metadata that executor can use to build
    # the GQL query and materialize response rows.
    tables, _, _ = map_schema(SHOPIFY_SCHEMA, "Shopify", "shopify-src")
    orders = next(t for t in tables if t["name"] == "Shopify__orders")

    for col in orders["columns"]:
        assert "name" in col
        assert "type" in col
        # description is present (may be None)
        assert "description" in col


def test_object_column_carries_gql_selection():
    # REQ-309: OBJECT-typed columns carry gql_selection so executor can embed
    # the sub-selection string when forwarding the query to the remote endpoint.
    tables, _, _ = map_schema(SHOPIFY_SCHEMA, "Shopify", "shopify-src")
    orders = next(t for t in tables if t["name"] == "Shopify__orders")
    col_map = {c["name"]: c for c in orders["columns"]}

    # "product" is an OBJECT field on Order → must have gql_selection
    assert "gql_selection" in col_map["product"]
    assert "product" in col_map["product"]["gql_selection"]


def test_pagination_args_detected():
    # REQ-309: pagination args on query fields detected and stored so executor
    # can pass limit/offset to the remote endpoint instead of truncating locally.
    tables, _, _ = map_schema(SHOPIFY_SCHEMA, "Shopify", "shopify-src")
    orders = next(t for t in tables if t["name"] == "Shopify__orders")
    pg = orders["pagination"]
    assert pg["limit_arg"] == "first"
    assert pg["cursor_arg"] == "after"


# ---------------------------------------------------------------------------
# REQ-310 — Governance applied identically (field_name stored, source_id stored)
# ---------------------------------------------------------------------------


def test_tables_carry_source_id_and_domain_for_stage2():
    # REQ-310: virtual tables carry source_id and domain_id so Stage 2 can apply
    # RLS/masking/visibility identically to local tables.
    tables, functions, _ = map_schema(
        SHOPIFY_SCHEMA, "Shopify", "shopify-src", domain_id="ecommerce-domain"
    )

    for t in tables:
        assert t["source_id"] == "shopify-src"
        assert t["domain_id"] == "ecommerce-domain"

    for f in functions:
        assert f["source_id"] == "shopify-src"
        assert f["domain_id"] == "ecommerce-domain"


# ---------------------------------------------------------------------------
# REQ-311 — Schema refresh: updated fields reflected; custom rules preserved
# ---------------------------------------------------------------------------


def test_schema_refresh_updates_tables():
    # REQ-311: Calling map_schema again with evolved schema produces updated
    # table registrations (new field appears, old type count changes).
    v1_schema = _make_schema(
        query_fields=[
            {
                "name": "users",
                "type": _list_of(_object_ref("User")),
                "args": [],
            }
        ],
        extra_types=[
            {
                "kind": "OBJECT",
                "name": "User",
                "fields": [
                    {"name": "id", "type": _scalar("ID")},
                    {"name": "email", "type": _scalar("String")},
                ],
            }
        ],
    )

    v2_schema = _make_schema(
        query_fields=[
            {
                "name": "users",
                "type": _list_of(_object_ref("User")),
                "args": [],
            }
        ],
        extra_types=[
            {
                "kind": "OBJECT",
                "name": "User",
                "fields": [
                    {"name": "id", "type": _scalar("ID")},
                    {"name": "email", "type": _scalar("String")},
                    {"name": "phone", "type": _scalar("String")},  # new field
                ],
            }
        ],
    )

    tables_v1, _, _ = map_schema(v1_schema, "myns", "src1")
    tables_v2, _, _ = map_schema(v2_schema, "myns", "src1")

    cols_v1 = {c["name"] for c in tables_v1[0]["columns"]}
    cols_v2 = {c["name"] for c in tables_v2[0]["columns"]}

    assert "phone" not in cols_v1
    assert "phone" in cols_v2
    assert cols_v1.issubset(cols_v2)  # existing cols preserved


def test_schema_refresh_reflects_removed_fields():
    # REQ-311: fields removed from the upstream schema are absent after refresh.
    v1_schema = _make_schema(
        query_fields=[
            {
                "name": "items",
                "type": _list_of(_object_ref("Item")),
                "args": [],
            }
        ],
        extra_types=[
            {
                "kind": "OBJECT",
                "name": "Item",
                "fields": [
                    {"name": "id", "type": _scalar("ID")},
                    {"name": "legacyCode", "type": _scalar("String")},
                ],
            }
        ],
    )

    v2_schema = _make_schema(
        query_fields=[
            {
                "name": "items",
                "type": _list_of(_object_ref("Item")),
                "args": [],
            }
        ],
        extra_types=[
            {
                "kind": "OBJECT",
                "name": "Item",
                "fields": [
                    {"name": "id", "type": _scalar("ID")},
                    # legacyCode removed
                ],
            }
        ],
    )

    tables_v1, _, _ = map_schema(v1_schema, "myns", "src1")
    tables_v2, _, _ = map_schema(v2_schema, "myns", "src1")

    cols_v1 = {c["name"] for c in tables_v1[0]["columns"]}
    cols_v2 = {c["name"] for c in tables_v2[0]["columns"]}

    assert "legacyCode" in cols_v1
    assert "legacyCode" not in cols_v2


# ---------------------------------------------------------------------------
# REQ-312 — Namespace prefixing
# ---------------------------------------------------------------------------


def test_namespace_prefix_applied_to_all_table_names():
    # REQ-312: namespace prefix applied to every generated table and function name.
    tables, functions, _ = map_schema(SHOPIFY_SCHEMA, "Shopify", "shopify-src")

    for t in tables:
        assert t["name"].startswith("Shopify__"), f"Table {t['name']} missing prefix"

    for f in functions:
        assert f["name"].startswith("Shopify__"), f"Function {f['name']} missing prefix"


def test_namespace_prefix_double_underscore_separator():
    # REQ-312: separator is __ (double underscore), not single.
    tables, _, _ = map_schema(SHOPIFY_SCHEMA, "Shopify", "shopify-src")
    for t in tables:
        assert "__" in t["name"]
        prefix, _, field = t["name"].partition("__")
        assert prefix == "Shopify"
        assert field  # field name must not be empty


def test_different_namespaces_produce_distinct_names():
    # REQ-312: two sources with same field names but different namespaces produce
    # different table names — conflict resolved by prefix.
    base_schema = _make_schema(
        query_fields=[
            {
                "name": "orders",
                "type": _list_of(_object_ref("OrderType")),
                "args": [],
            }
        ],
        extra_types=[
            {
                "kind": "OBJECT",
                "name": "OrderType",
                "fields": [{"name": "id", "type": _scalar("ID")}],
            }
        ],
    )

    tables_a, _, _ = map_schema(base_schema, "Shopify", "src-a")
    tables_b, _, _ = map_schema(base_schema, "Stripe", "src-b")

    names_a = {t["name"] for t in tables_a}
    names_b = {t["name"] for t in tables_b}

    assert names_a.isdisjoint(names_b), "Namespace conflict: same name in both sources"
    assert "Shopify__orders" in names_a
    assert "Stripe__orders" in names_b


def test_empty_namespace_no_prefix():
    # REQ-312: empty namespace → no prefix, field name used as-is.
    base_schema = _make_schema(
        query_fields=[
            {
                "name": "products",
                "type": _object_ref("Product"),
                "args": [],
            }
        ],
        extra_types=[
            {
                "kind": "OBJECT",
                "name": "Product",
                "fields": [{"name": "id", "type": _scalar("ID")}],
            }
        ],
    )

    tables, _, _ = map_schema(base_schema, "", "src-bare")
    assert tables[0]["name"] == "products"


# ---------------------------------------------------------------------------
# REQ-313 — Relationships between remote virtual tables
# ---------------------------------------------------------------------------


def test_intra_source_relationship_detected():
    # REQ-313: Relationships between remote virtual tables detected automatically.
    # Order.product references the Product query type → relationship emitted.
    tables, _, relationships = map_schema(SHOPIFY_SCHEMA, "Shopify", "shopify-src")

    assert len(relationships) > 0
    rel_pairs = {(r["source_table_id"], r["target_table_id"]) for r in relationships}
    # orders → products relationship should be detected
    assert ("Shopify__orders", "Shopify__products") in rel_pairs


def test_auto_detected_relationships_carry_remote_managed_flag():
    # REQ-313 + REQ-598: auto-detected relationships have remote_managed=True so
    # schema refresh knows which ones to update vs preserve.
    _, _, relationships = map_schema(SHOPIFY_SCHEMA, "Shopify", "shopify-src")

    for rel in relationships:
        assert rel.get("remote_managed") is True, (
            f"Relationship {rel['id']} missing remote_managed=True"
        )


def test_relationship_cardinality_many_to_one_for_object_field():
    # REQ-313: OBJECT (non-list) field on a type that is also queryable → many-to-one.
    _, _, relationships = map_schema(SHOPIFY_SCHEMA, "Shopify", "shopify-src")
    orders_to_products = [
        r
        for r in relationships
        if r["source_table_id"] == "Shopify__orders" and r["target_table_id"] == "Shopify__products"
    ]
    assert len(orders_to_products) == 1
    assert orders_to_products[0]["cardinality"] == "many-to-one"


def test_manual_relationship_preserved_across_refresh():
    # REQ-311 + REQ-313: manually declared relationship (no remote_managed flag)
    # is structurally distinct from auto-detected ones.  After a refresh
    # (re-running map_schema), auto-detected rels carry remote_managed=True while
    # a simulated manual rel without the flag is preserved unchanged.
    _, _, auto_rels = map_schema(SHOPIFY_SCHEMA, "Shopify", "shopify-src")

    # Simulate a manually-declared relationship (steward-created, no remote_managed)
    manual_rel = {
        "id": "manual__shopify-src__Shopify__orders__local_customers",
        "source_table_id": "Shopify__orders",
        "target_table_id": "local__customers",
        "source_column": "customerId",
        "target_column": "id",
        "cardinality": "many-to-one",
        # deliberately no remote_managed key
    }

    all_rels_after_refresh = list(auto_rels) + [manual_rel]

    auto_ids = {r["id"] for r in auto_rels}
    preserved = [r for r in all_rels_after_refresh if r["id"] not in auto_ids]

    assert len(preserved) == 1
    assert preserved[0]["id"] == manual_rel["id"]
    assert "remote_managed" not in preserved[0]


def test_no_self_relationship_emitted():
    # REQ-313: a table must not emit a relationship to itself.
    _, _, relationships = map_schema(SHOPIFY_SCHEMA, "Shopify", "shopify-src")

    for rel in relationships:
        assert rel["source_table_id"] != rel["target_table_id"], (
            f"Self-relationship detected: {rel['id']}"
        )


def test_relationship_id_is_unique_per_source():
    # REQ-313: each relationship has a unique id scoped to the source.
    _, _, relationships = map_schema(SHOPIFY_SCHEMA, "Shopify", "shopify-src")

    ids = [r["id"] for r in relationships]
    assert len(ids) == len(set(ids)), "Duplicate relationship ids emitted"
