# Copyright (c) 2026 Kenneth Stott
# Canary: 1c6b3f80-9d24-4a7e-8f05-6e2a9c4d7b31
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-871: protocol-specific mutation-to-table association suggesters."""

from __future__ import annotations

from provisa.security.association_suggester import (
    suggest_graphql,
    suggest_grpc,
    suggest_openapi,
)

_TABLES = ["users", "orders", "order_items", "products"]


# ---- GraphQL ---------------------------------------------------------------


def test_graphql_single_object_return_type_is_top():
    out = suggest_graphql(
        return_leaf_types=["User"],
        list_valued_types=set(),
        type_to_table={"User": "users"},
    )
    assert out[0].table == "users"
    assert out[0].score == 1.0


def test_graphql_single_object_outranks_list_of_type():
    out = suggest_graphql(
        return_leaf_types=["User", "Order"],
        list_valued_types={"Order"},
        type_to_table={"User": "users", "Order": "orders"},
    )
    assert [c.table for c in out] == ["users", "orders"]
    assert out[0].score > out[1].score  # object > list-of-type


def test_graphql_ignores_scalar_stats_fields():
    # Caller passes only object leaf types; a return with only scalars → no match, fallback used.
    out = suggest_graphql(
        return_leaf_types=[],  # e.g. { affectedRows: Int }
        list_valued_types=set(),
        type_to_table={},
        op_name="createUser",
        table_names=_TABLES,
    )
    assert [c.table for c in out] == ["users"]  # via name-affix fallback
    assert out[0].score == 0.3


def test_graphql_falls_back_to_input_type_stem():
    out = suggest_graphql(
        return_leaf_types=[],
        list_valued_types=set(),
        type_to_table={},
        op_name="mutateThing",
        input_type_stem="ProductInput",
        table_names=_TABLES,
    )
    assert out[0].table == "products"


# ---- OpenAPI ---------------------------------------------------------------


def test_openapi_path_resource_is_primary():
    out = suggest_openapi(
        path="/users/{id}",
        operation_id="replaceUser",
        table_names=_TABLES,
    )
    assert out[0].table == "users"
    assert out[0].score >= 0.9


def test_openapi_operation_id_stem_when_path_generic():
    out = suggest_openapi(
        path="/v1/{tenant}/resource",
        operation_id="createOrder",
        table_names=_TABLES,
    )
    assert out[0].table == "orders"


def test_openapi_tag_signal():
    out = suggest_openapi(
        path="/do/{x}",
        operation_id="perform",
        tags=["Products"],
        table_names=_TABLES,
    )
    assert out[0].table == "products"


def test_openapi_response_schema_is_only_a_tiebreaker():
    # Path already points at orders; a response type of order_items must not outrank it.
    out = suggest_openapi(
        path="/orders",
        operation_id="postOrders",
        response_leaf_types=["OrderItem"],
        table_names=_TABLES,
    )
    assert out[0].table == "orders"
    assert out[0].score >= 0.9


def test_openapi_pluralization_and_casing_normalized():
    out = suggest_openapi(path="/Order-Items", operation_id="x", table_names=_TABLES)
    assert out[0].table == "order_items"


# ---- gRPC ------------------------------------------------------------------


def test_grpc_repeated_field_type_is_primary():
    out = suggest_grpc(
        response_repeated_types=["Product"],
        method_name="DoStuff",
        table_names=_TABLES,
    )
    assert out[0].table == "products"
    assert out[0].score == 0.6


def test_grpc_method_stem_weakest():
    out = suggest_grpc(
        response_repeated_types=[],
        method_name="CreateUser",
        table_names=_TABLES,
    )
    assert out[0].table == "users"
    assert out[0].score == 0.4


# ---- Ranking / dedup / false-negatives -------------------------------------


def test_multiple_signals_same_table_merge_and_boost():
    out = suggest_openapi(
        path="/products",
        operation_id="createProduct",  # stem also → products
        tags=["Products"],
        table_names=_TABLES,
    )
    products = [c for c in out if c.table == "products"]
    assert len(products) == 1  # deduped
    assert products[0].score > 0.9  # strongest signal + tiebreak boosts
    assert "path resource" in products[0].reason


def test_no_confident_suggestion_returns_empty():
    out = suggest_openapi(
        path="/telemetry/{id}",
        operation_id="ingestBlob",
        table_names=_TABLES,
    )
    assert out == []  # false negative expected, not an error


def test_candidates_sorted_strongest_first():
    out = suggest_graphql(
        return_leaf_types=["Order", "User"],
        list_valued_types={"Order"},  # order = list (0.8), user = object (1.0)
        type_to_table={"Order": "orders", "User": "users"},
    )
    scores = [c.score for c in out]
    assert scores == sorted(scores, reverse=True)
    assert out[0].table == "users"
