# Copyright (c) 2026 Kenneth Stott
# Canary: 7b3e9d15-4c28-4a6f-91e0-c5d2a8f6b437
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2128: /data/graph-schema names each table's root query field in the acting role's GraphQL
schema, so a client (the data product graph reads the meta domain) queries a table by the field
the compiler gave it instead of re-deriving the name. A domain-prefixed model gets the prefixed
field; the meta domain's views are listed with theirs."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from provisa.compiler.introspect import ColumnMetadata
from provisa.core import domain_policy

ORDERS, PRODUCTS = 1, 2


def _table(tid: int, domain_id: str, name: str) -> dict:
    return {
        "id": tid,
        "domain_id": domain_id,
        "source_id": "pg",
        "schema_name": "public",
        "table_name": name,
        "columns": [{"column_name": "id", "visible_to": [], "native_filter_type": None}],
        "write_ops": [],
        "write_returns_rows": True,
        "write_refused_forms": [],
    }


@pytest.fixture
def state(monkeypatch):
    import provisa.api.app as appmod
    from provisa.api.app_loaders import _build_and_register_schemas

    monkeypatch.setattr(domain_policy, "single_domain", lambda: False)
    state = appmod.state
    for name in ("roles", "schemas", "contexts", "rls_contexts", "table_path_maps", "proto_files"):
        monkeypatch.setattr(state, name, {}, raising=False)
    monkeypatch.setattr(state, "graphql_remote_sources", {}, raising=False)
    monkeypatch.setattr(state, "source_types", {"pg": "postgresql"}, raising=False)
    monkeypatch.setattr(state, "source_catalogs", {"pg": "pg"}, raising=False)
    _build_and_register_schemas(
        roles=[
            {
                "id": "r",
                "description": "Test role.",
                "capabilities": ["usage"],
                "domain_access": ["*"],
            }
        ],
        tables=[_table(ORDERS, "sales", "orders"), _table(PRODUCTS, "meta", "data_products")],
        relationships=[],
        col_types_converted={
            tid: [ColumnMetadata(column_name="id", data_type="integer", is_nullable=False)]
            for tid in (ORDERS, PRODUCTS)
        },
        naming_rules=[],
        domains=[{"id": "sales"}, {"id": "meta"}],
        domain_prefix=True,
        kafka_physical={},
        tracked_functions=[],
        tracked_webhooks=[],
        gql_object_cols={},
        rls_rules=[],
        metrics=[],
        draft_tables=[],
    )
    return state


def _graph_schema(role: str) -> dict:
    import json

    from provisa.api.rest.cypher_router import graph_schema

    request = SimpleNamespace(state=SimpleNamespace(role=role))
    response = asyncio.run(graph_schema(request))  # type: ignore[arg-type]
    return json.loads(bytes(response.body))


def _root_fields(role: str) -> dict[str, dict]:
    return {r["table_name"]: r for r in _graph_schema(role)["root_fields"]}


def test_each_table_names_the_root_field_the_role_queries_it_by(state):
    roots = _root_fields("r")
    field_of = {info["table_id"]: field for field, info in state.table_path_maps["r"].items()}
    # Exactly the role's own root field -- domain-prefixed as this model names them (the prefix
    # is the domain's alias, "s__orders", which is why a client must not derive it).
    assert roots["orders"]["graphql_field"] == field_of[ORDERS]
    assert roots["orders"]["graphql_field"].endswith("__orders")
    # The meta domain's view is listed with its field too.
    assert roots["data_products"]["domain_id"] == "meta"
    assert roots["data_products"]["graphql_field"] == field_of[PRODUCTS]
    assert "__" in roots["data_products"]["graphql_field"]
    # Both are node labels here, unlike a junction (next test).
    assert "Orders" in {n["table_label"] for n in _graph_schema("r")["node_labels"]}


def test_a_table_that_is_no_node_label_still_names_its_root_field(state, monkeypatch):
    # A junction a relationship traverses (glossary_term_edges under the term relationships) is
    # dropped from the node labels; its root field is still the role's to query it by.
    from provisa.api.rest import cypher_router

    built = cypher_router._build_label_map

    def without_orders(ctx, role_id, st):
        label_map = built(ctx, role_id, st)
        for label in [k for k, n in label_map.nodes.items() if n.table_id == ORDERS]:
            del label_map.nodes[label]
        return label_map

    monkeypatch.setattr(cypher_router, "_build_label_map", without_orders)
    body = _graph_schema("r")
    assert "Orders" not in {n["table_label"] for n in body["node_labels"]}
    field_of = {info["table_id"]: field for field, info in state.table_path_maps["r"].items()}
    assert _root_fields("r")["orders"]["graphql_field"] == field_of[ORDERS]


def test_the_path_map_carries_each_tables_id(state):
    by_id = {info["table_id"]: field for field, info in state.table_path_maps["r"].items()}
    assert set(by_id) == {ORDERS, PRODUCTS}
