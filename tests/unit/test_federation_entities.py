# Copyright (c) 2026 Kenneth Stott
# Canary: 0b6f3e52-6d0e-4c49-9a83-f8a7e1d2c4b9
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-259: the subgraph's _service and _entities (provisa/api/data/federation.py).

_entities is rewritten into an ordinary query of the declared role's own schema, so it is read
through the governed pipeline as that role; these tests hold the rewrite and the answer's shape.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from graphql import (
    GraphQLArgument,
    GraphQLField,
    GraphQLInputField,
    GraphQLInputObjectType,
    GraphQLInt,
    GraphQLList,
    GraphQLNonNull,
    GraphQLObjectType,
    GraphQLSchema,
    GraphQLString,
    parse,
    print_ast,
)

from provisa.api.data import federation as fed_mod
from provisa.api.errors import ApiError
from provisa.core.models import FederationConfig


def _comparison(name: str, scalar) -> GraphQLInputObjectType:
    return GraphQLInputObjectType(
        name,
        {
            "eq": GraphQLInputField(scalar),
            "in": GraphQLInputField(GraphQLList(GraphQLNonNull(scalar))),
        },
    )


_INT = _comparison("IntComparison", GraphQLInt)
_STR = _comparison("StringComparison", GraphQLString)


def _where(name: str, columns: dict) -> GraphQLInputObjectType:
    where: GraphQLInputObjectType = GraphQLInputObjectType(
        name,
        lambda: {
            **{c: GraphQLInputField(t) for c, t in columns.items()},
            "_or": GraphQLInputField(GraphQLList(GraphQLNonNull(where))),
        },
    )
    return where


def _surface():
    orders = GraphQLObjectType(
        "Orders",
        {
            "id": GraphQLField(GraphQLInt),
            "region": GraphQLField(GraphQLString),
            "amount": GraphQLField(GraphQLInt),
        },
    )
    customers = GraphQLObjectType(
        "Customers", {"id": GraphQLField(GraphQLInt), "name": GraphQLField(GraphQLString)}
    )
    shipments = GraphQLObjectType(
        "Shipments", {"region": GraphQLField(GraphQLString), "seq": GraphQLField(GraphQLInt)}
    )
    notes = GraphQLObjectType("Notes", {"body": GraphQLField(GraphQLString)})

    def root(gql_type, where):
        return GraphQLField(
            GraphQLList(GraphQLNonNull(gql_type)),
            args={"where": GraphQLArgument(where), "limit": GraphQLArgument(GraphQLInt)},
        )

    query = GraphQLObjectType(
        "Query",
        {
            "orders": root(
                orders,
                _where("OrdersWhere", {"id": _INT, "region": _STR, "amount": _INT}),
            ),
            "customers": root(customers, _where("CustomersWhere", {"id": _INT, "name": _STR})),
            "shipments": root(shipments, _where("ShipmentsWhere", {"region": _STR, "seq": _INT})),
            "notes": root(notes, _where("NotesWhere", {"body": _STR})),
        },
    )
    meta = {
        "orders": SimpleNamespace(table_id=1, field_name="orders", type_name="Orders"),
        "customers": SimpleNamespace(table_id=2, field_name="customers", type_name="Customers"),
        "shipments": SimpleNamespace(table_id=3, field_name="shipments", type_name="Shipments"),
        "notes": SimpleNamespace(table_id=4, field_name="notes", type_name="Notes"),
    }
    ctx = SimpleNamespace(
        tables=meta,
        # Notes has a designated key the role cannot see (its column is not in the type);
        # nothing else is designated beyond these.
        pk_columns={1: ["id"], 2: ["id"], 3: ["region", "seq"], 4: ["note_id"]},
        physical_to_gql={
            (1, "id"): "id",
            (2, "id"): "id",
            (3, "region"): "region",
            (3, "seq"): "seq",
        },
    )
    return SimpleNamespace(schema=GraphQLSchema(query=query), context=ctx)


@pytest.fixture
def fed():
    return fed_mod.federated(_surface(), "org-a", "partner")


def _read(fed, query: str, variables: dict | None = None):
    return fed_mod.federation_read(parse(query), variables, fed)


ENTITIES = """
query ($representations: [_Any!]!) {
  _entities(representations: $representations) {
    __typename
    ... on Orders { amount }
    ... on Customers { name }
  }
}
"""


class TestEntityTypes:
    def test_designated_keys_the_role_sees(self, fed):
        assert {n: t.keys for n, t in fed.types.items()} == {
            "Orders": ("id",),
            "Customers": ("id",),
            "Shipments": ("region", "seq"),
        }

    def test_key_the_role_cannot_see_is_no_entity(self, fed):
        assert "Notes" not in fed.types

    def test_sdl_keys_each_entity(self, fed):
        assert 'type Orders @key(fields: "id")' in fed.sdl
        assert 'type Shipments @key(fields: "region seq")' in fed.sdl
        assert "type Notes @key" not in fed.sdl

    def test_schema_offers_service_and_entities(self, fed):
        assert {"_service", "_entities"} <= set(fed.schema.query_type.fields)


class TestFederatedCache:
    def test_same_surface_reuses(self):
        surface = _surface()
        assert fed_mod.federated(surface, "o", "r") is fed_mod.federated(surface, "o", "r")

    def test_new_surface_rebuilds(self):
        first = fed_mod.federated(_surface(), "o", "r")
        assert fed_mod.federated(_surface(), "o", "r") is not first


class TestService:
    def test_service_answers_sdl(self, fed):
        read = _read(fed, "query __ApolloGetServiceDefinition__ { _service { sdl } }")
        assert isinstance(read, fed_mod.ServiceRead)
        assert read.answer(fed.sdl) == {"data": {"_service": {"sdl": fed.sdl}}}

    def test_plain_query_is_no_federation_read(self, fed):
        assert _read(fed, "{ orders { id } }") is None

    def test_federation_field_is_asked_alone(self, fed):
        with pytest.raises(ApiError) as exc:
            _read(fed, "{ _service { sdl } orders { id } }")
        assert exc.value.code == "data.federation_field_alone"


class TestEntitiesRewrite:
    def test_one_governed_read_per_type(self, fed):
        reps = [
            {"__typename": "Orders", "id": 1},
            {"__typename": "Customers", "id": 10},
            {"__typename": "Orders", "id": 2},
            {"__typename": "Orders", "id": 1},
        ]
        read = _read(fed, ENTITIES, {"representations": reps})
        assert isinstance(read, fed_mod.EntitiesRead)
        printed = print_ast(read.document)
        assert "e0: orders(where: {id: {in: $e0_id}}, limit: 2)" in printed
        assert "e1: customers(where: {id: {in: $e1_id}}, limit: 1)" in printed
        assert "_entities" not in printed
        assert read.variables == {"e0_id": [1, 2], "e1_id": [10]}

    def test_reads_what_the_router_asked_and_the_key(self, fed):
        reps = [{"__typename": "Orders", "id": 1}]
        printed = print_ast(_read(fed, ENTITIES, {"representations": reps}).document)
        assert "amount" in printed and "id" in printed
        assert "name" not in printed  # Customers' selection: no Customers asked for

    def test_named_fragment(self, fed):
        query = """
        query ($r: [_Any!]!) { _entities(representations: $r) { ...O } }
        fragment O on Orders { region }
        """
        read = _read(fed, query, {"r": [{"__typename": "Orders", "id": 3}]})
        assert "region" in print_ast(read.document)

    def test_composite_key_is_an_or_of_its_columns(self, fed):
        reps = [
            {"__typename": "Shipments", "region": "us", "seq": 1},
            {"__typename": "Shipments", "region": "eu", "seq": 2},
        ]
        query = (
            "query ($r: [_Any!]!) { _entities(representations: $r) { ... on Shipments { seq } } }"
        )
        read = _read(fed, query, {"r": reps})
        printed = print_ast(read.document)
        assert "_or: [{region: {eq: $e0_0_region}, seq: {eq: $e0_0_seq}}" in printed
        assert read.variables["e0_1_region"] == "eu"

    def test_unknown_type_refused(self, fed):
        with pytest.raises(ApiError) as exc:
            _read(fed, ENTITIES, {"representations": [{"__typename": "Notes", "note_id": 1}]})
        assert exc.value.code == "data.entity_type_unknown"

    def test_missing_key_refused(self, fed):
        with pytest.raises(ApiError) as exc:
            _read(fed, ENTITIES, {"representations": [{"__typename": "Orders"}]})
        assert exc.value.code == "data.entity_key_missing"


class TestEntitiesAnswer:
    def test_in_order_with_typename_and_null_for_unseen_rows(self, fed):
        reps = [
            {"__typename": "Orders", "id": 1},
            {"__typename": "Customers", "id": 10},
            {"__typename": "Orders", "id": 2},
        ]
        read = _read(fed, ENTITIES, {"representations": reps})
        # The role's row rules hid order 2: the governed read has no row for it.
        body = {"data": {"e0": [{"id": 1, "amount": 5}], "e1": [{"id": 10, "name": "Ada"}]}}
        assert read.answer(body) == {
            "data": {
                "_entities": [
                    {"__typename": "Orders", "id": 1, "amount": 5},
                    {"__typename": "Customers", "id": 10, "name": "Ada"},
                    None,
                ]
            }
        }

    def test_redirected_answer_refused(self, fed):
        read = _read(fed, ENTITIES, {"representations": [{"__typename": "Orders", "id": 1}]})
        with pytest.raises(ApiError) as exc:
            read.answer({"data": {"e0": None}, "redirect": {"redirect_url": "x"}})
        assert exc.value.status_code == 413


class TestSubgraph:
    def test_org_override_replaces_the_file(self):
        state = SimpleNamespace(
            settings_overrides={"federation": {"enabled": True, "role": "partner"}},
            config=SimpleNamespace(federation=FederationConfig()),
        )
        assert fed_mod.subgraph(state).role == "partner"

    def test_file_when_no_override(self):
        cfg = FederationConfig(subgraph_name="s")
        state = SimpleNamespace(settings_overrides={}, config=SimpleNamespace(federation=cfg))
        assert fed_mod.subgraph(state) is cfg

    def test_no_config_serves_no_subgraph(self):
        state = SimpleNamespace(settings_overrides={}, config=None)
        assert fed_mod.subgraph(state).enabled is False
