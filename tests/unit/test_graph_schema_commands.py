# Copyright (c) 2026 Kenneth Stott
# Canary: 4a8d2f61-7e39-4c15-b0a7-6d3e9f1c2b84
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1159 (amended 2026-11-27): /data/graph-schema lists every command the role's GraphQL
schema offers, by its GraphQL root and field and its name on each surface a call names it by, so a
command's Query it line writes its call by the names the compiler gave it."""

# Requirements: REQ-1159, REQ-2168

from __future__ import annotations

from graphql import GraphQLField, GraphQLObjectType, GraphQLSchema, GraphQLString

from provisa.api.rest.cypher_router import served_commands


def _field(definition: dict) -> GraphQLField:
    return GraphQLField(
        GraphQLString,
        extensions={"provisa_command": definition["name"], "provisa_command_def": definition},
    )


def _schema() -> GraphQLSchema:
    enrich = {"name": "enrich_set", "domain_id": "derived", "language_bindings": {}}
    move = {
        "name": "move_rows",
        "domain_id": "sales",
        "language_bindings": {"sql": {"name": "move_them"}, "cypher": {"exclude": True}},
    }
    query = GraphQLObjectType(
        "Query",
        {"derived__enrichSet": _field(enrich), "plain": GraphQLField(GraphQLString)},
    )
    mutation = GraphQLObjectType("Mutation", {"sales__moveRows": _field(move)})
    return GraphQLSchema(query=query, mutation=mutation)


def test_each_command_field_is_listed_with_its_names():
    assert served_commands(_schema()) == [
        {
            "command": "enrich_set",
            "domain_id": "derived",
            "graphql_root": "Query",
            "graphql_field": "derived__enrichSet",
            "names": {"sql": "enrich_set", "cypher": "enrich_set", "rest": "enrich_set"},
        },
        {
            "command": "move_rows",
            "domain_id": "sales",
            "graphql_root": "Mutation",
            "graphql_field": "sales__moveRows",
            # Renamed in SQL, excluded from Cypher (REQ-2168).
            "names": {"sql": "move_them", "cypher": None, "rest": "move_rows"},
        },
    ]


def test_a_table_field_is_no_command():
    schema = GraphQLSchema(query=GraphQLObjectType("Query", {"plain": GraphQLField(GraphQLString)}))
    assert served_commands(schema) == []
