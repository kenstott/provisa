# Copyright (c) 2026 Kenneth Stott
# Canary: 3f8a2c6d-9b4e-4a7f-8c1d-5e3b7a9c2f4e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""GraphQL type definitions for Node, Edge, Path, CypherResult.

Registered into the schema at startup alongside the existing schema types.
"""

from graphql import (
    GraphQLField,
    GraphQLID,
    GraphQLList,
    GraphQLNonNull,
    GraphQLObjectType,
    GraphQLString,
)

# JSON scalar — reuse if already defined, else create a basic scalar
try:
    from provisa.compiler.schema_gen import JSONScalar  # type: ignore[import]
except ImportError:
    from graphql import GraphQLScalarType
    JSONScalar = GraphQLScalarType(name="JSON", description="Arbitrary JSON value")


NodeType = GraphQLObjectType(
    name="Node",
    description="A graph node returned by a Cypher query.",
    fields=lambda: {
        "id": GraphQLField(GraphQLNonNull(GraphQLID), description="Node identifier"),
        "label": GraphQLField(GraphQLNonNull(GraphQLString), description="Node label (type name)"),
        "properties": GraphQLField(JSONScalar, description="Node property map"),
    },
)

EdgeType = GraphQLObjectType(
    name="Edge",
    description="A graph edge returned by a Cypher query.",
    fields=lambda: {
        "id": GraphQLField(GraphQLNonNull(GraphQLID), description="Edge identifier"),
        "type": GraphQLField(GraphQLNonNull(GraphQLString), description="Relationship type"),
        "startNode": GraphQLField(GraphQLNonNull(NodeType), description="Edge start node"),
        "endNode": GraphQLField(GraphQLNonNull(NodeType), description="Edge end node"),
        "properties": GraphQLField(JSONScalar, description="Edge property map"),
    },
)

PathType = GraphQLObjectType(
    name="Path",
    description="A graph path (sequence of nodes and edges) returned by a Cypher query.",
    fields=lambda: {
        "nodes": GraphQLField(
            GraphQLNonNull(GraphQLList(GraphQLNonNull(NodeType))),
            description="Ordered node sequence",
        ),
        "edges": GraphQLField(
            GraphQLNonNull(GraphQLList(GraphQLNonNull(EdgeType))),
            description="Ordered edge sequence",
        ),
    },
)

CypherResultType = GraphQLObjectType(
    name="CypherResult",
    description="Result set from a Cypher query execution.",
    fields=lambda: {
        "columns": GraphQLField(
            GraphQLNonNull(GraphQLList(GraphQLNonNull(GraphQLString))),
            description="Column names in order",
        ),
        "rows": GraphQLField(
            GraphQLNonNull(GraphQLList(GraphQLNonNull(JSONScalar))),
            description="Row values — each row maps column name to scalar, Node, Edge, or Path",
        ),
    },
)

GRAPH_TYPES = [NodeType, EdgeType, PathType, CypherResultType]
