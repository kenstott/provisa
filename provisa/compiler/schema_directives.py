# Copyright (c) 2026 Kenneth Stott
# Canary: fe7dee37-1a51-4599-a719-d5e9249736c4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""GraphQL directive, enum, and connection-type definitions for the generated schema.

Leaf module: static GraphQLDirective/enum/scalar/connection definitions used by
schema_gen and schema_inputs. Also re-binds the graphql-core scalar singletons
with explicit GraphQLScalarType annotations so Pyright narrows correctly.
"""

from typing import cast

from graphql import (
    GraphQLArgument,
    GraphQLBoolean as _GraphQLBoolean,
    GraphQLDirective,
    GraphQLEnumType,
    GraphQLEnumValue,
    GraphQLField,
    GraphQLFloat as _GraphQLFloat,
    GraphQLInt as _GraphQLInt,
    GraphQLList,
    GraphQLNonNull,
    GraphQLObjectType,
    GraphQLScalarType,
    GraphQLString as _GraphQLString,
)
from graphql.language import DirectiveLocation

# graphql-core 3.2.x: __new__ returns GraphQLNamedType instead of Self;
# re-bind scalars with explicit GraphQLScalarType annotation so Pyright narrows correctly.
GraphQLString: GraphQLScalarType = cast(GraphQLScalarType, _GraphQLString)
GraphQLInt: GraphQLScalarType = cast(GraphQLScalarType, _GraphQLInt)
GraphQLBoolean: GraphQLScalarType = cast(GraphQLScalarType, _GraphQLBoolean)
GraphQLFloat: GraphQLScalarType = cast(GraphQLScalarType, _GraphQLFloat)


# --- GraphQL enum for ORDER BY direction ---

OrderDirection = cast(
    GraphQLEnumType,
    GraphQLEnumType(
        "order_by",
        {
            "asc": GraphQLEnumValue("asc"),
            "desc": GraphQLEnumValue("desc"),
            "asc_nulls_first": GraphQLEnumValue("asc_nulls_first"),
            "asc_nulls_last": GraphQLEnumValue("asc_nulls_last"),
            "desc_nulls_first": GraphQLEnumValue("desc_nulls_first"),
            "desc_nulls_last": GraphQLEnumValue("desc_nulls_last"),
        },
    ),
)

# --- Provisa directive enums ---

RouteEngineEnum = cast(
    GraphQLEnumType,
    GraphQLEnumType(
        "RouteEngine",
        {
            "FEDERATED": GraphQLEnumValue("FEDERATED", description="Route vithe engine federation"),
            "DIRECT": GraphQLEnumValue("DIRECT", description="Route directly to source"),
        },
        description="Execution engine routing hint for @route.",
    ),
)

JoinStrategyEnum = cast(
    GraphQLEnumType,
    GraphQLEnumType(
        "JoinStrategy",
        {
            "BROADCAST": GraphQLEnumValue("BROADCAST", description="Broadcast join distribution"),
            "PARTITIONED": GraphQLEnumValue(
                "PARTITIONED", description="Partitioned (hash) join distribution"
            ),
        },
        description="Join distribution strategy hint for @join.",
    ),
)

# Directive locations
_QS = [DirectiveLocation.QUERY, DirectiveLocation.SUBSCRIPTION]
_QMS = [DirectiveLocation.QUERY, DirectiveLocation.MUTATION, DirectiveLocation.SUBSCRIPTION]

PROVISA_DIRECTIVES = [
    GraphQLDirective(
        name="route",
        locations=_QMS,
        args={
            "engine": GraphQLArgument(
                GraphQLNonNull(RouteEngineEnum), description="FEDERATED or DIRECT"
            )
        },
        description="Override execution engine routing.",
    ),
    GraphQLDirective(
        name="join",
        locations=_QS,
        args={
            "strategy": GraphQLArgument(
                GraphQLNonNull(JoinStrategyEnum), description="BROADCAST or PARTITIONED"
            )
        },
        description="Set the engine join distribution strategy.",
    ),
    GraphQLDirective(
        name="reorder",
        locations=_QS,
        args={
            "enabled": GraphQLArgument(
                GraphQLNonNull(GraphQLBoolean), description="Set false to disable join reordering"
            )
        },
        description="Control the engine join reordering.",
    ),
    GraphQLDirective(
        name="broadcastSize",
        locations=_QS,
        args={
            "size": GraphQLArgument(
                GraphQLNonNull(GraphQLString), description="Max broadcast table size, e.g. '100MB'"
            )
        },
        description="Override max broadcast table size for the engine.",
    ),
    GraphQLDirective(
        name="watermark",
        locations=[DirectiveLocation.FIELD],
        args={},
        description="Mark field as the watermark column for subscription polling.",
    ),
    GraphQLDirective(
        name="sink",
        locations=[DirectiveLocation.SUBSCRIPTION],
        args={
            "topic": GraphQLArgument(GraphQLNonNull(GraphQLString), description="Kafka topic name"),
            "broker": GraphQLArgument(
                GraphQLString, description="Kafka bootstrap server (host:port)"
            ),
        },
        description="Redirect subscription output to a Kafka topic.",
    ),
    GraphQLDirective(
        name="redirect",
        locations=_QS,
        args={
            "format": GraphQLArgument(
                GraphQLString, description="Output format: parquet, csv, arrow"
            ),
            "threshold": GraphQLArgument(
                GraphQLInt, description="Row count threshold to trigger redirect"
            ),
        },
        description="Redirect large results to object store.",
    ),
    GraphQLDirective(
        name="cached",
        locations=_QMS,
        args={
            "ttl": GraphQLArgument(
                GraphQLInt, description="Cache TTL in seconds; 0 = disable caching"
            )
        },
        description="Override response cache TTL for this query.",
    ),
    GraphQLDirective(
        name="noCache",
        locations=_QMS,
        args={},
        description="Bypass the response cache entirely — skip both read and write.",
    ),
]

# --- Relay-style connection types for cursor pagination (REQ-218) ---

PageInfoType = cast(
    GraphQLObjectType,
    GraphQLObjectType(
        "PageInfo",
        lambda: {
            "hasNextPage": GraphQLField(GraphQLNonNull(GraphQLBoolean)),
            "hasPreviousPage": GraphQLField(GraphQLNonNull(GraphQLBoolean)),
            "startCursor": GraphQLField(GraphQLString),
            "endCursor": GraphQLField(GraphQLString),
        },
    ),
)


def _build_connection_types(
    type_name: str,
    node_type: GraphQLObjectType,
) -> tuple[GraphQLObjectType, GraphQLObjectType]:
    """Build Edge and Connection types for cursor pagination."""
    edge_type = cast(
        GraphQLObjectType,
        GraphQLObjectType(
            f"{type_name}Edge",
            lambda node_type=node_type: {
                "cursor": GraphQLField(GraphQLNonNull(GraphQLString)),
                "node": GraphQLField(GraphQLNonNull(node_type)),
            },
        ),
    )
    connection_type = cast(
        GraphQLObjectType,
        GraphQLObjectType(
            f"{type_name}Connection",
            lambda edge_type=edge_type: {
                "edges": GraphQLField(GraphQLNonNull(GraphQLList(GraphQLNonNull(edge_type)))),
                "pageInfo": GraphQLField(GraphQLNonNull(PageInfoType)),
            },
        ),
    )
    return edge_type, connection_type
