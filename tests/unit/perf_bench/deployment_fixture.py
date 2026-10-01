# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""What the perf stack's deployment answers to the name lookup, in the shapes the product serves
(``lookup.py`` documents each endpoint). Built from one description per table so a test can change a
fact (a transport does not expose a table) and see the resolver react.

FIXTURE: recorded shapes, not a live server. The real answers are proven by the local integration
run."""

from __future__ import annotations

import copy
import sys
from pathlib import Path
from typing import Any

BENCH = Path(__file__).resolve().parents[3] / "demo" / "named" / "perf" / "bench"
sys.path.insert(0, str(BENCH))

import lookup  # noqa: E402

DOMAIN = "perf-bench"
INT, STR = "Int", "String"


def _col(
    name: str,
    gql: str,
    cypher: str | None = None,
    grpc: str | None = None,
    rest: str | None = None,
    jsonapi: str | None = None,
) -> dict[str, Any]:
    return {
        "name": name,
        "gql": gql,
        "cypher": cypher or gql,
        "grpc": grpc or name,
        "rest": rest or gql,
        "jsonapi": jsonapi or name,
    }


# The JSON:API fields/filter spelling is the physical column name: queries.py's optimistic request
# (verified against a server) sends fields[orders]=order_id. REST spells the GraphQL names.
# one description per registered table
TABLES: list[dict[str, Any]] = [
    {
        "id": 1,
        "source": "bench-postgresql",
        "schema": "public",
        "table": "orders",
        "alias": None,
        "sql": "perf_bench.orders",
        "root": "pb__orders",
        "item": "PbOrders",
        "label": "PerfBench:Orders",
        "table_label": "Orders",
        "grpc": "PbOrders",
        "grpc_field": "pb_orders",
        "columns": [
            _col("order_id", "orderId"),
            _col("customer_id", "customerId"),
            _col("region", "region"),
            _col("status", "status"),
            _col("amount", "amount"),
        ],
        "prefer_materialized": None,
        "cache_ttl": None,
    },
    {
        "id": 2,
        "source": "bench-postgresql",
        "schema": "public",
        "table": "order_items",
        "alias": None,
        "sql": "perf_bench.order_items",
        "root": "pb__orderItems",
        "item": "PbOrderItems",
        "label": "PerfBench:OrderItems",
        "table_label": "OrderItems",
        "grpc": "PbOrderItems",
        "grpc_field": "pb_order_items",
        "columns": [
            _col("item_id", "itemId"),
            _col("order_id", "orderId"),
            _col("sku", "sku"),
            _col("quantity", "quantity"),
        ],
        "prefer_materialized": None,
        "cache_ttl": None,
    },
    {
        "id": 3,
        "source": "bench-clickhouse",
        "schema": "default",
        "table": "order_events",
        "alias": None,
        "sql": "perf_bench.order_events",
        "root": "pb__orderEvents",
        "item": "PbOrderEvents",
        "label": "PerfBench:OrderEvents",
        "table_label": "OrderEvents",
        "grpc": "PbOrderEvents",
        "grpc_field": "pb_order_events",
        "columns": [
            _col("event_id", "eventId"),
            _col("order_id", "orderId"),
            _col("customer_id", "customerId"),
            _col("event_type", "eventType"),
            _col("channel", "channel"),
            _col("device", "device"),
        ],
        "prefer_materialized": True,
        "cache_ttl": 300,
    },
    {
        "id": 4,
        "source": "bench-mongodb",
        "schema": "provisa_bench",
        "table": "order_docs",
        "alias": None,
        "sql": "perf_bench.order_docs",
        "root": "pb__orderDocs",
        "item": "PbOrderDocs",
        "label": "PerfBench:OrderDocs",
        "table_label": "OrderDocs",
        "grpc": "PbOrderDocs",
        "grpc_field": "pb_order_docs",
        "columns": [
            _col("order_id", "orderId"),
            _col("customer_id", "customerId"),
            _col("status", "status"),
            _col("shipping_carrier", "shippingCarrier"),
        ],
        "prefer_materialized": True,
        "cache_ttl": 300,
    },
    {
        "id": 5,
        "source": "bench-neo4j",
        "schema": "neo4j",
        "table": "bench_order_node",
        "alias": "Order",
        "sql": "perf_bench.bench_order_node",
        "root": "pb__order",
        "item": "PbOrder",
        "label": "PerfBench:Order",
        "table_label": "Order",
        "grpc": "PbOrder",
        "grpc_field": "pb_order",
        "columns": [
            _col("order_id", "orderId"),
            _col("customer_id", "customerId"),
            _col("region", "region"),
            _col("status", "status"),
        ],
        "prefer_materialized": True,
        "cache_ttl": 300,
    },
]

# (source table id, target table id, source column, target column, cardinality, cypher type, nested field)
RELATIONSHIPS = [
    (1, 2, "order_id", "order_id", "one-to-many", "HAS_ITEM", "orderItems"),
    (1, 3, "order_id", "order_id", "one-to-many", "HAS_EVENT", "orderEvents"),
    (1, 4, "order_id", "order_id", "one-to-one", "HAS_DOC", "orderDoc"),
]

SOURCES = [
    {
        "id": "bench-postgresql",
        "type": "postgresql",
        "preferMaterialized": False,
        "cacheEnabled": True,
        "cacheTtl": None,
    },
    {
        "id": "bench-clickhouse",
        "type": "clickhouse",
        "preferMaterialized": True,
        "cacheEnabled": True,
        "cacheTtl": 300,
    },
    {
        "id": "bench-mongodb",
        "type": "mongodb",
        "preferMaterialized": True,
        "cacheEnabled": True,
        "cacheTtl": 300,
    },
    {
        "id": "bench-neo4j",
        "type": "neo4j",
        "preferMaterialized": True,
        "cacheEnabled": True,
        "cacheTtl": 300,
    },
]


def _tref(name: str, kind: str = "OBJECT") -> dict[str, Any]:
    return {"kind": kind, "name": name, "ofType": None}


def _list_of(name: str) -> dict[str, Any]:
    return {
        "kind": "NON_NULL",
        "name": None,
        "ofType": {"kind": "LIST", "name": None, "ofType": _tref(name)},
    }


def build(
    *,
    drop: dict[str, set[str]] | None = None,
    tables: list[dict[str, Any]] | None = None,
    relationships: list[tuple] | None = None,
) -> lookup.RawLookup:
    """The deployment's raw lookup answers. ``drop`` maps a table name to the languages that do not
    expose it (graphql, cypher, grpc, rest, jsonapi)."""
    drop = drop or {}
    tables = copy.deepcopy(tables if tables is not None else TABLES)
    rels = RELATIONSHIPS if relationships is None else relationships
    by_id = {t["id"]: t for t in tables}

    admin_tables = []
    for t in tables:
        admin_tables.append(
            {
                "id": t["id"],
                "sourceId": t["source"],
                "domainId": DOMAIN,
                "schemaName": t["schema"],
                "tableName": t["table"],
                "alias": t["alias"],
                "cacheTtl": t["cache_ttl"],
                "preferMaterialized": t["prefer_materialized"],
                "graphqlFieldName": None if "graphql" in drop.get(t["table"], ()) else t["root"],
                "dqDataset": f"pgwire/{t['sql'].split('.')[0]}/{t['sql'].split('.')[1]}",
                "columns": [
                    {
                        "columnName": c["name"],
                        "computedSqlAlias": c["name"],
                        "computedGqlAlias": c["gql"],
                    }
                    for c in t["columns"]
                ],
            }
        )
    admin_rels = [
        {
            "id": f"rel{i}",
            "sourceTableId": s,
            "targetTableId": tg,
            "sourceColumn": sc,
            "targetColumn": tc,
            "cardinality": card,
            "graphqlAlias": None,
            "computedCypherAlias": cy,
            "disableCypher": False,
        }
        for i, (s, tg, sc, tc, card, cy, _nested) in enumerate(rels)
    ]

    gql_types: list[dict[str, Any]] = []
    root_fields = []
    for t in tables:
        if "graphql" in drop.get(t["table"], ()):
            continue
        root_fields.append({"name": t["root"], "type": _list_of(t["item"])})
        fields = [{"name": c["gql"], "type": _tref(INT, "SCALAR")} for c in t["columns"]]
        for s, tg, _sc, _tc, _card, _cy, nested in rels:
            if s == t["id"] and "graphql" not in drop.get(by_id[tg]["table"], ()):
                fields.append({"name": nested, "type": _list_of(by_id[tg]["item"])})
        gql_types.append({"kind": "OBJECT", "name": t["item"], "fields": fields})
    gql_types.append({"kind": "OBJECT", "name": "Query", "fields": root_fields})
    introspection = {"data": {"__schema": {"queryType": {"name": "Query"}, "types": gql_types}}}

    nodes, rel_types = [], []
    for t in tables:
        if "cypher" in drop.get(t["table"], ()):
            continue
        nodes.append(
            {
                "label": t["label"],
                "domain_id": DOMAIN,
                "domain_label": "PerfBench",
                "table_label": t["table_label"],
                "properties": [c["cypher"] for c in t["columns"]],
            }
        )
    for s, tg, _sc, _tc, _card, cy, _nested in rels:
        if "cypher" not in drop.get(by_id[s]["table"], ()) and "cypher" not in drop.get(
            by_id[tg]["table"], ()
        ):
            rel_types.append(
                {"type": cy, "source": by_id[s]["label"], "target": by_id[tg]["label"]}
            )
    graph = {"node_labels": nodes, "relationship_types": rel_types}

    def rest_spec(kind: str) -> dict[str, Any]:
        paths: dict[str, Any] = {}
        comps: dict[str, Any] = {}
        for t in tables:
            if kind in drop.get(t["table"], ()):
                continue
            name = t["alias"] or t["table"]
            if kind == "rest":
                comps[f"{t['item']}Field"] = {
                    "type": "string",
                    "enum": [c["rest"] for c in t["columns"]],
                }
                params = [
                    {"name": "limit", "in": "query", "schema": {"type": "integer"}},
                    {
                        "name": "fields",
                        "in": "query",
                        "schema": {
                            "type": "array",
                            "items": {"$ref": f"#/components/schemas/{t['item']}Field"},
                        },
                    },
                ]
            else:
                params = [{"name": f"fields[{name}]", "in": "query", "schema": {"type": "string"}}]
                for c in t["columns"]:
                    # the served spec spells attributes by their GraphQL names; the server takes the
                    # physical name (local run: "Unknown field 'orderId'")
                    params += [
                        {"name": f"filter[{c['gql']}]", "in": "query", "schema": {}},
                        {"name": f"filter[{c['gql']}][gt]", "in": "query", "schema": {}},
                    ]
            paths[f"/{DOMAIN}/{name}"] = {"get": {"parameters": params}}
        return {
            "servers": [{"url": "/data/rest" if kind == "rest" else "/data/jsonapi"}],
            "paths": paths,
            "components": {"schemas": comps},
        }

    proto = ['syntax = "proto3";', "message Query {"]
    for i, t in enumerate(tables, 1):
        if "grpc" not in drop.get(t["table"], ()):
            proto.append(f"  repeated {t['grpc']} {t['grpc_field']} = {i};")
    proto.append("}")
    for t in tables:
        if "grpc" in drop.get(t["table"], ()):
            continue
        proto.append(f"message {t['grpc']} {{")
        proto += [f"  int32 {c['grpc']} = {i};" for i, c in enumerate(t["columns"], 1)]
        proto.append("}")
        proto.append(f"message {t['grpc']}Filter {{")
        proto += [f"  optional int32 {c['grpc']} = {i};" for i, c in enumerate(t["columns"], 1)]
        proto.append("}")

    return lookup.RawLookup(
        admin={"data": {"sources": SOURCES, "tables": admin_tables, "relationships": admin_rels}},
        introspection=introspection,
        graph_schema=graph,
        rest_spec=rest_spec("rest"),
        jsonapi_spec=rest_spec("jsonapi"),
        proto="\n".join(proto) + "\n",
    )
