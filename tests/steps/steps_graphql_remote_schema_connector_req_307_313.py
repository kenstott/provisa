# Copyright (c) 2026 Kenneth Stott
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1

"""Step definitions for REQ-602: Synthesized ColumnMetadata for remote schema tables,
REQ-313: Federation routing, REQ-308: Auto-registration of Query/Mutation fields,
REQ-309: S3/Iceberg materialization of remote GraphQL query results,
REQ-311: On-demand schema refresh with RLS/masking rule preservation,
REQ-597: field_overrides map for GraphQL remote schema connector,
REQ-598: Distinguishing manual from auto-detected relationships, and
REQ-599: Native filter columns for remote schema source types."""

from __future__ import annotations

import os
import time
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import respx
import httpx
from pytest_bdd import given, when, then, scenarios

scenarios("../../../features/req_602.feature")
scenarios("../../../features/req_313.feature")
scenarios("../../../features/req_308.feature")
scenarios("../../../features/req_309.feature")
scenarios("../../../features/req_311.feature")
scenarios("../../../features/req_597.feature")
scenarios("../../../features/req_598.feature")
scenarios("../../../features/req_599.feature")


@pytest.fixture
def shared_data() -> dict:
    return {}


# ---------------------------------------------------------------------------
# Helpers / inline stubs for Provisa relationship + federation routing
# ---------------------------------------------------------------------------

def _make_local_table(name: str, rows: list[dict]) -> dict:
    """Return a minimal local table descriptor with an in-memory row cache."""
    return {"name": name, "rows": rows}


def _make_remote_virtual_table(field_name: str, source_url: str, cached_rows: list[dict]) -> dict:
    """Return a minimal remote virtual-table descriptor."""
    return {
        "field_name": field_name,
        "source_url": source_url,
        "cached_rows": cached_rows,
    }


def _define_relationship(
    local_table: dict,
    remote_table: dict,
    local_key: str,
    remote_key: str,
) -> dict:
    """Build a standard Provisa-style relationship definition."""
    return {
        "local_table": local_table,
        "remote_table": remote_table,
        "local_key": local_key,
        "remote_key": remote_key,
    }


def _resolve_local_side(relationship: dict) -> list[dict]:
    """Simulate resolving the local side from cache/DB."""
    return relationship["local_table"]["rows"]


def _resolve_remote_side_from_cache(relationship: dict) -> list[dict]:
    """Simulate resolving the remote side from the cached remote call."""
    return relationship["remote_table"]["cached_rows"]


def _federation_join(
    local_rows: list[dict],
    remote_rows: list[dict],
    local_key: str,
    remote_key: str,
) -> list[dict]:
    """Perform a hash join following the federation routing rules."""
    remote_index: dict[Any, dict] = {}
    for row in remote_rows:
        key_val = row.get(remote_key)
        if key_val is not None:
            remote_index[key_val] = row

    joined: list[dict] = []
    for local_row in local_rows:
        key_val = local_row.get(local_key)
        if key_val in remote_index:
            merged = {**local_row, **remote_index[key_val]}
            joined.append(merged)
    return joined


# ---------------------------------------------------------------------------
# Helpers for REQ-602: remote schema type synthesis
# ---------------------------------------------------------------------------

def _make_scalar_gql_type(name: str) -> dict:
    return {"kind": "SCALAR", "name": name, "ofType": None}


def _make_object_gql_type(name: str) -> dict:
    return {"kind": "OBJECT", "name": name, "ofType": None}


def _make_list_of_object(name: str) -> dict:
    return {"kind": "LIST", "name": None, "ofType": _make_object_gql_type(name)}


def _make_non_null(inner: dict) -> dict:
    return {"kind": "NON_NULL", "name": None, "ofType": inner}


def _make_graphql_remote_source(source_id: str, namespace: str) -> dict:
    """Return a minimal GraphQL remote source registration."""
    user_type = {
        "kind": "OBJECT",
        "name": "User",
        "fields": [
            {"name": "id", "type": _make_scalar_gql_type("ID")},
            {"name": "name", "type": _make_scalar_gql_type("String")},
            {"name": "age", "type": _make_scalar_gql_type("Int")},
            {"name": "score", "type": _make_scalar_gql_type("Float")},
            {"name": "active", "type": _make_scalar_gql_type("Boolean")},
            {"name": "meta", "type": _make_object_gql_type("UserMeta")},
        ],
    }
    schema = {
        "queryType": {"name": "Query"},
        "mutationType": None,
        "types": [
            {
                "kind": "OBJECT",
                "name": "Query",
                "fields": [
                    {
                        "name": "users",
                        "type": _make_list_of_object("User"),
                        "args": [],
                    }
                ],
            },
            user_type,
        ],
    }
    return {
        "source_id": source_id,
        "namespace": namespace,
        "source_type": "graphql_remote",
        "schema": schema,
    }


def _make_grpc_remote_source(source_id: str, namespace: str) -> dict:
    """Return a minimal gRPC remote source registration with proto-like column definitions."""
    columns = [
        {"name": "id", "proto_type": "string"},
        {"name": "order_total", "proto_type": "double"},
        {"name": "quantity", "proto_type": "int32"},
        {"name": "fulfilled", "proto_type": "bool"},
        {"name": "created_at", "proto_type": "int64"},
    ]
    return {
        "source_id": source_id,
        "namespace": namespace,
        "source_type": "grpc_remote",
        "service": "OrderService",
        "rpc": "ListOrders",
        "columns": columns,
    }


def _make_openapi_source(source_id: str, namespace: str) -> dict:
    """Return a minimal OpenAPI source registration with JSON-Schema column definitions."""
    columns = [
        {"name": "product_id", "json_schema_type": "string"},
        {"name": "price", "json_schema_type": "number"},
        {"name": "stock", "json_schema_type": "integer"},
        {"name": "available", "json_schema_type": "boolean"},
        {"name": "tags", "json_schema_type": "array"},
        {"name": "attributes", "json_schema_type": "object"},
    ]
    return {
        "source_id": source_id,
        "namespace": namespace,
        "source_type": "openapi",
        "path": "/products",
        "columns": columns,
    }


# Type mapping helpers that mirror what schema_gen / mapper would produce
_GQL_SCALAR_TO_PROVISA = {
    "String": "text",
    "ID": "text",
    "Int": "integer",
    "Float": "numeric",
    "Boolean": "boolean",
}

_PROTO_SCALAR_TO_SQL = {
    "string": "text",
    "double": "numeric",
    "float": "numeric",
    "int32": "integer",
    "int64": "bigint",
    "uint32": "integer",
    "uint64": "bigint",
    "bool": "boolean",
    "bytes": "text",
}

_JSON_SCHEMA_TO_PROVISA = {
    "string": "text",
    "number": "numeric",
    "integer": "integer",
    "boolean": "boolean",
    "array": "jsonb",
    "object": "jsonb",
    "null": "text",
}


def _synthesize_column_metadata_graphql(source: dict) -> list[dict]:
    """Synthesize ColumnMetadata for a GraphQL remote source."""
    from provisa.graphql_remote.mapper import map_schema

    tables, _, _ = map_schema(source["schema"], source["namespace"], source["source_id"])
    result = []
    for table in tables:
        for col in table["columns"]:
            result.append(
                {
                    "table_name": table["name"],
                    "column_name": col["name"],
                    "provisa_type": col["type"],
                    "source_type": "graphql_remote",
                }
            )
    return result


def _synthesize_column_metadata_grpc(source: dict) -> list[dict]:
    """Synthesize ColumnMetadata for a gRPC remote source via proto scalar → SQL type mapping."""
    result = []
    table_name = f"{source['namespace']}__{source['rpc'].lower()}"
    for col in source["columns"]:
        provisa_type = _PROTO_SCALAR_TO_SQL.get(col["proto_type"], "text")
        result.append(
            {
                "table_name": table_name,
                "column_name": col["name"],
                "provisa_type": provisa_type,
                "source_type": "grpc_remote",
            }
        )
    return result


def _synthesize_column_metadata_openapi(source: dict) -> list[dict]:
    """Synthesize ColumnMetadata for an OpenAPI source via JSON Schema → Provisa type mapping."""
    result = []
    table_name = f"{source['namespace']}__{source['path'].strip('/').replace('/', '_')}"
    for col in source["columns"]:
        provisa_type = _JSON_SCHEMA_TO_PROVISA.get(col["json_schema_type"], "text")
        result.append(
            {
                "table_name": table_name,
                "column_name": col["name"],
                "provisa_type": provisa_type,
                "source_type": "openapi",
            }
        )
    return result


def _synthesize_all(sources: list[dict]) -> dict[str, list[dict]]:
    """Run synthesis for all source types, keyed by source_id."""
    result: dict[str, list[dict]] = {}
    for source in sources:
        stype = source["source_type"]
        if stype == "graphql_remote":
            result[source["source_id"]] = _synthesize_column_metadata_graphql(source)
        elif stype == "grpc_remote":
            result[source["source_id"]] = _synthesize_column_metadata_grpc(source)
        elif stype == "openapi":
            result[source["source_id"]] = _synthesize_column_metadata_openapi(source)
        else:
            result[source["source_id"]] = []
    return result


def _build_column_metadata_objects(synthesized: list[dict]):
    """Convert synthesized dicts into ColumnMetadata instances."""
    from provisa.compiler.introspect import ColumnMetadata

    return [
        ColumnMetadata(
            column_name=entry["column_name"],
            data_type=entry["provisa_type"],
            is_nullable=True,
        )
        for entry in synthesized
    ]


# ---------------------------------------------------------------------------
# Helpers for REQ-308: building a rich schema with both Query and Mutation fields
# ---------------------------------------------------------------------------

def _make_req308_schema() -> dict:
    """Build a representative GraphQL __schema dict with Query and Mutation fields."""
    product_type = {
        "kind": "OBJECT",
        "name": "Product",
        "fields": [
            {"name": "id", "type": _make_scalar_gql_type("ID")},
            {"name": "name", "type": _make_scalar_gql_type("String")},
            {"name": "price", "type": _make_scalar_gql_type("Float")},
            {"name": "inStock", "type": _make_scalar_gql_type("Boolean")},
        ],
    }
    order_type = {
        "kind": "OBJECT",
        "name": "Order",
        "fields": [
            {"name": "orderId", "type": _make_scalar_gql_type("ID")},
            {"name": "total", "type": _make_scalar_gql_type("Float")},
            {"name": "status", "type": _make_scalar_gql_type("String")},
        ],
    }
    create_order_result_type = {
        "kind": "OBJECT",
        "name": "CreateOrderResult",
        "fields": [
            {"name": "success", "type": _make_scalar_gql_type("Boolean")},
            {"name": "orderId", "type": _make_scalar_gql_type("ID")},
            {"name": "message", "type": _make_scalar_gql_type("String")},
        ],
    }
    update_product_result_type = {
        "kind": "OBJECT",
        "name": "UpdateProductResult",
        "fields": [
            {"name": "updated", "type": _make_scalar_gql_type("Boolean")},
            {"name": "productId", "type": _make_scalar_gql_type("ID")},
        ],
    }

    query_fields = [
        {
            "name": "products",
            "description": "List all products",
            "type": _make_list_of_object("Product"),
            "args": [],
        },
        {
            "name": "orders",
            "description": "List all orders",
            "type": _make_list_of_object("Order"),
            "args": [
                {"name": "limit", "type": _make_scalar_gql_type("Int")},
            ],
        },
    ]
    mutation_fields = [
        {
            "name": "createOrder",
            "description": "Create a new order",
            "type": _make_object_gql_type("CreateOrderResult"),
            "args": [
                {"name": "productId", "type": _make_scalar_gql_type("ID")},
                {"name": "quantity", "type": _make_scalar_gql_type("Int")},
            ],
        },
        {
            "name": "updateProduct",
            "description": "Update product details",
            "type": _make_object_gql_type("UpdateProductResult"),
            "args": [
                {"name": "productId", "type": _make_scalar_gql_type("ID")},
                {"name": "price", "type": _make_scalar_gql_type("Float")},
            ],
        },
    ]

    return {
        "queryType": {"name": "Query"},
        "mutationType": {"name": "Mutation"},
        "types": [
            {"kind": "OBJECT", "name": "Query", "fields": query_fields},
            {"kind": "OBJECT", "name": "Mutation", "fields": mutation_fields},
            product_type,
            order_type,
            create_order_result_type,
            update_product_result_type,
        ],
    }


# ---------------------------------------------------------------------------
# Helpers for REQ-309: Iceberg/Trino cache simulation
# ---------------------------------------------------------------------------

class _FakeTrinoConnection:
    """In-memory fake Trino connection that records executed SQL and serves SELECT queries."""

    def __init__(self):
        self._tables: dict[str, list[dict]] = {}  # fqn -> rows
        self._executed: list[str] = []
        self._remote_call_count: int = 0

    def cursor(self):
        return _FakeTrinoConnectionCursor(self)

    def record_remote_call(self):
        self._remote_call_count += 1

    @property
    def remote_call_count(self) -> int:
        return self._remote_call_count


class _FakeTrinoConnectionCursor:
    def __init__(self, conn: _FakeTrinoConnection):
        self._conn = conn
        self._result = []

    def execute(self, sql: str, params=None):
        self._conn._executed.append(sql)
        sql_upper = sql.strip().upper()

        if sql_upper.startswith("CREATE SCHEMA"):
            self._result = []
        elif sql_upper.startswith("CREATE TABLE"):
            import re
            m = re.search(
                r'CREATE TABLE IF NOT EXISTS\s+([\w.]+)\."([\w]+)"',
                sql,
                re.IGNORECASE,
            )
            if m:
                fqn = f'{m.group(1)}."{m.group(2)}"'
                if fqn not in self._conn._tables:
                    self._conn._tables[fqn] = []
            self._result = []
        elif sql_upper.startswith("INSERT"):
            self._result = []
        elif sql_upper.startswith("SELECT"):
            matched_rows = []
            for fqn, rows in self._conn._tables.items():
                if any(part in sql for part in fqn.replace('"', '').split('.')):
                    matched_rows = rows
                    break
            self._result = (
                [(list(r.values()) if r else [1]) for r in matched_rows]
                if matched_rows
                else [[1]]
            )
        elif sql_upper.startswith("DROP TABLE"):
            import re
            m = re.search(
                r'DROP TABLE\s+(?:IF EXISTS\s+)?([\w.]+\."[\w]+")',
                sql,
                re.IGNORECASE,
            )
            if m:
                fqn = m.group(1)
                self._conn._tables.pop(fqn, None)
            self._result = []
        else:
            self._result = []

    def fetchall(self):
        return self._result

    def fetchone(self):
        return self._result[0] if self._result else None


def _make_iceberg_cache_location():
    """Return a CacheLocation targeting the Iceberg results catalog."""
    from provisa.api_source.trino_cache import cache_location
    return cache_location("graphql-remote-src", cache_catalog="results")


def _make_fake_columns():
    """Return a list of fake column descriptors compatible with create_and_insert."""

    class _FakeType:
        def __init__(self, value: str):
            self.value = value

    class _FakeCol:
        def __init__(self, name: str, type_value: str):
            self.name = name
            self.type = _FakeType(type_value)

    return [
        _FakeCol("id", "string"),
        _FakeCol("name", "string"),
        _FakeCol("age", "integer"),
    ]


def _simulate_first_query_execution(
    conn: _FakeTrinoConnection,
    remote_rows: list[dict],
    table_name: str,
    loc,
    columns,
) -> list[dict]:
    """Simulate first query: call remote endpoint, materialise rows into Iceberg cache."""
    from provisa.api_source.trino_cache import ensure_cache_schema, create_and_insert

    conn.record_remote_call()
    ensure_cache_schema(conn, loc)
    create_and_insert(conn, loc, table_name, remote_rows, columns)

    fqn = f'{loc.catalog}.{loc.schema}."{table_name}"'
    conn._tables[fqn] = remote_rows

    return remote_rows


def _simulate_second_query_from_cache(
    conn: _FakeTrinoConnection,
    table_name: str,
    loc,
    ttl: int,
) -> tuple[list[dict], bool]:
    """Simulate second query: check cache, serve from Iceberg — no remote hop."""
    from provisa.api_source.trino_cache import table_exists

    cache_hit = table_exists(conn, loc, table_name, ttl=ttl)
    if not cache_hit:
        return [], False

    fqn = f'{loc.catalog}.{loc.schema}."{table_name}"'
    rows = conn._tables.get(fqn, [])
    return rows, True


# ---------------------------------------------------------------------------
# Helpers for REQ-311: On-demand schema refresh registry
# ---------------------------------------------------------------------------

class _InMemorySchemaRegistry:
    """Simulates the Provisa server-side registry for remote GraphQL schema sources."""

    def __init__(self):
        # source_id -> {tables, functions, rls_rules, masking_rules, schema}
        self._registry: dict[str, dict] = {}

    def register_source(
        self,
        source_id: str,
        url: str,
        namespace: str,
        schema: dict,
        auth: dict | None = None,
    ) -> None:
        from provisa.graphql_remote.mapper import map_schema

        tables, functions, raw_schema = map_schema(schema, namespace, source_id)
        self._registry[source_id] = {
            "url": url,
            "namespace": namespace,
            "auth": auth,
            "tables": tables,
            "functions": functions,
            "raw_schema": raw_schema,
            # RLS rules: list of {table_name, rule_expr}
            "rls_rules": [],
            # Masking rules: list of {table_name, column_name, strategy}
            "masking_rules": [],
        }

    def add_rls_rule(self, source_id: str, table_name: str, rule_expr: str) -> None:
        """Attach an RLS rule to a registered source table."""
        entry = self._registry[source_id]
        entry["rls_rules"].append({"table_name": table_name, "rule_expr": rule_expr})

    def add_masking_rule(
        self, source_id: str, table_name: str, column_name: str, strategy: str
    ) -> None:
        """Attach a masking rule to a registered source table column."""
        entry = self._registry[source_id]
        entry["masking_rules"].append(
            {"table_name": table_name, "column_name": column_name, "strategy": strategy}
        )

    def refresh_schema(self, source_id: str, new_schema: dict) -> dict:
        """Re-run introspection result through mapper and update registrations.

        Preserves existing RLS and masking rules.
        Returns a summary of what changed.
        """
        from provisa.graphql_remote.mapper import map_schema

        entry = self._registry[source_id]
        namespace = entry["namespace"]

        old_table_names = {t["name"] for t in entry["tables"]}
        old_function_names = {f["field_name"] for f in entry["functions"]}

        # Preserve governance rules before overwrite
        preserved_rls = list(entry["rls_rules"])
        preserved_masking = list(entry["masking_rules"])

        # Re-map with new schema
        new_tables, new_functions, new_raw_schema = map_schema(new_schema, namespace, source_id)

        # Update registrations
        entry["tables"] = new_tables
        entry["functions"] = new_functions
        entry["raw_schema"] = new_raw_schema
        # Restore preserved rules unchanged
        entry["rls_rules"] = preserved_rls
        entry["masking_rules"] = preserved_masking

        new_table_names = {t["name"] for t in new_tables}
        new_function_names = {f["field_name"] for f in new_functions}

        return {
            "added_tables": new_table_names - old_table_names,
            "removed_tables": old_table_names - new_table_names,
            "added_functions": new_function_names - old_function_names,
            "removed_functions": old_function_names - new_function_names,
            "rls_rules_preserved": preserved_rls,
            "masking_rules_preserved": preserved_masking,
        }

    def get_entry(self, source_id: str) -> dict:
        return self._registry[source_id]

    def has_source(self, source_id: str) -> bool:
        return source_id in self._registry


def _build_initial_schema_v1() -> dict:
    """Build the original remote GraphQL schema (before upstream change)."""
    user_type = {
        "kind": "OBJECT",
        "name": "User",
        "fields": [
            {"name": "id", "type": _make_scalar_gql_type("ID")},
            {"name": "name", "type": _make_scalar_gql_type("String")},
            {"name": "email", "type": _make_scalar_gql_type("String")},
        ],
    }
    return {
        "queryType": {"name": "Query"},
        "mutationType": None,
        "types": [
            {
                "kind": "OBJECT",
                "name": "Query",
                "fields": [
                    {
                        "name": "users",
                        "description": "List users",
                        "type": _make_list_of_object("User"),
                        "args": [],
                    }
                ],
            },
            user_type,
        ],
    }


def _build_evolved_schema_v2() -> dict:
    """Build the evolved remote GraphQL schema (after upstream change).

    Adds a new 'products' query field and a new 'phone' column on User.
    """
    user_type = {
        "kind": "OBJECT",
        "name": "User",
        "fields": [
            {"name": "id", "type": _make_scalar_gql_type("ID")},
            {"name": "name", "type": _make_scalar_gql_type("String")},
            {"name": "email", "type": _make_scalar_gql_type("String")},
            # New field added by upstream
            {"name": "phone", "type": _make_scalar_gql_type("String")},
        ],
    }
    product_type = {
        "kind": "OBJECT",
        "name": "Product",
        "fields": [
            {"name": "sku", "type": _make_scalar_gql_type("String")},
            {"name": "price", "type": _make_scalar_gql_type("Float")},
        ],
    }
    return {
        "queryType": {"name": "Query"},
        "mutationType": None,
        "types": [
            {
                "kind": "OBJECT",
                "name": "Query",
                "fields": [
                    {
                        "name": "users",
                        "description": "List users",
                        "type": _make_list_of_object("User"),
                        "args": [],
                    },
                    # New query field added by upstream
                    {
                        "name": "products",
                        "description": "List products",
                        "type": _make_list_of_object("Product"),
                        "args": [],
                    },
                ],
            },
            user_type,
            product_type,
        ],
    }


# ---------------------------------------------------------------------------
# Helpers for REQ-597: field_overrides for GraphQL remote schema connector
# ---------------------------------------------------------------------------

def _make_req597_schema_with_misclassified_query_field() -> dict:
    """Build a GraphQL schema that has a query-type field behaving like a mutation.

    The field 'submitReport' is structurally a query field (lives under Query type)
    but semantically behaves as a mutation (it has side effects).  A steward would
    register it with field_overrides={"submitReport": "mutation"}.
    """
    report_result_type = {
        "kind": "OBJECT",
        "name": "ReportResult",
        "fields": [
            {"name": "reportId", "type": _make_scalar_gql_type("ID")},
            {"name": "status", "type": _make_scalar_gql_type("String")},
            {"name": "createdAt", "type": _make_scalar_gql_type("String")},
        ],
    }
    query_fields = [
        {
            "name": "reports",
            "description": "List all reports",
            "type": _make_list_of_object("ReportResult"),
            "args": [],
        },
        {
            # This field lives under Query but has mutation semantics
            "name": "submitReport",
            "description": "Submit a new report (mis-classified as query by upstream)",
            "type": _make_object_gql_type("ReportResult"),
            "args": [
                {"name": "title", "type": _make_scalar_gql_type("String")},
                {"name": "body", "type": _make_scalar_gql_type("String")},
            ],
        },
    ]
    return {
        "queryType": {"name": "Query"},
        "mutationType": None,
        "types": [
            {
                "kind": "OBJECT",
                "name": "Query",
                "fields": query_fields,
            },
            report_result_type,
        ],
    }


# ---------------------------------------------------------------------------
# Helpers for REQ-598: manual vs auto-detected relationships
# ---------------------------------------------------------------------------

def _make_manual_relationship(
    rel_id: str,
    source_table: str,
    target_table: str,
    source_col: str,
    target_col: str,
) -> dict:
    """Build a manually declared relationship (no remote_managed flag)."""
    return {
        "id": rel_id,
        "source_table_id": source_table,
        "target_table_id": target_table,
        "source_column": source_col,
        "target_column": target_col,
        "cardinality": "many-to-one",
    }
    # Note: no "remote_managed" key — that is the distinguishing characteristic


def _make_auto_detected_relationship(
    rel_id: str,
    source_table: str,
    target_table: str,
    source_col: str,
    target_col: str,
) -> dict:
    """Build an auto-detected relationship (remote_managed: True)."""
    return {
        "id": rel_id,
        "source_table_id": source_table,
        "target_table_id": target_table,
        "source_column": source_col,
        "target_column": target_col,
        "cardinality": "many-to-one",
        "remote_managed": True,
    }


class _InMemoryRelationshipRegistry:
    """In-memory store that simulates the Provisa relationship persistence layer.

    Implements the semantics described in REQ-598:
    - Manually declared relationships (no remote_managed) are preserved on refresh.
    - Auto-detected relationships (remote_managed: True) are replaced on refresh.
    """

    def __init__(self):
        # rel_id -> relationship dict
        self._store: dict[str, dict] = {}

    def upsert(self, relationship: dict) -> None
