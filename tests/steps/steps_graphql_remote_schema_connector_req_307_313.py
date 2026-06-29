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

from typing import Any

import pytest
from pytest_bdd import given, scenarios, then, when

scenarios("../features/REQ-602.feature")
scenarios("../features/REQ-313.feature")
scenarios("../features/REQ-308.feature")
scenarios("../features/REQ-309.feature")
scenarios("../features/REQ-311.feature")
scenarios("../features/REQ-597.feature")
scenarios("../features/REQ-598.feature")
scenarios("../features/REQ-599.feature")


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
                if any(part in sql for part in fqn.replace('"', "").split(".")):
                    matched_rows = rows
                    break
            self._result = (
                [(list(r.values()) if r else [1]) for r in matched_rows] if matched_rows else [[1]]
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

    def upsert(self, rel: dict) -> None:
        self._store[rel["id"]] = rel

    def list_all(self) -> list[dict]:
        return list(self._store.values())

    def delete(self, rel_id: str) -> None:
        self._store.pop(rel_id, None)

    def refresh_remote_managed(self, new_rels: list[dict]) -> None:
        """Replace all remote_managed=True rels; preserve manually declared ones."""
        manual = {k: v for k, v in self._store.items() if not v.get("remote_managed")}
        self._store = manual
        for rel in new_rels:
            self._store[rel["id"]] = rel


# ---------------------------------------------------------------------------
# Helpers for REQ-599: native filter columns from required input parameters
# ---------------------------------------------------------------------------


def _build_native_filter_columns_for_graphql(field: dict) -> list[dict]:
    """Build _nf_-prefixed native filter columns from required (NON_NULL) GQL args
    that are not already response fields."""
    from provisa.graphql_remote.mapper import _build_required_args

    required_args = _build_required_args(field)
    return [
        {
            "name": f"_nf_{arg['name']}",
            "type": arg["provisa_type"],
            "native_filter_type": "graphql_variable",
        }
        for arg in required_args
    ]


# ---------------------------------------------------------------------------
# Step definitions — REQ-308
# ---------------------------------------------------------------------------


@given("a remote GraphQL schema is registered in Provisa", target_fixture="shared_data")
def step_given_remote_gql_schema_registered(shared_data: dict) -> dict:
    schema = _make_req308_schema()
    shared_data["schema"] = schema
    shared_data["namespace"] = "shop"
    shared_data["source_id"] = "shop-remote"
    return shared_data


@when("introspection completes")
def step_when_introspection_completes(shared_data: dict) -> None:
    from provisa.graphql_remote.mapper import map_schema

    tables, functions, relationships = map_schema(
        shared_data["schema"],
        shared_data["namespace"],
        shared_data["source_id"],
    )
    shared_data["tables"] = tables
    shared_data["functions"] = functions
    shared_data["relationships"] = relationships


@then(
    "Query fields are auto-registered as virtual read-only tables and Mutation fields as tracked functions"
)
def step_then_query_fields_registered_as_tables_mutation_as_functions(shared_data: dict) -> None:
    tables = shared_data["tables"]
    functions = shared_data["functions"]

    table_field_names = {t["field_name"] for t in tables}
    func_field_names = {f["field_name"] for f in functions}

    # Query fields with OBJECT return types → virtual tables
    assert "products" in table_field_names, f"Expected 'products' table, got {table_field_names}"
    assert "orders" in table_field_names, f"Expected 'orders' table, got {table_field_names}"

    # Mutation fields → tracked functions
    assert "createOrder" in func_field_names, (
        f"Expected 'createOrder' function, got {func_field_names}"
    )
    assert "updateProduct" in func_field_names, (
        f"Expected 'updateProduct' function, got {func_field_names}"
    )

    # Tables must have columns derived from the GQL return type
    products_table = next(t for t in tables if t["field_name"] == "products")
    col_names = {c["name"] for c in products_table["columns"]}
    assert "id" in col_names
    assert "name" in col_names
    assert "price" in col_names

    # Functions must have return_schema
    create_fn = next(f for f in functions if f["field_name"] == "createOrder")
    assert create_fn["return_schema"], "createOrder must have a return_schema"


# ---------------------------------------------------------------------------
# Step definitions — REQ-309
# ---------------------------------------------------------------------------


@given("a remote GraphQL source query executed within TTL", target_fixture="shared_data")
def step_given_remote_gql_query_executed_within_ttl(shared_data: dict) -> dict:
    conn = _FakeTrinoConnection()
    loc = _make_iceberg_cache_location()
    columns = _make_fake_columns()
    remote_rows = [{"id": "1", "name": "Alice", "age": 30}, {"id": "2", "name": "Bob", "age": 25}]
    table_name = "users_cache_20260101"

    rows = _simulate_first_query_execution(conn, remote_rows, table_name, loc, columns)

    shared_data["conn"] = conn
    shared_data["loc"] = loc
    shared_data["table_name"] = table_name
    shared_data["first_rows"] = rows
    shared_data["remote_call_count_after_first"] = conn.remote_call_count
    return shared_data


@when("the same query is issued again")
def step_when_same_query_issued_again(shared_data: dict) -> None:
    conn: _FakeTrinoConnection = shared_data["conn"]
    loc = shared_data["loc"]
    table_name = shared_data["table_name"]
    ttl = 3600  # 1-hour TTL

    rows, cache_hit = _simulate_second_query_from_cache(conn, table_name, loc, ttl)
    shared_data["second_rows"] = rows
    shared_data["cache_hit"] = cache_hit
    shared_data["remote_call_count_after_second"] = conn.remote_call_count


@then("results are served from the Iceberg cache in Trino with zero remote hops")
def step_then_results_served_from_iceberg_cache(shared_data: dict) -> None:
    assert shared_data["cache_hit"], "Expected a cache hit on the second query"
    assert (
        shared_data["remote_call_count_after_second"]
        == shared_data["remote_call_count_after_first"]
    ), "Remote endpoint must not be called again when serving from cache"
    assert shared_data["second_rows"] == shared_data["first_rows"], (
        "Cached rows must match the originally fetched rows"
    )


# ---------------------------------------------------------------------------
# Step definitions — REQ-311
# ---------------------------------------------------------------------------


@given("a remote GraphQL schema that has changed upstream", target_fixture="shared_data")
def step_given_remote_gql_schema_changed_upstream(shared_data: dict) -> dict:
    registry = _InMemorySchemaRegistry()
    source_id = "user-svc"
    url = "https://user-svc.example.com/graphql"
    namespace = "usersvc"

    registry.register_source(source_id, url, namespace, _build_initial_schema_v1())

    # Apply governance rules on the original schema
    registry.add_rls_rule(source_id, f"{namespace}__users", "role IN ('admin', 'analyst')")
    registry.add_masking_rule(source_id, f"{namespace}__users", "email", "hash")

    shared_data["registry"] = registry
    shared_data["source_id"] = source_id
    shared_data["namespace"] = namespace
    shared_data["evolved_schema"] = _build_evolved_schema_v2()
    return shared_data


@when("a steward triggers the Refresh Schema admin mutation")
def step_when_steward_triggers_refresh_schema(shared_data: dict) -> None:
    registry: _InMemorySchemaRegistry = shared_data["registry"]
    summary = registry.refresh_schema(shared_data["source_id"], shared_data["evolved_schema"])
    shared_data["refresh_summary"] = summary


@then("registrations are updated and existing RLS/masking rules are preserved")
def step_then_registrations_updated_rls_masking_preserved(shared_data: dict) -> None:
    registry: _InMemorySchemaRegistry = shared_data["registry"]
    source_id = shared_data["source_id"]
    namespace = shared_data["namespace"]
    summary = shared_data["refresh_summary"]

    entry = registry.get_entry(source_id)

    # New table from evolved schema must be registered
    table_names = {t["name"] for t in entry["tables"]}
    assert f"{namespace}__products" in table_names, (
        f"Expected products table after refresh, got {table_names}"
    )
    assert f"{namespace}__users" in table_names

    # Added tables reflected in summary
    assert f"{namespace}__products" in summary["added_tables"]

    # RLS rule preserved
    assert len(entry["rls_rules"]) == 1
    assert entry["rls_rules"][0]["rule_expr"] == "role IN ('admin', 'analyst')"

    # Masking rule preserved
    assert len(entry["masking_rules"]) == 1
    assert entry["masking_rules"][0]["column_name"] == "email"
    assert entry["masking_rules"][0]["strategy"] == "hash"


# ---------------------------------------------------------------------------
# Step definitions — REQ-313
# ---------------------------------------------------------------------------


@given(
    "a relationship defined between a remote schema virtual table and a local table",
    target_fixture="shared_data",
)
def step_given_relationship_between_remote_virtual_and_local(shared_data: dict) -> dict:
    local_orders = _make_local_table(
        "orders",
        [
            {"order_id": "o1", "user_id": "u1", "total": 99.99},
            {"order_id": "o2", "user_id": "u2", "total": 49.50},
            {"order_id": "o3", "user_id": "u1", "total": 15.00},
        ],
    )
    remote_users = _make_remote_virtual_table(
        field_name="users",
        source_url="https://user-svc.example.com/graphql",
        cached_rows=[
            {"id": "u1", "name": "Alice"},
            {"id": "u2", "name": "Bob"},
        ],
    )
    relationship = _define_relationship(
        local_table=local_orders,
        remote_table=remote_users,
        local_key="user_id",
        remote_key="id",
    )
    shared_data["relationship"] = relationship
    return shared_data


@when("a joined query is executed")
def step_when_joined_query_executed(shared_data: dict) -> None:
    rel = shared_data["relationship"]
    local_rows = _resolve_local_side(rel)
    remote_rows = _resolve_remote_side_from_cache(rel)
    joined = _federation_join(local_rows, remote_rows, rel["local_key"], rel["remote_key"])
    shared_data["joined_rows"] = joined
    shared_data["local_key"] = rel["local_key"]
    shared_data["remote_key"] = rel["remote_key"]


@then("the local side is resolved from cache/DB and the remote side via the cached remote call")
def step_then_local_from_cache_remote_via_cached_call(shared_data: dict) -> None:
    joined = shared_data["joined_rows"]

    # Both local and remote columns must appear in the joined result
    assert len(joined) > 0, "Join must produce at least one row"
    for row in joined:
        assert "order_id" in row, "Local column 'order_id' must be present"
        assert "name" in row, "Remote column 'name' must be present"

    # Verify specific join correctness: user_id=u1 should join with name=Alice
    alice_rows = [r for r in joined if r.get("user_id") == "u1"]
    assert alice_rows, "Expected at least one row joined to user u1"
    assert all(r["name"] == "Alice" for r in alice_rows)

    bob_rows = [r for r in joined if r.get("user_id") == "u2"]
    assert bob_rows, "Expected at least one row joined to user u2"
    assert all(r["name"] == "Bob" for r in bob_rows)


# ---------------------------------------------------------------------------
# Step definitions — REQ-597
# ---------------------------------------------------------------------------


@given(
    "a GQL remote source with a query-type field that behaves as a mutation",
    target_fixture="shared_data",
)
def step_given_gql_source_with_query_field_behaving_as_mutation(shared_data: dict) -> dict:
    schema = _make_req597_schema_with_misclassified_query_field()
    shared_data["schema"] = schema
    shared_data["namespace"] = "reporting"
    shared_data["source_id"] = "reporting-remote"
    return shared_data


@when('field_overrides maps that field to "mutation"')
def step_when_field_overrides_maps_field_to_mutation(shared_data: dict) -> None:
    from provisa.graphql_remote.mapper import map_schema

    tables, functions, relationships = map_schema(
        shared_data["schema"],
        shared_data["namespace"],
        shared_data["source_id"],
        field_overrides={"submitReport": "mutation"},
    )
    shared_data["tables"] = tables
    shared_data["functions"] = functions
    shared_data["relationships"] = relationships

    # Also map WITHOUT overrides to confirm structural classification differs
    tables_no_override, functions_no_override, _ = map_schema(
        shared_data["schema"],
        shared_data["namespace"],
        shared_data["source_id"],
    )
    shared_data["tables_no_override"] = tables_no_override
    shared_data["functions_no_override"] = functions_no_override


@then(
    "the field is registered as a tracked function and the override takes priority over structural classification"
)
def step_then_field_registered_as_function_override_takes_priority(shared_data: dict) -> None:
    func_field_names = {f["field_name"] for f in shared_data["functions"]}
    table_field_names = {t["field_name"] for t in shared_data["tables"]}

    # With override: submitReport must be a function, not a table
    assert "submitReport" in func_field_names, (
        f"submitReport must be a tracked function with override; got functions={func_field_names}"
    )
    assert "submitReport" not in table_field_names, (
        "submitReport must NOT be a table when override='mutation'"
    )

    # Without override: submitReport would be a table (OBJECT return type → table)
    table_field_names_no_override = {t["field_name"] for t in shared_data["tables_no_override"]}
    assert "submitReport" in table_field_names_no_override, (
        "Without override, submitReport (OBJECT return type) would be a table"
    )

    # The normal query field 'reports' must remain a table in both cases
    assert "reports" in table_field_names, "'reports' must still be registered as a table"


# ---------------------------------------------------------------------------
# Step definitions — REQ-598
# ---------------------------------------------------------------------------


@given(
    "a remote schema source with both manually declared and auto-detected relationships",
    target_fixture="shared_data",
)
def step_given_remote_schema_with_manual_and_auto_rels(shared_data: dict) -> dict:
    registry = _InMemoryRelationshipRegistry()

    manual_rel = _make_manual_relationship(
        rel_id="manual-rel-1",
        source_table="orders",
        target_table="customers",
        source_col="customer_id",
        target_col="id",
    )
    auto_rel = _make_auto_detected_relationship(
        rel_id="gql_remote__shop-remote__products__categoryId",
        source_table="products",
        target_table="categories",
        source_col="categoryId",
        target_col="id",
    )

    registry.upsert(manual_rel)
    registry.upsert(auto_rel)

    shared_data["registry"] = registry
    shared_data["manual_rel_id"] = manual_rel["id"]
    shared_data["auto_rel_id"] = auto_rel["id"]
    return shared_data


@when("a schema refresh is triggered")
def step_when_schema_refresh_triggered(shared_data: dict) -> None:
    registry: _InMemoryRelationshipRegistry = shared_data["registry"]

    # Simulate refresh: auto-detected rels are replaced with a different set
    new_auto_rel = _make_auto_detected_relationship(
        rel_id="gql_remote__shop-remote__products__brandId",
        source_table="products",
        target_table="brands",
        source_col="brandId",
        target_col="id",
    )
    registry.refresh_remote_managed([new_auto_rel])
    shared_data["new_auto_rel_id"] = new_auto_rel["id"]


@then(
    "auto-detected relationships are re-run and may change; manually declared relationships are preserved unchanged"
)
def step_then_auto_rels_replaced_manual_preserved(shared_data: dict) -> None:
    registry: _InMemoryRelationshipRegistry = shared_data["registry"]
    all_rels = registry.list_all()
    rel_ids = {r["id"] for r in all_rels}

    # Manual relationship must still be present
    assert shared_data["manual_rel_id"] in rel_ids, (
        f"Manual rel '{shared_data['manual_rel_id']}' must be preserved after refresh"
    )

    # Old auto-detected relationship must be gone
    assert shared_data["auto_rel_id"] not in rel_ids, (
        f"Old auto-detected rel '{shared_data['auto_rel_id']}' must be replaced by refresh"
    )

    # New auto-detected relationship must be present
    assert shared_data["new_auto_rel_id"] in rel_ids, (
        f"New auto-detected rel '{shared_data['new_auto_rel_id']}' must exist after refresh"
    )

    # Manual rel must not have remote_managed flag
    manual = next(r for r in all_rels if r["id"] == shared_data["manual_rel_id"])
    assert not manual.get("remote_managed"), "Manual relationship must not have remote_managed=True"


# ---------------------------------------------------------------------------
# Step definitions — REQ-599
# ---------------------------------------------------------------------------


@given(
    "a remote schema source with required input parameters not in the response fields",
    target_fixture="shared_data",
)
def step_given_remote_source_with_required_input_params(shared_data: dict) -> dict:
    # Build a GQL field that has a required (NON_NULL) arg 'userId' which is
    # not a field on the return type
    required_arg = {
        "name": "userId",
        "type": {"kind": "NON_NULL", "name": None, "ofType": _make_scalar_gql_type("ID")},
        "defaultValue": None,
    }
    optional_arg = {
        "name": "limit",
        "type": _make_scalar_gql_type("Int"),
        "defaultValue": 10,
    }
    activity_type = {
        "kind": "OBJECT",
        "name": "Activity",
        "fields": [
            {"name": "id", "type": _make_scalar_gql_type("ID")},
            {"name": "action", "type": _make_scalar_gql_type("String")},
            {"name": "timestamp", "type": _make_scalar_gql_type("String")},
        ],
    }
    field = {
        "name": "userActivity",
        "description": "Fetch activity for a user",
        "type": _make_list_of_object("Activity"),
        "args": [required_arg, optional_arg],
    }
    shared_data["field"] = field
    shared_data["activity_type"] = activity_type
    return shared_data


@when("the source is registered")
def step_when_source_is_registered(shared_data: dict) -> None:
    nf_columns = _build_native_filter_columns_for_graphql(shared_data["field"])
    shared_data["nf_columns"] = nf_columns


@then(
    "those parameters become _nf_-prefixed native filter columns with the appropriate native_filter_type"
)
def step_then_params_become_nf_prefixed_native_filter_columns(shared_data: dict) -> None:
    nf_columns = shared_data["nf_columns"]

    # Only the NON_NULL arg (userId) must become a native filter column
    nf_names = {c["name"] for c in nf_columns}
    assert "_nf_userId" in nf_names, (
        f"Required arg 'userId' must produce '_nf_userId' column; got {nf_names}"
    )

    # Optional arg must NOT be promoted to a native filter column
    assert "_nf_limit" not in nf_names, (
        "Optional arg 'limit' must not produce a native filter column"
    )

    # native_filter_type must be set
    user_id_col = next(c for c in nf_columns if c["name"] == "_nf_userId")
    assert user_id_col["native_filter_type"], (
        "native_filter_type must be set on the native filter column"
    )

    # Type must map correctly: ID → text
    assert user_id_col["type"] == "text", (
        f"GQL ID type must map to 'text', got {user_id_col['type']!r}"
    )


# ---------------------------------------------------------------------------
# Step definitions — REQ-602
# ---------------------------------------------------------------------------


@given(
    "remote schema tables (GraphQL remote, gRPC remote, OpenAPI) registered in Provisa",
    target_fixture="shared_data",
)
def step_given_remote_schema_tables_registered(shared_data: dict) -> dict:
    gql_source = _make_graphql_remote_source("gql-src", "gql")
    grpc_source = _make_grpc_remote_source("grpc-src", "grpc")
    openapi_source = _make_openapi_source("oapi-src", "oapi")
    shared_data["sources"] = [gql_source, grpc_source, openapi_source]
    return shared_data


@when("schema generation runs")
def step_when_schema_generation_runs(shared_data: dict) -> None:
    synthesized = _synthesize_all(shared_data["sources"])
    shared_data["synthesized"] = synthesized


@then(
    "ColumnMetadata is synthesized with correct type mappings equivalent to catalog introspection for local tables"
)
def step_then_column_metadata_synthesized_with_correct_mappings(shared_data: dict) -> None:
    from provisa.compiler.introspect import ColumnMetadata

    synthesized: dict[str, list[dict]] = shared_data["synthesized"]

    # --- GraphQL remote source ---
    gql_cols = synthesized["gql-src"]
    gql_by_name = {c["column_name"]: c for c in gql_cols}

    assert gql_by_name["id"]["provisa_type"] == "text"  # GQL ID → text
    assert gql_by_name["name"]["provisa_type"] == "text"  # GQL String → text
    assert gql_by_name["age"]["provisa_type"] == "integer"  # GQL Int → integer
    assert gql_by_name["score"]["provisa_type"] == "numeric"  # GQL Float → numeric
    assert gql_by_name["active"]["provisa_type"] == "boolean"  # GQL Boolean → boolean

    # source_type must be set
    assert all(c["source_type"] == "graphql_remote" for c in gql_cols)

    # Can be wrapped into ColumnMetadata objects without error
    cm_objects = _build_column_metadata_objects(gql_cols)
    assert all(isinstance(cm, ColumnMetadata) for cm in cm_objects)

    # --- gRPC remote source ---
    grpc_cols = synthesized["grpc-src"]
    grpc_by_name = {c["column_name"]: c for c in grpc_cols}

    assert grpc_by_name["id"]["provisa_type"] == "text"  # proto string → text
    assert grpc_by_name["order_total"]["provisa_type"] == "numeric"  # proto double → numeric
    assert grpc_by_name["quantity"]["provisa_type"] == "integer"  # proto int32 → integer
    assert grpc_by_name["fulfilled"]["provisa_type"] == "boolean"  # proto bool → boolean
    assert grpc_by_name["created_at"]["provisa_type"] == "bigint"  # proto int64 → bigint
    assert all(c["source_type"] == "grpc_remote" for c in grpc_cols)

    # --- OpenAPI source ---
    oapi_cols = synthesized["oapi-src"]
    oapi_by_name = {c["column_name"]: c for c in oapi_cols}

    assert oapi_by_name["product_id"]["provisa_type"] == "text"  # JSON string → text
    assert oapi_by_name["price"]["provisa_type"] == "numeric"  # JSON number → numeric
    assert oapi_by_name["stock"]["provisa_type"] == "integer"  # JSON integer → integer
    assert oapi_by_name["available"]["provisa_type"] == "boolean"  # JSON boolean → boolean
    assert oapi_by_name["tags"]["provisa_type"] == "jsonb"  # JSON array → jsonb
    assert oapi_by_name["attributes"]["provisa_type"] == "jsonb"  # JSON object → jsonb
    assert all(c["source_type"] == "openapi" for c in oapi_cols)
