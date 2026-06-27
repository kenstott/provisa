# Copyright (c) 2026 Kenneth Stott
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1

"""pytest-bdd step implementations for REQ-008: Schema generation pass, REQ-009: Single-statement SQL compilation, REQ-259: Apollo Federation v2, REQ-478: Statistical row sampling, REQ-655: group_by HAVING/FILTER clauses, REQ-252: Schema inference for supported connectors, REQ-253: Immediate schema rebuild on naming convention changes, REQ-403: RLS injection table-level precedence, REQ-409: Cypher datetime coercion, REQ-411: hasura-default naming convention, REQ-412: graphql-default naming convention, REQ-525: Per-role proto generation, REQ-534: Multi-root GraphQL query compilation, REQ-537: Schema version endpoint, and REQ-654: group_by root query field."""

from __future__ import annotations

import os
import re
import uuid
from typing import Any

import pytest
from pytest_bdd import given, parsers, scenario, then, when

from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.schema_gen import SchemaInput, generate_schema
from provisa.compiler.sql_gen import build_context, compile_query


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _col(name: str, data_type: str = "varchar(100)", nullable: bool = False) -> ColumnMetadata:
    return ColumnMetadata(column_name=name, data_type=data_type, is_nullable=nullable)


def _make_default_schema_input(role_id: str = "admin") -> SchemaInput:
    """Build a minimal but realistic SchemaInput that exercises all REQ-008 paths."""
    tables = [
        {
            "id": 1,
            "source_id": "sales-pg",
            "domain_id": "sales",
            "schema_name": "public",
            "table_name": "orders",
            "governance": "pre-approved",
            "columns": [
                {"column_name": "id", "visible_to": ["admin", "analyst"]},
                {"column_name": "customer_id", "visible_to": ["admin", "analyst"]},
                {"column_name": "amount", "visible_to": ["admin"]},
                {"column_name": "region", "visible_to": ["admin"]},
                {"column_name": "status", "visible_to": ["admin"]},
                {"column_name": "created_at", "visible_to": ["admin"]},
            ],
        },
        {
            "id": 2,
            "source_id": "sales-pg",
            "domain_id": "sales",
            "schema_name": "public",
            "table_name": "customers",
            "governance": "pre-approved",
            "columns": [
                {"column_name": "id", "visible_to": ["admin", "analyst"]},
                {"column_name": "name", "visible_to": ["admin", "analyst"]},
                {"column_name": "email", "visible_to": ["admin"]},
            ],
        },
    ]

    relationships = [
        {
            "id": "ord-cust",
            "source_table_id": 1,
            "target_table_id": 2,
            "source_column": "customer_id",
            "target_column": "id",
            "cardinality": "many-to-one",
        },
    ]

    column_types: dict[int, list[ColumnMetadata]] = {
        1: [
            _col("id", "integer"),
            _col("customer_id", "integer"),
            _col("amount", "decimal(10,2)"),
            _col("region", "varchar(50)"),
            _col("status", "varchar(20)"),
            _col("created_at", "timestamp"),
        ],
        2: [
            _col("id", "integer"),
            _col("name", "varchar(100)"),
            _col("email", "varchar(200)"),
        ],
    }

    role = {
        "id": role_id,
        "capabilities": ["query_development"],
        "domain_access": ["*"],
    }
    domains = [{"id": "sales", "description": "Sales"}]

    return SchemaInput(
        tables=tables,
        relationships=relationships,
        column_types=column_types,
        naming_rules=[],
        role=role,
        domains=domains,
    )


def _make_federation_schema_input(role_id: str = "admin") -> SchemaInput:
    """Build a SchemaInput with federation_v2=True enabled for REQ-259 tests."""
    tables = [
        {
            "id": 10,
            "source_id": "catalog-pg",
            "domain_id": "catalog",
            "schema_name": "public",
            "table_name": "products",
            "governance": "pre-approved",
            "pk_columns": ["id"],
            "columns": [
                {"column_name": "id", "visible_to": ["admin", "analyst"]},
                {"column_name": "name", "visible_to": ["admin", "analyst"]},
                {"column_name": "price", "visible_to": ["admin"]},
                {"column_name": "category", "visible_to": ["admin", "analyst"]},
            ],
        },
        {
            "id": 11,
            "source_id": "catalog-pg",
            "domain_id": "catalog",
            "schema_name": "public",
            "table_name": "reviews",
            "governance": "pre-approved",
            "pk_columns": ["id"],
            "columns": [
                {"column_name": "id", "visible_to": ["admin", "analyst"]},
                {"column_name": "product_id", "visible_to": ["admin", "analyst"]},
                {"column_name": "body", "visible_to": ["admin", "analyst"]},
                {"column_name": "rating", "visible_to": ["admin"]},
            ],
        },
    ]

    relationships = [
        {
            "id": "rev-prod",
            "source_table_id": 11,
            "target_table_id": 10,
            "source_column": "product_id",
            "target_column": "id",
            "cardinality": "many-to-one",
        },
    ]

    column_types: dict[int, list[ColumnMetadata]] = {
        10: [
            _col("id", "integer"),
            _col("name", "varchar(200)"),
            _col("price", "decimal(10,2)"),
            _col("category", "varchar(100)"),
        ],
        11: [
            _col("id", "integer"),
            _col("product_id", "integer"),
            _col("body", "text"),
            _col("rating", "integer"),
        ],
    }

    role = {
        "id": role_id,
        "capabilities": ["query_development"],
        "domain_access": ["*"],
    }
    domains = [{"id": "catalog", "description": "Product Catalog"}]

    return SchemaInput(
        tables=tables,
        relationships=relationships,
        column_types=column_types,
        naming_rules=[],
        role=role,
        domains=domains,
        federation_v2=True,
    )


def _make_sampling_schema_input() -> SchemaInput:
    """Build a minimal SchemaInput suitable for testing the sample argument (REQ-478)."""
    tables = [
        {
            "id": 1,
            "source_id": "sales-pg",
            "domain_id": "sales",
            "schema_name": "public",
            "table_name": "orders",
            "governance": "pre-approved",
            "columns": [
                {"column_name": "id", "visible_to": ["admin"]},
                {"column_name": "customer_id", "visible_to": ["admin"]},
                {"column_name": "amount", "visible_to": ["admin"]},
                {"column_name": "region", "visible_to": ["admin"]},
                {"column_name": "status", "visible_to": ["admin"]},
                {"column_name": "created_at", "visible_to": ["admin"]},
            ],
        },
    ]

    column_types: dict[int, list[ColumnMetadata]] = {
        1: [
            _col("id", "integer"),
            _col("customer_id", "integer"),
            _col("amount", "decimal(10,2)"),
            _col("region", "varchar(50)"),
            _col("status", "varchar(20)"),
            _col("created_at", "timestamp"),
        ],
    }

    role = {
        "id": "admin",
        "capabilities": ["query_development"],
        "domain_access": ["*"],
    }
    domains = [{"id": "sales", "description": "Sales"}]

    return SchemaInput(
        tables=tables,
        relationships=[],
        column_types=column_types,
        naming_rules=[],
        role=role,
        domains=domains,
    )


def _make_group_by_schema_input() -> SchemaInput:
    """Build a SchemaInput with enable_group_by=True for REQ-655 tests."""
    from provisa.compiler import naming as _naming

    _naming.configure(gql="snake")

    tables = [
        {
            "id": 1,
            "source_id": "sales-pg",
            "domain_id": "sales",
            "schema_name": "public",
            "table_name": "orders",
            "governance": "pre-approved",
            "enable_group_by": True,
            "enable_aggregates": False,
            "columns": [
                {"column_name": "id", "visible_to": ["admin"]},
                {"column_name": "amount", "visible_to": ["admin"]},
                {"column_name": "region", "visible_to": ["admin"]},
                {"column_name": "status", "visible_to": ["admin"]},
                {"column_name": "created_at", "visible_to": ["admin"]},
            ],
        },
    ]

    column_types: dict[int, list[ColumnMetadata]] = {
        1: [
            _col("id", "integer"),
            _col("amount", "decimal(10,2)"),
            _col("region", "varchar(20)"),
            _col("status", "varchar(20)"),
            _col("created_at", "timestamp"),
        ],
    }

    role = {
        "id": "admin",
        "capabilities": ["query_development"],
        "domain_access": ["*"],
    }
    domains = [{"id": "sales", "description": "Sales"}]

    return SchemaInput(
        tables=tables,
        relationships=[],
        column_types=column_types,
        naming_rules=[],
        role=role,
        domains=domains,
    )


def _make_req009_schema_input() -> SchemaInput:
    """Build a SchemaInput with a join relationship for REQ-009 single-statement SQL tests."""
    tables = [
        {
            "id": 1,
            "source_id": "sales-pg",
            "domain_id": "sales",
            "schema_name": "public",
            "table_name": "orders",
            "governance": "pre-approved",
            "columns": [
                {"column_name": "id", "visible_to": ["admin"]},
                {"column_name": "customer_id", "visible_to": ["admin"]},
                {"column_name": "amount", "visible_to": ["admin"]},
                {"column_name": "status", "visible_to": ["admin"]},
            ],
        },
        {
            "id": 2,
            "source_id": "sales-pg",
            "domain_id": "sales",
            "schema_name": "public",
            "table_name": "customers",
            "governance": "pre-approved",
            "columns": [
                {"column_name": "id", "visible_to": ["admin"]},
                {"column_name": "name", "visible_to": ["admin"]},
                {"column_name": "email", "visible_to": ["admin"]},
            ],
        },
    ]

    relationships = [
        {
            "id": "ord-cust",
            "source_table_id": 1,
            "target_table_id": 2,
            "source_column": "customer_id",
            "target_column": "id",
            "cardinality": "many-to-one",
        },
    ]

    column_types: dict[int, list[ColumnMetadata]] = {
        1: [
            _col("id", "integer"),
            _col("customer_id", "integer"),
            _col("amount", "decimal(10,2)"),
            _col("status", "varchar(20)"),
        ],
        2: [
            _col("id", "integer"),
            _col("name", "varchar(100)"),
            _col("email", "varchar(200)"),
        ],
    }

    role = {
        "id": "admin",
        "capabilities": ["query_development"],
        "domain_access": ["*"],
    }
    domains = [{"id": "sales", "description": "Sales"}]

    return SchemaInput(
        tables=tables,
        relationships=relationships,
        column_types=column_types,
        naming_rules=[],
        role=role,
        domains=domains,
    )


# ---------------------------------------------------------------------------
# Helpers for REQ-252: Schema inference for supported connectors
# ---------------------------------------------------------------------------

# Columns returned by a simulated MongoDB auto-discovery introspection.
_MONGO_INFERRED_COLUMNS: list[ColumnMetadata] = [
    _col("_id", "varchar(100)"),
    _col("user_name", "varchar(255)"),
    _col("email", "varchar(255)"),
    _col("created_at", "timestamp"),
    _col("score", "double"),
]

# Explicit column overrides supplied by the schema author.  These must win over
# the inferred list when both are present.
_MONGO_EXPLICIT_COLUMNS: list[dict] = [
    {"column_name": "_id", "visible_to": ["admin"]},
    {"column_name": "user_name", "visible_to": ["admin", "analyst"]},
    # 'email' is intentionally absent from the explicit list — it should still
    # appear in the merged column set (inferred columns that have no explicit
    # override are included).
    # 'score' is also absent — will be provided from inference.
]


def _simulate_mongo_discover(inferred: list[ColumnMetadata]) -> list[ColumnMetadata]:
    """
    Simulate the Trino connector auto-discovery call for a MongoDB source.

    In a live integration environment this would call
    ``introspect_table_columns(trino_conn, catalog, schema, table)`` against
    the MongoDB connector, which supports schema inference via Trino's
    ``information_schema.columns`` table (populated lazily on first access).

    Here we return the pre-built inferred list directly so the unit test
    exercises the compiler logic without requiring a live Trino/MongoDB stack.
    """
    return list(inferred)


def _make_mongo_discover_schema_input(
    *,
    explicit_columns: list[dict] | None = None,
    include_discover_flag: bool = True,
) -> SchemaInput:
    """
    Build a SchemaInput representing a MongoDB source registered with
    ``discover: true``.

    Parameters
    ----------
    explicit_columns:
        Optional list of explicit column dicts.  When provided they are merged
        with the inferred set; explicit definitions take precedence.
    include_discover_flag:
        When True the table definition carries ``"discover": True``.
    """
    if explicit_columns is None:
        explicit_columns = list(_MONGO_EXPLICIT_COLUMNS)

    # Simulate what the schema compiler sees after auto-discovery:
    # combine inferred columns with the explicit overrides.  Explicit entries
    # shadow inferred ones with the same column_name.
    explicit_names: set[str] = {c["column_name"] for c in explicit_columns}

    # Merged column list visible to the schema registration layer.
    merged_columns: list[dict] = list(explicit_columns)
    for inferred_col in _MONGO_INFERRED_COLUMNS:
        if inferred_col.column_name not in explicit_names:
            merged_columns.append(
                {"column_name": inferred_col.column_name, "visible_to": ["admin"]}
            )

    table_def: dict = {
        "id": 20,
        "source_id": "mongo-events",
        "domain_id": "events",
        "schema_name": "events_db",
        "table_name": "user_events",
        "governance": "pre-approved",
        "source_type": "mongodb",
        "columns": merged_columns,
    }
    if include_discover_flag:
        table_def["discover"] = True

    # column_types mirrors what introspect_table_columns would return for a
    # MongoDB collection — we use the inferred list as the ground truth here.
    column_types: dict[int, list[ColumnMetadata]] = {
        20: _simulate_mongo_discover(_MONGO_INFERRED_COLUMNS),
    }

    role = {
        "id": "admin",
        "capabilities": ["query_development"],
        "domain_access": ["*"],
    }
    domains = [{"id": "events", "description": "Event Tracking"}]

    return SchemaInput(
        tables=[table_def],
        relationships=[],
        column_types=column_types,
        naming_rules=[],
        role=role,
        domains=domains,
    )


# ---------------------------------------------------------------------------
# Helpers for REQ-253: Immediate schema rebuild on naming convention changes
# ---------------------------------------------------------------------------


def _make_naming_convention_schema_input(naming_rules: list[dict], role_id: str = "admin") -> SchemaInput:
    """
    Build a SchemaInput with the given naming_rules applied, simulating the
    state of Provisa after an admin mutation changes a naming convention.

    The table 'order_items' has an underscore, which is a common target for
    naming convention rules (e.g. strip underscores, camelCase, etc.).
    """
    tables = [
        {
            "id": 30,
            "source_id": "sales-pg",
            "domain_id": "sales",
            "schema_name": "public",
            "table_name": "order_items",
            "governance": "pre-approved",
            "columns": [
                {"column_name": "id", "visible_to": ["admin", "analyst"]},
                {"column_name": "order_id", "visible_to": ["admin", "analyst"]},
                {"column_name": "product_name", "visible_to": ["admin", "analyst"]},
                {"column_name": "quantity", "visible_to": ["admin", "analyst"]},
                {"column_name": "unit_price", "visible_to": ["admin"]},
            ],
        },
        {
            "id": 31,
            "source_id": "sales-pg",
            "domain_id": "sales",
            "schema_name": "public",
            "table_name": "customers",
            "governance": "pre-approved",
            "columns": [
                {"column_name": "id", "visible_to": ["admin", "analyst"]},
                {"column_name": "full_name", "visible_to": ["admin", "analyst"]},
                {"column_name": "email_address", "visible_to": ["admin", "analyst"]},
            ],
        },
    ]

    column_types: dict[int, list[ColumnMetadata]] = {
        30: [
            _col("id", "integer"),
            _col("order_id", "integer"),
            _col("product_name", "varchar(200)"),
            _col("quantity", "integer"),
            _col("unit_price", "decimal(10,2)"),
        ],
        31: [
            _col("id", "integer"),
            _col("full_name", "varchar(200)"),
            _col("email_address", "varchar(255)"),
        ],
    }

    role = {
        "id": role_id,
        "capabilities": ["query_development"],
        "domain_access": ["*"],
    }
    domains = [{"id": "sales", "description": "Sales"}]

    return SchemaInput(
        tables=tables,
        relationships=[],
        column_types=column_types,
        naming_rules=naming_rules,
        role=role,
        domains=domains,
    )


def _simulate_rebuild_schemas(schema_inputs: list[SchemaInput]) -> list[Any]:
    """
    Simulate what Provisa's _rebuild_schemas() does: re-run generate_schema()
    for every active SchemaInput (one per role) and return the regenerated
    in-memory schema objects.

    In production _rebuild_schemas() is called by the admin mutation handler
    and updates the module-level schema registry. Here we replicate that
    regeneration loop so the BDD steps can assert on the returned objects
    without requiring a live server.
    """
    regenerated = []
    for si in schema_inputs:
        schema = generate_schema(si)
        assert schema is not None, (
            f"generate_schema returned None for role '{si.role.get('id', '?')}' "
            "during _rebuild_schemas() simulation"
        )
        regenerated.append(schema)
    return regenerated


def _simulate_introspection_query(schema: Any) -> dict:
    """
    Execute a GraphQL introspection query against the given in-memory schema
    object and return the parsed result dict.

    This mirrors what GraphiQL and Voyager do on each page load: they send
    a full introspection document to the server, which resolves it against
    the current in-memory schema.  The result here is produced synchronously
    using graphql-core's ``graphql_sync`` so no HTTP server is needed.
    """
    from graphql import graphql_sync, build_introspection_query

    # Standard introspection query
    introspection_query = build_introspection_query()
    result = graphql_sync(schema, introspection_query)

    assert result.errors is None or len(result.errors) == 0, (
        f"Introspection query returned errors: {result.errors}"
    )
    assert result.data is not None, "Introspection query returned no data"
    assert "__schema" in result.data, (
        "Introspection result must contain a '__schema' key"
    )
    return result.data


# ---------------------------------------------------------------------------
# Helpers for REQ-403: RLS injection table-level precedence
# ---------------------------------------------------------------------------


def _make_rls_meta(domain_id: str = "sales"):
    """Build a TableMeta for the orders table in the given domain."""
    from provisa.compiler.sql_gen import TableMeta

    return TableMeta(
        table_id=1,
        field_name="orders",
        type_name="Orders",
        source_id="pg",
        catalog_name="pg",
        schema_name="public",
        table_name="orders",
        domain_id=domain_id,
    )


def _make_rls_ctx(meta):
    """Build a minimal CompilationContext referencing the given TableMeta."""
    from provisa.compiler.sql_gen import CompilationContext

    ctx = CompilationContext()
    ctx.tables = {"orders": meta}
    ctx.joins = {}
    return ctx


def _make_rls_compiled():
    """Build a minimal CompiledQuery for the orders table."""
    from provisa.compiler.sql_gen import ColumnRef, CompiledQuery

    return CompiledQuery(
        sql='SELECT "id" FROM "public"."orders"',
        params=[],
        root_field="orders",
        columns=[ColumnRef(alias=None, column="id", field_name="id", nested_in=None)],
        sources={"pg"},
    )


# ---------------------------------------------------------------------------
# Helpers for REQ-409: Cypher datetime coercion
# ---------------------------------------------------------------------------

# A representative set of ISO 8601 datetime strings to exercise the coercion.
_ISO8601_SAMPLES = [
    "2024-01-15T00:00:00",
    "2023-06-30T12:34:56",
    "2024-03-01T08:00:00.000",
    "2024-12-31T23:59:59Z",
    "2024-01-15T00:00:00+05:30",
]

# Pattern that mirrors what the translator uses internally (ISO 8601 date-time).
_ISO8601_RE = re.compile(
    r"'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})?'"
)


def _build_cypher_where_with_datetime(dt_string: str = "2024-01-15T00:00:00") -> str:
    """
    Return a complete Cypher query whose WHERE clause contains an ISO 8601
    datetime string literal.  The Event node uses a 'created_at' timestamp PK.
    """
    return (
        f"MATCH (e:Event) "
        f"WHERE e.created_at = '{dt_string}' "
        f"RETURN e.created_at"
    )


def _build_cypher_label_map_for_event() -> Any:
    """
    Build a minimal CypherLabelMap for an Event node with a timestamp PK column.
    """
    from provisa.cypher.label_map import CypherLabelMap, NodeMapping

    event_meta = NodeMapping(
        label="Event",
        type_name="Event",
        domain_label=None,
        table_label="Event",
        table_id=99,
        source_id="trino-main",
        id_column="created_at",
        pk_columns=["created_at"],
        catalog_name="hive",
        schema_name="analytics",
        table_name="events",
        properties={"created_at": "created_at", "name": "name"},
    )
    return CypherLabelMap(nodes={"Event": event_meta}, relationships={})


# ---------------------------------------------------------------------------
# Helpers for REQ-411: hasura-default naming convention
# ---------------------------------------------------------------------------


def _make_hasura_default_schema_input() -> SchemaInput:
    """
    Build a SchemaInput for testing hasura-default naming convention.

    Uses the orders table as the primary subject since the requirement
    explicitly calls out insert_orders, update_orders, delete_orders as
    the expected mutation names.
    """
    from provisa.compiler import naming as _naming

    _naming.configure(gql="hasura-default")

    tables = [
        {
            "id": 1,
            "source_id": "sales-pg",
            "domain_id": "sales",
            "schema_name": "public",
            "table_name": "orders",
            "governance": "pre-approved",
            "columns": [
                {"column_name": "id", "visible_to": ["admin"]},
                {"column_name": "customer_id", "visible_to": ["admin"]},
                {"column_name": "order_total", "visible_to": ["admin"]},
                {"column_name": "order_status", "visible_to": ["admin"]},
                {"column_name": "created_at", "visible_to": ["admin"]},
            ],
        },
    ]

    column_types: dict[int, list[ColumnMetadata]] = {
        1: [
            _col("id", "integer"),
            _col("customer_id", "integer"),
            _col("order_total", "decimal(10,2)"),
            _col("order_status", "varchar(20)"),
            _col("created_at", "timestamp"),
        ],
    }

    role = {
        "id": "admin",
        "capabilities": ["query_development"],
        "domain_access": ["*"],
    }
    domains = [{"id": "sales", "description": "Sales"}]

    return SchemaInput(
        tables=tables,
        relationships=[],
        column_types=column_types,
        naming_rules=[],
        role=role,
        domains=domains,
    )


def _is_snake_case(name: str) -> bool:
    """Return True if name is valid snake_case (lowercase letters, digits, underscores)."""
    return bool(re.match(r'^[a-z][a-z0-9_]*$', name))


# ---------------------------------------------------------------------------
# Helpers for REQ-412: graphql-default naming convention
# ---------------------------------------------------------------------------


def _make_graphql_default_schema_input() -> SchemaInput:
    """
    Build a SchemaInput for testing graphql-default naming convention.

    Uses an orders table with multi-word column names so that camelCase
    conversion is observable (e.g. customer_id → customerId,
    order_total → orderTotal).  Also includes a second table (order_items)
    so we can assert PascalCase type names for both.
    """
    from provisa.compiler import naming as _naming

    _naming.configure(gql="graphql-default")

    tables = [
        {
            "id": 1,
            "source_id": "sales-pg",
            "domain_id": "sales",
            "schema_name": "public",
            "table_name": "orders",
            "governance": "pre-approved",
            "columns": [
                {"column_name": "id", "visible_to": ["admin"]},
                {"column_name": "customer_id", "visible_to": ["admin"]},
                {"column_name": "order_total", "visible_to": ["admin"]},
                {"column_name": "order_status", "visible_to": ["admin"]},
                {"column_name": "created_at", "visible_to": ["admin"]},
            ],
        },
        {
            "id": 2,
            "source_id": "sales-pg",
            "domain_id": "sales",
            "schema_name": "public",
            "table_name": "order_items",
            "governance": "pre-approved",
            "columns": [
                {"column_name": "id", "visible_to": ["admin"]},
                {"column_name": "order_id", "visible_to": ["admin"]},
                {"column_name": "product_name", "visible_to": ["admin"]},
                {"column_name": "unit_price", "visible_to": ["admin"]},
            ],
        },
    ]

    column_types: dict[int, list[ColumnMetadata]] = {
        1: [
            _col("id", "integer"),
            _col("customer_id", "integer"),
            _col("order_total", "decimal(10,2)"),
            _col("order_status", "varchar(20)"),
            _col("created_at", "timestamp"),
        ],
        2: [
            _col("id", "integer"),
            _col("order_id", "integer"),
            _col("product_name", "varchar(200)"),
            _col("unit_price", "decimal(10,2)"),
        ],
    }

    role = {
        "id": "admin",
        "capabilities": ["query_development"],
        "domain_access": ["*"],
    }
    domains = [{"id": "sales", "description": "Sales"}]

    return SchemaInput(
        tables=tables,
        relationships=[],
        column_types
