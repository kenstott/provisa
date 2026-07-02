# Copyright (c) 2026 Kenneth Stott
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1

"""pytest-bdd step implementations for REQ-008: Schema generation pass, REQ-009: Single-statement SQL compilation, REQ-259: Apollo Federation v2, REQ-478: Statistical row sampling, REQ-655: group_by HAVING/FILTER clauses, REQ-252: Schema inference for supported connectors, REQ-253: Immediate schema rebuild on naming convention changes, REQ-403: RLS injection table-level precedence, REQ-409: Cypher datetime coercion, REQ-411: hasura-default naming convention, REQ-412: graphql-default naming convention, REQ-525: Per-role proto generation, REQ-534: Multi-root GraphQL query compilation, REQ-537: Schema version endpoint, and REQ-654: group_by root query field."""

from __future__ import annotations

import re
from typing import Any

import pytest

from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.schema_gen import SchemaInput, generate_schema


# ---------------------------------------------------------------------------
# Shared data fixture
# ---------------------------------------------------------------------------


@pytest.fixture
def shared_data():
    return {}


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


def _make_naming_convention_schema_input(
    naming_rules: list[dict], role_id: str = "admin"
) -> SchemaInput:
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
    using graphql-core's ``graphql_sync`` so no GraphQL server is needed.
    """
    from graphql import graphql_sync, get_introspection_query

    # Standard introspection query
    introspection_query = get_introspection_query()
    result = graphql_sync(schema, introspection_query)

    assert result.errors is None or len(result.errors) == 0, (
        f"Introspection query returned errors: {result.errors}"
    )
    assert result.data is not None, "Introspection query returned no data"
    assert "__schema" in result.data, "Introspection result must contain a '__schema' key"
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
_ISO8601_RE = re.compile(r"'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})?'")


def _build_cypher_where_with_datetime(dt_string: str = "2024-01-15T00:00:00") -> str:
    """
    Return a complete Cypher query whose WHERE clause contains an ISO 8601
    datetime string literal.  The Event node uses a 'created_at' timestamp PK.
    """
    return f"MATCH (e:Event) WHERE e.created_at = '{dt_string}' RETURN e.created_at"


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
    return bool(re.match(r"^[a-z][a-z0-9_]*$", name))


# ---------------------------------------------------------------------------
# Helpers for REQ-412: graphql-default naming convention
# ---------------------------------------------------------------------------


def _make_graphql_default_schema_input() -> SchemaInput:
    """
    Build a SchemaInput for testing graphql-default naming convention.

    Uses an orders table with multi-word column names so that camelCase
    conversion is observable (e.g. customer_id -> customerId,
    order_total -> orderTotal).  Also includes a second table (order_items)
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
        column_types=column_types,
        naming_rules=[],
        role=role,
        domains=domains,
    )


# ===========================================================================
# Scenario bindings
# ===========================================================================

from graphql import print_schema, parse
from pytest_bdd import given, when, then, scenarios

from provisa.compiler.rls import build_rls_context, inject_rls
from provisa.compiler.sql_gen import build_context, compile_query
from provisa.cypher.translator import _coerce_ts_literals
from provisa.grpc.proto_gen import generate_proto

scenarios("../features/REQ-008.feature")
scenarios("../features/REQ-253.feature")
scenarios("../features/REQ-403.feature")
scenarios("../features/REQ-409.feature")
scenarios("../features/REQ-525.feature")
scenarios("../features/REQ-534.feature")
scenarios("../features/REQ-537.feature")
scenarios("../features/REQ-654.feature")


# ---------------------------------------------------------------------------
# REQ-008: Schema generation pass
# ---------------------------------------------------------------------------


@given("a table is registered")
def req008_table_registered(shared_data):
    from provisa.compiler import naming as _naming

    _naming.configure(gql="snake")
    shared_data["req008_admin_input"] = _make_default_schema_input("admin")
    shared_data["req008_analyst_input"] = _make_default_schema_input("analyst")


@when("the schema generation pass runs")
def req008_run_generation(shared_data):
    admin_schema = generate_schema(shared_data["req008_admin_input"])
    analyst_schema = generate_schema(shared_data["req008_analyst_input"])
    shared_data["req008_admin_sdl"] = print_schema(admin_schema)
    shared_data["req008_analyst_sdl"] = print_schema(analyst_schema)


@then(
    "it queries Trino INFORMATION_SCHEMA, applies per-role column visibility, "
    "incorporates relationships, and produces GraphQL SDL"
)
def req008_assert_sdl(shared_data):
    admin_sdl = shared_data["req008_admin_sdl"]
    analyst_sdl = shared_data["req008_analyst_sdl"]

    # Produces GraphQL SDL with the registered tables as types.
    assert "type Query" in admin_sdl
    assert "Orders" in admin_sdl
    assert "Customers" in admin_sdl

    # Per-role column visibility: 'amount'/'email' are visible only to admin.
    assert "amount" in admin_sdl
    assert "amount" not in analyst_sdl
    assert "email" in admin_sdl
    assert "email" not in analyst_sdl

    # Columns visible to both roles appear for both.
    assert "customer_id" in admin_sdl
    assert "customer_id" in analyst_sdl

    # Incorporates relationships: the many-to-one order->customer link surfaces
    # a nested customers field on the Orders type.
    assert "customers" in admin_sdl.lower()


# ---------------------------------------------------------------------------
# REQ-253: Immediate schema rebuild on naming convention changes
# ---------------------------------------------------------------------------


@given("a naming convention change is applied via admin mutation")
def req253_naming_change(shared_data):
    from provisa.compiler import naming as _naming

    # Baseline schema under snake naming, then flip to graphql-default (camelCase
    # fields, PascalCase types) to simulate an admin naming-convention mutation.
    _naming.configure(gql="snake")
    shared_data["req253_before"] = generate_schema(
        _make_naming_convention_schema_input([])
    )
    _naming.configure(gql="graphql-default")
    shared_data["req253_input"] = _make_naming_convention_schema_input([])


@when("_rebuild_schemas() completes")
def req253_rebuild(shared_data):
    schemas = _simulate_rebuild_schemas([shared_data["req253_input"]])
    shared_data["req253_after"] = schemas[0]


@then(
    "the in-memory GraphQL schema is regenerated and fresh introspection is "
    "returned on the next request"
)
def req253_assert_fresh(shared_data):
    before_sdl = print_schema(shared_data["req253_before"])
    after = shared_data["req253_after"]
    after_sdl = print_schema(after)

    # Regenerated schema differs from the pre-change one (new naming applied).
    assert before_sdl != after_sdl
    # snake baseline had order_items; graphql-default converts to orderItems.
    assert "order_items" in before_sdl.lower() or "orderItems" in before_sdl
    assert "orderItems" in after_sdl

    # Fresh introspection resolves against the regenerated in-memory schema.
    data = _simulate_introspection_query(after)
    type_names = {t["name"] for t in data["__schema"]["types"]}
    # The camelCase-derived PascalCase type name is present after rebuild.
    assert any("OrderItem" in n for n in type_names)


# ---------------------------------------------------------------------------
# REQ-403: RLS injection table-level precedence
# ---------------------------------------------------------------------------


@given("a table with both table-specific and domain-level RLS rules")
def req403_rules(shared_data):
    shared_data["req403_rls"] = build_rls_context(
        [
            {
                "table_id": 1,
                "domain_id": None,
                "role_id": "analyst",
                "filter_expr": "owner_id = current_user",
            },
            {
                "table_id": None,
                "domain_id": "sales",
                "role_id": "analyst",
                "filter_expr": "region = 'us'",
            },
        ],
        "analyst",
    )
    meta = _make_rls_meta(domain_id="sales")
    shared_data["req403_ctx"] = _make_rls_ctx(meta)
    shared_data["req403_compiled"] = _make_rls_compiled()


@when("inject_rls() runs")
def req403_run(shared_data):
    shared_data["req403_result"] = inject_rls(
        shared_data["req403_compiled"],
        shared_data["req403_ctx"],
        shared_data["req403_rls"],
    )


@then("table-specific rules take precedence over domain-level rules")
def req403_assert(shared_data):
    sql = shared_data["req403_result"].sql
    # Table-specific filter is applied.
    assert "owner_id = current_user" in sql
    # Domain-level filter is NOT applied (table rule wins, no fallback).
    assert "region = 'us'" not in sql


# ---------------------------------------------------------------------------
# REQ-409: Cypher datetime coercion
# ---------------------------------------------------------------------------


@given("a Cypher WHERE clause with an ISO 8601 datetime string literal")
def req409_where(shared_data):
    shared_data["req409_where"] = "e.created_at = '2024-01-15T00:00:00'"


@when("the translator processes it")
def req409_process(shared_data):
    shared_data["req409_out"] = _coerce_ts_literals(shared_data["req409_where"])


@then("it wraps the literal as TIMESTAMP '...' before SQLGlot parsing")
def req409_assert(shared_data):
    out = shared_data["req409_out"]
    assert out == "e.created_at = TIMESTAMP '2024-01-15T00:00:00'"
    assert "TIMESTAMP '2024-01-15T00:00:00'" in out

    # The coerced literal must be parseable by SQLGlot as a real TIMESTAMP.
    import sqlglot

    parsed = sqlglot.parse_one(f"SELECT * FROM t WHERE {out}", read="trino")
    assert "TIMESTAMP" in parsed.sql(dialect="trino").upper()

    # A plain non-datetime string literal is left untouched.
    assert _coerce_ts_literals("e.name = 'alice'") == "e.name = 'alice'"


# ---------------------------------------------------------------------------
# REQ-525: Per-role proto generation
# ---------------------------------------------------------------------------


@given("two roles with different table and column visibility")
def req525_roles(shared_data):
    from provisa.compiler import naming as _naming

    _naming.configure(gql="snake")
    shared_data["req525_admin_input"] = _make_default_schema_input("admin")
    shared_data["req525_analyst_input"] = _make_default_schema_input("analyst")


@when("proto definitions are generated")
def req525_generate(shared_data):
    shared_data["req525_admin_proto"] = generate_proto(
        shared_data["req525_admin_input"]
    )
    shared_data["req525_analyst_proto"] = generate_proto(
        shared_data["req525_analyst_input"]
    )


@then("each role receives a proto reflecting only its visible tables and columns")
def req525_assert(shared_data):
    admin = shared_data["req525_admin_proto"]
    analyst = shared_data["req525_analyst_proto"]

    # Both protos are valid proto3 documents.
    assert 'syntax = "proto3";' in admin
    assert 'syntax = "proto3";' in analyst

    # Admin-only columns appear in the admin proto but not the analyst proto.
    assert "amount" in admin
    assert "amount" not in analyst
    assert "email" in admin
    assert "email" not in analyst

    # Columns visible to both roles are present in both protos.
    assert "customer_id" in admin
    assert "customer_id" in analyst


# ---------------------------------------------------------------------------
# REQ-534: Multi-root GraphQL query compilation
# ---------------------------------------------------------------------------


@given("a GraphQL query with multiple root fields")
def req534_query(shared_data):
    from provisa.compiler import naming as _naming

    _naming.configure(gql="snake")
    si = _make_req009_schema_input()
    shared_data["req534_ctx"] = build_context(si)
    shared_data["req534_doc"] = parse("{ orders { id } customers { name } }")


@when("it is executed")
def req534_execute(shared_data):
    shared_data["req534_results"] = compile_query(
        shared_data["req534_doc"], shared_data["req534_ctx"]
    )


@then(
    "each root field is compiled and executed independently and results are "
    "merged into one response"
)
def req534_assert(shared_data):
    results = shared_data["req534_results"]

    # One independent CompiledQuery per root field.
    assert len(results) == 2
    roots = {r.root_field for r in results}
    assert roots == {"orders", "customers"}

    orders = next(r for r in results if r.root_field == "orders")
    customers = next(r for r in results if r.root_field == "customers")

    # Each root field compiles to its own independent SQL statement.
    assert '"orders"' in orders.sql
    assert '"customers"' not in orders.sql
    assert '"customers"' in customers.sql
    assert '"orders"' not in customers.sql

    # Each is a standalone single-statement query (mergeable into one response
    # by root_field key).
    assert orders.sql.strip().upper().startswith("SELECT")
    assert customers.sql.strip().upper().startswith("SELECT")


# ---------------------------------------------------------------------------
# REQ-537: Schema version endpoint
# ---------------------------------------------------------------------------


@given("the schema is rebuilt after a naming convention change")
def req537_rebuild(shared_data):
    import uuid

    import provisa.api.data.sdl as _sdl

    class _FakeState:
        schema_boot_id: str = ""
        schema_version: int = 0

    state = _FakeState()
    state.schema_boot_id = uuid.uuid4().hex
    state.schema_version = 0
    setattr(_sdl, "state", state)
    shared_data["req537_state"] = state
    shared_data["req537_sdl_module"] = _sdl

    # Capture the version before the rebuild-driven increment.
    shared_data["req537_before"] = _call_schema_version(_sdl)

    # A naming-convention rebuild bumps the monotonic counter (mirrors
    # app._rebuild_schemas: state.schema_version += 1).
    state.schema_version += 1


def _call_schema_version(sdl_module) -> str:
    import asyncio
    import json

    resp = asyncio.run(sdl_module.get_schema_version())
    return json.loads(resp.body)["version"]


@when("GET /data/schema-version is called")
def req537_call(shared_data):
    shared_data["req537_after"] = _call_schema_version(
        shared_data["req537_sdl_module"]
    )


@then("it returns a new <boot-id>-<counter> string reflecting the rebuild")
def req537_assert(shared_data):
    state = shared_data["req537_state"]
    before = shared_data["req537_before"]
    after = shared_data["req537_after"]

    # Format is <boot-id>-<counter>.
    assert after == f"{state.schema_boot_id}-{state.schema_version}"
    assert after.startswith(state.schema_boot_id + "-")
    assert after.rsplit("-", 1)[1] == str(state.schema_version)

    # The rebuild produced a strictly new version string.
    assert after != before
    assert int(after.rsplit("-", 1)[1]) == int(before.rsplit("-", 1)[1]) + 1

    # Clean up the module-level state we injected.
    del shared_data["req537_sdl_module"].state


# ---------------------------------------------------------------------------
# REQ-654: group_by root query field
# ---------------------------------------------------------------------------


@given("a registered table with numeric columns")
def req654_table(shared_data):
    si = _make_group_by_schema_input()
    shared_data["req654_schema"] = generate_schema(si)
    shared_data["req654_ctx"] = build_context(si)


@when(
    "a _group_by query is submitted with by: [category] and aggregate count"
)
def req654_submit(shared_data):
    # 'region' is the categorical/grouping column on the orders table.
    doc = parse(
        """
        query {
            orders_group_by(by: [region]) {
                groupKey
                aggregate { count }
            }
        }
        """
    )
    shared_data["req654_compiled"] = compile_query(
        doc, shared_data["req654_ctx"], variables=None
    )


@then("the response contains one GroupByRow per distinct category value")
def req654_assert_group(shared_data):
    compiled = shared_data["req654_compiled"]
    assert len(compiled) == 1
    sql = compiled[0].sql
    # GROUP BY the categorical column yields one row per distinct value.
    assert 'GROUP BY "region"' in sql


@then("each row includes groupKey and aggregates fields")
def req654_assert_fields(shared_data):
    schema = shared_data["req654_schema"]
    compiled = shared_data["req654_compiled"]

    # Schema: GroupByRow exposes groupKey + aggregate fields.
    field = schema.query_type.fields["orders_group_by"]
    row_type = field.type.of_type.of_type.of_type
    assert "groupKey" in row_type.fields
    assert "aggregate" in row_type.fields

    # Compiled SQL projects the group key and the aggregate under their
    # respective nested containers.
    col_refs = compiled[0].columns
    group_key_refs = [c for c in col_refs if c.nested_in == "groupKey"]
    agg_refs = [c for c in col_refs if c.nested_in == "aggregate"]
    assert any(c.field_name == "region" for c in group_key_refs)
    assert any(c.field_name == "count" for c in agg_refs)
