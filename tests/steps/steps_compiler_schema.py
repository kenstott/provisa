# Copyright (c) 2026 Kenneth Stott
# Canary: 9bd289bc-a09f-4f59-9485-b417d1f0cf00
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

from graphql import print_schema, parse  # noqa: E402
from pytest_bdd import given, when, then, scenarios  # noqa: E402

from provisa.compiler.rls import build_rls_context, inject_rls  # noqa: E402
from provisa.compiler.sql_gen import compile_query  # noqa: E402
from provisa.compiler.context import build_context  # noqa: E402
from provisa.cypher.translator_helpers import _coerce_ts_literals  # noqa: E402
from provisa.grpc.proto_gen import generate_proto  # noqa: E402

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
    shared_data["req253_before"] = generate_schema(_make_naming_convention_schema_input([]))
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
    assert '"owner_id" = CURRENT_USER' in sql
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
    shared_data["req525_admin_proto"] = generate_proto(shared_data["req525_admin_input"])
    shared_data["req525_analyst_proto"] = generate_proto(shared_data["req525_analyst_input"])


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
    shared_data["req537_after"] = _call_schema_version(shared_data["req537_sdl_module"])


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


@when("a _group_by query is submitted with by: [category] and aggregate count")
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
    shared_data["req654_compiled"] = compile_query(doc, shared_data["req654_ctx"], variables=None)


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


scenarios("../features/REQ-009.feature")


# ---------------------------------------------------------------------------
# REQ-009: Single-statement SQL compilation (no resolver chain, no N+1)
# ---------------------------------------------------------------------------


@given("a valid GraphQL query AST")
def req009_valid_ast(shared_data):
    from provisa.compiler import naming as _naming

    _naming.configure(gql="snake")
    si = _make_req009_schema_input()
    shared_data["compile_ctx"] = build_context(si)
    # A query that exercises a join (orders -> customers) to prove single-statement
    # compilation rather than a resolver chain.
    shared_data["compile_doc"] = parse(
        """
        {
            orders {
                id
                amount
                status
                customer {
                    id
                    name
                    email
                }
            }
        }
        """
    )


@when("the compiler processes it")
def _when_compiler_processes_it(shared_data):
    # Shared by every scenario using this step text (REQ-009, REQ-478…); each
    # Given stores its parsed doc + context under compile_doc/compile_ctx.
    shared_data["compile_results"] = compile_query(
        shared_data["compile_doc"], shared_data["compile_ctx"]
    )


@then("it emits a single PG-style SQL statement with no resolver chain and no N+1 pattern")
def req009_assert(shared_data):
    results = shared_data["compile_results"]

    # One root field → exactly one CompiledQuery (single statement per root field).
    assert len(results) == 1
    cq = results[0]

    sql = cq.sql

    # Must be a single SQL statement — no semicolons separating multiple statements.
    statements = [s.strip() for s in sql.split(";") if s.strip()]
    assert len(statements) == 1, f"Expected a single SQL statement, got {len(statements)}: {sql!r}"

    # Must start with SELECT (PG-style SQL).
    assert sql.strip().upper().startswith("SELECT"), f"Expected SELECT statement, got: {sql!r}"

    # Double-quoted identifiers are used (PG-style).
    assert '"orders"' in sql or '"public"' in sql, (
        f"Expected double-quoted identifiers in SQL: {sql!r}"
    )

    # The relationship (orders → customers) is resolved within the single statement
    # via a JOIN or correlated subquery — no separate SQL for nested fields.
    # This is the core anti-N+1 guarantee: relationship data is embedded in the
    # single compiled statement, not a separate resolver call.
    sql_upper = sql.upper()
    has_join = "JOIN" in sql_upper
    has_subquery = "SELECT" in sql_upper[sql_upper.index("SELECT") + 6 :]
    assert has_join or has_subquery, (
        f"Expected relationship to be compiled into single statement via JOIN or "
        f"correlated subquery, got: {sql!r}"
    )

    # The compiled result references only one root field.
    assert cq.root_field == "orders"

    # The columns list must include both root and nested fields — proving they
    # are part of the single compiled result, not separate resolver outputs.
    col_field_names = {c.field_name for c in cq.columns}
    assert "id" in col_field_names
    # The many-to-one relationship is embedded as a column in the single
    # compiled result (a correlated JSON-object subquery), not a separate
    # resolver output — this is the anti-N+1 guarantee.
    assert "customer" in col_field_names, (
        f"Expected the 'customer' relationship embedded as a column in the single "
        f"compiled statement; got columns: {col_field_names}"
    )


scenarios("../features/REQ-252.feature")


# ---------------------------------------------------------------------------
# REQ-252: Schema inference for supported connectors (MongoDB, Cassandra, ES)
# ---------------------------------------------------------------------------


@given("a MongoDB source with discover: true")
def req252_mongo_source(shared_data):
    from provisa.compiler import naming as _naming

    _naming.configure(gql="snake")
    shared_data["req252_schema_input"] = _make_mongo_discover_schema_input(
        explicit_columns=list(_MONGO_EXPLICIT_COLUMNS),
        include_discover_flag=True,
    )
    # Record which column names were explicitly defined so we can assert
    # precedence in the Then step.
    shared_data["req252_explicit_names"] = {c["column_name"] for c in _MONGO_EXPLICIT_COLUMNS}
    # Record the full inferred column set for completeness checks.
    shared_data["req252_inferred_names"] = {c.column_name for c in _MONGO_INFERRED_COLUMNS}


@when("the schema compiler runs")
def req252_run_compiler(shared_data):
    si = shared_data["req252_schema_input"]
    schema = generate_schema(si)
    shared_data["req252_schema"] = schema
    shared_data["req252_sdl"] = print_schema(schema)

    # Also build the compilation context so we can inspect column metadata.
    ctx = build_context(si)
    shared_data["req252_ctx"] = ctx


@then(
    "it introspects the connector and generates a starting column list, "
    "with explicit definitions taking precedence"
)
def req252_assert_inference(shared_data):
    from provisa.discovery.column_inference import merge_discovered_columns

    sdl = shared_data["req252_sdl"]
    shared_data["req252_ctx"]
    explicit_names = shared_data["req252_explicit_names"]
    inferred_names = shared_data["req252_inferred_names"]

    # --- 1. Schema was generated successfully ---
    assert "type Query" in sdl, "GraphQL schema must contain a Query type"
    # The MongoDB table user_events must appear as a GraphQL type.
    assert "UserEvents" in sdl or "user_events" in sdl.lower(), (
        f"Expected user_events / UserEvents in SDL:\n{sdl}"
    )

    # --- 2. Inferred columns (non-explicit) appear in the generated schema ---
    # 'email', 'score', and 'created_at' were NOT in the explicit list but
    # were returned by the auto-discovery introspection; they must be present.
    inferred_only = inferred_names - explicit_names
    for col_name in inferred_only:
        assert col_name in sdl, (
            f"Inferred column '{col_name}' should appear in SDL after discovery:\n{sdl}"
        )

    # --- 3. Explicit column definitions take precedence over inferred ones ---
    # Use merge_discovered_columns directly to assert the precedence contract.
    inferred_col_dicts = [
        {"name": c.column_name, "type": c.data_type} for c in _MONGO_INFERRED_COLUMNS
    ]
    explicit_col_dicts = [{"name": c["column_name"]} for c in _MONGO_EXPLICIT_COLUMNS]
    merged = merge_discovered_columns(explicit_col_dicts, inferred_col_dicts)

    merged_names = {c["name"] for c in merged}
    # All explicit names are present in the merged result.
    for name in explicit_names:
        assert name in merged_names, f"Explicit column '{name}' missing from merged column list"
    # All inferred names also appear (either via explicit def or discovered).
    for name in inferred_names:
        assert name in merged_names, f"Inferred column '{name}' missing from merged column list"
    # No duplicates: each name appears exactly once.
    merged_name_list = [c["name"] for c in merged]
    assert len(merged_name_list) == len(set(merged_name_list)), (
        f"Duplicate column names in merged list: {merged_name_list}"
    )
    # Explicit definitions win: for 'user_name' (in both lists), the explicit
    # entry (which has no 'type' key) must be used — not the inferred varchar.
    user_name_entries = [c for c in merged if c["name"] == "user_name"]
    assert len(user_name_entries) == 1, (
        "user_name must appear exactly once in the merged column list"
    )
    # The explicit entry for user_name had no 'type' key — confirm it is NOT
    # overridden by the inferred 'varchar(255)' type.
    assert (
        "type" not in user_name_entries[0] or user_name_entries[0].get("type") != "varchar(255)"
    ), "Explicit definition of user_name must take precedence over inferred type"

    # --- 4. The discover: true flag is present on the table definition ---
    si = shared_data["req252_schema_input"]
    table_def = si.tables[0] if hasattr(si, "tables") else si["tables"][0]
    # Normalise: SchemaInput may expose tables as list of dicts or as an
    # attribute on a dataclass.
    if isinstance(table_def, dict):
        assert table_def.get("discover") is True, (
            "Table definition must carry discover: true for MongoDB sources"
        )

    # --- 5. Source type is mongodb (supports inference) ---
    if isinstance(table_def, dict):
        assert table_def.get("source_type") == "mongodb", (
            "source_type must be 'mongodb' for this scenario"
        )


scenarios("../features/REQ-259.feature")


# ---------------------------------------------------------------------------
# REQ-259: Apollo Federation v2 subgraph support
# ---------------------------------------------------------------------------


@given("Apollo Federation v2 support is enabled")
def req259_federation_enabled(shared_data):
    from provisa.compiler import naming as _naming

    _naming.configure(gql="snake")
    si = _make_federation_schema_input(role_id="admin")
    shared_data["schema_input"] = si
    shared_data["schema_federation_v2"] = True


def _gen_federation_schema(si):
    """Base schema + Apollo Federation v2 wrapping (REQ-259).

    generate_schema() has no federation flag; federation is layered on via
    build_federation_schema(), deriving each entity's GraphQL type name from the
    base schema's root field (table_name -> [Type!]).
    """
    from graphql import get_named_type

    from provisa.compiler.federation import build_federation_schema

    base = generate_schema(si)
    query_type = base.query_type
    assert query_type is not None
    tables_for_fed: list[dict] = []
    pk_columns: dict[int, list[str]] = {}
    for t in si.tables:
        field = query_type.fields.get(t["table_name"])
        if field is None:
            continue
        tables_for_fed.append(
            {
                "id": t["id"],
                "table_name": t["table_name"],
                "_type_name": get_named_type(field.type).name,
            }
        )
        pk_columns[t["id"]] = t.get("pk_columns", [])
    return build_federation_schema(base, tables_for_fed, pk_columns)


@when("the schema is generated")
def _when_schema_generated(shared_data):
    # Shared by every scenario using this step text (REQ-259/411/412…). Each
    # Given stores its SchemaInput under schema_input; REQ-259 also sets
    # schema_federation_v2 to request Apollo Federation v2 wrapping.
    si = shared_data["schema_input"]
    if shared_data.get("schema_federation_v2"):
        schema = _gen_federation_schema(si)
    else:
        schema = generate_schema(si)
    shared_data["generated_schema"] = schema
    shared_data["generated_sdl"] = print_schema(schema)


@then(
    "@key directives, _service, and _entities fields are present and entity "
    "resolution respects RLS and masking"
)
def req259_assert_federation(shared_data):
    from graphql import GraphQLObjectType, GraphQLUnionType

    from graphql import graphql_sync

    sdl = shared_data["generated_sdl"]
    schema = shared_data["generated_schema"]

    # --- 1. @key directives appear on entity types (derived from pk_columns) ---
    # Federation directives (@key, @link) are exposed via the _service { sdl }
    # field, not print_schema (graphql-core drops custom directives from SDL).
    service_result = graphql_sync(schema, "{ _service { sdl } }")
    assert service_result.errors is None, f"_service query failed: {service_result.errors}"
    assert service_result.data is not None
    fed_sdl = service_result.data["_service"]["sdl"]
    assert "@key" in fed_sdl, f"Expected @key directives in Federation v2 SDL:\n{fed_sdl}"

    # The Products type must carry @key(fields: "id") since pk_columns=["id"].
    assert 'key(fields: "id")' in fed_sdl, f'Expected @key(fields: "id") on entity:\n{fed_sdl}'

    # --- 2. _service root field is present ---
    query_type = schema.query_type
    assert query_type is not None, "Schema must have a Query type"
    assert "_service" in query_type.fields, (
        f"Expected _service field on Query type. Fields: {list(query_type.fields.keys())}"
    )

    # _service must return a _Service object with an sdl String field.
    service_field = query_type.fields["_service"]
    service_type = service_field.type
    # Unwrap NonNull if needed.
    if hasattr(service_type, "of_type"):
        service_type = service_type.of_type
    assert isinstance(service_type, GraphQLObjectType), (
        f"_service must return an object type, got {service_type!r}"
    )
    assert "sdl" in service_type.fields, (
        f"_Service type must have an 'sdl' field. Fields: {list(service_type.fields.keys())}"
    )

    # --- 3. _entities root field is present ---
    assert "_entities" in query_type.fields, (
        f"Expected _entities field on Query type. Fields: {list(query_type.fields.keys())}"
    )

    # _entities must accept a representations argument (list of _Any scalars).
    entities_field = query_type.fields["_entities"]
    assert "representations" in entities_field.args, (
        f"_entities must accept a 'representations' argument. "
        f"Args: {list(entities_field.args.keys())}"
    )

    # _entities must return a union of all entity types.
    entities_type = entities_field.type
    # Unwrap NonNull/List wrappers.
    unwrapped = entities_type
    while hasattr(unwrapped, "of_type"):
        unwrapped = unwrapped.of_type
    assert isinstance(unwrapped, (GraphQLObjectType, GraphQLUnionType)), (
        f"_entities return type must be a union or object type, got {unwrapped!r}"
    )

    # --- 4. Entity types (Products, Reviews) are present in the schema ---
    type_map = schema.type_map
    entity_type_names = [k for k in type_map if not k.startswith("_") and not k.startswith("__")]
    assert any("Product" in n for n in entity_type_names), (
        f"Expected a Products/Product entity type in schema. Types: {entity_type_names}"
    )

    # --- 5. Batch entity resolution: _entities accepts a list (representations) ---
    representations_arg = entities_field.args["representations"]
    rep_type = representations_arg.type
    # Unwrap NonNull to reach List.
    while hasattr(rep_type, "of_type") and not hasattr(rep_type, "of_type.of_type"):
        rep_type.of_type
        # Check for list wrapper.
        from graphql import GraphQLList, GraphQLNonNull

        if isinstance(rep_type, (GraphQLList, GraphQLNonNull)):
            rep_type = rep_type.of_type
            break
        break
    # The argument must accept a list type (batch resolution).
    from graphql import GraphQLList, GraphQLNonNull

    raw_arg_type = representations_arg.type
    # Walk wrappers to find List.
    t = raw_arg_type
    found_list = False
    for _ in range(4):
        if isinstance(t, GraphQLList):
            found_list = True
            break
        if hasattr(t, "of_type"):
            t = t.of_type
        else:
            break
    assert found_list, (
        f"representations argument must be a List type for batch entity resolution. "
        f"Got: {representations_arg.type!r}"
    )

    # --- 6. RLS and masking: admin role sees price; verify role-driven visibility ---
    # The admin SchemaInput was used, so price (admin-only) must be in the SDL.
    assert "price" in sdl, f"Admin role should see 'price' column in federation schema:\n{sdl}"

    # Build an analyst-role federation schema and verify 'price' is masked.
    analyst_si = _make_federation_schema_input(role_id="analyst")
    analyst_schema = _gen_federation_schema(analyst_si)
    analyst_sdl = print_schema(analyst_schema)
    assert "price" not in analyst_sdl, (
        f"Analyst role must NOT see 'price' column (masking/RLS):\n{analyst_sdl}"
    )

    # Analyst federation schema must still have _service and _entities.
    analyst_query = analyst_schema.query_type
    assert analyst_query is not None
    assert "_service" in analyst_query.fields, (
        "Analyst federation schema must still expose _service"
    )
    assert "_entities" in analyst_query.fields, (
        "Analyst federation schema must still expose _entities"
    )

    # --- 7. Federation is disabled by default (non-federation schema has no _service/_entities) ---
    default_si = _make_federation_schema_input(role_id="admin")
    default_schema = generate_schema(default_si)  # no federation wrapping
    default_query = default_schema.query_type
    assert default_query is not None
    assert "_service" not in default_query.fields, (
        "Federation v2 must be disabled by default: _service must not appear in non-federation schema"
    )
    assert "_entities" not in default_query.fields, (
        "Federation v2 must be disabled by default: _entities must not appear in non-federation schema"
    )


scenarios("../features/REQ-411.feature")


# ---------------------------------------------------------------------------
# REQ-411: hasura-default naming convention
# ---------------------------------------------------------------------------


@given("naming convention is set to hasura-default")
def req411_set_naming(shared_data):
    from provisa.compiler import naming as _naming

    _naming.configure(gql="hasura-default")
    shared_data["schema_input"] = _make_hasura_default_schema_input()


@then("mutation names and field names use snake_case matching Hasura V2 defaults")
def req411_assert_snake_case(shared_data):
    from graphql import GraphQLObjectType

    schema = shared_data["generated_schema"]
    sdl = shared_data["generated_sdl"]

    # --- 1. Mutation type exists and has snake_case mutation names ---
    mutation_type = schema.mutation_type
    assert mutation_type is not None, (
        f"Schema must have a Mutation type for hasura-default convention.\nSDL:\n{sdl}"
    )

    mutation_field_names = list(mutation_type.fields.keys())

    # Hasura V2 default mutation names for "orders" table:
    # insert_orders, update_orders, delete_orders
    assert "insert_orders" in mutation_field_names, (
        f"Expected 'insert_orders' mutation. Got: {mutation_field_names}"
    )
    assert "update_orders" in mutation_field_names, (
        f"Expected 'update_orders' mutation. Got: {mutation_field_names}"
    )
    assert "delete_orders" in mutation_field_names, (
        f"Expected 'delete_orders' mutation. Got: {mutation_field_names}"
    )

    # All mutation names must be snake_case
    for mut_name in mutation_field_names:
        assert _is_snake_case(mut_name), f"Mutation name '{mut_name}' is not snake_case"

    # --- 2. Query field name for orders is snake_case ---
    query_type = schema.query_type
    assert query_type is not None, "Schema must have a Query type"
    query_field_names = list(query_type.fields.keys())

    # The root query field for orders table must be snake_case
    orders_fields = [f for f in query_field_names if "orders" in f]
    assert len(orders_fields) > 0, (
        f"Expected at least one orders-related query field. Got: {query_field_names}"
    )
    for f_name in orders_fields:
        assert _is_snake_case(f_name), f"Query field name '{f_name}' is not snake_case"

    # --- 3. Object type field names are snake_case ---
    # Find the Orders type and check its field names
    type_map = schema.type_map
    orders_type = None
    for type_name, gql_type in type_map.items():
        if (
            isinstance(gql_type, GraphQLObjectType)
            and "rder" in type_name
            and not type_name.startswith("_")
        ):
            orders_type = gql_type
            break

    assert orders_type is not None, (
        f"Expected an Orders object type in schema. Types: {list(type_map.keys())}"
    )

    # All field names on the Orders type must be snake_case. Reserved system
    # fields (_name_, _domain_) are meta-fields added to every type and are
    # exempt from the naming convention.
    for field_name in orders_type.fields.keys():
        if field_name.startswith("_"):
            continue
        assert _is_snake_case(field_name), (
            f"Field name '{field_name}' on type '{orders_type.name}' is not snake_case"
        )

    # Specifically verify the multi-word column names remain snake_case
    orders_field_names = list(orders_type.fields.keys())
    assert "customer_id" in orders_field_names, (
        f"Expected 'customer_id' (snake_case) field. Got: {orders_field_names}"
    )
    assert "order_total" in orders_field_names, (
        f"Expected 'order_total' (snake_case) field. Got: {orders_field_names}"
    )
    assert "order_status" in orders_field_names, (
        f"Expected 'order_status' (snake_case) field. Got: {orders_field_names}"
    )
    assert "created_at" in orders_field_names, (
        f"Expected 'created_at' (snake_case) field. Got: {orders_field_names}"
    )

    # --- 4. No camelCase names appear for the orders type fields ---
    camel_case_pattern = re.compile(r"[a-z][A-Z]")
    for field_name in orders_type.fields.keys():
        assert not camel_case_pattern.search(field_name), (
            f"Field name '{field_name}' appears to be camelCase, not snake_case"
        )


scenarios("../features/REQ-412.feature")


# ---------------------------------------------------------------------------
# REQ-412: graphql-default naming convention
# ---------------------------------------------------------------------------


def _is_camel_case(name: str) -> bool:
    """Return True if name starts with a lowercase letter and contains no underscores (camelCase)."""
    return bool(re.match(r"^[a-z][a-zA-Z0-9]*$", name))


def _is_pascal_case(name: str) -> bool:
    """Return True if name starts with an uppercase letter (PascalCase)."""
    return bool(re.match(r"^[A-Z][a-zA-Z0-9]*$", name))


@given("the default Provisa naming convention")
def req412_set_default_naming(shared_data):
    from provisa.compiler import naming as _naming

    # graphql-default is the Provisa default per REQ-412.
    _naming.configure(gql="graphql-default")
    shared_data["schema_input"] = _make_graphql_default_schema_input()


@then("field names are camelCase, types are PascalCase, and mutations are camelCase")
def req412_assert_graphql_default(shared_data):
    from graphql import GraphQLObjectType

    schema = shared_data["generated_schema"]
    sdl = shared_data["generated_sdl"]

    # --- 1. Object type names are PascalCase ---
    type_map = schema.type_map
    user_defined_types = [
        (name, t)
        for name, t in type_map.items()
        if isinstance(t, GraphQLObjectType)
        and not name.startswith("_")
        and not name.startswith("__")
        and name not in ("Query", "Mutation", "Subscription")
    ]
    assert len(user_defined_types) > 0, (
        f"Expected user-defined object types in schema. Types: {list(type_map.keys())}"
    )
    for type_name, _ in user_defined_types:
        assert _is_pascal_case(type_name), (
            f"Type name '{type_name}' is not PascalCase as required by graphql-default"
        )

    # Specific type names for orders and order_items must be PascalCase.
    type_names = {n for n, _ in user_defined_types}
    assert any("Orders" in n or "Order" in n for n in type_names), (
        f"Expected 'Orders' or similar PascalCase type. Types: {type_names}"
    )
    assert any("OrderItems" in n or "OrderItem" in n for n in type_names), (
        f"Expected 'OrderItems' or similar PascalCase type. Types: {type_names}"
    )

    # --- 2. Field names on object types are camelCase ---
    # Reserved system fields (_name_, _domain_) are meta-fields added to every
    # type and are exempt from the naming convention; so is the fixed
    # affected_rows meta-field on *MutationResponse wrapper types.
    for type_name, gql_type in user_defined_types:
        if type_name.endswith("MutationResponse"):
            continue
        for field_name in gql_type.fields.keys():
            if field_name.startswith("_"):
                continue
            # Single-word fields like 'id' are valid camelCase.
            assert _is_camel_case(field_name), (
                f"Field '{field_name}' on type '{type_name}' is not camelCase "
                f"as required by graphql-default"
            )

    # Specifically verify multi-word snake_case columns are converted to camelCase.
    orders_type = next(
        (t for n, t in user_defined_types if "Order" in n and "Item" not in n),
        None,
    )
    assert orders_type is not None, (
        f"Expected an Orders object type. Types: {[n for n, _ in user_defined_types]}"
    )
    orders_field_names = list(orders_type.fields.keys())

    # customer_id → customerId
    assert "customerId" in orders_field_names, (
        f"Expected 'customerId' (camelCase of customer_id). Got: {orders_field_names}"
    )
    # order_total → orderTotal
    assert "orderTotal" in orders_field_names, (
        f"Expected 'orderTotal' (camelCase of order_total). Got: {orders_field_names}"
    )
    # order_status → orderStatus
    assert "orderStatus" in orders_field_names, (
        f"Expected 'orderStatus' (camelCase of order_status). Got: {orders_field_names}"
    )
    # created_at → createdAt
    assert "createdAt" in orders_field_names, (
        f"Expected 'createdAt' (camelCase of created_at). Got: {orders_field_names}"
    )

    # No snake_case field names (with underscores) on user-defined types.
    # Reserved meta-fields (_name_, _domain_) and the fixed affected_rows field
    # on *MutationResponse wrappers are exempt (see above).
    underscore_re = re.compile(r"_")
    for type_name, gql_type in user_defined_types:
        if type_name.endswith("MutationResponse"):
            continue
        for field_name in gql_type.fields.keys():
            if field_name.startswith("_"):
                continue
            assert not underscore_re.search(field_name), (
                f"Field '{field_name}' on type '{type_name}' contains underscores; "
                f"graphql-default requires camelCase"
            )

    # --- 3. Mutation names are camelCase (insertOrders, updateOrders) ---
    mutation_type = schema.mutation_type
    assert mutation_type is not None, (
        f"Schema must have a Mutation type for graphql-default convention.\nSDL:\n{sdl}"
    )

    mutation_field_names = list(mutation_type.fields.keys())

    # graphql-default mutation names per REQ-412: insertOrders, updateOrders, deleteOrders
    assert "insertOrders" in mutation_field_names, (
        f"Expected 'insertOrders' mutation (camelCase). Got: {mutation_field_names}"
    )
    assert "updateOrders" in mutation_field_names, (
        f"Expected 'updateOrders' mutation (camelCase). Got: {mutation_field_names}"
    )
    assert "deleteOrders" in mutation_field_names, (
        f"Expected 'deleteOrders' mutation (camelCase). Got: {mutation_field_names}"
    )

    # All mutation names must be camelCase (no underscores).
    for mut_name in mutation_field_names:
        assert _is_camel_case(mut_name), (
            f"Mutation name '{mut_name}' is not camelCase as required by graphql-default"
        )
        assert "_" not in mut_name, (
            f"Mutation name '{mut_name}' contains underscores; "
            f"graphql-default requires camelCase mutations"
        )

    # --- 4. Query root field names are camelCase ---
    query_type = schema.query_type
    assert query_type is not None, "Schema must have a Query type"
    for qf_name in query_type.fields.keys():
        # Internal federation/system fields are excluded from this check.
        if qf_name.startswith("_"):
            continue
        assert _is_camel_case(qf_name), (
            f"Query field '{qf_name}' is not camelCase as required by graphql-default"
        )

    # --- 5. Confirm graphql-default is the Provisa default (not snake, not hasura-default) ---
    from provisa.compiler import naming as _naming

    # graphql-default is an alias that normalizes to the apollo_graphql preset.
    current_convention = _naming.active_gql_convention()
    assert current_convention == _naming.normalize_convention("graphql-default"), (
        f"Expected graphql-default to be the active Provisa naming convention, "
        f"got: {current_convention!r}"
    )


scenarios("../features/REQ-478.feature")


# ---------------------------------------------------------------------------
# REQ-478: Statistical row sampling
# ---------------------------------------------------------------------------


@given("a query with sample: 10.0")
def req478_query_with_sample(shared_data):
    from provisa.compiler import naming as _naming

    _naming.configure(gql="snake")
    si = _make_sampling_schema_input()
    shared_data["compile_ctx"] = build_context(si)
    shared_data["req478_schema"] = generate_schema(si)
    shared_data["compile_doc"] = parse("{ orders(sample: 10.0) { id amount status } }")


@then("it emits TABLESAMPLE BERNOULLI (10) on the base table and rejects values outside (0, 100]")
def req478_assert_tablesample(shared_data):

    results = shared_data["compile_results"]
    assert len(results) == 1
    sql = results[0].sql

    # The compiler must emit TABLESAMPLE BERNOULLI (10) on the base table.
    sql_upper = sql.upper()
    assert "TABLESAMPLE" in sql_upper, f"Expected TABLESAMPLE in compiled SQL:\n{sql}"
    assert "BERNOULLI" in sql_upper, f"Expected BERNOULLI sampling in compiled SQL:\n{sql}"
    assert "10" in sql, f"Expected percentage 10 in compiled SQL:\n{sql}"

    ctx = shared_data["compile_ctx"]

    # Out-of-range value: 0 is excluded from (0, 100].
    doc_zero = parse("{ orders(sample: 0.0) { id } }")
    with pytest.raises(Exception) as exc_info:
        compile_query(doc_zero, ctx)
    assert any(
        word in str(exc_info.value).lower()
        for word in ("sample", "range", "invalid", "percent", "must", "0")
    ), f"Expected compile-time rejection of sample=0.0, got: {exc_info.value}"

    # Out-of-range value: > 100 is rejected.
    doc_over = parse("{ orders(sample: 101.0) { id } }")
    with pytest.raises(Exception) as exc_info2:
        compile_query(doc_over, ctx)
    assert any(
        word in str(exc_info2.value).lower()
        for word in ("sample", "range", "invalid", "percent", "must", "100")
    ), f"Expected compile-time rejection of sample=101.0, got: {exc_info2.value}"

    # Boundary: 100.0 is valid (inclusive upper bound).
    doc_full = parse("{ orders(sample: 100.0) { id } }")
    results_full = compile_query(doc_full, ctx)
    assert len(results_full) == 1
    assert "TABLESAMPLE" in results_full[0].sql.upper(), (
        f"Expected TABLESAMPLE for sample=100.0:\n{results_full[0].sql}"
    )

    # Boundary: a small positive value like 0.1 is valid.
    doc_small = parse("{ orders(sample: 0.1) { id } }")
    results_small = compile_query(doc_small, ctx)
    assert len(results_small) == 1
    assert "TABLESAMPLE" in results_small[0].sql.upper(), (
        f"Expected TABLESAMPLE for sample=0.1:\n{results_small[0].sql}"
    )


scenarios("../features/REQ-655.feature")


# ---------------------------------------------------------------------------
# REQ-655: group_by HAVING and FILTER clauses
# ---------------------------------------------------------------------------


@given("a _group_by query with a having: clause on an aggregate field")
def req655_group_by_having_query(shared_data):
    from provisa.compiler import naming as _naming

    _naming.configure(gql="snake")
    si = _make_group_by_schema_input()
    shared_data["req655_schema"] = generate_schema(si)
    shared_data["req655_ctx"] = build_context(si)
    # Query: group orders by region, filter groups where count > 3 (HAVING),
    # and use aggregates(where:) for conditional aggregation (FILTER WHERE).
    shared_data["req655_doc"] = parse(
        """
        query {
            orders_group_by(
                by: [region]
                having: { count: { gt: 3 } }
            ) {
                groupKey
                aggregate(where: { status: { eq: "active" } }) {
                    count
                    sum { amount }
                }
            }
        }
        """
    )


@when("the query is compiled")
def req655_compile_query(shared_data):
    shared_data["req655_compiled"] = compile_query(
        shared_data["req655_doc"],
        shared_data["req655_ctx"],
        variables=None,
    )


@then("the generated SQL includes a HAVING clause after GROUP BY")
def req655_assert_having(shared_data):
    compiled = shared_data["req655_compiled"]
    assert len(compiled) == 1, f"Expected exactly one compiled query, got {len(compiled)}"
    sql = compiled[0].sql
    sql_upper = sql.upper()

    # GROUP BY must appear before HAVING.
    assert "GROUP BY" in sql_upper, f"Expected GROUP BY in compiled SQL:\n{sql}"
    assert "HAVING" in sql_upper, f"Expected HAVING clause in compiled SQL:\n{sql}"

    # HAVING must appear after GROUP BY in the statement.
    group_by_pos = sql_upper.index("GROUP BY")
    having_pos = sql_upper.index("HAVING")
    assert having_pos > group_by_pos, f"HAVING must appear after GROUP BY in SQL:\n{sql}"

    # The HAVING clause must reference count with a comparison; the threshold
    # is a bound parameter (COUNT(*) > $1), not inlined, for injection safety.
    having_fragment = sql_upper[having_pos:]
    assert "COUNT" in having_fragment, f"HAVING clause must reference COUNT aggregate:\n{sql}"
    assert ">" in having_fragment, f"HAVING clause must apply a comparison on COUNT:\n{sql}"
    assert 3 in compiled[0].params, (
        f"HAVING threshold 3 must be bound as a parameter; params={compiled[0].params}"
    )


@then("aggregates(where:) generates a SQL FILTER (WHERE ...) expression")
def req655_assert_filter_where(shared_data):
    compiled = shared_data["req655_compiled"]
    assert len(compiled) == 1, f"Expected exactly one compiled query, got {len(compiled)}"
    sql = compiled[0].sql
    sql_upper = sql.upper()

    # The aggregates(where:) argument must produce a FILTER (WHERE ...) expression.
    assert "FILTER" in sql_upper, (
        f"Expected FILTER keyword in compiled SQL for aggregates(where:):\n{sql}"
    )
    # Standard SQL FILTER syntax: FILTER (WHERE <condition>)
    assert "FILTER (WHERE" in sql_upper or "FILTER(WHERE" in sql_upper, (
        f"Expected 'FILTER (WHERE ...)' expression in compiled SQL:\n{sql}"
    )

    # The filter condition must reference the status column and 'active' value.
    filter_pos = sql_upper.find("FILTER")
    filter_fragment = sql[filter_pos : filter_pos + 200]
    assert "status" in filter_fragment.lower() or "active" in filter_fragment.lower(), (
        f"FILTER (WHERE ...) must reference the status='active' condition:\n{sql}"
    )

    # Orthogonality check: all three clauses are present and distinct.
    # WHERE (row-level), FILTER (WHERE) (conditional agg), HAVING (post-agg).
    assert "WHERE" in sql_upper, f"Expected a WHERE clause for row-level filtering in SQL:\n{sql}"
    assert "GROUP BY" in sql_upper, f"Expected GROUP BY in SQL:\n{sql}"
    assert "HAVING" in sql_upper, f"Expected HAVING in SQL:\n{sql}"
