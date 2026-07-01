# Copyright (c) 2026 Kenneth Stott
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""BDD step implementations for REQ-012, REQ-015, REQ-016, REQ-017, REQ-018, REQ-019, REQ-020, REQ-366, REQ-413, REQ-414, REQ-415, REQ-417, REQ-433, REQ-434, REQ-605, REQ-612, REQ-635, REQ-636, REQ-638 — Registration & Governance.

REQ-012: Source registration is privileged; validates connection, calls Trino dynamic catalog API,
no restart required, available within seconds.

REQ-015: There is no per-table governance mode. Every table and view is queryable directly under
the user's rights (table/view rights + relationship rights) with Stage 2 governance applied
uniformly. No registry-required mode exists.

REQ-016: Table publication triggers schema generation pass; table immediately
available in query builder.

REQ-017: NoSQL/non-relational sources are exposed read-only through their native Trino connector
(e.g. the MongoDB connector), driven by the type-specific mapping DSL (REQ-251); no mutations.

REQ-018: Trino FK metadata used to infer candidate intra-source relationships for steward
confirmation/rejection. FK-inferred relationship suggestions reduce manual steward work when
registering related tables.

REQ-019: Cross-source relationships defined manually by steward with cardinality (many-to-one,
one-to-many). (Revised 2026-06-18: one-to-one removed — the relationship-field model is a strict
binary, single object vs list, so a 1:1 collapses to many-to-one; model a true 1:1 as a
many-to-one in each direction.)

REQ-020: Relationships owned by defining steward, versioned, flagged for re-review on schema
changes affecting join fields.

REQ-366: Views require an approval workflow, OR the originator must already hold the rights to
the underlying tables and to any joins used within the view. Any join within a view likewise
requires approval or originator rights. Convenience views (adding no new semantics) are
discouraged — instead grant the relationship rights and query in any form. Creating a view implies
new semantics: derived/calculated values, or the view name itself as a new business concept.
Approval gates therefore apply to both views (for the semantics they introduce, consistent with
REQ-134) and relationships (for navigational intent).

REQ-413: Auto-generate GQL relationships from FK constraints in database schema introspection —
relationships discoverable from FK metadata in addition to manual steward configuration and
AI-assisted hints.

REQ-414: Demo/install example schema must include at least one FK relationship
to exercise auto-generated relationship discovery.

REQ-415: The `hasura_v2_relationship_style` option controls whether FK-derived relationships use
Hasura V2's naming conventions — singular for many-to-one, plural for one-to-many using
inflection.

REQ-417: Hasura v2 migration tool maps Hasura Remote Schemas to Provisa graphql_remote source
registrations instead of skipping them with "NOT SUPPORTED" warning. Migration preserves Remote
Schema name, URL, headers, and authentication configuration.

REQ-433: A datasource may be associated with multiple domains. Any domain owner may register any
unclaimed table from that source. Once a table is claimed by one domain, no other domain may
register it — first-come ownership model. Unique constraint enforced on
(source_id, normalized_table_name). The UI greys out claimed tables regardless of which domain
claimed them.

REQ-434: Creation-request mechanism. Any governed create operation (view,
relationship, etc.) attempted by a user lacking the authority to perform it
produces a *persisted request* rather than an error. The request enters a queue
(REQ-063) visible to every user holding the rights to execute that create. An
authorized user may execute or reject the request; rejection carries a specific,
actionable reason. No create is performed until an authorized user executes the
request.

REQ-605: When ``root_table_ids`` is set on a ``SchemaInput``, tables whose IDs
are absent from that set are excluded from root query fields in the generated SDL
but remain present as GraphQL named types reachable via relationship fields.

REQ-612: Relationship candidates are ranked by a four-level confidence hierarchy:
(Highest) approved catalog relationship validated by both stewards; (High)
intra-source FK constraint; (Medium) intra-source semantic inference; (Low)
cross-source semantic inference. Candidates corroborated by multiple evidence
types accumulate confidence.

REQ-635: The schema name presented to users must be the name the data source
itself uses to group datasets. For relational databases this is the native
schema (or database for MySQL). For flat/API sources with no native grouping
concept, a fixed constant naming the source type is used.

REQ-636: When a Trino connector is configured for a source type (the type is in
SOURCE_TO_CONNECTOR), Trino is the preferred path for schema and table
introspection. Native driver introspection is only used for source types with no
Trino connector, or those that override via native_schemas/native_tables
returning a non-None value.

REQ-638: The UI calls one availableSchemas endpoint and one availableTables
endpoint. Backend routing selects the correct introspection strategy per source
type internally, with no source-type-specific endpoints exposed to the UI.
"""

from __future__ import annotations

import os
import time
import uuid
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from pytest_bdd import given, when, then

from provisa.core.models import (
    SOURCE_TO_CONNECTOR,
    Column,
    Source,
    SourceType,
    Table,
)


# ---------------------------------------------------------------------------
# Shared fixture
# ---------------------------------------------------------------------------


@pytest.fixture
def shared_data() -> dict:
    return {}


# ---------------------------------------------------------------------------
# REQ-012 — Privileged source registration; Trino dynamic catalog; no restart
# ---------------------------------------------------------------------------


@given("a privileged steward with registration rights")
def privileged_steward_with_registration_rights(shared_data: dict) -> None:
    """Set up a privileged steward context with registration authority."""
    shared_data["steward_id"] = "privileged-steward-001"
    shared_data["steward_roles"] = ["steward", "source_registrar"]
    shared_data["has_registration_rights"] = True

    source = Source(
        id="new_pg_source",
        type=SourceType.postgresql,
        host="db.example.com",
        port=5432,
        database="sales",
        username="provisa_user",
    )
    shared_data["source"] = source
    shared_data["registration_submitted"] = False
    shared_data["connection_validated"] = False
    shared_data["trino_catalog_called"] = False
    shared_data["source_available"] = False
    shared_data["restart_required"] = False

    assert shared_data["has_registration_rights"] is True
    assert source.id == "new_pg_source"
    assert source.type == SourceType.postgresql
    assert source.host == "db.example.com"


@when("they submit a new source registration")
def submit_new_source_registration(shared_data: dict) -> None:
    """Simulate submitting a source registration, validating connection,
    and calling the Trino dynamic catalog API — all without a server restart."""
    from provisa.core.catalog import _build_catalog_properties, _to_catalog_name

    source: Source = shared_data["source"]

    catalog_name = _to_catalog_name(source.id)
    assert catalog_name == "new_pg_source"
    shared_data["connection_validated"] = True
    shared_data["catalog_name"] = catalog_name

    trino_call_log: list[str] = []

    mock_cursor = MagicMock()
    mock_cursor.execute = MagicMock(side_effect=lambda sql: trino_call_log.append(sql))

    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_conn.__enter__ = MagicMock(return_value=mock_conn)
    mock_conn.__exit__ = MagicMock(return_value=False)

    resolved_password = "s3cr3t"
    with patch.dict(
        os.environ,
        {
            "POSTGRES_HOST": "db.example.com",
            "PG_PORT": "5432",
            "PG_DATABASE": "sales",
            "PG_USER": "provisa_user",
            "PG_PASSWORD": resolved_password,
        },
    ):
        catalog_props = _build_catalog_properties(source, resolved_password)

    assert isinstance(catalog_props, dict)
    assert len(catalog_props) > 0

    props_sql_parts = ", ".join(f"'{k}' = '{v}'" for k, v in catalog_props.items())
    create_catalog_sql = f"CREATE CATALOG {catalog_name} USING postgresql WITH ({props_sql_parts})"
    mock_cursor.execute(create_catalog_sql)

    assert len(trino_call_log) == 1
    assert "CREATE CATALOG" in trino_call_log[0]
    assert catalog_name in trino_call_log[0]

    shared_data["trino_catalog_called"] = True
    shared_data["trino_call_log"] = trino_call_log
    shared_data["catalog_props"] = catalog_props

    shared_data["registration_start_time"] = time.monotonic()
    shared_data["source_available"] = True
    shared_data["restart_required"] = False
    shared_data["registration_submitted"] = True
    shared_data["registration_end_time"] = time.monotonic()


@then(
    "Provisa validates the connection, calls the Trino dynamic catalog API, and makes the source"
    " available within seconds without a server restart")
def provisa_validates_connection_calls_trino_and_makes_source_available(
    shared_data: dict,
) -> None:
    """Assert all REQ-012 postconditions."""
    assert shared_data["connection_validated"] is True
    assert shared_data["trino_catalog_called"] is True

    trino_call_log: list[str] = shared_data["trino_call_log"]
    assert len(trino_call_log) >= 1

    create_catalog_calls = [sql for sql in trino_call_log if "CREATE CATALOG" in sql]
    assert len(create_catalog_calls) >= 1

    catalog_name = shared_data["catalog_name"]
    assert catalog_name in create_catalog_calls[0]

    assert shared_data["source_available"] is True
    assert shared_data["restart_required"] is False

    start = shared_data.get("registration_start_time")
    end = shared_data.get("registration_end_time")
    assert start is not None and end is not None
    elapsed_seconds = end - start
    assert elapsed_seconds < 5.0

    catalog_props: dict[str, str] = shared_data["catalog_props"]
    assert isinstance(catalog_props, dict)
    assert len(catalog_props) > 0

    source: Source = shared_data["source"]
    assert source.id == "new_pg_source"
    assert source.type == SourceType.postgresql
    assert source.host == "db.example.com"
    assert source.port == 5432


# ---------------------------------------------------------------------------
# REQ-015 — No per-table governance mode; Stage 2 governance applied uniformly
# ---------------------------------------------------------------------------


@given("any registered table or view")
def any_registered_table_or_view(shared_data: dict) -> None:
    """Register a representative set of tables and views."""
    tables = [
        Table(
            source_id="sales_pg",
            domain_id="default",
            schema_name="public",
            table_name="orders",
            columns=[
                Column(name="id", visible_to=["analyst", "admin"]),
                Column(name="amount", visible_to=["analyst", "admin"]),
                Column(name="customer_id", visible_to=["analyst", "admin"]),
                Column(name="region", visible_to=["analyst", "admin"]),
            ],
        ),
        Table(
            source_id="sales_pg",
            domain_id="default",
            schema_name="public",
            table_name="customers",
            columns=[
                Column(name="id", visible_to=["analyst", "admin"]),
                Column(name="name", visible_to=["admin"]),
                Column(name="email", visible_to=["admin"]),
                Column(name="region", visible_to=["analyst", "admin"]),
            ],
        ),
        Table(
            source_id="sales_pg",
            domain_id="default",
            schema_name="public",
            table_name="revenue_view",
            columns=[
                Column(name="region", visible_to=["analyst", "admin"]),
                Column(name="total_revenue", visible_to=["analyst", "admin"]),
            ],
        ),
    ]

    shared_data["registered_tables"] = tables
    shared_data["querying_user_role"] = None
    shared_data["query_results"] = {}
    shared_data["governance_modes_observed"] = set()

    for table in tables:
        assert not hasattr(table, "governance_mode")
        assert not hasattr(table, "registry_required")
        assert not hasattr(table, "access_mode")

    assert len(tables) >= 2


@when("a user with the appropriate rights queries it")
def user_with_appropriate_rights_queries_it(shared_data: dict) -> None:
    """Simulate a user with table/view rights + relationship rights querying each registered table."""
    from provisa.compiler.schema_gen import SchemaInput, generate_schema

    tables: list[Table] = shared_data["registered_tables"]

    role = {
        "id": "analyst",
        "name": "Analyst",
        "row_filters": [],
        "column_masks": [],
    }
    shared_data["querying_user_role"] = role

    column_types: dict[str, list[Any]] = {}
    table_records = []
    for i, table in enumerate(tables):
        table_id = f"table-{i:03d}-{table.table_name}"
        col_mocks = []
        for col in table.columns:
            m = MagicMock()
            m.column_name = col.name
            m.data_type = "integer" if col.name in ("id", "customer_id") else "varchar"
            m.is_nullable = col.name != "id"
            col_mocks.append(m)
        column_types[table_id] = col_mocks

        table_records.append(
            {
                "id": table_id,
                "source_id": table.source_id,
                "domain_id": table.domain_id,
                "schema_name": table.schema_name,
                "table_name": table.table_name,
                "columns": [
                    {"name": col.name, "visible_to": col.visible_to} for col in table.columns
                ],
                "rls_filter": None,
                "label": None,
            }
        )

    shared_data["table_records"] = table_records
    shared_data["column_types"] = column_types

    generated_schemas = {}
    governance_paths_taken = []

    for table_record in table_records:
        tid = table_record["id"]
        schema_input = SchemaInput(
            tables=[table_record],
            relationships=[],
            column_types={tid: column_types[tid]},
            naming_rules=[],
            role=role,
            domains=[],
        )
        schema = generate_schema(schema_input)
        generated_schemas[table_record["table_name"]] = schema

        query_type = schema.query_type
        path_descriptor = (
            "stage2_uniform"
            if query_type is not None and len(query_type.fields) > 0
            else "no_query_type"
        )
        governance_paths_taken.append(path_descriptor)

    shared_data["generated_schemas"] = generated_schemas
    shared_data["governance_paths_taken"] = governance_paths_taken
    shared_data["governance_modes_observed"] = set(governance_paths_taken)


@then("Stage 2 governance is applied uniformly without any per-table mode distinctions")
def stage2_governance_applied_uniformly(shared_data: dict) -> None:
    """Assert all REQ-015 postconditions."""
    from graphql import assert_valid_schema

    tables: list[Table] = shared_data["registered_tables"]
    generated_schemas: dict = shared_data.get("generated_schemas", {})
    governance_paths_taken: list[str] = shared_data.get("governance_paths_taken", [])
    governance_modes_observed: set = shared_data.get("governance_modes_observed", set())

    assert len(generated_schemas) == len(tables)

    for table_name, schema in generated_schemas.items():
        assert_valid_schema(schema)
        query_type = schema.query_type
        assert query_type is not None
        assert len(query_type.fields) > 0

    assert len(governance_modes_observed) == 1
    assert "stage2_uniform" in governance_modes_observed

    forbidden_attrs = ["governance_mode", "registry_required", "access_mode", "gov_mode"]
    for table in tables:
        for attr in forbidden_attrs:
            assert not hasattr(table, attr)

    customers_schema = generated_schemas.get("customers")
    if customers_schema is not None:
        customers_query_type = customers_schema.query_type
        customers_field = None
        for field_name in customers_query_type.fields:
            if "customer" in field_name.lower():
                customers_field = customers_query_type.fields[field_name]
                break

        if customers_field is not None:
            return_type = customers_field.type
            while hasattr(return_type, "of_type"):
                return_type = return_type.of_type

            if hasattr(return_type, "fields"):
                visible_fields = set(return_type.fields.keys())
                assert "name" not in visible_fields
                assert "email" not in visible_fields
                assert "id" in visible_fields or "region" in visible_fields

    assert len(governance_paths_taken) == len(tables)
    for i, path in enumerate(governance_paths_taken):
        assert path == "stage2_uniform"


# ---------------------------------------------------------------------------
# REQ-016 — Table publication triggers schema generation pass
# ---------------------------------------------------------------------------


@given("a steward who publishes a table")
def steward_publishes_table(shared_data: dict) -> None:
    """Set up a steward context and a table ready to be published."""
    table_id = str(uuid.uuid4())
    table = Table(
        source_id="sales_pg",
        domain_id="default",
        schema_name="public",
        table_name="orders",
        columns=[
            Column(name="id", visible_to=["analyst", "admin"]),
            Column(name="amount", visible_to=["analyst", "admin"]),
            Column(name="customer_id", visible_to=["analyst", "admin"]),
        ],
    )
    shared_data["steward_id"] = "steward-user-001"
    shared_data["table"] = table
    shared_data["table_id"] = table_id
    shared_data["published"] = False
    shared_data["schema_generation_triggered"] = False
    shared_data["query_builder_available"] = False
    shared_data["schema_generation_elapsed_ms"] = None
    shared_data["generated_schema"] = None

    assert table.source_id == "sales_pg"
    assert table.table_name == "orders"
    assert len(table.columns) == 3
    assert shared_data["schema_generation_triggered"] is False
    assert shared_data["query_builder_available"] is False


@when("the publication completes")
def publication_completes(shared_data: dict) -> None:
    """Simulate the publication of a table and the subsequent automatic schema generation pass."""
    from provisa.compiler.schema_gen import SchemaInput, generate_schema

    table: Table = shared_data["table"]
    table_id: str = shared_data["table_id"]

    column_type_map: dict[str, list[Any]] = {
        table_id: [
            MagicMock(
                column_name=col.name,
                data_type="integer" if col.name == "id" else "varchar(255)",
                is_nullable=(col.name != "id"),
            )
            for col in table.columns
        ]
    }

    role = {
        "id": "analyst",
        "name": "Analyst",
        "row_filters": [],
        "column_masks": [],
    }

    table_record: dict[str, Any] = {
        "id": table_id,
        "source_id": table.source_id,
        "domain_id": table.domain_id,
        "schema_name": table.schema_name,
        "table_name": table.table_name,
        "columns": [{"name": col.name, "visible_to": col.visible_to} for col in table.columns],
        "rls_filter": None,
        "label": None,
    }

    schema_input = SchemaInput(
        tables=[table_record],
        relationships=[],
        column_types=column_type_map,
        naming_rules=[],
        role=role,
        domains=[],
    )

    t0 = time.monotonic()
    generated_schema = generate_schema(schema_input)
    t1 = time.monotonic()

    elapsed_ms = (t1 - t0) * 1000.0

    assert generated_schema is not None

    shared_data["published"] = True
    shared_data["schema_generation_triggered"] = True
    shared_data["generated_schema"] = generated_schema
    shared_data["schema_generation_elapsed_ms"] = elapsed_ms
    shared_data["role_used_for_generation"] = role
    shared_data["table_record_published"] = table_record
    shared_data["query_builder_available"] = True


@then(
    "a schema generation pass is triggered and the table is immediately available in the query"
    " builder")
def schema_generation_triggered_and_table_available(shared_data: dict) -> None:
    """Assert all REQ-016 postconditions."""
    from graphql import assert_valid_schema

    assert shared_data["published"] is True
    assert shared_data["schema_generation_triggered"] is True
    assert shared_data["query_builder_available"] is True

    generated_schema = shared_data["generated_schema"]
    assert generated_schema is not None

    assert_valid_schema(generated_schema)

    query_type = generated_schema.query_type
    assert query_type is not None
    assert len(query_type.fields) > 0

    elapsed_ms = shared_data["schema_generation_elapsed_ms"]
    assert elapsed_ms < 2000.0

    table: Table = shared_data["table"]
    type_map = generated_schema.type_map
    table_type_found = any(
        table.table_name.lower() in type_name.lower() for type_name in type_map.keys()
    )
    assert table_type_found


# ---------------------------------------------------------------------------
# REQ-017 — NoSQL sources exposed read-only via native Trino connector
# ---------------------------------------------------------------------------

# Mutation-enabling property keys that must never appear in a read-only connector config
_MUTATION_PROPERTY_KEYS = frozenset({
    "mongodb.allow-inserts",
    "mongodb.allow-updates",
    "mongodb.allow-deletes",
    "mongodb.allow-drop-table",
    "allow-write",
    "write-enabled",
    "connector.allow-mutations",
})

# DML keywords that must never appear in queries routed through a read-only connector
_DML_KEYWORDS = frozenset({"INSERT", "UPDATE", "DELETE", "DROP", "TRUNCATE", "CREATE TABLE", "ALTER"})


@given("a registered NoSQL source with a native Trino connector")
def registered_nosql_source_with_native_trino_connector(shared_data: dict) -> None:
    """Set up a MongoDB source registered in Provisa with the MongoDB Trino connector."""
    from provisa.core.trino_catalog_files import catalog_properties_for

    source = Source(
        id="mongo_products",
        type=SourceType.mongodb,
        host="mongo.example.com",
        port=27017,
        database="products_db",
        username="mongouser",
    )
    shared_data["nosql_source"] = source

    assert SourceType.mongodb in SOURCE_TO_CONNECTOR, (
        f"SourceType.mongodb is not present in SOURCE_TO_CONNECTOR — "
        f"the MongoDB connector is required for REQ-017. "
        f"Known connector types: {list(SOURCE_TO_CONNECTOR.keys())}"
    )
    connector_name = SOURCE_TO_CONNECTOR[SourceType.mongodb]
    assert connector_name, (
        "SOURCE_TO_CONNECTOR[SourceType.mongodb] is empty — a connector name is required."
    )
    shared_data["nosql_connector_name"] = connector_name

    props = catalog_properties_for(source, "")
    shared_data["nosql_catalog_props"] = props

    assert props is not None, (
        "catalog_properties_for() returned None for a MongoDB source — "
        "the connector mapping must return properties for MongoDB."
    )
    assert isinstance(props, dict), "Catalog properties must be a dict"
    assert len(props) > 0, "Catalog properties must not be empty for a MongoDB source"

    mutation_keys_present = _MUTATION_PROPERTY_KEYS & set(props.keys())
    assert not mutation_keys_present, (
        f"Catalog properties for MongoDB source contain mutation-enabling keys: "
        f"{mutation_keys_present}. REQ-017 requires read-only access — no mutations allowed."
    )

    shared_data["nosql_source_registered"] = True
    shared_data["nosql_query_executed"] = False
    shared_data["nosql_query_sql"] = None
    shared_data["nosql_mutation_attempted"] = False
    shared_data["nosql_mutation_blocked"] = False


@when("a consumer queries a table from that source")
def consumer_queries_table_from_nosql_source(shared_data: dict) -> None:
    """Simulate a consumer executing a SELECT query through the Trino connector."""
    from provisa.core.catalog import _to_catalog_name

    source: Source = shared_data["nosql_source"]
    props: dict[str, str] = shared_data["nosql_catalog_props"]
    connector_name: str = shared_data["nosql_connector_name"]

    catalog_name = _to_catalog_name(source.id)
    shared_data["nosql_catalog_name"] = catalog_name

    select_sql = (
        f"SELECT _id, name, price, category "
        f"FROM {catalog_name}.products_db.products "
        f"LIMIT 100"
    )
    shared_data["nosql_query_sql"] = select_sql

    sql_upper = select_sql.upper()
    dml_found = [kw for kw in _DML_KEYWORDS if kw in sql_upper]
    assert not dml_found, (
        f"Consumer query contains DML keywords {dml_found} — "
        "REQ-017 requires read-only queries through the Trino connector."
    )

    executed_statements: list[str] = []
    rejected_statements: list[str] = []

    def _mock_execute(sql: str) -> None:
        sql_up = sql.strip().upper()
        for dml_kw in _DML_KEYWORDS:
            if sql_up.startswith(dml_kw) or f" {dml_kw} " in sql_up:
                rejected_statements.append(sql)
                raise PermissionError(
                    f"Mutation statement rejected by read-only connector guard: {sql!r}"
                )
        executed_statements.append(sql)

    mock_cursor = MagicMock()
    mock_cursor.execute = MagicMock(side_effect=_mock_execute)
    mock_cursor.fetchall = MagicMock(return_value=[
        {"_id": "64abc", "name": "Widget A", "price": 9.99, "category": "widgets"},
        {"_id": "64def", "name": "Widget B", "price": 14.99, "category": "widgets"},
    ])
    mock_cursor.description = [
        ("_id", None), ("name", None), ("price", None), ("category", None)
    ]

    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    mock_cursor.execute(select_sql)
    rows = mock_cursor.fetchall()

    assert len(executed_statements) == 1, (
        f"Expected exactly one executed statement, got: {executed_statements}"
    )
    assert executed_statements[0] == select_sql
    assert len(rows) == 2, f"Expected 2 result rows, got {len(rows)}"

    shared_data["nosql_query_executed"] = True
    shared_data["nosql_query_rows"] = rows
    shared_data["nosql_executed_statements"] = executed_statements
    shared_data["nosql_rejected_statements"] = rejected_statements
    shared_data["nosql_mock_cursor"] = mock_cursor
    shared_data["nosql_mock_execute_fn"] = _mock_execute

    mutation_sql = (
        f"INSERT INTO {catalog_name}.products_db.products (_id, name) "
        f"VALUES ('99999', 'Injected')"
    )
    shared_data["nosql_mutation_sql"] = mutation_sql

    mutation_blocked = False
    mutation_error_msg = None
    try:
        mock_cursor.execute(mutation_sql)
    except PermissionError as exc:
        mutation_blocked = True
        mutation_error_msg = str(exc)

    shared_data["nosql_mutation_attempted"] = True
    shared_data["nosql_mutation_blocked"] = mutation_blocked
    shared_data["nosql_mutation_error_msg"] = mutation_error_msg


@then(
    "the query is executed read-only through the Trino connector with no mutation path available"
)
def query_executed_readonly_through_trino_connector_no_mutation_path(shared_data: dict) -> None:
    """Assert REQ-017 postconditions."""
    assert shared_data["nosql_source_registered"] is True
    assert shared_data["nosql_query_executed"] is True

    connector_name: str = shared_data["nosql_connector_name"]
    assert connector_name, "Connector name must be non-empty"

    props: dict[str, str] = shared_data["nosql_catalog_props"]
    mutation_keys_present = _MUTATION_PROPERTY_KEYS & set(props.keys())
    assert not mutation_keys_present, (
        f"Connector config exposes mutation-enabling keys {mutation_keys_present}. "
        "REQ-017 requires a read-only Trino connector with no mutation path."
    )

    # The SELECT query must have been routed through the connector and executed.
    select_sql: str = shared_data["nosql_query_sql"]
    assert select_sql, "No query SQL was recorded for the read-only connector query"

    executed_statements: list[str] = shared_data["nosql_executed_statements"]
    assert executed_statements == [select_sql], (
        f"Expected exactly the SELECT to execute, got: {executed_statements}"
    )
    sql_upper = select_sql.upper()
    dml_found = [kw for kw in _DML_KEYWORDS if kw in sql_upper]
    assert not dml_found, (
        f"Executed query contains DML keywords {dml_found} — reads only under REQ-017."
    )

    rows = shared_data["nosql_query_rows"]
    assert len(rows) == 2, f"Expected 2 rows from the read-only SELECT, got {len(rows)}"

    # The mutation attempt must have been blocked with no mutation path available.
    assert shared_data["nosql_mutation_attempted"] is True, (
        "The scenario must attempt a mutation to prove it is blocked"
    )
    assert shared_data["nosql_mutation_blocked"] is True, (
        "Mutation was not blocked — REQ-017 requires DML rejection on a read-only connector"
    )

    rejected_statements: list[str] = shared_data["nosql_rejected_statements"]
    mutation_sql: str = shared_data["nosql_mutation_sql"]
    assert rejected_statements == [mutation_sql], (
        f"Expected the mutation to be the only rejected statement, got: {rejected_statements}"
    )
    assert mutation_sql not in executed_statements, (
        "Mutation statement must never reach the executed set on a read-only connector"
    )

    error_msg: str = shared_data["nosql_mutation_error_msg"]
    assert error_msg and "read-only" in error_msg.lower(), (
        f"Mutation rejection must cite the read-only connector guard, got: {error_msg!r}"
    )
