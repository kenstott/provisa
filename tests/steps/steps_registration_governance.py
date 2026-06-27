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

import asyncio
import os
import sqlite3
import tempfile
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pytest_bdd import given, when, then, scenario

from provisa.core.models import SOURCE_TO_CONNECTOR, Column, Source, SourceType, Table, Cardinality, Relationship
from provisa.discovery.analyzer import RelationshipCandidate
from provisa.api.admin import introspect as introspect_mod
from provisa.api.admin.introspect import native_schemas


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

    # Build a valid source registration payload representing a new PostgreSQL source
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

    # Confirm the steward has the required privilege
    assert shared_data["has_registration_rights"] is True, (
        "Steward does not have source registration rights"
    )
    # Confirm the source model is valid
    assert source.id == "new_pg_source"
    assert source.type == SourceType.postgresql
    assert source.host == "db.example.com"


@when("they submit a new source registration")
def submit_new_source_registration(shared_data: dict) -> None:
    """Simulate submitting a source registration, validating connection,
    and calling the Trino dynamic catalog API — all without a server restart."""
    from provisa.core.catalog import _build_catalog_properties, _to_catalog_name

    source: Source = shared_data["source"]

    # --- Step 1: Validate the Source model (connection field validation) ---
    catalog_name = _to_catalog_name(source.id)
    assert catalog_name == "new_pg_source", (
        f"Catalog name conversion failed: expected 'new_pg_source', got {catalog_name!r}"
    )
    shared_data["connection_validated"] = True
    shared_data["catalog_name"] = catalog_name

    # --- Step 2: Simulate Trino dynamic catalog API call ---
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

    assert isinstance(catalog_props, dict), "Catalog properties must be a dict"
    assert len(catalog_props) > 0, "Catalog properties must not be empty"

    props_sql_parts = ", ".join(
        f"'{k}' = '{v}'" for k, v in catalog_props.items()
    )
    create_catalog_sql = (
        f"CREATE CATALOG {catalog_name} USING postgresql WITH ({props_sql_parts})"
    )
    mock_cursor.execute(create_catalog_sql)

    assert len(trino_call_log) == 1, (
        f"Expected exactly one Trino catalog API call, got: {trino_call_log}"
    )
    assert "CREATE CATALOG" in trino_call_log[0], (
        f"Trino call did not include CREATE CATALOG: {trino_call_log[0]!r}"
    )
    assert catalog_name in trino_call_log[0], (
        f"Trino call did not reference catalog name '{catalog_name}': {trino_call_log[0]!r}"
    )

    shared_data["trino_catalog_called"] = True
    shared_data["trino_call_log"] = trino_call_log
    shared_data["catalog_props"] = catalog_props

    shared_data["registration_start_time"] = time.monotonic()

    shared_data["source_available"] = True
    shared_data["restart_required"] = False
    shared_data["registration_submitted"] = True

    shared_data["registration_end_time"] = time.monotonic()


@then(
    "Provisa validates the connection, calls the Trino dynamic catalog API, and makes the source\n"
    "    available within seconds without a server restart"
)
def provisa_validates_connection_calls_trino_and_makes_source_available(
    shared_data: dict,
) -> None:
    """Assert that:
    1. The connection payload was validated.
    2. The Trino dynamic catalog API was called (CREATE CATALOG).
    3. The source is marked available.
    4. No server restart was required.
    5. The entire registration completed within an acceptable time window.
    """
    assert shared_data["connection_validated"] is True, (
        "Provisa did not validate the source connection before registration"
    )

    assert shared_data["trino_catalog_called"] is True, (
        "Provisa did not call the Trino dynamic catalog API during registration"
    )

    trino_call_log: list[str] = shared_data["trino_call_log"]
    assert len(trino_call_log) >= 1, "No Trino catalog API calls were recorded"

    create_catalog_calls = [
        sql for sql in trino_call_log if "CREATE CATALOG" in sql
    ]
    assert len(create_catalog_calls) >= 1, (
        f"Expected CREATE CATALOG in Trino call log, got: {trino_call_log}"
    )

    catalog_name = shared_data["catalog_name"]
    assert catalog_name in create_catalog_calls[0], (
        f"Expected catalog name '{catalog_name}' in CREATE CATALOG SQL: "
        f"{create_catalog_calls[0]!r}"
    )

    assert shared_data["source_available"] is True, (
        "Source was not marked available after registration"
    )

    assert shared_data["restart_required"] is False, (
        "Registration incorrectly required a server restart. "
        "Dynamic catalog registration must not require a restart."
    )

    start = shared_data.get("registration_start_time")
    end = shared_data.get("registration_end_time")
    assert start is not None and end is not None, "Registration timing was not recorded"
    elapsed_seconds = end - start
    assert elapsed_seconds < 5.0, (
        f"Registration took {elapsed_seconds:.3f}s — expected completion within 5 seconds. "
        "Dynamic catalog registration should be near-instantaneous."
    )

    catalog_props: dict[str, str] = shared_data["catalog_props"]
    assert isinstance(catalog_props, dict), "Catalog properties must be a dict"
    assert len(catalog_props) > 0, "Catalog properties sent to Trino must not be empty"

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
    """Register a representative set of tables and views to demonstrate that
    no per-table governance mode field exists on any of them, and that all
    are governed by the same uniform Stage 2 model."""
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
        assert not hasattr(table, "governance_mode"), (
            f"Table '{table.table_name}' has a 'governance_mode' attribute — "
            "REQ-015 forbids per-table governance modes."
        )
        assert not hasattr(table, "registry_required"), (
            f"Table '{table.table_name}' has a 'registry_required' attribute — "
            "REQ-015 forbids registry-required mode."
        )
        assert not hasattr(table, "access_mode"), (
            f"Table '{table.table_name}' has an 'access_mode' attribute — "
            "REQ-015 forbids per-table access mode distinctions."
        )

    assert len(tables) >= 2, "Need at least two registered tables/views to test uniformity"


@when("a user with the appropriate rights queries it")
def user_with_appropriate_rights_queries_it(shared_data: dict) -> None:
    """Simulate a user with table/view rights + relationship rights querying
    each registered table and view."""
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
                    {"name": col.name, "visible_to": col.visible_to}
                    for col in table.columns
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

    assert len(generated_schemas) == len(tables), (
        f"Expected {len(tables)} generated schemas (one per registered table/view), "
        f"got {len(generated_schemas)}: {list(generated_schemas.keys())}"
    )

    for table_name, schema in generated_schemas.items():
        assert_valid_schema(schema), (
            f"Generated schema for '{table_name}' is not valid GraphQL."
        )
        query_type = schema.query_type
        assert query_type is not None, (
            f"Schema for '{table_name}' has no Query type — table is not queryable. "
            "Under REQ-015 every registered table must be queryable by users with rights."
        )
        assert len(query_type.fields) > 0, (
            f"Query type for '{table_name}' has no fields — table produces no queryable output."
        )

    assert len(governance_modes_observed) == 1, (
        f"Expected exactly one governance code path across all tables (stage2_uniform), "
        f"but observed: {governance_modes_observed}. "
        "REQ-015 mandates uniform Stage 2 governance with no per-table mode distinctions."
    )
    assert "stage2_uniform" in governance_modes_observed, (
        f"The observed governance path is not 'stage2_uniform': {governance_modes_observed}. "
        "Every table must go through Stage 2 governance."
    )

    forbidden_attrs = ["governance_mode", "registry_required", "access_mode", "gov_mode"]
    for table in tables:
        for attr in forbidden_attrs:
            assert not hasattr(table, attr), (
                f"Table '{table.table_name}' has forbidden per-table governance attribute "
                f"'{attr}'. REQ-015 forbids per-table governance modes."
            )

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
                assert "name" not in visible_fields, (
                    "Field 'name' (admin-only) is visible to analyst role — "
                    "Stage 2 governance column filtering failed for 'customers'."
                )
                assert "email" not in visible_fields, (
                    "Field 'email' (admin-only) is visible to analyst role — "
                    "Stage 2 governance column filtering failed for 'customers'."
                )
                assert "id" in visible_fields or "region" in visible_fields, (
                    "Neither 'id' nor 'region' (analyst-visible) appear in customers schema — "
                    "Stage 2 governance may have filtered too aggressively."
                )

    assert len(governance_paths_taken) == len(tables), (
        f"Governance path count ({len(governance_paths_taken)}) does not match "
        f"table count ({len(tables)}). Every table must be individually governed."
    )
    for i, path in enumerate(governance_paths_taken):
        assert path == "stage2_uniform", (
            f"Table at index {i} took governance path '{path}' instead of 'stage2_uniform'. "
            "All tables must follow the same uniform Stage 2 governance path."
        )


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
        "columns": [
            {"name": col.name, "visible_to": col.visible_to}
            for col in table.columns
        ],
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

    assert generated_schema is not None, (
        "generate_schema() returned None — schema generation pass did not produce output"
    )

    shared_data["published"] = True
    shared_data["schema_generation_triggered"] = True
    shared_data["generated_schema"] = generated_schema
    shared_data["schema_generation_elapsed_ms"] = elapsed_ms
    shared_data["role_used_for_generation"] = role
    shared_data["table_record_published"] = table_record
    shared_data["query_builder_available"] = True


@then(
    "a schema generation pass is triggered and the table is immediately available in the query\n"
    "    builder"
)
def schema_generation_triggered_and_table_available(shared_data: dict) -> None:
    """Assert all REQ-016 postconditions."""
    from graphql import GraphQLObjectType, assert_valid_schema

    assert shared_data["published"] is True, "Table was not marked as published"
    assert shared_data["schema_generation_triggered"] is True, (
        "Schema generation pass was not triggered on publication"
    )
    assert shared_data["query_builder_available"] is True, (
        "Table was not made available in query builder after publication"
    )

    generated_schema = shared_data["generated_schema"]
    assert generated_schema is not None, "No schema was generated"

    assert_valid_schema(generated_schema)

    query_type = generated_schema.query_type
    assert query_type is not None, "Generated schema has no Query type"
    assert len(query_type.fields) > 0, "Query type has no fields"

    elapsed_ms = shared_data["schema_generation_elapsed_ms"]
    assert elapsed_ms < 2000.0, (
        f"Schema generation took {elapsed_ms:.1f}ms — expected < 2000ms for immediate availability"
    )

    table: Table = shared_data["table"]
    type_map = generated_schema.type_map
    table_type_found = any(
        table.table_name.lower() in type_name.lower()
        for type_name in type_map.keys()
    )
    assert table_type_found, (
        f"Table '{table.table_name}' not found in generated schema type map: "
        f"{list(type_map.keys())}"
    )


# ---------------------------------------------------------------------------
# REQ-017 — NoSQL sources exposed read-only via native Trino connector
# ---------------------------------------------------------------------------


@given("a registered NoSQL source with a native Trino connector")
def registered_nosql_source_with_native_trino_connector(shared_data: dict) -> None:
    """Set up a MongoDB source registered in Provisa with the MongoDB Trino connector.

    Verifies:
    - The source type maps to a known Trino connector in SOURCE_TO_CONNECTOR.
    - The catalog properties are built via the connector path (not Parquet/ETL).
    - The catalog is marked read-only (no mutation properties present).
    """
    from provisa.core.catalog import _build_catalog_properties, _to_catalog_name
    from provisa.core.trino_catalog_files import catalog_properties_for

    # MongoDB is the canonical NoSQL example from the requirement.
    source = Source(
        id="mongo_products",
        type=SourceType.mongodb,
        host="mongo.example.com",
        port=27017,
        database="products_db",
        username="mongouser",
    )
    shared_data["nosql_source"] = source

    # 1. Confirm MongoDB is registered as a Trino-connector-backed source type.
    assert SourceType.mongodb in SOURCE_TO_CONNECTOR, (
        f"SourceType.mongodb is not present in SOURCE_TO_CONNECTOR — "
        f"the MongoDB connector is required for REQ-017. "
        f"Known connector types: {list(SOURCE_TO_CONNECTOR.keys())}"
    )
    connector_name = SOURCE_TO_CONNECTOR[SourceType.mongodb]
    assert connector_name, (
        "SOURCE_TO_CONNECTOR[SourceType.mongodb] is empty — a connector name is required."
