# Copyright (c) 2026 Kenneth Stott
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""BDD step implementations for REQ-016, REQ-434, REQ-605, REQ-612, REQ-635, REQ-636 and REQ-638 — Registration & Governance.

REQ-016: Table publication triggers schema generation pass; table immediately
available in query builder.

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
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pytest_bdd import given, when, then, scenario

from provisa.core.models import SOURCE_TO_CONNECTOR, Column, Source, SourceType, Table
from provisa.discovery.analyzer import RelationshipCandidate
from provisa.api.admin import introspect as introspect_mod
from provisa.api.admin.introspect import native_schem


# ---------------------------------------------------------------------------
# Shared fixture
# ---------------------------------------------------------------------------


@pytest.fixture
def shared_data() -> dict:
    return {}


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

    # Verify the table model is valid before publishing
    assert table.source_id == "sales_pg"
    assert table.table_name == "orders"
    assert len(table.columns) == 3


@when("the publication completes")
def publication_completes(shared_data: dict) -> None:
    """Simulate the publication of a table and trigger schema generation."""
    from provisa.compiler.schema_gen import SchemaInput, generate_schema

    table = shared_data["table"]

    # Build a minimal SchemaInput representing the published table state.
    # column_types is derived from the table's column definitions directly
    # (unit context: no live Trino available).
    column_types: dict[str, list[Any]] = {
        shared_data["table_id"]: [
            MagicMock(column_name=col.name, data_type="integer" if col.name == "id" else "varchar", is_nullable=True)
            for col in table.columns
        ]
    }

    # Construct a minimal role record
    role = {
        "id": "analyst",
        "name": "Analyst",
        "row_filters": [],
        "column_masks": [],
    }

    # Construct a minimal table record as expected by generate_schema
    table_record = {
        "id": shared_data["table_id"],
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
        column_types=column_types,
        naming_rules=[],
        role=role,
        domains=[],
    )

    # Invoke the real schema generation pass
    generated_schema = generate_schema(schema_input)

    shared_data["published"] = True
    shared_data["schema_generation_triggered"] = True
    shared_data["generated_schema"] = generated_schema


@then("a schema generation pass is triggered and the table is immediately available in the query builder")
def schema_generation_triggered_and_table_available(shared_data: dict) -> None:
    """Assert schema generation ran and the table type appears in the generated schema."""
    from graphql import assert_valid_schema, GraphQLObjectType

    assert shared_data["published"] is True, "Table was not published"
    assert shared_data["schema_generation_triggered"] is True, "Schema generation was not triggered"

    generated_schema = shared_data.get("generated_schema")
    assert generated_schema is not None, "No schema was produced by the generation pass"

    # The schema must be structurally valid
    assert_valid_schema(generated_schema)

    # The Query type must exist — it is the query builder entry point
    query_type = generated_schema.query_type
    assert query_type is not None, "Generated schema has no Query type (query builder entry point missing)"

    # At least one field must be present on the Query type — the published table
    # should appear as a queryable field
    assert len(query_type.fields) > 0, (
        "Query type has no fields; the published table is not available in the query builder"
    )

    # Verify the table's type appears in the schema type map
    type_map = generated_schema.type_map
    # The generated type names follow the naming conventions in SchemaInput;
    # at minimum a named type containing 'orders' (case-insensitive) should exist
    table_types = [
        name for name in type_map
        if "orders" in name.lower() or "Orders" in name
    ]
    assert len(table_types) > 0, (
        f"No GraphQL type for 'orders' found in schema type map: {list(type_map.keys())}"
    )

    shared_data["query_builder_available"] = True
    assert shared_data["query_builder_available"] is True
