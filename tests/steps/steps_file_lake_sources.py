# Copyright (c) 2026 Kenneth Stott
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""BDD step implementations for REQ-372 — Iceberg/Delta time-travel queries."""

from __future__ import annotations

import pytest

from graphql import parse
from pytest_bdd import given, when, then, scenarios

from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.schema_gen import SchemaInput, generate_schema
from provisa.compiler.sql_gen import build_context, compile_query
from provisa.core.models import TIME_TRAVEL_SOURCES


scenarios("../features/REQ-372.feature")


@pytest.fixture
def shared_data() -> dict:
    return {}


def _col(name: str, data_type: str = "varchar(100)", nullable: bool = False) -> ColumnMetadata:
    return ColumnMetadata(column_name=name, data_type=data_type, is_nullable=nullable)


def _build_lake_ctx(source_type: str):
    """Build schema + compilation context for a single lake source table."""
    tables = [
        {
            "id": 10,
            "source_id": "lake-src",
            "domain_id": "datalake",
            "schema_name": "db",
            "table_name": "events",
            "governance": "pre-approved",
            "columns": [
                {"column_name": "id", "visible_to": ["admin"]},
                {"column_name": "ts", "visible_to": ["admin"]},
                {"column_name": "payload", "visible_to": ["admin"]},
            ],
        }
    ]
    column_types = {
        10: [
            _col("id", "bigint"),
            _col("ts", "timestamp"),
            _col("payload", "varchar"),
        ]
    }
    role = {"id": "admin", "capabilities": ["query_development"], "domain_access": ["*"]}
    domains = [{"id": "datalake", "description": "Data Lake"}]
    si = SchemaInput(
        tables=tables,
        relationships=[],
        column_types=column_types,
        naming_rules=[],
        role=role,
        domains=domains,
        source_types={"lake-src": source_type},
    )
    schema = generate_schema(si)
    ctx = build_context(si)
    return schema, ctx


@given("an Iceberg source with an as_of argument supplied at query time")
def given_iceberg_source_with_as_of(shared_data):
    # Confirm iceberg is a recognised time-travel source.
    assert "iceberg" in TIME_TRAVEL_SOURCES

    schema, ctx = _build_lake_ctx("iceberg")
    shared_data["iceberg_ctx"] = ctx

    # Build a non-capable source context for the rejection assertion.
    assert "postgresql" not in TIME_TRAVEL_SOURCES
    _, pg_ctx = _build_lake_ctx("postgresql")
    shared_data["pg_ctx"] = pg_ctx

    # Stash queries that exercise both timestamp and version time-travel.
    shared_data["timestamp_query"] = parse('{ events(as_of: "2024-01-15T12:00:00") { id ts } }')
    shared_data["version_query"] = parse("{ events(as_of: 42) { id } }")
    shared_data["bad_query"] = parse('{ events(as_of: "2024-01-15T12:00:00") { id } }')


@when("the compiler processes the query")
def when_compiler_processes_query(shared_data):
    iceberg_ctx = shared_data["iceberg_ctx"]

    ts_results = compile_query(shared_data["timestamp_query"], iceberg_ctx)
    shared_data["timestamp_sql"] = ts_results[0].sql

    ver_results = compile_query(shared_data["version_query"], iceberg_ctx)
    shared_data["version_sql"] = ver_results[0].sql

    # Attempt the same as_of against a non-capable source; capture rejection.
    pg_ctx = shared_data["pg_ctx"]
    shared_data["rejected"] = False
    try:
        compile_query(shared_data["bad_query"], pg_ctx)
    except Exception as exc:  # noqa: BLE001 - we assert on the rejection below
        shared_data["rejected"] = True
        shared_data["rejection_error"] = str(exc)


@then(
    "FOR TIMESTAMP AS OF / FOR VERSION AS OF syntax is emitted; "
    "non-capable sources with as_of are rejected"
)
def then_time_travel_emitted_and_rejected(shared_data):
    ts_sql = shared_data["timestamp_sql"]
    assert "FOR TIMESTAMP AS OF TIMESTAMP '2024-01-15T12:00:00'" in ts_sql
    assert "FOR VERSION AS OF" not in ts_sql

    ver_sql = shared_data["version_sql"]
    assert "FOR VERSION AS OF 42" in ver_sql
    assert "FOR TIMESTAMP AS OF" not in ver_sql

    # Non-time-travel source supplying as_of must be rejected at compile time.
    assert shared_data["rejected"] is True
    assert shared_data.get("rejection_error")
