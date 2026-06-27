# Copyright (c) 2026 Kenneth Stott
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""BDD step implementations for REQ-372 — Iceberg/Delta time-travel queries
and REQ-736 — File & Lake Sources (SQLite, CSV, Parquet)."""

from __future__ import annotations

import sqlite3
import tempfile
from pathlib import Path

import pytest

from graphql import parse
from pytest_bdd import given, when, then, scenarios

from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.schema_gen import SchemaInput, generate_schema
from provisa.compiler.sql_gen import build_context, compile_query
from provisa.core.models import TIME_TRAVEL_SOURCES
from provisa.file_source.source import (
    FileSourceConfig,
    _sqlite_type_to_sql,
    discover_schema,
    execute_query,
)


scenarios("../features/REQ-372.feature")
scenarios("../features/REQ-736.feature")


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


# ---------------------------------------------------------------------------
# REQ-372 steps
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# REQ-736 steps
# ---------------------------------------------------------------------------


@given("a SQLite database with multiple tables")
def given_sqlite_database_with_multiple_tables(shared_data, tmp_path):
    """Create a SQLite database with two tables covering INTEGER, REAL, TEXT, and BOOLEAN."""
    db_path = tmp_path / "test_req736.sqlite"
    conn = sqlite3.connect(db_path)
    conn.executescript("""
        CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            customer_id INTEGER NOT NULL,
            amount REAL NOT NULL,
            status TEXT NOT NULL
        );
        INSERT INTO orders VALUES (1, 10, 99.99, 'delivered');
        INSERT INTO orders VALUES (2, 11, 49.50, 'pending');

        CREATE TABLE customers (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            active BOOLEAN NOT NULL DEFAULT 1
        );
        INSERT INTO customers VALUES (10, 'Alice', 1);
        INSERT INTO customers VALUES (11, 'Bob', 1);
    """)
    conn.commit()
    conn.close()

    config = FileSourceConfig(
        id="req736-sqlite",
        source_type="sqlite",
        path=str(db_path),
    )
    shared_data["sqlite_config"] = config
    shared_data["db_path"] = db_path


@when("the adapter discovers schema and executes queries")
def when_adapter_discovers_schema_and_executes_queries(shared_data):
    config = shared_data["sqlite_config"]

    # Discover schema for all tables
    columns = discover_schema(config)
    shared_data["discovered_columns"] = columns

    # Build a lookup: (table, column_name) -> sql_type
    type_map = {(col["table"], col["name"]): col["type"] for col in columns}
    shared_data["type_map"] = type_map

    # Execute a query against the orders table
    orders_rows = execute_query(config, "SELECT id, customer_id, amount, status FROM orders ORDER BY id")
    shared_data["orders_rows"] = orders_rows

    # Execute a query against the customers table
    customers_rows = execute_query(config, "SELECT id, name, active FROM customers ORDER BY id")
    shared_data["customers_rows"] = customers_rows

    # Execute a filtered query to verify WHERE clause support
    filtered_rows = execute_query(config, "SELECT id, amount FROM orders WHERE amount > 50.0 ORDER BY id")
    shared_data["filtered_rows"] = filtered_rows


@then("column types are mapped correctly and results are returned as row dicts")
def then_column_types_mapped_and_results_returned(shared_data):
    type_map = shared_data["type_map"]

    # --- Verify SQLite type mapping: INTEGER → BIGINT ---
    assert type_map[("orders", "id")] == "BIGINT", (
        f"Expected BIGINT for INTEGER column, got {type_map[('orders', 'id')]!r}"
    )
    assert type_map[("orders", "customer_id")] == "BIGINT", (
        f"Expected BIGINT for INTEGER column, got {type_map[('orders', 'customer_id')]!r}"
    )

    # --- Verify SQLite type mapping: REAL → DOUBLE ---
    assert type_map[("orders", "amount")] == "DOUBLE", (
        f"Expected DOUBLE for REAL column, got {type_map[('orders', 'amount')]!r}"
    )

    # --- Verify SQLite type mapping: TEXT → VARCHAR ---
    assert type_map[("orders", "status")] == "VARCHAR", (
        f"Expected VARCHAR for TEXT column, got {type_map[('orders', 'status')]!r}"
    )

    # --- Verify SQLite type mapping: BOOLEAN → BOOLEAN ---
    assert type_map[("customers", "active")] == "BOOLEAN", (
        f"Expected BOOLEAN for BOOLEAN column, got {type_map[('customers', 'active')]!r}"
    )

    # --- Verify that results are returned as lists of dicts ---
    orders_rows = shared_data["orders_rows"]
    assert isinstance(orders_rows, list), f"execute_query must return a list, got {type(orders_rows)}"
    assert len(orders_rows) == 2, f"Expected 2 orders rows, got {len(orders_rows)}"

    first_order = orders_rows[0]
    assert isinstance(first_order, dict), f"Each row must be a dict, got {type(first_order)}"
    assert first_order["id"] == 1
    assert first_order["customer_id"] == 10
    assert abs(first_order["amount"] - 99.99) < 1e-6
    assert first_order["status"] == "delivered"

    second_order = orders_rows[1]
    assert second_order["id"] == 2
    assert second_order["status"] == "pending"

    # --- Verify customers rows ---
    customers_rows = shared_data["customers_rows"]
    assert isinstance(customers_rows, list)
    assert len(customers_rows) == 2

    alice = customers_rows[0]
    assert isinstance(alice, dict)
    assert alice["id"] == 10
    assert alice["name"] == "Alice"

    bob = customers_rows[1]
    assert bob["id"] == 11
    assert bob["name"] == "Bob"

    # --- Verify filtered query returns only rows matching WHERE clause ---
    filtered_rows = shared_data["filtered_rows"]
    assert isinstance(filtered_rows, list)
    assert len(filtered_rows) == 1, (
        f"Expected 1 row with amount > 50.0, got {len(filtered_rows)}: {filtered_rows}"
    )
    assert filtered_rows[0]["id"] == 1
    assert abs(filtered_rows[0]["amount"] - 99.99) < 1e-6

    # --- Verify _sqlite_type_to_sql directly for all required mappings ---
    assert _sqlite_type_to_sql("INTEGER") == "BIGINT"
    assert _sqlite_type_to_sql("INT") == "BIGINT"
    assert _sqlite_type_to_sql("REAL") == "DOUBLE"
    assert _sqlite_type_to_sql("BOOLEAN") == "BOOLEAN"
    assert _sqlite_type_to_sql("TEXT") == "VARCHAR"

    # --- Verify schema discovery returned entries for both tables ---
    discovered = shared_data["discovered_columns"]
    tables_found = {col["table"] for col in discovered}
    assert "orders" in tables_found, f"Expected 'orders' table in discovered schema, got {tables_found}"
    assert "customers" in tables_found, f"Expected 'customers' table in discovered schema, got {tables_found}"

    # Each column dict must have the required keys
    for col in discovered:
        assert "table" in col, f"Column dict missing 'table' key: {col}"
        assert "name" in col, f"Column dict missing 'name' key: {col}"
        assert "type" in col, f"Column dict missing 'type' key: {col}"
        assert "nullable" in col, f"Column dict missing 'nullable' key: {col}"
