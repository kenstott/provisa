# Copyright (c) 2026 Kenneth Stott
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""BDD step implementations for REQ-372 — Iceberg/Delta time-travel queries,
REQ-736 — File & Lake Sources (SQLite, CSV, Parquet),
REQ-788 — File glob patterns for CSV source connectors,
REQ-789 — CSV camelCase header to snake_case column name mapping, and
REQ-790 — File connector table enumeration via UI table registration form."""

from __future__ import annotations

import csv
import os
import re
import sqlite3
import tempfile
from pathlib import Path
from collections import defaultdict

import pytest

from graphql import parse
from pytest_bdd import given, when, then, scenarios

from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.schema_gen import SchemaInput, generate_schema
from provisa.compiler.sql_gen import build_context, compile_query
from provisa.core.models import TIME_TRAVEL_SOURCES
from provisa.file_source.source import (
    FileSourceConfig,
    _arrow_schema_to_columns,
    _sqlite_type_to_sql,
    discover_schema,
    execute_query,
    generate_table_definitions,
)


scenarios("../features/REQ-372.feature")
scenarios("../features/REQ-736.feature")
scenarios("../features/REQ-788.feature")
scenarios("../features/REQ-789.feature")
scenarios("../features/REQ-790.feature")


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
# Shared helper: camelCase -> snake_case
# ---------------------------------------------------------------------------

def _camel_to_snake(name: str) -> str:
    """Convert a camelCase identifier to snake_case using the LINQ4J convention."""
    s = re.sub(r"(?<=[a-z0-9])([A-Z])", r"_\1", name)
    return s.lower()


def _write_csv(path: Path, headers: list[str], rows: list[list]) -> None:
    """Helper: write a CSV file with the given headers and data rows."""
    with path.open("w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(headers)
        for row in rows:
            writer.writerow(row)


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


# ---------------------------------------------------------------------------
# REQ-788 steps
# ---------------------------------------------------------------------------


@given("a directory containing multiple CSV files matching a glob pattern")
def given_directory_with_multiple_csv_files(shared_data, tmp_path):
    """Create a directory containing three CSV files with distinct schemas."""
    csv_dir = tmp_path / "lake_data"
    csv_dir.mkdir(parents=True, exist_ok=True)

    # customers.csv
    customers_path = csv_dir / "customers.csv"
    _write_csv(
        customers_path,
        headers=["customerId", "companyName", "contactName", "country"],
        rows=[
            ["C001", "Acme Corp", "Alice Smith", "US"],
            ["C002", "Globex", "Bob Jones", "UK"],
        ],
    )

    # orders.csv
    orders_path = csv_dir / "orders.csv"
    _write_csv(
        orders_path,
        headers=["orderId", "customerId", "amount", "status"],
        rows=[
            ["O001", "C001", "199.99", "shipped"],
            ["O002", "C002", "49.50", "pending"],
        ],
    )

    # products.csv
    products_path = csv_dir / "products.csv"
    _write_csv(
        products_path,
        headers=["productId", "productName", "unitPrice", "discontinued"],
        rows=[
            ["P001", "Widget", "9.99", "false"],
            ["P002", "Gadget", "24.99", "true"],
        ],
    )

    glob_pattern = str(csv_dir / "**/*.csv")

    shared_data["csv_dir"] = csv_dir
    shared_data["glob_pattern"] = glob_pattern
    shared_data["expected_files"] = {
        customers_path,
        orders_path,
        products_path,
    }
    shared_data["expected_table_names"] = {"customers", "orders", "products"}
    shared_data["expected_headers"] = {
        "customers": ["customerId", "companyName", "contactName", "country"],
        "orders": ["orderId", "customerId", "amount", "status"],
        "products": ["productId", "productName", "unitPrice", "discontinued"],
    }


@when("a file connector source is registered with the directory glob pattern")
def when_file_connector_registered_with_glob(shared_data):
    """Create a FileSourceConfig with the glob pattern and run discovery."""
    glob_pattern = shared_data["glob_pattern"]

    config = FileSourceConfig(
        id="req788-csv-lake",
        source_type="csv",
        path=glob_pattern,
    )
    shared_data["file_source_config"] = config

    # generate_table_definitions crawls the glob and introspects each CSV file.
    table_definitions = generate_table_definitions(config)
    shared_data["table_definitions"] = table_definitions

    # Also exercise discover_schema which returns column-level metadata.
    schema_columns = discover_schema(config)
    shared_data["schema_columns"] = schema_columns


@then("all matching files are discovered and enumerated as available tables")
def then_all_matching_files_discovered(shared_data):
    """Assert that every CSV file matched by the glob is represented as a table."""
    table_definitions = shared_data["table_definitions"]
    expected_table_names = shared_data["expected_table_names"]

    assert isinstance(table_definitions, list), (
        f"generate_table_definitions must return a list, got {type(table_definitions)}"
    )
    assert len(table_definitions) >= len(expected_table_names), (
        f"Expected at least {len(expected_table_names)} table definitions, "
        f"got {len(table_definitions)}: {table_definitions}"
    )

    # Collect the table names produced by the connector.
    discovered_table_names = set()
    for tdef in table_definitions:
        # Table definitions may be dicts or objects; handle both.
        if isinstance(tdef, dict):
            name = tdef.get("table_name") or tdef.get("name") or tdef.get("table")
        else:
            name = getattr(tdef, "table_name", None) or getattr(tdef, "name", None)
        assert name is not None, f"Table definition has no recognisable name field: {tdef!r}"
        discovered_table_names.add(name)

    # Every expected table must be present.
    for expected in expected_table_names:
        assert expected in discovered_table_names, (
            f"Expected table '{expected}' not found in discovered tables: {discovered_table_names}"
        )

    shared_data["discovered_table_names"] = discovered_table_names


@then("the table schema is extracted from CSV headers")
def then_table_schema_extracted_from_csv_headers(shared_data):
    """Assert that column names in the discovered schema match the CSV header rows."""
    schema_columns = shared_data["schema_columns"]
    expected_headers = shared_data["expected_headers"]

    assert isinstance(schema_columns, list), (
        f"discover_schema must return a list, got {type(schema_columns)}"
    )
    assert len(schema_columns) > 0, "discover_schema returned an empty list; expected column metadata"

    # Build a lookup: table_name -> set of discovered column names.
    table_columns: dict[str, set[str]] = {}
    for col in schema_columns:
        assert isinstance(col, dict), f"Each column entry must be a dict, got {type(col)}: {col!r}"
        assert "table" in col, f"Column entry missing 'table' key: {col}"
        assert "name" in col, f"Column entry missing 'name' key: {col}"

        table_name = col["table"]
        col_name = col["name"]
        table_columns.setdefault(table_name, set()).add(col_name)

    for table_name, raw_headers in expected_headers.items():
        assert table_name in table_columns, (
            f"Expected schema for table '{table_name}' but only found: {set(table_columns.keys())}"
        )
        discovered_cols = table_columns[table_name]

        for raw_header in raw_headers:
            snake_header = _camel_to_snake(raw_header)
            assert raw_header in discovered_cols or snake_header in discovered_cols, (
                f"Table '{table_name}': expected column '{raw_header}' (or '{snake_header}') "
                f"not found in discovered columns {discovered_cols}"
            )

    # Each column entry must also carry a type field.
    for col in schema_columns:
        assert "type" in col, (
            f"Column entry for table '{col.get('table')}' / '{col.get('name')}' "
            f"is missing a 'type' field: {col!r}"
        )
        col_type = col["type"]
        assert isinstance(col_type, str) and col_type.strip(), (
            f"Column '{col.get('name')}' in table '{col.get('table')}' "
            f"has an empty or non-string type: {col_type!r}"
        )


# ---------------------------------------------------------------------------
# REQ-789 steps
# ---------------------------------------------------------------------------


@given('a CSV file with camelCase headers (e.g., "companyName", "customerId")')
def given_csv_file_with_camel_case_headers(shared_data, tmp_path):
    """Write a CSV file whose headers follow camelCase naming as produced by typical exports."""
    csv_path = tmp_path / "req789_customers.csv"

    camel_headers = [
        "customerId",
        "companyName",
        "contactName",
        "contactTitle",
        "postalCode",
        "phoneNumber",
    ]
    rows = [
        ["C001", "Acme Corp", "Alice Smith", "CEO", "10001", "555-1234"],
        ["C002", "Globex Inc", "Bob Jones", "CTO", "20002", "555-5678"],
    ]

    with csv_path.open("w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(camel_headers)
        for row in rows:
            writer.writerow(row)

    shared_data["csv_path"] = csv_path
    shared_data["camel_headers"] = camel_headers

    # Pre-compute expected snake_case mappings for assertion use.
    expected_snake = {h: _camel_to_snake(h) for h in camel_headers}
    shared_data["expected_snake"] = expected_snake


@when("the file is introspected by the file connector")
def when_file_introspected_by_file_connector(shared_data):
    """Run discover_schema over the CSV file and capture the resulting column names."""
    csv_path = shared_data["csv_path"]

    config = FileSourceConfig(
        id="req789-camel-csv",
        source_type="csv",
        path=str(csv_path),
    )
    shared_data["req789_config"] = config

    schema_columns = discover_schema(config)
    shared_data["req789_schema_columns"] = schema_columns

    # Also exercise generate_table_definitions to capture the full table entry.
    table_definitions = generate_table_definitions(config)
    shared_data["req789_table_definitions"] = table_definitions

    # Build a flat set of discovered column names for quick membership tests.
    discovered_col_names = {col["name"] for col in schema_columns}
    shared_data["discovered_col_names"] = discovered_col_names


@then('headers are automatically converted to snake_case (e.g., "company_name", "customer_id")')
def then_headers_converted_to_snake_case(shared_data):
    """Assert that every camelCase header has been normalised to snake_case by the connector."""
    discovered_col_names = shared_data["discovered_col_names"]
    expected_snake = shared_data["expected_snake"]
    camel_headers = shared_data["camel_headers"]

    assert len(discovered_col_names) > 0, (
        "discover_schema returned no columns; expected snake_case column names"
    )

    for camel_header in camel_headers:
        snake_name = expected_snake[camel_header]

        # The connector MUST produce the snake_case form.
        assert snake_name in discovered_col_names, (
            f"Expected snake_case column '{snake_name}' (from camelCase '{camel_header}') "
            f"not found in discovered columns: {discovered_col_names}"
        )

    # Spot-check the two canonical examples from the requirement text.
    assert "company_name" in discovered_col_names, (
        f"'company_name' not found in discovered columns: {discovered_col_names}"
    )
    assert "customer_id" in discovered_col_names, (
        f"'customer_id' not found in discovered columns: {discovered_col_names}"
    )

    # The raw camelCase forms must NOT appear as column names (conversion is mandatory).
    for camel_header in camel_headers:
        # Only flag if the camelCase name is distinct from its snake_case equivalent.
        snake_name = expected_snake[camel_header]
        if camel_header != snake_name:
            assert camel_header not in discovered_col_names, (
                f"Raw camelCase header '{camel_header}' should have been converted to "
                f"'{snake_name}' but was left unconverted in: {discovered_col_names}"
            )


@then("GraphQL field names reflect the snake_case conversion")
def then_graphql_field_names_reflect_snake_case(shared_data):
    """Assert that the table definition columns carry snake_case names suitable for GraphQL."""
    table_definitions = shared_data["req789_table_definitions"]
    expected_snake = shared_data["expected_snake"]

    assert isinstance(table_definitions, list) and len(table_definitions) > 0, (
        f"generate_table_definitions returned no table definitions: {table_definitions!r}"
    )

    # Extract the column list from the first (and only) table definition.
    tdef = table_definitions[0]
    if isinstance(tdef, dict):
        columns = (
            tdef.get("columns")
            or tdef.get("fields")
            or tdef.get("schema")
            or []
        )
    else:
        columns = (
            getattr(tdef, "columns", None)
            or getattr(tdef, "fields", None)
            or []
        )

    assert len(columns) > 0, (
        f"Table definition has no columns/fields entry; tdef={tdef!r}"
    )

    # Collect column names from the table definition.
    tdef_col_names: set[str] = set()
    for col in columns:
        if isinstance(col, dict):
            name = col.get("column_name") or col.get("name") or col.get("field")
        else:
            name = getattr(col, "column_name", None) or getattr(col, "name", None)
        assert name is not None, f"Column entry in table definition has no recognisable name: {col!r}"
        tdef_col_names.add(name)

    # Every expected snake_case name must appear as a GraphQL-ready field name.
    for camel_header, snake_name in expected_snake.items():
        assert snake_name in tdef_col_names, (
            f"Expected GraphQL field '{snake_name}' (from camelCase '{camel_header}') "
            f"not found in table definition columns: {tdef_col_names}"
        )

    # snake_case names must be valid GraphQL identifiers: [_a-zA-Z][_a-zA-Z0-9]*
    graphql_identifier_re = re.compile(r"^[_a-zA-Z][_a-zA-Z0-9]*$")
    for name in tdef_col_names:
        assert graphql_identifier_re.match(name), (
            f"Column name '{name}' is not a valid GraphQL field identifier"
        )

    # Confirm that no camelCase originals leaked through into the table definition.
    for camel_header in expected_snake:
        snake_name = expected_snake[camel_header]
        if camel_header != snake_name:
            assert camel_header not in tdef_col_names, (
                f"Raw camelCase header '{camel_header}' must not appear as a GraphQL field name; "
                f"found in table definition columns: {tdef_col_names}"
            )


# ---------------------------------------------------------------------------
# REQ-790 helpers
# ---------------------------------------------------------------------------

def _build_req790_file_source(tmp_path: Path) -> tuple[FileSourceConfig, dict[str, list[str]]]:
    """
    Create a directory tree that mimics a Northwind-style file lake:

        <tmp>/northwind/
            northwind/          <- schema directory
                customers.csv
                orders.csv
                products.csv
            analytics/          <- second schema directory
                summary.csv

    Returns the FileSourceConfig (glob pointing at the root) and a mapping of
    schema_name -> [table_name, ...] for assertions.
    """
    root = tmp_path / "northwind"
    schema_dir = root / "northwind"
    analytics_dir = root / "analytics"
    schema_dir.mkdir(parents=True, exist_ok=True)
    analytics_dir.mkdir(parents=True, exist_ok=True)

    # northwind/customers.csv — Northwind-style camelCase headers
    _write_csv(
        schema_dir / "customers.csv",
        headers=[
            "customerId", "companyName", "contactName", "contactTitle",
            "address", "city", "region", "postalCode", "country", "phone", "fax",
        ],
        rows=[["ALFKI", "Alfreds Futterkiste", "Maria Anders", "Sales Rep",
               "Obere Str. 57", "Berlin", "", "12209", "Germany", "030-0074321", "030-0076545"]],
    )

    # northwind/orders.csv
    _write_csv(
        schema_dir / "orders.csv",
        headers=["orderId", "customerId", "employeeId", "orderDate", "freight"],
        rows=[["10248", "ALFKI", "5", "1996-07-04", "32.38"]],
    )

    # northwind/products.csv
    _write_csv(
        schema_dir / "products.csv",
        headers=["productId", "productName", "unitPrice", "unitsInStock"],
        rows=[["1", "Chai", "18.00", "39"]],
    )

    # analytics/summary.csv
    _write_csv(
        analytics_dir / "summary.csv",
        headers=["reportId", "reportName", "generatedAt"],
        rows=[["1", "Monthly Sales", "2024-01-01"]],
    )

    glob_pattern = str(root / "**/*.csv")
    config = FileSourceConfig(
        id="req790-northwind",
        source_type="csv",
        path=glob_pattern,
    )
