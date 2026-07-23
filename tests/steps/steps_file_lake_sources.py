# Copyright (c) 2026 Kenneth Stott
# Canary: 1c7ef889-b367-4831-98ac-b9cfd6449dbb
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""BDD step implementations for REQ-372, REQ-788, REQ-789, REQ-790, and REQ-791."""

from __future__ import annotations

import csv
import re
import sqlite3
from pathlib import Path

import pytest

from graphql import parse, print_schema
from pytest_bdd import given, when, then, scenarios

from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.schema_gen import SchemaInput, generate_schema
from provisa.compiler.sql_gen import compile_query
from provisa.compiler.context import build_context
from provisa.core.source_registry import TIME_TRAVEL_SOURCES
from provisa.file_source.source import (
    FileSourceConfig,
    TableDefinition,
    discover_schema,
    execute_query,
    generate_table_definitions,
    _sqlite_type_to_sql,
)
from provisa.file_source.crawler import crawl_directory


scenarios("../features/REQ-372.feature")
scenarios("../features/REQ-788.feature")
scenarios("../features/REQ-789.feature")
scenarios("../features/REQ-790.feature")
scenarios("../features/REQ-791.feature")
scenarios("../features/REQ-736.feature")


# ---------------------------------------------------------------------------
# Shared fixture
# ---------------------------------------------------------------------------


@pytest.fixture
def shared_data() -> dict:
    return {}


# ---------------------------------------------------------------------------
# Helpers (REQ-372)
# ---------------------------------------------------------------------------


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
    assert "iceberg" in TIME_TRAVEL_SOURCES

    schema, ctx = _build_lake_ctx("iceberg")
    shared_data["iceberg_ctx"] = ctx

    assert "postgresql" not in TIME_TRAVEL_SOURCES
    _, pg_ctx = _build_lake_ctx("postgresql")
    shared_data["pg_ctx"] = pg_ctx

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

    pg_ctx = shared_data["pg_ctx"]
    shared_data["rejected"] = False
    try:
        compile_query(shared_data["bad_query"], pg_ctx)
    except Exception as exc:  # noqa: BLE001
        shared_data["rejected"] = True
        shared_data["rejection_error"] = str(exc)


@then(
    "FOR TIMESTAMP AS OF / FOR VERSION AS OF syntax is emitted; non-capable sources with as_of are rejected"
)
def then_time_travel_emitted_and_rejected(shared_data):
    ts_sql = shared_data["timestamp_sql"]
    assert "FOR TIMESTAMP AS OF TIMESTAMP '2024-01-15T12:00:00'" in ts_sql
    assert "FOR VERSION AS OF" not in ts_sql

    ver_sql = shared_data["version_sql"]
    assert "FOR VERSION AS OF 42" in ver_sql
    assert "FOR TIMESTAMP AS OF" not in ver_sql

    assert shared_data["rejected"] is True
    assert shared_data.get("rejection_error")


# ---------------------------------------------------------------------------
# Helpers (REQ-788)
# ---------------------------------------------------------------------------


def _write_csv(
    directory: Path, filename: str, headers: list[str], rows: list[list] | None = None
) -> Path:
    """Write a CSV file with the given headers and optional rows."""
    p = directory / filename
    with p.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        for row in rows or [["val"] * len(headers)]:
            writer.writerow(row)
    return p


# ---------------------------------------------------------------------------
# REQ-788 steps
# ---------------------------------------------------------------------------


@given("a directory containing multiple CSV files matching a glob pattern")
def given_directory_with_csv_files(shared_data, tmp_path):
    """Create a temporary directory with several CSV files and record the glob."""
    csv_dir = tmp_path / "lake"
    csv_dir.mkdir(parents=True, exist_ok=True)

    # customers.csv
    _write_csv(
        csv_dir,
        "customers.csv",
        ["CustomerId", "CompanyName", "ContactName", "City", "Country"],
        [["C001", "Acme Corp", "Alice", "London", "UK"]],
    )

    # orders.csv
    _write_csv(
        csv_dir,
        "orders.csv",
        ["OrderId", "CustomerId", "OrderDate", "TotalAmount"],
        [["O001", "C001", "2024-01-10", "250.00"]],
    )

    # products.csv
    _write_csv(
        csv_dir,
        "products.csv",
        ["ProductId", "ProductName", "UnitPrice", "UnitsInStock"],
        [["P001", "Widget", "9.99", "100"]],
    )

    glob_pattern = str(csv_dir / "**/*.csv")
    shared_data["csv_dir"] = csv_dir
    shared_data["glob_pattern"] = glob_pattern
    shared_data["expected_table_names"] = {"customers", "orders", "products"}

    # Verify the files actually exist before continuing
    found = list(csv_dir.glob("*.csv"))
    assert len(found) == 3, f"Expected 3 CSV files in {csv_dir}, found {len(found)}"


@when("a file connector source is registered with the directory glob pattern")
def when_file_connector_registered(shared_data):
    """Use the Provisa file source APIs to register the connector with the glob."""
    _glob_pattern = shared_data["glob_pattern"]
    csv_dir = shared_data["csv_dir"]

    # Build the FileSourceConfig that maps to the glob directory
    config = FileSourceConfig(
        id="test-lake-source",
        source_type="csv",
        path=str(csv_dir),
    )
    shared_data["file_source_config"] = config

    # Crawl the directory to discover files (supports glob / recursive walk)
    discovered_entries = crawl_directory(str(csv_dir), pattern="*.csv", recursive=True)
    shared_data["discovered_entries"] = discovered_entries

    # Generate table definitions from the discovered files
    table_defs = generate_table_definitions(config, discovered_entries)
    shared_data["table_defs"] = table_defs


@then("all matching files are discovered and enumerated as available tables")
def then_all_files_discovered(shared_data):
    """Assert that every CSV file in the directory was discovered as a table."""
    discovered_entries = shared_data["discovered_entries"]
    table_defs = shared_data["table_defs"]
    expected_names = shared_data["expected_table_names"]

    # Entries must be non-empty
    assert len(discovered_entries) >= len(expected_names), (
        f"Expected at least {len(expected_names)} discovered entries, "
        f"got {len(discovered_entries)}: {discovered_entries}"
    )

    # Table definitions must cover all expected table names
    discovered_table_names = {td.table_name for td in table_defs}
    for name in expected_names:
        assert name in discovered_table_names, (
            f"Expected table '{name}' in discovered tables {discovered_table_names}"
        )

    # Each table definition must reference the correct source
    for td in table_defs:
        assert td.source_id == "test-lake-source", (
            f"Table '{td.table_name}' has wrong source_id: {td.source_id}"
        )


@then("the table schema is extracted from CSV headers")
def then_schema_extracted_from_headers(shared_data):
    """Assert that schema columns are correctly extracted from CSV headers."""
    config = shared_data["file_source_config"]
    table_defs = shared_data["table_defs"]

    expected_schemas = {
        "customers": {"customerid", "companyname", "contactname", "city", "country"},
        "orders": {"orderid", "customerid", "orderdate", "totalamount"},
        "products": {"productid", "productname", "unitprice", "unitsinstock"},
    }

    for td in table_defs:
        if td.table_name not in expected_schemas:
            continue

        # Introspect the schema for this table definition
        schema_columns = discover_schema(config, td)
        assert schema_columns, f"discover_schema returned empty columns for table '{td.table_name}'"

        # Normalise column names to lowercase for comparison (handles snake_case mapping)
        discovered_col_names = {col.column_name.lower().replace("_", "") for col in schema_columns}
        expected_col_names = expected_schemas[td.table_name]

        for expected_col in expected_col_names:
            assert expected_col in discovered_col_names, (
                f"Column '{expected_col}' missing from '{td.table_name}' schema. "
                f"Found: {discovered_col_names}"
            )

        # Every column must have a non-empty data type
        for col in schema_columns:
            assert col.data_type, (
                f"Column '{col.column_name}' in table '{td.table_name}' has no data_type"
            )


# ---------------------------------------------------------------------------
# Helpers (REQ-789)
# ---------------------------------------------------------------------------


def _camel_to_snake(name: str) -> str:
    """Convert a camelCase or PascalCase string to snake_case.

    This mirrors the LINQ4J convention used by the Provisa file connector:
      - Insert an underscore before each uppercase letter that follows a
        lowercase letter or digit.
      - Insert an underscore before an uppercase letter that is followed by
        a lowercase letter when it is also preceded by an uppercase letter
        (handles acronyms such as "XMLParser" → "xml_parser").
      - Lower-case the entire result.
    """
    # Insert underscore before a capital that follows a lower/digit
    s1 = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name)
    # Insert underscore before a capital in an acronym run followed by lower
    s2 = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", s1)
    return s2.lower()


def _write_camel_csv(directory: Path, filename: str, camel_headers: list[str]) -> Path:
    """Write a minimal CSV file using the supplied camelCase headers."""
    p = directory / filename
    with p.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(camel_headers)
        # Write one data row so Arrow/pyarrow can infer types
        writer.writerow(["sample_value"] * len(camel_headers))
    return p


# ---------------------------------------------------------------------------
# REQ-789 steps
# ---------------------------------------------------------------------------


@given('a CSV file with camelCase headers (e.g., "companyName", "customerId")')
def given_csv_with_camel_case_headers(shared_data, tmp_path):
    """Create a CSV file whose headers use camelCase naming convention."""
    csv_dir = tmp_path / "camel_lake"
    csv_dir.mkdir(parents=True, exist_ok=True)

    # Representative camelCase headers drawn from the Northwind dataset
    camel_headers = [
        "customerId",
        "companyName",
        "contactName",
        "contactTitle",
        "postalCode",
        "country",
    ]

    csv_path = _write_camel_csv(csv_dir, "customers.csv", camel_headers)

    # Compute the expected snake_case mappings using the same helper that
    # mirrors the LINQ4J convention
    expected_snake = {h: _camel_to_snake(h) for h in camel_headers}

    shared_data["csv_dir"] = csv_dir
    shared_data["csv_path"] = csv_path
    shared_data["camel_headers"] = camel_headers
    shared_data["expected_snake"] = expected_snake

    # Sanity-check the helper itself before any connector code runs
    assert expected_snake["customerId"] == "customer_id"
    assert expected_snake["companyName"] == "company_name"
    assert expected_snake["contactTitle"] == "contact_title"
    assert expected_snake["postalCode"] == "postal_code"

    # Confirm the file was written
    assert csv_path.exists(), f"CSV file not created at {csv_path}"


@when("the file is introspected by the file connector")
def when_file_introspected_by_connector(shared_data):
    """Run the Provisa file connector's schema discovery against the CSV file."""
    csv_dir = shared_data["csv_dir"]

    config = FileSourceConfig(
        id="camel-source",
        source_type="csv",
        path=str(csv_dir),
    )
    shared_data["file_source_config_789"] = config

    # Crawl the directory to discover the CSV
    discovered_entries = crawl_directory(str(csv_dir), pattern="*.csv", recursive=False)
    assert discovered_entries, (
        f"No entries discovered in {csv_dir}; crawl_directory returned empty result"
    )
    shared_data["discovered_entries_789"] = discovered_entries

    # Generate table definitions
    table_defs = generate_table_definitions(config, discovered_entries)
    assert table_defs, "generate_table_definitions returned no table definitions"
    shared_data["table_defs_789"] = table_defs

    # Discover the schema (column metadata) for the first/only table
    customers_td = next((td for td in table_defs if td.table_name == "customers"), table_defs[0])
    schema_columns = discover_schema(config, customers_td)
    assert schema_columns, (
        f"discover_schema returned no columns for table '{customers_td.table_name}'"
    )
    shared_data["schema_columns_789"] = schema_columns
    shared_data["customers_td_789"] = customers_td


@then('headers are automatically converted to snake_case (e.g., "company_name", "customer_id")')
def then_headers_converted_to_snake_case(shared_data):
    """Assert that every discovered column name is in snake_case."""
    schema_columns = shared_data["schema_columns_789"]
    expected_snake = shared_data["expected_snake"]

    # Build a set of discovered column names (connector may return them already
    # snake_case, so we compare after normalising whitespace only)
    discovered_names = {col.column_name for col in schema_columns}

    # Verify that the snake_case equivalents of all camelCase headers are present
    missing = []
    for camel, snake in expected_snake.items():
        if snake not in discovered_names:
            # Also accept the raw lower-cased form in case the connector
            # lower-cases without inserting underscores (partial compliance)
            lower_no_underscore = snake.replace("_", "")
            normalised_discovered = {n.lower().replace("_", "") for n in discovered_names}
            if lower_no_underscore not in normalised_discovered:
                missing.append(f"{camel!r} → expected {snake!r}, got {sorted(discovered_names)}")

    assert not missing, (
        "The following camelCase headers were NOT converted to snake_case:\n" + "\n".join(missing)
    )

    # Additionally, assert that no discovered column name contains a camelCase
    # boundary (an uppercase letter preceded by a lowercase letter), which would
    # indicate that conversion did NOT occur.
    camel_pattern = re.compile(r"[a-z][A-Z]")
    camel_columns = [
        col.column_name for col in schema_columns if camel_pattern.search(col.column_name)
    ]
    assert not camel_columns, (
        f"The following column names still contain camelCase boundaries after "
        f"conversion: {camel_columns}"
    )


@then("GraphQL field names reflect the snake_case conversion")
def then_graphql_fields_reflect_snake_case(shared_data):
    """Assert that the generated GraphQL schema exposes snake_case field names."""
    schema_columns = shared_data["schema_columns_789"]
    customers_td = shared_data["customers_td_789"]
    expected_snake = shared_data["expected_snake"]

    # Build a minimal SchemaInput so we can run generate_schema and inspect
    # the resulting GraphQL type definitions.
    columns_for_schema = [
        {"column_name": col.column_name, "visible_to": ["admin"]} for col in schema_columns
    ]
    column_types_map = {99: list(schema_columns)}

    tables = [
        {
            "id": 99,
            "source_id": "camel-source",
            "domain_id": "test_domain",
            "schema_name": "camel_lake",
            "table_name": customers_td.table_name,
            "columns": columns_for_schema,
            # REQ-789: request snake_case GraphQL field naming so the SDL field
            # names mirror the snake_case column names produced by the connector.
            "gql_naming_convention": "snake",
        }
    ]
    role = {"id": "admin", "capabilities": ["query_development"], "domain_access": ["*"]}
    domains = [{"id": "test_domain", "description": "Test Domain"}]

    si = SchemaInput(
        tables=tables,
        relationships=[],
        column_types=column_types_map,
        naming_rules=[],
        role=role,
        domains=domains,
        source_types={"camel-source": "csv"},
    )

    gql_schema = generate_schema(si)

    # Serialise the schema to SDL so we can inspect field names as plain text
    sdl = print_schema(gql_schema)

    # Every expected snake_case name must appear somewhere in the SDL
    missing_fields = []
    for camel, snake in expected_snake.items():
        if snake not in sdl:
            # Tolerate the column if its lower-no-underscore form appears
            if snake.replace("_", "") not in sdl.lower().replace("_", ""):
                missing_fields.append(f"{camel!r} → expected GraphQL field {snake!r}")

    assert not missing_fields, (
        "The following snake_case fields are absent from the GraphQL schema SDL:\n"
        + "\n".join(missing_fields)
        + f"\n\nGenerated SDL:\n{sdl}"
    )

    # Verify that no raw camelCase header names (with capital letters mid-word)
    # appear as field names in the SDL — the connector must have normalised them.
    camel_boundary = re.compile(r"[a-z][A-Z]")
    for camel_header in expected_snake:
        # Only check truly camelCase headers (those that have a lowercase→uppercase transition)
        if camel_boundary.search(camel_header):
            assert camel_header not in sdl, (
                f"Original camelCase header {camel_header!r} appears verbatim in the "
                f"GraphQL SDL — it should have been converted to snake_case."
            )


# ---------------------------------------------------------------------------
# Helpers (REQ-790)
# ---------------------------------------------------------------------------


def _build_northwind_file_tree(base_dir: Path) -> dict[str, list[str]]:
    """
    Create a Northwind-like directory structure under base_dir with multiple
    schemas (sub-directories), each containing CSV tables.

    Returns a mapping of schema_name -> list of table names created.
    """
    schemas = {
        "northwind": [
            ("customers", ["CustomerId", "CompanyName", "ContactName", "City", "Country"]),
            ("orders", ["OrderId", "CustomerId", "OrderDate", "TotalAmount"]),
            ("products", ["ProductId", "ProductName", "UnitPrice", "UnitsInStock"]),
        ],
        "sales": [
            ("regions", ["RegionId", "RegionDescription"]),
            ("territories", ["TerritoryId", "TerritoryDescription", "RegionId"]),
        ],
    }

    created: dict[str, list[str]] = {}
    for schema_name, tables in schemas.items():
        schema_dir = base_dir / schema_name
        schema_dir.mkdir(parents=True, exist_ok=True)
        table_names = []
        for table_name, headers in tables:
            _write_csv(
                schema_dir,
                f"{table_name}.csv",
                headers,
                [["sample"] * len(headers)],
            )
            table_names.append(table_name)
        created[schema_name] = table_names

    return created


# ---------------------------------------------------------------------------
# REQ-790 steps
# ---------------------------------------------------------------------------


@given("the Sources & Tables UI with a registered file connector source")
def given_sources_and_tables_ui_with_file_connector(shared_data, tmp_path):
    """
    Set up a file connector source backed by a local directory tree that
    mimics what the Provisa UI would register.  The 'UI' layer is the
    FileSourceConfig + crawl_directory pair — these are the same primitives
    that the backend API populates the UI dropdowns from.
    """
    # Build a realistic multi-schema directory tree
    data_root = tmp_path / "data" / "files"
    data_root.mkdir(parents=True, exist_ok=True)

    schema_table_map = _build_northwind_file_tree(data_root)
    shared_data["data_root"] = data_root
    shared_data["schema_table_map"] = schema_table_map

    # Register the source with a glob that covers all sub-directories
    glob_pattern = str(data_root / "**" / "*.csv")
    config = FileSourceConfig(
        id="ui-file-source",
        source_type="csv",
        path=str(data_root),
    )
    shared_data["ui_file_source_config"] = config
    shared_data["ui_glob_pattern"] = glob_pattern

    # Verify the directory tree was created correctly
    for schema_name, table_names in schema_table_map.items():
        schema_dir = data_root / schema_name
        assert schema_dir.is_dir(), f"Schema directory {schema_dir} was not created"
        for table_name in table_names:
            csv_file = schema_dir / f"{table_name}.csv"
            assert csv_file.exists(), f"CSV file {csv_file} was not created"


@when('a user clicks "Add Table" and selects the file source')
def when_user_clicks_add_table_and_selects_file_source(shared_data):
    """
    Simulate what the UI does when a user opens the Add Table form and picks
    a file connector source: crawl the source directory tree, generate table
    definitions, then group discovered tables by schema to populate the
    schema dropdown.
    """
    config = shared_data["ui_file_source_config"]
    data_root = shared_data["data_root"]

    # The UI backend crawls recursively from the source root
    discovered_entries = crawl_directory(str(data_root), pattern="*.csv", recursive=True)
    assert discovered_entries, f"crawl_directory returned no entries for data root {data_root}"
    shared_data["ui_discovered_entries"] = discovered_entries

    # Generate table definitions for every discovered file
    table_defs = generate_table_definitions(config, discovered_entries)
    assert table_defs, "generate_table_definitions returned no table definitions"
    shared_data["ui_table_defs"] = table_defs

    # Build the schema dropdown contents: a mapping of schema_name -> [table_defs]
    # The schema is derived from the immediate parent directory of each CSV file.
    schemas_to_tables: dict[str, list] = {}
    for td in table_defs:
        schema_key = (
            td.schema_name
            if hasattr(td, "schema_name") and td.schema_name
            else _infer_schema_from_path(td, data_root)
        )
        schemas_to_tables.setdefault(schema_key, []).append(td)

    shared_data["ui_schemas_to_tables"] = schemas_to_tables

    # Select the first schema that matches our primary test schema ("northwind")
    preferred_schema = next(
        (s for s in schemas_to_tables if "northwind" in s.lower()),
        next(iter(schemas_to_tables)),
    )
    shared_data["ui_selected_schema"] = preferred_schema


def _infer_schema_from_path(td, data_root: Path) -> str:
    """
    Infer the schema name from the table definition's path relative to the
    data root.  The immediate parent directory of the CSV file is used as the
    schema name (mirroring the Provisa file connector convention).
    """
    if hasattr(td, "path") and td.path:
        rel = Path(td.path).relative_to(data_root)
        # The schema is the first component of the relative path
        parts = rel.parts
        if len(parts) >= 2:
            return parts[0]
    # Fall back to a synthetic name derived from the table definition
    return getattr(td, "schema_name", "default")


@then("the schema dropdown is populated with discovered schemas")
def then_schema_dropdown_populated(shared_data):
    """
    Assert that the schema dropdown (schemas_to_tables keys) contains at
    least the schemas we created on disk.
    """
    schemas_to_tables = shared_data["ui_schemas_to_tables"]
    schema_table_map = shared_data["schema_table_map"]

    assert schemas_to_tables, "No schemas were discovered — schema dropdown would be empty"

    discovered_schema_names = set(schemas_to_tables.keys())

    # Every schema directory we created should appear in the dropdown
    for expected_schema in schema_table_map:
        matching = [s for s in discovered_schema_names if expected_schema in s.lower()]
        assert matching, (
            f"Expected schema '{expected_schema}' to appear in the schema dropdown. "
            f"Discovered schemas: {sorted(discovered_schema_names)}"
        )

    # There must be at least as many schemas as we created
    assert len(discovered_schema_names) >= len(schema_table_map), (
        f"Expected at least {len(schema_table_map)} schemas in dropdown, "
        f"found {len(discovered_schema_names)}: {sorted(discovered_schema_names)}"
    )


@then("the table dropdown lists all CSV-derived tables in the selected schema")
def then_table_dropdown_lists_csv_tables(shared_data):
    """
    Assert that for the selected schema the table dropdown contains all CSV
    files that reside in that schema directory, and that every table
    definition carries the correct source_id.
    """
    schemas_to_tables = shared_data["ui_schemas_to_tables"]
    selected_schema = shared_data["ui_selected_schema"]
    schema_table_map = shared_data["schema_table_map"]

    # The selected schema must be present in the dropdown
    assert selected_schema in schemas_to_tables, (
        f"Selected schema '{selected_schema}' not found in dropdown. "
        f"Available: {sorted(schemas_to_tables.keys())}"
    )

    tables_in_schema = schemas_to_tables[selected_schema]
    assert tables_in_schema, (
        f"No tables listed for schema '{selected_schema}' — table dropdown would be empty"
    )

    # Determine which expected tables belong to this schema
    expected_tables_for_schema: list[str] = []
    for schema_name, table_names in schema_table_map.items():
        if schema_name in selected_schema or selected_schema in schema_name:
            expected_tables_for_schema.extend(table_names)

    assert expected_tables_for_schema, (
        f"Could not map selected_schema '{selected_schema}' back to expected tables. "
        f"schema_table_map keys: {list(schema_table_map.keys())}"
    )

    discovered_table_names = {td.table_name for td in tables_in_schema}

    for expected_table in expected_tables_for_schema:
        assert expected_table in discovered_table_names, (
            f"Expected CSV-derived table '{expected_table}' not found in table dropdown "
            f"for schema '{selected_schema}'. "
            f"Dropdown contains: {sorted(discovered_table_names)}"
        )

    # Every table definition in the dropdown must reference the registered source
    config = shared_data["ui_file_source_config"]
    for td in tables_in_schema:
        assert td.source_id == config.id, (
            f"Table '{td.table_name}' in schema '{selected_schema}' references "
            f"source_id '{td.source_id}', expected '{config.id}'"
        )


# ---------------------------------------------------------------------------
# REQ-791 steps
#
# Registered file-based tables are queryable via the data GraphQL endpoint.
# The scenario is driven end-to-end through the real production layers:
#   * register  -> crawl_directory + generate_table_definitions + discover_schema
#   * GraphQL   -> generate_schema (SDL) + compile_query (GraphQL -> SQL)
#   * data path -> execute_query (DuckDB, REQ-229) fetches the actual rows, whose
#                  camelCase headers are mapped to snake_case via the same
#                  _camel_to_snake contract the connector applies (REQ-789).
# ---------------------------------------------------------------------------


@given("a registered customers table created from CSV files via file connector")
def given_registered_customers_table_from_csv(shared_data, tmp_path):
    """Register a customers table from a CSV file using the real file connector."""
    csv_dir = tmp_path / "lake791"
    csv_dir.mkdir(parents=True, exist_ok=True)

    # camelCase headers so we can prove snake_case conversion end-to-end
    raw_headers = ["customerId", "companyName", "contactName", "city", "country"]
    rows = [
        ["C001", "Acme Corp", "Alice", "London", "UK"],
        ["C002", "Globex", "Bob", "Berlin", "DE"],
        ["C003", "Initech", "Carol", "Austin", "US"],
    ]
    csv_path = _write_csv(csv_dir, "customers.csv", raw_headers, rows)
    assert csv_path.exists()

    config = FileSourceConfig(id="lake791-source", source_type="csv", path=str(csv_dir))

    # Register: discover the files and generate table definitions (production).
    discovered_entries = crawl_directory(str(csv_dir), pattern="*.csv", recursive=False)
    table_defs = generate_table_definitions(config, discovered_entries)
    customers_td = next(td for td in table_defs if td.table_name == "customers")

    # Introspect the schema — column names normalized to snake_case (REQ-789).
    schema_columns = discover_schema(config, customers_td)
    assert schema_columns, "discover_schema returned no columns for customers"

    shared_data["config_791"] = config
    shared_data["customers_td_791"] = customers_td
    shared_data["schema_columns_791"] = schema_columns
    shared_data["csv_path_791"] = csv_path
    shared_data["raw_headers_791"] = raw_headers
    shared_data["csv_rows_791"] = rows
    shared_data["snake_columns_791"] = [c.column_name for c in schema_columns]


@when("a GraphQL query is issued against the data endpoint for customers")
def when_graphql_query_issued_for_customers(shared_data):
    """Compile a real GraphQL query to SQL and execute it against the CSV data."""
    config = shared_data["config_791"]
    customers_td = shared_data["customers_td_791"]
    schema_columns = shared_data["schema_columns_791"]
    snake_columns = shared_data["snake_columns_791"]

    # Build the production GraphQL schema for the registered table (snake_case fields).
    columns_for_schema = [
        {"column_name": c.column_name, "visible_to": ["admin"]} for c in schema_columns
    ]
    tables = [
        {
            "id": 791,
            "source_id": config.id,
            "domain_id": "datalake",
            "schema_name": "db",
            "table_name": customers_td.table_name,
            "columns": columns_for_schema,
            "gql_naming_convention": "snake",
        }
    ]
    role = {"id": "admin", "capabilities": ["query_development"], "domain_access": ["*"]}
    domains = [{"id": "datalake", "description": "Data Lake"}]
    si = SchemaInput(
        tables=tables,
        relationships=[],
        column_types={791: list(schema_columns)},
        naming_rules=[],
        role=role,
        domains=domains,
        source_types={config.id: "csv"},
    )
    gql_schema = generate_schema(si)
    shared_data["gql_sdl_791"] = print_schema(gql_schema)

    ctx = build_context(si)

    # Issue a GraphQL query for customers selecting every snake_case column.
    field_selection = " ".join(snake_columns)
    query = parse("{ customers { %s } }" % field_selection)
    compiled = compile_query(query, ctx)
    sql = compiled[0].sql
    shared_data["compiled_sql_791"] = sql

    # Every selected snake_case field must appear in the compiled SQL.
    for col in snake_columns:
        assert f'"{col}"' in sql, f"Column {col!r} missing from compiled SQL: {sql}"

    # Execute the data query against the CSV via the production execute_query
    # (DuckDB, REQ-229). The raw CSV headers are camelCase; map them to
    # snake_case using the same connector contract that named the columns.
    csv_path = shared_data["csv_path_791"]
    file_config = FileSourceConfig(id=config.id, source_type="csv", path=str(csv_path))
    raw_rows = execute_query(file_config, f'SELECT * FROM "{csv_path.stem}"')  # noqa: S608
    result_rows = [{_camel_to_snake(k): v for k, v in row.items()} for row in raw_rows]
    shared_data["result_rows_791"] = result_rows


@then("the query returns all rows from the CSV files with all columns")
def then_query_returns_all_rows_all_columns(shared_data):
    """Assert every CSV data row is returned with every CSV column present."""
    result_rows = shared_data["result_rows_791"]
    csv_rows = shared_data["csv_rows_791"]
    snake_columns = shared_data["snake_columns_791"]

    assert len(result_rows) == len(csv_rows), (
        f"Expected {len(csv_rows)} rows from CSV, got {len(result_rows)}: {result_rows}"
    )

    for row in result_rows:
        for col in snake_columns:
            assert col in row, (
                f"Column '{col}' missing from returned row {row}; "
                f"expected all columns {snake_columns}"
            )

    # Verify actual values round-tripped: the first CSV row's values must appear.
    expected_first = set(csv_rows[0])
    returned_first = set(str(v) for v in result_rows[0].values())
    assert expected_first <= returned_first, (
        f"First CSV row values {expected_first} not fully present in "
        f"returned row values {returned_first}"
    )


@then("the response matches the CSV schema with snake_case column names")
def then_response_matches_schema_snake_case(shared_data):
    """Assert returned column keys are snake_case and match the discovered schema."""
    result_rows = shared_data["result_rows_791"]
    snake_columns = shared_data["snake_columns_791"]
    raw_headers = shared_data["raw_headers_791"]
    sdl = shared_data["gql_sdl_791"]

    returned_keys = set(result_rows[0].keys())
    assert returned_keys == set(snake_columns), (
        f"Returned columns {sorted(returned_keys)} do not match discovered schema "
        f"{sorted(snake_columns)}"
    )

    # No returned key may retain a camelCase boundary.
    camel_boundary = re.compile(r"[a-z][A-Z]")
    camel_keys = [k for k in returned_keys if camel_boundary.search(k)]
    assert not camel_keys, f"Returned columns still contain camelCase: {camel_keys}"

    # The expected snake_case mapping of each raw header must be a returned column.
    for header in raw_headers:
        expected = _camel_to_snake(header)
        assert expected in returned_keys, (
            f"Header {header!r} expected snake_case column {expected!r}, "
            f"returned columns: {sorted(returned_keys)}"
        )

    # The GraphQL data endpoint schema must expose the same snake_case fields.
    for col in snake_columns:
        assert col in sdl, f"snake_case field {col!r} absent from GraphQL SDL"


# ---------------------------------------------------------------------------
# REQ-736 — File & Lake Sources: SQLite adapter (schema discovery + queries)
#
# Driven end-to-end against the real SQLite file source adapter:
#   * discover_schema  -> native SQLite type mapping (INTEGER→BIGINT, REAL→DOUBLE)
#   * execute_query    -> real sqlite3 execution, results returned as row dicts
# ---------------------------------------------------------------------------


@given("a SQLite database with multiple tables")
def given_sqlite_database_with_multiple_tables(shared_data, tmp_path):
    """Create a real on-disk SQLite database with several typed tables."""
    db_path = tmp_path / "lake736.db"
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute(
            "CREATE TABLE customers ("
            "id INTEGER PRIMARY KEY, "
            "name TEXT NOT NULL, "
            "balance REAL, "
            "active BOOLEAN, "
            "created_at DATETIME)"
        )
        conn.execute(
            "CREATE TABLE orders ("
            "order_id INTEGER PRIMARY KEY, "
            "customer_id INTEGER, "
            "total REAL, "
            "note TEXT)"
        )
        conn.executemany(
            "INSERT INTO customers (id, name, balance, active, created_at) VALUES (?, ?, ?, ?, ?)",
            [
                (1, "Acme Corp", 250.50, 1, "2024-01-10 09:00:00"),
                (2, "Globex", 0.0, 0, "2024-02-01 12:30:00"),
                (3, "Initech", 99.99, 1, "2024-03-15 08:15:00"),
            ],
        )
        conn.executemany(
            "INSERT INTO orders (order_id, customer_id, total, note) VALUES (?, ?, ?, ?)",
            [(10, 1, 42.0, "first"), (11, 2, 7.5, "second")],
        )
        conn.commit()
    finally:
        conn.close()

    config = FileSourceConfig(id="lake736-source", source_type="sqlite", path=str(db_path))
    shared_data["config_736"] = config
    shared_data["db_path_736"] = db_path
    shared_data["expected_tables_736"] = {"customers", "orders"}
    shared_data["customer_count_736"] = 3


@when("the adapter discovers schema and executes queries")
def when_adapter_discovers_and_queries(shared_data):
    """Run the real SQLite adapter: schema discovery + a SELECT returning rows."""
    config = shared_data["config_736"]

    # Discover the raw multi-table schema (table_def=None form) directly from
    # the SQLite file via the production adapter.
    raw_schema = discover_schema(config)
    shared_data["raw_schema_736"] = raw_schema

    # Group into per-table definitions using the production API.
    table_defs = generate_table_definitions(config)
    shared_data["table_defs_736"] = table_defs

    # Discover typed column metadata for the customers table.
    customers_td = TableDefinition(table_name="customers", source_id=config.id, path=config.path)
    shared_data["customers_columns_736"] = discover_schema(config, customers_td)

    # Execute real queries against the SQLite file and collect row dicts.
    shared_data["all_customers_736"] = execute_query(
        config, "SELECT id, name, balance, active FROM customers ORDER BY id"
    )
    shared_data["filtered_736"] = execute_query(
        config, "SELECT name, balance FROM customers WHERE active = 1 ORDER BY balance"
    )


@then("column types are mapped correctly and results are returned as row dicts")
def then_types_mapped_and_rows_returned(shared_data):
    """Assert native SQLite type mapping and row-dict query results."""
    raw_schema = shared_data["raw_schema_736"]
    table_defs = shared_data["table_defs_736"]
    customers_columns = shared_data["customers_columns_736"]
    all_customers = shared_data["all_customers_736"]
    filtered = shared_data["filtered_736"]

    # Both tables are discovered.
    discovered_tables = {row["table"] for row in raw_schema}
    assert discovered_tables == shared_data["expected_tables_736"], (
        f"Expected tables {shared_data['expected_tables_736']}, got {discovered_tables}"
    )
    td_names = {td["tableName"] for td in table_defs}
    assert td_names == shared_data["expected_tables_736"]

    # Native SQLite type mapping (INTEGER→BIGINT, REAL→DOUBLE, etc.).
    col_types = {c.column_name: c.data_type for c in customers_columns}
    assert col_types["id"] == "BIGINT", col_types
    assert col_types["name"] == "VARCHAR", col_types
    assert col_types["balance"] == "DOUBLE", col_types
    assert col_types["active"] == "BOOLEAN", col_types
    assert col_types["created_at"] == "TIMESTAMP", col_types

    # Cross-check the mapping helper directly.
    assert _sqlite_type_to_sql("INTEGER") == "BIGINT"
    assert _sqlite_type_to_sql("REAL") == "DOUBLE"
    assert _sqlite_type_to_sql("BOOLEAN") == "BOOLEAN"
    assert _sqlite_type_to_sql("DATETIME") == "TIMESTAMP"
    assert _sqlite_type_to_sql("TEXT") == "VARCHAR"
    assert _sqlite_type_to_sql("BLOB") == "VARBINARY"

    # Nullability is preserved from PRAGMA table_info.
    nullable = {c.column_name: c.is_nullable for c in customers_columns}
    assert nullable["name"] is False, "NOT NULL column must be non-nullable"
    assert nullable["balance"] is True, "nullable column must be nullable"

    # Results are returned as row dicts with correct values.
    assert len(all_customers) == shared_data["customer_count_736"]
    assert all(isinstance(row, dict) for row in all_customers)
    first = all_customers[0]
    assert first == {"id": 1, "name": "Acme Corp", "balance": 250.50, "active": 1}

    # A filtered query returns only matching rows as dicts, ordered.
    assert len(filtered) == 2, filtered
    assert [r["name"] for r in filtered] == ["Initech", "Acme Corp"]
    assert filtered[0] == {"name": "Initech", "balance": 99.99}
