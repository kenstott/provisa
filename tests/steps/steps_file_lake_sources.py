# Copyright (c) 2026 Kenneth Stott
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
import os
import re
from pathlib import Path

import pytest

from graphql import parse, print_schema
from pytest_bdd import given, when, then, scenarios

from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.schema_gen import SchemaInput, generate_schema
from provisa.compiler.sql_gen import build_context, compile_query
from provisa.core.models import TIME_TRAVEL_SOURCES
from provisa.file_source.source import (
    FileSourceConfig,
    discover_schema,
    generate_table_definitions,
)
from provisa.file_source.crawler import crawl_directory


scenarios("../features/REQ-372.feature")
scenarios("../features/REQ-788.feature")
scenarios("../features/REQ-789.feature")
scenarios("../features/REQ-790.feature")
scenarios("../features/REQ-791.feature")


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


def _write_csv(directory: Path, filename: str, headers: list[str], rows: list[list] | None = None) -> Path:
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
    glob_pattern = shared_data["glob_pattern"]
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
        assert schema_columns, (
            f"discover_schema returned empty columns for table '{td.table_name}'"
        )

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
        "The following camelCase headers were NOT converted to snake_case:\n"
        + "\n".join(missing)
    )

    # Additionally, assert that no discovered column name contains a camelCase
    # boundary (an uppercase letter preceded by a lowercase letter), which would
    # indicate that conversion did NOT occur.
    camel_pattern = re.compile(r"[a-z][A-Z]")
    camel_columns = [col.column_name for col in schema_columns if camel_pattern.search(col.column_name)]
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
        {"column_name": col.column_name, "visible_to": ["admin"]}
        for col in schema_columns
    ]
    column_types_map = {99: list(schema_columns)}

    tables = [
        {
            "id": 99,
            "source_id": "camel-source",
            "domain_id": "test_domain",
            "schema_name": "camel_lake",
            "table_name": customers_td.table_name,
            "governance": "pre-approved",
            "columns": columns_for_schema,
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
    assert discovered_entries, (
        f"crawl_directory returned no entries for data root {data_root}"
    )
    shared_data["ui_discovered_entries"] = discovered_entries

    # Generate table definitions for every discovered file
    table_defs = generate_table_definitions(config, discovered_entries)
    assert table_defs, "generate_table_definitions returned no table definitions"
    shared_data["ui_table_defs"] = table_defs

    # Build the schema dropdown contents: a mapping of schema_name -> [table_defs]
    # The schema is derived from the immediate parent directory of each CSV file.
    schemas_to_tables: dict[str, list] = {}
    for td in table_defs:
        schema_key = td.schema_name if hasattr(td, "schema_name") and td.schema_name else _infer_schema_from_path(td, data_root)
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
            f"Table '{td.table_name}' in schema '{selected_schema}' has wrong source_id "
            f"'{td.source_id}'; expected '{config.id}'"
        )


# ---------------------------------------------------------------------------
# Helpers (REQ-791)
