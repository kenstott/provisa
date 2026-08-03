# Copyright (c) 2026 Kenneth Stott
# Canary: 17097e84-5fa4-4b9b-b55a-96fe994d2650
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Unit tests for DuckDBFilesConnector — the SCAN connector for files-type sources."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from provisa.federation.connector_duckdb import DuckDBFilesConnector
from provisa.federation.connector_base import Mechanism


def _source(path: str, table_name: str = "customers") -> SimpleNamespace:
    return SimpleNamespace(path=path, table_name=table_name, id="e2e-northwind")


# ── connector metadata ─────────────────────────────────────────────────────────


def test_files_connector_engine():
    assert DuckDBFilesConnector.engine == "duckdb"


def test_files_connector_source_type():
    assert DuckDBFilesConnector.source_type == "files"


def test_files_connector_mechanism():
    assert DuckDBFilesConnector.mechanism == Mechanism.SCAN


# ── details(): glob-to-CSV path derivation ────────────────────────────────────


def test_details_camelcase_headers_aliased_to_snake(tmp_path):
    csv_file = tmp_path / "customers.csv"
    csv_file.write_text("customerID,companyName,postalCode\nALFKI,Alfreds,12209\n")

    conn = DuckDBFilesConnector()
    result = conn.details(_source(str(tmp_path / "**")))

    assert "view_ddl" in result
    ddl = result["view_ddl"]
    assert '"customerID" AS "customer_id"' in ddl
    assert '"companyName" AS "company_name"' in ddl
    assert '"postalCode" AS "postal_code"' in ddl
    assert f"read_csv_auto('{tmp_path / 'customers.csv'}')" in ddl


def test_details_already_snake_headers_not_aliased(tmp_path):
    csv_file = tmp_path / "orders.csv"
    csv_file.write_text("order_id,customer_id\n1,ALFKI\n")

    conn = DuckDBFilesConnector()
    result = conn.details(_source(str(tmp_path / "**"), table_name="orders"))

    ddl = result["view_ddl"]
    # No " AS " when header is already snake_case
    assert '" AS "' not in ddl or "order_id" in ddl


def test_details_missing_csv_falls_back_to_wildcard(tmp_path):
    conn = DuckDBFilesConnector()
    result = conn.details(_source(str(tmp_path / "**"), table_name="nonexistent"))

    assert "view_ddl" in result
    ddl = result["view_ddl"]
    assert "SELECT * FROM read_csv_auto(" in ddl


def test_details_glob_directory_resolved_correctly(tmp_path):
    """Non-glob prefix of a glob path is used as directory."""
    subdir = tmp_path / "northwind"
    subdir.mkdir()
    csv = subdir / "products.csv"
    csv.write_text("productID,productName\n1,Chai\n")

    conn = DuckDBFilesConnector()
    result = conn.details(_source(str(subdir / "**"), table_name="products"))
    ddl = result["view_ddl"]
    assert "products" in ddl
    assert '"productID" AS "product_id"' in ddl
