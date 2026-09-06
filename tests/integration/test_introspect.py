# Copyright (c) 2026 Kenneth Stott
# Canary: 41d4643c-10e0-46a2-bb03-87e73671fb1b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration tests for Trino INFORMATION_SCHEMA introspection."""

import os
import time

import pytest
import trino.dbapi

from provisa.compiler.introspect import (
    ColumnMetadata,
    introspect_fk_candidates,
    introspect_table_columns,
)
from provisa.core.catalog import create_catalog
from provisa.core.models import Source, SourceType

pytestmark = [pytest.mark.integration]


@pytest.fixture(scope="module", autouse=True)
def _wait_for_trino():
    """Wait for Trino to finish initializing before running Trino tests."""
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        try:
            conn = trino.dbapi.connect(
                host="localhost", port=int(os.environ.get("TRINO_PORT", "8080")), user="test"
            )
            cur = conn.cursor()
            cur.execute("SELECT 1")
            cur.fetchall()
            conn.close()
            return
        except Exception:
            time.sleep(2)
    raise RuntimeError("Trino did not become ready within 120s")


@pytest.fixture(scope="module", autouse=True)
def _sales_pg_catalog(_wait_for_trino):
    """Provision the ``sales_pg`` Trino catalog this module introspects.

    Must not depend on some other module (e.g. test_schema_gen.py's ``_load_config``) having
    registered it first — that hidden cross-file ordering dependency produces CATALOG_NOT_FOUND
    whenever this module happens to run before one that does the registration (see
    test_schema_gen.py's ``_load_config`` docstring for the same defect class, fixed there the
    same way: provision the catalog this module needs directly).
    """
    source = Source(
        id="sales-pg",
        type=SourceType.postgresql,
        host=os.environ.get("PG_HOST", "localhost"),
        port=int(os.environ.get("PG_PORT", "5432")),
        database=os.environ.get("PG_DATABASE", "provisa"),
        username=os.environ.get("PG_USER", "provisa"),
        password=os.environ.get("PG_PASSWORD", "provisa"),
    )
    conn = trino.dbapi.connect(
        host=os.environ.get("TRINO_HOST", "localhost"),
        port=int(os.environ.get("TRINO_PORT", "8080")),
        user="test",
    )
    try:
        create_catalog(conn, source, resolved_password=source.password)
    finally:
        conn.close()


class TestIntrospectTableColumns:
    def test_orders_columns(self, trino_conn):
        columns = introspect_table_columns(trino_conn, "sales_pg", "public", "orders")
        names = [c.column_name for c in columns]
        assert "id" in names
        assert "customer_id" in names
        assert "amount" in names
        assert "region" in names
        assert "status" in names
        assert "created_at" in names

    def test_column_types(self, trino_conn):
        columns = introspect_table_columns(trino_conn, "sales_pg", "public", "orders")
        col_map = {c.column_name: c for c in columns}
        assert col_map["id"].data_type == "integer"
        assert col_map["amount"].data_type.startswith("decimal")
        assert col_map["region"].data_type.startswith("varchar")

    def test_nullability(self, trino_conn):
        columns = introspect_table_columns(trino_conn, "sales_pg", "public", "orders")
        col_map = {c.column_name: c for c in columns}
        # id is NOT NULL (serial primary key)
        assert col_map["id"].is_nullable is False
        # region is NOT NULL per schema
        assert col_map["region"].is_nullable is False

    def test_customers_columns(self, trino_conn):
        columns = introspect_table_columns(trino_conn, "sales_pg", "public", "customers")
        names = [c.column_name for c in columns]
        assert "id" in names
        assert "name" in names
        assert "email" in names
        assert "region" in names

    def test_products_columns(self, trino_conn):
        columns = introspect_table_columns(trino_conn, "sales_pg", "public", "products")
        names = [c.column_name for c in columns]
        assert "id" in names
        assert "name" in names
        assert "price" in names
        assert "category" in names

    def test_nonexistent_table_returns_empty(self, trino_conn):
        columns = introspect_table_columns(trino_conn, "sales_pg", "public", "nonexistent")
        assert columns == []

    def test_returns_column_metadata_type(self, trino_conn):
        columns = introspect_table_columns(trino_conn, "sales_pg", "public", "orders")
        assert all(isinstance(c, ColumnMetadata) for c in columns)


class TestIntrospectFKCandidates:
    def test_fk_candidates_returns_list(self, trino_conn):
        """FK introspection may not be supported by all connectors."""
        result = introspect_fk_candidates(trino_conn, "sales_pg", "public", "orders")
        assert isinstance(result, list)
