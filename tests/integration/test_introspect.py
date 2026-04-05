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

import pytest

from provisa.compiler.introspect import (
    ColumnMetadata,
    introspect_fk_candidates,
    introspect_table_columns,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]


class TestIntrospectTableColumns:
    def test_orders_columns(self, trino_conn):
        columns = introspect_table_columns(trino_conn, "postgresql", "public", "orders")
        names = [c.column_name for c in columns]
        assert "id" in names
        assert "customer_id" in names
        assert "amount" in names
        assert "region" in names
        assert "status" in names
        assert "created_at" in names

    def test_column_types(self, trino_conn):
        columns = introspect_table_columns(trino_conn, "postgresql", "public", "orders")
        col_map = {c.column_name: c for c in columns}
        assert col_map["id"].data_type == "integer"
        assert col_map["amount"].data_type.startswith("decimal")
        assert col_map["region"].data_type.startswith("varchar")

    def test_nullability(self, trino_conn):
        columns = introspect_table_columns(trino_conn, "postgresql", "public", "orders")
        col_map = {c.column_name: c for c in columns}
        # id is NOT NULL (serial primary key)
        assert col_map["id"].is_nullable is False
        # region is NOT NULL per schema
        assert col_map["region"].is_nullable is False

    def test_customers_columns(self, trino_conn):
        columns = introspect_table_columns(trino_conn, "postgresql", "public", "customers")
        names = [c.column_name for c in columns]
        assert "id" in names
        assert "name" in names
        assert "email" in names
        assert "region" in names

    def test_products_columns(self, trino_conn):
        columns = introspect_table_columns(trino_conn, "postgresql", "public", "products")
        names = [c.column_name for c in columns]
        assert "id" in names
        assert "name" in names
        assert "price" in names
        assert "category" in names

    def test_nonexistent_table_returns_empty(self, trino_conn):
        columns = introspect_table_columns(trino_conn, "postgresql", "public", "nonexistent")
        assert columns == []

    def test_returns_column_metadata_type(self, trino_conn):
        columns = introspect_table_columns(trino_conn, "postgresql", "public", "orders")
        assert all(isinstance(c, ColumnMetadata) for c in columns)


class TestIntrospectFKCandidates:
    def test_fk_candidates_returns_list(self, trino_conn):
        """FK introspection may not be supported by all connectors."""
        result = introspect_fk_candidates(trino_conn, "postgresql", "public", "orders")
        assert isinstance(result, list)
