# Copyright (c) 2025 Kenneth Stott
# Canary: f7d57b70-894f-4d1b-96b9-94957268f424
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for schema visibility enforcement."""

from provisa.security.visibility import (
    is_column_visible,
    visible_column_names,
    visible_tables,
)


def _table(name="orders", domain="sales", columns=None):
    return {
        "id": 1,
        "source_id": "pg1",
        "domain_id": domain,
        "schema_name": "public",
        "table_name": name,
        "columns": columns or [
            {"column_name": "id", "visible_to": ["admin", "analyst"]},
            {"column_name": "amount", "visible_to": ["admin"]},
            {"column_name": "secret", "visible_to": ["admin"]},
        ],
    }


class TestVisibleTables:
    def test_wildcard_domain_access(self):
        role = {"id": "admin", "domain_access": ["*"]}
        tables = [_table()]
        result = visible_tables(tables, role)
        assert len(result) == 1

    def test_domain_filtered(self):
        role = {"id": "analyst", "domain_access": ["sales"]}
        tables = [_table(domain="sales"), _table(name="secret", domain="internal")]
        result = visible_tables(tables, role)
        assert len(result) == 1
        assert result[0]["table_name"] == "orders"

    def test_no_visible_columns_excluded(self):
        role = {"id": "nobody", "domain_access": ["*"]}
        tables = [_table(columns=[
            {"column_name": "id", "visible_to": ["admin"]},
        ])]
        result = visible_tables(tables, role)
        assert len(result) == 0

    def test_columns_filtered_by_role(self):
        role = {"id": "analyst", "domain_access": ["*"]}
        tables = [_table()]
        result = visible_tables(tables, role)
        col_names = [c["column_name"] for c in result[0]["columns"]]
        assert "id" in col_names
        assert "amount" not in col_names
        assert "secret" not in col_names


class TestIsColumnVisible:
    def test_visible(self):
        table = _table()
        assert is_column_visible(table, "id", "analyst")

    def test_not_visible(self):
        table = _table()
        assert not is_column_visible(table, "amount", "analyst")

    def test_unknown_column(self):
        table = _table()
        assert not is_column_visible(table, "nonexistent", "admin")


class TestVisibleColumnNames:
    def test_admin_sees_all(self):
        table = _table()
        names = visible_column_names(table, "admin")
        assert names == {"id", "amount", "secret"}

    def test_analyst_sees_subset(self):
        table = _table()
        names = visible_column_names(table, "analyst")
        assert names == {"id"}
