# Copyright (c) 2026 Kenneth Stott
# Canary: f7d57b70-894f-4d1b-96b9-94957268f424
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for schema visibility enforcement."""

from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.schema_gen import SchemaInput, generate_schema
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


def _mutable_table(tid, name, domain, source="pg1"):
    return {
        "id": tid,
        "source_id": source,
        "domain_id": domain,
        "schema_name": "public",
        "table_name": name,
        "governance": "pre-approved",
        "columns": [{"column_name": "id", "visible_to": ["admin"]}],
    }


class TestDomainFilteredMutations:
    """A domain-filtered schema must only expose mutations for the requested (seed) domains,
    not for reachable-via-JOIN tables in other domains or always-visible meta/ops tables."""

    def _schema(self, root_ids):
        tables = [
            _mutable_table(1, "orders", "sales"),
            _mutable_table(2, "customers", "crm"),
            _mutable_table(3, "registered_tables", "meta"),
        ]
        col_types = {
            t["id"]: [ColumnMetadata(column_name="id", data_type="integer", is_nullable=False)]
            for t in tables
        }
        si = SchemaInput(
            tables=tables,
            root_table_ids=root_ids,
            relationships=[],
            column_types=col_types,
            naming_rules=[],
            role={"id": "admin", "capabilities": ["admin"], "domain_access": ["sales", "crm"]},
            domains=[
                {"id": "sales", "description": "Sales"},
                {"id": "crm", "description": "CRM"},
                {"id": "meta", "description": "Meta"},
            ],
            source_types={"pg1": "postgresql"},
        )
        return generate_schema(si)

    def test_only_seed_domain_mutations(self):
        # Only the sales table (id=1) is a seed/root; crm + meta are reachable/always-visible.
        schema = self._schema(root_ids={1})
        assert schema.mutation_type is not None
        fields = schema.mutation_type.fields
        assert "insertOrders" in fields
        assert "insertCustomers" not in fields
        assert "insertRegisteredTables" not in fields

    def test_full_schema_mutations_unfiltered(self):
        # No root_table_ids (full role schema) keeps mutations for accessible domains.
        schema = self._schema(root_ids=None)
        assert schema.mutation_type is not None
        fields = schema.mutation_type.fields
        assert "insertOrders" in fields
        assert "insertCustomers" in fields
        # meta is an implicit-traversal domain the role lacks explicit access to → no mutations.
        assert "insertRegisteredTables" not in fields


class TestViewMutationsSuppressed:
    """REQ-1157: a view_sql/MV-backed relation is query-only — no insert/update/delete mutations,
    while its read (query) surface is unaffected."""

    def _schema(self):
        base = _mutable_table(1, "orders", "sales")
        view = _mutable_table(2, "daily_totals", "sales")
        view["view_sql"] = "SELECT count(*) AS id FROM orders"  # marks it derived → read_only
        tables = [base, view]
        col_types = {
            t["id"]: [ColumnMetadata(column_name="id", data_type="integer", is_nullable=False)]
            for t in tables
        }
        si = SchemaInput(
            tables=tables,
            relationships=[],
            column_types=col_types,
            naming_rules=[],
            role={"id": "admin", "capabilities": ["admin"], "domain_access": ["*"]},
            domains=[{"id": "sales", "description": "Sales"}],
            source_types={"pg1": "postgresql"},
        )
        return generate_schema(si)

    def test_view_has_no_mutations(self):
        schema = self._schema()
        assert schema.mutation_type is not None
        fields = schema.mutation_type.fields
        # base table keeps its write surface
        assert "insertOrders" in fields
        # view/MV gets NO write surface
        assert not any("DailyTotals" in f for f in fields)

    def test_view_still_queryable(self):
        schema = self._schema()
        assert schema.query_type is not None
        qfields = schema.query_type.fields
        assert any("dailyTotals" in f or "daily_totals" in f for f in qfields)
