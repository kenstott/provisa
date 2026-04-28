# Copyright (c) 2026 Kenneth Stott
# Canary: f1a2b3c4-d5e6-7f8a-9b0c-1d2e3f4a5b6c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for schema visibility filter logic (REQ-430, REQ-431, REQ-432).

Tests _filter_tables_by_schema_cfg in isolation — no database, no async.
Also tests Source.allowed_domains model field parsing.
"""

from __future__ import annotations

import pytest

from provisa.api.app import _filter_tables_by_schema_cfg
from provisa.core.models import Source


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _table(table_name: str, domain_id: str, source_id: str = "pg-main") -> dict:
    return {"table_name": table_name, "domain_id": domain_id, "source_id": source_id}


def _ops_tables() -> list[dict]:
    return [
        _table("traces", "ops", "provisa-otel"),
        _table("metrics", "ops", "provisa-otel"),
        _table("logs", "ops", "provisa-otel"),
        _table("provisa_queries", "ops", "provisa-otel"),
    ]


def _mixed_tables() -> list[dict]:
    return [
        _table("orders", "sales", "pg-main"),
        _table("customers", "crm", "pg-main"),
        _table("traces", "ops", "provisa-otel"),
        _table("metrics", "ops", "provisa-otel"),
        _table("logs", "ops", "provisa-otel"),
    ]


# ---------------------------------------------------------------------------
# schema.include_ops
# ---------------------------------------------------------------------------


class TestIncludeOps:
    def test_include_ops_true_keeps_all(self):
        tables = _mixed_tables()
        result = _filter_tables_by_schema_cfg(tables, {"include_ops": True}, {})
        assert len(result) == len(tables)

    def test_include_ops_omitted_defaults_true(self):
        tables = _mixed_tables()
        result = _filter_tables_by_schema_cfg(tables, {}, {})
        assert len(result) == len(tables)

    def test_include_ops_false_removes_ops_domain(self):
        tables = _mixed_tables()
        result = _filter_tables_by_schema_cfg(tables, {"include_ops": False}, {})
        domain_ids = {t["domain_id"] for t in result}
        assert "ops" not in domain_ids

    def test_include_ops_false_keeps_non_ops(self):
        tables = _mixed_tables()
        result = _filter_tables_by_schema_cfg(tables, {"include_ops": False}, {})
        names = {t["table_name"] for t in result}
        assert "orders" in names
        assert "customers" in names

    def test_include_ops_false_removes_all_four_ops_tables(self):
        tables = _ops_tables()
        result = _filter_tables_by_schema_cfg(tables, {"include_ops": False}, {})
        assert result == []

    def test_include_ops_false_no_tables_empty_result(self):
        result = _filter_tables_by_schema_cfg([], {"include_ops": False}, {})
        assert result == []


# ---------------------------------------------------------------------------
# schema.include_metrics
# ---------------------------------------------------------------------------


class TestIncludeMetrics:
    def test_include_metrics_true_keeps_all(self):
        tables = _mixed_tables()
        result = _filter_tables_by_schema_cfg(tables, {"include_metrics": True}, {})
        assert len(result) == len(tables)

    def test_include_metrics_false_removes_only_metrics_table(self):
        tables = _mixed_tables()
        result = _filter_tables_by_schema_cfg(tables, {"include_metrics": False}, {})
        table_names = {t["table_name"] for t in result}
        assert "metrics" not in table_names

    def test_include_metrics_false_keeps_other_ops_tables(self):
        tables = _mixed_tables()
        result = _filter_tables_by_schema_cfg(tables, {"include_metrics": False}, {})
        table_names = {t["table_name"] for t in result}
        assert "traces" in table_names
        assert "logs" in table_names

    def test_include_metrics_false_keeps_non_ops_tables(self):
        tables = _mixed_tables()
        result = _filter_tables_by_schema_cfg(tables, {"include_metrics": False}, {})
        table_names = {t["table_name"] for t in result}
        assert "orders" in table_names
        assert "customers" in table_names

    def test_include_metrics_false_non_ops_metrics_table_kept(self):
        """A table named 'metrics' in a non-ops domain must NOT be removed."""
        tables = [
            _table("metrics", "analytics", "pg-main"),
            _table("metrics", "ops", "provisa-otel"),
        ]
        result = _filter_tables_by_schema_cfg(tables, {"include_metrics": False}, {})
        assert len(result) == 1
        assert result[0]["domain_id"] == "analytics"

    def test_include_ops_false_takes_precedence_over_include_metrics(self):
        """When include_ops is false, include_metrics has no additional effect."""
        tables = _mixed_tables()
        result_ops_false = _filter_tables_by_schema_cfg(
            tables, {"include_ops": False}, {}
        )
        result_both_false = _filter_tables_by_schema_cfg(
            tables, {"include_ops": False, "include_metrics": False}, {}
        )
        assert result_ops_false == result_both_false

    def test_include_ops_true_include_metrics_false_removes_only_metrics(self):
        tables = _ops_tables()
        result = _filter_tables_by_schema_cfg(
            tables, {"include_ops": True, "include_metrics": False}, {}
        )
        names = {t["table_name"] for t in result}
        assert "metrics" not in names
        assert "traces" in names
        assert "logs" in names
        assert "provisa_queries" in names


# ---------------------------------------------------------------------------
# source allowed_domains filtering
# ---------------------------------------------------------------------------


class TestSourceAllowedDomains:
    def test_no_restrictions_all_tables_pass(self):
        tables = _mixed_tables()
        result = _filter_tables_by_schema_cfg(tables, {}, {})
        assert len(result) == len(tables)

    def test_empty_allowed_list_treated_as_unrestricted(self):
        tables = _mixed_tables()
        result = _filter_tables_by_schema_cfg(tables, {}, {"pg-main": []})
        assert len(result) == len(tables)

    def test_allowed_domains_filters_out_wrong_domain(self):
        tables = [
            _table("orders", "sales", "pg-main"),
            _table("employees", "hr", "pg-main"),
        ]
        result = _filter_tables_by_schema_cfg(tables, {}, {"pg-main": ["sales"]})
        assert len(result) == 1
        assert result[0]["table_name"] == "orders"

    def test_allowed_domains_keeps_matching_domain(self):
        tables = [
            _table("orders", "sales", "pg-main"),
            _table("customers", "sales", "pg-main"),
        ]
        result = _filter_tables_by_schema_cfg(tables, {}, {"pg-main": ["sales"]})
        assert len(result) == 2

    def test_allowed_domains_multiple_domains_allowed(self):
        tables = [
            _table("orders", "sales", "pg-main"),
            _table("invoices", "finance", "pg-main"),
            _table("employees", "hr", "pg-main"),
        ]
        result = _filter_tables_by_schema_cfg(
            tables, {}, {"pg-main": ["sales", "finance"]}
        )
        names = {t["table_name"] for t in result}
        assert "orders" in names
        assert "invoices" in names
        assert "employees" not in names

    def test_restriction_only_applies_to_named_source(self):
        tables = [
            _table("orders", "sales", "pg-main"),
            _table("employees", "hr", "pg-secondary"),
        ]
        result = _filter_tables_by_schema_cfg(
            tables, {}, {"pg-main": ["sales"]}
        )
        # pg-secondary has no restriction, so hr table passes through
        assert len(result) == 2

    def test_multiple_sources_each_restricted_independently(self):
        tables = [
            _table("orders", "sales", "pg-main"),
            _table("employees", "hr", "pg-main"),
            _table("invoices", "finance", "pg-secondary"),
            _table("payroll", "hr", "pg-secondary"),
        ]
        result = _filter_tables_by_schema_cfg(
            tables,
            {},
            {"pg-main": ["sales"], "pg-secondary": ["finance"]},
        )
        names = {t["table_name"] for t in result}
        assert "orders" in names
        assert "invoices" in names
        assert "employees" not in names
        assert "payroll" not in names

    def test_source_restriction_combined_with_include_ops_false(self):
        tables = [
            _table("orders", "sales", "pg-main"),
            _table("employees", "hr", "pg-main"),
            _table("traces", "ops", "provisa-otel"),
        ]
        result = _filter_tables_by_schema_cfg(
            tables,
            {"include_ops": False},
            {"pg-main": ["sales"]},
        )
        names = {t["table_name"] for t in result}
        assert "orders" in names
        assert "employees" not in names
        assert "traces" not in names


# ---------------------------------------------------------------------------
# Source.allowed_domains model field
# ---------------------------------------------------------------------------


class TestSourceAllowedDomainsField:
    def _src(self, **kwargs) -> Source:
        base = {
            "id": "test-src",
            "type": "postgresql",
            "host": "localhost",
            "port": 5432,
            "database": "db",
            "username": "u",
        }
        base.update(kwargs)
        return Source(**base)

    def test_allowed_domains_defaults_to_empty_list(self):
        src = self._src()
        assert src.allowed_domains == []

    def test_allowed_domains_single_entry(self):
        src = self._src(allowed_domains=["sales"])
        assert src.allowed_domains == ["sales"]

    def test_allowed_domains_multiple_entries(self):
        src = self._src(allowed_domains=["sales", "finance", "hr"])
        assert src.allowed_domains == ["sales", "finance", "hr"]

    def test_allowed_domains_empty_list_explicit(self):
        src = self._src(allowed_domains=[])
        assert src.allowed_domains == []
