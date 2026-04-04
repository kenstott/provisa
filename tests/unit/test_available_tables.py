# Copyright (c) 2025 Kenneth Stott
# Canary: 24ad7984-89e0-43c5-9508-31888e884869
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for available_tables query returning table metadata with comments."""

import pytest

from provisa.api.admin.types import AvailableTableType


def test_available_table_type_has_name_and_comment():
    """AvailableTableType stores name and optional comment."""
    t = AvailableTableType(name="orders", comment="Customer purchase orders")
    assert t.name == "orders"
    assert t.comment == "Customer purchase orders"


def test_available_table_type_null_comment():
    """Tables without comments have comment=None."""
    t = AvailableTableType(name="legacy_data", comment=None)
    assert t.name == "legacy_data"
    assert t.comment is None


def test_available_table_type_from_trino_row():
    """Simulates constructing AvailableTableType from a Trino result row."""
    rows = [
        ("customers", "Registered customer accounts"),
        ("products", "Product catalog"),
        ("orders", "Customer purchase orders"),
        ("audit_log", None),
    ]
    results = [AvailableTableType(name=row[0], comment=row[1]) for row in rows]

    assert len(results) == 4
    assert results[0].name == "customers"
    assert results[0].comment == "Registered customer accounts"
    assert results[3].name == "audit_log"
    assert results[3].comment is None


def test_admin_tables_filtered():
    """Simulates the admin table filtering logic from the resolver."""
    _ADMIN_TABLES = {
        "sources", "domains", "registered_tables", "table_columns",
        "relationships", "roles", "rls_rules", "persisted_queries",
    }
    rows = [
        ("customers", "Registered customer accounts"),
        ("orders", "Customer purchase orders"),
        ("registered_tables", None),
        ("roles", None),
    ]
    results = [
        AvailableTableType(name=row[0], comment=row[1])
        for row in rows
        if row[0] not in _ADMIN_TABLES
    ]

    assert len(results) == 2
    assert results[0].name == "customers"
    assert results[1].name == "orders"
