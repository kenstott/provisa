# Copyright (c) 2026 Kenneth Stott
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1

"""BDD step implementations for Hasura Migration Converters.

REQ-186 — v2 converter maps `insert/update_permissions[].columns` per role to the
Provisa column `writable_by`. Column write permissions from Hasura v2 are preserved
automatically on import: a column is writable by a role if that role's insert or
update permission lists the column.
"""

import os

import pytest
from pytest_bdd import given, scenario, then, when

from provisa.core.models import ProvisaConfig
from provisa.hasura_v2.mapper import convert_metadata
from provisa.hasura_v2.models import (
    HasuraMetadata,
    HasuraPermission,
    HasuraSource,
    HasuraTable,
)
from provisa.import_shared.warnings import WarningCollector


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def shared_data():
    """Plain dict to pass state between Given/When/Then steps."""
    return {}


# ---------------------------------------------------------------------------
# Scenario binding
# ---------------------------------------------------------------------------


@scenario(
    "req_186.feature",
    "REQ-186 default behaviour",
)
def test_req_186_default_behaviour():
    """v2 converter maps insert/update column permissions to writable_by."""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _find_table(config: ProvisaConfig, table_name: str):
    """Locate a converted Provisa table by source table name."""
    for domain in config.domains:
        for table in getattr(domain, "tables", []):
            # Match on the originating Hasura table name when available,
            # otherwise on the Provisa alias/name.
            candidates = {
                getattr(table, "name", None),
                getattr(table, "alias", None),
                getattr(table, "source_table", None),
                getattr(table, "table", None),
            }
            if table_name in candidates:
                return table
    # Fallback: flat tables attribute
    for table in getattr(config, "tables", []) or []:
        candidates = {
            getattr(table, "name", None),
            getattr(table, "alias", None),
            getattr(table, "source_table", None),
            getattr(table, "table", None),
        }
        if table_name in candidates:
            return table
    return None


def _all_tables(config: ProvisaConfig):
    """Yield every table in the converted config regardless of nesting."""
    seen = []
    for domain in getattr(config, "domains", []) or []:
        for table in getattr(domain, "tables", []) or []:
            seen.append(table)
    for table in getattr(config, "tables", []) or []:
        seen.append(table)
    return seen


def _column_writable_by(column) -> set[str]:
    """Return the writable_by set for a converted column."""
    value = getattr(column, "writable_by", None)
    if value is None:
        return set()
    if isinstance(value, str):
        return {value}
    return set(value)


# ---------------------------------------------------------------------------
# Given
# ---------------------------------------------------------------------------


@given(
    "a Hasura v2 metadata export with insert/update_permissions[].columns per role"
)
def hasura_v2_metadata_with_write_permissions(shared_data):
    """Build a Hasura v2 metadata export with column write permissions."""
    table = HasuraTable(
        table={"schema": "public", "name": "orders"},
        insert_permissions=[
            HasuraPermission(
                role="editor",
                columns=["id", "customer_id", "status"],
                check={},
                filter={},
            ),
        ],
        update_permissions=[
            HasuraPermission(
                role="editor",
                columns=["status", "updated_at"],
                check={},
                filter={},
            ),
            HasuraPermission(
                role="manager",
                columns=["customer_id", "status"],
                check={},
                filter={},
            ),
        ],
        select_permissions=[
            HasuraPermission(
                role="editor",
                columns=["id", "customer_id", "status", "updated_at"],
                filter={},
            ),
        ],
    )
    source = HasuraSource(
        name="default",
        kind="postgres",
        tables=[table],
    )
    metadata = HasuraMetadata(sources=[source])

    shared_data["metadata"] = metadata
    shared_data["table_name"] = "orders"
    # Expected writable_by per column derived from insert+update column lists.
    shared_data["expected_writable_by"] = {
        "id": {"editor"},
        "customer_id": {"editor", "manager"},
        "status": {"editor", "manager"},
        "updated_at": {"editor"},
    }


# ---------------------------------------------------------------------------
# When
# ---------------------------------------------------------------------------


@when("the v2 converter runs")
def run_v2_converter(shared_data):
    """Run the Hasura v2 converter on the prepared metadata."""
    warnings = WarningCollector()
    config = convert_metadata(shared_data["metadata"], warnings=warnings)
    assert isinstance(config, ProvisaConfig)
    shared_data["config"] = config
    shared_data["warnings"] = warnings


# ---------------------------------------------------------------------------
# Then
# ---------------------------------------------------------------------------


@then(
    "each column's writable_by is populated from the role's insert/update column list"
)
def assert_writable_by_populated(shared_data):
    """Verify writable_by reflects the union of insert/update column grants."""
    config = shared_data["config"]
    table = _find_table(config, shared_data["table_name"])
    assert table is not None, (
        f"Converted table {shared_data['table_name']!r} not found in "
        f"{[getattr(t, 'name', None) for t in _all_tables(config)]}"
    )

    columns = {getattr(c, "name", None): c for c in getattr(table, "columns", [])}
    assert columns, "Converted table has no columns"

    expected = shared_data["expected_writable_by"]
    for col_name, expected_roles in expected.items():
        assert col_name in columns, f"Missing column {col_name!r} after conversion"
        actual_roles = _column_writable_by(columns[col_name])
        assert actual_roles == expected_roles, (
            f"Column {col_name!r} writable_by {actual_roles} "
            f"!= expected {expected_roles}"
        )

    # A column never granted for write must not be writable by any role.
    for col_name, column in columns.items():
        if col_name not in expected:
            assert _column_writable_by(column) == set(), (
                f"Column {col_name!r} unexpectedly writable: "
                f"{_column_writable_by(column)}"
            )
