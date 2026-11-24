# Copyright (c) 2026 Kenneth Stott
# Canary: cc8cf65c-da64-48f7-a527-a26b51a7c354
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for list_native_tables / describe_native_table (REQ-1833, REQ-2114).

Polly's search_catalog/list_tables/describe_table only ever see the already-registered
governed catalog, which is empty for a source that has no tables registered yet. These
two tools read a source's real, native schema directly instead — the same seam the admin
UI's Register Table form uses (available_tables / resolve_available_columns_metadata).

REQ-2114: both tools are refused for a role that lacks table_registration, matching the
UI gate, and route through offered_tables directly (not Query().available_tables) so the
user's real source_id and schema_name are passed without the resolver's info-slot shift.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from provisa.api.mcp import tools
from tests.role_state import RoleState

pytestmark = pytest.mark.asyncio


def _state():
    return RoleState(contexts={"analyst": object()}, roles={"analyst": {"id": "analyst"}})


def _state_with_table_registration() -> RoleState:
    return RoleState(
        contexts={"analyst": object()},
        roles={"analyst": {"id": "analyst", "capabilities": ["table_registration"]}},
    )


async def test_list_native_tables_reads_source_directly():
    fake_tables = [
        SimpleNamespace(name="pets", comment="Shelter pets"),
        SimpleNamespace(name="species", comment=""),
    ]
    with patch(
        "provisa.api.admin.schema_query.offered_tables",
        new=AsyncMock(return_value=fake_tables),
    ):
        result = await tools.list_native_tables(
            _state_with_table_registration(), "analyst", "kaggle-shelter"
        )

    assert result == [
        {"name": "pets", "comment": "Shelter pets"},
        {"name": "species", "comment": ""},
    ]


async def test_list_native_tables_requires_role():
    with pytest.raises(ValueError):
        await tools.list_native_tables(_state(), "", "kaggle-shelter")


async def test_describe_native_table_reads_source_directly():  # REQ-1833
    fake_cols = [
        SimpleNamespace(name="id", data_type="integer", comment="Primary key"),
        SimpleNamespace(name="species", data_type="varchar", comment=""),
    ]
    with patch(
        "provisa.api.admin.schema_query.resolve_available_columns_metadata",
        new=AsyncMock(return_value=fake_cols),
    ):
        result = await tools.describe_native_table(
            _state_with_table_registration(), "analyst", "kaggle-shelter", "shelter", "pets"
        )

    assert result == [
        {"name": "id", "data_type": "integer", "comment": "Primary key"},
        {"name": "species", "data_type": "varchar", "comment": ""},
    ]


async def test_describe_native_table_requires_role():
    with pytest.raises(ValueError):
        await tools.describe_native_table(_state(), "", "kaggle-shelter", "shelter", "pets")


def _state_with_capability(capability: str) -> RoleState:
    """A state whose 'analyst' role explicitly carries a single capability."""
    return RoleState(
        contexts={"analyst": object()},
        roles={"analyst": {"id": "analyst", "capabilities": [capability]}},
    )


def _state_without_capability() -> RoleState:
    """A state whose 'analyst' role carries no capabilities."""
    return RoleState(contexts={"analyst": object()}, roles={"analyst": {"id": "analyst"}})


# -- real resolver path (REQ-2114): offered_tables, not Query().available_tables ----------------


async def test_list_native_tables_routes_through_offered_tables():
    """list_native_tables must call offered_tables(source_id, schema_name) directly.

    The old path called Query().available_tables(source_id, schema_name) where `source_id`
    landed in the `info` slot and `schema_name` landed in `source_id` — a silent arg shift.
    """
    fake_tables = [SimpleNamespace(name="orders", comment="Order rows")]
    with patch(
        "provisa.api.admin.schema_query.offered_tables",
        new=AsyncMock(return_value=fake_tables),
    ) as mock_offered:
        result = await tools.list_native_tables(
            _state_with_capability("table_registration"), "analyst", "mydb", "myschema"
        )
    mock_offered.assert_awaited_once_with("mydb", "myschema")
    assert result == [{"name": "orders", "comment": "Order rows"}]


# -- table_registration capability gate (REQ-2114) -----------------------------------------------


async def test_list_native_tables_refuses_without_table_registration():
    """A role that lacks table_registration is refused by name."""
    with patch(
        "provisa.api.admin.schema_query.offered_tables",
        new=AsyncMock(return_value=[]),
    ):
        with pytest.raises(PermissionError, match="table_registration"):
            await tools.list_native_tables(
                _state_without_capability(), "analyst", "mydb"
            )


async def test_describe_native_table_refuses_without_table_registration():
    """A role that lacks table_registration is refused by name."""
    with patch(
        "provisa.api.admin.schema_query.resolve_available_columns_metadata",
        new=AsyncMock(return_value=[]),
    ):
        with pytest.raises(PermissionError, match="table_registration"):
            await tools.describe_native_table(
                _state_without_capability(), "analyst", "mydb", "public", "orders"
            )
