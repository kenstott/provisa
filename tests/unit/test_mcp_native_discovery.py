# Copyright (c) 2026 Kenneth Stott
# Canary: cc8cf65c-da64-48f7-a527-a26b51a7c354
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for list_native_tables / describe_native_table (REQ-1833).

Polly's search_catalog/list_tables/describe_table only ever see the already-registered
governed catalog, which is empty for a source that has no tables registered yet. These
two tools read a source's real, native schema directly instead — the same seam the admin
UI's Register Table form uses (available_tables / resolve_available_columns_metadata).
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from provisa.api.mcp import tools

pytestmark = pytest.mark.asyncio


def _state():
    return SimpleNamespace(contexts={"analyst": object()}, roles={"analyst": {"id": "analyst"}})


async def test_list_native_tables_reads_source_directly():
    fake_tables = [
        SimpleNamespace(name="pets", comment="Shelter pets"),
        SimpleNamespace(name="species", comment=""),
    ]
    with patch(
        "provisa.api.admin.schema_query.Query.available_tables",
        new=AsyncMock(return_value=fake_tables),
    ):
        result = await tools.list_native_tables(_state(), "analyst", "kaggle-shelter")

    assert result == [
        {"name": "pets", "comment": "Shelter pets"},
        {"name": "species", "comment": ""},
    ]


async def test_list_native_tables_requires_role():
    with pytest.raises(ValueError):
        await tools.list_native_tables(_state(), "", "kaggle-shelter")


async def test_describe_native_table_reads_source_directly():
    fake_cols = [
        SimpleNamespace(name="id", data_type="integer", comment="Primary key"),
        SimpleNamespace(name="species", data_type="varchar", comment=""),
    ]
    with patch(
        "provisa.api.admin.schema_query.resolve_available_columns_metadata",
        new=AsyncMock(return_value=fake_cols),
    ):
        result = await tools.describe_native_table(
            _state(), "analyst", "kaggle-shelter", "shelter", "pets"
        )

    assert result == [
        {"name": "id", "data_type": "integer", "comment": "Primary key"},
        {"name": "species", "data_type": "varchar", "comment": ""},
    ]


async def test_describe_native_table_requires_role():
    with pytest.raises(ValueError):
        await tools.describe_native_table(_state(), "", "kaggle-shelter", "shelter", "pets")
