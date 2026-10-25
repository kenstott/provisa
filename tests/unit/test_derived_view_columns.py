# Copyright (c) 2026 Kenneth Stott
# Canary: 4eb4ac87-38ea-4c30-ab30-ca73d83666a9
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A derived view's columns are the ones it is registered with (REQ-1426).

A derived view has no object in any engine's catalog: it is expanded into the statement that reads
it. Its columns were read from the engine's information_schema all the same, under a catalog no
native engine has ("memory".information_schema), so every derived view listed no columns: the
table editor offered every column as a watermark, and a column left untyped could not be typed.
Every column of a derived view is typed when it is registered, so that record is its schema."""

# Requirements: REQ-1426

from __future__ import annotations

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, patch

import pytest

from provisa.api.admin import schema_query
from provisa.core.models import DERIVED_SOURCE_ID

_ROWS = [
    {"column_name": "id", "data_type": "bigint", "description": "Pet id", "is_primary_key": True},
    {"column_name": "name", "data_type": "varchar", "description": None, "is_primary_key": False},
    {"column_name": "born", "data_type": "timestamp", "description": None, "is_primary_key": False},
]


class _Pool:
    @asynccontextmanager
    async def acquire(self):
        yield object()


async def _resolve(rows):
    asked: list[tuple] = []

    async def registered(conn, source_id, schema_name, table_name):
        asked.append((source_id, schema_name, table_name))
        return rows

    with (
        patch.object(schema_query, "_get_pool", new=AsyncMock(return_value=_Pool())),
        patch("provisa.core.repositories.table.registered_columns", new=registered),
        patch.object(
            schema_query,
            "run_admin_catalog_sql",
            new=AsyncMock(side_effect=AssertionError("a derived view is not read from an engine")),
        ),
    ):
        columns = await schema_query.resolve_available_columns_metadata(
            DERIVED_SOURCE_ID, "views", "dim_pet"
        )
    return columns, asked


@pytest.mark.asyncio
async def test_a_derived_view_lists_the_columns_it_is_registered_with():
    columns, asked = await _resolve(_ROWS)
    assert asked == [(DERIVED_SOURCE_ID, "views", "dim_pet")]
    assert [(c.name, c.data_type, c.comment, c.is_primary_key) for c in columns] == [
        ("id", "bigint", "Pet id", True),
        ("name", "varchar", None, False),
        ("born", "timestamp", None, False),
    ]


@pytest.mark.asyncio
async def test_a_derived_view_not_registered_lists_none():
    columns, _ = await _resolve([])
    assert columns == []
