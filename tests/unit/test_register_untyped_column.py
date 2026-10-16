# Copyright (c) 2026 Kenneth Stott
# Canary: 32246443-fd05-489e-986e-c8186d09fb80
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-846: a relational table is registered fully typed or not at all.

A column registered with no data type is typed from the source's own metadata, the same
discovery the register form shows. A column the source cannot type is refused by name, never
given a default; so a short register step that sends every discovered column must still ask for
the types discovery could not give.
"""

from unittest.mock import AsyncMock, patch

import pytest

from provisa.api.admin import _table_ops
from provisa.api.admin.types import AvailableColumnType, ColumnInput, TableInput


def _input(*columns: ColumnInput) -> TableInput:
    return TableInput(
        source_id="sales-pg",
        domain_id="sales",
        schema_name="public",
        table_name="orders",
        columns=list(columns),
    )


def _discovered(*pairs: tuple[str, str]) -> list[AvailableColumnType]:
    return [AvailableColumnType(name=n, data_type=t, comment=None) for n, t in pairs]


async def _build(meta: list[AvailableColumnType], *columns: ColumnInput):
    with (
        patch.object(_table_ops, "_graphql_columns_for_input", new=AsyncMock(return_value=None)),
        patch.object(_table_ops, "_grpc_columns_for_input", new=AsyncMock(return_value=None)),
        patch(
            "provisa.api.admin.schema_query.resolve_available_columns_metadata",
            new=AsyncMock(return_value=meta),
        ),
        # what a natively nested column is at its source (REQ-1965) is not this test's subject
        patch.object(_table_ops, "_settle_nested_columns", new=AsyncMock(return_value=None)),
        # nor is the commit time a change-data-capture table carries (REQ-1163)
        patch.object(_table_ops, "_commit_time_column", new=AsyncMock(return_value=[])),
    ):
        return await _table_ops._build_columns_for_input(None, _input(*columns))


@pytest.mark.asyncio
async def test_an_untyped_column_is_typed_from_the_source():
    columns, refused = await _build(
        _discovered(("id", "integer"), ("total", "decimal")),
        ColumnInput(name="id", visible_to=[]),
        ColumnInput(name="total", visible_to=[]),
    )
    assert refused is None
    assert [(c.name, c.data_type) for c in columns] == [("id", "integer"), ("total", "decimal")]


@pytest.mark.asyncio
async def test_a_column_the_source_cannot_type_is_refused_by_name():
    columns, refused = await _build(
        _discovered(("id", "integer"), ("payload", "")),
        ColumnInput(name="id", visible_to=[]),
        ColumnInput(name="payload", visible_to=[]),
    )
    assert columns == []
    assert refused is not None and refused.success is False
    assert refused.code == "schema.column_types_unresolved"
    assert refused.params == {"columns": "payload"}


@pytest.mark.asyncio
async def test_a_type_the_caller_gives_is_kept():
    columns, refused = await _build(
        _discovered(("payload", "")),
        ColumnInput(name="payload", visible_to=[], data_type="json"),
    )
    assert refused is None
    assert columns[0].data_type == "json"
