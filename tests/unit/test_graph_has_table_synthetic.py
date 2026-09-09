# Copyright (c) 2026 Kenneth Stott
# Canary: 9e2d7a41-5f8c-4b36-8c1d-2a6e0f93b5d7
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1631: the synthetic HAS_TABLE relationship rows the admin resolver computes at query time --
one per non-meta registered table, pointing at the registered_tables meta definition, never
persisted."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.api.admin.schema_query import _has_table_synthetic_relationships

pytestmark = pytest.mark.unit


class _Result:
    def __init__(self, rows):
        self._rows = rows

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return list(self._rows)


class _Conn:
    """Answers the meta lookup first, then the data-table listing."""

    def __init__(self, meta_row, data_rows):
        self._answers = [meta_row, data_rows]

    async def execute_core(self, stmt):
        return _Result(self._answers.pop(0))


@pytest.mark.asyncio
async def test_one_has_table_row_per_data_table_pointing_at_the_meta_definition():
    conn = _Conn(
        meta_row=[(7, "registered_tables")],
        data_rows=[
            SimpleNamespace(id=1, table_name="pets", domain_id="pet_store"),
            SimpleNamespace(id=2, table_name="vets", domain_id=None),
        ],
    )
    rows = await _has_table_synthetic_relationships(conn)
    assert [(r.id, r.source_table_id, r.target_table_id) for r in rows] == [
        ("meta:has_table:1", 1, 7),
        ("meta:has_table:2", 2, 7),
    ]
    assert all(r.alias == "HAS_TABLE" and r.target_table_name == "registered_tables" for r in rows)
    assert all((r.source_column, r.target_column) == ("__table_id__", "id") for r in rows)
    assert [r.source_domain_id for r in rows] == ["pet_store", ""]
    assert all(r.cardinality == "many-to-one" and r.materialize is False for r in rows)


@pytest.mark.asyncio
async def test_no_meta_definition_yields_no_rows():
    conn = _Conn(meta_row=[], data_rows=[SimpleNamespace(id=1, table_name="pets", domain_id="x")])
    assert await _has_table_synthetic_relationships(conn) == []
