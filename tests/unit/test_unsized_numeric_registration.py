# Copyright (c) 2026 Kenneth Stott
# Canary: 39b2bd7c-3f8a-4eff-b79a-eddbf1e91805
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An unsized PostgreSQL numeric registers as numeric(38, s), s the largest scale among its
values (10 for a table with none); a sized one keeps its size (REQ-1961, REQ-2167)."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.api.admin import introspect


class _Pool:
    def __init__(self, scale):
        self.scale = scale
        self.sql: list[str] = []

    def has(self, source_id):
        return True

    async def execute(self, source_id, sql, params=None):
        self.sql.append(sql)
        if sql.startswith("SELECT max(scale("):
            return SimpleNamespace(rows=[(self.scale,)])
        return SimpleNamespace(
            rows=[
                ("amount", "numeric", "numeric", "numeric", "b"),
                ("price", "numeric", "numeric", "numeric(10,2)", "b"),
                ("n", "integer", "int4", "integer", "b"),
            ]
        )


async def _typings(scale):
    pool = _Pool(scale)
    typings = await introspect.native_column_typings("src", "postgresql", "s", "t", pool)
    return {name: t.data_type for name, t in typings}, pool


@pytest.mark.asyncio
async def test_an_unsized_numeric_takes_the_largest_scale_of_its_values():
    typings, pool = await _typings(6)
    assert typings == {"amount": "numeric(38,6)", "price": "numeric(10,2)", "n": "integer"}
    assert any('scale("amount")' in sql and 'FROM "s"."t" LIMIT' in sql for sql in pool.sql)


@pytest.mark.asyncio
async def test_an_unsized_numeric_with_no_values_takes_scale_ten():
    typings, _ = await _typings(None)
    assert typings["amount"] == "numeric(38,10)"
