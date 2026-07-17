# Copyright (c) 2026 Kenneth Stott
# Canary: 7843e533-bbb8-42f8-9b4e-42b5a1305674
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-433: first-come dataset ownership — a physical table claimed by one domain
cannot be registered by another (normalized name, per source)."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from provisa.api.admin.schema_helpers import _dataset_ownership_conflict, _normalize_dataset_name

pytestmark = [pytest.mark.asyncio(loop_scope="session")]


class _Conn:
    def __init__(self, rows):
        self._rows = rows

    async def execute_core(self, stmt):
        res = MagicMock()
        res.fetchall.return_value = self._rows
        return res


def _rows(*pairs):
    return [MagicMock(domain_id=d, table_name=t) for d, t in pairs]


class TestNormalization:
    async def test_camel_and_snake_normalize_equal(self):
        assert _normalize_dataset_name("MyTable") == _normalize_dataset_name("my_table")
        assert _normalize_dataset_name("orderItems") == "order_items"


class TestOwnershipConflict:
    async def test_unclaimed_table_no_conflict(self):
        conn = _Conn([])
        assert await _dataset_ownership_conflict(conn, "pg", "orders", "sales") is None

    async def test_same_domain_reregister_allowed(self):
        conn = _Conn(_rows(("sales", "orders")))
        assert await _dataset_ownership_conflict(conn, "pg", "orders", "sales") is None

    async def test_different_domain_rejected(self):
        conn = _Conn(_rows(("sales", "orders")))
        msg = await _dataset_ownership_conflict(conn, "pg", "orders", "finance")
        assert msg is not None
        assert "already claimed by domain 'sales'" in msg

    async def test_normalized_variant_cross_domain_rejected(self):
        # "MyTable" (finance attempt) collides with claimed "my_table" (sales).
        conn = _Conn(_rows(("sales", "my_table")))
        msg = await _dataset_ownership_conflict(conn, "pg", "MyTable", "finance")
        assert msg is not None

    async def test_virtual_provisa_views_exempt(self):
        # __provisa__ is a shared virtual source — many domains hold views there.
        conn = _Conn(_rows(("sales", "revenue")))
        assert await _dataset_ownership_conflict(conn, "__provisa__", "revenue", "finance") is None

    async def test_different_source_no_conflict(self):
        # The query is scoped to source_id; a same-named table on another source is
        # never returned by the fetch, so no conflict.
        conn = _Conn([])
        assert await _dataset_ownership_conflict(conn, "pg2", "orders", "finance") is None
