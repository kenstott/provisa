# Copyright (c) 2026 Kenneth Stott
# Canary: a75a79d8-c2c8-47d5-bbf8-bfe8ddd3e245
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The DIRECT terminal runs a statement on its own source's connection, or not at all.

The org's model store serves the ``provisa-admin`` source (meta.* views, REQ-1919) and no other.
A DIRECT plan for any other source that this node holds no connection for is refused by name —
it is never handed to the model store, where a read would answer from the control plane and a
write would land in it.
"""

# Requirements: REQ-825, REQ-1919, REQ-031

from __future__ import annotations

from contextlib import asynccontextmanager

import pytest

from provisa.api.errors import ApiError
from provisa.pgwire._pipeline import _Plan, _run_plan_terminal
from provisa.transpiler.router import Route


class _ModelStore:
    """The org's model store: records every statement it is asked to run."""

    def __init__(self) -> None:
        self.ran: list[str] = []

    @asynccontextmanager
    async def acquire(self):
        store = self

        class _Conn:
            async def fetch_with_columns(self, sql):
                store.ran.append(sql)
                return ["n"], [(1,)]

        yield _Conn()


class _Pools:
    """Connections this node holds: none."""

    def has(self, _source_id: str) -> bool:
        return False


def _state(store: _ModelStore):
    return type(
        "_State",
        (),
        {
            "model_db": store,
            "source_pools": _Pools(),
            "source_types": {"cassandra_writes": "cassandra"},
            "federation_engine": None,
        },
    )()


def _plan(source_id: str, sql: str) -> _Plan:
    return _Plan(route=Route.DIRECT, sql=sql, source_id=source_id, dialect="postgres")


async def test_the_admin_source_is_served_by_the_model_store():
    store = _ModelStore()
    result = await _run_plan_terminal(_plan("provisa-admin", "SELECT 1 AS n"), _state(store))
    assert result.rows == [(1,)]
    assert store.ran == ["SELECT 1 AS n"]


@pytest.mark.parametrize(
    "sql",
    [
        'INSERT INTO "provisa_pipeline"."gadgets" (id, name) VALUES (2, \'B\')',
        'SELECT id FROM "provisa_pipeline"."gadgets"',
    ],
)
async def test_another_source_with_no_connection_is_refused_and_the_model_store_untouched(sql):
    store = _ModelStore()
    with pytest.raises(ApiError) as refused:
        await _run_plan_terminal(_plan("cassandra_writes", sql), _state(store))
    assert (refused.value.status_code, refused.value.code) == (500, "data.no_direct_route")
    assert "'cassandra_writes'" in refused.value.detail
    assert store.ran == []
