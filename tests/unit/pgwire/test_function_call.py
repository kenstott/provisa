# Copyright (c) 2026 Kenneth Stott
# Canary: 2f7a9c04-6b13-4d85-8e51-1c6a0d4f7b93
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-872: pgwire SELECT-of-a-registered-function binding to the shared executor."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.pgwire.function_call import (
    detect_sql_function_call,
    maybe_invoke_registered_function,
    rows_to_query_result,
)
from provisa.security.rights import Capability


class _FakeResult:
    def __init__(self, cols, rows):
        self.column_names = cols
        self.rows = rows


class _FakePools:
    def __init__(self, result=None):
        self._result = result or _FakeResult(["id", "name"], [(1, "ada")])
        self.calls: list = []

    def has(self, src_id):
        return True

    async def execute(self, src_id, sql, params):
        self.calls.append((src_id, sql, params))
        return self._result


def _state(**over):
    fn = {
        "name": "createOrder",
        "source_id": "s1",
        "schema_name": "public",
        "function_name": "create_order",
        "kind": "mutation",
        "writable_by": ["ops"],
    }
    return SimpleNamespace(
        roles={"ops": {"id": "ops", "capabilities": [Capability.ADMIN.value]}},
        tracked_functions={"createOrder": fn},
        source_pools=over.get("pools") or _FakePools(),
    )


# ---- detection --------------------------------------------------------------


def test_detect_table_valued_form():
    name, args = detect_sql_function_call("SELECT * FROM createOrder(1, 'x')", _state())
    assert name == "createOrder"
    assert args == [1, "x"]


def test_detect_scalar_form():
    name, args = detect_sql_function_call("SELECT createOrder(7, 3.5, true, null)", _state())
    assert name == "createOrder"
    assert args == [7, 3.5, True, None]


def test_normal_query_is_not_a_function_call():
    assert detect_sql_function_call("SELECT * FROM orders WHERE id = 1", _state()) is None


def test_unregistered_function_ignored():
    assert detect_sql_function_call("SELECT now()", _state()) is None


def test_unparseable_sql_returns_none():
    assert detect_sql_function_call("NOT valid ((( sql", _state()) is None


def test_detect_inside_sample_wrapper():
    # The in-app SQL Explorer wraps the query as `SELECT * FROM (<sql>) AS _sample LIMIT N`.
    # Detection must still find the nested command call so it routes through the executor.
    name, args = detect_sql_function_call(
        "SELECT * FROM (SELECT * FROM createOrder(9)) AS _sample LIMIT 100", _state()
    )
    assert name == "createOrder"
    assert args == [9]


# ---- REQ-1159: composed statements are NOT the standalone path (localization owns them) ----


def test_composed_join_is_not_standalone():
    sql = "SELECT o.id, e.x FROM orders o JOIN createOrder('a') e ON o.id = e.id"
    assert detect_sql_function_call(sql, _state()) is None


def test_command_with_other_table_is_not_standalone():
    sql = "SELECT * FROM orders o, createOrder('a') e"
    assert detect_sql_function_call(sql, _state()) is None


def test_two_commands_is_not_standalone():
    st = _state()
    st.tracked_functions["labelRows"] = {"name": "labelRows", "kind": "query"}
    sql = "SELECT * FROM createOrder('a') e JOIN labelRows('b') l ON e.id = l.id"
    assert detect_sql_function_call(sql, st) is None


# ---- result adaptation ------------------------------------------------------


def test_rows_to_query_result_orders_columns():
    qr = rows_to_query_result([{"id": 1, "name": "ada"}, {"id": 2, "name": "bo"}])
    assert qr.column_names == ["id", "name"]
    assert qr.rows == [(1, "ada"), (2, "bo")]


def test_rows_to_query_result_empty():
    qr = rows_to_query_result([])
    assert qr.column_names == [] and qr.rows == []


# ---- end-to-end routing -----------------------------------------------------


@pytest.mark.asyncio
async def test_maybe_invoke_routes_registered_call():
    st = _state()
    qr = await maybe_invoke_registered_function("SELECT * FROM createOrder(5)", "ops", st)
    assert qr is not None
    assert qr.column_names == ["id", "name"]
    assert qr.rows == [(1, "ada")]
    # The executor built the source-native call with the parsed arg.
    _src, sql, params = st.source_pools.calls[0]
    assert sql == 'SELECT * FROM "public"."create_order"($1)'
    assert params == [5]


@pytest.mark.asyncio
async def test_maybe_invoke_passes_through_normal_query():
    assert await maybe_invoke_registered_function("SELECT 1", "ops", _state()) is None
