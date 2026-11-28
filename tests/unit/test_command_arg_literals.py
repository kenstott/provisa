# Copyright (c) 2026 Kenneth Stott
# Canary: 9a4c1e72-3d58-4b06-8f2e-6b0d7c5a1e94
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1159, REQ-2236: a quoted literal a SQL statement passes to a command is read as its
parameter's declared type, as PostgreSQL reads a literal of type unknown; one that type does not
read is refused with PostgreSQL's error and SQLSTATE. A value of another type is still refused."""

from __future__ import annotations

import datetime
import math
from decimal import Decimal
from types import SimpleNamespace

import pytest

from provisa.api.data.action_exec import ArgumentInputRefused, bind_command_args
from provisa.api.data.argument_literals import UnknownLiteral
from provisa.api.errors import ApiError
from provisa.executor.result import QueryResult
from provisa.pgwire.function_call import maybe_invoke_registered_function


class _Pools:
    def __init__(self):
        self.calls: list = []

    def has(self, src_id):
        return True

    async def execute(self, src_id, sql, params):
        self.calls.append((src_id, sql, params))
        return QueryResult(rows=[("hi",)], column_names=["greeting"], column_types=["text"])


def _state(*types: str):
    command = {
        "name": "greet",
        "source_id": "s1",
        "schema_name": "public",
        "function_name": "greet",
        "kind": "query",
        "domain_id": "d",
        "visible_to": ["reader"],
        "arguments": [{"name": f"x{i}", "type": t} for i, t in enumerate(types)],
    }
    return SimpleNamespace(
        roles={
            "reader": {
                "id": "reader",
                "capabilities": ["command_execution"],
                "domain_access": ["d"],
            }
        },
        tracked_functions={"greet": command},
        tracked_webhooks={},
        source_pools=_Pools(),
        ephemeral=False,
    )


def _bind(arg_type: str, value):
    return bind_command_args("greet", [value], _state(arg_type), "reader")["x0"]


@pytest.mark.parametrize(
    ("arg_type", "text", "value"),
    [
        ("double", "1", 1.0),
        ("double", " -2.5e3 ", -2500.0),
        ("double", "Infinity", math.inf),
        ("float", "1.5", 1.5),
        ("integer", "42", 42),
        ("integer", " -7 ", -7),
        ("bigint", "9223372036854775807", 9223372036854775807),
        ("smallint", "0x10", 16),
        ("numeric", "1.50", Decimal("1.50")),
        ("boolean", "t", True),
        ("boolean", " Off ", False),
        ("boolean", "1", True),
        ("date", "2024-02-29", datetime.date(2024, 2, 29)),
        ("timestamp", "2024-01-02 03:04:05.5", datetime.datetime(2024, 1, 2, 3, 4, 5, 500000)),
        ("text", "1", "1"),
    ],
)
def test_a_quoted_literal_is_read_as_its_declared_type(arg_type, text, value):
    read = _bind(arg_type, UnknownLiteral(text))
    assert read == value
    assert type(read) is type(value)


def test_a_quoted_nan_is_read_as_a_double():
    assert math.isnan(_bind("double", UnknownLiteral("NaN")))


@pytest.mark.parametrize(
    ("arg_type", "text", "sqlstate", "message"),
    [
        ("double", "abc", "22P02", 'invalid input syntax for type double precision: "abc"'),
        ("double", "", "22P02", 'invalid input syntax for type double precision: ""'),
        ("double", "1e400", "22003", '"1e400" is out of range for type double precision'),
        ("float", "1e39", "22003", '"1e39" is out of range for type real'),
        ("integer", "4.2", "22P02", 'invalid input syntax for type integer: "4.2"'),
        ("integer", "3000000000", "22003", 'value "3000000000" is out of range for type integer'),
        ("numeric", "abc", "22P02", 'invalid input syntax for type numeric: "abc"'),
        ("boolean", "o", "22P02", 'invalid input syntax for type boolean: "o"'),
        ("date", "soon", "22P02", 'invalid input syntax for type date: "soon"'),
        ("date", "2023-02-29", "22008", 'date/time field value out of range: "2023-02-29"'),
        ("timestamp", "x", "22P02", 'invalid input syntax for type timestamp: "x"'),
    ],
)
def test_a_quoted_literal_its_type_does_not_read_is_refused_as_postgresql(
    arg_type, text, sqlstate, message
):
    with pytest.raises(ArgumentInputRefused) as raised:
        _bind(arg_type, UnknownLiteral(text))
    assert raised.value.status_code == 400
    assert raised.value.sqlstate == sqlstate
    assert raised.value.detail == message
    assert raised.value.params["argument"] == "x0"


@pytest.mark.parametrize(
    ("arg_type", "value"),
    [("double", True), ("double", "1"), ("integer", False), ("boolean", 1)],
)
def test_a_typed_value_of_another_type_is_still_refused(arg_type, value):
    """A typed value (a SQL TRUE, a Cypher string) is not a literal of type unknown."""
    with pytest.raises(ApiError, match="argument 'x0' is") as raised:
        _bind(arg_type, value)
    assert raised.value.code == "functions.argument_type"


@pytest.mark.asyncio
async def test_a_sql_call_of_quoted_literals_runs_with_typed_values():
    """The maintainer's report: SELECT * FROM greet('1','2','3','4') of four double arguments."""
    state = _state("double", "double", "double", "double")
    result = await maybe_invoke_registered_function(
        "SELECT * FROM greet('1','2','3','4')", "reader", state
    )
    assert result is not None and result.rows == [("hi",)]
    _src, _sql, params = state.source_pools.calls[0]
    assert params == [1.0, 2.0, 3.0, 4.0]
    assert all(type(p) is float for p in params)


@pytest.mark.asyncio
async def test_a_sql_call_of_an_unreadable_quoted_literal_is_refused_22P02():
    state = _state("double")
    with pytest.raises(ArgumentInputRefused) as raised:
        await maybe_invoke_registered_function("SELECT * FROM greet('abc')", "reader", state)
    assert raised.value.sqlstate == "22P02"
    assert raised.value.detail == 'invalid input syntax for type double precision: "abc"'
    assert state.source_pools.calls == []


@pytest.mark.asyncio
async def test_a_sql_true_for_a_double_is_still_refused():
    with pytest.raises(ApiError, match="argument 'x0' is double"):
        await maybe_invoke_registered_function(
            "SELECT * FROM greet(TRUE)", "reader", _state("double")
        )
