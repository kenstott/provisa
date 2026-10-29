# Copyright (c) 2026 Kenneth Stott
# Canary: 4e88a014-2474-41fd-a485-8c51643fbfa3
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A Cypher command call that openCypher refuses at compile time is refused by name, never run
(REQ-872, REQ-2168; openCypher TCK Call1[12], Call1[15], Call2[5]-[6], Call5[5]-[7]):

* a RETURN after a call with no YIELD reads nothing the call binds;
* a YIELD naming one variable twice, or ``YIELD *`` followed by RETURN;
* a YIELD binding a variable the statement already binds;
* an argument whose value is not of its declared type (a boolean for an INTEGER)."""

# Requirements: REQ-872, REQ-2168, REQ-2167

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.api.errors import ApiError
from provisa.cypher.command_call import CommandCallRefused, parse_command_call

pytestmark = [pytest.mark.unit]


@pytest.mark.parametrize(
    "query",
    [
        # Call1[12]: RETURN with no YIELD
        "CALL test.my.proc(1) RETURN out",
        # Call5[5], Call5[6]: one variable yielded twice
        "CALL test.my.proc(null) YIELD a, b AS a RETURN a",
        "CALL test.my.proc(null) YIELD a AS c, b AS c RETURN c",
        "CALL test.my.proc(null) YIELD a AS c, b AS c",
        # Call5[7]: YIELD * with a RETURN
        "CALL test.my.proc('Stefan', 1) YIELD * RETURN city, country_code",
    ],
)
def test_a_call_openCypher_refuses_is_refused_as_read(query):
    with pytest.raises(CommandCallRefused):
        parse_command_call(query, {})


@pytest.mark.parametrize(
    "query",
    [
        "CALL test.my.proc('Stefan', 1) YIELD *",  # Call5[8]
        "CALL test.my.proc(null) YIELD a AS b, b AS a RETURN b, a",  # Call5[4]
        "CALL test.my.proc(null) YIELD a AS a, b RETURN a, b",
        "CALL test.my.proc(null) YIELD out RETURN *",  # Call5[2]
    ],
)
def test_a_call_openCypher_takes_is_read(query):
    assert parse_command_call(query, {}) is not None


def test_a_yield_rebinding_a_bound_variable_is_refused():
    """Call1[15]: WITH 'Hi' AS label CALL test.labels() YIELD label."""
    from provisa.cypher.parser import CypherParseError, parse_cypher

    with pytest.raises(CypherParseError, match="label"):
        parse_cypher("WITH 'Hi' AS label CALL test.labels() YIELD label RETURN *")
    with pytest.raises(CypherParseError, match="x"):
        parse_cypher("UNWIND [1] AS x CALL test.labels() YIELD label AS x RETURN x")
    # A variable a WITH no longer carries may be yielded again.
    parse_cypher("WITH 1 AS label WITH 2 AS other CALL test.labels() YIELD label RETURN *")


def _state(arg_type: str) -> SimpleNamespace:
    command = {
        "name": "tck_my_proc",
        "kind": "query",
        "domain_id": "d",
        "visible_to": ["reader"],
        "arguments": [{"name": "in", "type": arg_type}],
    }
    return SimpleNamespace(
        roles={"reader": {"id": "reader", "capabilities": [], "domain_access": ["d"]}},
        tracked_functions={"tck_my_proc": command},
        tracked_webhooks={},
    )


@pytest.mark.parametrize(
    ("arg_type", "value"),
    [
        ("bigint", True),  # Call2[5], Call2[6]
        ("integer", 4.2),
        ("bigint", "42"),
        ("double", False),
        ("double", "4.2"),
        ("boolean", 1),
    ],
)
def test_an_argument_not_of_its_declared_type_is_refused(arg_type, value):
    from provisa.api.data.action_exec import bind_command_args

    with pytest.raises(ApiError, match="'in'") as raised:
        bind_command_args("tck_my_proc", [value], _state(arg_type), "reader")
    assert raised.value.status_code == 400


@pytest.mark.parametrize(
    ("arg_type", "value"),
    [
        ("bigint", 42),
        ("bigint", None),
        ("double", 42),  # Call3: a NUMBER or FLOAT takes an INTEGER
        ("double", 42.3),
        ("numeric", 42),
        ("boolean", True),
        ("text", "x"),
        ("date", "2024-01-01"),  # a type no JSON kind holds alone: its value is not checked
    ],
)
def test_an_argument_of_its_declared_type_is_bound(arg_type, value):
    from provisa.api.data.action_exec import bind_command_args

    assert bind_command_args("tck_my_proc", [value], _state(arg_type), "reader") == {"in": value}
