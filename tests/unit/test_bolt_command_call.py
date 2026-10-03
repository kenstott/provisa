# Copyright (c) 2026 Kenneth Stott
# Canary: 7d2e9a41-5c86-40b3-91f7-3e0a8c6d24b9
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1156: `CALL <command>(args)` over Bolt/Cypher invokes a registered command through the
single governed executor (invoke_tracked_function): read by the one Cypher command-call reader
(cypher/command_call.py), admitted, bound to the declared arguments, shaped by YIELD/RETURN."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import HTTPException

from provisa.bolt.session import _maybe_invoke_command_call, _parse_call_arg
from provisa.cypher.command_call import CommandCallRefused

pytestmark = [pytest.mark.asyncio(loop_scope="session")]


def _state(visible_to=()):
    return SimpleNamespace(
        roles={"admin": {"id": "admin", "capabilities": ["write"], "domain_access": ["*"]}},
        tracked_functions={
            "random_python_set": {
                "name": "random_python_set",
                "kind": "query",
                "domain_id": "sales",
                "visible_to": list(visible_to),
                "arguments": [{"name": "rows"}, {"name": "seed"}],
            }
        },
    )


def _executor(rows):
    return patch(
        "provisa.api.data.action_exec.invoke_tracked_function", new=AsyncMock(return_value=rows)
    )


async def test_call_command_invokes_executor():
    rows = [{"id": 1, "region": "east"}]
    with _executor(rows) as inv:
        result = await _maybe_invoke_command_call(
            "CALL random_python_set(3, 7)", {}, "admin", _state()
        )
    assert result == (["id", "region"], [[1, "east"]])
    # positional args mapped to the command's declared argument names
    inv.assert_awaited_once()
    assert inv.await_args is not None
    assert inv.await_args.args[0] == "random_python_set"
    assert inv.await_args.args[1] == {"rows": 3, "seed": 7}


async def test_a_quoted_comma_is_one_argument_and_a_parameter_is_bound():
    with _executor([{"id": 1}]) as inv:
        await _maybe_invoke_command_call(
            "CALL random_python_set('a, b', $seed)", {"seed": 9}, "admin", _state()
        )
    assert inv.await_args is not None
    assert inv.await_args.args[1] == {"rows": "a, b", "seed": 9}


async def test_a_parameter_not_supplied_is_refused_by_name():
    with _executor([]) as inv, pytest.raises(CommandCallRefused, match=r"\$seed"):
        await _maybe_invoke_command_call("CALL random_python_set(1, $seed)", {}, "admin", _state())
    inv.assert_not_awaited()


@pytest.mark.parametrize("args", ["1", "1, 2, 3"])
async def test_a_count_off_the_signature_is_refused_naming_it(args):
    with _executor([]) as inv, pytest.raises(HTTPException) as refused:
        await _maybe_invoke_command_call(f"CALL random_python_set({args})", {}, "admin", _state())
    assert refused.value.status_code == 400
    assert "random_python_set(rows :: STRING, seed :: STRING)" in str(refused.value.detail)
    inv.assert_not_awaited()


async def test_an_unknown_command_and_one_not_assigned_answer_alike():
    answers = []
    for state, name in ((_state(), "nope"), (_state(visible_to=["other"]), "random_python_set")):
        with _executor([]) as inv, pytest.raises(HTTPException) as hidden:
            await _maybe_invoke_command_call(f"CALL {name}(1, 2)", {}, "admin", state)
        inv.assert_not_awaited()
        answers.append((hidden.value.status_code, str(hidden.value.detail).replace(name, "<n>")))
    assert answers[0] == answers[1] == (404, "Unknown command: '<n>'")


async def test_non_command_call_falls_through():
    # a namespaced procedure and a plain MATCH are not command calls
    assert await _maybe_invoke_command_call("CALL db.labels()", {}, "admin", _state()) is None
    assert await _maybe_invoke_command_call("MATCH (n) RETURN n", {}, "admin", _state()) is None


async def test_yield_with_as_and_return_shape_the_rows():
    with _executor([{"id": 1, "region": "east"}]):
        result = await _maybe_invoke_command_call(
            "CALL random_python_set(2, 3) YIELD id AS n, region RETURN region AS r, n",
            {},
            "admin",
            _state(),
        )
    assert result == (["r", "n"], [["east", 1]])


async def test_a_return_it_cannot_apply_is_refused_not_ignored():
    for tail in ("RETURN id + 1", "WHERE id > 1 RETURN id", "YIELD id ORDER BY id"):
        with _executor([{"id": 1}]), pytest.raises(CommandCallRefused):
            await _maybe_invoke_command_call(
                f"CALL random_python_set(2, 3) {tail}", {}, "admin", _state()
            )


async def test_parse_call_arg_types():
    assert _parse_call_arg("3") == 3
    assert _parse_call_arg("3.5") == 3.5
    assert _parse_call_arg("'x'") == "x"
    assert _parse_call_arg("true") is True
    assert _parse_call_arg("null") is None
