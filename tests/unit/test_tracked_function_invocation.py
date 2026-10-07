# Copyright (c) 2026 Kenneth Stott
# Canary: 5e8b3a10-2c74-4f96-8d05-1a6c9f4d7b28
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-872: shared tracked-function executor + Cypher CALL binding."""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from provisa.api.data.action_exec import invoke_tracked_function
from provisa.cypher.command_call import CommandCallRefused, parse_command_call
from provisa.security.rights import Capability


class _FakeResult:
    def __init__(self, cols, rows):
        self.column_names = cols
        self.rows = rows


class _FakePools:
    def __init__(self, connected=True, result=None):
        self._connected = connected
        self._result = result or _FakeResult(["id", "name"], [(1, "ada")])
        self.calls: list = []

    def has(self, src_id):
        return self._connected

    async def execute(self, src_id, sql, params):
        self.calls.append((src_id, sql, params))
        return self._result


def _fn(**over):
    base = {
        "name": "createOrder",
        "source_id": "s1",
        "schema_name": "public",
        "function_name": "create_order",
        "kind": "mutation",
        "visible_to": ["ops"],
        "domain_id": "sales",
        "returns": "",
    }
    base.update(over)
    return base


def _state(*, role_caps=(), visible_to=("ops",), connected=True, pools=None):
    role = {"id": "ops", "capabilities": list(role_caps), "domain_access": ["sales"]}
    pools = pools or _FakePools(connected=connected)
    return SimpleNamespace(
        roles={
            "ops": role,
            "reader": {"id": "reader", "capabilities": [], "domain_access": ["sales"]},
        },
        tracked_functions={"createOrder": _fn(visible_to=list(visible_to))},
        undefined_commands={},
        source_pools=pools,
        ephemeral=False,
    )


# ---- shared executor (REQ-872 / REQ-869) -----------------------------------


@pytest.mark.asyncio
async def test_an_assigned_role_with_write_calls_and_builds_sql():
    st = _state(role_caps=[Capability.WRITE.value], visible_to=["ops"])
    rows = await invoke_tracked_function("createOrder", {"a0": 7, "a1": "x"}, st, "ops")
    assert rows == [{"id": 1, "name": "ada"}]
    src, sql, params = st.source_pools.calls[0]
    assert src == "s1"
    assert sql == 'SELECT * FROM "public"."create_order"($1, $2)'
    assert params == [7, "x"]


@pytest.mark.asyncio
async def test_unauthorized_write_is_403():
    st = _state(role_caps=[], visible_to=["ops"])  # no WRITE cap
    with pytest.raises(HTTPException) as ei:
        await invoke_tracked_function("createOrder", {}, st, "ops")
    assert ei.value.status_code == 403


@pytest.mark.asyncio
async def test_a_role_not_assigned_finds_no_command():
    # The same answer as a command never registered: no existence leak.
    st = _state(role_caps=[Capability.WRITE.value], visible_to=["someone_else"])
    with pytest.raises(HTTPException) as ei:
        await invoke_tracked_function("createOrder", {}, st, "ops")
    assert ei.value.status_code == 404
    assert "Unknown command: 'createOrder'" in str(ei.value.detail)
    assert st.source_pools.calls == []


@pytest.mark.asyncio
async def test_a_role_outside_the_command_domain_finds_no_command():
    st = _state(role_caps=[Capability.WRITE.value])
    st.roles["ops"]["domain_access"] = ["hr"]
    with pytest.raises(HTTPException) as ei:
        await invoke_tracked_function("createOrder", {}, st, "ops")
    assert ei.value.status_code == 404
    assert st.source_pools.calls == []


@pytest.mark.asyncio
async def test_a_call_without_a_role_is_refused():
    # REQ-1758: no call runs, and no rows come back ungoverned, without an acting role.
    st = _state(role_caps=[Capability.WRITE.value], visible_to=[])
    st.tracked_functions["createOrder"]["kind"] = "query"
    with pytest.raises(HTTPException) as ei:
        await invoke_tracked_function("createOrder", {}, st, None)
    assert ei.value.status_code == 403
    assert st.source_pools.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "held", [["admin"], ["superadmin"], ["platform_settings", "cross_org"], ["write"]]
)
async def test_nothing_reaches_a_command_assigned_to_someone_else(held):
    # REQ-1327: the assignment is the whole answer; no capability stands above it.
    st = _state(role_caps=held, visible_to=["someone_else"])
    with pytest.raises(HTTPException) as ei:
        await invoke_tracked_function("createOrder", {}, st, "ops")
    assert ei.value.status_code == 404
    assert st.source_pools.calls == []


@pytest.mark.asyncio
async def test_unknown_function_is_not_found():
    st = _state(role_caps=[Capability.WRITE.value])
    with pytest.raises(HTTPException) as ei:
        await invoke_tracked_function("nope", {}, st, "ops")
    assert ei.value.status_code == 404


@pytest.mark.asyncio
async def test_disconnected_source_is_503():
    st = _state(role_caps=[Capability.WRITE.value], connected=False)
    with pytest.raises(HTTPException) as ei:
        await invoke_tracked_function("createOrder", {}, st, "ops")
    assert ei.value.status_code == 503


# ---- Cypher CALL parsing (REQ-872) -----------------------------------------


def test_a_quoted_comma_does_not_split_an_argument():
    call = parse_command_call("CALL createOrder(1, 'a, b')", {})
    assert call is not None and call.values == [1, "a, b"]


def test_argument_literal_types():
    call = parse_command_call(
        "CALL f($x, 'hi', 7, 3.5, true, null, [1, 'two'], 'it\\'s')", {"x": 42}
    )
    assert call is not None
    assert call.values == [42, "hi", 7, 3.5, True, None, [1, "two"], "it's"]


def test_an_argument_that_is_no_value_is_refused():
    with pytest.raises(CommandCallRefused, match="argument 1"):
        parse_command_call("CALL createOrder(id + 1)", {})


def test_detect_registered_call_with_yield():
    call = parse_command_call("CALL createOrder(7, 'x') YIELD id, name AS n", {})
    assert call is not None
    assert call.name == "createOrder"
    assert call.values == [7, "x"]
    assert call.yields == [("id", "id"), ("name", "n")]


def test_detect_registered_call_binds_params():
    call = parse_command_call("CALL createOrder($cid)", {"cid": 99})
    assert call is not None and call.values == [99]


def test_a_missing_parameter_is_refused_by_name():
    with pytest.raises(CommandCallRefused, match=r"\$cid"):
        parse_command_call("CALL createOrder($cid)", {})


def test_namespaced_procedures_are_not_command_calls():
    assert parse_command_call("CALL db.labels()", {}) is None
    assert parse_command_call("MATCH (n) RETURN n", {}) is None
