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
from provisa.api.rest.registered_call import (
    _parse_call_literal,
    _split_call_args,
    detect_registered_call,
)
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
        "writable_by": ["ops"],
        "returns": "",
    }
    base.update(over)
    return base


def _state(*, role_caps=(), writable_by=("ops",), connected=True, pools=None):
    role = {"id": "ops", "capabilities": list(role_caps)}
    pools = pools or _FakePools(connected=connected)
    return SimpleNamespace(
        roles={"ops": role, "reader": {"id": "reader", "capabilities": []}},
        tracked_functions={"createOrder": _fn(writable_by=list(writable_by))},
        source_pools=pools,
    )


# ---- shared executor (REQ-872 / REQ-869) -----------------------------------


@pytest.mark.asyncio
async def test_write_capability_and_acl_allows_and_builds_sql():
    st = _state(role_caps=[Capability.WRITE.value], writable_by=["ops"])
    rows = await invoke_tracked_function("createOrder", {"a0": 7, "a1": "x"}, st, "ops")
    assert rows == [{"id": 1, "name": "ada"}]
    src, sql, params = st.source_pools.calls[0]
    assert src == "s1"
    assert sql == 'SELECT * FROM "public"."create_order"($1, $2)'
    assert params == [7, "x"]


@pytest.mark.asyncio
async def test_unauthorized_write_is_403():
    st = _state(role_caps=[], writable_by=["ops"])  # no WRITE cap
    with pytest.raises(HTTPException) as ei:
        await invoke_tracked_function("createOrder", {}, st, "ops")
    assert ei.value.status_code == 403


@pytest.mark.asyncio
async def test_role_not_in_writable_by_is_403():
    st = _state(role_caps=[Capability.WRITE.value], writable_by=["someone_else"])
    with pytest.raises(HTTPException) as ei:
        await invoke_tracked_function("createOrder", {}, st, "ops")
    assert ei.value.status_code == 403


@pytest.mark.asyncio
async def test_admin_bypasses_acl():
    st = _state(role_caps=[Capability.ADMIN.value], writable_by=[])
    rows = await invoke_tracked_function("createOrder", {}, st, "ops")
    assert rows == [{"id": 1, "name": "ada"}]


@pytest.mark.asyncio
async def test_unknown_function_is_400():
    st = _state(role_caps=[Capability.ADMIN.value])
    with pytest.raises(HTTPException) as ei:
        await invoke_tracked_function("nope", {}, st, "ops")
    assert ei.value.status_code == 400


@pytest.mark.asyncio
async def test_disconnected_source_is_503():
    st = _state(role_caps=[Capability.ADMIN.value], connected=False)
    with pytest.raises(HTTPException) as ei:
        await invoke_tracked_function("createOrder", {}, st, "ops")
    assert ei.value.status_code == 503


# ---- Cypher CALL parsing (REQ-872) -----------------------------------------


def test_split_call_args_respects_quotes():
    assert _split_call_args("1, 'a, b', $x") == ["1", " 'a, b'", " $x"]


def test_parse_call_literal_types():
    p = {"x": 42}
    assert _parse_call_literal("$x", p) == 42
    assert _parse_call_literal("'hi'", {}) == "hi"
    assert _parse_call_literal("7", {}) == 7
    assert _parse_call_literal("3.5", {}) == 3.5
    assert _parse_call_literal("true", {}) is True
    assert _parse_call_literal("null", {}) is None


def test_detect_registered_call_with_yield():
    st = _state(role_caps=[Capability.ADMIN.value])
    got = detect_registered_call("CALL createOrder(7, 'x') YIELD id, name AS n", st, {})
    assert got is not None
    name, args, yields = got
    assert name == "createOrder"
    assert list(args.values()) == [7, "x"]
    assert yields == [("id", "id"), ("name", "n")]


def test_detect_registered_call_binds_params():
    st = _state(role_caps=[Capability.ADMIN.value])
    _n, args, _y = detect_registered_call("CALL createOrder($cid)", st, {"cid": 99})
    assert list(args.values()) == [99]


def test_detect_ignores_unregistered_name():
    st = _state(role_caps=[Capability.ADMIN.value])
    assert detect_registered_call("CALL db.labels()", st, {}) is None
    assert detect_registered_call("CALL somethingElse(1)", st, {}) is None
