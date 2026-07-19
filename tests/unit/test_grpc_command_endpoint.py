# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1150: HTTP mirror of the gRPC CallCommand RPC used by the gRPC Explorer.

Covers the role-visible command listing and the invoke endpoint's arg handling.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import cast

import pytest
from fastapi import HTTPException, Request

import provisa.api.app as app_mod
import provisa.api.data.action_exec as action_exec
from provisa.api.data.endpoint_grpc_proxy import grpc_command, grpc_commands


class _FakeRequest:
    def __init__(self, body: dict):
        self._body = body

    async def json(self):
        return self._body


def _req(body: dict) -> Request:
    return cast(Request, _FakeRequest(body))


def _state():
    grpc_fn = {
        "name": "random_grpc_set",
        "description": "demo",
        "domain_id": "pet-store",
        "visible_to": ["admin"],
        "arguments": [],
    }
    py_fn = {
        "name": "random_python_set",
        "description": "demo",
        "domain_id": "pet-store",
        "visible_to": ["admin"],
        "arguments": [{"name": "rows", "type": "Int"}, {"name": "seed", "type": "Int"}],
    }
    hidden_fn = {
        "name": "secret_cmd",
        "domain_id": "pet-store",
        "visible_to": ["ops"],
        "arguments": [],
    }
    return SimpleNamespace(
        # both plain and domain-prefixed keys point at the same dict (mirrors app_loaders)
        tracked_functions={
            "random_grpc_set": grpc_fn,
            "ps__random_grpc_set": grpc_fn,
            "random_python_set": py_fn,
            "secret_cmd": hidden_fn,
        },
        roles={"admin": {"domain_access": ["*"]}},
    )


@pytest.fixture(autouse=True)
def _patch_state(monkeypatch):
    monkeypatch.setattr(app_mod, "state", _state(), raising=False)


@pytest.mark.asyncio
async def test_commands_list_dedupes_and_filters_by_visibility():
    out = await grpc_commands("admin")
    names = [c["name"] for c in out]
    assert names.count("random_grpc_set") == 1  # deduped despite the prefixed alias
    assert "random_python_set" in names
    assert "secret_cmd" not in names  # not visible to admin


@pytest.mark.asyncio
async def test_commands_list_reports_arguments():
    out = await grpc_commands("admin")
    py = next(c for c in out if c["name"] == "random_python_set")
    assert py["arguments"] == [
        {"name": "rows", "type": "Int"},
        {"name": "seed", "type": "Int"},
    ]


@pytest.mark.asyncio
async def test_invoke_routes_through_executor(monkeypatch):
    captured = {}

    async def _fake_invoke(name, args, state, role_id):
        captured["name"] = name
        captured["args"] = args
        captured["role_id"] = role_id
        return [{"id": 1, "region": "east"}]

    monkeypatch.setattr(action_exec, "invoke_tracked_function", _fake_invoke)
    resp = await grpc_command(
        "admin", _req({"name": "random_python_set", "args_json": '{"rows": 10, "seed": 42}'})
    )
    import json

    assert json.loads(bytes(resp.body)) == [{"id": 1, "region": "east"}]
    assert captured == {
        "name": "random_python_set",
        "args": {"rows": 10, "seed": 42},
        "role_id": "admin",
    }


@pytest.mark.asyncio
async def test_invoke_empty_args_json_defaults_to_empty_dict(monkeypatch):
    captured = {}

    async def _fake_invoke(name, args, state, role_id):
        captured["args"] = args
        return []

    monkeypatch.setattr(action_exec, "invoke_tracked_function", _fake_invoke)
    await grpc_command("admin", _req({"name": "random_grpc_set", "args_json": ""}))
    assert captured["args"] == {}


@pytest.mark.asyncio
async def test_invoke_unknown_command_404():
    with pytest.raises(HTTPException) as ei:
        await grpc_command("admin", _req({"name": "nope", "args_json": "{}"}))
    assert ei.value.status_code == 404


@pytest.mark.asyncio
async def test_invoke_bad_args_json_400():
    with pytest.raises(HTTPException) as ei:
        await grpc_command(
            "admin", _req({"name": "random_grpc_set", "args_json": "{not json"})
        )
    assert ei.value.status_code == 400


@pytest.mark.asyncio
async def test_invoke_non_object_args_400():
    with pytest.raises(HTTPException) as ei:
        await grpc_command(
            "admin", _req({"name": "random_grpc_set", "args_json": "[1, 2]"})
        )
    assert ei.value.status_code == 400
