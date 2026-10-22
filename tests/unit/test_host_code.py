# Copyright (c) 2026 Kenneth Stott
# Canary: 5fb610b3-77e2-4bfc-b116-e1a8ba9bae5d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2134: host code -- the python and script command kinds -- is off unless the operator
turns commands.allow_host_code on; off, registering, updating and calling one is refused."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.api.admin import actions_router
from provisa.core import settings_registry
from provisa.core.host_code import HostCodeRefused
from provisa.core.models import Function
from provisa.core.repositories import function as function_repo
from provisa.executor.function_dispatch import dispatch_function
from provisa.otel_compat import MemoryUdfTraceSink

_BINDINGS = {"python": {"callable": "json:dumps"}, "script": {"argv": ["/bin/true"]}}


def _state():
    return SimpleNamespace(
        roles={"ops": {"id": "ops", "capabilities": []}},
        udf_trace_sink=MemoryUdfTraceSink(),
        minted_sessions=[],
        udf_egress_allowlist=[],
    )


def _fn(impl_kind: str) -> dict:
    return {
        "name": "run_it",
        "source_id": "",
        "function_name": "run_it",
        "kind": "mutation",
        "returns": "",
        "impl_kind": impl_kind,
        "binding": _BINDINGS[impl_kind],
        "arguments": [],
    }


def test_host_code_is_off_by_default(monkeypatch):
    monkeypatch.delenv("PROVISA_COMMANDS_ALLOW_HOST_CODE", raising=False)
    settings_registry.bind_config({})
    assert settings_registry.value("commands.allow_host_code") is False


def test_the_config_file_turns_host_code_on(monkeypatch):
    monkeypatch.delenv("PROVISA_COMMANDS_ALLOW_HOST_CODE", raising=False)
    settings_registry.bind_config({"server": {"commands": {"allow_host_code": True}}})
    assert settings_registry.value("commands.allow_host_code") is True


@pytest.mark.asyncio
@pytest.mark.parametrize("impl_kind", ["python", "script"])
async def test_calling_host_code_is_refused_while_it_is_off(monkeypatch, impl_kind):
    monkeypatch.delenv("PROVISA_COMMANDS_ALLOW_HOST_CODE", raising=False)
    settings_registry.bind_config({})
    st = _state()
    with pytest.raises(HostCodeRefused) as refused:
        await dispatch_function(_fn(impl_kind), {}, st, "ops")
    assert refused.value.code == "functions.host_code_refused"
    assert refused.value.params == {"name": "run_it", "impl_kind": impl_kind}
    assert "commands.allow_host_code" in str(refused.value)


@pytest.mark.asyncio
async def test_calling_host_code_runs_it_while_it_is_on(monkeypatch):
    monkeypatch.setenv("PROVISA_COMMANDS_ALLOW_HOST_CODE", "true")
    fn = _fn("python") | {"binding": {"callable": "provisa_test_host_code:answer"}}
    module = SimpleNamespace(answer=lambda _payload, _session: [{"ok": 1}])
    monkeypatch.setitem(__import__("sys").modules, "provisa_test_host_code", module)
    assert await dispatch_function(fn, {}, _state(), "ops") == [{"ok": 1}]


@pytest.mark.asyncio
async def test_a_command_of_another_kind_is_not_host_code(monkeypatch):
    monkeypatch.delenv("PROVISA_COMMANDS_ALLOW_HOST_CODE", raising=False)
    settings_registry.bind_config({})
    fn = _fn("python") | {"impl_kind": "http", "binding": {"url": "http://localhost:1/x"}}
    # Not refused as host code: whatever stops it is its own kind's check.
    with pytest.raises(Exception) as failed:
        await dispatch_function(fn, {"t": {"kind": "table_ref"}}, _state(), "ops")
    assert not isinstance(failed.value, HostCodeRefused)


@pytest.mark.asyncio
@pytest.mark.parametrize("impl_kind", ["python", "script"])
async def test_registering_host_code_is_refused_while_it_is_off(monkeypatch, impl_kind):
    """Config load and the admin API's create both register through upsert_function."""
    monkeypatch.delenv("PROVISA_COMMANDS_ALLOW_HOST_CODE", raising=False)
    settings_registry.bind_config({})
    func = Function(**_fn(impl_kind))
    with pytest.raises(HostCodeRefused):
        await function_repo.upsert_function(None, func)  # pyright: ignore[reportArgumentType]


@pytest.mark.asyncio
@pytest.mark.parametrize("impl_kind", ["python", "script"])
async def test_updating_to_host_code_is_refused_while_it_is_off(monkeypatch, impl_kind):
    monkeypatch.delenv("PROVISA_COMMANDS_ALLOW_HOST_CODE", raising=False)
    settings_registry.bind_config({})
    monkeypatch.setattr(actions_router, "require_capability_request", lambda *_a: None)
    body = actions_router.FunctionInput(
        name="run_it", implKind=impl_kind, binding=_BINDINGS[impl_kind]
    )
    with pytest.raises(HostCodeRefused):
        await actions_router.update_function(SimpleNamespace(), "run_it", body)  # pyright: ignore[reportArgumentType]
