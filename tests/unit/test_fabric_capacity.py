# Copyright (c) 2026 Kenneth Stott
# Canary: 8d3b1f6a-5c2e-4a97-9e0b-4f6c2a8d1e53
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The Fabric engine resumes its own capacity before connecting (REQ-1775)."""

# Requirements: REQ-1775

from __future__ import annotations

from types import SimpleNamespace

import httpx
import pytest

from provisa.federation import fabric_capacity as fc


@pytest.fixture
def configured(monkeypatch):
    monkeypatch.setenv("FABRIC_CAPACITY_NAME", "cap1")
    monkeypatch.setenv("FABRIC_RESOURCE_GROUP", "rg1")
    monkeypatch.setattr(fc, "_POLL_S", 0.0)


def _arm(states: list[str], calls: list[str]):
    """A fake ARM: two subscriptions, the capacity lives in the second; GETs walk ``states``."""
    seq = iter(states)
    current = {"state": None}

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        calls.append(f"{request.method} {path}")
        if path == "/subscriptions":
            return httpx.Response(
                200, json={"value": [{"subscriptionId": "s0"}, {"subscriptionId": "s1"}]}
            )
        if "/subscriptions/s0/" in path:
            return httpx.Response(404)
        if request.method == "POST":
            return httpx.Response(202)
        current["state"] = next(seq, current["state"])
        return httpx.Response(200, json={"properties": {"state": current["state"]}})

    return httpx.Client(transport=httpx.MockTransport(handler))


def test_unset_means_the_capacity_is_managed_externally(monkeypatch):
    monkeypatch.delenv("FABRIC_CAPACITY_NAME", raising=False)
    monkeypatch.delenv("FABRIC_RESOURCE_GROUP", raising=False)
    assert fc.capacity_configured() is False


def test_half_configured_is_a_misconfiguration(monkeypatch):
    monkeypatch.setenv("FABRIC_CAPACITY_NAME", "cap1")
    monkeypatch.delenv("FABRIC_RESOURCE_GROUP", raising=False)
    with pytest.raises(RuntimeError, match="set together"):
        fc.capacity_configured()


def test_an_active_capacity_is_left_alone(configured, monkeypatch):
    calls: list[str] = []
    monkeypatch.setattr(fc, "_client", lambda: _arm(["Active"], calls))
    fc.ensure_capacity_resumed()
    assert not any(c.startswith("POST") for c in calls)


def test_a_paused_capacity_is_resumed_and_waited_for(configured, monkeypatch):
    calls: list[str] = []
    monkeypatch.setattr(
        fc, "_client", lambda: _arm(["Paused", "Resuming", "Resuming", "Active"], calls)
    )
    fc.ensure_capacity_resumed()
    posts = [c for c in calls if c.startswith("POST")]
    assert posts == [
        "POST /subscriptions/s1/resourceGroups/rg1/providers/Microsoft.Fabric/capacities/cap1/resume"
    ]


def test_a_capacity_found_while_pausing_is_resumed_once_it_has_paused(configured, monkeypatch):
    """Found mid-pause (another client just suspended it), the capacity settles at Paused. Nothing
    resumes it but this call, so the resume is sent when it gets there — once."""
    calls: list[str] = []
    monkeypatch.setattr(fc, "_RESUME_TIMEOUT_S", 2.0)
    monkeypatch.setattr(
        fc,
        "_client",
        lambda: _arm(["Pausing", "Pausing", "Paused", "Paused", "Resuming", "Active"], calls),
    )
    fc.ensure_capacity_resumed()
    assert len([c for c in calls if c.startswith("POST")]) == 1


def test_a_terminal_state_raises(configured, monkeypatch):
    monkeypatch.setattr(fc, "_client", lambda: _arm(["Failed"], []))
    with pytest.raises(RuntimeError, match="terminal state"):
        fc.ensure_capacity_resumed()


class _CapacityNotActive(Exception):
    """A statement Fabric refused while its data plane does not yet serve the capacity, as
    pyodbc raises it (run 40024234397)."""

    def __init__(self) -> None:
        super().__init__(
            "('42000', '[42000] [Microsoft][ODBC Driver 18 for SQL Server][SQL Server]"
            "CapacityNotActive (24800) (SQLExecDirectW)')"
        )


def test_the_data_plane_is_waited_for_until_a_statement_is_served(configured, monkeypatch):
    """ARM reports a resumed capacity Active before Fabric's data plane serves it, and a statement
    in between is refused CapacityNotActive (run 40024234397). The wait is the warehouse's own
    answer, through the access the engine already has: Fabric's capacity listing shows only
    capacities its credential holds a Fabric role on (run 40033495801)."""
    tries: list[int] = []

    def probe() -> str:
        tries.append(1)
        if len(tries) < 3:
            raise _CapacityNotActive()
        return "connection"

    assert fc.await_data_plane(probe) == "connection"
    assert len(tries) == 3


def test_a_data_plane_that_never_serves_raises_naming_the_capacity(configured, monkeypatch):
    monkeypatch.setattr(fc, "_RESUME_TIMEOUT_S", 0.0)

    def probe() -> None:
        raise _CapacityNotActive()

    with pytest.raises(RuntimeError, match="'cap1' is Active in Azure but its warehouse"):
        fc.await_data_plane(probe)


def test_any_other_refusal_is_raised_at_once(configured):
    tries: list[int] = []

    def probe() -> None:
        tries.append(1)
        raise PermissionError("Login failed for user")

    with pytest.raises(PermissionError, match="Login failed"):
        fc.await_data_plane(probe)
    assert len(tries) == 1


class _ServingConnection:
    """A pyodbc connection whose warehouse serves statements."""

    def add_output_converter(self, *_args) -> None:
        pass

    def cursor(self) -> SimpleNamespace:
        cursor = SimpleNamespace(fetchall=lambda: [(1,)], close=lambda: None)
        cursor.execute = lambda _sql: cursor
        return cursor

    def close(self) -> None:
        pass


def test_the_fabric_runtime_resumes_before_it_connects_and_synapse_does_not(monkeypatch):
    from provisa.federation import mssql_warehouse_runtime as mwr

    order: list[str] = []
    monkeypatch.setattr(fc, "capacity_configured", lambda: True)
    monkeypatch.setattr(fc, "ensure_capacity_resumed", lambda: order.append("resume"))
    monkeypatch.setitem(
        __import__("sys").modules,
        "pyodbc",
        # A connection takes the datetimeoffset converter (REQ-2050), as pyodbc's does, and
        # answers the probe a managed capacity's connection is asked (await_data_plane).
        SimpleNamespace(connect=lambda *a, **k: order.append("connect") or _ServingConnection()),
    )

    class _Cred:
        def get_token(self, _scope):
            return SimpleNamespace(token="t")

    monkeypatch.setattr("azure.identity.DefaultAzureCredential", _Cred)
    mwr.MssqlWarehouseRuntime(server="s", database="d", engine_name="fabric")
    assert order == ["resume", "connect"]
    order.clear()
    mwr.MssqlWarehouseRuntime(server="s", database="d", engine_name="synapse")
    assert order == ["connect"]


def test_the_fabric_engine_connects_once_its_warehouse_serves_a_statement(configured, monkeypatch):
    """The engine's connection is the probe: opened and asked SELECT 1 until the warehouse
    serves it, each refused connection closed."""
    import sys
    import types

    from provisa.federation import mssql_warehouse_runtime as mwr

    opened: list["_Conn"] = []

    class _Cursor:
        def __init__(self, conn: "_Conn") -> None:
            self._conn = conn

        def execute(self, sql: str) -> "_Cursor":
            assert sql == "SELECT 1"
            if len(opened) < 3:
                raise _CapacityNotActive()
            return self

        def fetchall(self) -> list:
            return [(1,)]

        def close(self) -> None:
            pass

    class _Conn:
        closed = False

        def cursor(self) -> _Cursor:
            return _Cursor(self)

        def close(self) -> None:
            self.closed = True

    def connect(*args, **kwargs) -> _Conn:
        opened.append(_Conn())
        return opened[-1]

    monkeypatch.setitem(sys.modules, "pyodbc", types.SimpleNamespace(connect=connect))
    monkeypatch.setattr(
        "azure.identity.DefaultAzureCredential",
        lambda: types.SimpleNamespace(get_token=lambda scope: types.SimpleNamespace(token="t")),
    )
    monkeypatch.setattr("provisa.executor.drivers.zoned.mssql_connection", lambda conn: conn)
    monkeypatch.setattr(fc, "ensure_capacity_resumed", lambda: None)
    runtime = object.__new__(mwr.MssqlWarehouseRuntime)
    runtime._server, runtime._database, runtime._engine_name = "s", "d", "fabric"
    assert runtime._connect() is opened[-1]
    assert len(opened) == 3
    assert [c.closed for c in opened] == [True, True, False]
