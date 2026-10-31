# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""The kill-a-node check's classification and verdict (REQ-1911)."""

from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import grpc
import httpx
import pytest

BENCH = Path(__file__).resolve().parents[3] / "demo" / "named" / "perf" / "bench"
sys.path.insert(0, str(BENCH))

import chaos_check as cc  # noqa: E402


class _Rpc(grpc.RpcError):
    def __init__(self, status: grpc.StatusCode) -> None:
        self._status = status

    def code(self) -> grpc.StatusCode:
        return self._status


@pytest.mark.parametrize(
    "exc",
    [ConnectionResetError("reset"), httpx.ReadError("gone"), _Rpc(grpc.StatusCode.UNAVAILABLE)],
)
def test_a_lost_connection_is_a_dropped_session(exc: BaseException) -> None:
    assert cc.is_dropped(exc)


@pytest.mark.parametrize(
    "exc",
    [
        RuntimeError("HTTP 500"),
        httpx.HTTPStatusError(
            "500", request=httpx.Request("GET", "http://x"), response=httpx.Response(500)
        ),
        _Rpc(grpc.StatusCode.INTERNAL),
    ],
)
def test_a_failure_the_server_answered_is_an_error(exc: BaseException) -> None:
    assert not cc.is_dropped(exc)


def _records(*extra: tuple[float, str, str, str]) -> list[tuple[float, str, str, str]]:
    return [(1.0, "pgwire", "ok", ""), (30.0, "pgwire", "ok", ""), *extra]


def _verdict(*extra: tuple[float, str, str, str]) -> dict[str, Any]:
    return cc.verdict(_records(*extra), transports=["pgwire"], killed_at=10, grace_s=15)


def test_a_dropped_session_within_the_grace_period_passes() -> None:
    out = _verdict((11.0, "pgwire", "dropped", "reset"))
    assert out["passed"] and out["problems"] == []
    assert out["transports"]["pgwire"]["during"] == {"dropped": 1}
    assert out["transports"]["pgwire"]["first_failure"] == "dropped: reset"


@pytest.mark.parametrize(
    "extra, problem",
    [
        ((11.0, "pgwire", "wrong", ""), "pgwire: wrong during: "),
        ((11.0, "pgwire", "error", "500"), "pgwire: error during: 500"),
        ((26.0, "pgwire", "dropped", "reset"), "pgwire: dropped after: reset"),
        ((5.0, "pgwire", "dropped", "reset"), "pgwire: dropped before: reset"),
    ],
)
def test_anything_else_fails(extra: tuple[float, str, str, str], problem: str) -> None:
    out = _verdict(extra)
    assert not out["passed"] and out["problems"] == [problem]


def test_a_transport_silent_after_the_kill_fails() -> None:
    out = cc.verdict([(1.0, "bolt", "ok", "")], transports=["bolt"], killed_at=10, grace_s=15)
    assert out["problems"] == ["bolt: answered nothing after the grace period"]


def test_haproxy_has_a_frontend_per_listener_over_both_nodes() -> None:
    a = cc.Node("a", "10.0.0.2", {"http": 1, "pgwire": 2, "bolt": 3, "flight": 4, "grpc": 5})
    b = cc.Node("b", "10.0.0.5", {"http": 6, "pgwire": 7, "bolt": 8, "flight": 9, "grpc": 10})
    front = {"http": 11, "pgwire": 12, "bolt": 13, "flight": 14, "grpc": 15}
    config = cc.haproxy_config([a, b], front)
    assert "bind *:12" in config
    assert "server a 10.0.0.2:2 check port 1 inter 500ms fall 2 rise 2" in config
    assert "server b 10.0.0.5:7 check port 6 inter 500ms fall 2 rise 2" in config


def test_the_check_reconnects_after_a_drop_and_notices_a_wrong_answer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    script = iter(["one", ConnectionResetError("reset"), "one", "two"])

    class Client:
        def __init__(self, _ep: Any) -> None:
            pass

        def rows(self, _query: Any, _role: str) -> tuple[Any, ...]:
            step = next(script, None)
            if step is None:
                stop.set()
                return (["c"], [["one"]], None)
            if isinstance(step, Exception):
                raise step
            return (["c"], [[step]], None)

        def close(self) -> None:
            pass

    monkeypatch.setitem(cc.ol.CLIENTS, "fake", Client)
    monkeypatch.setattr(cc.request_render, "build_query", lambda *_a, **_k: None)
    monkeypatch.setattr(cc, "_RECONNECT_S", 0)
    setup = SimpleNamespace(request_role=lambda _t, role: role)
    ep = SimpleNamespace(setup=setup, role="r")
    seen: list[str] = []
    stop = cc.threading.Event()
    cc._loop("fake", ep, stop, lambda _t, outcome, _d: seen.append(outcome))
    assert seen == ["ok", "dropped", "ok", "wrong", "ok"]
