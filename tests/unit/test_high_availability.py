# Copyright (c) 2026 Kenneth Stott
# Canary: 9f3a1c2e-7d4b-4e8a-b5c6-2e0f8d3a4b7c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for REQ-703 — two-tier HA: retry backoff and Trino watcher."""

from __future__ import annotations

import sys
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


async def _run_in_thread(fn, *args):
    return fn(*args)


# ---------------------------------------------------------------------------
# Tier 1 — backoff helpers
# ---------------------------------------------------------------------------


def test_trino_backoff_bounded():
    from provisa.executor.trino import _backoff_secs

    for attempt in range(10):
        val = _backoff_secs(attempt, cap=30.0)
        assert 0.0 <= val <= 30.0


def test_trino_backoff_grows_with_attempt():
    from provisa.executor.trino import _backoff_secs

    samples_low = [_backoff_secs(0) for _ in range(200)]
    samples_high = [_backoff_secs(5) for _ in range(200)]
    assert sum(samples_high) > sum(samples_low)


def test_direct_backoff_bounded():
    from provisa.executor.direct import _backoff_secs

    for attempt in range(10):
        val = _backoff_secs(attempt, cap=30.0)
        assert 0.0 <= val <= 30.0


def test_retry_budget_env(monkeypatch):
    monkeypatch.setenv("PROVISA_RETRY_BUDGET_SECS", "15")
    with patch.dict(sys.modules, {"provisa.api.app": None}):
        from provisa.executor.trino import _retry_budget

        assert _retry_budget() == 15.0


def test_is_retryable_connection_error():
    from provisa.executor.trino import _is_retryable

    assert _is_retryable(ConnectionError("dropped"))


def test_is_retryable_coordinator_not_available():
    from provisa.executor.errors import FederationError
    from provisa.executor.trino import _is_retryable

    exc = FederationError(error_type=None, error_name="COORDINATOR_NOT_AVAILABLE", message="gone")
    assert _is_retryable(exc)


def test_is_retryable_memory_error_is_false():
    from provisa.executor.errors import FederationError
    from provisa.executor.trino import _is_retryable

    exc = FederationError(error_type=None, error_name="EXCEEDED_LOCAL_MEMORY_LIMIT", message="oom")
    assert not _is_retryable(exc)


def test_is_retryable_generic_exception_is_false():
    from provisa.executor.trino import _is_retryable

    assert not _is_retryable(ValueError("nope"))


# ---------------------------------------------------------------------------
# Tier 1 — writes are never retried
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_direct_write_not_retried():
    """INSERT should raise immediately on ConnectionError without retry."""
    from provisa.executor.direct import execute_direct

    pool = MagicMock()
    pool.execute = AsyncMock(side_effect=ConnectionError("db down"))

    with pytest.raises(ConnectionError):
        await execute_direct(pool, "src1", "INSERT INTO t VALUES (1)")

    assert pool.execute.call_count == 1


@pytest.mark.asyncio
async def test_direct_read_retries_on_connection_error():
    """SELECT retries on transient ConnectionError and succeeds on third attempt."""
    from provisa.executor.direct import execute_direct, QueryResult

    calls = []

    async def flaky(*_a):
        calls.append(1)
        if len(calls) < 3:
            raise ConnectionError("transient")
        return QueryResult(rows=[(1,)], column_names=["x"])

    pool = MagicMock()
    pool.execute = flaky

    with (
        patch("provisa.executor.direct._retry_budget", return_value=10.0),
        patch("provisa.executor.direct._backoff_secs", return_value=0.0),
    ):
        result = await execute_direct(pool, "src1", "SELECT 1")

    assert result.rows == [(1,)]
    assert len(calls) == 3


# ---------------------------------------------------------------------------
# Tier 2 — watch_trino watcher
# ---------------------------------------------------------------------------


def _app_module(state_obj: MagicMock) -> MagicMock:
    """Return a fake provisa.api.app module with the given state."""
    mod = MagicMock()
    mod.state = state_obj
    return mod


@pytest.mark.asyncio
async def test_watch_trino_no_op_when_healthy():
    """watch_trino exits early without calling docker when Trino responds."""
    from provisa.scheduler.jobs import watch_trino

    mock_state = MagicMock()
    mock_state.trino_conn = MagicMock()

    with (
        patch.dict(sys.modules, {"provisa.api.app": _app_module(mock_state)}),
        patch("asyncio.to_thread", side_effect=_run_in_thread),
        patch("provisa.scheduler.jobs._trino_ping", return_value=None),
        patch("asyncio.create_subprocess_exec") as mock_exec,
    ):
        await watch_trino()

    mock_exec.assert_not_called()


@pytest.mark.asyncio
async def test_watch_trino_calls_docker_start_when_unresponsive():
    """watch_trino issues 'docker start provisa-trino-1' when ping fails."""
    from provisa.scheduler.jobs import watch_trino

    mock_state = MagicMock()
    mock_state.trino_conn = MagicMock()
    mock_state.trino_conn_kwargs = {}

    mock_proc = AsyncMock()
    mock_proc.returncode = 0
    mock_proc.communicate = AsyncMock(return_value=(b"", b""))

    ping_calls = []

    def ping_side_effect(conn):
        ping_calls.append(conn)
        if len(ping_calls) == 1:
            raise ConnectionError("down")

    with (
        patch.dict(sys.modules, {"provisa.api.app": _app_module(mock_state)}),
        patch("asyncio.to_thread", side_effect=_run_in_thread),
        patch("provisa.scheduler.jobs._trino_ping", side_effect=ping_side_effect),
        patch("asyncio.create_subprocess_exec", return_value=mock_proc) as mock_exec,
        patch("asyncio.sleep", new_callable=AsyncMock),
        patch("trino.dbapi.connect", return_value=MagicMock()),
    ):
        await watch_trino()

    mock_exec.assert_called_once()
    args = mock_exec.call_args[0]
    assert "docker" in args
    assert "start" in args
    assert "provisa-trino-1" in args


@pytest.mark.asyncio
async def test_watch_trino_skips_when_no_conn():
    """watch_trino exits immediately when state.trino_conn is None."""
    from provisa.scheduler.jobs import watch_trino

    mock_state = MagicMock()
    mock_state.trino_conn = None

    with (
        patch.dict(sys.modules, {"provisa.api.app": _app_module(mock_state)}),
        patch("asyncio.create_subprocess_exec") as mock_exec,
    ):
        await watch_trino()

    mock_exec.assert_not_called()


@pytest.mark.asyncio
async def test_watch_trino_logs_error_on_docker_failure():
    """watch_trino returns without raising when docker start fails."""
    from provisa.scheduler.jobs import watch_trino

    mock_state = MagicMock()
    mock_state.trino_conn = MagicMock()

    mock_proc = AsyncMock()
    mock_proc.returncode = 1
    mock_proc.communicate = AsyncMock(return_value=(b"", b"container not found"))

    with (
        patch.dict(sys.modules, {"provisa.api.app": _app_module(mock_state)}),
        patch("asyncio.to_thread", side_effect=_run_in_thread),
        patch("provisa.scheduler.jobs._trino_ping", side_effect=ConnectionError("down")),
        patch("asyncio.create_subprocess_exec", return_value=mock_proc),
    ):
        await watch_trino()
