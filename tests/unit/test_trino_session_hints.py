# Copyright (c) 2026 Kenneth Stott
# Canary: 9a4b17c6-52e8-4d31-b7f0-63c8a05e2914
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1188: per-plan Trino session properties, and the ones that are never optional.

A session hint is a plan-level decision — this query needs a bigger memory ceiling, that one is
retryable — so it has to travel with the plan rather than be set globally and hoped for. Three
things then matter: the hints reach the connection as ``SET SESSION`` before the query, the
deployment-wide hints are underneath the plan's rather than over them, and the runaway-query
timeout is always applied even when the plan asked for nothing.

Ordering is the part a live Trino would not catch cheaply: a hint issued after the query has no
effect on it, and the failure looks like the hint being ignored.
"""

# Requirements: REQ-1188

from __future__ import annotations

import types

import pytest

from provisa.executor.trino import execute_trino


class _Cursor:
    def __init__(self, log):
        self._log = log
        self.description = [("id", "integer")]

    def execute(self, sql, params=None):
        self._log.append(sql)

    def fetchall(self):
        return [[1]]


class _Conn:
    def __init__(self, log):
        self._log = log

    def cursor(self):
        return _Cursor(self._log)


@pytest.fixture
def issued(monkeypatch):
    """Every statement the executor put on the connection, in order."""
    log: list[str] = []
    monkeypatch.setattr("provisa.executor.trino._alive", lambda conn: True)
    monkeypatch.setattr(
        "provisa.api.app.state",
        types.SimpleNamespace(engine_session_hints={}, engine_conn_kwargs={}),
        raising=False,
    )
    return types.SimpleNamespace(log=log, conn=_Conn(log))


def _hints(log):
    return [s for s in log if s.startswith("SET SESSION ")]


def test_a_plans_hints_are_set_on_the_session_before_its_query(issued):
    execute_trino(
        issued.conn,
        "SELECT id FROM orders",
        session_hints={"query_max_memory": "8GB"},
    )

    assert "SET SESSION query_max_memory = '8GB'" in issued.log
    # After the query it would apply to the next one, not this one.
    assert issued.log.index("SET SESSION query_max_memory = '8GB'") < issued.log.index(
        "SELECT id FROM orders"
    )


def test_the_runaway_query_timeout_is_applied_even_with_no_plan_hints(issued):
    """A query with no hints is the common case and the one that starves workers when it hangs."""
    execute_trino(issued.conn, "SELECT id FROM orders")

    assert any(s.startswith("SET SESSION query_max_execution_time = ") for s in _hints(issued.log))


def test_a_plan_may_override_the_default_timeout(issued):
    execute_trino(
        issued.conn,
        "SELECT id FROM orders",
        session_hints={"query_max_execution_time": "30s"},
    )

    timeouts = [s for s in _hints(issued.log) if "query_max_execution_time" in s]
    assert timeouts == ["SET SESSION query_max_execution_time = '30s'"]


def test_the_plans_hints_win_over_the_deployments(issued, monkeypatch):
    """The deployment sets a floor; a plan that asked for something specific asked for a reason."""
    monkeypatch.setattr(
        "provisa.api.app.state",
        types.SimpleNamespace(
            engine_session_hints={"query_max_memory": "2GB", "retry_policy": "QUERY"},
            engine_conn_kwargs={},
        ),
        raising=False,
    )

    execute_trino(
        issued.conn, "SELECT id FROM orders", session_hints={"query_max_memory": "8GB"}
    )

    assert "SET SESSION query_max_memory = '8GB'" in issued.log
    assert "SET SESSION query_max_memory = '2GB'" not in issued.log
    # A deployment hint the plan said nothing about still applies.
    assert "SET SESSION retry_policy = 'QUERY'" in issued.log


def test_a_quote_in_a_hint_cannot_close_the_set_statement(issued):
    """The hint value is interpolated into SQL, so a quote in it would end the string and let
    whatever follows execute as its own statement."""
    execute_trino(
        issued.conn,
        "SELECT id FROM orders",
        session_hints={"query_max_memory": "8GB'; DROP TABLE orders; --"},
    )

    injected = [s for s in _hints(issued.log) if "query_max_memory" in s]
    assert injected == ["SET SESSION query_max_memory = '8GB; DROP TABLE orders; --'"]
    assert not any("DROP TABLE" in s and s.startswith("DROP") for s in issued.log)
