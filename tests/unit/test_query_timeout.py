# Copyright (c) 2026 Kenneth Stott
# Canary: e61ee5c0-c37d-4c20-92e5-d61e27420d3c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Query-timeout enforcement (REQ-064).

Runaway Trino queries are bounded by the ``query_max_execution_time`` session
property, which ``execute_trino`` always injects. The timeout value resolves
from ``server_limits['engine_query_timeout']``, else the
``PROVISA_ENGINE_QUERY_TIMEOUT`` env var, else a 120-second default
(``provisa/executor/trino.py``).
"""

from __future__ import annotations

import pytest


from provisa.executor.trino import _trino_query_timeout, execute_trino


@pytest.fixture
def _clean_limits(monkeypatch):
    """Force _trino_query_timeout to resolve from env/default, not app state."""
    monkeypatch.delenv("PROVISA_ENGINE_QUERY_TIMEOUT", raising=False)
    import provisa.api.app as app_mod

    monkeypatch.setattr(app_mod.state, "server_limits", {}, raising=False)
    return app_mod


class _FakeCursor:
    def __init__(self) -> None:
        self.executed: list[str] = []
        self.description = [("n",)]

    def execute(self, sql, params=None):  # noqa: D401
        self.executed.append(sql)

    def fetchone(self):
        return (1,)

    def fetchall(self):
        return [(1,)]


class _FakeConn:
    def __init__(self) -> None:
        self.cursor_obj = _FakeCursor()

    def cursor(self):
        return self.cursor_obj


class TestTrinoQueryTimeoutValue:
    def test_default_is_120_seconds(self, _clean_limits):
        assert _trino_query_timeout() == 120

    def test_env_var_override(self, _clean_limits, monkeypatch):
        monkeypatch.setenv("PROVISA_ENGINE_QUERY_TIMEOUT", "45")
        assert _trino_query_timeout() == 45

    def test_server_limit_takes_precedence(self, _clean_limits, monkeypatch):
        monkeypatch.setenv("PROVISA_ENGINE_QUERY_TIMEOUT", "45")
        monkeypatch.setattr(
            _clean_limits.state, "server_limits", {"engine_query_timeout": 77}, raising=False
        )
        assert _trino_query_timeout() == 77


class TestExecuteTrinoInjectsTimeout:
    def test_timeout_hint_always_injected(self, _clean_limits):
        conn = _FakeConn()
        execute_trino(conn, "SELECT 1 AS n")
        set_stmts = [s for s in conn.cursor_obj.executed if s.upper().startswith("SET SESSION")]
        assert any("query_max_execution_time" in s and "'120s'" in s for s in set_stmts), (
            f"expected query_max_execution_time='120s' SET SESSION; got {set_stmts}"
        )

    def test_explicit_hint_not_overridden(self, _clean_limits):
        conn = _FakeConn()
        execute_trino(conn, "SELECT 1 AS n", session_hints={"query_max_execution_time": "5s"})
        timeout_sets = [
            s
            for s in conn.cursor_obj.executed
            if "query_max_execution_time" in s and s.upper().startswith("SET SESSION")
        ]
        # setdefault must not clobber the caller-supplied value.
        assert timeout_sets, "timeout hint missing"
        assert all("'5s'" in s for s in timeout_sets), timeout_sets
        assert not any("'120s'" in s for s in timeout_sets), timeout_sets
