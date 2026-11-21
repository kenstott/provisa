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
property, which every Trino read carries in its session (REQ-2194: the
``x-provisa-trino-context`` call header the Zaychik proxy reads). The timeout value resolves
from ``server_limits['engine_query_timeout']``, else the
``PROVISA_ENGINE_QUERY_TIMEOUT`` env var, else a 120-second default
(``provisa/executor/trino.py``).
"""

from __future__ import annotations

import pytest


from provisa.executor.trino import _trino_query_timeout
from tests.zaychik_double import ProxyConnection, backend_state, trino_backend


@pytest.fixture
def _clean_limits(monkeypatch):
    """Force _trino_query_timeout to resolve from env/default, not app state."""
    monkeypatch.delenv("PROVISA_ENGINE_QUERY_TIMEOUT", raising=False)
    import provisa.api.app as app_mod

    monkeypatch.setattr(app_mod.state, "server_limits", {}, raising=False)
    return app_mod


class TestTrinoQueryTimeoutValue:
    def test_default_is_120_seconds(self, _clean_limits):
        assert _trino_query_timeout() == 120

    def test_env_var_override(self, _clean_limits, monkeypatch):
        monkeypatch.setenv("PROVISA_ENGINE_QUERY_TIMEOUT", "45")
        assert _trino_query_timeout() == 45

    def test_config_value_applies_and_env_overrides_it(self, _clean_limits, monkeypatch):
        """REQ-1913: one resolution order — stored, then env, then config, then the default.
        The copy app state takes at boot is not consulted (it cannot see a stored change)."""
        from provisa.core import settings_registry

        monkeypatch.setattr(
            settings_registry, "_config", {"server": {"limits": {"engine_query_timeout": 77}}}
        )
        monkeypatch.setattr(
            _clean_limits.state, "server_limits", {"engine_query_timeout": 999}, raising=False
        )
        assert _trino_query_timeout() == 77
        monkeypatch.setenv("PROVISA_ENGINE_QUERY_TIMEOUT", "45")
        assert _trino_query_timeout() == 45


class TestExecuteTrinoInjectsTimeout:
    def _session(self, session_hints=None) -> dict:
        proxy = ProxyConnection()
        trino_backend().execute_sync(
            backend_state(proxy), "SELECT 1 AS n", session_hints=session_hints
        ).rows
        (session,) = proxy.sessions
        return session["properties"]

    def test_timeout_hint_always_injected(self, _clean_limits):
        properties = self._session()
        assert properties["query_max_execution_time"] == "120s", properties

    def test_explicit_hint_not_overridden(self, _clean_limits):
        properties = self._session({"query_max_execution_time": "5s"})
        # setdefault must not clobber the caller-supplied value.
        assert properties["query_max_execution_time"] == "5s", properties
