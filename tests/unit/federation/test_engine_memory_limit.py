# Copyright (c) 2026 Kenneth Stott
# Canary: 6e9fcc8c-ca3f-4fac-824e-5488f78c0368
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2006: a deployment may cap the memory of every DuckDB engine it builds
(engine.duckdb_memory_limit); a demo sandbox's own cap is its engine's, and unset leaves DuckDB's
own (a share of the host)."""

# Requirements: REQ-2006

from __future__ import annotations

from types import SimpleNamespace

from provisa.core import settings_registry
from provisa.federation.duckdb_runtime import DuckDBFederationRuntime

_MIB = 1024 * 1024


def _limit(store_engine) -> str:
    runtime = DuckDBFederationRuntime(store_engine=store_engine)
    try:
        return runtime._con.execute("SELECT current_setting('memory_limit')").fetchone()[0]
    finally:
        runtime._con.close()


def _engine(memory_limit=None):
    return SimpleNamespace(memory_limit=memory_limit, temp_org=None)


def test_the_deployment_cap_is_every_engine_s(monkeypatch):
    monkeypatch.setenv("PROVISA_ENGINE_DUCKDB_MEMORY_LIMIT", str(512 * _MIB))
    assert settings_registry.value("engine.duckdb_memory_limit") == 512 * _MIB
    assert _limit(_engine()) == "512.0 MiB"
    assert _limit(None) == "512.0 MiB"  # a runtime built for no org's engine too


def test_a_sandbox_s_own_cap_wins(monkeypatch):
    monkeypatch.setenv("PROVISA_ENGINE_DUCKDB_MEMORY_LIMIT", str(512 * _MIB))
    assert _limit(_engine(256 * _MIB)) == "256.0 MiB"


def test_unset_leaves_duckdb_s_own(monkeypatch):
    monkeypatch.delenv("PROVISA_ENGINE_DUCKDB_MEMORY_LIMIT", raising=False)
    assert settings_registry.value("engine.duckdb_memory_limit") is None
    import duckdb

    own = duckdb.connect().execute("SELECT current_setting('memory_limit')").fetchone()[0]
    assert _limit(_engine()) == own
