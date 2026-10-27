# Copyright (c) 2026 Kenneth Stott
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


def test_the_in_memory_store_and_a_duckdb_source_are_capped_too(monkeypatch, caplog):
    """Each DuckDB database is capped on its own: the federation engine's cap leaves the
    in-memory store and a DuckDB source's database at DuckDB's own unless they take it too."""
    import asyncio

    from provisa.executor.drivers.duckdb_driver import DuckDBDriver
    from provisa.federation import materialize_broker

    monkeypatch.setenv("PROVISA_ENGINE_DUCKDB_MEMORY_LIMIT", str(512 * _MIB))
    with caplog.at_level("INFO", logger="uvicorn.error"):
        store = materialize_broker._MemoryStore()
        driver = DuckDBDriver()
        asyncio.run(driver.connect("", 0, ":memory:", "", ""))
    try:
        setting = "SELECT current_setting('memory_limit')"
        assert store.db.execute(setting).fetchone()[0] == "512.0 MiB"
        assert driver._root is not None
        assert driver._root.execute(setting).fetchone()[0] == "512.0 MiB"
    finally:
        store.db.close()
        driver._let_go()
    said = [r.getMessage() for r in caplog.records]
    assert "DuckDB in-memory store: memory_limit 512.0 MiB" in said
    assert "DuckDB source :memory:: memory_limit 512.0 MiB" in said


def test_every_duckdb_this_process_opens_is_capped(monkeypatch):
    """No module opens a DuckDB database but through engine_memory.connect, which caps it: a
    database opened bare runs under DuckDB's own 80% of the host, whatever the deployment set."""
    import pathlib
    import re

    root = pathlib.Path(__file__).resolve().parents[3] / "provisa"
    bare = [
        f"{path.relative_to(root)}:{n}"
        for path in root.rglob("*.py")
        if path.name != "engine_memory.py"
        for n, line in enumerate(path.read_text().splitlines(), 1)
        if re.search(r"\bduckdb\.connect\(", line)
    ]
    assert bare == []


def test_a_short_lived_database_is_capped(monkeypatch):
    from provisa.core import engine_memory

    monkeypatch.setenv("PROVISA_ENGINE_DUCKDB_MEMORY_LIMIT", str(512 * _MIB))
    con = engine_memory.connect()
    try:
        assert con.execute("SELECT current_setting('memory_limit')").fetchone()[0] == "512.0 MiB"
    finally:
        con.close()


def test_an_extra_cap_stands_over_the_deployment_s(monkeypatch):
    from provisa.core import engine_memory

    monkeypatch.setenv("PROVISA_ENGINE_DUCKDB_MEMORY_LIMIT", str(512 * _MIB))
    con = engine_memory.connect(extra=engine_memory.config(256 * _MIB))
    try:
        assert con.execute("SELECT current_setting('memory_limit')").fetchone()[0] == "256.0 MiB"
    finally:
        con.close()
