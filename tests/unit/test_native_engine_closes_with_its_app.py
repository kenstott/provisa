# Copyright (c) 2026 Kenneth Stott
# Canary: aff5a66c-bc01-490d-8939-1eaafd7f0f81
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A native engine's runtime belongs to the app that built it.

One process ran two apps in turn, the first on an embedded DuckDB store and the second on a
Postgres store. The engine's runtime outlived the first app: asked by the second, it answered
"the store is attached" from what it remembered, and "which store" from the environment as it now
stood, so the cache write was sent to a ``mat_store`` catalog that had never been attached
(``Binder Error: Catalog "mat_store" does not exist!``, a 500). The runtime is closed with its
app, and a runtime's store is the one it attached."""

from __future__ import annotations

import os
from types import SimpleNamespace

import pytest

from provisa.federation.duckdb_runtime import DuckDBFederationRuntime
from provisa.federation.engine import build_engine


class _EnvStore:
    """A store engine that names the store as the deployment's variable has it now."""

    def materialize_store(self) -> str:
        return os.environ["PROVISA_MATERIALIZE_URL"]


def test_a_runtimes_store_is_the_one_it_attached(monkeypatch, tmp_path):
    monkeypatch.setenv("PROVISA_MATERIALIZE_URL", f"duckdb:///{tmp_path / 'first.duckdb'}")
    runtime = DuckDBFederationRuntime(store_engine=_EnvStore())
    try:
        assert runtime.ensure_materialize_attached() == "mat_store"
        assert runtime.mv_store_broker() is not None  # embedded store: reached through its broker

        # The variable moves on (another app in this process names another store).
        monkeypatch.setenv("PROVISA_MATERIALIZE_URL", "postgresql://u:p@localhost:1/elsewhere")
        # This runtime attached the embedded store and nothing else: it must not now claim a
        # store attached on its connection that it never attached.
        assert runtime.mv_store_broker() is not None
        assert runtime._store_dsn().startswith("duckdb:///")  # noqa: SLF001
    finally:
        runtime.close()


def test_closing_the_engine_drops_its_runtime_and_the_next_app_builds_its_own(
    monkeypatch, tmp_path
):
    engine = build_engine("duckdb")
    backend = engine.backend
    state = SimpleNamespace()

    monkeypatch.setenv("PROVISA_MATERIALIZE_URL", f"duckdb:///{tmp_path / 'first.duckdb'}")
    first = backend._store_runtime()  # noqa: SLF001
    first.ensure_materialize_attached()
    assert first._store_dsn().endswith("first.duckdb")  # noqa: SLF001

    backend.close(state)  # the lifespan's shutdown
    assert backend._runtime is None  # noqa: SLF001
    with pytest.raises(Exception, match="(?i)clos"):
        first.connection.execute("SELECT 1")  # its connection is closed, not left open

    monkeypatch.setenv("PROVISA_MATERIALIZE_URL", f"duckdb:///{tmp_path / 'second.duckdb'}")
    second = backend._store_runtime()  # noqa: SLF001
    try:
        assert second is not first
        second.ensure_materialize_attached()
        assert second._store_dsn().endswith("second.duckdb")  # noqa: SLF001
    finally:
        backend.close(state)


def test_closing_an_engine_that_built_no_runtime_does_nothing():
    backend = build_engine("duckdb").backend
    backend.close(SimpleNamespace())
    assert backend._runtime is None  # noqa: SLF001
