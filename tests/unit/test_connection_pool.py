# Copyright (c) 2026 Kenneth Stott
# Canary: 72f96a03-1196-4712-adfe-2125ce634ea9
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Pure unit tests for SourcePool non-I/O logic (REQ-052)."""

from __future__ import annotations

import pytest

from provisa.executor.drivers.base import DirectDriver
from provisa.executor.pool import _SOURCE_DIALECT, SourcePool
from provisa.executor.result import QueryResult


class _FakeDriver(DirectDriver):
    """Stand-in for DirectDriver — no network I/O."""

    def __init__(self) -> None:
        self.close_called = False
        self._is_connected = True

    async def connect(
        self,
        host: str,
        port: int,
        database: str,
        user: str,
        password: str,
        min_pool: int = 1,
        max_pool: int = 5,
    ) -> None:
        pass

    async def execute(self, sql: str, params: list | None = None) -> QueryResult:
        return QueryResult(rows=[], column_names=[])

    async def close(self) -> None:
        self.close_called = True
        self._is_connected = False

    @property
    def is_connected(self) -> bool:
        return self._is_connected


class TestSourcePoolPureLogic:
    def test_has_returns_false_initially(self):
        assert not SourcePool().has("x")

    def test_get_raises_key_error_for_unknown(self):
        with pytest.raises(KeyError):
            SourcePool().get("nonexistent")

    def test_source_ids_empty_initially(self):
        assert SourcePool().source_ids == []

    def test_source_ids_after_inject(self):
        sp = SourcePool()
        sp._drivers["alpha"] = _FakeDriver()
        sp._drivers["beta"] = _FakeDriver()
        assert set(sp.source_ids) == {"alpha", "beta"}

    def test_has_returns_true_after_inject(self):
        sp = SourcePool()
        sp._drivers["src"] = _FakeDriver()
        assert sp.has("src")

    def test_get_returns_injected_driver(self):
        sp = SourcePool()
        drv = _FakeDriver()
        sp._drivers["src"] = drv
        assert sp.get("src") is drv

    @pytest.mark.asyncio
    async def test_close_all_clears_drivers_and_calls_close(self):
        sp = SourcePool()
        drv1, drv2 = _FakeDriver(), _FakeDriver()
        sp._drivers["a"] = drv1
        sp._drivers["b"] = drv2
        await sp.close_all()
        assert sp.source_ids == []
        assert drv1.close_called
        assert drv2.close_called

    @pytest.mark.asyncio
    async def test_close_single_removes_driver_and_calls_close(self):
        sp = SourcePool()
        drv = _FakeDriver()
        sp._drivers["src"] = drv
        await sp.close("src")
        assert not sp.has("src")
        assert drv.close_called

    @pytest.mark.asyncio
    async def test_close_nonexistent_is_noop(self):
        sp = SourcePool()
        drv = _FakeDriver()
        sp._drivers["other"] = drv
        await sp.close("nonexistent")  # must not raise
        assert sp.has("other")
        assert not drv.close_called
        assert sp.source_ids == ["other"]

    @pytest.mark.asyncio
    async def test_close_all_on_empty_pool_is_noop(self):
        sp = SourcePool()
        await sp.close_all()  # must not raise
        assert sp.source_ids == []


class TestSourceDialectMap:
    """REQ-1754: cockroachdb/yugabytedb/greenplum/tidb were missing from _SOURCE_DIALECT, so
    dialect_for() fell back to their own (invalid) name via `.get(source_type, source_type)` —
    none of "cockroachdb"/"yugabytedb"/"greenplum"/"tidb" are real SQLGlot dialects. Each is
    wire-compatible with a base protocol (registry.py's _DRIVER_FACTORIES: the first three reuse
    the postgres driver, tidb reuses mysql) and must resolve to that dialect instead."""

    def test_dialect_for_wire_compatible_types(self):
        sp = SourcePool()
        sp._dialects["cr"] = _SOURCE_DIALECT.get("cockroachdb", "cockroachdb")
        sp._dialects["yb"] = _SOURCE_DIALECT.get("yugabytedb", "yugabytedb")
        sp._dialects["gp"] = _SOURCE_DIALECT.get("greenplum", "greenplum")
        sp._dialects["td"] = _SOURCE_DIALECT.get("tidb", "tidb")
        assert sp.dialect_for("cr") == "postgres"
        assert sp.dialect_for("yb") == "postgres"
        assert sp.dialect_for("gp") == "postgres"
        assert sp.dialect_for("td") == "mysql"

    def test_every_mapped_dialect_is_a_real_sqlglot_dialect(self):
        import sqlglot

        for source_type, dialect in _SOURCE_DIALECT.items():
            try:
                sqlglot.transpile("SELECT 1", write=dialect)
            except Exception as exc:  # pragma: no cover - failure path, asserted below
                pytest.fail(f"{source_type!r} maps to invalid SQLGlot dialect {dialect!r}: {exc}")
