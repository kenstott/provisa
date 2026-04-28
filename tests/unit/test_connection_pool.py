# Copyright (c) 2026 Kenneth Stott
# Canary: a1b2c3d4-e5f6-7890-abcd-ef1234567890
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

from provisa.executor.pool import SourcePool


class _FakeDriver:
    """Stand-in for DirectDriver — no network I/O."""

    def __init__(self) -> None:
        self.close_called = False
        self.is_connected = True

    async def close(self) -> None:
        self.close_called = True
        self.is_connected = False


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
        await sp.close("nonexistent")  # must not raise

    @pytest.mark.asyncio
    async def test_close_all_on_empty_pool_is_noop(self):
        sp = SourcePool()
        await sp.close_all()  # must not raise
        assert sp.source_ids == []
