# Copyright (c) 2026 Kenneth Stott
# Canary: 5b0e8f2c-7d41-4a9e-b6c3-2f8a1d9e4c70
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""REQ-1911: a worker moves what its boot built to the permanent generation once, when it is
ready, so a full collection stops walking it; a request never freezes."""

from __future__ import annotations

import gc
import logging

import pytest
from httpx import ASGITransport, AsyncClient

from provisa.api import app_startup
from provisa.api.admin import schema as admin_schema_mod
from provisa.api.app import create_app, state


class _NoEngine:
    def is_connected(self) -> bool:
        return False


class _Result:
    errors = None


@pytest.fixture
def app():
    return create_app()


@pytest.fixture
def warmup(app, monkeypatch):
    monkeypatch.setattr(state, "federation_engine", _NoEngine(), raising=False)
    monkeypatch.setattr(state, "is_warm", False, raising=False)

    async def _execute(_query, context_value):
        return _Result()

    monkeypatch.setattr(admin_schema_mod.admin_schema, "execute", _execute)
    gc.unfreeze()
    yield
    gc.unfreeze()


@pytest.mark.asyncio
async def test_the_worker_freezes_its_heap_once_when_ready(app, warmup, monkeypatch) -> None:
    assert gc.get_freeze_count() == 0
    await app_startup._warmup_readiness(logging.getLogger("test"))
    assert state.is_warm
    assert gc.get_freeze_count() > 0

    calls: list[None] = []
    monkeypatch.setattr(gc, "freeze", lambda: calls.append(None))
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        for _ in range(3):
            assert (await c.get("/live")).status_code == 200
    assert calls == []
