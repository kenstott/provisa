# Copyright (c) 2026 Kenneth Stott
# Canary: 7d1e5c3a-94b2-4f68-a0c7-2b8e6f19d4a3
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""GET /admin/traces/latest: when the ops traces table last received a row.

Admin > Observability warns when an OTLP endpoint is set but the table is not filling, which it
must read from the table rather than infer from the endpoint's URL (REQ-302, REQ-303, REQ-1349)."""

# Requirements: REQ-302, REQ-303, REQ-1349

from __future__ import annotations

import asyncio
import types
from datetime import datetime, timedelta, timezone

import pytest

import provisa.api.app  # noqa: F401 - imported before any test narrows process state
from provisa.api.admin import settings_router
from provisa.api.errors import ApiError


class _Engine:
    def __init__(self, signals, rows):
        self.landed_signals = frozenset(signals)
        self._rows = rows
        self.sql: list[str] = []

    def execute_engine_sync(self, sql, params=None, *, authorization=None):
        assert authorization is not None
        self.sql.append(sql)
        return types.SimpleNamespace(rows=self._rows)


def _call(monkeypatch, engine, *, gate=lambda request: None):
    monkeypatch.setattr(
        "provisa.api.app.state",
        types.SimpleNamespace(shared_federation_engine=engine),
        raising=False,
    )
    monkeypatch.setattr("provisa.api.admin._platform_guard.require_platform_settings", gate)
    return asyncio.run(settings_router.get_traces_table_latest(types.SimpleNamespace()))


def _utc_naive(delta: timedelta) -> datetime:
    return (datetime.now(timezone.utc) - delta).replace(tzinfo=None)


def test_reports_the_newest_row_and_its_age(monkeypatch):
    newest = _utc_naive(timedelta(seconds=90))
    engine = _Engine({"traces"}, [(newest,)])
    out = _call(monkeypatch, engine)
    assert out["tracked"] is True
    assert out["latest"] == newest.isoformat()
    assert out["age_seconds"] == pytest.approx(90, abs=5)
    assert out["stale_after_seconds"] == settings_router.TRACES_STALE_AFTER_SECONDS
    assert engine.sql == ['SELECT max("timestamp") FROM otel.signals.traces']


def test_an_empty_table_has_no_latest_row(monkeypatch):
    out = _call(monkeypatch, _Engine({"traces"}, [(None,)]))
    assert out["tracked"] is True
    assert out["latest"] is None
    assert out["age_seconds"] is None


def test_an_engine_reading_no_traces_table_is_not_tracked(monkeypatch):
    engine = _Engine(set(), [])
    out = _call(monkeypatch, engine)
    assert out == {
        "tracked": False,
        "stale_after_seconds": settings_router.TRACES_STALE_AFTER_SECONDS,
    }
    assert engine.sql == []


def test_the_read_is_gated_on_platform_settings(monkeypatch):
    def deny(request):
        raise ApiError(403, "platform.settings_capability_required", "denied")

    engine = _Engine({"traces"}, [(None,)])
    with pytest.raises(ApiError):
        _call(monkeypatch, engine, gate=deny)
    assert engine.sql == []
