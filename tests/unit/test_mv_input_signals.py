# Copyright (c) 2026 Kenneth Stott
# Canary: 7c1f9a4e-3b26-4d80-9e51-2a6c8d0f4b73
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-862: per-source input-version signal gathering for MV refresh."""

from __future__ import annotations

from provisa.executor.result import QueryResult
from provisa.lineage import resolve_input_version
from provisa.mv.input_signals import gather_input_signals

_WATERMARK_SQL_MARK = "registered_tables"


class _FakeEngine:
    """Engine terminal that answers input-signal probes (watermark registry, Iceberg
    $snapshots, MAX(watermark)) with configured data."""

    def __init__(self, *, iceberg=None, watermark_registry=None, watermark_values=None):
        self.iceberg = iceberg or {}
        self.watermark_registry = watermark_registry or {}
        self.watermark_values = watermark_values or {}
        self.queries: list[str] = []

    async def execute_engine(self, sql, *a, **k):
        self.queries.append(sql)
        if _WATERMARK_SQL_MARK in sql:
            return QueryResult(rows=list(self.watermark_registry.items()), column_names=[])
        if "$snapshots" in sql:
            base = sql.split('"')[1].replace("$snapshots", "")
            if base not in self.iceberg:
                raise RuntimeError(f"table {base}$snapshots does not exist")
            return QueryResult(rows=[(self.iceberg[base],)], column_names=[])
        if sql.startswith("SELECT MAX("):
            base = sql.split('FROM "')[1].rstrip('"')
            return QueryResult(rows=[(self.watermark_values.get(base),)], column_names=[])
        raise AssertionError(f"unexpected SQL: {sql}")


async def test_iceberg_snapshot_is_strongest_signal():
    engine = _FakeEngine(iceberg={"orders": 5551212})
    signals = await gather_input_signals(engine, ["orders"])
    assert [(s.value, s.kind) for s in signals] == [("5551212", "iceberg_snapshot")]
    # Resolver keeps the iceberg signal over the refresh epoch.
    assert resolve_input_version(signals, "1000").kind == "iceberg_snapshot"


async def test_watermark_used_when_not_iceberg():
    engine = _FakeEngine(
        watermark_registry={"events": "updated_at"},
        watermark_values={"events": "2026-07-04T00:00:00"},
    )
    signals = await gather_input_signals(engine, ["events"])
    assert [(s.value, s.kind) for s in signals] == [("2026-07-04T00:00:00", "watermark")]


async def test_iceberg_preferred_over_watermark_for_same_table():
    engine = _FakeEngine(
        iceberg={"orders": 42},
        watermark_registry={"orders": "updated_at"},
        watermark_values={"orders": "ignored"},
    )
    signals = await gather_input_signals(engine, ["orders"])
    assert [s.kind for s in signals] == ["iceberg_snapshot"]


async def test_plain_source_contributes_nothing_and_falls_back_to_epoch():
    engine = _FakeEngine()  # no iceberg, no watermark
    signals = await gather_input_signals(engine, ["legacy_rows"])
    assert signals == []
    assert resolve_input_version(signals, "epoch-123").kind == "refresh_epoch"
    assert resolve_input_version(signals, "epoch-123").value == "epoch-123"


async def test_mixed_sources_gather_independently():
    engine = _FakeEngine(
        iceberg={"orders": 7},
        watermark_registry={"events": "ts"},
        watermark_values={"events": "99"},
    )
    signals = await gather_input_signals(engine, ["orders", "events", "plain"])
    kinds = sorted(s.kind for s in signals)
    assert kinds == ["iceberg_snapshot", "watermark"]


async def test_registry_lookup_failure_is_non_fatal():
    class _BrokenRegistryEngine(_FakeEngine):
        async def execute_engine(self, sql, *a, **k):
            if _WATERMARK_SQL_MARK in sql:
                raise RuntimeError("provisa_admin catalog unavailable")
            return await super().execute_engine(sql, *a, **k)

    engine = _BrokenRegistryEngine(iceberg={"orders": 3})
    # Iceberg still gathered even though the watermark registry lookup blew up.
    signals = await gather_input_signals(engine, ["orders", "events"])
    assert [s.kind for s in signals] == ["iceberg_snapshot"]


async def test_null_snapshot_value_is_skipped():
    # $snapshots exists but yields a NULL id (fresh/empty table) → no signal, epoch used.
    engine = _FakeEngine(iceberg={"orders": None})
    signals = await gather_input_signals(engine, ["orders"])
    assert signals == []
    assert resolve_input_version(signals, "epoch-9").kind == "refresh_epoch"
