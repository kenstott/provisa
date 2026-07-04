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

from provisa.lineage import resolve_input_version
from provisa.mv.input_signals import gather_input_signals

_WATERMARK_SQL_MARK = "registered_tables"


class _FakeCursor:
    def __init__(self, conn):
        self._conn = conn
        self._result = None

    def execute(self, sql):
        self._conn.queries.append(sql)
        # Watermark-column registry lookup.
        if _WATERMARK_SQL_MARK in sql:
            self._result = ("rows", list(self._conn.watermark_registry.items()))
            return
        # Iceberg snapshot metadata. A missing $snapshots table raises (non-Iceberg);
        # a present table may still return a NULL id (fresh/empty table).
        if "$snapshots" in sql:
            base = sql.split('"')[1].replace("$snapshots", "")
            if base not in self._conn.iceberg:
                raise RuntimeError(f"table {base}$snapshots does not exist")
            self._result = ("one", (self._conn.iceberg[base],))
            return
        # MAX(watermark) probe.
        if sql.startswith("SELECT MAX("):
            base = sql.split('FROM "')[1].rstrip('"')
            val = self._conn.watermark_values.get(base)
            self._result = ("one", (val,))
            return
        raise AssertionError(f"unexpected SQL: {sql}")

    def fetchone(self):
        assert self._result and self._result[0] == "one"
        return self._result[1]

    def fetchall(self):
        assert self._result and self._result[0] == "rows"
        return self._result[1]


class _FakeConn:
    def __init__(self, *, iceberg=None, watermark_registry=None, watermark_values=None):
        self.iceberg = iceberg or {}
        self.watermark_registry = watermark_registry or {}
        self.watermark_values = watermark_values or {}
        self.queries: list[str] = []

    def cursor(self):
        return _FakeCursor(self)


def test_iceberg_snapshot_is_strongest_signal():
    conn = _FakeConn(iceberg={"orders": 5551212})
    signals = gather_input_signals(conn, ["orders"])
    assert [(s.value, s.kind) for s in signals] == [("5551212", "iceberg_snapshot")]
    # Resolver keeps the iceberg signal over the refresh epoch.
    assert resolve_input_version(signals, "1000").kind == "iceberg_snapshot"


def test_watermark_used_when_not_iceberg():
    conn = _FakeConn(
        watermark_registry={"events": "updated_at"},
        watermark_values={"events": "2026-07-04T00:00:00"},
    )
    signals = gather_input_signals(conn, ["events"])
    assert [(s.value, s.kind) for s in signals] == [("2026-07-04T00:00:00", "watermark")]


def test_iceberg_preferred_over_watermark_for_same_table():
    conn = _FakeConn(
        iceberg={"orders": 42},
        watermark_registry={"orders": "updated_at"},
        watermark_values={"orders": "ignored"},
    )
    signals = gather_input_signals(conn, ["orders"])
    assert [s.kind for s in signals] == ["iceberg_snapshot"]


def test_plain_source_contributes_nothing_and_falls_back_to_epoch():
    conn = _FakeConn()  # no iceberg, no watermark
    signals = gather_input_signals(conn, ["legacy_rows"])
    assert signals == []
    assert resolve_input_version(signals, "epoch-123").kind == "refresh_epoch"
    assert resolve_input_version(signals, "epoch-123").value == "epoch-123"


def test_mixed_sources_gather_independently():
    conn = _FakeConn(
        iceberg={"orders": 7},
        watermark_registry={"events": "ts"},
        watermark_values={"events": "99"},
    )
    signals = gather_input_signals(conn, ["orders", "events", "plain"])
    kinds = sorted(s.kind for s in signals)
    assert kinds == ["iceberg_snapshot", "watermark"]


def test_registry_lookup_failure_is_non_fatal():
    class _BrokenRegistryConn(_FakeConn):
        def cursor(self):
            cur = super().cursor()
            orig = cur.execute

            def execute(sql):
                if _WATERMARK_SQL_MARK in sql:
                    raise RuntimeError("provisa_admin catalog unavailable")
                return orig(sql)

            cur.execute = execute
            return cur

    conn = _BrokenRegistryConn(iceberg={"orders": 3})
    # Iceberg still gathered even though the watermark registry lookup blew up.
    signals = gather_input_signals(conn, ["orders", "events"])
    assert [s.kind for s in signals] == ["iceberg_snapshot"]


def test_null_snapshot_value_is_skipped():
    # $snapshots exists but yields a NULL id (fresh/empty table) → no signal, epoch used.
    conn = _FakeConn(iceberg={"orders": None})
    signals = gather_input_signals(conn, ["orders"])
    assert signals == []
    assert resolve_input_version(signals, "epoch-9").kind == "refresh_epoch"
