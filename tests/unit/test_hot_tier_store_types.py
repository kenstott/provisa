# Copyright (c) 2026 Kenneth Stott
# Canary: 7b2d4e91-3c5a-4f68-9d17-8a0e6c1b5f43
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Every way the hot manager stores a table keeps the registered column types (REQ-232): a jsonb
or timestamptz column loaded at startup, on a refresh, on a promotion or from a caller's rows is
typed in the entry, so a VALUES CTE casts it and ``@>`` works on it."""

# Requirements: REQ-232

from __future__ import annotations

import pytest

from provisa.cache import hot_tables
from provisa.cache.hot_tables import HotTableCandidate, HotTableManager

TABLE_ID = 21
REGISTERED = {
    "id": TABLE_ID,
    "source_id": "pg",
    "schema_name": "public",
    "table_name": "events",
    "columns": [
        {"column_name": "id", "data_type": "integer"},
        {"column_name": "payload", "data_type": "jsonb"},
        {"column_name": "seenAt", "data_type": "timestamptz"},
    ],
}
TYPES = {
    "id": "integer",
    "payload": "jsonb",
    "seenAt": "timestamptz",
    "seen_at": "timestamptz",
}
COLUMNS = ["id", "payload", "seen_at"]
ROWS = [(1, '{"a": 1}', "2026-01-05T00:00:00+00:00")]


class _Engine:
    def engine_physical(self, sql: str) -> str:
        return sql

    async def execute_engine(self, sql: str, *args, **kwargs):
        from provisa.executor.result import QueryResult

        return QueryResult(rows=ROWS, column_names=COLUMNS)


@pytest.fixture
def acting(monkeypatch):
    monkeypatch.setattr(hot_tables, "_scope_parts", lambda: ("acme", 7))


@pytest.fixture
async def manager(acting):
    mgr = HotTableManager(redis_url=None, auto_threshold=100, max_rows=1000)
    await mgr._connect()
    await mgr._redis.flushall()
    yield mgr
    await mgr._redis.flushall()
    await mgr.close()


def _assert_typed(mgr: HotTableManager) -> None:
    entry = mgr.get_entry(TABLE_ID)
    assert entry is not None
    assert entry.column_types["payload"] == "jsonb"
    assert entry.column_types["seen_at"] == "timestamptz"


def _candidate() -> HotTableCandidate:
    return HotTableCandidate(TABLE_ID, "events", "id", "pg", "public", TYPES)


async def test_startup_load_keeps_registered_types(monkeypatch, acting):
    from provisa.core import settings_registry

    values = {
        "cache.enabled": True,
        "hot_tables.auto_threshold": 100,
        "hot_tables.max_bytes": 10_000_000,
        "hot_tables.max_rows": 1000,
        "hot_tables.refresh_interval": 300,
    }
    monkeypatch.setattr(settings_registry, "value", lambda key: values[key])
    monkeypatch.setattr("provisa.core.redis_location.redis_url", lambda: None)
    raw = {
        "sources": [{"id": "pg", "type": "postgresql"}],
        "tables": [{"source_id": "pg", "schema": "public", "table": "events", "hot": True}],
    }
    mgr = await hot_tables.init_hot_tables(raw, _Engine(), [REGISTERED], {}, {})
    try:
        _assert_typed(mgr)
        (candidate,) = mgr.snapshot()
        assert candidate["table_id"] == TABLE_ID
    finally:
        await mgr.close()


async def test_load_table_keeps_the_types_it_is_given(manager):
    await manager.load_table(_Engine(), TABLE_ID, "events", "public", "pg", "id", TYPES)
    _assert_typed(manager)


async def test_refresh_after_write_keeps_the_entry_types(manager):
    await manager.load_table(_Engine(), TABLE_ID, "events", "public", "pg", "id", TYPES)
    await manager.refresh_after_write(_Engine(), TABLE_ID)
    _assert_typed(manager)


async def test_promote_on_read_keeps_the_candidate_types(manager, monkeypatch):
    async def count(*_a, **_k):
        return 1

    monkeypatch.setattr(hot_tables, "count_table_rows", count)
    manager.register_candidate(_candidate())
    await manager.promote_on_read(_Engine(), TABLE_ID)
    _assert_typed(manager)


async def test_maybe_promote_dicts_keeps_the_candidate_types(manager):
    manager.register_candidate(_candidate())
    await manager.maybe_promote_dicts(
        TABLE_ID, [{"id": 1, "payload": '{"a": 1}', "seen_at": "2026-01-05T00:00:00+00:00"}]
    )
    _assert_typed(manager)


async def test_sqlite_load_keeps_the_types_it_is_given(manager, monkeypatch):
    monkeypatch.setattr(
        "provisa.file_source.source.execute_query",
        lambda cfg, sql: [{"id": 1, "payload": '{"a": 1}', "seen_at": "2026-01-05"}],
    )
    await manager.load_table_from_sqlite(
        {"id": "pg", "path": "x.db"}, TABLE_ID, "events", "id", TYPES
    )
    _assert_typed(manager)
