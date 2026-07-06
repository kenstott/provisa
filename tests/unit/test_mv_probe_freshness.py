# Copyright (c) 2026 Kenneth Stott
# Canary: 6c1f9a40-3b28-4d75-8e02-1a7c0d4f9b62
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-881: MV refresh probe-freshness gate (skip rebuild when sources unchanged)."""

from __future__ import annotations

import pytest

from provisa.lineage import InputVersion
from provisa.mv.input_signals import input_token
from provisa.mv.models import MVDefinition, MVStatus
from provisa.mv.refresh import refresh_mv
from provisa.mv.registry import MVRegistry


# ---- input_token ------------------------------------------------------------


def test_input_token_requires_all_sources():
    sigs = [InputVersion("100", "iceberg_snapshot")]
    assert input_token(sigs, ["a"]) == "iceberg_snapshot:100"
    assert input_token(sigs, ["a", "b"]) is None  # partial → None (degrade to TTL)
    assert input_token([], []) is None


def test_input_token_is_order_independent():
    a = [InputVersion("2", "watermark"), InputVersion("1", "iceberg_snapshot")]
    b = [InputVersion("1", "iceberg_snapshot"), InputVersion("2", "watermark")]
    assert input_token(a, ["x", "y"]) == input_token(b, ["x", "y"])


# ---- registry gate ----------------------------------------------------------


def _mv(mv_id, **kw):
    return MVDefinition(
        id=mv_id,
        source_tables=kw.pop("source_tables", ["orders"]),
        target_catalog="pg",
        target_schema="public",
        sql="SELECT 1",
        **kw,
    )


def test_probe_mode_is_always_due_ignoring_ttl():
    reg = MVRegistry()
    mv = _mv("p", freshness_mode="probe", refresh_interval=99999)
    reg.register(mv)
    reg.mark_refreshed("p", 5)  # just refreshed — TTL not elapsed
    assert any(m.id == "p" for m in reg.get_due_for_refresh())


def test_ttl_probe_still_respects_ttl_floor():
    reg = MVRegistry()
    mv = _mv("t", freshness_mode="ttl_probe", refresh_interval=99999)
    reg.register(mv)
    reg.mark_refreshed("t", 5)
    assert not any(m.id == "t" for m in reg.get_due_for_refresh())  # within TTL → not due


def test_mark_unchanged_resets_ttl_keeps_rows():
    reg = MVRegistry()
    mv = _mv("m", freshness_mode="probe")
    reg.register(mv)
    reg.mark_refreshed("m", 42)
    mv.last_input_token = "iceberg_snapshot:7"
    reg.mark_unchanged("m")
    assert mv.status == MVStatus.FRESH
    assert mv.row_count == 42  # rows preserved
    assert mv.last_input_token == "iceberg_snapshot:7"  # token preserved


# ---- refresh_mv gate (fake Trino) -------------------------------------------

_WM_MARK = "registered_tables"


class _FakeEngine:
    def __init__(self, snapshot):
        self.snapshot = snapshot
        self.queries: list[str] = []

    async def execute_engine(self, sql, *a, **k):
        from provisa.executor.result import QueryResult

        self.queries.append(sql)
        if _WM_MARK in sql:
            return QueryResult(rows=[], column_names=[])  # no watermark columns
        if "$snapshots" in sql:
            return QueryResult(rows=[(self.snapshot,)], column_names=[])
        if sql.startswith("SELECT COUNT(*)"):
            return QueryResult(rows=[(3,)], column_names=[])
        # DDL (DELETE/INSERT/CREATE) and the LIMIT-0 existence probe: no result needed
        return QueryResult(rows=[], column_names=[])


@pytest.mark.asyncio
async def test_refresh_skips_when_token_unchanged():
    reg = MVRegistry()
    mv = _mv("agg", freshness_mode="probe", source_tables=["orders"])
    reg.register(mv)
    reg.mark_refreshed("agg", 10)
    mv.last_input_token = "iceberg_snapshot:555"  # matches the fake snapshot below

    conn = _FakeEngine(snapshot=555)
    await refresh_mv(conn, mv, reg)

    # No rebuild DDL executed — only the gather probe ($snapshots) + watermark lookup ran.
    assert not any("DELETE FROM" in q or "CREATE TABLE" in q for q in conn.queries)
    assert mv.status == MVStatus.FRESH
    assert mv.row_count == 10  # unchanged


@pytest.mark.asyncio
async def test_refresh_rebuilds_and_stores_token_when_changed():
    reg = MVRegistry()
    mv = _mv("agg2", freshness_mode="probe", source_tables=["orders"])
    reg.register(mv)
    reg.mark_refreshed("agg2", 10)
    mv.last_input_token = "iceberg_snapshot:1"  # stale — source is now 999

    conn = _FakeEngine(snapshot=999)
    await refresh_mv(conn, mv, reg)

    # A rebuild happened and the new token was stored.
    assert any("CREATE TABLE" in q or "INSERT INTO" in q for q in conn.queries)
    assert mv.last_input_token == "iceberg_snapshot:999"
