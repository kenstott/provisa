# Copyright (c) 2026 Kenneth Stott
# Canary: 8c4e1a7d-2b9f-4e36-a5d0-7f3b6c1e9a24
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""MV refresh into an embedded DuckDB-file store (REQ-1901): the engine computes the fresh rows, the
store broker writes them — full rebuild, DELETE+INSERT refresh, shape rebuild, bitemporal snapshot
and delta appends, and reclaim — and the engine connection never ATTACHes the store file."""

# Requirements: REQ-1901, REQ-1162, REQ-135, REQ-234

from __future__ import annotations

import asyncio

import pytest

from provisa.federation.duckdb_runtime import DuckDBFederationRuntime
from provisa.mv.bitemporal import MODE_DELTA, BitemporalSpec
from provisa.mv.models import MVDefinition
from provisa.mv.refresh import apply_bitemporal_append, reclaim_removed_mvs, refresh_mv
from provisa.mv.registry import MVRegistry


class _Engine:
    """The engine-terminal surface refresh uses, over a real DuckDB runtime."""

    dialect = "duckdb"

    def __init__(self, rt: DuckDBFederationRuntime) -> None:
        self.rt = rt

    async def execute_engine(self, sql, params=None, *, authorization=None):
        del authorization
        return await self.rt.run(sql, params)

    def execute_engine_stream(self, sql, params=None, *, authorization=None):
        from provisa.federation.execution_auth import verify_execution_authorization

        assert authorization is not None  # the refresh stream carries its SystemAuth (REQ-1760)
        verify_execution_authorization(authorization, sql)
        return self.rt.run_arrow_stream(sql, params)

    def mv_store_broker(self):
        return self.rt.mv_store_broker()


@pytest.fixture
def engine(tmp_path):
    rt = DuckDBFederationRuntime(materialize_dsn=f"duckdb:///{tmp_path / 'mat.duckdb'}")
    rt.connection.execute("CREATE TABLE src (id INTEGER, amount INTEGER)")
    rt.connection.execute("INSERT INTO src VALUES (1, 10), (2, 20)")
    return _Engine(rt)


def _mv(sql: str = "SELECT id, amount FROM src", **kw) -> MVDefinition:
    return MVDefinition(
        id="mv_x",
        source_tables=[],
        target_catalog="mat_store",
        target_schema="org_x",
        target_table="mv_x",
        sql=sql,
        consistency="distributed",
        **kw,
    )


def _store_rows(engine, sql: str) -> list[tuple]:
    return engine.rt.mv_store_broker().execute(sql)


def _attached(engine) -> set[str]:
    return {
        r[0]
        for r in engine.rt.connection.execute(
            "SELECT database_name FROM duckdb_databases()"
        ).fetchall()
    }


def _refresh(engine, mv) -> MVRegistry:
    reg = MVRegistry()
    reg.register(mv)
    asyncio.run(refresh_mv(engine, mv, reg))
    assert reg.get(mv.id).last_error is None, reg.get(mv.id).last_error
    return reg


def test_first_refresh_creates_the_store_table_through_the_broker(engine):
    reg = _refresh(engine, _mv())
    assert _store_rows(engine, 'SELECT * FROM mat_store."org_x"."mv_x" ORDER BY id') == [
        (1, 10),
        (2, 20),
    ]
    assert reg.get("mv_x").row_count == 2
    assert "mat_store" not in _attached(engine)


def test_a_later_refresh_replaces_the_rows(engine):
    mv = _mv()
    _refresh(engine, mv)
    engine.rt.connection.execute("DELETE FROM src WHERE id = 1")
    engine.rt.connection.execute("INSERT INTO src VALUES (3, 30)")
    reg = _refresh(engine, mv)
    assert _store_rows(engine, 'SELECT * FROM mat_store."org_x"."mv_x" ORDER BY id') == [
        (2, 20),
        (3, 30),
    ]
    assert reg.get("mv_x").row_count == 2
    assert "mat_store" not in _attached(engine)


def test_a_shape_change_rebuilds_the_store_table(engine):
    _refresh(engine, _mv())
    _refresh(engine, _mv("SELECT id, amount, amount * 2 AS doubled FROM src"))
    cols = engine.rt.mv_store_broker().table_columns("org_x", "mv_x")
    assert cols == ["id", "amount", "doubled"]


def test_a_refreshed_mv_reads_back_through_the_engine(engine):
    _refresh(engine, _mv())
    res = asyncio.run(engine.rt.run('SELECT count(*) FROM mat_store."org_x"."mv_x"'))
    assert res.rows == [(2,)]


def test_bitemporal_snapshot_appends_never_rewrite_history(engine):
    mv = _mv(bitemporal=BitemporalSpec(key=("id",)))
    asyncio.run(apply_bitemporal_append(engine, mv, system_ts="TIMESTAMP '2026-01-01 00:00:00'"))
    engine.rt.connection.execute("UPDATE src SET amount = 11 WHERE id = 1")
    asyncio.run(apply_bitemporal_append(engine, mv, system_ts="TIMESTAMP '2026-02-01 00:00:00'"))
    rows = _store_rows(
        engine,
        'SELECT id, amount, sys_recorded_at FROM mat_store."org_x"."mv_x" '
        "ORDER BY sys_recorded_at, id",
    )
    assert [(r[0], r[1]) for r in rows] == [(1, 10), (2, 20), (1, 11), (2, 20)]
    assert "mat_store" not in _attached(engine)


def test_bitemporal_delta_appends_upserts_and_tombstones(engine):
    mv = _mv(bitemporal=BitemporalSpec(key=("id",), mode=MODE_DELTA))
    asyncio.run(apply_bitemporal_append(engine, mv, system_ts="TIMESTAMP '2026-01-01 00:00:00'"))
    engine.rt.connection.execute("UPDATE src SET amount = 11 WHERE id = 1")
    engine.rt.connection.execute("DELETE FROM src WHERE id = 2")
    asyncio.run(apply_bitemporal_append(engine, mv, system_ts="TIMESTAMP '2026-02-01 00:00:00'"))
    second = _store_rows(
        engine,
        'SELECT id, amount, sys_op FROM mat_store."org_x"."mv_x" '
        "WHERE sys_recorded_at = TIMESTAMP '2026-02-01 00:00:00' ORDER BY id",
    )
    assert second == [(1, 11, "upsert"), (2, None, "delete")]


def test_bitemporal_refresh_through_refresh_mv(engine):
    mv = _mv(bitemporal=BitemporalSpec(key=("id",)))
    _refresh(engine, mv)
    _refresh(engine, mv)
    assert _store_rows(engine, 'SELECT count(*) FROM mat_store."org_x"."mv_x"') == [(4,)]


def test_reclaim_drops_the_store_table_through_the_broker(engine):
    mv = _mv()
    reg = _refresh(engine, mv)
    asyncio.run(reclaim_removed_mvs(engine, reg, set()))
    assert engine.rt.mv_store_broker().table_columns("org_x", "mv_x") is None
