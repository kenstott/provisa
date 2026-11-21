# Copyright (c) 2026 Kenneth Stott
# Canary: 5f0c8e2a-7b41-4d96-a3e8-c21b9f6d04e7
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A Trino statement holding a geometric form defines its routines at its head (REQ-2171): each
step that rewrites the statement, or wraps it in parentheses, keeps that head in front of the
statement Trino reads, for Trino takes no routine definition inside parentheses."""

# Requirements: REQ-2171

from __future__ import annotations

from types import SimpleNamespace

import pytest
import sqlglot

from provisa.mv.bitemporal import MODE_DELTA, BitemporalSpec, append_sql, create_sql
from provisa.mv.refresh import _probe_source_count
from provisa.mv.store_shape import output_columns, shape_probe
from provisa.transpiler import pg_forms_geometric as geometric
from provisa.transpiler.pg_forms_geometric import around, routines_apart
from provisa.transpiler.transpile import transpile, transpile_to_trino

_CLOSE_SB = "SELECT close_sb('[(1,2),(3,4)]'::lseg, '(5,5),(0,0)'::box) AS v"
_SQL = transpile(_CLOSE_SB, "trino")
_HEAD, _STATEMENT = routines_apart(_SQL)


def _kept(written: str, before: str = "") -> str:
    """What follows the head in ``written``, which holds it once, right after ``before``."""
    assert written.startswith(before + _HEAD), written[:300]
    rest = written[len(before + _HEAD) :]
    assert "FUNCTION pg_geo_" not in rest, rest[:300]
    return rest


def test_the_head_is_each_routine_and_the_statement_follows():
    assert _HEAD.startswith("WITH FUNCTION pg_geo_") and _HEAD + _STATEMENT == _SQL
    assert _STATEMENT.startswith("SELECT CASE WHEN ")
    assert "FUNCTION pg_geo_" not in _STATEMENT


def test_the_head_is_found_once_the_statement_is_written_again():
    written = sqlglot.parse_one(_SQL, read="trino").sql(dialect="trino")
    head, statement = routines_apart(written)
    assert head.startswith("WITH FUNCTION") and statement.startswith("SELECT CASE WHEN ")


def test_a_statement_after_the_head_may_have_its_own_with():
    sql = f"{_HEAD}WITH t AS (SELECT 1 AS x) SELECT x FROM t"
    assert routines_apart(sql) == (_HEAD, "WITH t AS (SELECT 1 AS x) SELECT x FROM t")


@pytest.mark.parametrize("sql", ["SELECT 1 AS v", "WITH t AS (SELECT 1 AS x) SELECT x FROM t"])
def test_a_statement_of_no_routine_is_wrapped_as_it_is(sql: str):
    assert routines_apart(sql) == ("", sql)
    assert around(sql, lambda s: f"SELECT * FROM ({s}) w") == f"SELECT * FROM ({sql}) w"


def test_transpile_to_trino_keeps_each_routine_as_written():
    written = transpile_to_trino(_CLOSE_SB)
    _kept(written)
    for name in geometric._CALLED.findall(_HEAD):
        assert geometric.ROUTINES[name.lower()][0] in written, name


def test_the_trino_backend_keeps_the_head(monkeypatch):
    from provisa.federation import trino_connectors
    from provisa.federation.backend import TrinoBackend
    from provisa.federation.engine import build_trino_engine

    monkeypatch.setattr(trino_connectors, "trino_utc_wall_columns", lambda _state: {})
    _kept(TrinoBackend(build_trino_engine()).transpile_physical(_CLOSE_SB))


def test_a_zero_row_probe_keeps_the_head():
    assert _kept(shape_probe(_SQL)) == f"SELECT * FROM ({_STATEMENT}) AS _shape LIMIT 0"


class _Engine:
    """An engine that records what it is asked to run."""

    def __init__(self) -> None:
        self.ran: list[str] = []
        self.engine = SimpleNamespace(dialect="trino")

    async def execute_engine(self, sql: str, authorization=None):
        self.ran.append(sql)
        return SimpleNamespace(rows=[[2]], column_names=["v"], column_types=["varchar"])


@pytest.mark.asyncio
async def test_the_count_probe_keeps_the_head():
    engine = _Engine()
    assert await _probe_source_count(engine, _SQL) == 2
    assert _kept(engine.ran[0]) == f"SELECT COUNT(*) FROM ({_STATEMENT}) _probe"


@pytest.mark.asyncio
async def test_the_store_shape_probe_keeps_the_head():
    engine = _Engine()
    await output_columns(engine, _SQL, authorization=None)
    assert _kept(engine.ran[0]) == f"SELECT * FROM ({_STATEMENT}) AS _mv_probe LIMIT 0"


def test_a_bitemporal_create_keeps_the_head_after_as():
    written = create_sql("mv_t", _SQL, BitemporalSpec(key=("v",)), "TIMESTAMP '2026-01-01'")
    assert f"FROM ({_STATEMENT}) _s" in _kept(written, "CREATE TABLE mv_t AS ")


def test_a_snapshot_append_keeps_the_head_after_its_columns():
    [written] = append_sql("mv_t", _SQL, BitemporalSpec(key=("v",)), ["v"], "now()", "trino")
    assert _STATEMENT in _kept(written, 'INSERT INTO mv_t ("v", "sys_recorded_at") ')


def test_each_delta_append_keeps_the_head_after_its_columns():
    spec = BitemporalSpec(key=("v",), mode=MODE_DELTA)
    upserts, tombstones = append_sql("mv_t", _SQL, spec, ["v"], "now()", "trino")
    assert _STATEMENT in _kept(upserts, 'INSERT INTO mv_t ("v", "sys_recorded_at", "sys_op") ')
    assert _STATEMENT in _kept(tombstones, 'INSERT INTO mv_t ("v", "sys_recorded_at", "sys_op") ')
