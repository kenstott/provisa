# Copyright (c) 2026 Kenneth Stott
# Canary: b22aebf8-ed22-41f4-8db3-98e91b3c7d8e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The synthetic report profiles each generated table whole (REQ-1939, REQ-1934).

``report`` passed ``None`` as the profile statement's sample after the statement took a
``Sample``; building the statement then raised before the generated table was read."""

# Requirements: REQ-1939, REQ-1934

from __future__ import annotations

import contextlib
from types import SimpleNamespace

import provisa.profiler.run as run_mod
import provisa.synthetic.report as report_mod
from provisa.profiler.statement import ColumnSpec
from provisa.synthetic.run import report


class _Conn:
    def __init__(self) -> None:
        self.executed: list = []

    @contextlib.asynccontextmanager
    async def transaction(self):
        yield

    async def execute_core(self, stmt):
        self.executed.append(stmt)


class _Db:
    def __init__(self) -> None:
        self.conn = _Conn()

    @contextlib.asynccontextmanager
    async def acquire(self):
        yield self.conn


async def test_the_report_reads_each_generated_table_whole(monkeypatch):
    statements: list[str] = []
    columns = [ColumnSpec("id", "bigint", "numeric", "id")]

    async def _tags(conn, table_id):
        return {}

    def _target(state, table_id, name, tags):
        return SimpleNamespace(
            pgwire_name="synth.orders", columns=columns, fanouts=[], keys=[], checks=[]
        )

    async def _governed(sql):
        statements.append(sql)
        return ["k"], []

    monkeypatch.setattr(run_mod, "column_tags", _tags)
    monkeypatch.setattr(run_mod, "resolve_target", _target)
    monkeypatch.setattr(run_mod, "_governed", _governed)
    monkeypatch.setattr(report_mod, "compare", lambda p, synthetic, measures: [])
    planned = SimpleNamespace(table=SimpleNamespace(table_id=3, name="orders"))
    await report(SimpleNamespace(model_db=_Db()), "ds1", [planned])
    [sql] = statements
    assert "TABLESAMPLE" not in sql and "RANDOM()" not in sql and "BETWEEN" not in sql
    assert 'FROM "synth"."orders" t' in sql
