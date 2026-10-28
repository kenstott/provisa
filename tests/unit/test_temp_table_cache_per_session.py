# Copyright (c) 2026 Kenneth Stott
# Canary: 9c2e4a71-5b08-4d3f-a6e9-0f1d7b38c254
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-615, REQ-2176: a cached answer to a read of a session's temporary table is that session's.

Two pgwire sessions each hold their own temporary table of one name; the same opted-in SQL over it
reads each session's own rows, and a cached answer is served only to the session it was read in,
as its tables stood -- never to another session that sends the same words.
"""

# Requirements: REQ-615, REQ-2176

from __future__ import annotations

import pytest

from provisa.compiler import temp_tables
from provisa.compiler.temp_tables import TempSession, TempTable
from provisa.transpiler.router import Route
from tests.unit.test_buffered_auto_threshold import _Source
from tests.unit.test_cache_before_route import pipe  # noqa: F401  (the shared pipeline fixture)

pytestmark = [pytest.mark.asyncio(loop_scope="session")]

_SQL = "-- @provisa cache=true\nSELECT s.id FROM scratch s"


def _session(rows: int) -> TempSession:
    session = TempSession(tables={"scratch": TempTable("scratch", [("id", "integer")])})
    session.extensions["rows"] = [(rows,)]
    return session


class _Store(_Source):
    """The engine reading a session's temporary table: the rows that session's table holds."""

    async def execute_engine(self, sql, params=None, **_kwargs):
        from provisa.executor.result import QueryResult

        self.engine_statements.append(sql)
        held = temp_tables.current()
        assert held is not None
        return QueryResult(rows=held.extensions["rows"], column_names=["id"])


async def _read(p, session: TempSession):
    token = temp_tables.bind(session)
    try:
        plan = await p.mod._govern_and_route(_SQL, "analyst", params=[], serve_cached=True)
        return plan.route, (await p.mod._execute_plan(plan, p.state)).rows
    finally:
        temp_tables.unbind(token)


async def test_the_same_sql_over_each_sessions_own_temp_table_answers_each_its_own(
    pipe,  # noqa: F811
):
    pipe.state.federation_engine = _Store(lambda _sql, _params: [])
    first, second = _session(1), _session(2)
    cached = Route.CACHE
    first_route, rows = await _read(pipe, first)
    assert first_route != cached and rows == [(1,)]
    second_route, rows = await _read(pipe, second)
    assert second_route != cached and rows == [(2,)]
    assert await _read(pipe, first) == (cached, [(1,)])
    assert await _read(pipe, second) == (cached, [(2,)])
    # A changed table is read again, not served the answer to what it held before.
    second.generation += 1
    second.extensions["rows"] = [(3,)]
    changed_route, rows = await _read(pipe, second)
    assert changed_route != cached and rows == [(3,)]
    assert len(pipe.state.federation_engine.engine_statements) == 3
