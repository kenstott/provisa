# Copyright (c) 2026 Kenneth Stott
# Canary: 3d7e5b21-94c6-4f0a-8b1e-6c2a9f4d7e13
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A NULL result of a typed expression is described with the PostgreSQL type the statement
declares, on an engine that gives its rows no column types (REQ-2167). The URL-driven engines
(mysql, tidb, oracle, mssql) return rows alone; a column whose rows hold NULL alone has no value
to type it by, and was described as text (oid 25): the polyfill triage of 2026-11-22 (group G2)
counted 547 such wrong answers, e.g. ``abs(NULL::bigint)`` on mysql answering oid 25 for 20."""

# Requirements: REQ-2167

from __future__ import annotations

import datetime

from buenavista.core import BVType

from provisa.executor.result import QueryResult, StreamingQueryResult
from provisa.pgwire.server import ProvisaQueryResult

_INT4, _INT8, _FLOAT8, _DATE = 23, 20, 701, 1082


def _oids(sql: str, rows: list[tuple]) -> list[int | None]:
    result = ProvisaQueryResult(QueryResult(rows=rows, column_names=["v"]), sql)
    return [result.column_type_oid(i) for i in range(result.column_count())]


def test_a_null_cast_is_described_as_its_cast():
    assert _oids("SELECT NULL::int4 AS v", [(None,)]) == [_INT4]


def test_a_null_function_result_is_described_as_the_function_returns():
    assert _oids('SELECT pg_catalog."abs"(NULL::bigint) AS v', [(None,)]) == [_INT8]
    assert _oids('SELECT pg_catalog."acos"(NULL::double precision) AS v', [(None,)]) == [_FLOAT8]


def test_a_null_date_function_result_is_described_as_date():
    sql = 'SELECT pg_catalog."date"(NULL::timestamp without time zone) AS v'
    assert _oids(sql, [(None,)]) == [_DATE]


def test_a_streamed_null_column_with_later_values_keeps_them():
    """The first batch holds NULL alone; a later batch's value is the engine's own and is sent."""
    sql = 'SELECT pg_catalog."date"(t.ts) AS v FROM t'
    stream = StreamingQueryResult(
        iter([[(None,)], [(datetime.date(2026, 1, 2),)]]), column_names=["v"]
    )
    result = ProvisaQueryResult(stream, sql)
    assert result.column_type_oid(0) == _DATE
    assert result.coercers() is None


def test_an_untyped_null_column_stays_text():
    """A column the statement declares no type for has none to describe it by."""
    result = ProvisaQueryResult(QueryResult(rows=[(None,)], column_names=["v"]), "SELECT NULL AS v")
    assert result.column(0)[1] is BVType.TEXT
    assert result.column_type_oid(0) is None


def test_a_valued_column_keeps_its_typing():
    assert _oids('SELECT pg_catalog."abs"((-3)::bigint) AS v', [(3,)]) == [_INT8]
