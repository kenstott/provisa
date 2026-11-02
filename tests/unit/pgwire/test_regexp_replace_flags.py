# Copyright (c) 2026 Kenneth Stott
# Canary: 2e7c9b41-6d18-4a3f-b5e0-8f1a3c6d9b27
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""regexp_replace(source, pattern, replacement, start, N, flags) (REQ-2167, REQ-2171), as
PostgreSQL 17's textregexreplace_extended answers it: strict, so a NULL flags is a NULL; start,
then N, checked before the flags; an option PostgreSQL does not know its 22023 error; the 'g' flag
ignored beside N, and c, s and t its defaults. A flag that changes what matches (i in effect,
b, e, m, n, p, q, w, x) is refused by name, as is flags no statement says."""

# Requirements: REQ-2167, REQ-2171

from __future__ import annotations

import chdb
import duckdb
import pytest

from provisa.core.pg_error import from_engine
from provisa.transpiler.function_macros import FunctionRefused
from provisa.transpiler.transpile import transpile

_ENGINES = ("duckdb", "trino", "clickhouse")


def _duckdb(sql: str) -> list[tuple]:
    return duckdb.connect().execute(transpile(sql, "duckdb")).fetchall()


def _clickhouse(sql: str) -> str:
    return chdb.query(transpile(sql, "clickhouse"), "CSV").bytes().decode().strip()


@pytest.mark.parametrize(
    "args",
    [
        "NULL::text, NULL::text, NULL::text, NULL::integer, NULL::integer, NULL::text",
        "'aXbXc'::text, 'X'::text, '-'::text, 1, 2, NULL::text",
        "NULL::text, 'X'::text, '-'::text, 1, 2, 'z'",
    ],
)
def test_a_null_argument_answers_null(args: str):
    sql = f'SELECT pg_catalog."regexp_replace"({args}) AS v'
    assert _duckdb(sql) == [(None,)]
    assert _clickhouse(sql) == "\\N"


@pytest.mark.parametrize("flags", ["c", "s", "t", "g", "ic", "gcst", ""])
@pytest.mark.parametrize(("n", "want"), [(2, "aXb-c"), (0, "a-b-c"), (3, "aXbXc")])
def test_a_flag_of_no_effect_beside_n_answers_as_none(flags: str, n: int, want: str):
    sql = f"SELECT regexp_replace('aXbXc', 'X', '-', 1, {n}, '{flags}') AS v"
    assert _duckdb(sql) == [(want,)]
    assert _clickhouse(sql) == f'"{want}"'


@pytest.mark.parametrize("flags", ["z", "cz", "gZ"])
def test_an_unknown_option_is_postgresqls_error(flags: str):
    bad = next(c for c in flags if c not in "cg")
    sql = f"SELECT regexp_replace('aXbXc', 'X', '-', 1, 1, '{flags}') AS v"
    with pytest.raises(duckdb.Error) as raised:
        _duckdb(sql)
    error = from_engine(raised.value)
    assert error is not None, raised.value
    assert (error.sqlstate, str(error)) == (
        "22023",
        f'invalid regular expression option: "{bad}"',
    )


def test_start_is_checked_before_the_flags():
    sql = "SELECT regexp_replace('aXbXc', 'X', '-', 0, 1, 'z') AS v"
    with pytest.raises(duckdb.Error) as raised:
        _duckdb(sql)
    error = from_engine(raised.value)
    assert error is not None, raised.value
    assert (error.sqlstate, str(error)) == ("22023", 'invalid value for parameter "start": 0')


@pytest.mark.parametrize("flags", ["'i'", "'ci'", "'n'", "'x'", "'q'", "f"])
@pytest.mark.parametrize("dialect", _ENGINES)
def test_a_flag_that_changes_the_match_is_refused_by_name(flags: str, dialect: str):
    sql = f"SELECT regexp_replace(a, 'X', '-', 1, 2, {flags}) AS v FROM t"
    with pytest.raises(FunctionRefused, match=f"regexp_replace.*has no form on {dialect}"):
        transpile(sql, dialect)
