# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A DuckDB statement's LIKE matches what the PostgreSQL statement it is written from matches
(REQ-066): in PostgreSQL a backslash escapes the pattern character after it unless an ESCAPE
clause names another; DuckDB's LIKE has no escape character unless one is written, so
``'a%b' LIKE 'a\\%b'`` was false there. Each pattern is answered by PostgreSQL itself and by
DuckDB over the attached table."""

# Requirements: REQ-066

from typing import LiteralString, cast

import duckdb
import psycopg
import pytest

from provisa.transpiler.transpile import transpile_to_duckdb

_VALUES = ["a%b", "axb", "a_b", "back\\slash", "backslash", "A%B", "r!x", "r%x"]


@pytest.fixture(scope="module")
def attached(embedded_postgres):
    pg = embedded_postgres
    with psycopg.connect(pg.dsn(), autocommit=True) as conn:
        conn.execute("DROP DATABASE IF EXISTS like_escape")
        conn.execute("CREATE DATABASE like_escape")
    with psycopg.connect(pg.dsn("like_escape"), autocommit=True) as conn:
        conn.execute("CREATE TABLE t (v text)")
        for v in _VALUES:
            conn.execute("INSERT INTO t VALUES (%s)", [v])
    duck = duckdb.connect()
    duck.execute(f"ATTACH '{pg.dsn('like_escape')}' AS p (TYPE POSTGRES, READ_ONLY)")
    yield pg, duck
    duck.close()
    with psycopg.connect(pg.dsn(), autocommit=True) as conn:
        conn.execute("DROP DATABASE like_escape WITH (FORCE)")


@pytest.mark.parametrize(
    "condition",
    [
        "v LIKE 'a\\%b'",
        "v LIKE 'a\\_b'",
        "v LIKE 'back\\\\slash'",
        "v LIKE 'a%b'",
        "v ILIKE 'a\\%b'",
        "NOT v LIKE 'a\\%b'",
        "v LIKE 'r!%x' ESCAPE '!'",
    ],
)
def test_a_like_matches_as_in_postgresql(attached, condition):
    pg, duck = attached
    sql = f"SELECT v FROM t WHERE {condition}"
    with psycopg.connect(pg.dsn("like_escape")) as conn:
        expected = sorted(r[0] for r in conn.execute(cast(LiteralString, sql)).fetchall())
    duck_sql = transpile_to_duckdb(f"SELECT v FROM p.public.t WHERE {condition}")
    assert sorted(r[0] for r in duck.execute(duck_sql).fetchall()) == expected, duck_sql
