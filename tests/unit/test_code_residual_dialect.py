# Copyright (c) 2026 Kenneth Stott
# Canary: 274d9d5f-2287-4b1f-b9f0-9cc1b0722b86
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A code source's DuckDB answers as PostgreSQL does (REQ-2100, REQ-066): the statement a code
source's DuckDB applies to its functions' rows -- read DIRECT (the DuckDB statement Provisa's
DIRECT terminal writes) or pushed by an engine through the pgwire front end (PostgreSQL text) --
answers what PostgreSQL answers over the same rows, for null ordering (#294), LIKE and ILIKE with
PostgreSQL's backslash escape, and text order. Both go through codehost.duckdb_sql, the one form
every DuckDB answering a statement runs. Text is ordered by code point, as PostgreSQL's C
collation orders it (the database here is made with it)."""

# Requirements: REQ-2100, REQ-066
from __future__ import annotations

from typing import LiteralString, cast

import psycopg
import pyarrow as pa
import pytest

from provisa.codehost import residual
from provisa.transpiler.transpile import transpile

_ROWS = [
    ("a%b", 1),
    ("axb", None),
    ("a_b", 3),
    ("back\\slash", None),
    ("Zeta", 5),
    ("alpha", 6),
    ("Älpha", None),
    ("_under", 8),
    ("A%B", 9),
]


@pytest.fixture(scope="module")
def pg(embedded_postgres):
    with psycopg.connect(embedded_postgres.dsn(), autocommit=True) as conn:
        conn.execute("DROP DATABASE IF EXISTS code_dialect")
        conn.execute(
            "CREATE DATABASE code_dialect TEMPLATE template0 LC_COLLATE 'C' LC_CTYPE 'C' "
            "ENCODING 'UTF8'"
        )
    with psycopg.connect(embedded_postgres.dsn("code_dialect"), autocommit=True) as conn:
        conn.execute("CREATE TABLE t (s text, v int)")
        for row in _ROWS:
            conn.execute("INSERT INTO t VALUES (%s, %s)", row)
    yield embedded_postgres
    with psycopg.connect(embedded_postgres.dsn(), autocommit=True) as conn:
        conn.execute("DROP DATABASE code_dialect WITH (FORCE)")


_ROW_TABLE = pa.table(
    {"s": [r[0] for r in _ROWS], "v": pa.array([r[1] for r in _ROWS], type=pa.int64())}
)

_STATEMENTS = [
    # #294: null ordering, with and without a limit.
    "SELECT v FROM t ORDER BY v",
    "SELECT v FROM t ORDER BY v DESC",
    "SELECT v FROM t ORDER BY v DESC LIMIT 3",
    "SELECT v FROM t ORDER BY v ASC NULLS FIRST LIMIT 3",
    "SELECT v FROM t ORDER BY v DESC NULLS LAST LIMIT 3",
    "SELECT s, v FROM t ORDER BY 2 DESC, 1 LIMIT 4",
    # LIKE and ILIKE: PostgreSQL's backslash escape, and an ESCAPE clause of their own.
    "SELECT s FROM t WHERE s LIKE 'a\\%b' ORDER BY s",
    "SELECT s FROM t WHERE s LIKE 'a\\_b' ORDER BY s",
    "SELECT s FROM t WHERE s LIKE '\\_%' ORDER BY s",
    "SELECT s FROM t WHERE s LIKE 'back\\\\slash' ORDER BY s",
    "SELECT s FROM t WHERE s ILIKE 'a\\%b' ORDER BY s",
    "SELECT s FROM t WHERE NOT s LIKE 'a\\%b' ORDER BY s",
    "SELECT s FROM t WHERE s LIKE 'a!%b' ESCAPE '!' ORDER BY s",
    "SELECT s FROM t WHERE s LIKE 'a%' ORDER BY s",
    # Text order: by code point, as the C collation orders it.
    "SELECT s FROM t ORDER BY s",
    "SELECT s FROM t ORDER BY s DESC LIMIT 4",
    "SELECT min(s), max(s) FROM t",
]


def _postgres(pg, sql: str) -> list[tuple]:
    with psycopg.connect(pg.dsn("code_dialect")) as conn:
        return [tuple(r) for r in conn.execute(cast(LiteralString, sql)).fetchall()]


def _rows(answer: pa.RecordBatchReader) -> list[tuple]:
    return [tuple(r.values()) for r in answer.read_all().to_pylist()]


@pytest.mark.parametrize("sql", _STATEMENTS)
def test_a_direct_read_answers_as_postgresql(pg, sql):
    # The DIRECT terminal writes the governed statement in the source's dialect, DuckDB's.
    direct = transpile(sql, "duckdb")
    assert _rows(residual.answer(direct, "duckdb", "t", _ROW_TABLE)) == _postgres(pg, sql), direct


@pytest.mark.parametrize("sql", _STATEMENTS)
def test_an_engines_pushed_read_answers_as_postgresql(pg, sql):
    # An engine attaching the image sends its part over pgwire, PostgreSQL text.
    assert _rows(residual.answer(sql, "postgres", "t", _ROW_TABLE)) == _postgres(pg, sql)
