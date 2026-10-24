# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration (REQ-066): a LIKE Trino runs over a PostgreSQL catalog matches what it matches in
PostgreSQL, the dialect every statement is written in. PostgreSQL's LIKE takes a backslash as its
escape character unless an ESCAPE clause names another; Trino's has none unless one is written,
so Provisa writes PostgreSQL's for Trino (provisa.transpiler.transpile). Each condition is
answered by the test stack's PostgreSQL itself and by its Trino over a catalog of that database."""

# Requirements: REQ-066

from __future__ import annotations

import os
from typing import LiteralString, cast

import psycopg
import pytest

from provisa.transpiler.transpile import transpile

pytestmark = [pytest.mark.integration, pytest.mark.requires_trino]

_SCHEMA = "like_escape"
_CATALOG = "like_escape"
_VALUES = ["a%b", "axb", "a_b", "back\\slash", "backslash", "A%B", "r!x", "r%x"]
_CONDITIONS = [
    "v LIKE 'a\\%b'",
    "v LIKE 'a\\_b'",
    "v LIKE 'back\\\\slash'",
    "v LIKE 'a%b'",
    "v ILIKE 'a\\%b'",
    "NOT v LIKE 'a\\%b'",
    "v LIKE 'r!%x' ESCAPE '!'",
]


def _pg_dsn() -> str:
    return (
        f"postgresql://{os.environ.get('PG_USER', 'provisa')}:"
        f"{os.environ.get('PG_PASSWORD', 'provisa')}@{os.environ.get('PG_HOST', 'localhost')}:"
        f"{os.environ.get('PG_PORT', '5432')}/{os.environ.get('PG_DATABASE', 'provisa')}"
    )


@pytest.fixture(scope="module")
def trino_over_postgres():
    import trino.dbapi

    with psycopg.connect(_pg_dsn(), autocommit=True) as conn:
        conn.execute(f"DROP SCHEMA IF EXISTS {_SCHEMA} CASCADE")
        conn.execute(f"CREATE SCHEMA {_SCHEMA}")
        conn.execute(f"CREATE TABLE {_SCHEMA}.t (v text)")
        for v in _VALUES:
            conn.execute(f"INSERT INTO {_SCHEMA}.t VALUES (%s)", [v])
    # The test stack's own coordinator, which reaches the stack's PostgreSQL by its service name.
    tr = trino.dbapi.connect(
        host=os.environ.get("TRINO_HOST", "localhost"),
        port=int(os.environ["TRINO_PORT"]),
        user="itest",
    )
    cur = tr.cursor()
    cur.execute(f"DROP CATALOG IF EXISTS {_CATALOG}")
    cur.fetchall()
    cur.execute(
        f"CREATE CATALOG {_CATALOG} USING postgresql WITH ("
        "\"connection-url\" = 'jdbc:postgresql://postgres:5432/provisa', "
        "\"connection-user\" = 'provisa', \"connection-password\" = 'provisa')"
    )
    cur.fetchall()
    try:
        yield cur
    finally:
        cur.execute(f"DROP CATALOG IF EXISTS {_CATALOG}")
        cur.fetchall()
        tr.close()
        with psycopg.connect(_pg_dsn(), autocommit=True) as conn:
            conn.execute(f"DROP SCHEMA IF EXISTS {_SCHEMA} CASCADE")


@pytest.mark.parametrize("condition", _CONDITIONS)
def test_a_like_matches_on_trino_as_on_postgresql(trino_over_postgres, condition):
    sql = f'SELECT "v" FROM "{_SCHEMA}"."t" WHERE {condition}'
    with psycopg.connect(_pg_dsn()) as conn:
        expected = sorted(r[0] for r in conn.execute(cast(LiteralString, sql)).fetchall())
    lowered = transpile(f'SELECT "v" FROM "{_CATALOG}"."{_SCHEMA}"."t" WHERE {condition}', "trino")
    trino_over_postgres.execute(lowered)
    assert sorted(r[0] for r in trino_over_postgres.fetchall()) == expected, lowered
