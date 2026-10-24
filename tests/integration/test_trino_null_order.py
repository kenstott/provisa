# Copyright (c) 2026 Kenneth Stott
# Canary: 22300adb-f47d-4e79-a62a-fedcb3cae7ac
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration (#294, REQ-066): a statement Trino runs over a PostgreSQL catalog orders and limits
as PostgreSQL does. Provisa writes a DESC for Trino as DESC NULLS FIRST (PostgreSQL's null order),
and Trino's postgresql connector pushes an ORDER BY ... LIMIT down to the source as a TopN; DuckDB's
postgres scanner lost rows on the same shape. Each case is answered by the test stack's PostgreSQL
itself and by its Trino over a catalog of that database, the statement lowered as Provisa lowers
one for Trino (provisa.transpiler.transpile)."""

# Requirements: REQ-066

from __future__ import annotations

import os
from typing import LiteralString, cast

import psycopg
import pytest

from provisa.transpiler.transpile import transpile

pytestmark = [pytest.mark.integration, pytest.mark.requires_trino]

_SCHEMA = "null_order_294"
_CATALOG = "null_order_294"
_ORDERS = [
    "DESC",
    "DESC NULLS FIRST",
    "DESC NULLS LAST",
    "ASC",
    "ASC NULLS FIRST",
    "ASC NULLS LAST",
]
_LIMITS = ["LIMIT 1", "LIMIT 2", "OFFSET 1 LIMIT 2", ""]


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
        conn.execute(f"CREATE TABLE {_SCHEMA}.t (d date, v int)")
        conn.execute(f"CREATE TABLE {_SCHEMA}.no_nulls (d date, v int)")
        conn.execute(
            f"INSERT INTO {_SCHEMA}.t VALUES "
            "('2026-09-30', 1), ('2026-09-01', 2), (NULL, 3), ('2026-09-15', 4)"
        )
        conn.execute(f"INSERT INTO {_SCHEMA}.no_nulls VALUES ('2026-09-30', 1), ('2026-09-01', 2)")
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


@pytest.mark.parametrize("table", ["t", "no_nulls"])
@pytest.mark.parametrize("limit", _LIMITS)
@pytest.mark.parametrize("order", _ORDERS)
def test_an_ordered_limited_read_answers_on_trino_as_on_postgresql(
    trino_over_postgres, table, order, limit
):
    sql = f'SELECT "v" FROM "{_SCHEMA}"."{table}" ORDER BY "d" {order} {limit}'
    with psycopg.connect(_pg_dsn()) as conn:
        expected = [tuple(r) for r in conn.execute(cast(LiteralString, sql)).fetchall()]
    lowered = transpile(
        f'SELECT "v" FROM "{_CATALOG}"."{_SCHEMA}"."{table}" ORDER BY "d" {order} {limit}', "trino"
    )
    trino_over_postgres.execute(lowered)
    assert [tuple(r) for r in trino_over_postgres.fetchall()] == expected, lowered
