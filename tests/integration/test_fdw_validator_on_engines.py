# Copyright (c) 2026 Kenneth Stott
# Canary: 3e7b1c58-9d24-4a60-b5f1-8c0a2d6e4973
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""postgresql_fdw_validator(text[], oid) on Trino, ClickHouse and PostgreSQL (REQ-2236: exact, or
a named refusal): the statement as Provisa writes it for the engine and run there. An option the
catalog does not list is 42601 naming the first such option, as PostgreSQL's foreign.c raises it;
an array holding a NULL element is 22004, as PostgreSQL's untransformRelOptions raises it before
any option is read. ClickHouse 24.3's throwIf takes only a constant message, so there an unlisted
option is refused by name."""

# Requirements: REQ-2236

from __future__ import annotations

import os
from collections.abc import Callable
from typing import Any

import pytest
import sqlglot

from provisa.core.pg_error import from_engine, postgres_error
from provisa.transpiler.metadata_facts import CatalogFacts
from provisa.transpiler.metadata_functions import expand
from provisa.transpiler.metadata_server_forms import FDW_OPTION_REFUSAL
from provisa.transpiler.transpile import transpile

pytestmark = [pytest.mark.integration]

#: (options, catalog, the option PostgreSQL's 42601 names, or None where it answers true)
CASES = [
    ("{}", 12345, None),
    ("{host,port=1}", 1417, None),
    ("{user}", 1418, None),
    ('{"",日本語😀,é}', 12345, ""),
    ("{host,user}", 1417, "user"),
    ("{a=1}", 1418, "a"),
    ("{host,b,c}", 1417, "b"),
    # untransformRelOptions deconstructs an array of any dimensions into its elements in order.
    ("{{a,b},{c,d}}", 12345, "a"),
    ("{{host,port},{dbname,user}}", 1417, "user"),
    ("{{host,port},{dbname,port=1}}", 1417, None),
]
#: Arrays holding a NULL element: 22004 wherever the NULL stands, an unlisted option before it too.
NULL_ELEMENTS = [
    ("{host,NULL}", 1417),
    ("{b,NULL}", 1417),
    ("{NULL}", 12345),
    ("{{host,port},{b,NULL}}", 1417),
]
NULL_ERROR = ("22004", "null array element not allowed in this context")


def _written(options: str, catalog: int, dialect: str) -> str:
    sql = (
        f"SELECT pg_catalog.\"postgresql_fdw_validator\"('{options}'::text[], {catalog}::oid) AS v"
    )
    tree = sqlglot.parse_one(sql, read="postgres")
    return transpile(expand(tree, CatalogFacts("analyst")).sql("postgres"), dialect)


def _check(run: Callable[[str], Any], dialect: str) -> None:
    wrong = {}
    for options, catalog, name in CASES:
        try:
            got: Any = run(_written(options, catalog, dialect))
        except Exception as raised:  # noqa: BLE001 -- every case is reported, not the first
            error = postgres_error(raised) or from_engine(raised)
            got = None if error is None else (error.sqlstate, str(error))
        want: Any = True if name is None else ("42601", f'invalid option "{name}"')
        if name is not None and dialect == "clickhouse":
            want = ("0A000", FDW_OPTION_REFUSAL)
        if got != want:
            wrong[(options, catalog)] = got
    for options, catalog in NULL_ELEMENTS:
        try:
            wrong[(options, catalog)] = run(_written(options, catalog, dialect))
        except Exception as raised:  # noqa: BLE001
            error = postgres_error(raised) or from_engine(raised)
            got = None if error is None else (error.sqlstate, str(error))
            if got != NULL_ERROR:
                wrong[(options, catalog)] = got or str(raised)[:200]
    assert not wrong, wrong


@pytest.mark.requires_trino
def test_fdw_validator_on_trino(trino_conn):
    def run(sql: str) -> Any:
        cursor = trino_conn.cursor()
        cursor.execute(sql)
        return cursor.fetchall()[0][0]

    _check(run, "trino")


@pytest.mark.requires_clickhouse
def test_fdw_validator_on_clickhouse():
    import clickhouse_connect

    client = clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST", "localhost"),
        port=int(os.environ.get("CLICKHOUSE_PORT", "8123")),
        username=os.environ.get("CLICKHOUSE_USER", "default"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
    )
    _check(lambda sql: client.query(sql).result_rows[0][0], "clickhouse")


@pytest.mark.usefixtures("docker_postgres")
def test_fdw_validator_on_postgresql(pg_dsn):
    import psycopg2

    from provisa.transpiler.metadata_server_forms import RAISE_FUNCTIONS_SQL

    con = psycopg2.connect(pg_dsn)
    con.autocommit = True
    try:
        with con.cursor() as cur:
            for statement in RAISE_FUNCTIONS_SQL:
                cur.execute(statement)

        def run(sql: str) -> Any:
            with con.cursor() as cur:
                cur.execute(sql)
                return cur.fetchall()[0][0]

        _check(run, "postgres")
        # The form is PostgreSQL's own answer: the native function says the same on each case.
        with con.cursor() as cur:
            for options, catalog, name in CASES:
                try:
                    cur.execute(
                        "SELECT postgresql_fdw_validator(%s::text[], %s::oid)", (options, catalog)
                    )
                    native: Any = cur.fetchall()[0][0]
                except psycopg2.Error as raised:
                    native = (raised.pgcode, raised.diag.message_primary)
                assert native == (
                    True if name is None else ("42601", f'invalid option "{name}"')
                ), (options, catalog)
            for options, catalog in NULL_ELEMENTS:
                with pytest.raises(psycopg2.Error) as raised:
                    cur.execute(
                        "SELECT postgresql_fdw_validator(%s::text[], %s::oid)", (options, catalog)
                    )
                assert (raised.value.pgcode, raised.value.diag.message_primary) == NULL_ERROR
    finally:
        con.close()
