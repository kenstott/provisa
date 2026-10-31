# Copyright (c) 2026 Kenneth Stott
# Canary: 7ac2e851-49d6-4f03-b1e8-d62f03c7a914
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A range's emulation on an engine that is not DuckDB (REQ-2171, RANGES ARE EMULATED ON EVERY
ENGINE). The statements of tests/unit/range_corpus.py, written as Provisa writes them for the
engine and run there over a table of the values' text, against PostgreSQL 17's recorded answers:
what tests/unit/test_pg_ranges.py proves on DuckDB, on each engine's own functions and types."""

# Requirements: REQ-2171

from __future__ import annotations

import os
import re
from collections.abc import Callable
from typing import Any

import pytest

from provisa.compiler.native_subtype import mark_subtype_operations
from provisa.executor.pool import SourcePool
from provisa.transpiler.transpile import transpile
from tests.unit import range_corpus as corpus
from tests.unit.pg17_range_answers import RECORDED

pytestmark = [pytest.mark.integration]

#: The operators on every pair of a type's values.
_STATEMENTS = corpus.pair_statements()
_PREDICATES = frozenset(corpus.predicate_statements())
_TABLE = re.compile(r"\bs\.t\b(?: AS (\w+))?")


#: The dialect's spelling of a NULL of the column's text type.
_NULL_TEXT = {"clickhouse": "CAST(NULL AS Nullable(String))"}
#: The engines whose value of a predicate is the integer 1 or 0, read here as PostgreSQL's boolean
#: as pgwire describes it (cast_types). Any other value is no boolean, and fails.
_INTEGER_BOOLEANS = frozenset({"clickhouse", "tsql", "mysql"})
_BOOLEAN_TEXT = {1: "t", 0: "f", None: None}


def _literal(value: str | None, dialect: str) -> str:
    if value is None:
        return _NULL_TEXT.get(dialect, "CAST(NULL AS VARCHAR(255))")
    return f"'{value}'"


def _inline(sql: str, samples: list[str | None], dialect: str) -> str:
    """``sql`` over the values themselves: a subquery of ``i`` and ``v`` where it reads s.t."""
    rows = " UNION ALL ".join(
        f"SELECT {i} AS i, {_literal(value, dialect)} AS v" for i, value in enumerate(samples)
    )
    return _TABLE.sub(lambda m: f"({rows}) AS {m[1] or 't'}", sql)


def _text(value: Any, statement: str, dialect: str) -> str | None:
    if dialect in _INTEGER_BOOLEANS and statement in _PREDICATES:
        return _BOOLEAN_TEXT[value]
    return corpus.text_of(value)


def _check(run: Callable[[str], list[Any]], dialect: str, type_name: str) -> None:
    samples = corpus.SAMPLES[type_name]
    for statement in _STATEMENTS:
        marked = mark_subtype_operations(statement, {("s", "t"): {"v": type_name}})
        rows = run(_inline(transpile(marked, dialect), samples, dialect))
        got = [_text(row[-1], statement, dialect) for row in rows]
        assert got == RECORDED[type_name][statement], statement


_TYPES = list(corpus.SAMPLES)


@pytest.mark.requires_trino
@pytest.mark.parametrize("type_name", _TYPES)
def test_ranges_on_trino(trino_conn, type_name):
    def run(sql: str) -> list[Any]:
        cursor = trino_conn.cursor()
        cursor.execute(sql)
        return cursor.fetchall()

    _check(run, "trino", type_name)


@pytest.mark.requires_clickhouse
@pytest.mark.parametrize("type_name", _TYPES)
def test_ranges_on_clickhouse(type_name):
    import clickhouse_connect

    client = clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST", "localhost"),
        port=int(os.environ.get("CLICKHOUSE_PORT", "8123")),
        username=os.environ.get("CLICKHOUSE_USER", "default"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
    )
    _check(lambda sql: list(client.query(sql).result_rows), "clickhouse", type_name)


async def _read(source_type: str, port: int, database: str, user: str, password: str, sql: str):
    pool = SourcePool()
    await pool.add(
        source_id=f"ranges-{source_type}",
        source_type=source_type,
        host="localhost",
        port=port,
        database=database,
        user=user,
        password=password,
    )
    try:
        return (await pool.execute(f"ranges-{source_type}", sql)).rows
    finally:
        await pool.close_all()


def _driven(source_type: str, port: int, database: str, user: str, password: str):
    import asyncio

    def run(sql: str) -> list[Any]:
        return asyncio.run(_read(source_type, port, database, user, password, sql))

    return run


@pytest.mark.requires_sqlserver
@pytest.mark.parametrize("type_name", _TYPES)
def test_ranges_on_sql_server(type_name):
    port = int(os.environ["SQLSERVER_PORT"])
    _check(_driven("sqlserver", port, "master", "sa", "Provisa_2026!"), "tsql", type_name)


@pytest.mark.requires_mariadb
@pytest.mark.parametrize("type_name", _TYPES)
def test_ranges_on_mysql_family(type_name):
    port = int(os.environ["MARIADB_PORT"])
    _check(_driven("mariadb", port, "provisa", "root", "provisa"), "mysql", type_name)
