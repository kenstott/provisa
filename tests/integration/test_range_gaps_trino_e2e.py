# Copyright (c) 2026 Kenneth Stott
# Canary: 01f8fe5c-d738-4e7e-95ce-8ad1ff043353
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The range corpus's statements of an untyped NULL beside a column, multirange_cmp of columns and
a multirange of an array the rows hold, written for Trino and run there (REQ-2171). The expected
answers are PostgreSQL 17's, recorded (tests/unit/pg17_range_answers.py); tests/unit/test_pg_ranges.py
replays the same statements on DuckDB.

The table ``s.t`` is the corpus's samples as a VALUES list in the statement Trino is sent: each
range is its text on the engine, as a registered range column is."""

# Requirements: REQ-2171

from __future__ import annotations

from typing import Any

import pytest
import sqlglot
import sqlglot.expressions as exp
import trino

from provisa.compiler.native_subtype import mark_subtype_operations
from provisa.core.pg_error import from_engine
from provisa.transpiler.transpile import transpile
from tests.unit import range_corpus as corpus
from tests.unit.pg17_range_answers import RECORDED

pytestmark = [pytest.mark.requires_trino, pytest.mark.integration]


def _cases() -> list[tuple[str, str]]:
    cases = [
        (type_name, statement)
        for type_name in corpus.SAMPLES
        for statement in corpus.null_statements("range_union")
        + corpus.variadic_statements(type_name)
    ]
    cases += [
        (type_name, statement)
        for type_name in corpus.MULTIRANGES
        for statement in corpus.multirange_cmp_statements()
        + corpus.null_statements("multirange_union")
    ]
    return cases


def _samples(type_name: str) -> list[str | None]:
    return {**corpus.SAMPLES, **corpus.MULTIRANGES}[type_name]


def _with_samples(trino_sql: str, samples: list[str | None]) -> str:
    """``trino_sql`` with each read of ``s.t`` a VALUES list of the samples, under its alias."""

    def rows() -> list[exp.Tuple]:
        return [
            exp.tuple_(
                exp.Literal.number(i),
                exp.cast(exp.null(), "VARCHAR") if v is None else exp.Literal.string(v),
            )
            for i, v in enumerate(samples)
        ]

    def swapped(node: exp.Expr) -> exp.Expr:
        if isinstance(node, exp.Table) and (node.db, node.name) == ("s", "t"):
            return exp.values(rows(), alias=node.alias_or_name, columns=["i", "v"])
        return node

    return sqlglot.parse_one(trino_sql, read="trino").transform(swapped).sql(dialect="trino")


def _answer(conn: Any, statement: str, type_name: str) -> list | str:
    written = transpile(mark_subtype_operations(statement, {("s", "t"): {"v": type_name}}), "trino")
    cur = conn.cursor()
    try:
        cur.execute(_with_samples(written, _samples(type_name)))
        rows = cur.fetchall()
    except trino.exceptions.TrinoQueryError as error:
        known = from_engine(error)
        if known is None:
            raise
        return f"ERROR {known.sqlstate}"
    return [corpus.text_of(row[-1]) for row in rows]


@pytest.fixture(scope="module")
def utc_trino() -> Any:
    """The corpus was recorded with the session in UTC."""
    import os

    conn = trino.dbapi.connect(
        host=os.environ.get("TRINO_HOST", "localhost"),
        port=int(os.environ.get("TRINO_PORT", "8080")),
        user="test",
        timezone="UTC",
    )
    yield conn
    conn.close()


@pytest.mark.parametrize(("type_name", "statement"), _cases())
def test_a_range_gap_answers_on_trino_as_postgresql_does(utc_trino, type_name, statement):
    assert _answer(utc_trino, statement, type_name) == RECORDED[type_name][statement]
