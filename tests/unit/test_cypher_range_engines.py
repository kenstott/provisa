# Copyright (c) 2026 Kenneth Stott
# Canary: 7d2e9b41-c6a3-4f08-9e15-3b8a0c4d6f27
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit: Cypher's range(start, end[, step]), the list from start to end inclusive, as each engine
computes it. The Cypher read names it sequence (Trino's); PostgreSQL has no sequence(), so a
session write UNWIND range(1, 40) AS i CREATE (:Big {k: i}) on the pg engine was refused with
"function sequence(integer, integer) does not exist". On PostgreSQL it is the array of
generate_series (inclusive), on DuckDB generate_series (inclusive), on Trino sequence."""

# Requirements: REQ-345, REQ-066

from __future__ import annotations

import duckdb
import pytest
import sqlglot
import sqlglot.expressions as exp

from provisa.cypher.parser import parse_cypher
from provisa.gql.label_map import CypherLabelMap
from provisa.gql.translator import cypher_to_sql
from provisa.transpiler.transpile import transpile, transpile_to_duckdb, transpile_to_trino

_UNWIND = "UNWIND range(1, 40) AS i RETURN i"


def _semantic_sql(statement: str) -> str:
    """The statement's SQL as the Cypher read writes it, before an engine's transpile."""
    sql_ast, _, _ = cypher_to_sql(
        parse_cypher(statement), CypherLabelMap(nodes={}, relationships={}), {}
    )
    return sql_ast.sql(dialect="postgres")


def _series(sql: str, dialect: str) -> list[tuple[type, list[int]]]:
    """Each integer series ``sql`` calls, as sqlglot reads it in ``dialect`` (PostgreSQL's
    generate_series is ExplodingGenerateSeries, Trino's sequence GenerateSeries), with its bounds
    and step."""
    tree = sqlglot.parse_one(sql, read=dialect)
    found = []
    for node in tree.find_all(exp.GenerateSeries, exp.ExplodingGenerateSeries):
        args = (node.args.get("start"), node.args.get("end"), node.args.get("step"))
        found.append((type(node), [int(a.this) for a in args if a is not None]))
    return found


def _postgres(statement: str) -> str:
    sql = transpile(_semantic_sql(statement), "postgres")
    assert "sequence" not in sql.lower(), sql
    return sql


def test_unwind_range_on_postgres_is_the_array_of_generate_series():
    sql = _postgres(_UNWIND)
    assert _series(sql, "postgres") == [(exp.ExplodingGenerateSeries, [1, 40])], sql
    # A list, as range() is: generate_series returns rows on PostgreSQL, so they are collected.
    tree = sqlglot.parse_one(sql, read="postgres")
    unnested = tree.find(exp.Unnest)
    assert unnested is not None, sql
    (listed,) = unnested.expressions
    assert isinstance(listed, exp.Array), sql
    assert isinstance(listed.expressions[0], exp.Select), sql


def test_a_stepped_range_on_postgres_keeps_its_step():
    sql = _postgres("UNWIND range(1, 10, 3) AS i RETURN i")
    assert _series(sql, "postgres") == [(exp.ExplodingGenerateSeries, [1, 10, 3])], sql


def test_a_returned_range_on_postgres_is_an_array():
    sql = _postgres("RETURN range(1, 3) AS r")
    (projected,) = sqlglot.parse_one(sql, read="postgres").selects
    assert isinstance(projected.this, exp.Array), sql
    assert _series(sql, "postgres") == [(exp.ExplodingGenerateSeries, [1, 3])], sql


def test_unwind_range_on_trino_is_sequence():
    sql = transpile_to_trino(_semantic_sql(_UNWIND))
    assert "generate_series" not in sql.lower(), sql
    assert "SEQUENCE(1, 40)" in sql.upper(), sql
    assert _series(sql, "trino") == [(exp.GenerateSeries, [1, 40])], sql


@pytest.mark.parametrize(
    ("statement", "expected"),
    [
        (_UNWIND, list(range(1, 41))),
        ("UNWIND range(10, 1, -3) AS i RETURN i", [10, 7, 4, 1]),
        ("UNWIND range(1, 0) AS i RETURN i", []),
    ],
)
def test_unwind_range_on_duckdb_is_inclusive(statement, expected):
    sql = transpile_to_duckdb(_semantic_sql(statement))
    assert "sequence" not in sql.lower(), sql
    assert [r[0] for r in duckdb.connect().execute(sql).fetchall()] == expected
