# Copyright (c) 2026 Kenneth Stott
# Canary: d6fecc1b-c175-42d5-bb1b-8838a73d36e7
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A statement whose clauses are out of order is refused as a syntax error, as PostgreSQL
refuses it (REQ-264).

sqlglot's parser reads a SELECT's modifiers in any order and writes them back in the right one,
so ``... GROUP BY species LIMIT 1 ORDER BY pet_count DESC`` was answered as if it had been
written ``ORDER BY pet_count DESC LIMIT 1``: a statement PostgreSQL refuses was accepted, and
its answer was one the statement as written does not name."""

# Requirements: REQ-264

from __future__ import annotations

from types import SimpleNamespace

import pytest
import sqlglot

from provisa.compiler.prepared import prepare_front_end

_REPORTED = (
    "SELECT species, COUNT(*) * 2 AS pet_count, ROUND(AVG(price), 2) AS avg_price "
    "FROM pet_store.pets GROUP BY species LIMIT 1 ORDER BY pet_count DESC"
)


async def _prepare(sql: str):
    async def _no_commands(*_args) -> bool:
        return False

    return await prepare_front_end(sql, "analyst", SimpleNamespace(), _no_commands)


def test_the_parser_alone_moves_the_order_by_ahead_of_the_limit():
    """What was answered: the ORDER BY applied, before the LIMIT, though written after it."""
    tree = sqlglot.parse_one(_REPORTED, read="postgres")
    assert tree.sql("postgres").endswith("ORDER BY pet_count DESC LIMIT 1")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("sql", "late", "after"),
    [
        (_REPORTED, "ORDER BY", "LIMIT"),
        ("SELECT a FROM t LIMIT 1 WHERE a > 0", "WHERE", "LIMIT"),
        ("SELECT a FROM t ORDER BY a GROUP BY a", "GROUP BY", "ORDER BY"),
        ("SELECT a FROM t GROUP BY a HAVING COUNT(*) > 1 WHERE a > 0", "WHERE", "HAVING"),
        ("SELECT a FROM t OFFSET 2 ORDER BY a", "ORDER BY", "OFFSET"),
        ("SELECT a FROM (SELECT a FROM t LIMIT 1 ORDER BY a) s", "ORDER BY", "LIMIT"),
    ],
)
async def test_a_clause_written_after_one_it_precedes_is_a_syntax_error(sql, late, after):
    with pytest.raises(sqlglot.errors.ParseError) as refused:
        await _prepare(sql)
    assert str(refused.value) == f'syntax error at or near "{late}": {late} cannot follow {after}'


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "sql",
    [
        "SELECT species, COUNT(*) AS n FROM pets WHERE price > 1 GROUP BY species "
        "HAVING COUNT(*) > 1 ORDER BY n DESC LIMIT 1 OFFSET 2",
        "SELECT a FROM t OFFSET 2 LIMIT 1",  # PostgreSQL takes LIMIT and OFFSET either way
        "SELECT a FROM t ORDER BY a FETCH FIRST 1 ROWS ONLY",
        "SELECT a FROM t WHERE a IS DISTINCT FROM b ORDER BY a",
        "SELECT a, ROW_NUMBER() OVER (PARTITION BY b ORDER BY c) FROM t WHERE a > 0 LIMIT 5",
        "SELECT string_agg(a, ',' ORDER BY a) FROM t WHERE a > 0 GROUP BY b",
        "SELECT COUNT(*) FILTER (WHERE a > 0) FROM t GROUP BY b ORDER BY 1",
        "SELECT a FROM (SELECT a FROM t ORDER BY a LIMIT 3) s WHERE a > 0 ORDER BY a LIMIT 1",
        "WITH s AS (SELECT a FROM t ORDER BY a LIMIT 3) SELECT a FROM s WHERE a > 0",
        "SELECT a FROM t WHERE a > 0 UNION SELECT a FROM u WHERE a < 0 ORDER BY a LIMIT 2",
        "INSERT INTO t (a) VALUES (1) ON CONFLICT (a) WHERE a > 0 DO UPDATE SET a = 2 WHERE t.a > 1",
        "SELECT SUBSTRING(a FROM 2) FROM t ORDER BY 1",
        "INSERT INTO t SELECT a FROM u ORDER BY a ON CONFLICT (a) DO UPDATE SET a = 1 WHERE t.a > 0",
    ],
)
async def test_clauses_in_their_order_are_taken(sql):
    await _prepare(sql)
