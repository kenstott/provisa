# Copyright (c) 2026 Kenneth Stott
# Canary: 9d2f4b17-6a3e-4c58-b0e1-58f7c3a2d4e6
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An array of arrays on DuckDB by the statement's origin (REQ-2236). SQL a client wrote (pgwire,
/data/sql, Flight SQL) reads ARRAY[ARRAY[...]] as PostgreSQL does: one array of two dimensions,
read element by element. SQL Provisa generated from a graph IR (Cypher, SPARQL, GraphQL: the
compiled path) reads it as a list of lists, as the graph language does."""

# Requirements: REQ-2236

from __future__ import annotations

import duckdb
import pytest

from provisa.cypher.parser import parse_cypher
from provisa.federation.duckdb_backend import DuckDBBackend
from provisa.gql.label_map import CypherLabelMap
from provisa.gql.translator import cypher_to_sql
from provisa.transpiler.transpile import transpile, transpile_to_duckdb

pytestmark = pytest.mark.unit

_EMPTY = CypherLabelMap(nodes={}, relationships={})
_COUNT = "SELECT cardinality(ARRAY[ARRAY[1, 2], ARRAY[3, 4]]) AS n"
_UNNEST = "SELECT x FROM UNNEST(ARRAY[ARRAY[1, 2], ARRAY[3, 4]]) AS u(x)"


def _run(sql: str, *, generated: bool) -> list[tuple]:
    return duckdb.connect().execute(transpile_to_duckdb(sql, generated=generated)).fetchall()


def test_client_sql_counts_every_element_of_an_array_of_arrays() -> None:
    assert _run(_COUNT, generated=False) == [(4,)]


def test_generated_sql_counts_the_lists_of_a_list_of_lists() -> None:
    assert _run(_COUNT, generated=True) == [(2,)]


def test_client_sql_unnests_every_element_in_storage_order() -> None:
    assert _run(_UNNEST, generated=False) == [(1,), (2,), (3,), (4,)]


def test_generated_sql_unnests_each_inner_list() -> None:
    assert _run(_UNNEST, generated=True) == [([1, 2],), ([3, 4],)]


def test_the_two_origins_of_one_statement_are_kept_apart() -> None:
    """The DuckDB form is kept by text: one origin's form is never served for the other's."""
    assert _run(_COUNT, generated=True) == [(2,)]
    assert _run(_COUNT, generated=False) == [(4,)]
    assert _run(_COUNT, generated=True) == [(2,)]


def test_transpile_reads_by_origin() -> None:
    assert "[[1, 2], [3, 4]]" in transpile(_UNNEST, "duckdb", generated=True)
    assert "[[1, 2], [3, 4]]" not in transpile(_UNNEST, "duckdb", generated=False)


def test_the_duckdb_engine_seam_carries_the_origin() -> None:
    """The engine route's transpile (FederationRuntime -> DuckDBBackend.transpile_physical): the
    compiled path's generated=True reaches the dimension reading."""
    physical = DuckDBBackend.transpile_physical
    backend = object.__new__(DuckDBBackend)
    assert duckdb.connect().execute(physical(backend, _COUNT, generated=True)).fetchall() == [(2,)]
    assert duckdb.connect().execute(physical(backend, _COUNT, generated=False)).fetchall() == [(4,)]


@pytest.mark.parametrize(
    "query",
    [
        "UNWIND [[[1, 2], [3, 4]]] AS l RETURN size(l) AS n",
        "WITH [[1, 2], [3, 4]] AS l RETURN size(l) AS n",
        "RETURN size([[1, 2], [3, 4]]) AS n",
    ],
)
def test_cypher_size_of_a_list_of_lists_on_duckdb_is_two(query: str) -> None:
    sql_ast, _, _ = cypher_to_sql(parse_cypher(query), _EMPTY, {})
    assert _run(sql_ast.sql(dialect="postgres"), generated=True) == [(2,)]


def test_cypher_unwind_of_lists_appends_to_each_list() -> None:
    sql_ast, _, _ = cypher_to_sql(parse_cypher("UNWIND [[1], [2]] AS xs RETURN xs + 0"), _EMPTY, {})
    assert _run(sql_ast.sql(dialect="postgres"), generated=True) == [([1, 0],), ([2, 0],)]
