# Copyright (c) 2026 Kenneth Stott
# Canary: 85d00744-c1b1-42c7-b625-0cc435273041
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Cypher's size() of a string is its character count (openCypher): a registered text column is
counted as text on every engine, never as a list (CARDINALITY), which PostgreSQL refuses."""

# Requirements: REQ-345

from __future__ import annotations

import duckdb
import pytest

from provisa.cypher.parser import parse_cypher
from provisa.gql.translator import cypher_to_sql
from provisa.transpiler.transpile import transpile, transpile_to_duckdb
from tests.unit.test_apoc_cypher import _graph

pytestmark = pytest.mark.unit


def _semantic(query: str) -> str:
    sql_ast, _, _ = cypher_to_sql(parse_cypher(query), _graph(), {})
    return sql_ast.sql(dialect="postgres")


@pytest.mark.parametrize("dialect", ["postgres", "trino", "duckdb"])
def test_size_of_a_text_column_is_its_character_count(dialect):
    semantic = _semantic(
        "MATCH (e:Employees)-[:WORKS_IN]->(d:Departments) RETURN size(e.name) AS a, "
        "size(d.title) AS b"
    )
    written = transpile_to_duckdb(semantic) if dialect == "duckdb" else transpile(semantic, dialect)
    assert "CARDINALITY" not in written.upper(), written
    assert "LENGTH(" in written.upper(), written


def test_the_count_is_of_characters():
    semantic = _semantic("WITH 'héllo' AS s RETURN size(s) AS n")
    assert duckdb.sql(transpile_to_duckdb(semantic)).fetchall() == [(5,)]
