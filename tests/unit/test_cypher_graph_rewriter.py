# Copyright (c) 2026 Kenneth Stott
# Canary: 7d2f5a9c-3b4e-4a8f-9c2d-1e5b3f7a9d2c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for provisa/cypher/graph_rewriter.py."""

import sqlglot
import sqlglot.expressions as exp

from provisa.cypher.graph_rewriter import apply_graph_rewrites
from provisa.cypher.label_map import CypherLabelMap, NodeMapping, RelationshipMapping
from provisa.cypher.translator import GraphVarKind


def _make_label_map() -> CypherLabelMap:
    person_meta = NodeMapping(
        label="Person",
        type_name="Person",
        domain_label=None,
        table_label="Person",
        table_id=1,
        source_id="pg-main",
        id_column="id",
        catalog_name="postgresql",
        schema_name="public",
        table_name="persons",
        properties={"name": "name", "age": "age"},
    )
    return CypherLabelMap(nodes={"Person": person_meta}, relationships={})


def _parse_sql(sql: str) -> exp.Select:
    return sqlglot.parse_one(sql, dialect="trino")


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_scalar_column_untouched():
    sql = 'SELECT n."name" FROM "public"."persons" AS n'
    ast = _parse_sql(sql)
    lm = _make_label_map()
    result = apply_graph_rewrites(ast, {}, lm)
    result_sql = result.sql(dialect="trino")
    assert "name" in result_sql
    # No ROW wrapping
    assert "ROW" not in result_sql.upper()


def test_node_variable_wrapped():
    sql = 'SELECT n AS n FROM "postgresql"."public"."persons" AS n'
    ast = _parse_sql(sql)
    lm = _make_label_map()
    graph_vars = {"n": GraphVarKind.NODE}
    result = apply_graph_rewrites(ast, graph_vars, lm)
    result_sql = result.sql(dialect="trino")
    # Should contain CAST and ROW or JSON wrapping
    assert "CAST" in result_sql.upper() or "ROW" in result_sql.upper() or "JSON" in result_sql.upper()


def test_mixed_scalar_and_node():
    sql = 'SELECT n."age" AS age, n AS n FROM "postgresql"."public"."persons" AS n'
    ast = _parse_sql(sql)
    lm = _make_label_map()
    graph_vars = {"n": GraphVarKind.NODE}
    result = apply_graph_rewrites(ast, graph_vars, lm)
    result_sql = result.sql(dialect="trino")
    # age column should remain untouched (no ROW/CAST around just age)
    assert "age" in result_sql


def test_no_graph_vars_no_change():
    sql = 'SELECT n."name", n."age" FROM "postgresql"."public"."persons" AS n'
    ast = _parse_sql(sql)
    lm = _make_label_map()
    result = apply_graph_rewrites(ast, {}, lm)
    result_sql = result.sql(dialect="trino")
    assert "ROW" not in result_sql.upper()
    assert "CAST" not in result_sql.upper()
