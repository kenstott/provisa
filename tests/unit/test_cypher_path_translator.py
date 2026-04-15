# Copyright (c) 2026 Kenneth Stott
# Canary: 3c8f5a1e-7b2d-4a9f-8c4e-1d3b5f7a9c2e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for provisa/cypher/path_translator.py."""

import pytest

from provisa.cypher.parser import CypherParseError, parse_cypher
from provisa.cypher.label_map import CypherLabelMap, NodeMapping, RelationshipMapping
from provisa.cypher.path_translator import PathTranslateError, path_to_recursive_sql


def _make_label_map() -> CypherLabelMap:
    person_meta = NodeMapping(
        label="Person",
        type_name="Person",
        domain_label=None,
        table_label="Person",
        table_id=1,
        source_id="pg-main",
        id_column="id",
        pk_columns=[],
        catalog_name="postgresql",
        schema_name="public",
        table_name="persons",
        properties={"name": "name"},
    )
    company_meta = NodeMapping(
        label="Company",
        type_name="Company",
        domain_label=None,
        table_label="Company",
        table_id=2,
        source_id="pg-main",
        id_column="id",
        pk_columns=[],
        catalog_name="postgresql",
        schema_name="public",
        table_name="companies",
        properties={"name": "name"},
    )
    works_at = RelationshipMapping(
        rel_type="WORKS_AT",
        source_label="Person",
        target_label="Company",
        join_source_column="company_id",
        join_target_column="id",
        field_name="works_at",
    )
    return CypherLabelMap(
        nodes={"Person": person_meta, "Company": company_meta},
        relationships={"WORKS_AT": works_at},
    )


def _extract_path_func(query: str):
    ast = parse_cypher(query)
    from provisa.cypher.parser import PathFunction
    for clause in ast.match_clauses:
        if isinstance(clause.pattern, PathFunction):
            return clause.pattern, clause.variable
    raise ValueError("No path function found")


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_shortest_path_generates_recursive_cte():
    lm = _make_label_map()
    path_func, path_var = _extract_path_func(
        "MATCH p = shortestPath((a:Person)-[:WORKS_AT*..5]->(b:Company)) RETURN p"
    )
    sql, final_alias, filter_clause = path_to_recursive_sql(
        path_func, lm, "a", "b", "p", max_depth=10
    )
    assert "WITH RECURSIVE" in sql
    assert "_cypher_path" in sql
    assert "_depth" in sql


def test_depth_limit_enforced():
    lm = _make_label_map()
    path_func, _ = _extract_path_func(
        "MATCH p = shortestPath((a:Person)-[:WORKS_AT*..3]->(b:Company)) RETURN p"
    )
    sql, _, _ = path_to_recursive_sql(path_func, lm, "a", "b", "p", max_depth=10)
    # Depth limit 3 should appear in the SQL
    assert "3" in sql


def test_all_shortest_paths_generates_cte():
    lm = _make_label_map()
    path_func, _ = _extract_path_func(
        "MATCH p = allShortestPaths((a:Person)-[:WORKS_AT*..5]->(b:Company)) RETURN p"
    )
    sql, final_alias, _ = path_to_recursive_sql(path_func, lm, "a", "b", "p", max_depth=10)
    assert "WITH RECURSIVE" in sql
    assert "_min_depths" in sql or "_shortest" in sql


def test_max_depth_capped_by_argument():
    lm = _make_label_map()
    path_func, _ = _extract_path_func(
        "MATCH p = shortestPath((a:Person)-[:WORKS_AT*..20]->(b:Company)) RETURN p"
    )
    # Pass max_depth=5 to cap it
    sql, _, _ = path_to_recursive_sql(path_func, lm, "a", "b", "p", max_depth=5)
    # Should be capped at 5, not 20
    assert "20" not in sql


def test_unbounded_at_path_translator_level():
    """Unbounded [*] is rejected at parse time; verify parse_cypher enforces it."""
    with pytest.raises(CypherParseError, match="[Uu]nbounded"):
        parse_cypher("MATCH p = shortestPath((a:Person)-[*]->(b:Company)) RETURN p")


def test_missing_relationship_raises():
    lm = _make_label_map()
    path_func, _ = _extract_path_func(
        "MATCH p = shortestPath((a:Person)-[:KNOWS*..5]->(b:Company)) RETURN p"
    )
    with pytest.raises(PathTranslateError, match="No registered relationship"):
        path_to_recursive_sql(path_func, lm, "a", "b", "p", max_depth=10)
