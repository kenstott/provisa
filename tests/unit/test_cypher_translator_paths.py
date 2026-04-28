# Copyright (c) 2026 Kenneth Stott
# Canary: 4a1c7e3f-2b9d-4a5e-8f3c-6d4a8b2f7c9e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for provisa/cypher/translator.py — recursive CTEs, CASE, UNWIND, paths."""

import pytest

from provisa.cypher.parser import parse_cypher
from provisa.cypher.label_map import CypherLabelMap, NodeMapping, RelationshipMapping
from provisa.cypher.translator import cypher_to_sql, cypher_calls_to_sql_list


def _make_label_map_multi_path() -> CypherLabelMap:
    """Label map with two 1-hop paths from Person to Company: WORKS_AT and MANAGES."""
    person_meta = NodeMapping(
        label="Person", type_name="Person", domain_label=None, table_label="Person",
        table_id=1, source_id="pg-main", id_column="id", pk_columns=[],
        catalog_name="postgresql", schema_name="public", table_name="persons",
        properties={"name": "name", "age": "age"},
    )
    company_meta = NodeMapping(
        label="Company", type_name="Company", domain_label=None, table_label="Company",
        table_id=2, source_id="pg-main", id_column="id", pk_columns=[],
        catalog_name="postgresql", schema_name="public", table_name="companies",
        properties={"name": "name"},
    )
    rels = {
        "WORKS_AT": RelationshipMapping(
            rel_type="WORKS_AT", source_label="Person", target_label="Company",
            join_source_column="company_id", join_target_column="id", field_name="works_at",
        ),
        "MANAGES": RelationshipMapping(
            rel_type="MANAGES", source_label="Person", target_label="Company",
            join_source_column="managed_company_id", join_target_column="id", field_name="manages",
        ),
    }
    return CypherLabelMap(nodes={"Person": person_meta, "Company": company_meta}, relationships=rels)


def _make_label_map(multi_source: bool = False, with_domains: bool = False) -> CypherLabelMap:
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
        properties={"name": "name", "age": "age"},
    )
    company_meta = NodeMapping(
        label="Company",
        type_name="Company",
        domain_label=None,
        table_label="Company",
        table_id=2,
        source_id="pg-main" if not multi_source else "pg-secondary",
        id_column="id",
        pk_columns=[],
        catalog_name="postgresql",
        schema_name="public",
        table_name="companies",
        properties={"name": "name", "founded": "founded"},
    )
    knows_rel = RelationshipMapping(
        rel_type="KNOWS",
        source_label="Person",
        target_label="Person",
        join_source_column="person_id",
        join_target_column="id",
        field_name="knows",
    )
    works_at_rel = RelationshipMapping(
        rel_type="WORKS_AT",
        source_label="Person",
        target_label="Company",
        join_source_column="company_id",
        join_target_column="id",
        field_name="works_at",
    )
    nodes = {"Person": person_meta, "Company": company_meta}
    rels = {"KNOWS": knows_rel, "WORKS_AT": works_at_rel}
    domains = {"Sales": ["Person", "Company"]} if with_domains else {}
    return CypherLabelMap(nodes=nodes, relationships=rels, domains=domains)


def _make_label_map_indirect_cycle() -> CypherLabelMap:
    """Label map: Person -[WORKS_AT]-> Company -[EMPLOYS]-> Person (indirect cycle)."""
    person_meta = NodeMapping(
        label="Person", type_name="Person", domain_label=None, table_label="Person",
        table_id=1, source_id="pg", id_column="id", pk_columns=[],
        catalog_name="postgresql", schema_name="public", table_name="persons",
        properties={"name": "name"},
    )
    company_meta = NodeMapping(
        label="Company", type_name="Company", domain_label=None, table_label="Company",
        table_id=2, source_id="pg", id_column="id", pk_columns=[],
        catalog_name="postgresql", schema_name="public", table_name="companies",
        properties={"name": "name"},
    )
    rels = {
        "WORKS_AT": RelationshipMapping(
            rel_type="WORKS_AT", source_label="Person", target_label="Company",
            join_source_column="company_id", join_target_column="id", field_name="works_at",
        ),
        "EMPLOYS": RelationshipMapping(
            rel_type="EMPLOYS", source_label="Company", target_label="Person",
            join_source_column="id", join_target_column="company_id", field_name="employs",
        ),
    }
    return CypherLabelMap(nodes={"Person": person_meta, "Company": company_meta}, relationships=rels)


def _make_label_map_self_ref() -> CypherLabelMap:
    """Label map with a self-referential KNOWS relationship for recursive path tests."""
    person_meta = NodeMapping(
        label="Person", type_name="Person", domain_label=None, table_label="Person",
        table_id=1, source_id="pg-main", id_column="id", pk_columns=[],
        catalog_name="postgresql", schema_name="public", table_name="persons",
        properties={"name": "name", "age": "age"},
    )
    knows_rel = RelationshipMapping(
        rel_type="KNOWS", source_label="Person", target_label="Person",
        join_source_column="person_id", join_target_column="id", field_name="knows",
    )
    return CypherLabelMap(nodes={"Person": person_meta}, relationships={"KNOWS": knows_rel})


# ---------------------------------------------------------------------------
# Recursive CTE (self-referential variable-length paths)
# ---------------------------------------------------------------------------

def test_shortestpath_recursive_emits_with_recursive():
    # Person-[:KNOWS*..5]->Person: same src/tgt type → WITH RECURSIVE
    lm = _make_label_map()
    ast = parse_cypher(
        "MATCH p = shortestPath((a:Person)-[:KNOWS*..5]->(b:Person)) RETURN a.name, b.name"
    )
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "WITH RECURSIVE" in sql.upper()
    assert "persons" in sql.lower()


def test_shortestpath_recursive_order_by_hops_limit_1():
    # shortestPath on self-referential path: ORDER BY hops + LIMIT 1
    lm = _make_label_map()
    ast = parse_cypher(
        "MATCH p = shortestPath((a:Person)-[:KNOWS*..5]->(b:Person)) RETURN a.name, b.name"
    )
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "ORDER BY" in sql.upper()
    assert "hops" in sql.lower()
    assert "LIMIT" in sql.upper()
    assert "1" in sql


def test_allshortestpaths_recursive_order_by_no_extra_limit():
    # allShortestPaths: ORDER BY hops but no LIMIT 1 injected
    lm = _make_label_map()
    ast = parse_cypher(
        "MATCH p = allShortestPaths((a:Person)-[:KNOWS*..5]->(b:Person)) RETURN a.name, b.name"
    )
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "WITH RECURSIVE" in sql.upper()
    assert "ORDER BY" in sql.upper()
    assert "hops" in sql.lower()
    assert "LIMIT" not in sql.upper()


def test_shortestpath_recursive_indirect_cycle():
    # Person-[*..4]->Person via indirect cycle Person→Company→Person → WITH RECURSIVE
    lm = _make_label_map_indirect_cycle()
    ast = parse_cypher(
        "MATCH p = shortestPath((a:Person)-[*..4]->(b:Person)) RETURN a.name, b.name"
    )
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "WITH RECURSIVE" in sql.upper()
    assert "persons" in sql.lower()
    assert "ORDER BY" in sql.upper()
    assert "LIMIT" in sql.upper()


def test_shortestpath_recursive_cte_contains_hop_guard():
    # The recursive CTE body must enforce the max_hops cap via a WHERE clause
    lm = _make_label_map()
    ast = parse_cypher(
        "MATCH p = shortestPath((a:Person)-[:KNOWS*..3]->(b:Person)) RETURN a.name, b.name"
    )
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    # hops < max_hops guard must appear in the recursive CTE body
    assert "3" in sql  # max_hops value
    assert "hops" in sql.lower()


# ---------------------------------------------------------------------------
# CASE expression tests
# ---------------------------------------------------------------------------

def test_case_searched_in_return():
    lm = _make_label_map()
    ast = parse_cypher(
        "MATCH (n:Person) RETURN CASE WHEN n.age > 18 THEN 'adult' ELSE 'child' END AS category"
    )
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    sql_upper = sql.upper()
    assert "CASE" in sql_upper
    assert "WHEN" in sql_upper
    assert "THEN" in sql_upper
    assert "ELSE" in sql_upper
    assert "END" in sql_upper
    assert "category" in sql.lower()


def test_case_simple_in_return():
    lm = _make_label_map()
    ast = parse_cypher(
        "MATCH (n:Person) RETURN CASE n.name WHEN 'Alice' THEN 1 ELSE 0 END AS flag"
    )
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    sql_upper = sql.upper()
    assert "CASE" in sql_upper
    assert "WHEN" in sql_upper
    assert "THEN" in sql_upper
    assert "flag" in sql.lower()


def test_case_multiple_when_branches():
    lm = _make_label_map()
    ast = parse_cypher(
        "MATCH (n:Person) "
        "RETURN CASE "
        "  WHEN n.age < 18 THEN 'minor' "
        "  WHEN n.age < 65 THEN 'adult' "
        "  ELSE 'senior' "
        "END AS group"
    )
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert sql.upper().count("WHEN") >= 2
    assert "ELSE" in sql.upper()


def test_case_no_else():
    lm = _make_label_map()
    ast = parse_cypher(
        "MATCH (n:Person) RETURN CASE WHEN n.age > 18 THEN 'adult' END AS category"
    )
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "CASE" in sql.upper()
    assert "WHEN" in sql.upper()
    assert "ELSE" not in sql.upper()


def test_case_in_where():
    lm = _make_label_map()
    ast = parse_cypher(
        "MATCH (n:Person) "
        "WHERE CASE WHEN n.age > 18 THEN 'adult' ELSE 'child' END = 'adult' "
        "RETURN n.name"
    )
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "CASE" in sql.upper()
    assert "persons" in sql.lower()


def test_case_with_property_access():
    lm = _make_label_map()
    ast = parse_cypher(
        "MATCH (n:Person) "
        "RETURN n.name, CASE WHEN n.age > 30 THEN n.name ELSE 'young' END AS label"
    )
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "CASE" in sql.upper()
    assert "name" in sql.lower()


# ---------------------------------------------------------------------------
# UNWIND tests
# ---------------------------------------------------------------------------

def test_unwind_integer_list():
    lm = _make_label_map()
    ast = parse_cypher("UNWIND [1, 2, 3] AS x RETURN x")
    sql_ast, params, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "UNNEST" in sql.upper()
    assert "ARRAY" in sql.upper()
    assert not params


def test_unwind_string_list():
    lm = _make_label_map()
    ast = parse_cypher("UNWIND ['alice', 'bob'] AS name RETURN name")
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "UNNEST" in sql.upper()
    assert "alice" in sql


def test_unwind_param():
    lm = _make_label_map()
    ast = parse_cypher("UNWIND $list AS x RETURN x")
    sql_ast, params, _ = cypher_to_sql(ast, lm, {"list": [1, 2, 3]})
    sql = sql_ast.sql(dialect="trino")
    assert "UNNEST" in sql.upper()
    assert "$1" in sql
    assert params == ["list"]


def test_unwind_after_match():
    lm = _make_label_map()
    ast = parse_cypher("MATCH (n:Person) UNWIND [1, 2] AS i RETURN n.name, i")
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "persons" in sql.lower()
    assert "CROSS JOIN" in sql.upper()
    assert "UNNEST" in sql.upper()
    assert "name" in sql.lower()


def test_unwind_variable_accessible_in_return():
    lm = _make_label_map()
    ast = parse_cypher("UNWIND [10, 20, 30] AS val RETURN val")
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "val" in sql.lower()
    assert "FROM" in sql.upper()


def test_unwind_multiple_items():
    lm = _make_label_map()
    ast = parse_cypher("UNWIND [1, 2] AS a UNWIND [3, 4] AS b RETURN a, b")
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert sql.upper().count("UNNEST") == 2


def test_unwind_alias_in_return():
    lm = _make_label_map()
    ast = parse_cypher("UNWIND [1, 2, 3] AS num RETURN num AS value")
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "value" in sql.lower()
    assert "UNNEST" in sql.upper()


# ---------------------------------------------------------------------------
# Gap #11 — IN list predicate
# ---------------------------------------------------------------------------

def test_in_list_literal():
    lm = _make_label_map()
    ast = parse_cypher("MATCH (n:Person) WHERE n.name IN ['Alice', 'Bob'] RETURN n.name")
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert " IN " in sql.upper()
    assert "Alice" in sql or "'Alice'" in sql


def test_in_list_integer_values():
    lm = _make_label_map()
    ast = parse_cypher("MATCH (n:Person) WHERE n.age IN [25, 30, 35] RETURN n.name")
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert " IN " in sql.upper()
    assert "25" in sql


def test_in_list_return_expression():
    lm = _make_label_map()
    # IN in RETURN (e.g. inside CASE or boolean expression)
    ast = parse_cypher("MATCH (n:Person) RETURN n.name IN ['Alice', 'Bob'] AS is_known")
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert " IN " in sql.upper()
    assert "is_known" in sql.lower()


# ---------------------------------------------------------------------------
# Gap #3 — Backward traversal
# ---------------------------------------------------------------------------

def test_backward_traversal_basic():
    lm = _make_label_map()
    ast = parse_cypher("MATCH (c:Company)<-[:WORKS_AT]-(p:Person) RETURN p.name, c.name")
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "persons" in sql.lower()
    assert "companies" in sql.lower()
    assert "company_id" in sql.lower()


def test_backward_traversal_untyped():
    lm = _make_label_map()
    ast = parse_cypher("MATCH (c:Company)<-[]-(p:Person) RETURN p.name, c.name")
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "persons" in sql.lower()
    assert "companies" in sql.lower()


def test_backward_traversal_join_condition():
    lm = _make_label_map()
    ast = parse_cypher("MATCH (c:Company)<-[:WORKS_AT]-(p:Person) RETURN p.name")
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    # The ON condition should join p.company_id = c.id (forward: person.company_id = company.id)
    sql_lower = sql.lower()
    assert "company_id" in sql_lower
    assert "join" in sql_lower


# ---------------------------------------------------------------------------
# Gap #6 — length(p) for shortestPath recursive CTE
# ---------------------------------------------------------------------------

def test_length_p_shortestpath_recursive():
    lm = _make_label_map_self_ref()
    ast = parse_cypher(
        "MATCH p = shortestPath((a:Person)-[:KNOWS*..5]->(b:Person)) "
        "WHERE a.name = 'Alice' AND b.name = 'Bob' RETURN length(p)"
    )
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "hops" in sql.lower()


def test_length_p_flat_join_returns_1():
    lm = _make_label_map()
    ast = parse_cypher(
        "MATCH p = shortestPath((a:Person)-[:WORKS_AT*..3]->(c:Company)) "
        "WHERE a.name = 'Alice' RETURN length(p)"
    )
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    # flat JOIN path: length(p) → 1
    assert "1" in sql


# ---------------------------------------------------------------------------
# Gap #9 — Pattern comprehensions
# ---------------------------------------------------------------------------

def test_pattern_comprehension_basic():
    lm = _make_label_map()
    ast = parse_cypher(
        "MATCH (p:Person) RETURN [(p)-[:WORKS_AT]->(c:Company) | c.name] AS companies"
    )
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    sql_upper = sql.upper()
    assert "ARRAY" in sql_upper
    assert "SELECT" in sql_upper
    assert "companies" in sql.lower()


def test_pattern_comprehension_in_where():
    lm = _make_label_map()
    ast = parse_cypher(
        "MATCH (p:Person) WHERE size([(p)-[:WORKS_AT]->(c:Company) | c.name]) > 0 RETURN p.name"
    )
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    sql_upper = sql.upper()
    assert "ARRAY" in sql_upper
    assert "SELECT" in sql_upper
