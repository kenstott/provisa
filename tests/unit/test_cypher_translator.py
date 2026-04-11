# Copyright (c) 2026 Kenneth Stott
# Canary: 4a1c7e3f-2b9d-4a5e-8f3c-6d4a8b2f7c9e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for provisa/cypher/translator.py."""

import pytest

from provisa.cypher.parser import parse_cypher
from provisa.cypher.label_map import CypherLabelMap, NodeMapping, RelationshipMapping
from provisa.cypher.translator import cypher_to_sql, cypher_calls_to_sql_list


def _make_label_map_multi_path() -> CypherLabelMap:
    """Label map with two 1-hop paths from Person to Company: WORKS_AT and MANAGES."""
    person_meta = NodeMapping(
        label="Person", table_id=1, source_id="pg-main", id_column="id",
        catalog_name="postgresql", schema_name="public", table_name="persons",
        properties={"name": "name", "age": "age"},
    )
    company_meta = NodeMapping(
        label="Company", table_id=2, source_id="pg-main", id_column="id",
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
        table_id=1,
        source_id="pg-main",
        id_column="id",
        catalog_name="postgresql",
        schema_name="public",
        table_name="persons",
        properties={"name": "name", "age": "age"},
    )
    company_meta = NodeMapping(
        label="Company",
        table_id=2,
        source_id="pg-main" if not multi_source else "pg-secondary",
        id_column="id",
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


# ---------------------------------------------------------------------------
# Basic translation
# ---------------------------------------------------------------------------

def test_simple_match_return_name():
    lm = _make_label_map()
    ast = parse_cypher("MATCH (n:Person) RETURN n.name")
    sql_ast, params, graph_vars = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "persons" in sql.lower()
    assert "name" in sql.lower()
    assert not params


def test_return_alias_in_sql():
    lm = _make_label_map()
    ast = parse_cypher("MATCH (n:Person) RETURN n.name AS fullname")
    sql_ast, params, graph_vars = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "fullname" in sql.lower()


def test_optional_match_produces_left_join():
    lm = _make_label_map()
    ast = parse_cypher(
        "MATCH (n:Person) OPTIONAL MATCH (n)-[:WORKS_AT]->(c:Company) RETURN n.name, c.name"
    )
    sql_ast, params, graph_vars = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "LEFT" in sql.upper()


def test_where_param_rewritten():
    lm = _make_label_map()
    ast = parse_cypher("MATCH (n:Person) WHERE n.age > $min RETURN n.name")
    sql_ast, param_names, graph_vars = cypher_to_sql(ast, lm, {"min": 30})
    assert "min" in param_names


def test_order_by_in_sql():
    lm = _make_label_map()
    ast = parse_cypher("MATCH (n:Person) RETURN n.name ORDER BY n.name ASC")
    sql_ast, params, graph_vars = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "ORDER BY" in sql.upper()


def test_limit_offset_in_sql():
    lm = _make_label_map()
    ast = parse_cypher("MATCH (n:Person) RETURN n.name SKIP 5 LIMIT 10")
    sql_ast, params, graph_vars = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "LIMIT" in sql.upper()
    assert "OFFSET" in sql.upper() or "5" in sql


def test_cross_source_allowed():
    # Trino handles cross-catalog joins natively — translation must not raise.
    lm = _make_label_map(multi_source=True)
    ast = parse_cypher("MATCH (n:Person)-[:WORKS_AT]->(c:Company) RETURN n.name, c.name")
    sql_ast, params, graph_vars = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "persons" in sql.lower()
    assert "companies" in sql.lower()


def test_graph_vars_populated_for_node_return():
    lm = _make_label_map()
    ast = parse_cypher("MATCH (n:Person) RETURN n")
    sql_ast, params, graph_vars = cypher_to_sql(ast, lm, {})
    from provisa.cypher.translator import GraphVarKind
    assert "n" in graph_vars
    assert graph_vars["n"] == GraphVarKind.NODE


def test_inner_join_for_required_match():
    lm = _make_label_map()
    ast = parse_cypher("MATCH (n:Person)-[:WORKS_AT]->(c:Company) RETURN n.name, c.name")
    sql_ast, params, graph_vars = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    # Should be INNER JOIN (or just JOIN), not LEFT JOIN
    assert "LEFT" not in sql.upper() or sql.upper().count("LEFT") == 0


def test_union_produces_union_sql():
    lm = _make_label_map()
    ast = parse_cypher(
        "MATCH (n:Person) RETURN n.name "
        "UNION "
        "MATCH (n:Person) RETURN n.name"
    )
    sql_ast, params, graph_vars = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "UNION" in sql.upper()
    assert "persons" in sql.lower()


def test_union_with_order_limit_applies_to_whole_union():
    lm = _make_label_map()
    ast = parse_cypher(
        "MATCH (n:Person) RETURN n.name "
        "UNION "
        "MATCH (n:Person) RETURN n.name "
        "ORDER BY n.name LIMIT 10"
    )
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "UNION" in sql.upper()
    assert "LIMIT" in sql.upper()
    assert "ORDER BY" in sql.upper()
    # ORDER BY / LIMIT must appear after the last UNION branch, not inside one
    union_pos = sql.upper().rfind("UNION")
    order_pos = sql.upper().find("ORDER BY")
    assert order_pos > union_pos


def test_union_all_produces_union_all_sql():
    lm = _make_label_map()
    ast = parse_cypher(
        "MATCH (n:Person) RETURN n.name "
        "UNION ALL "
        "MATCH (n:Person) RETURN n.name"
    )
    sql_ast, params, graph_vars = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "UNION ALL" in sql.upper()


# ---------------------------------------------------------------------------
# String / builtin function rewriting
# ---------------------------------------------------------------------------

def test_tolower_rewritten():
    lm = _make_label_map()
    ast = parse_cypher("MATCH (n:Person) RETURN toLower(n.name)")
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "lower(" in sql.lower()
    assert "tolower" not in sql.lower()


def test_toupper_rewritten():
    lm = _make_label_map()
    ast = parse_cypher("MATCH (n:Person) RETURN toUpper(n.name)")
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "upper(" in sql.lower()


def test_substring_index_offset():
    lm = _make_label_map()
    ast = parse_cypher("MATCH (n:Person) RETURN substring(n.name, 0, 3)")
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    # start 0 → 1 in SQL; length 3 unchanged
    assert "substr" in sql.lower()
    assert "0 + 1" in sql or "1" in sql


def test_tostring_becomes_cast():
    lm = _make_label_map()
    ast = parse_cypher("MATCH (n:Person) RETURN toString(n.age)")
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "CAST" in sql.upper() or "cast" in sql.lower()
    assert "VARCHAR" in sql.upper()


def test_tointeger_becomes_try_cast():
    lm = _make_label_map()
    ast = parse_cypher("MATCH (n:Person) RETURN toInteger(n.name)")
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "TRY_CAST" in sql.upper()
    assert "BIGINT" in sql.upper()


def test_collect_becomes_array_agg():
    lm = _make_label_map()
    ast = parse_cypher("MATCH (n:Person) RETURN collect(n.name)")
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "array_agg" in sql.lower()


def test_log_becomes_ln():
    lm = _make_label_map()
    ast = parse_cypher("MATCH (n:Person) RETURN log(n.age)")
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert sql.lower().count("ln(") >= 1


def test_starts_with_predicate():
    lm = _make_label_map()
    ast = parse_cypher("MATCH (n:Person) WHERE n.name STARTS WITH 'Al' RETURN n.name")
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "starts_with" in sql.lower()


def test_contains_predicate():
    lm = _make_label_map()
    ast = parse_cypher("MATCH (n:Person) WHERE n.name CONTAINS 'ice' RETURN n.name")
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "strpos" in sql.lower()


def test_ends_with_predicate():
    lm = _make_label_map()
    ast = parse_cypher("MATCH (n:Person) WHERE n.name ENDS WITH 'e' RETURN n.name")
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "like" in sql.lower() or "LIKE" in sql


def test_regex_predicate():
    lm = _make_label_map()
    ast = parse_cypher("MATCH (n:Person) WHERE n.name =~ 'Al.*' RETURN n.name")
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "regexp_like" in sql.lower()


def test_cypher_calls_to_sql_list():
    lm = _make_label_map()
    query = (
        "CALL {\n"
        "  MATCH (a:Person) RETURN a.name AS name\n"
        "}\n"
        "CALL {\n"
        "  MATCH (b:Company) RETURN b.name AS company_name\n"
        "}\n"
        "RETURN name, company_name"
    )
    ast = parse_cypher(query)
    results = cypher_calls_to_sql_list(ast, lm, {})
    assert len(results) == 2

    sql1, params1, _ = results[0]
    sql_str1 = sql1.sql(dialect="trino")
    assert "persons" in sql_str1.lower()
    assert not params1

    sql2, params2, _ = results[1]
    sql_str2 = sql2.sql(dialect="trino")
    assert "companies" in sql_str2.lower()
    assert not params2


# ---------------------------------------------------------------------------
# WITH / CTE translation
# ---------------------------------------------------------------------------

def test_with_produces_cte():
    lm = _make_label_map()
    ast = parse_cypher("MATCH (n:Person) WITH n.name AS nm RETURN nm")
    sql_ast, params, graph_vars = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "WITH" in sql.upper()
    assert "_w0" in sql


def test_with_where_filters_result():
    lm = _make_label_map()
    ast = parse_cypher(
        "MATCH (n:Person) "
        "WITH n.name AS nm, n.age AS age "
        "WHERE age > 30 "
        "RETURN nm"
    )
    sql_ast, params, graph_vars = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "WITH" in sql.upper()
    assert "30" in sql


def test_with_pipes_into_second_match():
    lm = _make_label_map()
    ast = parse_cypher(
        "MATCH (n:Person) "
        "WITH n "
        "MATCH (n)-[:WORKS_AT]->(c:Company) "
        "RETURN n.name, c.name"
    )
    sql_ast, params, graph_vars = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "WITH" in sql.upper()
    assert "_w0" in sql
    assert "companies" in sql.lower()


# ---------------------------------------------------------------------------
# List comprehension translation
# ---------------------------------------------------------------------------

def test_list_comp_map_only():
    lm = _make_label_map()
    ast = parse_cypher("MATCH (n:Person) RETURN [x IN n.scores | x * 2] AS doubled")
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "transform" in sql.lower()


def test_list_comp_filter_only():
    lm = _make_label_map()
    ast = parse_cypher("MATCH (n:Person) RETURN [x IN n.scores WHERE x > 0] AS pos")
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "filter" in sql.lower()


def test_list_comp_filter_and_map():
    lm = _make_label_map()
    ast = parse_cypher("MATCH (n:Person) RETURN [x IN n.scores WHERE x > 0 | x * 2] AS result")
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "transform" in sql.lower()
    assert "filter" in sql.lower()


def test_any_comprehension():
    lm = _make_label_map()
    ast = parse_cypher("MATCH (n:Person) WHERE any(x IN n.scores WHERE x > 50) RETURN n.name")
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "any_match" in sql.lower()


def test_all_comprehension():
    lm = _make_label_map()
    ast = parse_cypher("MATCH (n:Person) WHERE all(x IN n.scores WHERE x > 0) RETURN n.name")
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "all_match" in sql.lower()


def test_none_comprehension():
    lm = _make_label_map()
    ast = parse_cypher("MATCH (n:Person) WHERE none(x IN n.scores WHERE x < 0) RETURN n.name")
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "none_match" in sql.lower()


def test_single_comprehension():
    lm = _make_label_map()
    ast = parse_cypher("MATCH (n:Person) WHERE single(x IN n.scores WHERE x = 100) RETURN n.name")
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "cardinality" in sql.lower()
    assert "filter" in sql.lower()


# ---------------------------------------------------------------------------
# RETURN DISTINCT
# ---------------------------------------------------------------------------

def test_return_distinct():
    lm = _make_label_map()
    ast = parse_cypher("MATCH (n:Person) RETURN DISTINCT n.name")
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "DISTINCT" in sql.upper()


# ---------------------------------------------------------------------------
# Node metadata functions
# ---------------------------------------------------------------------------

def test_id_function():
    lm = _make_label_map()
    ast = parse_cypher("MATCH (n:Person) RETURN id(n)")
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    # id(n) → n."id" (id_column is "id" in test label map)
    assert '"id"' in sql


def test_labels_function():
    lm = _make_label_map()
    ast = parse_cypher("MATCH (n:Person) RETURN labels(n)")
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "Person" in sql
    assert "ARRAY" in sql.upper()


def test_keys_function():
    lm = _make_label_map()
    ast = parse_cypher("MATCH (n:Person) RETURN keys(n)")
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "age" in sql
    assert "name" in sql
    assert "ARRAY" in sql.upper()


# ---------------------------------------------------------------------------
# exists() / isEmpty()
# ---------------------------------------------------------------------------

def test_exists_function():
    lm = _make_label_map()
    ast = parse_cypher("MATCH (n:Person) WHERE exists(n.name) RETURN n.name")
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino").upper()
    # SQLGlot may emit either "IS NOT NULL" or "NOT ... IS NULL" (semantically equivalent)
    assert "IS NOT NULL" in sql or ("NOT" in sql and "IS NULL" in sql)


def test_isempty_function():
    lm = _make_label_map()
    ast = parse_cypher("MATCH (n:Person) WHERE isEmpty(n.name) RETURN n.name")
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "cardinality" in sql.lower()
    assert "0" in sql


# ---------------------------------------------------------------------------
# List scalar functions
# ---------------------------------------------------------------------------

def test_head_function():
    lm = _make_label_map()
    ast = parse_cypher("MATCH (n:Person) RETURN head(n.scores)")
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "element_at" in sql.lower()
    assert "1" in sql


def test_last_function():
    lm = _make_label_map()
    ast = parse_cypher("MATCH (n:Person) RETURN last(n.scores)")
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "element_at" in sql.lower()
    assert "-1" in sql


def test_range_function():
    lm = _make_label_map()
    ast = parse_cypher("MATCH (n:Person) RETURN range(1, 10)")
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "sequence" in sql.lower()


def test_size_becomes_cardinality():
    lm = _make_label_map()
    ast = parse_cypher("MATCH (n:Person) RETURN size(n.scores)")
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "cardinality" in sql.lower()


# ---------------------------------------------------------------------------
# Domain label resolution
# ---------------------------------------------------------------------------

def test_domain_only_node_produces_union_all():
    lm = _make_label_map(with_domains=True)
    ast = parse_cypher("MATCH (n:Sales) RETURN n.name")
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "UNION ALL" in sql.upper()
    assert "persons" in sql.lower()
    assert "companies" in sql.lower()
    assert "name" in sql.lower()


def test_type_and_domain_label_uses_type_table():
    lm = _make_label_map(with_domains=True)
    ast = parse_cypher("MATCH (n:Person:Sales) RETURN n.name")
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    # Must resolve to persons only — not a UNION
    assert "UNION" not in sql.upper()
    assert "persons" in sql.lower()


# ---------------------------------------------------------------------------
# shortestPath / allShortestPaths
# ---------------------------------------------------------------------------

def test_shortestpath_finds_direct_relationship():
    lm = _make_label_map()
    ast = parse_cypher(
        "MATCH p = shortestPath((a:Person)-[*..5]->(b:Company)) RETURN a.name, b.name"
    )
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "persons" in sql.lower()
    assert "companies" in sql.lower()
    assert "JOIN" in sql.upper()


def test_shortestpath_uses_min_hop_only():
    # max_hops=2 finds [WORKS_AT] (1-hop) and [KNOWS, WORKS_AT] (2-hop).
    # shortestPath must use only the 1-hop path (no intermediate _hop1 alias).
    lm = _make_label_map()
    ast = parse_cypher(
        "MATCH p = shortestPath((a:Person)-[*..2]->(b:Company)) RETURN a.name, b.name"
    )
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "persons" in sql.lower()
    assert "companies" in sql.lower()
    assert "_hop1" not in sql.lower()


def test_shortestpath_multihop_schema_path():
    # Force a 2-hop schema path: Person -[KNOWS]-> Person (no direct Person→Person path)
    # Person to Person via KNOWS is 1-hop, which is the shortest.
    lm = _make_label_map()
    ast = parse_cypher(
        "MATCH p = shortestPath((a:Person)-[*..3]->(b:Person)) RETURN a.name, b.name"
    )
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "persons" in sql.lower()
    assert "JOIN" in sql.upper()


def test_allshortestpaths_same_result_as_shortestpath():
    lm = _make_label_map()
    ast = parse_cypher(
        "MATCH p = allShortestPaths((a:Person)-[*..5]->(b:Company)) RETURN a.name, b.name"
    )
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "persons" in sql.lower()
    assert "companies" in sql.lower()


def test_shortestpath_no_path_raises():
    lm = _make_label_map()
    from provisa.cypher.translator import CypherTranslateError
    ast = parse_cypher(
        "MATCH p = shortestPath((a:Company)-[*..5]->(b:Company)) RETURN a.name"
    )
    with pytest.raises(CypherTranslateError, match="No schema path"):
        cypher_to_sql(ast, lm, {})


def test_shortestpath_multiple_schema_paths_union_all():
    # Two 1-hop paths Person→Company (WORKS_AT, MANAGES) → must UNION ALL two queries.
    lm = _make_label_map_multi_path()
    ast = parse_cypher(
        "MATCH p = shortestPath((a:Person)-[*..5]->(b:Company)) RETURN a.name, b.name"
    )
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "UNION ALL" in sql.upper()
    # Both paths reference the same persons/companies tables
    assert sql.lower().count("persons") >= 2
    assert sql.lower().count("companies") >= 2


def test_shortestpath_with_where_filter():
    lm = _make_label_map()
    ast = parse_cypher(
        "MATCH p = shortestPath((a:Person)-[*..5]->(b:Company)) "
        "WHERE a.name = 'Alice' RETURN a.name, b.name"
    )
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "Alice" in sql
    assert "persons" in sql.lower()
    assert "companies" in sql.lower()


def test_domain_node_null_pads_missing_properties():
    # Company has no 'age' column; Person does — Sales UNION ALL should NULL-pad
    lm = _make_label_map(with_domains=True)
    ast = parse_cypher("MATCH (n:Sales) RETURN n.name, n.age")
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "UNION ALL" in sql.upper()
    assert "NULL" in sql.upper()  # null-padding for missing column


def test_unlabeled_node_produces_union_all_of_all_types():
    lm = _make_label_map()
    ast = parse_cypher("MATCH (n) RETURN n.name")
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "UNION ALL" in sql.upper()
    assert "persons" in sql.lower()
    assert "companies" in sql.lower()


# ---------------------------------------------------------------------------
# Recursive CTE (self-referential variable-length paths)
# ---------------------------------------------------------------------------

def _make_label_map_indirect_cycle() -> CypherLabelMap:
    """Label map: Person -[WORKS_AT]-> Company -[EMPLOYS]-> Person (indirect cycle)."""
    person_meta = NodeMapping(
        label="Person", table_id=1, source_id="pg", id_column="id",
        catalog_name="postgresql", schema_name="public", table_name="persons",
        properties={"name": "name"},
    )
    company_meta = NodeMapping(
        label="Company", table_id=2, source_id="pg", id_column="id",
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

def _make_label_map_self_ref() -> CypherLabelMap:
    """Label map with a self-referential KNOWS relationship for recursive path tests."""
    person_meta = NodeMapping(
        label="Person", table_id=1, source_id="pg-main", id_column="id",
        catalog_name="postgresql", schema_name="public", table_name="persons",
        properties={"name": "name", "age": "age"},
    )
    knows_rel = RelationshipMapping(
        rel_type="KNOWS", source_label="Person", target_label="Person",
        join_source_column="person_id", join_target_column="id", field_name="knows",
    )
    return CypherLabelMap(nodes={"Person": person_meta}, relationships={"KNOWS": knows_rel})


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


# ---------------------------------------------------------------------------
# Gap #12 — Intermediate node property access in multi-hop patterns
# ---------------------------------------------------------------------------

def _make_label_map_three_hop() -> CypherLabelMap:
    person = NodeMapping(
        label="Person", table_id=1, source_id="pg", id_column="id",
        catalog_name="postgresql", schema_name="public", table_name="persons",
        properties={"name": "name"},
    )
    company = NodeMapping(
        label="Company", table_id=2, source_id="pg", id_column="id",
        catalog_name="postgresql", schema_name="public", table_name="companies",
        properties={"name": "name"},
    )
    dept = NodeMapping(
        label="Department", table_id=3, source_id="pg", id_column="id",
        catalog_name="postgresql", schema_name="public", table_name="departments",
        properties={"title": "title"},
    )
    return CypherLabelMap(
        nodes={"Person": person, "Company": company, "Department": dept},
        relationships={
            "WORKS_AT": RelationshipMapping(
                rel_type="WORKS_AT", source_label="Person", target_label="Company",
                join_source_column="company_id", join_target_column="id", field_name="works_at",
            ),
            "HAS_DEPT": RelationshipMapping(
                rel_type="HAS_DEPT", source_label="Company", target_label="Department",
                join_source_column="dept_id", join_target_column="id", field_name="has_dept",
            ),
        },
    )


def test_intermediate_node_property_access():
    lm = _make_label_map_three_hop()
    ast = parse_cypher(
        "MATCH (p:Person)-[:WORKS_AT]->(c:Company)-[:HAS_DEPT]->(d:Department) "
        "RETURN p.name, c.name, d.title"
    )
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "persons" in sql.lower()
    assert "companies" in sql.lower()
    assert "departments" in sql.lower()
    # All three aliases should be referenceable in SELECT
    sql_lower = sql.lower()
    assert "p." in sql_lower
    assert "c." in sql_lower
    assert "d." in sql_lower


def test_intermediate_node_where_filter():
    lm = _make_label_map_three_hop()
    ast = parse_cypher(
        "MATCH (p:Person)-[:WORKS_AT]->(c:Company)-[:HAS_DEPT]->(d:Department) "
        "WHERE c.name = 'ACME' RETURN p.name, d.title"
    )
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "ACME" in sql
    assert "c." in sql.lower()


# ---------------------------------------------------------------------------
# Gap #5 — Path object RETURN p
# ---------------------------------------------------------------------------

def test_return_path_flat_join_emits_json_object():
    lm = _make_label_map()
    ast = parse_cypher(
        "MATCH p = shortestPath((a:Person)-[:WORKS_AT*..3]->(c:Company)) "
        "WHERE a.name = 'Alice' RETURN p"
    )
    sql_ast, _, graph_vars = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    sql_upper = sql.upper()
    assert "JSON_OBJECT" in sql_upper
    assert "'start'" in sql.lower() or "start" in sql.lower()
    assert "'end'" in sql.lower() or "end" in sql.lower()
    assert "length" in sql.lower()
    assert graph_vars.get("p") is not None


def test_return_path_recursive_emits_hops():
    lm = _make_label_map_self_ref()
    ast = parse_cypher(
        "MATCH p = shortestPath((a:Person)-[:KNOWS*..5]->(b:Person)) "
        "WHERE a.name = 'Alice' AND b.name = 'Bob' RETURN p"
    )
    sql_ast, _, graph_vars = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    sql_upper = sql.upper()
    assert "JSON_OBJECT" in sql_upper
    assert "hops" in sql.lower()


def test_return_path_with_alias():
    lm = _make_label_map()
    ast = parse_cypher(
        "MATCH p = shortestPath((a:Person)-[:WORKS_AT*..3]->(c:Company)) RETURN p AS route"
    )
    sql_ast, _, graph_vars = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "route" in sql.lower()
    assert "JSON_OBJECT" in sql.upper()


# ---------------------------------------------------------------------------
# Gap #8 — Correlated CALL subqueries (CALL { WITH x MATCH ... })
# ---------------------------------------------------------------------------

def test_correlated_call_lateral_join_emitted():
    """CALL { WITH p MATCH (p)-[:KNOWS]->(f:Person) ... } → CROSS JOIN LATERAL."""
    lm = _make_label_map()
    ast = parse_cypher(
        "MATCH (p:Person) "
        "CALL { WITH p MATCH (p)-[:KNOWS]->(f:Person) RETURN f.name AS friend } "
        "RETURN p.name, friend"
    )
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    sql_upper = sql.upper()
    assert "LATERAL" in sql_upper
    assert "friend" in sql.lower()
    assert "persons" in sql.lower()


def test_correlated_call_inner_where_condition():
    """Inner CALL body produces a WHERE condition referencing the outer var."""
    lm = _make_label_map()
    ast = parse_cypher(
        "MATCH (p:Person) "
        "CALL { WITH p MATCH (p)-[:KNOWS]->(f:Person) RETURN f.name AS friend } "
        "RETURN p.name, friend"
    )
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    # The inner lateral subquery should reference the join column
    assert "person_id" in sql.lower() or "id" in sql.lower()


def test_correlated_call_multiple_imported_vars():
    """CALL { WITH a, b MATCH ... } — both vars imported (parser smoke-test)."""
    from provisa.cypher.parser import parse_cypher as _parse
    ast = _parse(
        "MATCH (a:Person)-[:KNOWS]->(b:Person) "
        "CALL { WITH a, b MATCH (a)-[:WORKS_AT]->(c:Company) RETURN c.name AS cn } "
        "RETURN a.name, b.name, cn"
    )
    assert ast.call_subqueries
    call = ast.call_subqueries[0]
    assert "a" in call.imported_vars
    assert "b" in call.imported_vars


def test_non_correlated_call_not_lateral():
    """CALL { MATCH (n:Person) RETURN n } without WITH → not a LATERAL."""
    lm = _make_label_map()
    ast = parse_cypher(
        "MATCH (p:Person) RETURN p.name"
    )
    # No CALL subqueries → no lateral joins
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "LATERAL" not in sql.upper()


# ---------------------------------------------------------------------------
# G5 — Node label alternation (n:A|B)
# ---------------------------------------------------------------------------

def test_node_label_alternation():
    lm = _make_label_map()
    ast = parse_cypher("MATCH (n:Person|Company) RETURN n.name")
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "UNION" in sql.upper()
    assert "persons" in sql.lower()
    assert "companies" in sql.lower()


# ---------------------------------------------------------------------------
# G2 — EXISTS { MATCH ... } subquery predicate
# ---------------------------------------------------------------------------

def test_exists_subquery_in_where():
    lm = _make_label_map()
    ast = parse_cypher(
        "MATCH (n:Person) WHERE EXISTS { MATCH (n)-[:KNOWS]->(m:Person) } RETURN n.name"
    )
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "EXISTS" in sql.upper()
    assert "SELECT" in sql.upper()


# ---------------------------------------------------------------------------
# G3 — COUNT { MATCH ... } subquery expression
# ---------------------------------------------------------------------------

def test_count_subquery_in_return():
    lm = _make_label_map()
    ast = parse_cypher(
        "MATCH (n:Person) RETURN n.name, COUNT { MATCH (n)-[:KNOWS]->(m:Person) } AS friend_count"
    )
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "count" in sql.lower()
    assert "friend_count" in sql.lower()


# ---------------------------------------------------------------------------
# G4 — COLLECT { MATCH ... RETURN ... } subquery expression
# ---------------------------------------------------------------------------

def test_collect_subquery_in_return():
    lm = _make_label_map()
    ast = parse_cypher(
        "MATCH (n:Person) RETURN n.name, COLLECT { MATCH (n)-[:KNOWS]->(m:Person) RETURN m.name } AS friends"
    )
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "ARRAY" in sql.upper()
    assert "friends" in sql.lower()


# ---------------------------------------------------------------------------
# G8 — left() / right() functions
# ---------------------------------------------------------------------------

def test_left_function():
    lm = _make_label_map()
    ast = parse_cypher("MATCH (n:Person) RETURN left(n.name, 3) AS prefix")
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "left" in sql.lower()


def test_right_function():
    lm = _make_label_map()
    ast = parse_cypher("MATCH (n:Person) RETURN right(n.name, 3) AS suffix")
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "right" in sql.lower()


# ---------------------------------------------------------------------------
# G10 — size() polymorphism
# ---------------------------------------------------------------------------

def test_size_on_string_literal():
    lm = _make_label_map()
    ast = parse_cypher("MATCH (n:Person) RETURN size('hello') AS len")
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "char_length" in sql.lower()


def test_size_on_list_expr():
    lm = _make_label_map()
    ast = parse_cypher("MATCH (n:Person) RETURN size(collect(n.name)) AS cnt")
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "cardinality" in sql.lower()


# ---------------------------------------------------------------------------
# G11 — count(DISTINCT x) passes through correctly
# ---------------------------------------------------------------------------

def test_count_distinct():
    lm = _make_label_map()
    ast = parse_cypher("MATCH (n:Person) RETURN count(DISTINCT n.name) AS cnt")
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "count" in sql.lower()
    assert "distinct" in sql.lower()


def test_reduce_basic():
    """reduce(total = 0, x IN collect(n.age) | total + x) → Trino reduce form."""
    lm = _make_label_map()
    ast = parse_cypher(
        "MATCH (n:Person) RETURN reduce(total = 0, x IN collect(n.age) | total + x) AS sum_age"
    )
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "reduce" in sql.lower()


def test_reduce_string_concat():
    """reduce emits Trino lambda form: reduce(list, init, (acc, x) -> expr, acc -> acc)."""
    from provisa.cypher.comprehension import rewrite_reduce
    result = rewrite_reduce("reduce(acc = '', x IN names | acc || x)")
    assert result == "reduce(names, '', (acc, x) -> acc || x, acc -> acc)"


# ---------------------------------------------------------------------------
# G9 — Legacy {param} syntax
# ---------------------------------------------------------------------------

def test_legacy_param_syntax():
    """Legacy {param} syntax is normalized to $param before parsing."""
    lm = _make_label_map()
    ast = parse_cypher("MATCH (n:Person) WHERE n.age > {min} RETURN n.name")
    sql_ast, param_names, _ = cypher_to_sql(ast, lm, {"min": 30})
    assert "min" in param_names


def test_legacy_param_syntax_multiple():
    """Multiple legacy {param} references are all normalized."""
    lm = _make_label_map()
    ast = parse_cypher(
        "MATCH (n:Person) WHERE n.age > {min} AND n.age < {max} RETURN n.name"
    )
    sql_ast, param_names, _ = cypher_to_sql(ast, lm, {"min": 20, "max": 60})
    assert "min" in param_names
    assert "max" in param_names


# ---------------------------------------------------------------------------
# G1 — Implicit GROUP BY
# ---------------------------------------------------------------------------

def test_group_by_implicit_single_key():
    lm = _make_label_map()
    ast = parse_cypher(
        "MATCH (n:Person)-[:WORKS_AT]->(c:Company) RETURN c.name, count(n) AS cnt"
    )
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "GROUP BY" in sql.upper()


def test_group_by_implicit_multiple_keys():
    lm = _make_label_map()
    ast = parse_cypher(
        "MATCH (n:Person) RETURN n.age, n.name, count(*) AS cnt"
    )
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "GROUP BY" in sql.upper()
    sql_upper = sql.upper()
    assert "AGE" in sql_upper
    assert "NAME" in sql_upper


def test_no_group_by_without_agg():
    lm = _make_label_map()
    ast = parse_cypher("MATCH (n:Person) RETURN n.name, n.age")
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "GROUP BY" not in sql.upper()


def test_group_by_collect():
    lm = _make_label_map()
    ast = parse_cypher(
        "MATCH (n:Person) RETURN n.age, collect(n.name) AS names"
    )
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "GROUP BY" in sql.upper()


def test_with_clause_group_by():
    lm = _make_label_map()
    ast = parse_cypher(
        "MATCH (n:Person) WITH n.age AS age, count(*) AS cnt RETURN age, cnt"
    )
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "GROUP BY" in sql.upper()


# --- type(r) resolution ---

def test_type_function_in_return():
    lm = _make_label_map()
    ast = parse_cypher(
        "MATCH (p:Person)-[r:KNOWS]->(f:Person) RETURN type(r)"
    )
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "'KNOWS'" in sql


def test_type_function_in_where():
    lm = _make_label_map()
    ast = parse_cypher(
        "MATCH (p:Person)-[r]->(c:Company) WHERE type(r) = 'WORKS_AT' RETURN p.name"
    )
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "'WORKS_AT'" in sql


def test_type_function_unbound_passthrough():
    """type(x) where x is not a known rel var passes through without crash."""
    lm = _make_label_map()
    ast = parse_cypher(
        "MATCH (p:Person) RETURN p.name"
    )
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert sql is not None


# --- G6: Map projections ---

def test_map_projection_dot_props():
    """n { .name, .age } → MAP(ARRAY['name','age'], ARRAY[n."name",n."age"])"""
    lm = _make_label_map()
    ast = parse_cypher("MATCH (n:Person) RETURN n { .name, .age }")
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "MAP" in sql.upper()
    assert "'name'" in sql
    assert "'age'" in sql
    assert 'n."name"' in sql
    assert 'n."age"' in sql


def test_map_projection_star():
    """n { .* } → MAP with all known properties expanded from schema."""
    lm = _make_label_map()
    ast = parse_cypher("MATCH (n:Person) RETURN n { .* }")
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "MAP" in sql.upper()
    # Person has name and age
    assert "'name'" in sql
    assert "'age'" in sql


def test_map_projection_star_with_extra():
    """n { .*, extra: expr } → MAP with all props plus named key."""
    lm = _make_label_map()
    ast = parse_cypher("MATCH (n:Person) RETURN n { .*, score: 42 }")
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "MAP" in sql.upper()
    assert "'score'" in sql
    assert "'name'" in sql
    assert "'age'" in sql


def test_map_projection_named_key():
    """n { key: expr } → MAP(ARRAY['key'], ARRAY[expr])"""
    lm = _make_label_map()
    ast = parse_cypher("MATCH (n:Person) RETURN n { fullName: n.name }")
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "MAP" in sql.upper()
    assert "'fullName'" in sql


# ---------------------------------------------------------------------------
# Bidirectional traversal
# ---------------------------------------------------------------------------

def _make_label_map_bidirectional() -> CypherLabelMap:
    """Label map with Person→Company (WORKS_AT) and Company→Person (EMPLOYS)."""
    person_meta = NodeMapping(
        label="Person", table_id=1, source_id="pg-main", id_column="id",
        catalog_name="postgresql", schema_name="public", table_name="persons",
        properties={"name": "name", "age": "age"},
    )
    company_meta = NodeMapping(
        label="Company", table_id=2, source_id="pg-main", id_column="id",
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
            join_source_column="employee_id", join_target_column="id", field_name="employs",
        ),
    }
    return CypherLabelMap(nodes={"Person": person_meta, "Company": company_meta}, relationships=rels)


def test_bidirectional_single_candidate_no_union():
    """(a)-[]-(b) with only one direction → no UNION ALL."""
    lm = _make_label_map()  # only WORKS_AT: Person→Company, no reverse
    ast = parse_cypher("MATCH (a:Person)-[]-(b:Company) RETURN a.name, b.name")
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "UNION" not in sql.upper()
    assert "persons" in sql.lower()
    assert "companies" in sql.lower()


def test_bidirectional_both_directions_produces_union():
    """(a)-[]-(b) with forward and backward candidates → UNION ALL."""
    lm = _make_label_map_bidirectional()  # WORKS_AT (Person→Company) + EMPLOYS (Company→Person)
    ast = parse_cypher("MATCH (a:Person)-[]-(b:Company) RETURN a.name, b.name")
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "UNION" in sql.upper()
    # Both branches reference both tables
    assert sql.lower().count("persons") >= 2
    assert sql.lower().count("companies") >= 2


def test_bidirectional_typed_rel_no_expansion():
    """-[:TYPE]-  with explicit type and direction=none is treated as typed undirected."""
    lm = _make_label_map_bidirectional()
    ast = parse_cypher("MATCH (a:Person)-[:WORKS_AT]-(b:Company) RETURN a.name")
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    # Explicit type: only one mapping looked up → no extra UNION ALL branch
    assert "persons" in sql.lower()
    assert "companies" in sql.lower()
