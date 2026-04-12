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
        label="Person", type_name="Person", domain_label=None, table_label="Person",
        table_id=1, source_id="pg-main", id_column="id",
        catalog_name="postgresql", schema_name="public", table_name="persons",
        properties={"name": "name", "age": "age"},
    )
    company_meta = NodeMapping(
        label="Company", type_name="Company", domain_label=None, table_label="Company",
        table_id=2, source_id="pg-main", id_column="id",
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


def test_fully_unlabeled_match_produces_all_rels_union():
    """MATCH (n)-[r]->(m) RETURN n, r, m — all unlabeled; UNION ALL over all rel types."""
    lm = _make_label_map()
    ast = parse_cypher("MATCH (n)-[r]->(m) RETURN n, r, m LIMIT 50")
    sql_ast, _, graph_vars = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    from provisa.cypher.translator import GraphVarKind
    assert graph_vars.get("n") == GraphVarKind.PASSTHROUGH
    assert graph_vars.get("r") == GraphVarKind.PASSTHROUGH
    assert graph_vars.get("m") == GraphVarKind.PASSTHROUGH
    assert "UNION ALL" in sql.upper() or "union all" in sql.lower()
    # Each relationship type's tables should appear
    assert "persons" in sql.lower()
    assert "companies" in sql.lower()
    assert "LIMIT 50" in sql or "limit 50" in sql.lower()


def test_anonymous_nodes_relationship_return_r():
    """MATCH ()-[r:WORKS_AT]->() RETURN r — both endpoints anonymous; infer tables from rel type."""
    lm = _make_label_map()
    ast = parse_cypher("MATCH ()-[r:WORKS_AT]->() RETURN r LIMIT 25")
    sql_ast, cols, graph_vars = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    from provisa.cypher.translator import GraphVarKind
    assert graph_vars.get("r") == GraphVarKind.EDGE
    assert "persons" in sql.lower()
    assert "companies" in sql.lower()
    assert "WORKS_AT" in sql or "works_at" in sql.lower()


def test_anonymous_src_named_tgt_relationship():
    """MATCH ()-[r:WORKS_AT]->(c:Company) RETURN r.join_source_column — anonymous src inferred."""
    lm = _make_label_map()
    ast = parse_cypher("MATCH ()-[:WORKS_AT]->(c:Company) RETURN c.name")
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "persons" in sql.lower()
    assert "companies" in sql.lower()


# ---------------------------------------------------------------------------
# Relationship alias tests (REQ-390, REQ-391)
# ---------------------------------------------------------------------------

def _make_label_map_with_alias() -> CypherLabelMap:
    """Label map where Employee-[WORKS_FOR]->Department uses an alias."""
    employee = NodeMapping(
        label="Employee", type_name="Hr_Employee", domain_label="Hr", table_label="Employee",
        table_id=10, source_id="pg-main", id_column="id",
        catalog_name="postgresql", schema_name="hr", table_name="employees",
        properties={"id": "id", "name": "name", "dept_id": "dept_id"},
    )
    dept = NodeMapping(
        label="Department", type_name="Hr_Department", domain_label="Hr", table_label="Department",
        table_id=11, source_id="pg-main", id_column="id",
        catalog_name="postgresql", schema_name="hr", table_name="departments",
        properties={"id": "id", "name": "name"},
    )
    # WORKS_FOR is the alias; field_name in GraphQL is also "WORKS_FOR" after alias is applied
    rm = RelationshipMapping(
        rel_type="WORKS_FOR",
        source_label="Employee",
        target_label="Department",
        join_source_column="dept_id",
        join_target_column="id",
        field_name="WORKS_FOR",
        alias="WORKS_FOR",
    )
    rels = {"WORKS_FOR": rm}
    aliases = {"WORKS_FOR": [rm]}
    return CypherLabelMap(nodes={"Employee": employee, "Department": dept},
                          relationships=rels, aliases=aliases)


def _make_label_map_shared_alias() -> CypherLabelMap:
    """Two source/target pairs sharing the same alias REPORTS_TO — triggers UNION ALL."""
    emp = NodeMapping(
        label="Employee", type_name="Hr_Employee", domain_label="Hr", table_label="Employee",
        table_id=20, source_id="pg-main", id_column="id",
        catalog_name="postgresql", schema_name="hr", table_name="employees",
        properties={"id": "id", "manager_id": "manager_id"},
    )
    mgr = NodeMapping(
        label="Manager", type_name="Hr_Manager", domain_label="Hr", table_label="Manager",
        table_id=21, source_id="pg-main", id_column="id",
        catalog_name="postgresql", schema_name="hr", table_name="managers",
        properties={"id": "id", "director_id": "director_id"},
    )
    director = NodeMapping(
        label="Director", type_name="Hr_Director", domain_label="Hr", table_label="Director",
        table_id=22, source_id="pg-main", id_column="id",
        catalog_name="postgresql", schema_name="hr", table_name="directors",
        properties={"id": "id"},
    )
    rm1 = RelationshipMapping(
        rel_type="REPORTS_TO", source_label="Employee", target_label="Manager",
        join_source_column="manager_id", join_target_column="id",
        field_name="REPORTS_TO", alias="REPORTS_TO",
    )
    rm2 = RelationshipMapping(
        rel_type="REPORTS_TO", source_label="Manager", target_label="Director",
        join_source_column="director_id", join_target_column="id",
        field_name="REPORTS_TO", alias="REPORTS_TO",
    )
    rels = {"REPORTS_TO": rm2}  # last wins in single dict
    aliases = {"REPORTS_TO": [rm1, rm2]}
    nodes = {"Employee": emp, "Manager": mgr, "Director": director}
    return CypherLabelMap(nodes=nodes, relationships=rels, aliases=aliases)


def test_alias_rel_type_resolves_via_aliases_index():
    """REQ-390: query using alias rel type is resolved correctly."""
    lm = _make_label_map_with_alias()
    ast = parse_cypher("MATCH (e:Employee)-[:WORKS_FOR]->(d:Department) RETURN e.name, d.name")
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "employees" in sql.lower()
    assert "departments" in sql.lower()
    assert "dept_id" in sql.lower()


def test_shared_alias_produces_union_all():
    """REQ-391: alias shared by multiple source/target pairs generates UNION ALL."""
    lm = _make_label_map_shared_alias()
    ast = parse_cypher("MATCH (n)-[:REPORTS_TO]->(m) RETURN n.id, m.id")
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "UNION ALL" in sql.upper()


def test_unknown_rel_type_raises_error():
    """REQ-390: unknown relationship type raises CypherTranslateError."""
    from provisa.cypher.translator import CypherTranslateError
    lm = _make_label_map_with_alias()
    ast = parse_cypher("MATCH (e:Employee)-[:UNKNOWN_REL]->(d:Department) RETURN e.name")
    with pytest.raises(CypherTranslateError, match="Unknown relationship type or alias"):
        cypher_to_sql(ast, lm, {})


def _make_label_map_product_reviews() -> CypherLabelMap:
    """Label map for PRODUCT_CATALOG__PRODUCT_REVIEWS relationship."""
    product = NodeMapping(
        label="PRODUCT_CATALOG__PRODUCTS", type_name="PRODUCT_CATALOG__PRODUCTS",
        domain_label="PRODUCT_CATALOG", table_label="PRODUCTS",
        table_id=30, source_id="pg-main", id_column="product_id",
        catalog_name="postgresql", schema_name="product_catalog", table_name="products",
        properties={"product_id": "product_id", "name": "name"},
    )
    review = NodeMapping(
        label="PRODUCT_CATALOG__PRODUCT_REVIEWS", type_name="PRODUCT_CATALOG__PRODUCT_REVIEWS",
        domain_label="PRODUCT_CATALOG", table_label="PRODUCT_REVIEWS",
        table_id=31, source_id="pg-main", id_column="review_id",
        catalog_name="postgresql", schema_name="product_catalog", table_name="product_reviews",
        properties={"review_id": "review_id", "product_id": "product_id", "rating": "rating"},
    )
    rel = RelationshipMapping(
        rel_type="PRODUCT_CATALOG__PRODUCT_REVIEWS",
        source_label="PRODUCT_CATALOG__PRODUCTS",
        target_label="PRODUCT_CATALOG__PRODUCT_REVIEWS",
        join_source_column="product_id",
        join_target_column="product_id",
        field_name="PRODUCT_CATALOG__PRODUCT_REVIEWS",
        alias="PRODUCT_CATALOG__PRODUCT_REVIEWS",
    )
    nodes = {
        "PRODUCT_CATALOG__PRODUCTS": product,
        "PRODUCT_CATALOG__PRODUCT_REVIEWS": review,
    }
    rels = {"PRODUCT_CATALOG__PRODUCT_REVIEWS": rel}
    aliases = {"PRODUCT_CATALOG__PRODUCT_REVIEWS": [rel]}
    return CypherLabelMap(nodes=nodes, relationships=rels, aliases=aliases)


def test_path_returns_start_end_and_relationship():
    """MATCH p = ()-[r:PRODUCT_CATALOG__PRODUCT_REVIEWS]->() RETURN p LIMIT 25
    should produce SQL joining source and target tables and include both node columns."""
    lm = _make_label_map_product_reviews()
    ast = parse_cypher(
        "MATCH p = ()-[r:PRODUCT_CATALOG__PRODUCT_REVIEWS]->() RETURN p LIMIT 25"
    )
    sql_ast, _, graph_vars = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "products" in sql.lower()
    assert "product_reviews" in sql.lower()
    assert "p" in graph_vars
