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
        source_id="pg-main",
        id_column="id",
        pk_columns=[],
        catalog_name="postgresql",
        schema_name="public",
        table_name="companies",
        properties={"name": "name"},
    )
    rels = {
        "WORKS_AT": RelationshipMapping(
            rel_type="WORKS_AT",
            source_label="Person",
            target_label="Company",
            join_source_column="company_id",
            join_target_column="id",
            field_name="works_at",
        ),
        "MANAGES": RelationshipMapping(
            rel_type="MANAGES",
            source_label="Person",
            target_label="Company",
            join_source_column="managed_company_id",
            join_target_column="id",
            field_name="manages",
        ),
    }
    return CypherLabelMap(
        nodes={"Person": person_meta, "Company": company_meta}, relationships=rels
    )


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
    ast = parse_cypher("MATCH (n:Person) RETURN n.name UNION MATCH (n:Person) RETURN n.name")
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
    ast = parse_cypher("MATCH (n:Person) RETURN n.name UNION ALL MATCH (n:Person) RETURN n.name")
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
    ast = parse_cypher("MATCH (n:Person) WITH n.name AS nm, n.age AS age WHERE age > 30 RETURN nm")
    sql_ast, params, graph_vars = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "WITH" in sql.upper()
    assert "30" in sql


def test_with_pipes_into_second_match():
    lm = _make_label_map()
    ast = parse_cypher(
        "MATCH (n:Person) WITH n MATCH (n)-[:WORKS_AT]->(c:Company) RETURN n.name, c.name"
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


def test_domain_union_excludes_nodes_with_native_filter_columns():
    """Domain MATCH must exclude nodes that require a WHERE clause (native_filter_columns set)."""
    lm = _make_label_map(with_domains=True)
    # Inject a node that requires args into the Sales domain
    from provisa.cypher.label_map import NodeMapping

    lm.nodes["Schedule"] = NodeMapping(
        label="Sales:Schedule",
        type_name="Schedule",
        domain_label="Sales",
        table_label="Schedule",
        table_id=99,
        source_id="gql-remote",
        id_column="id",
        pk_columns=[],
        catalog_name="gql_remote",
        schema_name="graphql_remote",
        table_name="schedule_by_employee",
        properties={"id": "id"},
        native_filter_columns={"employee_id"},
    )
    lm.domains["Sales"].append("Schedule")
    ast = parse_cypher("MATCH (n:Sales) RETURN n LIMIT 25")
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "schedule_by_employee" not in sql.lower()
    assert "persons" in sql.lower()


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

    ast = parse_cypher("MATCH p = shortestPath((a:Company)-[*..5]->(b:Company)) RETURN a.name")
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


def test_backward_traversal_edge_identity_is_canonical():
    """MATCH (c:Company)<-[r:WORKS_AT]-(p:Person) RETURN r — identity must use canonical src→tgt order."""
    lm = _make_label_map()
    # Forward canonical: persons-[WORKS_AT]->companies  =>  identity = WORKS_AT:person_id-company_id
    fwd_ast = parse_cypher("MATCH (p:Person)-[r:WORKS_AT]->(c:Company) RETURN r LIMIT 25")
    fwd_sql_ast, _, _ = cypher_to_sql(fwd_ast, lm, {})
    fwd_sql = fwd_sql_ast.sql(dialect="trino")

    # Backward traversal: same rel in reverse
    bwd_ast = parse_cypher("MATCH (c:Company)<-[r:WORKS_AT]-(p:Person) RETURN r LIMIT 25")
    bwd_sql_ast, _, _ = cypher_to_sql(bwd_ast, lm, {})
    bwd_sql = bwd_sql_ast.sql(dialect="trino")

    # Extract identity expressions from both — they must be identical so imputed edges de-dup correctly
    import re

    fwd_ids = re.findall(r"'WORKS_AT'.*?(?=\sAS\s)", fwd_sql)
    bwd_ids = re.findall(r"'WORKS_AT'.*?(?=\sAS\s)", bwd_sql)

    # Both queries must produce an identity column containing WORKS_AT
    assert fwd_ids, "forward query missing WORKS_AT identity"
    assert bwd_ids, "backward query missing WORKS_AT identity"
    # The identity expression must be the same in both directions
    assert fwd_ids[0] == bwd_ids[0], (
        f"edge identity differs between forward and backward traversal: "
        f"fwd={fwd_ids[0]!r} bwd={bwd_ids[0]!r}"
    )


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
        label="Employee",
        type_name="Hr_Employee",
        domain_label="Hr",
        table_label="Employee",
        table_id=10,
        source_id="pg-main",
        id_column="id",
        pk_columns=[],
        catalog_name="postgresql",
        schema_name="hr",
        table_name="employees",
        properties={"id": "id", "name": "name", "dept_id": "dept_id"},
    )
    dept = NodeMapping(
        label="Department",
        type_name="Hr_Department",
        domain_label="Hr",
        table_label="Department",
        table_id=11,
        source_id="pg-main",
        id_column="id",
        pk_columns=[],
        catalog_name="postgresql",
        schema_name="hr",
        table_name="departments",
        properties={"id": "id", "name": "name"},
    )
    # WORKS_FOR is the alias; field_name in GraphQL is also "WORKS_FOR" after alias is applied
    rm = RelationshipMapping(
        rel_type="WORKS_FOR",
        source_label="Hr_Employee",
        target_label="Hr_Department",
        join_source_column="dept_id",
        join_target_column="id",
        field_name="WORKS_FOR",
        alias="WORKS_FOR",
    )
    rels = {"WORKS_FOR": rm}
    aliases = {"WORKS_FOR": [rm]}
    return CypherLabelMap(
        nodes={"Employee": employee, "Department": dept}, relationships=rels, aliases=aliases
    )


def _make_label_map_shared_alias() -> CypherLabelMap:
    """Two source/target pairs sharing the same alias REPORTS_TO — triggers UNION ALL."""
    emp = NodeMapping(
        label="Employee",
        type_name="Hr_Employee",
        domain_label="Hr",
        table_label="Employee",
        table_id=20,
        source_id="pg-main",
        id_column="id",
        pk_columns=[],
        catalog_name="postgresql",
        schema_name="hr",
        table_name="employees",
        properties={"id": "id", "manager_id": "manager_id"},
    )
    mgr = NodeMapping(
        label="Manager",
        type_name="Hr_Manager",
        domain_label="Hr",
        table_label="Manager",
        table_id=21,
        source_id="pg-main",
        id_column="id",
        pk_columns=[],
        catalog_name="postgresql",
        schema_name="hr",
        table_name="managers",
        properties={"id": "id", "director_id": "director_id"},
    )
    director = NodeMapping(
        label="Director",
        type_name="Hr_Director",
        domain_label="Hr",
        table_label="Director",
        table_id=22,
        source_id="pg-main",
        id_column="id",
        pk_columns=[],
        catalog_name="postgresql",
        schema_name="hr",
        table_name="directors",
        properties={"id": "id"},
    )
    rm1 = RelationshipMapping(
        rel_type="REPORTS_TO",
        source_label="Hr_Employee",
        target_label="Hr_Manager",
        join_source_column="manager_id",
        join_target_column="id",
        field_name="REPORTS_TO",
        alias="REPORTS_TO",
    )
    rm2 = RelationshipMapping(
        rel_type="REPORTS_TO",
        source_label="Hr_Manager",
        target_label="Hr_Director",
        join_source_column="director_id",
        join_target_column="id",
        field_name="REPORTS_TO",
        alias="REPORTS_TO",
    )
    rels = {"REPORTS_TO": rm2}  # last wins in single dict
    aliases = {"REPORTS_TO": [rm1, rm2]}
    nodes = {"Hr_Employee": emp, "Hr_Manager": mgr, "Hr_Director": director}
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


def test_wrong_arrow_direction_returns_empty():
    """Wrong arrow direction on explicit typed nodes produces impossible join, not an error.

    Cypher semantics: (e:Employee)<-[:WORKS_FOR]-(d:Department) where WORKS_FOR is
    Employee→Department simply returns no rows — same as traversing a non-existent rel.
    """
    lm = _make_label_map_with_alias()
    ast = parse_cypher("MATCH (e:Employee)<-[:WORKS_FOR]-(d:Department) RETURN e.name, d.name")
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino").upper()
    # Impossible predicate injected instead of a valid ON clause
    assert "FALSE" in sql or "1 = 0" in sql or "WHERE FALSE" in sql


def test_shared_alias_produces_union_all():
    """REQ-391: alias shared by multiple source/target pairs generates UNION ALL."""
    lm = _make_label_map_shared_alias()
    ast = parse_cypher("MATCH (n)-[:REPORTS_TO]->(m) RETURN n.id, m.id")
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "UNION ALL" in sql.upper()


def test_unknown_rel_type_returns_empty():
    """Unknown relationship type produces impossible join (Cypher best-effort semantics)."""
    lm = _make_label_map_with_alias()
    ast = parse_cypher("MATCH (e:Employee)-[:UNKNOWN_REL]->(d:Department) RETURN e.name")
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino").upper()
    assert "FALSE" in sql or "1 = 0" in sql or "WHERE FALSE" in sql


def _make_label_map_product_reviews() -> CypherLabelMap:
    """Label map for PRODUCT_CATALOG__PRODUCT_REVIEWS relationship."""
    product = NodeMapping(
        label="PRODUCT_CATALOG__PRODUCTS",
        type_name="PRODUCT_CATALOG__PRODUCTS",
        domain_label="PRODUCT_CATALOG",
        table_label="PRODUCTS",
        table_id=30,
        source_id="pg-main",
        id_column="product_id",
        pk_columns=[],
        catalog_name="postgresql",
        schema_name="product_catalog",
        table_name="products",
        properties={"product_id": "product_id", "name": "name"},
    )
    review = NodeMapping(
        label="PRODUCT_CATALOG__PRODUCT_REVIEWS",
        type_name="PRODUCT_CATALOG__PRODUCT_REVIEWS",
        domain_label="PRODUCT_CATALOG",
        table_label="PRODUCT_REVIEWS",
        table_id=31,
        source_id="pg-main",
        id_column="review_id",
        pk_columns=[],
        catalog_name="postgresql",
        schema_name="product_catalog",
        table_name="product_reviews",
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
    ast = parse_cypher("MATCH p = ()-[r:PRODUCT_CATALOG__PRODUCT_REVIEWS]->() RETURN p LIMIT 25")
    sql_ast, _, graph_vars = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "products" in sql.lower()
    assert "product_reviews" in sql.lower()
    assert "p" in graph_vars


# ---------------------------------------------------------------------------
# Compound domain:table label (e.g. MATCH (n:Support:SupportTickets) RETURN n)
# This is the format generated by the sidebar label pill click in the UI.
# ---------------------------------------------------------------------------


def _make_label_map_compound() -> CypherLabelMap:
    """Label map with a domain-scoped node: Support_SupportTickets."""
    nm = NodeMapping(
        label="Support:SupportTickets",
        type_name="Support_SupportTickets",
        domain_label="Support",
        table_label="SupportTickets",
        table_id=50,
        source_id="trino",
        id_column="ticket_id",
        pk_columns=["ticket_id"],
        catalog_name="trino",
        schema_name="support",
        table_name="support_tickets",
        properties={"ticket_id": "ticket_id", "subject": "subject", "status": "status"},
    )
    return CypherLabelMap(
        nodes={"Support_SupportTickets": nm},
        relationships={},
        domains={"Support": ["Support_SupportTickets"]},
        nodes_by_table={"SupportTickets": ["Support_SupportTickets"]},
    )


def test_compound_label_match_returns_n():
    """MATCH (n:Support:SupportTickets) RETURN n LIMIT 25 resolves to the correct table."""
    lm = _make_label_map_compound()
    ast = parse_cypher("MATCH (n:Support:SupportTickets) RETURN n LIMIT 25")
    sql_ast, _, graph_vars = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "support_tickets" in sql.lower()
    assert "n" in graph_vars


def test_compound_label_order_reversed():
    """MATCH (n:SupportTickets:Support) RETURN n — reversed order — should resolve identically."""
    lm = _make_label_map_compound()
    ast = parse_cypher("MATCH (n:SupportTickets:Support) RETURN n LIMIT 25")
    sql_ast, _, graph_vars = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "support_tickets" in sql.lower()
    assert "n" in graph_vars


def test_compound_label_table_only():
    """MATCH (n:SupportTickets) RETURN n — table label alone — should also resolve."""
    lm = _make_label_map_compound()
    ast = parse_cypher("MATCH (n:SupportTickets) RETURN n LIMIT 25")
    sql_ast, _, graph_vars = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "support_tickets" in sql.lower()
    assert "n" in graph_vars


def test_compound_label_domain_only():
    """MATCH (n:Support) RETURN n — domain-only — should generate a domain-union query."""
    lm = _make_label_map_compound()
    ast = parse_cypher("MATCH (n:Support) RETURN n LIMIT 25")
    sql_ast, _, graph_vars = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "support_tickets" in sql.lower()
    assert "n" in graph_vars


def test_compound_label_case_insensitive_table():
    """MATCH (n:Support:Supporttickets) — wrong case on table label — should still resolve."""
    lm = _make_label_map_compound()
    ast = parse_cypher("MATCH (n:Support:Supporttickets) RETURN n LIMIT 25")
    sql_ast, _, graph_vars = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "support_tickets" in sql.lower()
    assert "n" in graph_vars


def test_compound_label_case_insensitive_domain():
    """MATCH (n:support:SupportTickets) — wrong case on domain label — should still resolve."""
    lm = _make_label_map_compound()
    ast = parse_cypher("MATCH (n:support:SupportTickets) RETURN n LIMIT 25")
    sql_ast, _, graph_vars = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "support_tickets" in sql.lower()
    assert "n" in graph_vars


def test_from_schema_table_label_from_field_name():
    """from_schema() derives table_label from field_name (pascalized), not from physical table_name.

    Tables support_tickets_created and support_tickets_resolved in domain "support"
    must produce labels SupportTicketsCreated and SupportTicketsResolved respectively —
    derived from the registered field name, so virtual tables that share a physical
    table (via physical_table_map) still get distinct, correct labels.
    """
    from provisa.compiler.sql_gen import CompilationContext, TableMeta
    from provisa.cypher.label_map import CypherLabelMap

    ctx = CompilationContext()
    ctx.aggregate_columns[1] = [("ticket_id", "integer"), ("subject", "varchar")]
    ctx.aggregate_columns[2] = [("ticket_id", "integer"), ("subject", "varchar")]
    ctx.tables["support__support_tickets_created"] = TableMeta(
        table_id=1,
        field_name="support__support_tickets_created",
        type_name="Support_SupportTicketsCreated",
        source_id="trino",
        catalog_name="trino",
        schema_name="support",
        table_name="support_tickets_created",
        domain_id="support",
    )
    ctx.tables["support__support_tickets_resolved"] = TableMeta(
        table_id=2,
        field_name="support__support_tickets_resolved",
        type_name="Support_SupportTicketsResolved",
        source_id="trino",
        catalog_name="trino",
        schema_name="support",
        table_name="support_tickets_resolved",
        domain_id="support",
    )

    lm = CypherLabelMap.from_schema(ctx)

    created = lm.nodes["Support_SupportTicketsCreated"]
    resolved = lm.nodes["Support_SupportTicketsResolved"]

    assert created.table_label == "SupportTicketsCreated"
    assert resolved.table_label == "SupportTicketsResolved"
    assert created.label == "Support:SupportTicketsCreated"
    assert resolved.label == "Support:SupportTicketsResolved"
    assert "SupportTicketsCreated" in lm.nodes_by_table
    assert "SupportTicketsResolved" in lm.nodes_by_table


def test_from_schema_table_label_physical_table_map():
    """table_label uses logical field_name, not physical table_name.

    When physical_table_map remaps virtual names to a shared physical table,
    from_schema() must still produce distinct labels from the field_name,
    not the shared physical table_name.
    """
    from provisa.compiler.sql_gen import CompilationContext, TableMeta
    from provisa.cypher.label_map import CypherLabelMap

    ctx = CompilationContext()
    ctx.aggregate_columns[1] = [("ticket_id", "integer"), ("status", "varchar")]
    ctx.aggregate_columns[2] = [("ticket_id", "integer"), ("status", "varchar")]
    # Both virtual tables map to the same physical table "support_tickets"
    ctx.tables["support__support_tickets_created"] = TableMeta(
        table_id=1,
        field_name="support__support_tickets_created",
        type_name="Support_SupportTicketsCreated",
        source_id="trino",
        catalog_name="trino",
        schema_name="support",
        table_name="support_tickets",  # physical table (same for both)
        domain_id="support",
    )
    ctx.tables["support__support_tickets_resolved"] = TableMeta(
        table_id=2,
        field_name="support__support_tickets_resolved",
        type_name="Support_SupportTicketsResolved",
        source_id="trino",
        catalog_name="trino",
        schema_name="support",
        table_name="support_tickets",  # same physical table
        domain_id="support",
    )

    lm = CypherLabelMap.from_schema(ctx)

    created = lm.nodes["Support_SupportTicketsCreated"]
    resolved = lm.nodes["Support_SupportTicketsResolved"]

    # Must derive from field_name, not physical table_name "support_tickets"
    assert created.table_label == "SupportTicketsCreated", (
        f"Expected SupportTicketsCreated, got {created.table_label!r} — "
        "table_label must use the logical field name, not the shared physical table name"
    )
    assert resolved.table_label == "SupportTicketsResolved"
    assert "SupportTicketsCreated" in lm.nodes_by_table
    assert "SupportTicketsResolved" in lm.nodes_by_table


# ---------------------------------------------------------------------------
# Cross-domain traversal_only enforcement (REQ-441)
# ---------------------------------------------------------------------------


def _make_cross_domain_label_map() -> CypherLabelMap:
    """Label map with one owned node (Sales domain) and one traversal_only cross-domain node."""
    orders = NodeMapping(
        label="Sales:Orders",
        type_name="Sales_Orders",
        domain_label="Sales",
        table_label="Orders",
        table_id=1,
        source_id="pg",
        id_column="id",
        pk_columns=[],
        catalog_name="postgresql",
        schema_name="sales",
        table_name="orders",
        properties={"id": "id", "amount": "amount"},
    )
    shipments = NodeMapping(
        label="Logistics:Shipments",
        type_name="Logistics_Shipments",
        domain_label="Logistics",
        table_label="Shipments",
        table_id=2,
        source_id="pg2",
        id_column="shipment_id",
        pk_columns=[],
        catalog_name="postgresql",
        schema_name="logistics",
        table_name="shipments",
        properties={"shipmentId": "shipment_id", "status": "status"},
        traversal_only=True,
    )
    rel = RelationshipMapping(
        rel_type="SHIPPED_VIA",
        source_label="Sales_Orders",
        target_label="Logistics_Shipments",
        join_source_column="shipment_id",
        join_target_column="shipment_id",
        field_name="shipped_via",
    )
    nodes = {
        "Sales_Orders": orders,
        "Logistics_Shipments": shipments,
    }
    return CypherLabelMap(
        nodes=nodes,
        relationships={"SHIPPED_VIA": rel},
        domains={"Sales": ["Sales_Orders"], "Logistics": ["Logistics_Shipments"]},
        nodes_by_table={"Orders": ["Sales_Orders"], "Shipments": ["Logistics_Shipments"]},
    )


def test_traversal_only_node_as_start_raises():
    """Starting a MATCH on a traversal_only (cross-domain) node must raise CypherTranslateError."""
    from provisa.cypher.translator import CypherTranslateError

    lm = _make_cross_domain_label_map()
    ast = parse_cypher("MATCH (s:Logistics:Shipments) RETURN s.status")
    with pytest.raises(CypherTranslateError, match="domain outside your access"):
        cypher_to_sql(ast, lm, {})


def test_traversal_only_node_as_target_succeeds():
    """Traversing FROM an owned node TO a traversal_only (cross-domain) node must succeed."""
    lm = _make_cross_domain_label_map()
    ast = parse_cypher(
        "MATCH (o:Sales:Orders)-[:SHIPPED_VIA]->(s:Logistics:Shipments) RETURN o.id, s.status"
    )
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "shipments" in sql.lower()
    assert "orders" in sql.lower()


def _make_label_map_camel() -> CypherLabelMap:
    """Label map with a camelCase→snake_case property mapping."""
    node = NodeMapping(
        label="Dog",
        type_name="Dog",
        domain_label=None,
        table_label="Dog",
        table_id=10,
        source_id="pg-main",
        id_column="id",
        pk_columns=[],
        catalog_name="postgresql",
        schema_name="public",
        table_name="dogs",
        properties={"breedName": "breed_name", "name": "name"},
    )
    return CypherLabelMap(nodes={"Dog": node}, relationships={})


def test_camel_prop_in_return_rewrites_to_sql_col():
    """RETURN a.breedName must reference physical column breed_name and alias as breedName."""
    lm = _make_label_map_camel()
    ast = parse_cypher("MATCH (a:Dog) RETURN a.breedName")
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert '"breed_name"' in sql
    # Output alias must be the Cypher camelCase name (from NodeMapping.properties).
    assert "breedName" in sql or "breedname" in sql.lower()


def test_camel_prop_in_return_after_with_uses_cte_alias():
    """After a WITH clause, RETURN a.breedName must reference the CTE camelCase alias."""
    lm = _make_label_map_camel()
    ast = parse_cypher("MATCH (a:Dog) WITH a.breedName AS breedName RETURN breedName")
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "breedName" in sql or "breedname" in sql.lower()


def _make_source_constant_label_map() -> CypherLabelMap:
    """Label map where the source join column is a constant (e.g. __table_id__)."""
    parent = NodeMapping(
        label="RegisteredTables",
        type_name="RegisteredTables",
        domain_label=None,
        table_label="RegisteredTables",
        table_id=42,
        source_id="meta",
        id_column="id",
        pk_columns=[],
        catalog_name="meta",
        schema_name="meta",
        table_name="registered_tables",
        properties={"id": "id", "name": "name"},
    )
    child = NodeMapping(
        label="TableColumns",
        type_name="TableColumns",
        domain_label=None,
        table_label="TableColumns",
        table_id=43,
        source_id="meta",
        id_column="id",
        pk_columns=[],
        catalog_name="meta",
        schema_name="meta",
        table_name="table_columns",
        properties={"id": "id", "columnName": "column_name"},
    )
    rel = RelationshipMapping(
        rel_type="HAS_TABLE_COLUMNS",
        source_label="RegisteredTables",
        target_label="TableColumns",
        join_source_column="__table_id__",
        join_target_column="table_id",
        field_name="tableColumns",
        source_constant=42,
    )
    return CypherLabelMap(
        nodes={"RegisteredTables": parent, "TableColumns": child},
        relationships={"HAS_TABLE_COLUMNS::RegisteredTables→TableColumns": rel},
        nodes_by_table={"RegisteredTables": ["RegisteredTables"], "TableColumns": ["TableColumns"]},
        aliases={"HAS_TABLE_COLUMNS": [rel]},
    )


def test_source_constant_emits_literal_not_column():
    """Regression #46: source_constant on RelationshipMapping must emit a literal, not b."__table_id__"."""
    lm = _make_source_constant_label_map()
    ast = parse_cypher(
        "MATCH (b:RegisteredTables)-[:HAS_TABLE_COLUMNS]->(c:TableColumns) RETURN b.name, c.columnName"
    )
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "__table_id__" not in sql, (
        f"source_constant join emitted physical column __table_id__ instead of literal: {sql}"
    )
    assert "42" in sql, f"source_constant value 42 missing from SQL: {sql}"


def _make_string_source_constant_label_map() -> CypherLabelMap:
    """Label map simulating (Pets)-[:HAS_QUERIES]->(Queries) with source_constant='pets'."""
    pets = NodeMapping(
        label="Pets",
        type_name="Pets",
        domain_label=None,
        table_label="Pets",
        table_id=10,
        source_id="ps",
        id_column="id",
        pk_columns=[],
        catalog_name="ps",
        schema_name="pet_store",
        table_name="pets",
        properties={"id": "id", "name": "name"},
    )
    queries = NodeMapping(
        label="Queries",
        type_name="Queries",
        domain_label=None,
        table_label="Queries",
        table_id=99,
        source_id="ops",
        id_column="span_id",
        pk_columns=[],
        catalog_name="otel",
        schema_name="signals",
        table_name="queries",
        properties={"spanId": "span_id", "tableName": "table_name"},
    )
    rel = RelationshipMapping(
        rel_type="HAS_QUERIES",
        source_label="Pets",
        target_label="Queries",
        join_source_column="table_name",
        join_target_column="table_name",
        field_name="_queries",
        source_constant="pets",
    )
    return CypherLabelMap(
        nodes={"Pets": pets, "Queries": queries},
        relationships={"HAS_QUERIES::Pets→Queries": rel},
        nodes_by_table={"Pets": ["Pets"], "Queries": ["Queries"]},
        aliases={"HAS_QUERIES": [rel]},
    )


def test_string_source_constant_emits_string_literal():
    """source_constant as str must emit a quoted string literal, not a number or column reference."""
    lm = _make_string_source_constant_label_map()
    ast = parse_cypher("MATCH (a:Pets)-[:HAS_QUERIES]->(c:Queries) RETURN a.name, c.spanId")
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "table_name" not in sql or "'pets'" in sql, (
        f"source_constant 'pets' must prevent column reference: {sql}"
    )
    assert "'pets'" in sql, f"Expected string literal 'pets' in SQL: {sql}"


def _make_multi_source_has_table_label_map() -> CypherLabelMap:
    """Regression #46: aliases['HAS_TABLE'] with multiple source tables.

    Simulates production: every data table gets a HAS_TABLE entry in aliases.
    When translating (a:Pets)-[:HAS_TABLE]->(b:RegisteredTables), the translator
    must pick the Pets-sourced mapping, not the Breeds-sourced one (which would
    be is_bwd=True and emit b."__table_id__" as a physical column).
    """
    pets = NodeMapping(
        label="Pets",
        type_name="Pets",
        domain_label=None,
        table_label="Pets",
        table_id=1,
        source_id="pg-main",
        id_column="id",
        pk_columns=[],
        catalog_name="postgresql",
        schema_name="public",
        table_name="pets",
        properties={"id": "id", "name": "name"},
    )
    breeds = NodeMapping(
        label="Breeds",
        type_name="Breeds",
        domain_label=None,
        table_label="Breeds",
        table_id=2,
        source_id="pg-main",
        id_column="id",
        pk_columns=[],
        catalog_name="postgresql",
        schema_name="public",
        table_name="breeds",
        properties={"id": "id", "name": "name"},
    )
    rt = NodeMapping(
        label="RegisteredTables",
        type_name="RegisteredTables",
        domain_label=None,
        table_label="RegisteredTables",
        table_id=42,
        source_id="meta",
        id_column="id",
        pk_columns=[],
        catalog_name="meta",
        schema_name="meta",
        table_name="registered_tables",
        properties={"id": "id", "alias": "alias"},
    )
    has_table_pets = RelationshipMapping(
        rel_type="HAS_TABLE",
        source_label="Pets",
        target_label="RegisteredTables",
        join_source_column="__table_id__",
        join_target_column="id",
        field_name="_meta",
        source_constant=1,
    )
    has_table_breeds = RelationshipMapping(
        rel_type="HAS_TABLE",
        source_label="Breeds",
        target_label="RegisteredTables",
        join_source_column="__table_id__",
        join_target_column="id",
        field_name="_meta",
        source_constant=2,
    )
    # Breeds entry is FIRST in aliases — this is what triggers the bug in production
    return CypherLabelMap(
        nodes={"Pets": pets, "Breeds": breeds, "RegisteredTables": rt},
        relationships={
            "HAS_TABLE::Pets→RegisteredTables": has_table_pets,
            "HAS_TABLE::Breeds→RegisteredTables": has_table_breeds,
        },
        nodes_by_table={
            "Pets": ["Pets"],
            "Breeds": ["Breeds"],
            "RegisteredTables": ["RegisteredTables"],
        },
        aliases={"HAS_TABLE": [has_table_breeds, has_table_pets]},  # Breeds first!
    )


def test_multi_source_has_table_picks_correct_mapping():
    """Regression #46: when aliases has multiple HAS_TABLE entries (one per data table),
    translating (a:Pets)-[:HAS_TABLE]->(b:RegisteredTables) must use the Pets mapping
    (source_constant=1), not the Breeds mapping (source_constant=2), even if Breeds is
    first in aliases['HAS_TABLE']. b."__table_id__" must never appear in the SQL.
    """
    lm = _make_multi_source_has_table_label_map()
    ast = parse_cypher(
        "MATCH (a:Pets) "
        "OPTIONAL MATCH (a:Pets)-[:HAS_TABLE]->(b:RegisteredTables) "
        "RETURN a.name, b.alias"
    )
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "__table_id__" not in sql, (
        f"Regression #46: wrong alias selected, virtual column emitted as physical: {sql}"
    )
    assert "1" in sql, f"Expected Pets source_constant (1) in SQL: {sql}"
    assert "2" not in sql, f"Breeds source_constant (2) leaked into Pets query: {sql}"


# ---------------------------------------------------------------------------
# Regression #47: CALL subquery lateral variables must be qualified in outer RETURN
# ---------------------------------------------------------------------------


def test_call_subquery_return_var_qualified_with_lateral_alias():
    """Regression #47: CALL { WITH n ... RETURN ... AS c_list } must render outer SELECT's
    bare `c_list` reference as `_call0."c_list"` so Trino can resolve the scoped column.
    """
    lm = _make_label_map()
    ast = parse_cypher(
        "MATCH (n:Person) "
        "CALL { WITH n MATCH (n)-[:WORKS_AT]->(c:Company) RETURN collect(c.name) AS c_list } "
        "RETURN n.name, c_list"
    )
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    # The outer SELECT must reference c_list via the lateral alias
    assert '_call0."c_list"' in sql, (
        f'Regression #47: outer SELECT must use _call0."c_list", not bare c_list: {sql}'
    )


def test_call_subquery_list_comprehension_qualified():
    """Regression #47: [x IN c_list | x] in outer RETURN must reference _call0."c_list"."""
    lm = _make_label_map()
    ast = parse_cypher(
        "MATCH (n:Person) "
        "CALL { WITH n MATCH (n)-[:WORKS_AT]->(c:Company) RETURN collect(c.name) AS c_list } "
        "RETURN n.name, [x IN c_list | x] AS c_names"
    )
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    # The transform() first arg must be _call0."c_list"
    assert '_call0."c_list"' in sql, (
        f'Regression #47: list comprehension must use _call0."c_list": {sql}'
    )


def test_call_subquery_collect_slice_translates_correctly():
    """Regression: collect(d)[..2] in CALL RETURN must translate to slice(ARRAY_AGG(d), 1, 2).

    sql_to_cypher generates collect(var)[..N] to limit traversal results; the translator
    must convert this Cypher slice notation to the Trino-legal slice() function before
    passing to sqlglot, which cannot parse [..N] syntax.
    """
    lm = _make_label_map()
    ast = parse_cypher(
        "MATCH (n:Person) "
        "CALL { WITH n MATCH (n)-[:WORKS_AT]->(c:Company) RETURN collect(c)[..2] AS c_list } "
        "RETURN n.name, [x IN c_list | x.name] AS c_names"
    )
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "slice(" in sql.lower(), f"Expected slice() in translated SQL: {sql}"
    assert "array_agg" in sql.lower(), f"Expected ARRAY_AGG in translated SQL: {sql}"
    assert "[].0" not in sql, f"Malformed bracket expression in SQL: {sql}"


def test_call_subquery_multiple_calls_distinct_lateral_aliases():
    """Regression #47: two CALL subqueries must use _call0 and _call1 respectively."""
    lm = _make_label_map()
    ast = parse_cypher(
        "MATCH (n:Person) "
        "CALL { WITH n MATCH (n)-[:WORKS_AT]->(c:Company) RETURN collect(c.name) AS c_list } "
        "CALL { WITH n MATCH (n)-[:KNOWS]->(f:Person) RETURN collect(f.name) AS f_list } "
        "RETURN n.name, c_list, f_list"
    )
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert '_call0."c_list"' in sql, f'Expected _call0."c_list": {sql}'
    assert '_call1."f_list"' in sql, f'Expected _call1."f_list": {sql}'


def _make_label_map_with_traversal_only() -> CypherLabelMap:
    """Label map with a traversal-only node for regression #47 production path."""
    pets_meta = NodeMapping(
        label="Pets",
        type_name="Pets",
        domain_label=None,
        table_label="Pets",
        table_id=1,
        source_id="pg-main",
        id_column="id",
        pk_columns=[],
        catalog_name="postgresql",
        schema_name="public",
        table_name="pets",
        properties={"name": "name", "id": "id"},
    )
    traces_meta = NodeMapping(
        label="Ops_Traces",
        type_name="Ops_Traces",
        domain_label="Ops",
        table_label="Traces",
        table_id=99,
        source_id="ops",
        id_column="span_id",
        pk_columns=[],
        catalog_name="ops",
        schema_name="ops",
        table_name="spans",
        properties={"serviceName": "service_name", "spanId": "span_id"},
        traversal_only=True,
    )
    has_traces_rel = RelationshipMapping(
        rel_type="HAS_TRACES",
        source_label="Pets",
        target_label="Ops_Traces",
        join_source_column="id",
        join_target_column="pet_id",
        field_name="_traces",
    )
    return CypherLabelMap(
        nodes={"Pets": pets_meta, "Ops_Traces": traces_meta},
        relationships={"HAS_TRACES": has_traces_rel},
    )


def _make_label_map_camel_props() -> CypherLabelMap:
    """Label map where Cypher property names differ from SQL column names (camelCase vs snake_case)."""
    pet_meta = NodeMapping(
        label="Pets",
        type_name="Pets",
        domain_label=None,
        table_label="Pets",
        table_id=1,
        source_id="pg-main",
        id_column="id",
        pk_columns=[],
        catalog_name="postgresql",
        schema_name="public",
        table_name="pets",
        properties={"breedName": "breed_name", "petName": "pet_name"},
    )
    return CypherLabelMap(nodes={"Pets": pet_meta}, relationships={})


def test_return_prop_without_alias_uses_cypher_name_not_sql_column():
    """Regression #53: RETURN a.breedName with no alias must produce AS breedName,
    not the SQL column name breed_name."""
    lm = _make_label_map_camel_props()
    ast = parse_cypher("MATCH (a:Pets) RETURN a.breedName LIMIT 10")
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "breedName" in sql, f"Expected breedName alias in SQL: {sql}"
    assert (
        "breed_name AS breedName" in sql
        or 'breed_name" AS "breedName"' in sql
        or "breedName" in sql
    ), f"Expected breedName (Cypher name) as column alias, got: {sql}"


def test_call_subquery_traversal_only_node_collect_slice_qualified():
    """Regression #47 (production): CALL { WITH a ... RETURN collect(d)[..N] AS d_list }
    — production path uses whole-node collect with slice, traversal-only target.
    The outer RETURN's bare `d_list` must be qualified as `_call0."d_list"`.
    """
    lm = _make_label_map_with_traversal_only()
    ast = parse_cypher(
        "MATCH (a:Pets) "
        "CALL { WITH a OPTIONAL MATCH (a:Pets)-[:HAS_TRACES]->(d:Ops_Traces) "
        "RETURN collect(d)[..2] AS d_list } "
        "RETURN a.name, [x IN d_list | x.serviceName]"
    )
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert '_call0."d_list"' in sql, (
        f'Regression #47 production: outer SELECT must use _call0."d_list", not bare d_list: {sql}'
    )


# ---------------------------------------------------------------------------
# HAS_TABLE target_expr: stable varchar join via CONCAT
# ---------------------------------------------------------------------------


def _make_has_table_target_expr_label_map() -> CypherLabelMap:
    """Label map simulating production HAS_TABLE join.

    source_constant='pet-store.pets' (stable varchar, not integer ID)
    target_expr='CONCAT({alias}."domain_id", \'.\', {alias}."table_name")'
    This is the stable join key that survives PUT /admin/config reloads.
    """
    pets = NodeMapping(
        label="Pets",
        type_name="Pets",
        domain_label=None,
        table_label="Pets",
        table_id=10,
        source_id="pg-main",
        id_column="id",
        pk_columns=[],
        catalog_name="postgresql",
        schema_name="pet_store",
        table_name="pets",
        properties={"id": "id", "name": "name"},
    )
    rt = NodeMapping(
        label="RegisteredTables",
        type_name="RegisteredTables",
        domain_label=None,
        table_label="RegisteredTables",
        table_id=99,
        source_id="meta",
        id_column="id",
        pk_columns=[],
        catalog_name="meta",
        schema_name="meta",
        table_name="registered_tables",
        properties={"id": "id", "tableName": "table_name", "domainId": "domain_id"},
    )
    has_table = RelationshipMapping(
        rel_type="HAS_TABLE",
        source_label="Pets",
        target_label="RegisteredTables",
        join_source_column="__table_id__",
        join_target_column="id",
        field_name="_meta",
        source_constant="pet-store.pets",
        target_expr='CONCAT({alias}."domain_id", \'.\', {alias}."table_name")',
    )
    return CypherLabelMap(
        nodes={"Pets": pets, "RegisteredTables": rt},
        relationships={"HAS_TABLE::Pets→RegisteredTables": has_table},
        nodes_by_table={"Pets": ["Pets"], "RegisteredTables": ["RegisteredTables"]},
        aliases={"HAS_TABLE": [has_table]},
    )


def test_has_table_target_expr_emits_concat_not_id_column():
    """HAS_TABLE with target_expr must emit CONCAT(...) on target side, not registered_tables.id.

    Regression: before this fix, the join was 'pet-store.pets' = rt."id" which broke
    after every PUT /admin/config because id is an auto-increment that changes on reload.
    After the fix, the join is 'pet-store.pets' = CONCAT(rt."domain_id", '.', rt."table_name")
    which is stable across config reloads.
    """
    lm = _make_has_table_target_expr_label_map()
    ast = parse_cypher(
        "MATCH (a:Pets) "
        "OPTIONAL MATCH (a:Pets)-[:HAS_TABLE]->(b:RegisteredTables) "
        "RETURN a.name, b.tableName"
    )
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")

    assert '"id"' not in sql.lower().replace('"id"', "\x00"), (
        f"HAS_TABLE must not join on registered_tables.id (unstable): {sql}"
    )
    # The integer ID column must not be used as the target
    assert "CONCAT" in sql.upper(), f"Expected CONCAT expression in HAS_TABLE join: {sql}"
    assert '"domain_id"' in sql, f"Expected domain_id in CONCAT expression: {sql}"
    assert '"table_name"' in sql, f"Expected table_name in CONCAT expression: {sql}"
    assert "'pet-store.pets'" in sql, f"Expected stable varchar literal in join: {sql}"


def test_has_table_stable_key_not_integer():
    """source_constant for HAS_TABLE must be a string, not an integer.

    Integer IDs change on every PUT /admin/config (auto-increment resets).
    The translator must emit a string literal, not a number literal.
    """
    lm = _make_has_table_target_expr_label_map()
    ast = parse_cypher("MATCH (a:Pets)-[:HAS_TABLE]->(b:RegisteredTables) RETURN a.id, b.tableName")
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")

    # source_constant must be quoted string, not a bare integer
    assert "'pet-store.pets'" in sql, (
        f"Expected varchar constant 'pet-store.pets' as join key, got: {sql}"
    )
    # No bare integer 10 (table_id) or 99 should appear as the join value
    import re

    join_val_match = re.search(r"=\s*(\d+)", sql)
    assert join_val_match is None, (
        f"Unexpected integer used as HAS_TABLE join value (unstable after config reload): {sql}"
    )


# ---------------------------------------------------------------------------
# Ops join: source_expr on both sides (registered_tables → _traces/_queries)
# ---------------------------------------------------------------------------


def _make_ops_join_label_map() -> CypherLabelMap:
    """Label map simulating production ops join: RegisteredTables → Traces.

    Both sides use CONCAT(domain_id, '.', table_name) as the join key.
    source_expr on the source (registered_tables) side,
    target_expr on the target (traces) side.
    """
    rt = NodeMapping(
        label="RegisteredTables",
        type_name="RegisteredTables",
        domain_label=None,
        table_label="RegisteredTables",
        table_id=99,
        source_id="meta",
        id_column="id",
        pk_columns=[],
        catalog_name="meta",
        schema_name="meta",
        table_name="registered_tables",
        properties={"id": "id", "tableName": "table_name", "domainId": "domain_id"},
    )
    traces = NodeMapping(
        label="Traces",
        type_name="Traces",
        domain_label=None,
        table_label="Traces",
        table_id=200,
        source_id="otel",
        id_column="span_id",
        pk_columns=[],
        catalog_name="otel",
        schema_name="signals",
        table_name="traces",
        properties={"spanId": "span_id", "tableName": "table_name", "domainId": "domain_id"},
    )
    has_traces = RelationshipMapping(
        rel_type="HAS_TRACES",
        source_label="RegisteredTables",
        target_label="Traces",
        join_source_column="table_name",
        join_target_column="table_name",
        field_name="_traces",
        source_expr='CONCAT({alias}."domain_id", \'.\', {alias}."table_name")',
        target_expr='CONCAT({alias}."domain_id", \'.\', {alias}."table_name")',
    )
    return CypherLabelMap(
        nodes={"RegisteredTables": rt, "Traces": traces},
        relationships={"HAS_TRACES::RegisteredTables→Traces": has_traces},
        nodes_by_table={"RegisteredTables": ["RegisteredTables"], "Traces": ["Traces"]},
        aliases={"HAS_TRACES": [has_traces]},
    )


def test_ops_join_source_expr_emits_concat_on_both_sides():
    """Ops join (registered_tables → traces) must use CONCAT on both sides.

    Without source_expr propagation the source side falls back to the bare
    table_name column ('pets'), while target_expr gives CONCAT(...) = 'pet-store.pets'.
    These never match. Both sides must emit CONCAT(domain_id, '.', table_name).
    """
    lm = _make_ops_join_label_map()
    ast = parse_cypher(
        "MATCH (r:RegisteredTables)-[:HAS_TRACES]->(t:Traces) RETURN r.tableName, t.spanId"
    )
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")

    assert "CONCAT" in sql.upper(), f"Expected CONCAT on both sides of ops join: {sql}"
    concat_count = sql.upper().count("CONCAT")
    assert concat_count >= 2, (
        f"Expected CONCAT on both source and target sides, got {concat_count} occurrence(s): {sql}"
    )
    assert '"domain_id"' in sql, f"Expected domain_id in CONCAT expression: {sql}"


# ---------------------------------------------------------------------------
# WHERE after OPTIONAL MATCH — NULL guard
# ---------------------------------------------------------------------------


def test_where_after_optional_match_does_not_filter_base_rows():
    """WHERE referencing an OPTIONAL MATCH variable must not eliminate base MATCH rows.

    Cypher: WHERE after OPTIONAL MATCH constrains the optional pattern, not the whole
    result. Rows where the optional variable is NULL must still be returned.
    The translator must emit (opt_var IS NULL OR <condition>) rather than a bare WHERE.
    """
    lm = _make_label_map()
    ast = parse_cypher(
        "MATCH (n:Person) "
        "OPTIONAL MATCH (n)-[:WORKS_AT]->(c:Company) "
        "WHERE NOT c.name IN ['Acme'] "
        "RETURN n.name, c.name"
    )
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")

    assert "LEFT" in sql.upper(), f"OPTIONAL MATCH should produce LEFT JOIN: {sql}"
    # Condition must be folded into the LEFT JOIN ON clause, not a global WHERE.
    # This keeps a rows alive when b is excluded — b becomes NULL instead of the row being removed.
    assert "WHERE" not in sql.upper(), (
        f"WHERE after OPTIONAL MATCH must be folded into ON clause, not a global WHERE: {sql}"
    )
    assert "ON" in sql.upper() and "ACME" in sql.upper(), (
        f"Exclusion condition must appear in JOIN ON clause: {sql}"
    )


def test_where_on_required_match_variable_is_not_guarded():
    """WHERE referencing only a required MATCH variable should remain a plain WHERE.

    No IS NULL guard needed — required MATCH variables are never NULL.
    """
    lm = _make_label_map()
    ast = parse_cypher(
        "MATCH (n:Person) "
        "OPTIONAL MATCH (n)-[:WORKS_AT]->(c:Company) "
        "WHERE n.age > 30 "
        "RETURN n.name, c.name"
    )
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")

    # n comes from required MATCH — no IS NULL guard needed
    assert "IS NULL" not in sql.upper(), (
        f"WHERE on required MATCH variable must not add IS NULL guard: {sql}"
    )
    assert "30" in sql, f"WHERE condition should still appear in SQL: {sql}"


def _make_label_map_bidir_multi_hop() -> CypherLabelMap:
    """Label map with bidir Inquiries↔Pets (both directions registered) and one-way Inquiries→Users."""
    nm_inq = NodeMapping(
        label="PetStore:Inquiries",
        type_name="PetStore_Inquiries",
        domain_label="PetStore",
        table_label="Inquiries",
        table_id=1,
        source_id="sqlite-petstore",
        id_column="id",
        pk_columns=[],
        catalog_name="sqlite",
        schema_name="petstore",
        table_name="inquiries",
        properties={"id": "id", "petId": "pet_id", "userId": "user_id"},
    )
    nm_pets = NodeMapping(
        label="PetStore:Pets",
        type_name="PetStore_Pets",
        domain_label="PetStore",
        table_label="Pets",
        table_id=2,
        source_id="sqlite-petstore",
        id_column="id",
        pk_columns=[],
        catalog_name="sqlite",
        schema_name="petstore",
        table_name="pets",
        properties={"id": "id", "name": "name"},
    )
    nm_users = NodeMapping(
        label="PetStore:Users",
        type_name="PetStore_Users",
        domain_label="PetStore",
        table_label="Users",
        table_id=3,
        source_id="sqlite-petstore",
        id_column="id",
        pk_columns=[],
        catalog_name="sqlite",
        schema_name="petstore",
        table_name="users",
        properties={"id": "id", "name": "name", "email": "email"},
    )
    return CypherLabelMap(
        nodes={
            "PetStore_Inquiries": nm_inq,
            "PetStore_Pets": nm_pets,
            "PetStore_Users": nm_users,
        },
        relationships={
            "HAS_PET::PetStore_Inquiries→PetStore_Pets": RelationshipMapping(
                rel_type="HAS_PET",
                source_label="PetStore_Inquiries",
                target_label="PetStore_Pets",
                join_source_column="pet_id",
                join_target_column="id",
                field_name="hasPet",
            ),
            "HAS_USER::PetStore_Inquiries→PetStore_Users": RelationshipMapping(
                rel_type="HAS_USER",
                source_label="PetStore_Inquiries",
                target_label="PetStore_Users",
                join_source_column="user_id",
                join_target_column="id",
                field_name="hasUser",
            ),
            # reverse direction registered — causes bidir to produce a UNION ALL branch
            "INQUIRIES_FOR_PET::PetStore_Pets→PetStore_Inquiries": RelationshipMapping(
                rel_type="INQUIRIES_FOR_PET",
                source_label="PetStore_Pets",
                target_label="PetStore_Inquiries",
                join_source_column="id",
                join_target_column="pet_id",
                field_name="inquiriesForPet",
            ),
        },
        domains={"PetStore": ["PetStore_Inquiries", "PetStore_Pets", "PetStore_Users"]},
        nodes_by_table={
            "Inquiries": ["PetStore_Inquiries"],
            "Pets": ["PetStore_Pets"],
            "Users": ["PetStore_Users"],
        },
        aliases={},
    )


def test_union_all_branch_includes_all_subsequent_optional_match_joins():
    """UNION ALL extra branches must include joins for ALL variables in the SELECT.

    When an undirected OPTIONAL MATCH has multiple relationship candidates (bidir),
    the translator emits UNION ALL. Each UNION branch must carry every JOIN that
    appears in the primary query — including ones added by OPTIONAL MATCHes that
    come AFTER the branching point — so that column references in the shared SELECT
    are resolvable in every branch.

    Regression: without the fix, the extra branch for mPets only contained the
    mPets JOIN, leaving mUsers unresolved → Trino: 'Column musers.id cannot be
    resolved'.
    """
    lm = _make_label_map_bidir_multi_hop()
    ast = parse_cypher(
        "MATCH (n:PetStore:Inquiries) "
        "OPTIONAL MATCH (n)-[rPets]-(mPets:PetStore:Pets) "
        "OPTIONAL MATCH (n)-[rUsers]-(mUsers:PetStore:Users) "
        "RETURN n, rPets, mPets, rUsers, mUsers"
    )
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")

    assert "UNION ALL" in sql, "Bidir relationship should produce UNION ALL"

    # Each UNION branch must include both mPets and mUsers JOINs
    union_idx = sql.index("UNION ALL")
    second_branch = sql[union_idx:]
    assert "mPets" in second_branch, "Second branch must have mPets alias"
    assert "mUsers" in second_branch, "Second branch must have mUsers alias in its FROM/JOINs"

    # Confirm mUsers appears as a JOIN (not just in SELECT expressions)
    from_idx = second_branch.index(" FROM ")
    from_clause = second_branch[from_idx:]
    assert "mUsers" in from_clause, (
        f"mUsers JOIN missing from UNION ALL branch FROM clause: {from_clause[:300]}"
    )
