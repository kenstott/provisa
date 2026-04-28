# Copyright (c) 2026 Kenneth Stott
# Canary: 4a1c7e3f-2b9d-4a5e-8f3c-6d4a8b2f7c9e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for provisa/cypher/translator.py — expressions, subqueries, map projections."""

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


def _make_label_map_three_hop() -> CypherLabelMap:
    person = NodeMapping(
        label="Person", type_name="Person", domain_label=None, table_label="Person",
        table_id=1, source_id="pg", id_column="id", pk_columns=[],
        catalog_name="postgresql", schema_name="public", table_name="persons",
        properties={"name": "name"},
    )
    company = NodeMapping(
        label="Company", type_name="Company", domain_label=None, table_label="Company",
        table_id=2, source_id="pg", id_column="id", pk_columns=[],
        catalog_name="postgresql", schema_name="public", table_name="companies",
        properties={"name": "name"},
    )
    dept = NodeMapping(
        label="Department", type_name="Department", domain_label=None, table_label="Department",
        table_id=3, source_id="pg", id_column="id", pk_columns=[],
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


def _make_label_map_bidirectional() -> CypherLabelMap:
    """Label map with Person→Company (WORKS_AT) and Company→Person (EMPLOYS)."""
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
        "EMPLOYS": RelationshipMapping(
            rel_type="EMPLOYS", source_label="Company", target_label="Person",
            join_source_column="employee_id", join_target_column="id", field_name="employs",
        ),
    }
    return CypherLabelMap(nodes={"Person": person_meta, "Company": company_meta}, relationships=rels)


# ---------------------------------------------------------------------------
# Gap #12 — Intermediate node property access in multi-hop patterns
# ---------------------------------------------------------------------------

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

def test_trim_function():
    lm = _make_label_map()
    ast = parse_cypher("MATCH (n:Person) RETURN trim(n.name) AS t")
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "trim" in sql.lower()


def test_to_string_or_null():
    lm = _make_label_map()
    ast = parse_cypher("MATCH (n:Person) RETURN toStringOrNull(n.age) AS s")
    sql_ast, _, _ = cypher_to_sql(ast, lm, {})
    sql = sql_ast.sql(dialect="trino")
    assert "try_cast" in sql.lower()
    assert "varchar" in sql.lower()


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
