# Copyright (c) 2026 Kenneth Stott
# Canary: 9f3c5a2e-8b4d-4f7a-9c1e-3d5b7a9f2c4e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for provisa/cypher/parser.py."""

import pytest

from provisa.cypher.parser import (
    CypherAST,
    CallSubquery,
    CypherParseError,
    MatchClause,
    NodePattern,
    PathFunction,
    RelPattern,
    ReturnClause,
    WhereClause,
    extract_parameters,
    parse_cypher,
)


# ---------------------------------------------------------------------------
# Happy-path parsing
# ---------------------------------------------------------------------------

def test_simple_match_return():
    ast = parse_cypher("MATCH (n:Person) RETURN n.name")
    assert len(ast.match_clauses) == 1
    clause = ast.match_clauses[0]
    assert not clause.optional
    nodes = clause.pattern.nodes
    assert len(nodes) == 1
    assert nodes[0].labels == ["Person"]
    assert nodes[0].variable == "n"
    assert ast.return_clause.items[0].expression.strip() in ("n . name", "n.name", "n . name")


def test_optional_match_flag():
    ast = parse_cypher(
        "MATCH (n:Person) OPTIONAL MATCH (n)-[:KNOWS]->(m:Person) RETURN n.name"
    )
    assert ast.match_clauses[0].optional is False
    assert ast.match_clauses[1].optional is True


def test_where_clause_parsed():
    ast = parse_cypher("MATCH (n:Person) WHERE n.age > 30 RETURN n.name")
    assert ast.where is not None
    assert "30" in ast.where.expression


def test_limit_skip_parsed():
    ast = parse_cypher("MATCH (n:Person) RETURN n.name ORDER BY n.name SKIP 10 LIMIT 20")
    assert ast.skip == 10
    assert ast.limit == 20


def test_order_by_parsed():
    ast = parse_cypher("MATCH (n:Person) RETURN n.name ORDER BY n.name DESC")
    assert len(ast.order_by) == 1
    assert ast.order_by[0].direction == "DESC"


def test_return_alias():
    ast = parse_cypher("MATCH (n:Person) RETURN n.name AS fullname")
    item = ast.return_clause.items[0]
    assert item.alias == "fullname"


def test_named_parameters_extracted():
    ast = parse_cypher("MATCH (n:Person) WHERE n.age > $min AND n.age < $max RETURN n")
    params = extract_parameters("MATCH (n:Person) WHERE n.age > $min AND n.age < $max RETURN n")
    assert "min" in params
    assert "max" in params


def test_relationship_pattern_parsed():
    ast = parse_cypher("MATCH (n:Person)-[:KNOWS]->(m:Person) RETURN n.name, m.name")
    clause = ast.match_clauses[0]
    assert len(clause.pattern.nodes) == 2
    assert len(clause.pattern.rels) == 1
    assert clause.pattern.rels[0].types == ["KNOWS"]


def test_shortest_path_parsed():
    ast = parse_cypher(
        "MATCH p = shortestPath((a:Person)-[:KNOWS*..5]->(b:Person)) RETURN p"
    )
    clause = ast.match_clauses[0]
    assert isinstance(clause.pattern, PathFunction)
    assert clause.pattern.func_name == "shortestpath"
    assert clause.variable == "p"
    rel = clause.pattern.pattern.rels[0]
    assert rel.variable_length is True
    assert rel.max_hops == 5


def test_all_shortest_paths_parsed():
    ast = parse_cypher(
        "MATCH p = allShortestPaths((a:Person)-[:KNOWS*..3]->(b:Company)) RETURN p"
    )
    clause = ast.match_clauses[0]
    assert isinstance(clause.pattern, PathFunction)
    assert "allshortestpaths" in clause.pattern.func_name.lower()


# ---------------------------------------------------------------------------
# Rejection tests
# ---------------------------------------------------------------------------

def test_create_rejected():
    with pytest.raises(CypherParseError, match="CREATE"):
        parse_cypher("CREATE (n:Person {name: 'Alice'})")


def test_merge_rejected():
    with pytest.raises(CypherParseError, match="MERGE"):
        parse_cypher("MERGE (n:Person {name: 'Alice'})")


def test_set_rejected():
    with pytest.raises(CypherParseError, match="SET"):
        parse_cypher("MATCH (n:Person) SET n.name = 'Bob'")


def test_delete_rejected():
    with pytest.raises(CypherParseError, match="DELETE"):
        parse_cypher("MATCH (n:Person) DELETE n")


def test_detach_delete_rejected():
    with pytest.raises(CypherParseError, match="DETACH"):
        parse_cypher("MATCH (n:Person) DETACH DELETE n")


def test_remove_rejected():
    with pytest.raises(CypherParseError, match="REMOVE"):
        parse_cypher("MATCH (n:Person) REMOVE n.name")


def test_apoc_rejected():
    with pytest.raises(CypherParseError, match="APOC"):
        parse_cypher("MATCH (n) RETURN apoc.util.sleep(100)")


def test_unbounded_variable_length_rejected():
    with pytest.raises(CypherParseError, match="[Uu]nbounded"):
        parse_cypher("MATCH (n:Person)-[*]->(m:Company) RETURN n")


def test_missing_return_rejected():
    with pytest.raises(CypherParseError, match="RETURN"):
        parse_cypher("MATCH (n:Person) WHERE n.age > 30")


# ---------------------------------------------------------------------------
# CALL {} subquery tests
# ---------------------------------------------------------------------------

def test_call_subquery_parsed():
    query = (
        "CALL {\n"
        "  MATCH (a:Person) RETURN a.name AS name\n"
        "}\n"
        "CALL {\n"
        "  MATCH (b:Order) RETURN b.id AS order_id\n"
        "}\n"
        "RETURN name, order_id"
    )
    ast = parse_cypher(query)
    assert len(ast.call_subqueries) == 2
    sq1 = ast.call_subqueries[0]
    assert isinstance(sq1, CallSubquery)
    assert len(sq1.body.match_clauses) == 1
    assert sq1.body.match_clauses[0].pattern.nodes[0].labels == ["Person"]
    assert sq1.body.return_clause is not None
    assert sq1.body.return_clause.items[0].alias == "name"
    sq2 = ast.call_subqueries[1]
    assert len(sq2.body.match_clauses) == 1
    assert sq2.body.match_clauses[0].pattern.nodes[0].labels == ["Order"]
    assert sq2.body.return_clause.items[0].alias == "order_id"


def test_call_subquery_with_outer_return():
    query = (
        "CALL {\n"
        "  MATCH (x:Person) RETURN x.name AS x_name\n"
        "}\n"
        "CALL {\n"
        "  MATCH (y:Company) RETURN y.name AS y_name\n"
        "}\n"
        "RETURN x_name, y_name"
    )
    ast = parse_cypher(query)
    assert len(ast.call_subqueries) == 2
    assert ast.return_clause is not None
    outer_exprs = [item.expression.strip() for item in ast.return_clause.items]
    assert any("x_name" in e for e in outer_exprs)
    assert any("y_name" in e for e in outer_exprs)


def test_call_subquery_no_outer_return():
    query = (
        "CALL {\n"
        "  MATCH (n:Person) RETURN n.name AS name\n"
        "}"
    )
    ast = parse_cypher(query)
    assert len(ast.call_subqueries) == 1
    assert ast.return_clause is None


# ---------------------------------------------------------------------------
# UNION tests
# ---------------------------------------------------------------------------

def test_union_parsed():
    query = (
        "MATCH (n:Person) RETURN n.name "
        "UNION "
        "MATCH (n:Person) RETURN n.name"
    )
    ast = parse_cypher(query)
    assert len(ast.union_parts) == 1
    sub_ast, is_all = ast.union_parts[0]
    assert is_all is False
    assert sub_ast.return_clause is not None


def test_union_all_parsed():
    query = (
        "MATCH (n:Person) RETURN n.name "
        "UNION ALL "
        "MATCH (n:Person) RETURN n.name"
    )
    ast = parse_cypher(query)
    assert len(ast.union_parts) == 1
    _, is_all = ast.union_parts[0]
    assert is_all is True


def test_union_sub_ast_has_match():
    query = (
        "MATCH (n:Person) RETURN n.name "
        "UNION "
        "MATCH (c:Company) RETURN c.name"
    )
    ast = parse_cypher(query)
    sub_ast, _ = ast.union_parts[0]
    assert sub_ast.match_clauses[0].pattern.nodes[0].labels == ["Company"]


# ---------------------------------------------------------------------------
# WITH pipeline tests
# ---------------------------------------------------------------------------

def test_with_creates_pipeline_stages():
    ast = parse_cypher("MATCH (n:Person) WITH n.name AS nm RETURN nm")
    from provisa.cypher.parser import MatchStep, WithClause
    assert len(ast.pipeline) == 2
    assert isinstance(ast.pipeline[0], MatchStep)
    assert isinstance(ast.pipeline[1], WithClause)
    assert ast.pipeline[1].items[0].alias == "nm"


def test_with_preserves_match_group_where():
    ast = parse_cypher("MATCH (n:Person) WHERE n.age > 30 WITH n RETURN n")
    from provisa.cypher.parser import MatchStep
    step = ast.pipeline[0]
    assert isinstance(step, MatchStep)
    assert step.where is not None
    assert "30" in step.where.expression


def test_multi_with_pipeline():
    q = (
        "MATCH (n:Person) "
        "WITH n "
        "MATCH (n)-[:WORKS_AT]->(c:Company) "
        "WITH n.name AS nm, c.name AS cn "
        "RETURN nm, cn"
    )
    ast = parse_cypher(q)
    from provisa.cypher.parser import MatchStep, WithClause
    assert len(ast.pipeline) == 4  # MatchStep, WithClause, MatchStep, WithClause
    assert isinstance(ast.pipeline[0], MatchStep)
    assert isinstance(ast.pipeline[1], WithClause)
    assert isinstance(ast.pipeline[2], MatchStep)
    assert isinstance(ast.pipeline[3], WithClause)


def test_match_clauses_property_backward_compat():
    ast = parse_cypher("MATCH (n:Person) OPTIONAL MATCH (n)-[:KNOWS]->(m:Person) RETURN n.name")
    assert len(ast.match_clauses) == 2


def test_where_property_backward_compat():
    ast = parse_cypher("MATCH (n:Person) WHERE n.age > 30 RETURN n.name")
    assert ast.where is not None
    assert "30" in ast.where.expression


# ---------------------------------------------------------------------------
# _detect_procedure (cypher_router)
# ---------------------------------------------------------------------------

def test_detect_procedure_labels():
    from provisa.api.rest.cypher_router import _detect_procedure
    assert _detect_procedure("CALL db.labels()") == "db.labels"


def test_detect_procedure_relationship_types():
    from provisa.api.rest.cypher_router import _detect_procedure
    assert _detect_procedure("CALL db.relationshipTypes()") == "db.relationshiptypes"


def test_detect_procedure_property_keys():
    from provisa.api.rest.cypher_router import _detect_procedure
    assert _detect_procedure("CALL db.propertyKeys()") == "db.propertykeys"


def test_detect_procedure_case_insensitive():
    from provisa.api.rest.cypher_router import _detect_procedure
    assert _detect_procedure("call DB.LABELS()") == "db.labels"


def test_detect_procedure_with_whitespace():
    from provisa.api.rest.cypher_router import _detect_procedure
    assert _detect_procedure("  CALL  db.labels(  )  ") == "db.labels"


def test_detect_procedure_non_procedure_returns_none():
    from provisa.api.rest.cypher_router import _detect_procedure
    assert _detect_procedure("MATCH (n:Person) RETURN n") is None


def test_detect_procedure_unknown_proc_returns_none():
    from provisa.api.rest.cypher_router import _detect_procedure
    assert _detect_procedure("CALL db.unknown()") is None
