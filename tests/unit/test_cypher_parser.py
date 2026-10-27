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

from provisa.cypher.parser import CypherParseError, extract_parameters, parse_cypher
from provisa.gql.expr_ast import CypherExpr, Literal, Property, Variable, walk
from provisa.gql.ir import CallSubquery, PathFunction


def _holds_number(expr: CypherExpr, value: int) -> bool:
    """Whether the expression tree holds the number ``value``."""
    return any(isinstance(n, Literal) and n.value == value for n in walk(expr))


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
    # REQ-2135: a clause's expression is the IR's tree; the item keeps how it is written.
    assert ast.return_clause.items[0].expression == Property(Variable("n"), "name")
    assert ast.return_clause.items[0].name.replace(" ", "") == "n.name"


def test_optional_match_flag():
    ast = parse_cypher("MATCH (n:Person) OPTIONAL MATCH (n)-[:KNOWS]->(m:Person) RETURN n.name")
    assert ast.match_clauses[0].optional is False
    assert ast.match_clauses[1].optional is True


def test_comma_separated_patterns_are_clauses_of_one_match():
    ast = parse_cypher("MATCH (a:A)-[:R]->(b:B), (b)-[:S]->(c:C) WHERE a.x = 1 RETURN a.x")
    assert len(ast.pipeline[0].matches) == 2
    assert [m.optional for m in ast.match_clauses] == [False, False]
    assert ast.match_clauses[1].pattern.nodes[0].variable == "b"
    assert _holds_number(ast.where.expression, 1)


def test_where_clause_parsed():
    ast = parse_cypher("MATCH (n:Person) WHERE n.age > 30 RETURN n.name")
    assert ast.where is not None
    assert _holds_number(ast.where.expression, 30)


def test_limit_skip_parsed():
    ast = parse_cypher("MATCH (n:Person) RETURN n.name ORDER BY n.name SKIP 10 LIMIT 20")
    assert ast.skip == 10
    assert ast.limit == 20


def test_order_by_parsed():
    ast = parse_cypher("MATCH (n:Person) RETURN n.name ORDER BY n.name DESC")
    assert len(ast.order_by) == 1
    assert ast.order_by[0].direction == "DESC"


def test_order_by_multiple_columns_parsed():
    ast = parse_cypher("MATCH (u:Person)-[:WROTE]->(p:Post) RETURN u.id, p.id ORDER BY u.id, p.id")
    assert len(ast.order_by) == 2
    assert ast.order_by[0].expression == Property(Variable("u"), "id")
    assert ast.order_by[1].expression == Property(Variable("p"), "id")
    assert ast.order_by[0].direction == "ASC"
    assert ast.order_by[1].direction == "ASC"


def test_return_alias():
    ast = parse_cypher("MATCH (n:Person) RETURN n.name AS fullname")
    item = ast.return_clause.items[0]
    assert item.alias == "fullname"


def test_named_parameters_extracted():
    _ast = parse_cypher("MATCH (n:Person) WHERE n.age > $min AND n.age < $max RETURN n")
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
    ast = parse_cypher("MATCH p = shortestPath((a:Person)-[:KNOWS*..5]->(b:Person)) RETURN p")
    clause = ast.match_clauses[0]
    assert isinstance(clause.pattern, PathFunction)
    assert clause.pattern.func_name == "shortestpath"
    assert clause.variable == "p"
    rel = clause.pattern.pattern.rels[0]
    assert rel.variable_length is True
    assert rel.max_hops == 5


def test_all_shortest_paths_parsed():
    ast = parse_cypher("MATCH p = allShortestPaths((a:Person)-[:KNOWS*..3]->(b:Company)) RETURN p")
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


def test_an_apoc_function_is_read_as_a_function_call():
    # REQ-2168: a namespaced function is a function's Cypher command binding; one no function is
    # bound to is refused when the statement is bound for its role (api/data/cypher_commands.py).
    ast = parse_cypher("MATCH (n) RETURN apoc.util.sleep(100)")
    (item,) = ast.return_clause.items
    assert item.expression.name == "apoc.util.sleep"


def test_unbounded_variable_length_rejected():
    with pytest.raises(CypherParseError, match="[Uu]nbounded"):
        parse_cypher("MATCH (n:Person)-[*]->(m:Company) RETURN n")


def test_unbounded_variable_length_written_out_rejected():
    # [*..] is [*] written out (REQ-348; openCypher TCK Match5 [2]).
    with pytest.raises(CypherParseError, match="Unbounded variable-length pattern"):
        parse_cypher("MATCH (n:Person)-[*..]->(m:Company) RETURN n")


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
    outer_exprs = [item.expression for item in ast.return_clause.items]
    assert Variable("x_name") in outer_exprs
    assert Variable("y_name") in outer_exprs


def test_call_subquery_no_outer_return():
    query = "CALL {\n  MATCH (n:Person) RETURN n.name AS name\n}"
    ast = parse_cypher(query)
    assert len(ast.call_subqueries) == 1
    assert ast.return_clause is None


# ---------------------------------------------------------------------------
# UNION tests
# ---------------------------------------------------------------------------


def test_union_parsed():
    query = "MATCH (n:Person) RETURN n.name UNION MATCH (n:Person) RETURN n.name"
    ast = parse_cypher(query)
    assert len(ast.union_parts) == 1
    sub_ast, is_all = ast.union_parts[0]
    assert is_all is False
    assert sub_ast.return_clause is not None


def test_union_all_parsed():
    query = "MATCH (n:Person) RETURN n.name UNION ALL MATCH (n:Person) RETURN n.name"
    ast = parse_cypher(query)
    assert len(ast.union_parts) == 1
    _, is_all = ast.union_parts[0]
    assert is_all is True


def test_union_sub_ast_has_match():
    query = "MATCH (n:Person) RETURN n.name AS name UNION MATCH (c:Company) RETURN c.name AS name"
    ast = parse_cypher(query)
    sub_ast, _ = ast.union_parts[0]
    assert sub_ast.match_clauses[0].pattern.nodes[0].labels == ["Company"]


# ---------------------------------------------------------------------------
# WITH pipeline tests
# ---------------------------------------------------------------------------


def test_with_creates_pipeline_stages():
    ast = parse_cypher("MATCH (n:Person) WITH n.name AS nm RETURN nm")
    from provisa.gql.ir import MatchStep, WithClause

    assert len(ast.pipeline) == 2
    assert isinstance(ast.pipeline[0], MatchStep)
    assert isinstance(ast.pipeline[1], WithClause)
    assert ast.pipeline[1].items[0].alias == "nm"


def test_with_preserves_match_group_where():
    ast = parse_cypher("MATCH (n:Person) WHERE n.age > 30 WITH n RETURN n")
    from provisa.gql.ir import MatchStep

    step = ast.pipeline[0]
    assert isinstance(step, MatchStep)
    assert step.where is not None
    assert _holds_number(step.where.expression, 30)


def test_multi_with_pipeline():
    q = (
        "MATCH (n:Person) "
        "WITH n "
        "MATCH (n)-[:WORKS_AT]->(c:Company) "
        "WITH n.name AS nm, c.name AS cn "
        "RETURN nm, cn"
    )
    ast = parse_cypher(q)
    from provisa.gql.ir import MatchStep, WithClause

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
    assert _holds_number(ast.where.expression, 30)


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


# ---------------------------------------------------------------------------
# One statement per query
# ---------------------------------------------------------------------------


def test_two_statements_in_one_query_are_refused_never_joined():
    """Once read as one MATCH (a)...MATCH (b) RETURN b: a cross join that answered only the second."""
    with pytest.raises(CypherParseError, match="exactly one statement per query but got: 2"):
        parse_cypher("MATCH (a:A) RETURN a.x; MATCH (b:B) RETURN b.y")
    with pytest.raises(CypherParseError, match="but got: 3"):
        parse_cypher("MATCH (a:A) RETURN a.x;\nMATCH (b:B) RETURN b.y;\nRETURN 1;")


def test_a_semicolon_ending_the_one_statement_is_allowed():
    alone = parse_cypher("MATCH (a:A) RETURN a.x")
    assert parse_cypher("MATCH (a:A) RETURN a.x;") == alone
    assert parse_cypher("MATCH (a:A) RETURN a.x ;\n// done\n") == parse_cypher(
        "MATCH (a:A) RETURN a.x // done"
    )


def test_a_semicolon_in_a_string_or_a_comment_separates_nothing():
    parse_cypher("MATCH (a:A) WHERE a.x = 'one; two' RETURN a.x")
    parse_cypher('MATCH (a:A) WHERE a.x = "one; two" RETURN a.x')
    parse_cypher("MATCH (a:A) // first; then\nRETURN a.x")


def test_two_writes_in_one_query_are_refused():
    from provisa.cypher.write_translator import CypherWriteParseError, parse_cypher_write

    with pytest.raises(CypherWriteParseError, match="but got: 2"):
        parse_cypher_write("CREATE (a:A {x: 1}); CREATE (b:A {x: 2})")


@pytest.mark.parametrize(
    ("query", "direction"),
    [
        ("MATCH (a:A)--(b:A) RETURN a", "none"),
        ("MATCH (a:A)-->(b:A) RETURN a", "right"),
        ("MATCH (a:A)<--(b:A) RETURN a", "left"),
        ("MATCH (a:A)<-->(b:A) RETURN a", "none"),
    ],
)
def test_a_relationship_written_without_brackets_is_read(query, direction):
    """openCypher TCK: ``(me)--(you)`` is a relationship of any type, either direction."""
    rel = parse_cypher(query).match_clauses[0].pattern.rels[0]
    assert (rel.types, rel.direction) == ([], direction)


def test_a_single_dash_between_nodes_is_not_a_relationship():
    with pytest.raises(CypherParseError):
        parse_cypher("MATCH (a:A)-(b:A) RETURN a")


def test_a_statement_that_is_not_cypher_is_invalid_input_as_neo4j_says_it():
    """A Neo4j client reads a syntax error by its words (the driver's cypher-error example):
    the token the statement cannot start with is "Invalid input"."""
    with pytest.raises(CypherParseError, match=r"^Invalid input 'SELECT'"):
        parse_cypher("SELECT * FROM Employees WHERE name = $name")
