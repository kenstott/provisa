# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""RDF values in the graph IR (REQ-2135): subject IRIs built outside SQL and parsed back, RDF terms
in the answer, typed literals, a variable predicate's bounded union, and CONSTRUCT's triples. The
SQL runs on DuckDB, so each term is read back from rows.

No front end builds these yet (SPARQL's will), so each test builds the IR from a Cypher statement
it then edits.
"""

# Requirements: REQ-2135

import dataclasses
import json

import duckdb
import pytest

from provisa.cypher.parser import parse_cypher
from provisa.gql.assembler import assemble_rows
from provisa.gql.expr_ast import (
    Binary,
    IriConst,
    Property,
    Term,
    TypedLiteral,
    Variable,
)
from provisa.gql.ir import (
    CypherAST,
    MatchClause,
    MatchStep,
    Predicate,
    ReturnItem,
    TriplePattern,
    TripleTemplate,
    WhereClause,
)
from provisa.gql.iri import IriBaseUnset, SubjectRef, parse_subject_iri, subject_iri
from provisa.gql.label_map import CypherLabelMap
from provisa.gql.translator import cypher_to_sql
from provisa.gql.translator_types import CypherTranslateError, GraphVarKind
from tests.unit.test_cypher_min_hops import _node, _rel

BASE = "https://data.example.com"
EX = "http://example.org/"
XSD = "http://www.w3.org/2001/XMLSchema#"


@pytest.fixture(autouse=True)
def public_url(monkeypatch):
    monkeypatch.setenv("PROVISA_PUBLIC_URL", BASE + "/")


def _model() -> CypherLabelMap:
    person = dataclasses.replace(
        _node("Person", "persons"), domain_id="hr", properties={"name": "name", "age": "age"}
    )
    return CypherLabelMap(
        nodes={"Person": person},
        relationships={"KNOWS": _rel("KNOWS", "Person", "Person", "knows_id")},
    )


@pytest.fixture
def con() -> duckdb.DuckDBPyConnection:
    c = duckdb.connect()
    c.execute("ATTACH ':memory:' AS pg")
    c.execute("CREATE SCHEMA pg.public")
    c.execute(
        "CREATE TABLE pg.public.persons (id INTEGER, name VARCHAR, age INTEGER, knows_id INTEGER)"
    )
    c.execute("INSERT INTO pg.public.persons VALUES (1, 'Ann', 41, 2), (2, 'Bo', NULL, NULL)")
    return c


def _run(con, ast: CypherAST) -> list[dict]:
    sql, _, graph_vars = cypher_to_sql(ast, _model(), {})
    cur = con.execute(sql.sql(dialect="duckdb"))
    names = [d[0] for d in cur.description]
    return assemble_rows([dict(zip(names, r)) for r in cur.fetchall()], graph_vars)


def _match(cypher: str) -> tuple[CypherAST, MatchStep]:
    ast = parse_cypher(cypher)
    step = ast.pipeline[0]
    assert isinstance(step, MatchStep)
    return ast, step


# --- subject IRIs --------------------------------------------------------------------------


@pytest.mark.parametrize(
    "keys",
    [
        [42],
        ["a/b"],
        ["semi;colon"],
        ["with space"],
        ["Zoë", "東京"],
        ["a/b", "c;d", "e f"],
        ["100%", "?#&="],
    ],
)
def test_a_subject_iri_round_trips_whatever_its_keys_hold(keys):
    iri = subject_iri(BASE, "hr", "persons", keys)
    assert iri.startswith(f"{BASE}/id/hr/persons/")
    tail = iri[len(f"{BASE}/id/") :]
    assert " " not in tail and tail.count("/") == 2 and tail.count(";") == len(keys) - 1
    assert parse_subject_iri(BASE, iri) == SubjectRef("hr", "persons", tuple(str(k) for k in keys))


def test_an_iri_under_another_base_names_no_subject():
    assert parse_subject_iri(BASE, "http://elsewhere.org/id/hr/persons/1") is None
    assert parse_subject_iri(BASE, f"{BASE}/id/hr/persons") is None


def test_a_subject_is_refused_while_the_public_url_is_unset(monkeypatch):
    monkeypatch.delenv("PROVISA_PUBLIC_URL")
    ast, _ = _match("MATCH (a:Person) RETURN a")
    ast.return_clause.items = [ReturnItem(Term(Variable("a")), "s", name="s")]  # pyright: ignore[reportOptionalMemberAccess]
    with pytest.raises(IriBaseUnset, match="server.public_url"):
        cypher_to_sql(ast, _model(), {})


# --- terms ---------------------------------------------------------------------------------


def test_a_node_returns_as_its_table_and_typed_key(con):
    ast, _ = _match("MATCH (a:Person) RETURN a")
    ast.return_clause.items = [ReturnItem(Term(Variable("a")), "s", name="s")]  # pyright: ignore[reportOptionalMemberAccess]
    rows = _run(con, ast)
    assert sorted(r["s"]["node"]["key"][0] for r in rows) == [1, 2]
    assert rows[0]["s"]["node"]["domain"] == "hr" and rows[0]["s"]["node"]["table"] == "persons"
    node = rows[0]["s"]["node"]
    assert subject_iri(BASE, node["domain"], node["table"], node["key"]).startswith(BASE)


def test_a_literal_term_carries_its_datatype_and_a_null_is_no_term(con):
    ast, _ = _match("MATCH (a:Person) RETURN a")
    ast.return_clause.items = [  # pyright: ignore[reportOptionalMemberAccess]
        ReturnItem(Term(Property(Variable("a"), "age"), datatype=f"{XSD}integer"), "o", name="o")
    ]
    terms = [r["o"] for r in _run(con, ast)]
    assert {"literal": 41, "datatype": f"{XSD}integer"} in terms and None in terms


def test_a_typed_literal_compares_as_its_sql_value(con):
    ast, _ = _match("MATCH (a:Person) RETURN a.name AS n")
    ast.pipeline[0].where = WhereClause(  # pyright: ignore[reportAttributeAccessIssue]
        Binary(">", Property(Variable("a"), "age"), TypedLiteral("40", f"{XSD}integer"))
    )
    assert [r["n"] for r in _run(con, ast)] == ["Ann"]


def test_an_iri_constant_is_not_compared_in_sql():
    ast, _ = _match("MATCH (a:Person) RETURN a.name AS n")
    ast.pipeline[0].where = WhereClause(  # pyright: ignore[reportAttributeAccessIssue]
        Binary("=", Property(Variable("a"), "name"), IriConst(f"{EX}x"))
    )
    with pytest.raises(CypherTranslateError, match="compared through its key"):
        cypher_to_sql(ast, _model(), {})


def test_a_literal_of_an_unknown_datatype_is_refused():
    ast, _ = _match("MATCH (a:Person) RETURN a.name AS n")
    ast.pipeline[0].where = WhereClause(  # pyright: ignore[reportAttributeAccessIssue]
        Binary("=", Property(Variable("a"), "name"), TypedLiteral("x", f"{EX}custom"))
    )
    with pytest.raises(CypherTranslateError, match="no SQL value"):
        cypher_to_sql(ast, _model(), {})


# --- variable predicates -------------------------------------------------------------------

_PREDICATES = (
    Predicate(f"{EX}name", property="name", datatype=f"{XSD}string"),
    Predicate(f"{EX}age", property="age", datatype=f"{XSD}integer"),
    Predicate(f"{EX}knows", rel_type="KNOWS"),
)


def _spo() -> CypherAST:
    ast, step = _match("MATCH (a:Person) RETURN a")
    step.matches.append(MatchClause(TriplePattern("a", "p", "o", _PREDICATES), None))
    ast.return_clause.items = [  # pyright: ignore[reportOptionalMemberAccess]
        ReturnItem(Property(Variable("a"), "name"), "who", name="who"),
        ReturnItem(Variable("p"), None, name="p"),
        ReturnItem(Variable("o"), None, name="o"),
    ]
    return ast


def test_a_variable_predicate_reads_every_mapped_predicate(con):
    rows = _run(con, _spo())
    got = sorted((r["who"], r["p"]["iri"], json.dumps(r["o"], sort_keys=True)) for r in rows)
    knows_bo = {"node": {"domain": "hr", "key": [2], "table": "persons"}}
    assert got == sorted(
        [
            ("Ann", f"{EX}name", json.dumps({"datatype": f"{XSD}string", "literal": "Ann"})),
            ("Ann", f"{EX}age", json.dumps({"datatype": f"{XSD}integer", "literal": 41})),
            ("Ann", f"{EX}knows", json.dumps(knows_bo, sort_keys=True)),
            ("Bo", f"{EX}name", json.dumps({"datatype": f"{XSD}string", "literal": "Bo"})),
        ]
    )


def test_the_triple_variables_are_terms():
    _, _, graph_vars = cypher_to_sql(_spo(), _model(), {})
    assert graph_vars["p"] == graph_vars["o"] == GraphVarKind.TERM


def test_a_variable_predicate_past_the_union_limit_is_refused(monkeypatch):
    monkeypatch.setenv("PROVISA_GRAPH_UNION_LIMIT", "2")
    with pytest.raises(CypherTranslateError, match="graph union limit of 2"):
        cypher_to_sql(_spo(), _model(), {})


def test_a_predicate_of_a_property_the_label_lacks_is_refused():
    ast, step = _match("MATCH (a:Person) RETURN a")
    step.matches.append(
        MatchClause(TriplePattern("a", "p", "o", (Predicate(f"{EX}x", property="nope"),)), None)
    )
    with pytest.raises(CypherTranslateError, match="no property nope"):
        cypher_to_sql(ast, _model(), {})


# --- CONSTRUCT -----------------------------------------------------------------------------


def test_construct_returns_the_template_triples(con):
    ast, _ = _match("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a")
    ast.return_clause = None
    ast.construct = [
        TripleTemplate(Variable("a"), IriConst(f"{EX}knows"), Variable("b")),
        TripleTemplate(
            Variable("a"),
            IriConst(f"{EX}name"),
            Term(Property(Variable("a"), "name"), datatype=f"{XSD}string"),
        ),
    ]
    rows = _run(con, ast)
    ann = {"node": {"domain": "hr", "table": "persons", "key": [1]}}
    bo = {"node": {"domain": "hr", "table": "persons", "key": [2]}}
    assert sorted(json.dumps(r, sort_keys=True) for r in rows) == sorted(
        json.dumps(t, sort_keys=True)
        for t in (
            {"s": ann, "p": {"iri": f"{EX}knows"}, "o": bo},
            {
                "s": ann,
                "p": {"iri": f"{EX}name"},
                "o": {"literal": "Ann", "datatype": f"{XSD}string"},
            },
        )
    )
