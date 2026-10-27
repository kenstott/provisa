# Copyright (c) 2026 Kenneth Stott
# Canary: 442b97cf-1987-4007-a710-0c24b1807569
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The SPARQL front end (REQ-2135): a query parsed by rdflib, lowered to the graph IR, compiled by
the back end Cypher shares, and run on DuckDB over a small model, its answer read back as RDF
terms -- so each query is checked by what it returns."""

# Requirements: REQ-2135, REQ-2141

import dataclasses

import duckdb
import pytest

from provisa.gql.assembler import assemble_rows, take_cut
from provisa.gql.iri import subject_iri
from provisa.gql.graph_rewriter import apply_graph_rewrites
from provisa.gql.label_map import CypherLabelMap
from provisa.gql.translator import cypher_to_sql
from provisa.sparql_query.compiler import SparqlRefused, compile_sparql
from provisa.transpiler.transpile import transpile_to_duckdb
from provisa.sparql_query.vocabulary import (
    XSD,
    Grounding,
    build_vocabulary,
    column_iri,
    edge_iri,
    table_class_iri,
    term_iri,
)
from tests.unit.test_cypher_min_hops import _node, _rel

BASE = "https://data.example.com"
PERSON = table_class_iri(BASE, "hr", "persons")
COMPANY = table_class_iri(BASE, "hr", "companies")
NAME = column_iri(BASE, "hr", "persons", "name")
AGE = column_iri(BASE, "hr", "persons", "age")
CNAME = column_iri(BASE, "hr", "companies", "name")
KNOWS = edge_iri(BASE, "KNOWS")
WORKS_AT = edge_iri(BASE, "WORKS_AT")
PARENT = edge_iri(BASE, "PARENT")
EMPLOYEE = term_iri(BASE, "Employee")
FULL_NAME = term_iri(BASE, "Legal name")
PREFIXES = f"""
PREFIX p: <{PERSON}#>
PREFIX c: <{COMPANY}#>
PREFIX rel: <{BASE}/vocab/rel/>
PREFIX term: <{BASE}/vocab/term/>
PREFIX xsd: <{XSD}>
"""


@pytest.fixture(autouse=True)
def public_url(monkeypatch):
    monkeypatch.setenv("PROVISA_PUBLIC_URL", BASE)


def _model() -> CypherLabelMap:
    person = dataclasses.replace(
        _node("Person", "persons"),
        domain_id="hr",
        properties={"id": "id", "name": "name", "age": "age"},
    )
    company = dataclasses.replace(
        _node("Company", "companies"), domain_id="hr", properties={"id": "id", "name": "name"}
    )
    return CypherLabelMap(
        nodes={"Person": person, "Company": company},
        relationships={
            "KNOWS": _rel("KNOWS", "Person", "Person", "knows_id"),
            "WORKS_AT": _rel("WORKS_AT", "Person", "Company", "company_id"),
            "PARENT": _rel("PARENT", "Company", "Company", "parent_id"),
        },
    )


_TYPES = {
    ("Person", "id"): "INTEGER",
    ("Person", "name"): "VARCHAR",
    ("Person", "age"): "INTEGER",
    ("Company", "id"): "INTEGER",
    ("Company", "name"): "VARCHAR",
}
_GROUNDING = Grounding(
    class_terms={"Person": "Employee"},
    property_terms={("Person", "name"): ("Legal name",), ("Company", "name"): ("Legal name",)},
)


@pytest.fixture
def con() -> duckdb.DuckDBPyConnection:
    """Ann (41) knows Bo; Bo knows Cy (29); Cy knows no one, and nor does Di (no age). Ann and Bo
    work at Acme, Cy at Beta; Acme's parent is Beta, Beta's is Corp."""
    c = duckdb.connect()
    c.execute("ATTACH ':memory:' AS pg")
    c.execute("CREATE SCHEMA pg.public")
    c.execute(
        "CREATE TABLE pg.public.persons (id INTEGER, name VARCHAR, age INTEGER, knows_id INTEGER, "
        "company_id INTEGER)"
    )
    c.execute(
        "INSERT INTO pg.public.persons VALUES (1, 'Ann', 41, 2, 1), (2, 'Bo', 35, 3, 1), "
        "(3, 'Cy', 29, NULL, 2), (4, 'Di', NULL, NULL, NULL)"
    )
    c.execute("CREATE TABLE pg.public.companies (id INTEGER, name VARCHAR, parent_id INTEGER)")
    c.execute(
        "INSERT INTO pg.public.companies VALUES (1, 'Acme', 2), (2, 'Beta', 3), (3, 'Corp', NULL)"
    )
    return c


def _value(term):
    """An answer's term as a test reads it: a subject as its IRI, an IRI as itself, a literal as
    its value."""
    if term is None:
        return None
    if "node" in term:
        n = term["node"]
        return subject_iri(BASE, n["domain"], n["table"], n["key"])
    if "iri" in term:
        return term["iri"]
    return term["literal"]


def _answer(con, query: str) -> list[dict]:
    lm = _model()
    vocab = build_vocabulary(lm, BASE, _TYPES, _GROUNDING)
    compiled = compile_sparql(PREFIXES + query, vocab, lm)
    if compiled.empty:
        rows = [compiled.empty_row] if compiled.empty_row is not None else []
        return [{k: _value(r.get(k)) for k in compiled.variables} for r in rows]
    assert compiled.ast is not None
    sql, _, graph_vars = cypher_to_sql(compiled.ast, lm, {})
    # As the endpoint runs it (sparql_router): a term is its own value through the graph rewrite.
    sql = apply_graph_rewrites(sql, graph_vars, lm)
    # As the pipeline runs it: the semantic SQL in PostgreSQL's dialect, transpiled to the engine's.
    cur = con.execute(transpile_to_duckdb(sql.sql(dialect="postgres")))
    names = [d[0] for d in cur.description]
    rows = assemble_rows(take_cut([dict(zip(names, r)) for r in cur.fetchall()], []), graph_vars)
    if compiled.form == "ASK":
        return rows
    keep = compiled.variables or ["s", "p", "o"]
    return [{k: _value(r[k]) for k in keep} for r in rows]


def ann() -> str:
    return subject_iri(BASE, "hr", "persons", [1])


def test_a_class_and_a_column_predicate(con):
    rows = _answer(con, f"SELECT ?s ?n WHERE {{ ?s a <{PERSON}> ; p:name ?n }}")
    assert sorted(r["n"] for r in rows) == ["Ann", "Bo", "Cy", "Di"]
    assert ann() in {r["s"] for r in rows}


def test_a_class_term_types_the_table_it_grounds(con):
    rows = _answer(con, "SELECT ?n WHERE { ?s a term:Employee ; p:name ?n }")
    assert sorted(r["n"] for r in rows) == ["Ann", "Bo", "Cy", "Di"]


def test_a_property_term_is_a_predicate_of_every_column_it_grounds(con):
    rows = _answer(con, "SELECT ?n WHERE { ?s term:Legal%20name ?n }".replace("%20", "%20"))
    assert sorted(r["n"] for r in rows) == ["Acme", "Ann", "Beta", "Bo", "Corp", "Cy", "Di"]


def test_a_filter_on_a_typed_literal(con):
    rows = _answer(
        con, 'SELECT ?n WHERE { ?s p:name ?n ; p:age ?a FILTER(?a > "30"^^xsd:integer) }'
    )
    assert sorted(r["n"] for r in rows) == ["Ann", "Bo"]


def test_a_required_property_drops_a_row_without_it_and_optional_keeps_it(con):
    required = _answer(con, "SELECT ?n ?a WHERE { ?s p:name ?n ; p:age ?a }")
    assert "Di" not in {r["n"] for r in required}
    optional = _answer(con, "SELECT ?n ?a WHERE { ?s p:name ?n OPTIONAL { ?s p:age ?a } }")
    assert {r["n"]: r["a"] for r in optional} == {"Ann": 41, "Bo": 35, "Cy": 29, "Di": None}


def test_an_edge_and_the_node_at_its_end(con):
    rows = _answer(con, "SELECT ?n ?cn WHERE { ?s p:name ?n ; rel:WORKS_AT ?c . ?c c:name ?cn }")
    assert sorted((r["n"], r["cn"]) for r in rows) == [
        ("Ann", "Acme"),
        ("Bo", "Acme"),
        ("Cy", "Beta"),
    ]


def test_an_edge_written_from_its_far_end(con):
    rows = _answer(con, "SELECT ?n WHERE { ?c c:name 'Beta' . ?s rel:WORKS_AT ?c ; p:name ?n }")
    assert [r["n"] for r in rows] == ["Cy"]


def test_a_property_path_reaches_each_pair_once(con):
    rows = _answer(con, "SELECT ?a ?b WHERE { ?x rel:PARENT+ ?y . ?x c:name ?a . ?y c:name ?b }")
    assert sorted((r["a"], r["b"]) for r in rows) == [
        ("Acme", "Beta"),
        ("Acme", "Corp"),
        ("Beta", "Corp"),
    ]


def test_a_sequence_path_with_an_inverse(con):
    # who works where someone Ann knows works: Ann knows Bo, Bo works at Acme, Acme employs Ann, Bo
    rows = _answer(
        con,
        "SELECT ?n WHERE { ?s p:name 'Ann' . ?s rel:KNOWS/rel:WORKS_AT/^rel:WORKS_AT ?o . "
        "?o p:name ?n }",
    )
    assert sorted(r["n"] for r in rows) == ["Ann", "Bo"]


def test_a_subject_named_by_its_iri_and_a_variable_predicate(con):
    rows = _answer(con, f"SELECT ?p ?o WHERE {{ <{ann()}> ?p ?o }}")
    got = {(r["p"], r["o"]) for r in rows}
    bo = subject_iri(BASE, "hr", "persons", [2])
    acme = subject_iri(BASE, "hr", "companies", [1])
    assert {
        ("http://www.w3.org/1999/02/22-rdf-syntax-ns#type", PERSON),
        ("http://www.w3.org/1999/02/22-rdf-syntax-ns#type", EMPLOYEE),
        (NAME, "Ann"),
        (FULL_NAME, "Ann"),
        (AGE, 41),
        (KNOWS, bo),
        (WORKS_AT, acme),
    } <= got


def test_filter_on_a_subject_iri(con):
    rows = _answer(con, f"SELECT ?n WHERE {{ ?s p:name ?n FILTER(?s = <{ann()}>) }}")
    assert [r["n"] for r in rows] == ["Ann"]


def test_an_untyped_subject_takes_the_class_of_its_predicates(con):
    rows = _answer(con, "SELECT ?n WHERE { ?s c:name ?n }")
    assert sorted(r["n"] for r in rows) == ["Acme", "Beta", "Corp"]


def test_union(con):
    rows = _answer(con, "SELECT ?n WHERE { { ?s p:name ?n } UNION { ?s c:name ?n } }")
    assert len(rows) == 7


def test_order_by_and_limit(con):
    rows = _answer(con, "SELECT ?n WHERE { ?s p:name ?n } ORDER BY DESC(?n) LIMIT 2")
    assert [r["n"] for r in rows] == ["Di", "Cy"]


def test_order_by_across_two_classes(con):
    rows = _answer(con, "SELECT ?n WHERE { ?s term:Legal%20name ?n } ORDER BY ?n LIMIT 3")
    assert [r["n"] for r in rows] == ["Acme", "Ann", "Beta"]


def test_distinct(con):
    rows = _answer(con, "SELECT DISTINCT ?c WHERE { ?s rel:WORKS_AT ?c }")
    assert len(rows) == 2


def test_ask(con):
    assert _answer(con, "ASK { ?s p:name 'Ann' }") != []
    assert _answer(con, "ASK { ?s p:name 'Zed' }") == []


def test_construct(con):
    rows = _answer(
        con,
        "CONSTRUCT { ?s <http://example.org/colleagueOf> ?o } "
        "WHERE { ?s rel:WORKS_AT ?c . ?o rel:WORKS_AT ?c FILTER(?s != ?o) }",
    )
    bo = subject_iri(BASE, "hr", "persons", [2])
    assert sorted((r["s"], r["o"]) for r in rows) == [(ann(), bo), (bo, ann())]


def test_a_predicate_the_vocabulary_lacks_matches_nothing(con):
    assert _answer(con, "SELECT ?o WHERE { ?s <http://example.org/nope> ?o }") == []


@pytest.mark.parametrize(
    "query, refusal",
    [
        (
            "SELECT (COUNT(?s) AS ?n) WHERE { ?s term:Legal%20name ?x }",
            "several classes",
        ),
        ("SELECT (SAMPLE(?s) AS ?x) WHERE { ?s p:name ?n }", "COUNT it"),
        ("CONSTRUCT { ?s ?s ?s } WHERE { ?s p:name ?n } GROUP BY ?s", "CONSTRUCT"),
        ("SELECT ?s WHERE { VALUES ?s { 1 } }", "VALUES"),
        ("DESCRIBE ?s WHERE { ?s p:name ?n }", "DESCRIBE|Describe"),
        ('SELECT ?n WHERE { ?s p:name ?n FILTER(MD5(?n) = "x") }', "MD5"),
        ('SELECT ?x WHERE { ?s p:name ?n BIND(REPLACE(?n, "a", "b", "i") AS ?x) }', "flags i"),
        ('SELECT ?x WHERE { ?s p:name ?n BIND(REPLACE(?n, "(?=a)", "b") AS ?x) }', "pattern"),
        ('SELECT ?x WHERE { ?s p:name ?n BIND(REPLACE(?n, ?n, "b") AS ?x) }', "literals"),
        ("SELECT ?x WHERE { ?s p:name ?n BIND(isNUMERIC(STRLEN(?n)) AS ?x) }", "isNUMERIC"),
        ("SELECT ?x WHERE { ?s p:name ?n BIND(<urn:f>(?n) AS ?x) }", "urn:f"),
    ],
)
def test_what_is_not_lowered_is_refused_by_name(query, refusal):
    lm = _model()
    vocab = build_vocabulary(lm, BASE, _TYPES, _GROUNDING)
    with pytest.raises(SparqlRefused, match=refusal):
        compile_sparql(PREFIXES + query, vocab, lm)


def test_grounding_and_column_types_come_from_the_model_s_own_references():
    from types import SimpleNamespace

    from provisa.sparql_query.vocabulary import column_types_of, grounding_from_refs

    lm = _model()
    person_id = lm.nodes["Person"].table_id
    grounding = grounding_from_refs(
        lm, {person_id: "Employee", 99999: "Ghost"}, {(person_id, "name"): ["Legal name"]}
    )
    assert grounding.class_terms == {"Person": "Employee"}
    assert grounding.property_terms == {("Person", "name"): ("Legal name",)}
    ctx = SimpleNamespace(aggregate_columns={person_id: [("age", "INTEGER"), ("gone", "TEXT")]})
    assert column_types_of(lm, ctx) == {("Person", "age"): "INTEGER"}


# --- aggregates ----------------------------------------------------------------------------


def test_count_per_group(con):
    rows = _answer(con, "SELECT ?c (COUNT(?s) AS ?n) WHERE { ?s rel:WORKS_AT ?c } GROUP BY ?c")
    acme, beta = (subject_iri(BASE, "hr", "companies", [k]) for k in (1, 2))
    assert sorted((r["c"], r["n"]) for r in rows) == [(acme, 2), (beta, 1)]


def test_having(con):
    rows = _answer(
        con,
        "SELECT ?c WHERE { ?s rel:WORKS_AT ?c } GROUP BY ?c HAVING (COUNT(?s) > 1)",
    )
    assert [r["c"] for r in rows] == [subject_iri(BASE, "hr", "companies", [1])]


def test_sum_avg_min_max_over_one_group(con):
    rows = _answer(
        con,
        "SELECT (SUM(?a) AS ?t) (AVG(?a) AS ?m) (MIN(?a) AS ?lo) (MAX(?a) AS ?hi) "
        "WHERE { ?s p:age ?a }",
    )
    assert [(r["t"], float(r["m"]), r["lo"], r["hi"]) for r in rows] == [(105, 35.0, 29, 41)]


def test_count_distinct_and_count_star(con):
    rows = _answer(
        con,
        "SELECT (COUNT(DISTINCT ?c) AS ?d) (COUNT(*) AS ?n) WHERE { ?s rel:WORKS_AT ?c }",
    )
    assert [(r["d"], r["n"]) for r in rows] == [(2, 3)]


def test_group_concat_in_the_order_of_its_text(con):
    rows = _answer(
        con,
        'SELECT ?cn (GROUP_CONCAT(?n; separator=", ") AS ?who) '
        "WHERE { ?s p:name ?n ; rel:WORKS_AT ?c . ?c c:name ?cn } GROUP BY ?cn",
    )
    assert sorted((r["cn"], r["who"]) for r in rows) == [("Acme", "Ann, Bo"), ("Beta", "Cy")]


def test_group_by_a_value_and_order_by_its_count(con):
    rows = _answer(
        con,
        "SELECT ?cn (COUNT(?s) AS ?n) WHERE { ?s rel:WORKS_AT ?c . ?c c:name ?cn } "
        "GROUP BY ?cn ORDER BY DESC(?n) LIMIT 1",
    )
    assert [(r["cn"], r["n"]) for r in rows] == [("Acme", 2)]


def test_an_aggregate_over_no_solution_is_one_row(con):
    rows = _answer(con, "SELECT (COUNT(*) AS ?n) WHERE { ?s <http://example.org/nope> ?o }")
    assert rows == [{"n": 0}]


def _sql(con, sql: str) -> list[tuple]:
    """The equivalent SQL's answer, over the same tables: what each lowering is checked by."""
    return sorted(con.execute(sql).fetchall())


def test_a_negated_set_of_forward_and_inverse_edges_is_either(con):
    # !(KNOWS|^WORKS_AT) from a person: any edge out of it but KNOWS (its WORKS_AT), or any edge
    # into it but WORKS_AT (a KNOWS of someone who knows it).
    rows = _answer(
        con,
        f"SELECT ?s ?o WHERE {{ ?s a <{PERSON}> . ?s !(rel:KNOWS|^rel:WORKS_AT) ?o }}",
    )
    got = sorted((r["s"], r["o"]) for r in rows)
    out = _sql(con, "SELECT id, company_id FROM pg.public.persons WHERE company_id IS NOT NULL")
    back = _sql(
        con,
        "SELECT p.id, q.id FROM pg.public.persons p JOIN pg.public.persons q ON q.knows_id = p.id",
    )
    want = sorted(
        [
            (subject_iri(BASE, "hr", "persons", [s]), subject_iri(BASE, "hr", "companies", [o]))
            for s, o in out
        ]
        + [
            (subject_iri(BASE, "hr", "persons", [s]), subject_iri(BASE, "hr", "persons", [o]))
            for s, o in back
        ]
    )
    assert got == want and want


def test_a_negated_set_of_inverse_edges_alone_walks_backward(con):
    # !(^WORKS_AT) into a person: an edge into it that is not WORKS_AT -- someone who knows it.
    rows = _answer(con, f"SELECT ?s ?o WHERE {{ ?s a <{PERSON}> . ?s !(^rel:WORKS_AT) ?o }}")
    back = _sql(
        con,
        "SELECT p.id, q.id FROM pg.public.persons p JOIN pg.public.persons q ON q.knows_id = p.id",
    )
    person = lambda k: subject_iri(BASE, "hr", "persons", [k])  # noqa: E731
    assert sorted((r["s"], r["o"]) for r in rows) == [(person(s), person(o)) for s, o in back]


@pytest.mark.parametrize(
    "regex, sql",
    [
        ('REGEX(?n, "n")', "regexp_matches(name, 'n')"),  # a match anywhere in the text
        ('REGEX(?n, "^a", "i")', "regexp_matches(name, '^a', 'i')"),
        ('REGEX(?n, "Y$", "i")', "regexp_matches(name, 'Y$', 'i')"),
        ('REGEX(?n, "a.n", "q")', "contains(name, 'a.n')"),  # q: the pattern is its text
        ('REGEX(?n, "^ a n", "ix")', "regexp_matches(name, '^an', 'i')"),  # x: spaces ignored
    ],
)
def test_regex_searches_the_text_under_its_flags(con, regex, sql):
    rows = _answer(con, f"SELECT ?n WHERE {{ ?s p:name ?n FILTER({regex}) }}")
    want = _sql(con, f"SELECT name FROM pg.public.persons WHERE {sql}")
    assert sorted((r["n"],) for r in rows) == want


def test_a_regex_pattern_read_at_run_time_is_searched_for_too(con):
    rows = _answer(
        con, 'SELECT ?n WHERE { ?s p:name ?n BIND("o" AS ?pat) FILTER(REGEX(?n, ?pat, "i")) }'
    )
    want = _sql(con, "SELECT name FROM pg.public.persons WHERE regexp_matches(name, 'o', 'i')")
    assert sorted((r["n"],) for r in rows) == want and want


def test_group_by_an_expression_groups_by_its_value(con):
    rows = _answer(
        con, "SELECT (COUNT(?s) AS ?n) WHERE { ?s p:age ?a } GROUP BY (?a > 30) ORDER BY ?n"
    )
    want = _sql(
        con, "SELECT count(*) FROM pg.public.persons WHERE age IS NOT NULL GROUP BY age > 30"
    )
    assert [(r["n"],) for r in rows] == want


@pytest.mark.parametrize(
    "sparql, sql",
    [
        ("STRLEN(?n)", "length(name)"),
        ('CONCAT(?n, "-", STR(?a))', "name || '-' || CAST(age AS VARCHAR)"),
        ("CONCAT()", "''"),
        ("SUBSTR(?n, 2)", "substr(name, 2)"),
        ("SUBSTR(?n, 1, 2)", "substr(name, 1, 2)"),
        ('REPLACE(?n, "[aeiou]", "_")', "regexp_replace(name, '[aeiou]', '_', 'g')"),
        ('REPLACE(?n, "(.)(.)", "$2$1")', "regexp_replace(name, '(.)(.)', '\\2\\1', 'g')"),
        ('REPLACE(?n, "n.", "_", "q")', "replace(name, 'n.', '_')"),
        ('REPLACE(?n, "^ A", "_", "x")', "regexp_replace(name, '^A', '_')"),
        (
            'STRBEFORE(?n, "n")',
            "CASE WHEN strpos(name, 'n') = 0 THEN '' "
            "ELSE substr(name, 1, strpos(name, 'n') - 1) END",
        ),
        (
            'STRAFTER(?n, "n")',
            "CASE WHEN strpos(name, 'n') = 0 THEN '' ELSE substr(name, strpos(name, 'n') + 1) END",
        ),
        ('STRAFTER(?n, "")', "name"),
        (
            'STRBEFORE(LCASE(SUBSTR(?n, 2)), "y")',
            "CASE WHEN strpos(lower(substr(name, 2)), 'y') = 0 "
            "THEN '' ELSE substr(lower(substr(name, 2)), 1, strpos(lower(substr(name, 2)), 'y') - 1) END",
        ),
        ("CEIL(?a / 10)", "ceil(age / 10)"),
        ("FLOOR(?a / 10)", "floor(age / 10)"),
        ("ROUND(?a / 10)", "floor(age / 10 + 0.5)"),  # a half rounds up
        ("ROUND(-?a / 10)", "floor(-age / 10 + 0.5)"),
        ("isNUMERIC(?a)", "true"),
        ("isNUMERIC(?n)", "false"),
        ('isNUMERIC("1"^^xsd:integer)', "true"),
        ("xsd:integer(STR(?a))", "TRY_CAST(CAST(age AS VARCHAR) AS BIGINT)"),
        ("xsd:double(?a)", "CAST(age AS DOUBLE)"),
        ("xsd:decimal(?a)", "CAST(age AS DECIMAL)"),
        ("xsd:string(?a)", "CAST(age AS VARCHAR)"),
        ("xsd:boolean(?a)", "CAST(age AS BOOLEAN)"),
        ("xsd:integer(?n)", "TRY_CAST(name AS BIGINT)"),
        ("xsd:integer(SUBSTR(STR(?a), 2))", "TRY_CAST(substr(CAST(age AS VARCHAR), 2) AS BIGINT)"),
    ],
)
def test_a_scalar_function_answers_as_its_sql_does(con, sparql, sql):
    rows = _answer(
        con,
        f"SELECT ?s ?x WHERE {{ ?s p:name ?n ; p:age ?a BIND({sparql} AS ?x) }}",
    )
    want = _sql(con, f"SELECT id, {sql} FROM pg.public.persons WHERE age IS NOT NULL")
    assert sorted((r["s"], r["x"]) for r in rows) == [
        (subject_iri(BASE, "hr", "persons", [i]), x) for i, x in want
    ]


@pytest.mark.parametrize(
    "part, field",
    [
        ("YEAR", "year"),
        ("MONTH", "month"),
        ("DAY", "day"),
        ("HOURS", "hour"),
        ("MINUTES", "minute"),
        ("SECONDS", "second"),
    ],
)
def test_a_date_part_of_a_date_time(con, part, field):
    at = "2024-03-05T10:20:30"
    rows = _answer(
        con, f'SELECT ?x WHERE {{ ?s p:name "Ann" BIND({part}("{at}"^^xsd:dateTime) AS ?x) }}'
    )
    want = _sql(con, f"SELECT extract({field} FROM TIMESTAMP '{at}')")
    assert [(r["x"],) for r in rows] == want


def test_a_date_part_of_a_cast(con):
    rows = _answer(
        con, 'SELECT ?x WHERE { ?s p:name "Ann" BIND(YEAR(xsd:date("2024-03-05")) AS ?x) }'
    )
    assert [r["x"] for r in rows] == [2024]


def test_now_is_one_instant_for_the_whole_query(con):
    rows = _answer(con, "SELECT ?s ?x WHERE { ?s p:name ?n BIND(YEAR(NOW()) AS ?x) }")
    assert {r["x"] for r in rows} == {_sql(con, "SELECT year(now())")[0][0]}


def test_a_bind_after_an_optional_reads_what_the_optional_binds(con):
    rows = _answer(
        con, "SELECT ?n ?x WHERE { ?s p:name ?n OPTIONAL { ?s p:age ?a } BIND(?a + 1 AS ?x) }"
    )
    want = _sql(con, "SELECT name, age + 1 FROM pg.public.persons")
    assert sorted((r["n"], r["x"]) for r in rows) == want


def test_a_filter_beside_an_optional_reads_what_the_optional_binds(con):
    rows = _answer(
        con, "SELECT ?n ?a WHERE { ?s p:name ?n OPTIONAL { ?s p:age ?a } FILTER(?a > 30) }"
    )
    want = _sql(con, "SELECT name, age FROM pg.public.persons WHERE age > 30")
    assert sorted((r["n"], r["a"]) for r in rows) == want and want


def test_a_filter_on_whether_an_optional_matched(con):
    rows = _answer(
        con, "SELECT ?n WHERE { ?s p:name ?n OPTIONAL { ?s p:age ?a } FILTER(!BOUND(?a)) }"
    )
    assert [r["n"] for r in rows] == ["Di"]


def test_a_bind_before_an_optional_is_read_inside_it(con):
    rows = _answer(
        con,
        'SELECT ?n ?cn WHERE { ?s p:name ?n BIND("Acme" AS ?skip) '
        "OPTIONAL { ?s rel:WORKS_AT ?c . ?c c:name ?cn FILTER(?cn != ?skip) } }",
    )
    assert sorted((r["n"], r["cn"]) for r in rows) == [
        ("Ann", None),
        ("Bo", None),
        ("Cy", "Beta"),
        ("Di", None),
    ]


def test_an_optional_on_a_matched_subject_keeps_the_rows_its_filter_refuses(con):
    rows = _answer(
        con, "SELECT ?n ?a WHERE { ?s p:name ?n OPTIONAL { ?s p:age ?a FILTER(?a > 30) } }"
    )
    want = _sql(con, "SELECT name, CASE WHEN age > 30 THEN age END FROM pg.public.persons")
    assert sorted(((r["n"], r["a"]) for r in rows), key=str) == sorted(want, key=str)


def test_an_optional_on_a_matched_subject_binds_nothing_where_one_of_its_triples_fails(con):
    # Di has no age: the OPTIONAL as a whole does not match, so its ?m is unbound too.
    rows = _answer(con, "SELECT ?n ?m WHERE { ?s p:name ?n OPTIONAL { ?s p:name ?m ; p:age ?a } }")
    want = _sql(con, "SELECT name, CASE WHEN age IS NOT NULL THEN name END FROM pg.public.persons")
    assert sorted(((r["n"], r["m"]) for r in rows), key=str) == sorted(want, key=str)


def test_an_optional_reaching_a_new_node_binds_nothing_where_it_does_not_match(con):
    # Only Cy works at Beta: Ann's and Bo's ?a, read off the matched subject, is unbound too.
    rows = _answer(
        con,
        'SELECT ?n ?a WHERE { ?s p:name ?n OPTIONAL { ?s p:age ?a ; rel:WORKS_AT ?c . ?c c:name "Beta" } }',
    )
    assert sorted(((r["n"], r["a"]) for r in rows), key=str) == sorted(
        [("Ann", None), ("Bo", None), ("Cy", 29), ("Di", None)], key=str
    )


def _rows(con, query: str) -> list[tuple]:
    return sorted((tuple(r.values()) for r in _answer(con, query)), key=repr)


def test_an_optional_nested_in_an_optional_extends_it(con):
    # REQ-2135: each person's company's name, through the inner OPTIONAL; the inner OPTIONAL is
    # lowered, never dropped.
    rows = _rows(
        con,
        f"SELECT ?n ?cn WHERE {{ ?s a <{PERSON}> ; p:name ?n . OPTIONAL {{ ?s rel:WORKS_AT ?c . "
        f"?c a <{COMPANY}> OPTIONAL {{ ?c c:name ?cn }} }} }}",
    )
    assert rows == [("Ann", "Acme"), ("Bo", "Acme"), ("Cy", "Beta"), ("Di", None)]


def test_a_filter_naming_what_an_inner_optional_binds_is_refused():
    # REQ-2135: an OPTIONAL's FILTER naming a variable only an OPTIONAL inside it binds.
    with pytest.raises(SparqlRefused, match="an OPTIONAL inside it binds"):
        _answer(
            None,
            f"SELECT ?n WHERE {{ ?s a <{PERSON}> ; p:name ?n . OPTIONAL {{ ?s rel:WORKS_AT ?c . "
            f"?c a <{COMPANY}> OPTIONAL {{ ?c c:name ?cn }} FILTER(?cn = 'Acme') }} }}",
        )
