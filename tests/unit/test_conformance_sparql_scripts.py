# Copyright (c) 2026 Kenneth Stott
# Canary: c81f5a36-9e02-4d7b-b6a4-3f28d15e7c90
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The SPARQL conformance job's scripts (REQ-2167): a W3C dataset as fixture tables, a query's
constants mapped onto them, an answer compared with the suite's expected result, and the report
the conformance page is filled from."""

# Requirements: REQ-2167

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest
from rdflib import BNode, Graph, Literal, URIRef, XSD

from provisa.core.models import Domain, GlossaryTermConfig, Relationship, Role, Table

CONFORMANCE = Path(__file__).resolve().parents[2] / "scripts" / "conformance"
sys.path.insert(0, str(CONFORMANCE))  # the scripts' sibling imports


def _load(name: str):
    spec = importlib.util.spec_from_file_location(name, CONFORMANCE / f"{name}.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module  # a dataclass resolves its module by name
    spec.loader.exec_module(module)
    return module


by_design = _load("by_design")
_load("pgwire_catalog_types")
manifest = _load("sparql_w3c_manifest")
fixtures = _load("sparql_w3c_fixtures")
results = _load("sparql_w3c_results")
runner = _load("sparql_w3c")
fill_docs = _load("fill_docs")

EX = "http://example.org/"


def _adapt(turtle: str):
    g = Graph()
    g.parse(data=f"@prefix : <{EX}> . @prefix xsd: <{XSD}> .\n{turtle}", format="turtle")
    return fixtures.adapt("d001", g)


def test_a_subject_is_a_row_a_literal_predicate_a_column_a_resource_predicate_an_edge():
    f = _adapt(":a :name 'Ann' ; :age 41 ; :knows :b . :b :name 'Bo' ; :knows :a, :c .")
    assert f.refusal is None
    (table,) = [t for t in f.tables if t.rows]
    assert {c.sql_type for c in table.columns} == {"text", "bigint"}
    assert len(table.rows) == 3  # :c, named only as an object, is a row
    (edge,) = f.edges
    assert sorted(edge.rows) == sorted(set(edge.rows)) and len(edge.rows) == 3
    assert f.term_map()[f"{EX}knows"] == f"{fixtures.BASE}/vocab/rel/D001_P0"
    assert f.term_map()[f"{EX}a"].startswith(f"{fixtures.BASE}/id/sparql_d001/d001_t0/")


def test_a_predicate_of_several_datatypes_splits_its_subjects_under_one_term():
    f = _adapt(":a :v 1 . :b :v 2.5 . :c :v 'x' .")
    held = [t for t in f.tables if t.columns]
    assert len(held) == 3 and len({t.columns[0].name for t in held}) == 1
    assert {t.columns[0].sql_type for t in held} == {"bigint", "numeric", "text"}
    assert f.term_map()[f"{EX}v"] == f"{fixtures.BASE}/vocab/term/d001%20p0"


def test_a_class_is_a_table_and_a_class_spanning_tables_a_term():
    one = _adapt(":a a :Person ; :n 'x' .")
    assert one.term_map()[f"{EX}Person"].startswith(f"{fixtures.BASE}/vocab/sparql_d001/")
    two = _adapt(":a a :P ; :v 1 . :b a :P ; :v 'x' .")
    assert two.term_map()[f"{EX}P"] == f"{fixtures.BASE}/vocab/term/d001%20c0"
    assert {t.class_term for t in two.tables if t.rows} == {"d001 c0"}


@pytest.mark.parametrize(
    ("turtle", "said"),
    [
        (":a :p 'x'@en .", "language-tagged"),
        (":a :p 1, 2 .", "several values"),
        (":a :p 1 ; :p :b .", "both literals and resources"),
        (":a a :C, :D .", "several classes"),
        (":a :p 'x'^^:custom .", "datatype"),
        (":a :p 'one'^^xsd:integer .", "ill-typed"),
    ],
)
def test_a_dataset_the_tables_cannot_state_is_refused_never_approximated(turtle, said):
    f = _adapt(turtle)
    assert f.refusal is not None and said in f.refusal
    assert not f.tables


def test_the_model_entries_load_as_the_models_own():
    f = _adapt(":a a :C ; :v 1 ; :k :b . :b a :C ; :v 'x' ; :k :c . :c :w 1.5 .")
    for entry in fixtures.table_entries(f):
        Table(
            **entry,
        )
    for entry in fixtures.relationship_entries(f):
        Relationship(**entry)
    access = fixtures.access([f])
    assert [Domain(**d).id for d in access["domains"]] == ["sparql_d001"]
    assert [Role(**r).id for r in access["roles"]] == ["sparql_d001"]
    assert {GlossaryTermConfig(**t).name for t in access["glossary_terms"]} == {
        "d001 p0",
        "d001 c0",
    }


def test_pgwire_config_takes_the_jobs_assignments_and_terms(tmp_path, monkeypatch):
    import yaml

    types = sys.modules["pgwire_catalog_types"]
    monkeypatch.setattr(types, "registrable_columns", lambda dsn: [])
    f = _adapt(":a :v 1 . :b :v 'x' .")
    access = tmp_path / "access.yaml"
    access.write_text(yaml.safe_dump(fixtures.access([f])))
    out = tmp_path / "provisa.yaml"
    types.write_config(out, 5432, "dsn", extra_access=access)
    config = yaml.safe_load(out.read_text())
    assert {"domain_id": "*", "role_id": "sparql_d001"} in config["auth"]["default_assignments"]
    assert {"domain_id": "*", "role_id": "org_admin"} in config["auth"]["default_assignments"]
    assert [t["name"] for t in config["glossary_terms"]] == ["d001 p0"]


TERMS = {f"{EX}p": "http://m/p", f"{EX}a": "http://m/a", "http://base/rel": "http://m/rel"}


@pytest.mark.parametrize(
    ("query", "want"),
    [
        (f"PREFIX : <{EX}> SELECT * {{ :a :p ?o }}", "<http://m/a> <http://m/p> ?o"),
        (f"PREFIX ex: <{EX}>\nASK {{ ?s ex:p ex:a. }}", "?s <http://m/p> <http://m/a>."),
        (f"SELECT * {{ <{EX}a> ?p ?o }}", "<http://m/a> ?p ?o"),
        ("BASE <http://base/> SELECT * { ?s <rel> ?o }", "?s <http://m/rel> ?o"),
    ],
)
def test_a_querys_constants_map_onto_the_fixture(query, want):
    assert want in results.map_query(query, "http://file/q.rq", TERMS)


@pytest.mark.parametrize(
    "query",
    [
        f"PREFIX : <{EX}> SELECT * {{ ?s ?p ':a <{EX}p>' }}",
        f"# <{EX}p>\nSELECT * {{ ?s ?p ?o }}",
        f"PREFIX : <{EX}> SELECT * {{ ?s :other ?o FILTER(?o < 3 && ?o > 1) }}",
    ],
)
def test_strings_comments_and_unmapped_tokens_stay(query):
    assert results.map_query(query, "http://file/q.rq", TERMS) == query


def _lit(v, dt=None) -> Literal:
    return Literal(v, datatype=dt)


def test_solutions_compare_as_multisets_under_one_blank_node_map():
    a, b, x, y = BNode(), BNode(), BNode(), BNode()
    actual = [{"s": a, "o": b}, {"s": b, "o": a}]
    assert results.same_solutions(actual, [{"s": y, "o": x}, {"s": x, "o": y}])
    assert not results.same_solutions(actual, [{"s": x, "o": y}, {"s": x, "o": y}])
    assert not results.same_solutions([{"v": _lit(1)}], [{"v": _lit(1)}, {"v": _lit(1)}])


def test_numbers_compare_by_value_within_a_datatype_and_never_across():
    assert results.same_term(_lit("1.10", XSD.decimal), _lit("1.1", XSD.decimal))
    assert not results.same_term(_lit("1", XSD.integer), _lit("1.0", XSD.decimal))
    assert not results.same_term(_lit("a"), Literal("a", lang="en"))


def test_an_answers_terms_read_back_and_scaffolding_leaves():
    reverse = {"http://m/id/1": URIRef(f"{EX}a"), "http://m/id/2": BNode("b")}
    rows = results.read_rows(
        [
            {"s": URIRef("http://m/id/1"), "p": URIRef("http://m/t#id")},
            {"s": URIRef("http://m/id/2"), "p": URIRef(f"{EX}p")},
        ],
        reverse,
        ("http://m/t#id",),
    )
    assert rows == [{"s": BNode("b"), "p": URIRef(f"{EX}p")}]


def test_a_graph_managed_update_is_by_design_only_with_its_refusal_and_its_group():
    declared = by_design.groups("sparql")
    refused = "400: ADD acts on a named graph, and the model has tables, not graphs"
    outcomes = [
        runner.Outcome("add", "add01", "failed", refused),
        runner.Outcome("add", "add02", "failed", "500: boom"),
        runner.Outcome("basic-update", "i1", "failed", refused),
        runner.Outcome("bind", "bind01", "passed"),
    ]
    doc = runner.report(outcomes, "abc", "")
    assert (doc["passed"], doc["by_design"], doc["failed"]) == (1, 1, 2)
    assert declared  # the page's by-design table comes from the file
    assert {g["group"]: g for g in doc["groups"]}["add"] == {
        "group": "add",
        "passed": 0,
        "by_design": 1,
        "failed": 1,
    }


def test_the_page_shows_the_groups_counts_and_version(tmp_path):
    doc = runner.report(
        [
            runner.Outcome("bind", "b1", "passed"),
            runner.Outcome("bind", "b2", "failed", "refused"),
        ],
        "d01b141",
        "",
    )
    json.dumps(doc)
    surface, suite, rate, cases = fill_docs.read(doc)
    assert surface == "sparql" and "d01b141" in suite
    assert "1 passed, 0 skipped by design, 1 failed" in rate
    assert "| bind | 1 | 0 | 1 |" in cases and "| **Total** | 1 | 0 | 1 |" in cases
    page = fill_docs.fill("**Suite:** x\n\n**Pass rate:** y\n", suite, rate, cases)
    assert "## Results by test group" in page and "| b2 | refused |" in page


def test_a_group_filter_naming_nothing_is_refused():
    case = manifest.Case("bind", "b1", "b1", "query", Path("q.rq"))
    assert runner.select([case], "bi*") == [case]
    with pytest.raises(SystemExit):
        runner.select([case], "nothing")


MANIFEST = """
@prefix : <#> . @prefix mf: <http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#> .
@prefix qt: <http://www.w3.org/2001/sw/DataAccess/tests/test-query#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
<> mf:entries (:e1 :s1) .
:e1 a mf:QueryEvaluationTest ; mf:name "one" ;
    mf:action [ qt:query <q.rq> ; qt:data <d.ttl> ] ; mf:result <r.srx> .
:s1 a mf:NegativeSyntaxTest11 ; mf:name "two" ; mf:action <bad.rq> .
"""


def test_a_manifest_reads_into_cases(tmp_path):
    group = tmp_path / "bind"
    group.mkdir()
    (group / "manifest.ttl").write_text(MANIFEST)
    one, two = manifest.cases_of(group / "manifest.ttl")
    assert (one.group, one.name, one.kind) == ("bind", "e1", "query")
    assert one.data == (group.resolve() / "d.ttl",) and one.result == group.resolve() / "r.srx"
    assert (two.kind, two.action, two.data) == ("negative-query", group.resolve() / "bad.rq", ())


def test_a_manifest_entry_of_an_unknown_type_is_refused(tmp_path):
    group = tmp_path / "x"
    group.mkdir()
    (group / "manifest.ttl").write_text(MANIFEST.replace("mf:NegativeSyntaxTest11", "mf:NewTest"))
    with pytest.raises(ValueError, match="NewTest"):
        manifest.cases_of(group / "manifest.ttl")
