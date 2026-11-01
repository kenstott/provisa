# Copyright (c) 2026 Kenneth Stott
# Canary: 8c1d7e52-4a90-4b36-9f27-e05a3d6c1b84
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
# ruff: noqa: F811 -- a fixture imported here is redefined by each test that takes it

"""What a pill dropped on a SPARQL frame writes (REQ-2135, provisa-ui's sparqlDrop.ts) runs: the
compiler reads each shape, and rows and resources the frame held stay beside what it reaches."""

# Requirements: REQ-2135

from tests.unit.test_sparql_compiler import (  # noqa: F401 -- the fixtures
    BASE,
    COMPANY,
    PERSON,
    PREFIXES,
    _answer,
    con,
    public_url,
)


def test_a_path_of_two_steps_in_one_optional_keeps_the_rows_it_does_not_reach(con):
    rows = _answer(
        con,
        f"SELECT ?n ?cn WHERE {{ ?s a <{PERSON}> ; p:name ?n . OPTIONAL {{ ?s rel:WORKS_AT ?c . "
        f"?c a <{COMPANY}> . ?c rel:PARENT ?d . ?d a <{COMPANY}> . ?d c:name ?cn }} }}",
    )
    assert {r["n"]: r["cn"] for r in rows} == {
        "Ann": "Beta",
        "Bo": "Beta",
        "Cy": "Corp",
        "Di": None,
    }


def test_each_tied_path_is_its_own_optional(con):
    rows = _answer(
        con,
        f"SELECT ?n ?k ?c WHERE {{ ?s a <{PERSON}> ; p:name ?n . "
        f"OPTIONAL {{ ?s rel:KNOWS ?x . ?x a <{PERSON}> . ?x p:name ?k }} "
        f"OPTIONAL {{ ?s rel:WORKS_AT ?y . ?y a <{COMPANY}> . ?y c:name ?c }} }}",
    )
    assert {r["n"]: (r["k"], r["c"]) for r in rows} == {
        "Ann": ("Bo", "Acme"),
        "Bo": ("Cy", "Acme"),
        "Cy": (None, "Beta"),
        "Di": (None, None),
    }


def test_a_path_walked_against_an_edge_reads_it_from_the_far_end(con):
    rows = _answer(
        con,
        f"SELECT ?cn ?n WHERE {{ ?c a <{COMPANY}> ; c:name ?cn . "
        f"OPTIONAL {{ ?x rel:WORKS_AT ?c . ?x a <{PERSON}> . ?x p:name ?n }} }}",
    )
    assert sorted((r["cn"], r["n"] or "") for r in rows) == [
        ("Acme", "Ann"),
        ("Acme", "Bo"),
        ("Beta", "Cy"),
        ("Corp", ""),
    ]


def test_a_filter_in_the_optional_holds_a_path_to_the_node_it_was_dropped_on(con):
    rows = _answer(
        con,
        f"SELECT ?n ?c WHERE {{ ?s a <{PERSON}> ; p:name ?n . OPTIONAL {{ ?s rel:WORKS_AT ?y . "
        f"?y a <{COMPANY}> . ?y c:name ?c . "
        f"FILTER(?s = <{BASE}/id/hr/persons/1>) }} }}",
    )
    assert {r["n"]: r["c"] for r in rows} == {"Ann": "Acme", "Bo": None, "Cy": None, "Di": None}


def test_a_construct_draws_a_dropped_path_and_keeps_what_it_drew(con):
    triples = _answer(
        con,
        f"CONSTRUCT {{ ?s p:name ?n . ?s rel:WORKS_AT ?y . ?y a <{COMPANY}> }} WHERE {{ "
        f"?s a <{PERSON}> ; p:name ?n . OPTIONAL {{ ?s rel:WORKS_AT ?y . ?y a <{COMPANY}> }} }}",
    )
    # A template triple an OPTIONAL left unbound is no triple: results.write_construct leaves it out.
    said = [t for t in triples if t["o"] is not None]
    edges = {
        (t["s"].rsplit("/", 1)[1], t["o"].rsplit("/", 1)[1]) for t in said if "WORKS_AT" in t["p"]
    }
    assert edges == {("1", "1"), ("2", "1"), ("3", "2")}
    assert "Di" in {t["o"] for t in said}


def test_a_variable_predicate_frame_takes_a_path(con):
    rows = _answer(
        con,
        f"SELECT ?s ?p ?o ?y WHERE {{ ?s a <{PERSON}> ; ?p ?o . OPTIONAL {{ ?s rel:WORKS_AT ?y . "
        f"?y a <{COMPANY}> }} }} LIMIT 200",
    )
    assert {r["y"] for r in rows if r["y"]} and any(r["y"] is None for r in rows)
