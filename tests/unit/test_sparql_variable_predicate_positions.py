# Copyright (c) 2026 Kenneth Stott
# Canary: 15387918-6ca6-4d04-813d-ae4ae66abb96
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A variable predicate wherever a query can put one (REQ-2135): inside an OPTIONAL, under an
aggregate, with its ?p or ?o shared, and read by a FILTER, BIND or ORDER BY. Each position answers
what naming each predicate answers, by the same governed read, and a table with no registered
primary key, whose subject is no one row, is refused there by name (V002). Compiled as /data/sparql
compiles it and run on DuckDB (:class:`tests.unit.test_sparql_open_pattern_rights.Model`)."""

# Requirements: REQ-2135, REQ-603, REQ-263

from __future__ import annotations

import json
import re

import pytest

from provisa.compiler.rls import RLSContext
from provisa.compiler.stage2 import build_governance_context
from provisa.gql.translator_types import CypherTranslateError
from provisa.sparql_query.vocabulary import RDF_TYPE, column_iri, edge_iri
from tests.unit.test_sparql_open_pattern_rights import (
    BASE,
    RELATIONSHIPS,
    ROLE,
    SECRET,
    Model,
    _class,
    _text,
)

ORDERS = _class("orders")
CUSTOMERS = _class("customers")
LINES = _class("lines")
TOTAL = column_iri(BASE, "shop", "orders", "total")
ORDER_ID = column_iri(BASE, "shop", "orders", "id")
ORDER_CUSTOMER = column_iri(BASE, "shop", "orders", "customer_id").replace(
    "customer_id", "customerId"
)
TIER = column_iri(BASE, "shop", "customers", "tier")
NAME = column_iri(BASE, "shop", "customers", "name")
CUSTOMER = edge_iri(BASE, "customer")


@pytest.fixture(autouse=True)
def public_url(monkeypatch):
    monkeypatch.setenv("PROVISA_PUBLIC_URL", BASE)


def _model(hidden: set | None = None, keyed: bool = True, role: dict = ROLE) -> Model:
    """Two customers, one with no tier, each with an order."""
    model = Model(hidden or set(), keyed=keyed, role=role)
    model.con.execute("INSERT INTO pg.public.customers VALUES (7002, 'Bob', NULL)")
    model.con.execute("INSERT INTO pg.public.orders VALUES (8002, 7002, 50)")
    return model


def _value(term: dict | None):
    """A term of the answer as a plain value: an IRI, a node's key, or a literal."""
    if term is None:
        return None
    if "iri" in term:
        return term["iri"]
    if "node" in term:
        return tuple(term["node"]["key"])
    return term["literal"]


def _rows(rows: list[dict], *names: str) -> list[tuple]:
    return sorted(
        (tuple(_value(r[n]) for n in names) for r in rows), key=lambda row: json.dumps(row)
    )


def _s(order: int) -> tuple[int]:
    return (order,)


# --- OPTIONAL ----------------------------------------------------------------------------------


def test_an_optional_binds_every_predicate_of_each_subject():
    query = f"SELECT ?s ?p ?o WHERE {{ ?s a <{ORDERS}> OPTIONAL {{ ?s ?p ?o }} }}"
    rows = _rows(_model().answer(query), "s", "p", "o")
    assert rows == sorted(
        [
            (_s(n), p, o)
            for n, total, cust in ((8001, 9001, 7001), (8002, 50, 7002))
            for p, o in (
                (RDF_TYPE, ORDERS),
                (ORDER_ID, n),
                (ORDER_CUSTOMER, cust),
                (TOTAL, total),
                (CUSTOMER, (cust,)),
            )
        ],
        key=lambda row: json.dumps(row),
    )


def test_an_optional_leaves_unbound_only_the_subject_no_predicate_of_it_matches():
    query = f"SELECT ?s ?p ?o WHERE {{ ?s a <{CUSTOMERS}> OPTIONAL {{ ?s ?p ?o FILTER(?o = 'gold') }} }}"
    rows = _rows(_model().answer(query), "s", "p", "o")
    # One row each: not a row of unbound values for each predicate 7001 has that is not gold.
    assert rows == sorted(
        [((7001,), TIER, "gold"), ((7002,), None, None)], key=lambda row: json.dumps(row)
    )


def test_a_filter_outside_the_optional_reads_what_it_bound():
    query = (
        f"SELECT ?s WHERE {{ ?s a <{CUSTOMERS}> "
        "OPTIONAL { ?s ?p ?o FILTER(?o = 'gold') } FILTER(!BOUND(?o)) }"
    )
    assert _rows(_model().answer(query), "s") == [((7002,),)]


def test_an_optional_edge_is_tested_along_its_relationship():
    query = (
        f"SELECT ?s ?o WHERE {{ ?s a <{ORDERS}> ; <{TOTAL}> ?t "
        "OPTIONAL { ?s ?p ?o FILTER(isIRI(?o) && ?p != <" + RDF_TYPE + ">) } }"
    )
    assert _rows(_model().answer(query), "s", "o") == [(_s(8001), (7001,)), (_s(8002), (7002,))]


def test_an_optional_naming_a_column_of_a_shared_object_leaves_other_kinds_their_rows():
    """A part whose ?o is a number or an IRI has an unmatched OPTIONAL, not no solution."""
    query = (
        f"SELECT ?p ?n WHERE {{ ?s a <{CUSTOMERS}> ; ?p ?o "
        f"OPTIONAL {{ ?s <{TIER}> ?o ; <{NAME}> ?n }} }}"
    )
    rows = _rows(_model().answer(query), "p", "n")
    assert (TIER, SECRET) in rows
    assert (RDF_TYPE, None) in rows
    assert (column_iri(BASE, "shop", "customers", "id"), None) in rows


# --- aggregates --------------------------------------------------------------------------------


def test_group_by_the_predicate():
    query = f"SELECT ?p (COUNT(*) AS ?n) WHERE {{ ?s a <{ORDERS}> ; ?p ?o }} GROUP BY ?p"
    assert dict(_rows(_model().answer(query), "p", "n")) == {
        RDF_TYPE: 2,
        ORDER_ID: 2,
        ORDER_CUSTOMER: 2,
        TOTAL: 2,
        CUSTOMER: 2,
    }


def test_a_count_of_every_triple_reads_every_class():
    rows = _model().answer("SELECT (COUNT(*) AS ?n) WHERE { ?s ?p ?o }")
    # customers: 2 types, 2 ids, 2 names, 1 tier; orders: 2 of each of five; lines: 2 of 4
    assert _rows(rows, "n") == [(7 + 10 + 8,)]


def test_an_aggregate_of_the_object_counts_its_bindings():
    query = (
        f"SELECT (COUNT(?o) AS ?a) (COUNT(DISTINCT ?o) AS ?b) (COUNT(DISTINCT ?p) AS ?c) "
        f"WHERE {{ ?s a <{CUSTOMERS}> ; ?p ?o }}"
    )
    # Two types (one class), two ids, two names, one tier.
    assert _rows(_model().answer(query), "a", "b", "c") == [(7, 6, 4)]


def test_group_by_the_subject_and_having_over_the_count():
    query = (
        f"SELECT ?s (COUNT(?o) AS ?n) WHERE {{ ?s a <{CUSTOMERS}> ; ?p ?o }} "
        "GROUP BY ?s HAVING(COUNT(?o) > 3) ORDER BY DESC(?n)"
    )
    assert _rows(_model().answer(query), "s", "n") == [((7001,), 4)]


def test_a_sum_reads_the_values_a_filter_leaves():
    query = f"SELECT (SUM(?o) AS ?t) WHERE {{ ?s a <{ORDERS}> ; ?p ?o FILTER(?p = <{TOTAL}>) }}"
    assert _rows(_model().answer(query), "t") == [(9051,)]


def test_a_sum_over_values_of_several_kinds_is_refused_by_name():
    query = f"SELECT (SUM(?o) AS ?t) WHERE {{ ?s a <{ORDERS}> ; ?p ?o }}"
    with pytest.raises(CypherTranslateError, match="SUM reads values of several kinds"):
        _model().semantic_sql(query)


def test_an_expression_reading_a_key_of_several_predicates_is_refused_by_name():
    query = (
        f"SELECT ?o (COUNT(*) AS ?n) WHERE {{ ?s a <{ORDERS}> ; ?p ?o }} GROUP BY ?o HAVING(?o > 5)"
    )
    with pytest.raises(CypherTranslateError, match="an expression can't read it"):
        _model().semantic_sql(query)


# --- ?p or ?o shared ---------------------------------------------------------------------------


def test_an_object_shared_with_a_named_column_is_equal_to_it():
    query = f"SELECT ?p WHERE {{ ?s <{TIER}> ?o ; ?p ?o }}"
    assert _rows(_model().answer(query), "p") == [(TIER,)]


def test_an_object_shared_by_two_variable_predicates_of_one_subject():
    query = f"SELECT ?p ?q WHERE {{ ?s a <{ORDERS}> ; ?p ?o ; ?q ?o FILTER(?p != ?q) }}"
    assert _rows(_model().answer(query), "p", "q") == []


def test_an_object_that_is_a_subject_elsewhere_is_an_edge_to_it():
    query = f"SELECT ?s ?p ?t WHERE {{ ?s ?p ?o . ?o <{TIER}> ?t }}"
    assert _rows(_model().answer(query), "s", "p", "t") == [(_s(8001), CUSTOMER, "gold")]


def test_a_predicate_shared_by_two_triples_of_one_subject_is_one_predicate():
    query = f"SELECT ?p ?a ?b WHERE {{ ?s a <{CUSTOMERS}> ; <{TIER}> ?t ; ?p ?a ; ?p ?b }}"
    rows = _rows(_model().answer(query), "p", "a", "b")
    assert (TIER, "gold", "gold") in rows
    assert all(a == b for _, a, b in rows)


def test_a_predicate_shared_across_an_edge_reads_each_end():
    query = f"SELECT ?s ?c ?p WHERE {{ ?s <{CUSTOMER}> ?c ; ?p ?x . ?c ?p ?y FILTER(?s != ?c) }}"
    # Only rdf:type is of both, and an order's class and a customer's are told apart by ?x and ?y.
    assert _rows(_model().answer(query), "p") == [(RDF_TYPE,), (RDF_TYPE,)]


def test_a_value_shared_across_subjects_is_refused_as_naming_it_is():
    query = f"SELECT ?a ?b WHERE {{ ?a ?p ?o . ?b <{TIER}> ?o }}"
    model = _model()
    sql, _ = model.semantic_sql(query)
    assert any(v.code == "V002" for v in model.violations(sql))


# --- FILTER ------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "condition,expected",
    [
        ("?o = 'gold'", [(TIER, "gold")]),
        ("?o != 'gold' && isLiteral(?o) && ?p = <" + NAME + ">", [(NAME, "Bob"), (NAME, SECRET)]),
        ("?p = <" + TIER + ">", [(TIER, "gold")]),
        ("?p IN (<" + TIER + ">)", [(TIER, "gold")]),
        ("CONTAINS(?o, 'ol')", [(TIER, "gold")]),
        ("STRLEN(?o) = 4 && isLiteral(?o)", [(TIER, "gold")]),
        ("REGEX(?o, '^go')", [(TIER, "gold")]),
        ("?o > 7001", [(column_iri(BASE, "shop", "customers", "id"), 7002)]),
        ("isNUMERIC(?o) && ?o < 7002", [(column_iri(BASE, "shop", "customers", "id"), 7001)]),
        ("?o + 1 = 7003", [(column_iri(BASE, "shop", "customers", "id"), 7002)]),
        ("STR(?p) = '" + TIER + "'", [(TIER, "gold")]),
        ("sameTerm(?o, <" + CUSTOMERS + ">) && ?s = ?s", None),
    ],
)
def test_a_filter_reads_each_predicates_value_as_its_own_kind(condition, expected):
    query = f"SELECT ?p ?o WHERE {{ ?s a <{CUSTOMERS}> ; ?p ?o FILTER({condition}) }}"
    rows = _rows(_model().answer(query), "p", "o")
    if expected is None:
        expected = [(RDF_TYPE, CUSTOMERS), (RDF_TYPE, CUSTOMERS)]
    assert rows == sorted(expected, key=lambda row: json.dumps(row))


def test_a_filter_over_every_class_keeps_what_holds():
    rows = _model().answer("SELECT ?s ?o WHERE { ?s ?p ?o FILTER(?o = 'sku-two') }")
    assert _rows(rows, "s", "o") == [((5001, 2), "sku-two")]


def test_a_filter_on_an_edges_object_compares_it_with_a_subject():
    query = f"SELECT ?s ?p WHERE {{ ?s a <{ORDERS}> ; ?p ?o FILTER(?o = <{BASE}/id/shop/customers/7001>) }}"
    assert _rows(_model().answer(query), "s", "p") == [(_s(8001), CUSTOMER)]


def test_a_filter_that_needs_a_subjects_iri_in_sql_is_refused_by_name():
    query = f"SELECT ?o WHERE {{ ?s a <{ORDERS}> ; ?p ?o FILTER(CONTAINS(STR(?o), 'x')) }}"
    with pytest.raises(CypherTranslateError, match=r"\?o is a subject"):
        _model().semantic_sql(query)


# --- BIND --------------------------------------------------------------------------------------


def test_a_bind_of_the_object_is_that_term():
    query = f"SELECT ?q ?x WHERE {{ ?s a <{ORDERS}> ; ?p ?o BIND(?o AS ?x) BIND(?p AS ?q) }}"
    rows = _rows(_model().answer(query), "q", "x")
    assert (TOTAL, 9001) in rows
    assert (CUSTOMER, (7001,)) in rows
    assert (RDF_TYPE, ORDERS) in rows


def test_a_bind_over_the_object_is_of_its_kind_or_unbound():
    query = (
        f"SELECT ?p ?x WHERE {{ ?s a <{CUSTOMERS}> ; <{TIER}> ?t ; ?p ?o BIND(UCASE(?o) AS ?x) }}"
    )
    rows = dict(_rows(_model().answer(query), "p", "x"))
    assert rows[TIER] == "GOLD"
    assert rows[column_iri(BASE, "shop", "customers", "id")] is None
    assert rows[RDF_TYPE] is None


def test_a_bind_of_a_datatype_stays_the_columns():
    query = f"SELECT ?x WHERE {{ ?s a <{ORDERS}> ; ?p ?o FILTER(?p = <{TOTAL}>) BIND(?o AS ?x) }}"
    (first, _second) = _model().answer(query)
    assert first["x"]["datatype"].endswith("#integer")


# --- ORDER BY ----------------------------------------------------------------------------------


def test_order_by_the_object_puts_iris_first_and_numbers_in_number_order():
    query = f"SELECT ?o WHERE {{ ?s a <{ORDERS}> ; ?p ?o }} ORDER BY ?o"
    values = [_value(r["o"]) for r in _model().answer(query)]
    assert values == [
        (7001,),
        (7002,),
        ORDERS,
        ORDERS,
        50,
        7001,
        7002,
        8001,
        8002,
        9001,
    ]


def test_order_by_descending_reverses_every_kind():
    query = f"SELECT ?o WHERE {{ ?s a <{ORDERS}> ; ?p ?o }} ORDER BY DESC(?o) LIMIT 2"
    assert [_value(r["o"]) for r in _model().answer(query)] == [9001, 8002]


def test_order_by_the_predicate():
    query = f"SELECT ?p WHERE {{ ?s a <{ORDERS}> ; ?p ?o }} ORDER BY ?p LIMIT 2"
    assert [_value(r["p"]) for r in _model().answer(query)] == sorted(
        [CUSTOMER, ORDER_CUSTOMER, ORDER_ID, TOTAL, RDF_TYPE] * 2
    )[:2]


def test_order_by_an_aggregate_of_the_parts():
    query = (
        f"SELECT ?s (COUNT(*) AS ?n) WHERE {{ ?s a <{CUSTOMERS}> ; ?p ?o }} GROUP BY ?s ORDER BY ?n"
    )
    assert [_value(r["s"]) for r in _model().answer(query)] == [(7002,), (7001,)]


# --- no registered key, by position ------------------------------------------------------------


POSITIONS = {
    "optional": f"SELECT ?s ?p ?o WHERE {{ ?s a <{ORDERS}> OPTIONAL {{ ?s ?p ?o }} }}",
    "aggregate": f"SELECT ?p (COUNT(*) AS ?n) WHERE {{ ?s a <{ORDERS}> ; ?p ?o }} GROUP BY ?p",
    "shared-p": f"SELECT ?p WHERE {{ ?s a <{ORDERS}> ; ?p ?a ; ?p ?b }}",
    "shared-o": f"SELECT ?p WHERE {{ ?s <{TOTAL}> ?o ; ?p ?o }}",
    "filter": f"SELECT ?o WHERE {{ ?s a <{ORDERS}> ; ?p ?o FILTER(?o = 50) }}",
    "bind": f"SELECT ?x WHERE {{ ?s a <{ORDERS}> ; ?p ?o BIND(?o AS ?x) }}",
    "order-by": f"SELECT ?o WHERE {{ ?s a <{ORDERS}> ; ?p ?o }} ORDER BY ?o",
}


@pytest.mark.parametrize("query", POSITIONS.values(), ids=POSITIONS.keys())
def test_a_table_with_no_registered_key_is_refused_by_name(query):
    """Its subject is no one row, so a read past the lone triple joins its rows, which no
    relationship approves. What it read would say nothing the join is not asked to."""
    model = _model(keyed=False)
    sql, _ = model.semantic_sql(query)
    refused = [v for v in model.violations(sql) if v.code == "V002"]
    assert refused, sql
    assert re.search(r"\borders\b", refused[0].message)


@pytest.mark.parametrize("query", POSITIONS.values(), ids=POSITIONS.keys())
def test_a_table_with_a_registered_key_is_read_with_no_join_of_itself(query):
    model = _model()
    sql, _ = model.semantic_sql(query)
    assert model.violations(sql) == []
    assert "_r0" not in sql  # the join of a subject's rows
    assert "_tr0" not in sql  # a union joined back on the key


@pytest.mark.parametrize("query", POSITIONS.values(), ids=POSITIONS.keys())
def test_a_role_that_may_join_outside_relationships_reads_a_subject_by_its_rows(query):
    role = {**ROLE, "capabilities": [*ROLE["capabilities"], "ignore_relationships"]}
    keyed, unkeyed = _model().answer(query), _model(keyed=False, role=role).answer(query)
    names = [n for n in keyed[0] if not n.startswith("__")]
    assert _rows(keyed, *names) == _rows(unkeyed, *names)


def test_the_lone_triple_is_still_answered_row_by_row():
    rows = _model(keyed=False).answer(f"SELECT ?s ?p ?o WHERE {{ ?s a <{ORDERS}> ; ?p ?o }}")
    assert (TOTAL, 50) in _rows(rows, "p", "o")


# --- rights ------------------------------------------------------------------------------------


OPEN_POSITIONS = {
    "aggregate": "SELECT ?p (COUNT(*) AS ?n) WHERE { ?s ?p ?o } GROUP BY ?p",
    "filter": "SELECT ?s ?p ?o WHERE { ?s ?p ?o FILTER(isLiteral(?o) || isIRI(?o)) }",
    "bind": "SELECT ?s ?p ?x WHERE { ?s ?p ?o BIND(?o AS ?x) }",
    "order-by": "SELECT ?s ?p ?o WHERE { ?s ?p ?o } ORDER BY ?o",
}


@pytest.mark.parametrize("query", OPEN_POSITIONS.values(), ids=OPEN_POSITIONS.keys())
def test_an_open_subject_leaves_out_a_table_whose_key_the_role_cannot_read(query):
    answer = _text(_model({(1, "id"), (1, "name"), (2, "customer_id")}).answer(query))
    assert SECRET not in answer
    assert "gold" not in answer
    assert "/vocab/rel/" not in answer  # nor the edge into it


@pytest.mark.parametrize("query", OPEN_POSITIONS.values(), ids=OPEN_POSITIONS.keys())
def test_a_hidden_column_gives_no_triple_in_any_position(query):
    answer = _text(_model({(1, "name")}).answer(query))
    assert SECRET not in answer
    assert "#name" not in answer


@pytest.mark.parametrize(
    "query",
    [
        f"SELECT ?s WHERE {{ ?s a <{CUSTOMERS}> OPTIONAL {{ ?s ?p ?o }} }}",
        f"SELECT ?s (COUNT(*) AS ?n) WHERE {{ ?s a <{CUSTOMERS}> ; ?p ?o }} GROUP BY ?s",
        f"SELECT ?s WHERE {{ ?s a <{CUSTOMERS}> ; ?p ?o FILTER(?o = 'gold') }}",
        f"SELECT ?s ?o WHERE {{ ?s a <{CUSTOMERS}> ; ?p ?o }} ORDER BY ?o",
    ],
    ids=["optional", "aggregate", "filter", "order-by"],
)
def test_naming_the_table_and_asking_for_its_subject_is_refused_by_name(query):
    with pytest.raises(CypherTranslateError, match="customers.* key is not readable") as raised:
        _model({(1, "id")}).semantic_sql(query)
    assert "id" not in str(raised.value).replace("readable", "").replace("customers", "")


@pytest.mark.parametrize(
    "query,expected",
    [
        (f"SELECT ?o WHERE {{ ?s a <{CUSTOMERS}> ; ?p ?o FILTER(?o = 'gold') }}", [("gold",)]),
        (
            f"SELECT (COUNT(?o) AS ?o) WHERE {{ ?s a <{CUSTOMERS}> ; ?p ?o FILTER(?o = 'gold') }}",
            [(1,)],
        ),
    ],
    ids=["filter", "aggregate"],
)
def test_reading_its_columns_without_its_subject_is_still_answered(query, expected):
    model = _model({(1, "id")})
    assert _rows(model.answer(query), "o") == expected


@pytest.mark.parametrize(
    "query,expected",
    [
        (f"SELECT (COUNT(*) AS ?n) WHERE {{ ?s <{TOTAL}> ?t ; ?p ?o }}", 5),
        (f"SELECT (COUNT(*) AS ?n) WHERE {{ ?s a <{ORDERS}> ; ?p ?o FILTER(?o > 0) }}", 3),
    ],
    ids=["aggregate", "aggregate-filter"],
)
def test_a_row_rule_holds_under_an_aggregate_of_the_parts(query, expected):
    model = _model()
    model.gov = build_governance_context(
        "analyst",
        RLSContext(rules={2: "total < 100"}),
        {},
        model.ctx,
        model.tables,
        model.role,
        RELATIONSHIPS,
    )
    assert _rows(model.answer(query), "n") == [(expected,)]


def test_a_construct_and_an_ask_read_the_filter_too():
    model = _model()
    construct = f"CONSTRUCT {{ ?s ?p ?o }} WHERE {{ ?s a <{ORDERS}> ; ?p ?o FILTER(?o = 50) }}"
    assert _rows(model.answer(construct), "p", "o") == [(TOTAL, 50)]
    ask = f"ASK {{ ?s a <{ORDERS}> ; ?p ?o FILTER(?o = 51) }}"
    assert model.answer(ask) == []


def test_a_lines_compound_key_tells_each_rows_triples_in_a_filter():
    query = f"SELECT ?s ?o WHERE {{ ?s a <{LINES}> ; ?p ?o FILTER(STRSTARTS(?o, 'sku')) }}"
    assert _rows(_model().answer(query), "s", "o") == [
        ((5001, 1), "sku-one"),
        ((5001, 2), "sku-two"),
    ]
