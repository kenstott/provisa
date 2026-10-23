# Copyright (c) 2026 Kenneth Stott
# Canary: 207b6819-218e-4c23-b020-9cfec15fd327
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A Cypher read answers what the statement says (REQ-345, REQ-347): a WITH's ORDER BY, SKIP and
LIMIT pick the rows that WITH carries on, not the statement's; a pattern predicate or count over a
node a WITH carries is about that node; and each relationship type an untyped hop reaches is named
by type(r) in its own rows. Each case runs on DuckDB against the rows it must answer."""

# Requirements: REQ-345, REQ-347, REQ-1586

import duckdb
import pytest

from provisa.cypher.parser import parse_cypher
from provisa.gql.graph_rewriter import apply_graph_rewrites
from provisa.gql.label_map import CypherLabelMap
from provisa.gql.translator import cypher_to_sql
from tests.unit.test_cypher_junction_relationships import _term_node
from tests.unit.test_sql_to_cypher_shapes import _edge


@pytest.fixture
def terms():
    """customer KIND_OF party; ledger PART_OF party, KIND_OF customer and KIND_OF party."""
    kind, part = _edge("KIND_OF", "kind_of"), _edge("PART_OF", "part_of")
    lm = CypherLabelMap(
        nodes={"GlossaryTerm": _term_node()},
        relationships={"KIND_OF": kind, "PART_OF": part},
        aliases={"KIND_OF": [kind], "PART_OF": [part]},
    )
    c = duckdb.connect()
    c.execute("ATTACH ':memory:' AS provisa_admin")
    c.execute("CREATE SCHEMA provisa_admin.public")
    c.execute("CREATE TABLE provisa_admin.public.glossary_terms (id INTEGER, name VARCHAR)")
    c.execute(
        "INSERT INTO provisa_admin.public.glossary_terms VALUES (1,'party'),(2,'customer'),(3,'ledger')"
    )
    c.execute(
        "CREATE TABLE provisa_admin.public.glossary_term_edges (id INTEGER, from_term_id INTEGER, "
        "to_term_id INTEGER, rel_type VARCHAR, note VARCHAR)"
    )
    c.execute(
        "INSERT INTO provisa_admin.public.glossary_term_edges VALUES (7, 2, 1, 'kind_of', NULL), "
        "(8, 3, 1, 'part_of', NULL), (9, 3, 2, 'kind_of', NULL), (10, 3, 1, 'kind_of', NULL)"
    )
    return lm, c


def _rows(terms, cypher: str) -> list[tuple]:
    lm, con = terms
    sql_ast, _, graph_vars = cypher_to_sql(parse_cypher(cypher), lm, {})
    sql = apply_graph_rewrites(sql_ast, graph_vars, lm).sql(dialect="duckdb")
    return sorted(con.execute(sql).fetchall(), key=repr)


def test_a_with_s_limit_picks_the_rows_it_carries_on(terms):
    # The two terms last by name, each with every term KIND_OF it: party has two, ledger none.
    rows = _rows(
        terms,
        "MATCH (a:GlossaryTerm) WITH a ORDER BY a.name DESC LIMIT 2 "
        "OPTIONAL MATCH (a)<-[:KIND_OF]-(b:GlossaryTerm) RETURN a.name, b.name",
    )
    assert rows == sorted([("party", "customer"), ("party", "ledger"), ("ledger", None)], key=repr)


def test_a_with_s_skip_and_where_apply_to_its_own_rows(terms):
    rows = _rows(
        terms,
        "MATCH (a:GlossaryTerm) WITH a ORDER BY a.name SKIP 1 LIMIT 2 WHERE a.name <> 'party' "
        "MATCH (a)-[:KIND_OF]->(b:GlossaryTerm) RETURN a.name, b.name",
    )
    # By name: customer, ledger, party; SKIP 1 LIMIT 2 keeps ledger and party, the WHERE ledger.
    assert rows == [("ledger", "customer"), ("ledger", "party")]


def test_a_pattern_predicate_over_a_carried_node_is_about_that_node(terms):
    assert _rows(
        terms,
        "MATCH (a:GlossaryTerm) WITH a MATCH (a) "
        "WHERE NOT EXISTS { MATCH (a)-[:KIND_OF]->(:GlossaryTerm) } RETURN a.name",
    ) == [("party",)]
    assert _rows(
        terms,
        "MATCH (a:GlossaryTerm) WITH a WHERE EXISTS { MATCH (a)-[:KIND_OF]->(:GlossaryTerm) } "
        "RETURN a.name",
    ) == [("customer",), ("ledger",)]


def test_a_count_over_a_carried_node_counts_its_own_relationships(terms):
    assert _rows(
        terms,
        "MATCH (a:GlossaryTerm) WITH a "
        "RETURN a.name, COUNT { MATCH (a)-[:KIND_OF]->(b:GlossaryTerm) } AS kinds",
    ) == sorted([("customer", 1), ("ledger", 2), ("party", 0)], key=repr)


def test_an_untyped_hop_names_each_relationship_s_own_type(terms):
    assert _rows(
        terms, "MATCH (a:GlossaryTerm)-[r]->(b:GlossaryTerm) RETURN a.name, type(r), b.name"
    ) == sorted(
        [
            ("customer", "KIND_OF", "party"),
            ("ledger", "PART_OF", "party"),
            ("ledger", "KIND_OF", "customer"),
            ("ledger", "KIND_OF", "party"),
        ],
        key=repr,
    )


def test_a_condition_on_an_untyped_hop_s_type_keeps_that_type_alone(terms):
    assert _rows(
        terms,
        "MATCH (a:GlossaryTerm)-[r]->(b:GlossaryTerm) WHERE type(r) = 'PART_OF' "
        "RETURN a.name, b.name",
    ) == [("ledger", "party")]


def test_a_backward_hop_of_a_relationship_from_a_label_to_itself_reads_its_arrow(terms):
    # Who is KIND_OF party: customer and ledger -- not what party is KIND_OF (nothing).
    assert _rows(
        terms, "MATCH (a:GlossaryTerm {name: 'party'})<-[:KIND_OF]-(b:GlossaryTerm) RETURN b.name"
    ) == [("customer",), ("ledger",)]
    assert _rows(
        terms,
        "MATCH (a:GlossaryTerm) OPTIONAL MATCH (a)<-[:KIND_OF]-(b:GlossaryTerm) "
        "RETURN a.name, b.name",
    ) == sorted(
        [("party", "customer"), ("party", "ledger"), ("customer", "ledger"), ("ledger", None)],
        key=repr,
    )


def test_a_pattern_ending_at_a_carried_node_goes_on_from_its_rows(terms):
    rows = _rows(
        terms,
        "MATCH (a:GlossaryTerm) WITH a ORDER BY a.name DESC LIMIT 2 "
        "OPTIONAL MATCH (b:GlossaryTerm)-[:KIND_OF]->(a) RETURN a.name, b.name",
    )
    assert rows == sorted([("party", "customer"), ("party", "ledger"), ("ledger", None)], key=repr)


def test_a_pattern_reaching_a_carried_node_only_midway_is_refused(terms):
    from provisa.gql.translator_types import CypherTranslateError

    lm, _con = terms
    with pytest.raises(CypherTranslateError, match="carries"):
        cypher_to_sql(
            parse_cypher(
                "MATCH (a:GlossaryTerm) WITH a "
                "MATCH (b:GlossaryTerm)-[:KIND_OF]->(a)-[:KIND_OF]->(c:GlossaryTerm) "
                "RETURN b.name, c.name"
            ),
            lm,
            {},
        )


def test_a_numeric_parameter_name_is_a_parameter_not_a_number(terms):
    """$3 is the parameter named "3" (Cypher allows $0, $1), never the number 3: run without it,
    it is refused as unbound instead of compared with 3."""
    from provisa.cypher.params import CypherParamError, bind_params, collect_param_names

    lm, con = terms
    text = "MATCH (a:GlossaryTerm) WHERE a.name = $3 RETURN a.name"
    assert collect_param_names(text) == ["3"]
    with pytest.raises(CypherParamError):
        bind_params(collect_param_names(text), {})
    sql_ast, ordered, graph_vars = cypher_to_sql(parse_cypher(text), lm, {"3": "ledger"})
    sql = apply_graph_rewrites(sql_ast, graph_vars, lm).sql(dialect="duckdb")
    assert ordered == ["3"]
    assert con.execute(sql, ["ledger"]).fetchall() == [("ledger",)]
