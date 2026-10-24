# Copyright (c) 2026 Kenneth Stott
# Canary: a01f95e7-f513-4eae-9bfe-f410919ddfb2
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The Ontology View's query (REQ-2135, REQ-863) lowered to the graph IR and printed in each of
the explorer's three projections, each read back by the Cypher front end and answered on DuckDB
over seeded glossary tables: GraphQL-shaped, it answers what the GraphQL compiler's own statement
does; with its fields, the same values under the graph's names; as nodes, the same terms and
term edges. Its terms' edges are rows of a junction, read as relationships."""

# Requirements: REQ-2135, REQ-863, REQ-1586

import functools
import importlib.util
import json
import sys
from pathlib import Path

import duckdb
import pytest
from graphql import FieldNode, parse

from provisa.compiler.params import embed_params_comment
from provisa.compiler.sql_gen import compile_query
from provisa.compiler.sql_rewrite import lower_to_catalog_physical, make_semantic_sql
from provisa.compiler.view_expand import expand_view_refs
from provisa.cypher.parser import parse_cypher
from provisa.cypher.printer import print_cypher
from provisa.executor.serialize import serialize_rows
from provisa.gql.assembler import assemble_rows
from provisa.gql.graph_rewriter import apply_graph_rewrites
from provisa.gql.translator import cypher_to_sql
from provisa.graphql_front.lower import (
    FIELDS,
    GRAPHQL,
    NODES,
    GraphQLLoweringRefused,
    lower_read,
)
from provisa.transpiler.transpile import transpile_to_duckdb

_REPO = Path(__file__).resolve().parents[2]
_META = _REPO / "tests" / "fixtures" / "graphql_golden" / "meta_model.yaml"
_READS = json.loads(
    (_REPO / "tests" / "fixtures" / "graphql_golden" / "ontology_queries.json").read_text()
)
#: The view as it opens, filtered to a domain (like), a relationship type, and a search (ilike,
#: edges both ways, a condition on a relationship).
_ONTOLOGY = [
    "ontology: ontology search= hidden= edges= domains=",
    "ontology: ontology search= hidden=retired edges= domains=sales",
    "ontology: ontology search= hidden= edges=KIND_OF,PART_OF domains=",
    "ontology: ontology search=party hidden=retired edges=KIND_OF,PART_OF domains=sales",
]
_DUCKDB_TYPES = {"integer": "INTEGER", "bigint": "BIGINT", "boolean": "BOOLEAN", "date": "DATE"}
_T = "provisa_admin.org_default."


def _script(name: str):
    spec = importlib.util.spec_from_file_location(name, _REPO / "scripts" / f"{name}.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module  # its dataclasses read their own module
    spec.loader.exec_module(module)
    return module


@functools.cache
def _model():
    golden, cypher_golden = _script("graphql_golden"), _script("cypher_golden")
    schema, ctx = golden.model(_META)
    return golden, cypher_golden, schema, ctx, cypher_golden.model_label_map(_META)


@pytest.fixture(scope="module")
def glossary():
    """party, customer, ledger, and a retired old party: customer KIND_OF party, ledger PART_OF
    party and RELATED_TO customer, party SYNONYM_OF old party."""
    golden, cypher_golden, _schema, _ctx, _lm = _model()
    _si, tables, _rels, types = cypher_golden.model_inputs(_META)
    con = duckdb.connect()
    con.execute("ATTACH ':memory:' AS provisa_admin")
    con.execute("CREATE SCHEMA provisa_admin.org_default")
    for t in tables:
        if "view_sql" in t or t["id"] not in types:
            continue
        cols = ", ".join(
            f'"{c.column_name}" {_DUCKDB_TYPES.get(c.data_type, "VARCHAR")}' for c in types[t["id"]]
        )
        con.execute(f'CREATE TABLE {_T}"{t["table_name"]}" ({cols})')
    con.execute(
        f"INSERT INTO {_T}glossary_terms (id, name, definition, ref_count, deprecated, retired, "
        "live, identity_gap, term_domains) VALUES "
        "(1, 'party', 'a party', 2, false, false, true, false, '|sales|'), "
        "(2, 'customer', 'who buys', 1, false, false, true, false, '|sales|'), "
        "(3, 'ledger', 'books', 0, false, false, true, false, '|finance|'), "
        "(4, 'old party', 'was a party', 0, true, true, false, false, '|sales|')"
    )
    con.execute(
        f"INSERT INTO {_T}glossary_term_edges (id, from_term_id, to_term_id, rel_type) VALUES "
        "(7, 2, 1, 'KIND_OF'), (8, 3, 1, 'PART_OF'), (9, 3, 2, 'RELATED_TO'), (10, 1, 4, 'SYNONYM_OF')"
    )
    con.execute(
        f"INSERT INTO {_T}glossary_term_table_refs (id, term_id, table_id, table_name, domain_id) "
        "VALUES (1, 1, 100, 'customers', 'sales'), (2, 3, 101, 'ledger', 'finance')"
    )
    con.execute(
        f"INSERT INTO {_T}glossary_term_domains (id, term_id, domain_id) "
        "VALUES ('a', 1, 'sales'), ('b', 3, 'finance')"
    )
    return con


def _run(con, sql: str, params=()) -> tuple[list[str], list[list]]:
    cursor = con.execute(transpile_to_duckdb(sql), list(params))
    return [d[0] for d in cursor.description], [list(r) for r in cursor.fetchall()]


def _graphql_answer(con, text: str) -> list[dict]:
    golden, _cg, _schema, ctx, _lm = _model()
    document = parse(text)
    (root,) = compile_query(document, ctx, None)
    physical = lower_to_catalog_physical(
        expand_view_refs(
            make_semantic_sql(embed_params_comment(root.sql, root.params), ctx),
            golden._view_sql(_META),
        ),
        ctx,
    )
    _cols, rows = _run(con, physical, root.params)
    own = [tuple(golden._compared(v) for v in r) for r in rows]
    return serialize_rows(own, root.columns, root.root_field)["data"][root.root_field]


def _cypher(text: str, projection: str) -> str:
    _g, _cg, _schema, ctx, lm = _model()
    (root,) = lower_read(parse(text), ctx, lm, None, projection)
    return print_cypher(root.statement)


def _cypher_answer(con, cypher: str) -> list[dict]:
    golden, _cg, _schema, ctx, lm = _model()
    sql_ast, _params, graph_vars = cypher_to_sql(parse_cypher(cypher), lm, {})
    sql_ast = apply_graph_rewrites(sql_ast, graph_vars, lm)
    physical = lower_to_catalog_physical(
        expand_view_refs(
            make_semantic_sql(sql_ast.sql(dialect="postgres"), ctx), golden._view_sql(_META)
        ),
        ctx,
    )
    cols, rows = _run(con, physical)
    named = [dict(zip(cols, r, strict=True)) for r in rows]
    return [
        {k: golden._compared(golden._cypher_value(v)) for k, v in r.items()}
        for r in assemble_rows(named, graph_vars)
    ]


def _sorted(rows: list) -> list:
    golden = _model()[0]
    return sorted(rows, key=golden._canonical)


def _field_names(text: str) -> dict[str, str]:
    """Each response key of ``text`` the field it names (the graph's name for it)."""
    out: dict[str, str] = {}

    def visit(node) -> None:
        for sel in getattr(node.selection_set, "selections", None) or []:
            if isinstance(sel, FieldNode):
                out[sel.alias.value if sel.alias else sel.name.value] = sel.name.value
                visit(sel)

    for definition in parse(text).definitions:
        visit(definition)
    return out


def _renamed(value, names: dict[str, str]):
    if isinstance(value, dict):
        return {names.get(k, k): _renamed(v, names) for k, v in value.items()}
    if isinstance(value, list):
        return [_renamed(v, names) for v in value]
    return value


@pytest.mark.parametrize("key", _ONTOLOGY)
def test_graphql_shaped_it_answers_what_the_graphql_does(glossary, key):
    expected = _graphql_answer(glossary, _READS[key])
    assert expected
    assert _sorted(_cypher_answer(glossary, _cypher(_READS[key], GRAPHQL))) == _sorted(expected)


@pytest.mark.parametrize("key", _ONTOLOGY)
def test_with_its_fields_it_answers_the_same_values_under_the_graph_s_names(glossary, key):
    shaped = _cypher_answer(glossary, _cypher(_READS[key], GRAPHQL))
    names = _field_names(_READS[key])
    fields = _cypher_answer(glossary, _cypher(_READS[key], FIELDS))
    assert _sorted(fields) == _sorted(_renamed(shaped, names))


def _graphql_terms_and_edges(rows: list[dict]) -> tuple[set, set]:
    terms: set = set()
    edges: set = set()

    def visit(row: dict) -> None:
        terms.add(row["id"])
        for side, end in (("edges_out", "target_term"), ("edges_in", "source_term")):
            for e in row.get(side) or []:
                edges.add(e["id"])
                if len(e[end]) > 1:
                    visit(e[end])
                else:
                    terms.add(e[end]["id"])

    for row in rows:
        visit(row)
    return terms, edges


def _graph_terms_and_edges(value, terms: set, edges: set) -> None:
    """Every glossary term node and term edge in a nodes-only answer."""
    from provisa.gql.assembler import Edge, Node

    if isinstance(value, Edge):
        if value.junction_table == "glossary_term_edges":
            edges.add(value.properties["id"])
        return
    if isinstance(value, Node) and value.label == "Meta:GlossaryTerms":
        terms.add(int(value.id.split("|", 1)[1]))


@pytest.mark.parametrize("key", _ONTOLOGY)
def test_as_nodes_it_answers_the_same_terms_and_term_edges(glossary, key):
    cypher = _cypher(_READS[key], NODES)
    # Flat patterns the frame draws: no list of each level's rows.
    assert "RETURN n0," in cypher and "COLLECT" not in cypher and "OPTIONAL MATCH" in cypher
    shaped = _cypher_answer(glossary, _cypher(_READS[key], GRAPHQL))
    terms: set = set()
    edges: set = set()
    for row in _cypher_answer(glossary, cypher):
        for v in row.values():
            _graph_terms_and_edges(v, terms, edges)
    assert (terms, edges) == _graphql_terms_and_edges(shaped)


def test_an_unfiltered_junction_field_is_an_untyped_relationship_and_a_filtered_one_its_types():
    unfiltered = _cypher(_READS[_ONTOLOGY[0]], GRAPHQL)
    assert "MATCH (n0)-[r0]->(n1:GlossaryTerms)" in unfiltered
    assert "rel_type: r0.relType" in unfiltered
    filtered = _cypher(_READS[_ONTOLOGY[2]], GRAPHQL)
    assert "-[r0:KIND_OF|PART_OF]->" in filtered


@pytest.mark.parametrize(
    "where",
    [
        '{ edgesOut: { targetTerm: { name: { eq: "party" } } } }',
        '{ edgesIn: { relType: { in: ["KIND_OF"] } } }',
        '{ tables: { domainId: { eq: "sales" } } }',
        '{ _or: [{ tables: { domainId: { eq: "finance" } } }, { name: { eq: "customer" } }] }',
    ],
)
def test_a_condition_on_a_relationship_is_an_exists_over_its_pattern(glossary, where):
    text = f"{{ m__glossaryTerms(where: {where}) {{ id name }} }}"
    cypher = _cypher(text, GRAPHQL)
    assert "EXISTS { MATCH (n0)" in cypher
    expected = _graphql_answer(glossary, text)
    assert _sorted(_cypher_answer(glossary, cypher)) == _sorted(expected)


@pytest.mark.parametrize(
    "pattern",
    ["party", "%party", "old%", "%par%", "%", "p_rty", "%a_t%", "o%y", "%e%y%", "_"],
)
@pytest.mark.parametrize("op", ["like", "ilike"])
def test_a_like_pattern_matches_as_sql_s_does(glossary, pattern, op):
    value = pattern.upper() if op == "ilike" else pattern
    text = f'{{ m__glossaryTerms(where: {{ name: {{ {op}: "{value}" }} }}) {{ id }} }}'
    expected = _graphql_answer(glossary, text)
    assert _sorted(_cypher_answer(glossary, _cypher(text, GRAPHQL))) == _sorted(expected)


def test_a_like_pattern_prints_as_the_string_predicate_it_is():
    text = '{{ m__glossaryTerms(where: {{ name: {{ {op}: "{p}" }} }}) {{ id }} }}'
    printed = {
        (op, p): _cypher(text.format(op=op, p=p), GRAPHQL).splitlines()[1]
        for op, p in [
            ("like", "party"),
            ("like", "par%"),
            ("like", "%ty"),
            ("like", "%ar%"),
            ("like", "p_r%"),
            ("like", "100\\\\%%"),
            ("ilike", "%ar%"),
        ]
    }
    assert printed == {
        ("like", "party"): "WHERE n0.name = 'party'",
        ("like", "par%"): "WHERE n0.name STARTS WITH 'par'",
        ("like", "%ty"): "WHERE n0.name ENDS WITH 'ty'",
        ("like", "%ar%"): "WHERE n0.name CONTAINS 'ar'",
        ("like", "p_r%"): "WHERE n0.name =~ '(?s)p.r.*'",
        # A backslash takes the next character literally: 100% and anything after.
        ("like", "100\\\\%%"): "WHERE n0.name STARTS WITH '100%'",
        ("ilike", "%ar%"): "WHERE toLower(n0.name) CONTAINS toLower('ar')",
    }


def test_an_unfiltered_junction_field_is_refused_where_other_relationships_join_its_labels():
    _g, _cg, _schema, ctx, lm = _model()
    from provisa.gql.label_map import RelationshipMapping

    extra = RelationshipMapping(
        rel_type="MENTORS",
        source_label="M__GlossaryTerms",
        target_label="M__GlossaryTerms",
        join_source_column="id",
        join_target_column="id",
        field_name="mentors",
    )
    lm.relationships["MENTORS::test"] = extra
    try:
        with pytest.raises(GraphQLLoweringRefused, match="other relationships join these labels"):
            lower_read(parse(_READS[_ONTOLOGY[0]]), ctx, lm, None)
        # Its types named, the junction's rows of them are all it reads.
        assert lower_read(parse(_READS[_ONTOLOGY[2]]), ctx, lm, None)
    finally:
        del lm.relationships["MENTORS::test"]


def test_as_nodes_a_level_with_its_own_limit_is_refused_by_name():
    text = "{ m__glossaryTerms { id edgesOut(limit: 1) { id targetTerm { id } } } }"
    _g, _cg, _schema, ctx, lm = _model()
    with pytest.raises(GraphQLLoweringRefused, match="the limit of the relationship edgesOut"):
        lower_read(parse(text), ctx, lm, None, NODES)
    # GraphQL-shaped, the level keeps its own limit.
    assert "LIMIT 1" in _cypher(text, GRAPHQL)
