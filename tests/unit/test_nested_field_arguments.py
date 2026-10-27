# Copyright (c) 2026 Kenneth Stott
# Canary: 186ccfa3-b32e-4b30-b555-1b48901005a2
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A relationship field's arguments hold at every depth, as at the first (#285): where,
order_by, limit, offset and distinct_on on a field nested below another relationship apply to
that field's rows. Each case is compiled from the meta domain's own relationships (a term's
edges out, each edge's target term) and the depth-2 and depth-3 answers are run on DuckDB."""

# Requirements: REQ-001, REQ-2136

import duckdb
import pytest
import sqlglot
import sqlglot.expressions as exp
from graphql import parse, validate

from provisa.api._meta_seed import META_RELATIONSHIPS
from provisa.compiler.context import build_context
from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.schema_gen import SchemaInput, generate_schema
from provisa.compiler.sql_gen import compile_query
from provisa.compiler.sql_rewrite import make_semantic_sql
from provisa.executor.serialize import serialize_rows

_COLUMNS = {
    "glossary_terms": [("id", "integer"), ("name", "varchar")],
    "glossary_term_edges": [
        ("id", "integer"),
        ("from_term_id", "integer"),
        ("to_term_id", "integer"),
        ("rel_type", "varchar"),
    ],
}
#: ledger -PART_OF-> party, -KIND_OF-> customer, -RELATED_TO-> customer;
#: customer -KIND_OF-> party, -RELATED_TO-> old, -PART_OF-> old; party -KIND_OF-> old.
_TERMS = [(1, "party"), (2, "customer"), (3, "ledger"), (4, "old")]
_EDGES = [
    (8, 3, 1, "PART_OF"),
    (9, 3, 2, "KIND_OF"),
    (12, 3, 2, "RELATED_TO"),
    (7, 2, 1, "KIND_OF"),
    (10, 2, 4, "RELATED_TO"),
    (13, 2, 4, "PART_OF"),
    (14, 1, 4, "KIND_OF"),
]


@pytest.fixture
def meta():
    ids = {name: i for i, name in enumerate(_COLUMNS, 1)}
    si = SchemaInput(
        tables=[
            {
                "id": ids[name],
                "source_id": "provisa-admin",
                "domain_id": "meta",
                "schema_name": "public",
                "table_name": name,
                "columns": [{"column_name": c, "visible_to": ["admin"]} for c, _ in cols],
                "write_ops": [],
                "write_returns_rows": False,
                "write_refused_forms": [],
            }
            for name, cols in _COLUMNS.items()
        ],
        relationships=[
            {
                "id": rid,
                "source_table_id": ids[st],
                "target_table_id": ids[tt],
                "source_column": sc,
                "target_column": tc,
                "cardinality": card,
                "alias": alias,
                "graphql_alias": field,
            }
            for (rid, _s, st, sc, tc, card, _t, tt, alias, field) in META_RELATIONSHIPS
            if st in ids and tt in ids
        ],
        column_types={
            ids[name]: [
                ColumnMetadata(column_name=c, data_type=t, is_nullable=True) for c, t in cols
            ]
            for name, cols in _COLUMNS.items()
        },
        naming_rules=[],
        role={"id": "admin", "capabilities": ["query_development"], "domain_access": ["*"]},
        domains=[{"id": "meta", "description": "Meta"}],
    )
    con = duckdb.connect()
    con.execute("CREATE SCHEMA meta")
    con.execute("CREATE TABLE meta.glossary_terms (id INTEGER, name VARCHAR)")
    con.executemany("INSERT INTO meta.glossary_terms VALUES (?, ?)", _TERMS)
    con.execute(
        "CREATE TABLE meta.glossary_term_edges (id INTEGER, from_term_id INTEGER, "
        "to_term_id INTEGER, rel_type VARCHAR)"
    )
    con.executemany("INSERT INTO meta.glossary_term_edges VALUES (?, ?, ?, ?)", _EDGES)
    return generate_schema(si), build_context(si), con


def _compiled(meta, query: str):
    schema, ctx, _con = meta
    doc = parse(query)
    assert not validate(schema, doc)
    (root,) = compile_query(doc, ctx)
    return root


def _answer(meta, query: str) -> list[dict]:
    _schema, ctx, con = meta
    root = _compiled(meta, query)
    tree = sqlglot.parse_one(make_semantic_sql(root.sql, ctx), read="postgres")
    for agg in list(tree.find_all(exp.JSONArrayAgg)):
        agg.replace(_duckdb(agg))
    sql = tree.sql(dialect="duckdb")
    rows = con.execute(sql, root.params).fetchall()
    return serialize_rows(rows, root.columns, "glossaryTerms")["data"]["glossaryTerms"]


def _duckdb(node: exp.Expression) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]
    # DuckDB names Postgres's json_agg json_group_array.
    if isinstance(node, exp.JSONArrayAgg):
        return exp.Anonymous(this="json_group_array", expressions=[node.this])
    return node


def _depth(args: str, depth: int, leaf: str = "id relType") -> str:
    """The ledger's edges, each target's edges, ... to ``depth``; ``args`` on the deepest."""
    body = f"edgesOut{args} {{ {leaf} }}"
    for _ in range(depth - 1):
        body = f"edgesOut {{ id targetTerm {{ id {body} }} }}"
    return f'{{ glossaryTerms(where: {{ name: {{ eq: "ledger" }} }}) {{ id {body} }} }}'


_KEPT = '(where: { relType: { in: ["KIND_OF", "PART_OF"] } })'


def test_a_depth_two_field_s_where_keeps_its_rows(meta):
    answer = _answer(
        meta,
        '{ glossaryTerms(where: { name: { eq: "ledger" } }) { id '
        f"edgesOut{_KEPT} {{ id targetTerm {{ id edgesOut{_KEPT} {{ id relType }} }} }} }} }}",
    )
    assert len(answer) == 1
    by_edge = {
        e["id"]: sorted(x["relType"] for x in e["targetTerm"]["edgesOut"])
        for e in answer[0]["edgesOut"]
    }
    # ledger's RELATED_TO edge (12) is not kept; customer's RELATED_TO edge (10) is not either.
    assert by_edge == {8: ["KIND_OF"], 9: ["KIND_OF", "PART_OF"]}


@pytest.mark.parametrize("depth", [2, 3])
def test_a_nested_where_holds_at_depth(meta, depth):
    answer = _answer(meta, _depth(_KEPT, depth))

    def deepest(node, d):
        if d == 1:
            return [e["relType"] for e in node["edgesOut"]]
        return [x for e in node["edgesOut"] for x in deepest(e["targetTerm"], d - 1)]

    kinds = deepest(answer[0], depth)
    assert kinds and set(kinds) <= {"KIND_OF", "PART_OF"}


@pytest.mark.parametrize("depth", [2, 3])
@pytest.mark.parametrize(
    "args, clause",
    [
        (_KEPT, '"rel_type" IN ('),
        ("(order_by: [{ id: desc }])", "ORDER BY"),
        ("(offset: 1)", "OFFSET"),
        ("(distinct_on: [relType])", "DISTINCT ON"),
    ],
)
def test_each_nested_argument_is_applied_at_depth(meta, args, clause, depth):
    sql = _compiled(meta, _depth(args, depth)).sql
    # The deepest field is its own lateral join, applying the argument to its rows.
    assert clause in sql.rsplit("LEFT JOIN LATERAL", 1)[1], sql


@pytest.mark.parametrize("depth", [2, 3])
def test_a_nested_limit_holds_at_depth(meta, depth):
    # A limit alone is applied by the aggregate subqueries, to each parent's rows.
    assert "LIMIT 1" in _compiled(meta, _depth("(limit: 1)", depth)).sql
    answer = _answer(meta, _depth("(limit: 1)", depth))

    def deepest(node, d):
        # The JSON path answers a term with no edges as null, not [] (as before #285).
        edges = node["edgesOut"] or []
        if d == 1:
            return [len(edges)]
        return [n for e in edges for n in deepest(e["targetTerm"], d - 1)]

    counts = deepest(answer[0], depth)
    assert counts and max(counts) == 1


def test_a_depth_two_order_and_limit_pick_each_parent_s_rows(meta):
    answer = _answer(
        meta,
        '{ glossaryTerms(where: { name: { eq: "ledger" } }) { id '
        "edgesOut(order_by: [{ id: asc }]) { id targetTerm { id "
        "edgesOut(order_by: [{ id: desc }], limit: 1) { id } } } } }",
    )
    firsts = {
        e["id"]: [x["id"] for x in e["targetTerm"]["edgesOut"]] for e in answer[0]["edgesOut"]
    }
    # customer's edges are 7, 10, 13: the one with the highest id, for each edge reaching it.
    assert firsts == {8: [14], 9: [13], 12: [13]}


def test_a_nested_field_with_arguments_is_answered_under_its_alias(meta):
    # The Ontology View reads every field under an alias (edges_out: edgesOut(where: ...)); a
    # field read through its own join is answered under it, at every depth.
    answer = _answer(
        meta,
        '{ glossaryTerms(where: { name: { eq: "ledger" } }) { id '
        f"eo: edgesOut{_KEPT} {{ id tt: targetTerm {{ id e2: edgesOut{_KEPT} {{ id }} }} }} }} }}",
    )
    assert [sorted(e) for e in answer[0]["eo"]] == [["id", "tt"], ["id", "tt"]]
    assert all("e2" in e["tt"] for e in answer[0]["eo"])


def test_one_field_read_twice_under_two_aliases_answers_each(meta):
    answer = _answer(
        meta,
        '{ glossaryTerms(where: { name: { eq: "ledger" } }) { id '
        'kinds: edgesOut(where: { relType: { eq: "KIND_OF" } }) { relType } '
        'parts: edgesOut(where: { relType: { eq: "PART_OF" } }) { relType } } }',
    )
    assert {e["relType"] for e in answer[0]["kinds"]} == {"KIND_OF"}
    assert {e["relType"] for e in answer[0]["parts"]} <= {"PART_OF"}
