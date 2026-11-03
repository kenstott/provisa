# Copyright (c) 2026 Kenneth Stott
# Canary: 9538efb2-c79f-4f27-a2bc-1cb7578c4a98
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A nested field answers what it holds at any depth (#286): a to-one is an object, a to-many
below a to-one keeps every row, sibling to-many fields are each their own list, and a list field
with no rows is [] -- whichever way the field is compiled (a correlated JSON value, or a lateral
join for a field with arguments). Each case runs on DuckDB and on PostgreSQL: a field nested in
a field nested in a lateral join is a correlated subquery two levels deep."""

# Requirements: REQ-047, REQ-048

import uuid

import psycopg
from psycopg import sql as pgsql
import pytest
import sqlglot
import sqlglot.expressions as exp

from provisa.compiler.sql_rewrite import make_semantic_sql
from tests.unit.json_encoder.rows import serialize_rows
from tests.unit.test_nested_field_arguments import _EDGES, _TERMS, _answer, _compiled, meta  # noqa: F401


@pytest.fixture
def pg(embedded_postgres):
    """The same terms and edges in a database of this test's own."""
    name = f"t_{uuid.uuid4().hex[:12]}"
    with psycopg.connect(embedded_postgres.dsn(), autocommit=True) as admin:
        admin.execute(pgsql.SQL("CREATE DATABASE {}").format(pgsql.Identifier(name)))
    con = psycopg.connect(embedded_postgres.dsn(name), autocommit=True)
    con.execute("CREATE SCHEMA ontology")
    con.execute("CREATE TABLE ontology.terms (id INTEGER, name VARCHAR)")
    con.cursor().executemany("INSERT INTO ontology.terms VALUES (%s, %s)", _TERMS)
    con.execute(
        "CREATE TABLE ontology.term_edges (id INTEGER, from_term_id INTEGER, "
        "to_term_id INTEGER, rel_type VARCHAR)"
    )
    con.cursor().executemany("INSERT INTO ontology.term_edges VALUES (%s, %s, %s, %s)", _EDGES)
    yield con
    con.close()
    with psycopg.connect(embedded_postgres.dsn(), autocommit=True) as admin:
        admin.execute(pgsql.SQL("DROP DATABASE {}").format(pgsql.Identifier(name)))


def _bound(node: exp.Expression, params: list) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]
    if isinstance(node, exp.Parameter) and isinstance(node.this, exp.Literal):
        value = params[int(node.this.this) - 1]
        return exp.Literal.string(value) if isinstance(value, str) else exp.Literal.number(value)
    return node


@pytest.fixture(params=["duckdb", "postgres"])
def answer(request, meta):  # noqa: F811
    if request.param == "duckdb":
        return lambda query: _answer(meta, query)
    con = request.getfixturevalue("pg")
    _schema, ctx, _duck = meta

    def _on_postgres(query: str) -> list[dict]:
        root = _compiled(meta, query)
        tree = sqlglot.parse_one(make_semantic_sql(root.sql, ctx), read="postgres")
        sql = tree.transform(lambda n: _bound(n, root.params)).sql(dialect="postgres")
        rows = con.execute(sql).fetchall()
        return serialize_rows(rows, root.columns, "on_terms")["data"]["on_terms"]

    return _on_postgres


def _ledger(body: str) -> str:
    return f'{{ on_terms(where: {{ name: {{ eq: "ledger" }} }}) {{ id {body} }} }}'


def test_a_to_many_below_a_to_one_keeps_every_row(answer):
    # ledger -KIND_OF-> customer, whose edges are 7, 10 and 13.
    (ledger,) = answer(
        _ledger(
            'edgesOut(where: { relType: { eq: "KIND_OF" } }) { id targetTerm { id name edgesOut { id } } }'
        )
    )
    (edge,) = ledger["edgesOut"]
    assert edge["targetTerm"]["name"] == "customer"  # an object, not a list of one
    assert sorted(e["id"] for e in edge["targetTerm"]["edgesOut"]) == [7, 10, 13]


@pytest.mark.parametrize("depth", [2, 3])
def test_every_row_at_depth(answer, depth):
    body = "edgesOut { id }"
    for _ in range(depth - 1):
        body = f"edgesOut {{ id targetTerm {{ id {body} }} }}"
    (ledger,) = answer(_ledger(body))

    def ids(node, d):
        if d == 1:
            return sorted(e["id"] for e in node["edgesOut"])
        return {e["id"]: ids(e["targetTerm"], d - 1) for e in node["edgesOut"]}

    # ledger: 8 -> party, 9 -> customer, 12 -> customer; party: 14 -> old; customer: 7, 10, 13.
    expected = {
        2: {8: [14], 9: [7, 10, 13], 12: [7, 10, 13]},
        3: {
            8: {14: []},
            9: {7: [14], 10: [], 13: []},
            12: {7: [14], 10: [], 13: []},
        },
    }[depth]
    assert ids(ledger, depth) == expected


def test_sibling_to_many_fields_are_each_their_own_list(answer):
    (ledger,) = answer(
        _ledger(
            'edgesOut(where: { relType: { eq: "KIND_OF" } }) { id targetTerm { id '
            "edgesOut { id } incoming: edgesIn { id } } }"
        )
    )
    customer = ledger["edgesOut"][0]["targetTerm"]
    assert sorted(e["id"] for e in customer["edgesOut"]) == [7, 10, 13]
    assert sorted(e["id"] for e in customer["incoming"]) == [9, 12]


@pytest.mark.parametrize(
    "query, path",
    [
        # A JSON value at the root.
        ('{ on_terms(where: { name: { eq: "old" } }) { id edgesOut { id } } }', ()),
        # A lateral join: the field takes a where.
        (
            '{ on_terms(where: { name: { eq: "old" } }) { id '
            'edgesOut(where: { relType: { eq: "KIND_OF" } }) { id } } }',
            (),
        ),
        # A JSON value under a lateral join: customer -RELATED_TO-> old has no edges.
        (
            '{ on_terms(where: { name: { eq: "customer" } }) { id '
            'edgesOut(where: { relType: { eq: "RELATED_TO" } }) { id targetTerm { id '
            "edgesOut { id } } } } }",
            ("edgesOut", 0, "targetTerm"),
        ),
        # A JSON value inside a JSON value: party -KIND_OF-> old has no edges.
        (
            '{ on_terms(where: { name: { eq: "party" } }) { id '
            "edgesOut { id targetTerm { id edgesOut { id } } } } }",
            ("edgesOut", 0, "targetTerm"),
        ),
    ],
)
def test_a_list_field_with_no_rows_is_an_empty_list(answer, query, path):
    node = answer(query)[0]
    for step in path:
        node = node[step]
    assert node["edgesOut"] == []


def test_on_trino_the_empty_default_is_an_empty_list_and_the_subquery_is_still_joined(meta):  # noqa: F811
    from provisa.transpiler.transpile import transpile_to_trino

    _schema, ctx, _con = meta
    root = _compiled(meta, "{ on_terms { id edgesOut { id } } }")
    trino = transpile_to_trino(make_semantic_sql(root.sql, ctx))
    # The correlated JSON subquery is still rewritten to a joined aggregate for Trino...
    assert "_grel_" in trino and "LEFT JOIN" in trino
    # ...and its default is the text '[]' its json_format(...) aggregate is compared with.
    assert "COALESCE(_grel_0._val, '[]')" in trino
    assert "CAST('[]' AS JSON)" not in trino
    assert "JSON_PARSE('[]')" not in trino


_TRINO_LIST = (
    'SELECT "t0"."order_id", COALESCE((SELECT json_agg(json_build_object(\'eventType\', '
    '"t1"."event_type")) FROM {source}), CAST(\'[]\' AS JSON)) AS "orderEvents" '
    'FROM "pg"."public"."orders" "t0"'
)


@pytest.mark.parametrize(
    "source",
    [
        # a relationship with a row limit: the correlated subquery stays inline
        '(SELECT * FROM "ch"."default"."order_events" "t1" '
        'WHERE "t1"."order_id" = "t0"."order_id" LIMIT 100) "t1"',
        # without one: it becomes a joined aggregate (_grel_0)
        '"ch"."default"."order_events" "t1" WHERE "t1"."order_id" = "t0"."order_id"',
    ],
    ids=["inline", "joined"],
)
def test_on_trino_a_list_default_is_text_like_its_json_format_aggregate(source):
    """Trino refuses COALESCE(varchar, json) (TYPE_MISMATCH): the aggregate is json_format(...)
    text, so its empty default is the text '[]', whatever form the json cast was written in."""
    from provisa.transpiler.transpile import transpile_to_trino

    trino = transpile_to_trino(_TRINO_LIST.format(source=source))
    coalesce = sqlglot.parse_one(trino, read="trino").find(exp.Coalesce)
    assert coalesce is not None
    (default,) = coalesce.expressions
    assert default == exp.Literal.string("[]")


def test_on_trino_a_json_cast_of_a_list_literal_not_defaulting_an_aggregate_stays_json():
    """A statement's own cast of '[]' to json is a json array on Trino (JSON_PARSE), not text."""
    from provisa.transpiler.transpile import transpile_to_trino

    trino = transpile_to_trino("SELECT CAST('[]' AS JSON) AS j, COALESCE(NULL, CAST('[]' AS JSON))")
    assert trino.count("JSON_PARSE('[]')") == 2
