# Copyright (c) 2026 Kenneth Stott
# Canary: 9b99721c-c105-4ff3-bc91-4e724cbc072e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A GraphQL root's Cypher means what its SQL means (REQ-2136, REQ-345): a relationship's
condition is a pattern predicate on the root's own rows; a to-one field is its node's map and a
to-many one the list of its maps, at any depth; a root over a junction is the relationships it
holds. Each case runs the root's SQL and its Cypher -- compiled as a Cypher read is -- on DuckDB,
to the same rows."""

# Requirements: REQ-2136, REQ-345, REQ-1586

import json
from types import SimpleNamespace

import duckdb
import pytest
import sqlglot
import sqlglot.expressions as exp
from graphql import parse, validate

from provisa.compiler.sql_gen import compile_query
from provisa.compiler.sql_rewrite import make_semantic_sql
from provisa.cypher.parser import parse_cypher
from provisa.cypher.sql_to_cypher import semantic_sql_to_cypher
from provisa.gql.graph_rewriter import apply_graph_rewrites
from provisa.gql.label_map import CypherLabelMap, JunctionMapping, RelationshipMapping
from provisa.gql.translator import cypher_to_sql
from tests.unit.test_cypher_junction_relationships import _term_node
from tests.unit.test_sql_gen import _build_schema_and_ctx
from tests.unit.test_where_relationships import RELATIONSHIPS


def _duckdb(node: exp.Expression) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]
    # DuckDB names Postgres's json_agg json_group_array.
    if isinstance(node, exp.JSONArrayAgg):
        return exp.Anonymous(this="json_group_array", expressions=[node.this])
    return node


def _value(v):
    """A value as both answers can be compared by: JSON decoded, a list in order of its items,
    an empty list as no list (SQL's json_agg of no row is NULL)."""
    if isinstance(v, str) and v[:1] in "[{":
        v = json.loads(v)
    if isinstance(v, list):
        return sorted((_value(x) for x in v), key=json.dumps) or None
    if isinstance(v, dict):
        # The Cypher read's map holds each value as JSON text.
        return {k: _value(_decoded(x)) for k, x in v.items()}
    return v


def _decoded(x):
    if isinstance(x, str):
        try:
            return json.loads(x)
        except ValueError:
            return x
    return x


def _rows(con, sql: str) -> list[str]:
    duck = sqlglot.parse_one(sql, read="postgres").transform(_duckdb).sql(dialect="duckdb")
    return sorted(
        json.dumps([_value(x) for x in row], default=str) for row in con.execute(duck).fetchall()
    )


def _inline(text: str, params: list) -> str:
    for i, v in reversed(list(enumerate(params or [], 1))):
        text = text.replace(f"${i}", f"'{v}'" if isinstance(v, str) else str(v))
    return text


def _cypher_rows(con, cypher: str, lm: CypherLabelMap, ctx) -> list[str]:
    sql_ast, _, graph_vars = cypher_to_sql(parse_cypher(cypher), lm, {})
    sql = apply_graph_rewrites(sql_ast, graph_vars, lm).sql(dialect="postgres")
    return _rows(con, make_semantic_sql(sql, ctx) if ctx is not None else sql)


# --- (1) and (2): relationship conditions and nested fields -------------------------------


@pytest.fixture
def model():
    schema, ctx = _build_schema_and_ctx(relationships=RELATIONSHIPS)
    return schema, ctx, CypherLabelMap.from_schema(ctx, domain_access=["*"])


@pytest.fixture
def con() -> duckdb.DuckDBPyConnection:
    """Ann has orders 10 (us, new) and 12 (us, shipped), Bo order 11 (eu), Cy none."""
    c = duckdb.connect()
    c.execute("CREATE SCHEMA sales")
    c.execute("CREATE TABLE sales.customers (id INTEGER, name VARCHAR, email VARCHAR)")
    c.execute("INSERT INTO sales.customers VALUES (1,'Ann','a@x'),(2,'Bo','b@x'),(3,'Cy','c@x')")
    c.execute(
        "CREATE TABLE sales.orders (id INTEGER, customer_id INTEGER, amount DECIMAL(10,2), "
        "region VARCHAR, status VARCHAR, created_at TIMESTAMP)"
    )
    c.execute(
        "INSERT INTO sales.orders VALUES (10,1,5,'us','new',NULL),(11,2,7,'eu','new',NULL),"
        "(12,1,9,'us','shipped',NULL)"
    )
    return c


def _same_rows(model, con, query: str) -> str:
    schema, ctx, lm = model
    doc = parse(query)
    assert not validate(schema, doc)
    (root,) = compile_query(doc, ctx)
    cypher = semantic_sql_to_cypher(
        make_semantic_sql(root.sql, ctx),
        lm,
        ctx,
        override_limit=root.result_limit,
        params=root.params,
    )
    assert cypher is not None
    sql_rows = _rows(con, _inline(make_semantic_sql(root.sql, ctx), root.params))
    assert sql_rows, "the case must answer some rows"
    assert _cypher_rows(con, _inline(cypher, root.params), lm, ctx) == sql_rows, cypher
    return cypher


@pytest.mark.parametrize(
    "query",
    [
        '{ customers(where: { orders: { region: { eq: "us" } } }) { id name } }',
        '{ customers(where: { _or: [{ orders: { region: { eq: "eu" } } }, { name: { eq: "Cy" } }] }) { id } }',
        '{ orders(where: { customer: { orders: { status: { eq: "shipped" } } } }) { id } }',
    ],
)
def test_a_relationship_condition_is_a_pattern_predicate(model, con, query):
    cypher = _same_rows(model, con, query)
    assert "SELECT" not in cypher and "EXISTS {" in cypher


def test_the_condition_is_on_the_root_s_rows_not_an_optional_match_s(model, con):
    cypher = _same_rows(
        model, con, "{ customers(where: { orders: {} }) { id name orders { id } } }"
    )
    lines = cypher.splitlines()
    assert lines[0].startswith("MATCH (a:Customers)") and lines[1].startswith("WHERE EXISTS {")


@pytest.mark.parametrize(
    "query",
    [
        "{ customers { id orders { id } } }",
        "{ orders { id customer { name } } }",
        "{ orders { id customer { name orders { id } } } }",
        "{ customers { id orders { id customer { name } } } }",
        "{ customers { name orders { id } o2: orders { region } } }",
    ],
)
def test_a_nested_field_is_its_map_or_its_list_of_maps(model, con, query):
    cypher = _same_rows(model, con, query)
    assert "collect(" not in cypher.lower().replace("collect {", "")


def test_a_to_one_field_is_a_map_not_a_list(model, con):
    cypher = _same_rows(model, con, "{ orders { id customer { name } } }")
    assert "CASE WHEN b.id IS NULL THEN NULL ELSE {name: b.name} END AS customer" in cypher


# --- (3): a junction root ------------------------------------------------------------------


def _edge(rel_type: str, stored: str) -> RelationshipMapping:
    return RelationshipMapping(
        rel_type=rel_type,
        source_label="GlossaryTerm",
        target_label="GlossaryTerm",
        join_source_column="id",
        join_target_column="id",
        field_name="edges",
        via=JunctionMapping(
            catalog_name="provisa_admin",
            schema_name="public",
            table_name="glossary_term_edges",
            type_name="GlossaryTermEdge",
            source_columns=("from_term_id",),
            target_columns=("to_term_id",),
            type_column="rel_type",
            type_value=stored,
            attributes={"id": "id", "note": "note"},
            label_source="column",
        ),
    )


@pytest.fixture
def terms():
    kind, part = _edge("KIND_OF", "kind_of"), _edge("PART_OF", "part_of")
    lm = CypherLabelMap(
        nodes={"GlossaryTerm": _term_node()},
        relationships={"KIND_OF": kind, "PART_OF": part},
        aliases={"KIND_OF": [kind], "PART_OF": [part]},
    )
    ctx = SimpleNamespace(
        tables={
            "meta__glossary_terms": SimpleNamespace(
                type_name="GlossaryTerm", domain_id="meta", field_name="meta__glossary_terms"
            ),
            "meta__glossary_term_edges": SimpleNamespace(
                type_name="GlossaryTermEdge",
                domain_id="meta",
                field_name="meta__glossary_term_edges",
            ),
        }
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
        "INSERT INTO provisa_admin.public.glossary_term_edges VALUES "
        "(7, 2, 1, 'kind_of', 'n1'), (8, 3, 1, 'part_of', NULL), (9, 3, 2, 'kind_of', 'n3')"
    )
    c.execute("CREATE SCHEMA meta")
    c.execute(
        "CREATE VIEW meta.glossary_term_edges AS SELECT * FROM provisa_admin.public.glossary_term_edges"
    )
    return lm, ctx, c


@pytest.mark.parametrize(
    "where",
    ["", " WHERE \"rel_type\" IN ('kind_of')", ' WHERE "note" IS NOT NULL'],
)
def test_a_junction_root_is_its_relationships(terms, where):
    lm, ctx, con = terms
    sql = (
        'SELECT "id", "from_term_id", "to_term_id", "rel_type" FROM "meta"."glossary_term_edges"'
        + where
    )
    cypher = semantic_sql_to_cypher(sql, lm, ctx)
    assert cypher is not None
    assert "MATCH (a:GlossaryTerm)-[r:KIND_OF]->(b:GlossaryTerm)" in cypher
    assert "UNION ALL" in cypher
    sql_rows = _rows(con, sql)
    assert sql_rows and _cypher_rows(con, cypher, lm, None) == sql_rows, cypher


def test_a_junction_root_with_an_order_across_its_types_has_no_cypher_form(terms):
    lm, ctx, _con = terms
    sql = 'SELECT "id" FROM "meta"."glossary_term_edges" ORDER BY "id" LIMIT 2'
    assert semantic_sql_to_cypher(sql, lm, ctx) is None
