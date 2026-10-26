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

from provisa.api._meta_seed import META_JUNCTION_RELATIONSHIPS, META_RELATIONSHIPS
from provisa.compiler.context import build_context
from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.schema_gen import SchemaInput, generate_schema

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
    # The Cypher is run as written, with no parameters: the values the SQL binds are in it.
    assert "$" not in cypher, cypher
    sql_rows = _rows(con, _inline(make_semantic_sql(root.sql, ctx), root.params))
    assert sql_rows, "the case must answer some rows"
    assert _cypher_rows(con, cypher, lm, ctx) == sql_rows, cypher
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


@pytest.mark.parametrize(
    "query",
    [
        "{ customers(where: { id: { in: [1, 3] } }) { id } }",
        '{ customers(where: { name: { eq: "Bo" } }) { id } }',
        '{ customers(where: { orders: { region: { in: ["eu"] } } }) { id } }',
    ],
)
def test_a_condition_of_one_table_s_columns_and_lists(model, con, query):
    # A column of the root's one table names its variable; SQL's IN (...) is Cypher's IN [...].
    cypher = _same_rows(model, con, query)
    assert " IN (" not in cypher and "WHERE id" not in cypher and "WHERE name" not in cypher


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
                type_name="GlossaryTerm",
                domain_id="meta",
                field_name="meta__glossary_terms",
                table_name="glossary_terms_meta",
                original_table_name="glossary_terms",
                display_name=None,
            ),
            "meta__glossary_term_edges": SimpleNamespace(
                type_name="GlossaryTermEdge",
                domain_id="meta",
                field_name="meta__glossary_term_edges",
                table_name="glossary_term_edges_meta",
                original_table_name="glossary_term_edges",
                display_name=None,
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
    "where, types",
    [
        (" WHERE \"rel_type\" IN ('kind_of', 'part_of')", ["KIND_OF", "PART_OF"]),
        (" WHERE \"rel_type\" = 'kind_of'", ["KIND_OF"]),
        (' WHERE "rel_type" IN (\'kind_of\') AND "note" IS NOT NULL', ["KIND_OF"]),
    ],
)
def test_a_junction_root_is_its_relationships(terms, where, types):
    lm, ctx, con = terms
    sql = (
        'SELECT "id", "from_term_id", "to_term_id", "rel_type" FROM "meta"."glossary_term_edges"'
        + where
    )
    cypher = semantic_sql_to_cypher(sql, lm, ctx)
    assert cypher is not None
    for t in types:
        assert f"MATCH (a:GlossaryTerm)-[r:{t}]->(b:GlossaryTerm)" in cypher
    sql_rows = _rows(con, sql)
    assert sql_rows and _cypher_rows(con, cypher, lm, None) == sql_rows, cypher


@pytest.mark.parametrize(
    "where",
    [
        "",  # every row: one of an undeclared type is no relationship
        ' WHERE "note" IS NOT NULL',
        " WHERE \"rel_type\" IN ('kind_of', 'antonym_of')",  # a type the graph does not declare
    ],
)
def test_a_junction_root_not_kept_to_declared_types_has_no_cypher_form(terms, where):
    lm, ctx, con = terms
    con.execute(
        "INSERT INTO provisa_admin.public.glossary_term_edges VALUES (10, 1, 3, 'antonym_of', 'x')"
    )
    sql = 'SELECT "id", "rel_type" FROM "meta"."glossary_term_edges"' + where
    assert semantic_sql_to_cypher(sql, lm, ctx) is None


def test_a_junction_root_with_an_order_across_its_types_has_no_cypher_form(terms):
    lm, ctx, _con = terms
    sql = (
        'SELECT "id" FROM "meta"."glossary_term_edges" WHERE "rel_type" IN (\'kind_of\', '
        "'part_of') ORDER BY \"id\" LIMIT 2"
    )
    assert semantic_sql_to_cypher(sql, lm, ctx) is None


# --- (4): the system catalog's own model: terms and their junction-backed edges (the ontology
# schema's, REQ-2163), and their tables (meta's) -----------------------------------------------

_DOMAIN_OF = {"_terms": "ontology", "_term_edges": "ontology", "registered_tables": "meta"}
_VIEW_OF = {"_terms": "_terms_ontology", "_term_edges": "_term_edges_ontology"}
#: Each table as semantic SQL names it (domain.field): an ontology table's leading underscore is
#: not in its field.
_SEMANTIC = {
    "_terms": "ontology.terms",
    "_term_edges": "ontology.term_edges",
    "registered_tables": "meta.registered_tables",
}

_META_COLUMNS = {
    "_terms": [("id", "integer"), ("name", "varchar"), ("retired", "boolean")],
    "_term_edges": [
        ("id", "integer"),
        ("from_term_id", "integer"),
        ("to_term_id", "integer"),
        ("rel_type", "varchar"),
    ],
    "registered_tables": [
        ("id", "integer"),
        ("table_name", "varchar"),
        ("domain_id", "varchar"),
        ("class_term_id", "integer"),
    ],
}


@pytest.fixture
def meta():
    """The three meta tables with the relationships the meta domain seeds between them
    (META_RELATIONSHIPS, META_JUNCTION_RELATIONSHIPS), as the Ontology View queries them.

    party; customer KIND_OF party and KIND_OF old (retired); ledger PART_OF party and KIND_OF
    customer; lonely ANTONYM_OF party (a declared type no filter here keeps). party grounds a
    table."""
    ids = {name: i for i, name in enumerate(_META_COLUMNS, 1)}
    tables = [
        {
            "id": ids[name],
            "source_id": "provisa-admin",
            "domain_id": _DOMAIN_OF[name],
            "schema_name": "public",
            "table_name": name,
            "columns": [{"column_name": c, "visible_to": []} for c, _ in cols],
            "write_ops": [],
            "write_returns_rows": False,
            "write_refused_forms": [],
        }
        for name, cols in _META_COLUMNS.items()
    ]
    rels = [
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
    ] + [
        {
            "id": rid,
            "source_table_id": ids[st],
            "target_table_id": ids[tt],
            "source_column": sc,
            "target_column": tc,
            "cardinality": card,
            "via_table_id": ids[vt],
            "via_source_column": vs,
            "via_target_column": vtc,
            "via_type_column": vty,
            "via_type_value": value,
            "via_label_source": label_source,
            "alias": alias,
        }
        for (rid, _s, st, sc, tc, card, _t, tt, vt, vs, vtc, vty, value, label_source, alias)
        in META_JUNCTION_RELATIONSHIPS
        if st in ids and tt in ids and vt in ids
    ]  # fmt: skip
    si = SchemaInput(
        tables=tables,
        relationships=rels,
        column_types={
            ids[name]: [
                ColumnMetadata(column_name=c, data_type=t, is_nullable=True) for c, t in cols
            ]
            for name, cols in _META_COLUMNS.items()
        },
        naming_rules=[],
        role={"id": "admin", "capabilities": ["query_development"], "domain_access": ["*"]},
        domains=[
            {"id": "meta", "description": "Meta"},
            {"id": "ontology", "description": "Ontology", "graphql_alias": "ont"},
        ],
        # As the server registers them: each table read through its system view, its fields
        # domain-prefixed.
        domain_prefix=True,
        physical_table_map={name: _VIEW_OF.get(name, f"{name}_meta") for name in _META_COLUMNS},
    )
    ctx = build_context(si)
    c = duckdb.connect()
    c.execute("CREATE SCHEMA meta")
    c.execute("CREATE SCHEMA ontology")
    c.execute("CREATE TABLE ontology.terms (id INTEGER, name VARCHAR, retired BOOLEAN)")
    c.execute(
        "INSERT INTO ontology.terms VALUES (1,'party',false),(2,'customer',false),"
        "(3,'ledger',false),(4,'old',true),(5,'lonely',false)"
    )
    c.execute(
        "CREATE TABLE ontology.term_edges (id INTEGER, from_term_id INTEGER, "
        "to_term_id INTEGER, rel_type VARCHAR)"
    )
    c.execute(
        "INSERT INTO ontology.term_edges VALUES (7,2,1,'KIND_OF'),(8,3,1,'PART_OF'),"
        "(9,3,2,'KIND_OF'),(10,2,4,'KIND_OF'),(11,5,1,'ANTONYM_OF')"
    )
    c.execute(
        "CREATE TABLE meta.registered_tables (id INTEGER, table_name VARCHAR, domain_id VARCHAR, "
        "class_term_id INTEGER)"
    )
    c.execute("INSERT INTO meta.registered_tables VALUES (10, 'orders', 'sales', 1)")
    # The Cypher read names the tables physically, in the admin store's own schema.
    c.execute("ATTACH ':memory:' AS provisa_admin")
    c.execute("CREATE SCHEMA provisa_admin.public")
    for name in _META_COLUMNS:
        c.execute(
            f"CREATE VIEW provisa_admin.public.{_VIEW_OF.get(name, f'{name}_meta')} AS "
            f"SELECT * FROM memory.{_SEMANTIC[name]}"
        )
    return (generate_schema(si), ctx, CypherLabelMap.from_schema(ctx, domain_access=["*"])), c


_KEPT = 'relType: { in: ["KIND_OF", "PART_OF"] }'
#: The Ontology View's one query: live terms with an edge of a kept type to a live term, either
#: way, or a table; each with its edges out of those types, each edge's target, and its tables.
_ONTOLOGY = (
    "{ ont__terms(where: { retired: { eq: false }, _or: ["
    f"{{ edgesOut: {{ {_KEPT}, targetTerm: {{ retired: {{ eq: false }} }} }} }}, "
    f"{{ edgesIn: {{ {_KEPT}, sourceTerm: {{ retired: {{ eq: false }} }} }} }}, "
    "{ tables: {} } ] }) { id name "
    f"edgesOut(where: {{ {_KEPT} }}) {{ id fromTermId toTermId relType targetTerm {{ id }} }} "
    "tables { id tableName domainId } } }"
)


def test_the_ontology_view_s_one_query_is_its_cypher(meta):
    model, con = meta
    cypher = _same_rows(model, con, _ONTOLOGY)
    assert "OPTIONAL MATCH (a)-[jr0:KIND_OF|PART_OF]->(j0:Terms)" in cypher
    assert "EXISTS { MATCH (a)<-[" in cypher and "-[:GROUNDS]->" in cypher
    assert "t1." not in cypher and "-[]-" not in cypher


@pytest.mark.parametrize(
    "query",
    [
        # A filtered root with a to-many junction field: the filter is the root's, not dropped.
        '{ ont__terms(where: { name: { eq: "ledger" } }) { id '
        f"edgesOut(where: {{ {_KEPT} }}) {{ id toTermId }} }} }}",
        # One kept type, and the edges into a term.
        '{ ont__terms { id edgesIn(where: { relType: { eq: "KIND_OF" } }) { id fromTermId } } }',
        # A relationship's condition over the junction, either way, with its far term's own.
        f"{{ ont__terms(where: {{ edgesIn: {{ {_KEPT}, sourceTerm: {{ name: {{ eq: "
        '"ledger" } } } }) { id } }',
        "{ ont__terms(where: { tables: {} }) { id tables { tableName } } }",
    ],
)
def test_a_junction_field_or_condition_is_its_relationships(meta, query):
    model, con = meta
    cypher = _same_rows(model, con, query)
    assert "t1." not in cypher and "-[]-" not in cypher


def test_a_junction_field_not_kept_to_declared_types_has_no_cypher_form(meta):
    # Every edge row, ANTONYM_OF's included: it has the declared types' rows and others'.
    (schema, ctx, lm), _con = meta
    (root,) = compile_query(parse("{ ont__terms { id edgesOut { id } } }"), ctx)
    semantic = make_semantic_sql(root.sql, ctx)
    assert semantic_sql_to_cypher(semantic, lm, ctx, params=root.params) is None


@pytest.mark.parametrize(
    "wrapper",
    [
        'SELECT * FROM "ontology"."terms" AS "t0" WHERE "t0"."name" = \'ledger\' LIMIT 100',
        'SELECT "t0"."id", "t0"."name" FROM "ontology"."terms" AS "t0" '
        'WHERE ("t0"."name" = \'ledger\') LIMIT 100',
    ],
)
def test_a_limit_wrapper_keeps_its_condition(meta, wrapper):
    # sql_gen's wrapper selects * or the table's columns; either way its WHERE is the root's.
    (_schema, ctx, lm), con = meta
    sql = f'SELECT "t0"."id" AS "id" FROM ({wrapper}) AS "t0"'
    cypher = semantic_sql_to_cypher(sql, lm, ctx, override_limit=100)
    assert cypher is not None and "ledger" in cypher
    assert _cypher_rows(con, cypher, lm, ctx) == _rows(con, sql) == ["[3]"]


def test_a_join_with_no_cypher_form_is_not_returned_unbound(meta):
    (_schema, ctx, lm), _con = meta
    sql = (
        'SELECT "t0"."id" AS "id", "t1"."x" AS "x" FROM "ontology"."terms" AS "t0" '
        'LEFT JOIN LATERAL (SELECT * FROM "meta"."unregistered" WHERE "k" = "t0"."id") AS "t1" '
        "ON TRUE"
    )
    assert semantic_sql_to_cypher(sql, lm, ctx) is None


# --- a to-many field's own pick of each parent's rows (Open as query, REQ-2136) -----------------


@pytest.mark.parametrize(
    "query",
    [
        '{ customers(where: { name: { eq: "Ann" } }) { id name '
        "orders(order_by: { amount: desc }, limit: 1) { id amount } } }",
        "{ customers { id name orders(order_by: { amount: desc }, limit: 1) { id amount } } }",
        "{ customers { id orders(order_by: { amount: asc }, offset: 1, limit: 1) { id amount } } }",
        "{ customers { id orders(limit: 1, order_by: { id: asc }) { id } } }",
    ],
)
def test_a_to_many_field_s_order_and_limit_are_each_parent_s(model, con, query):
    cypher = _same_rows(model, con, query)
    assert "CALL {" in cypher and "LIMIT 1\n}" in cypher, cypher


@pytest.mark.parametrize(
    "query",
    [
        # A condition beside the relationship's own.
        '{ customers { id orders(where: { status: { eq: "shipped" } }, limit: 1) { id } } }',
        # Nulls placed otherwise than Cypher places them.
        "{ customers { id orders(order_by: { amount: asc_nulls_first }, limit: 1) { id } } }",
        "{ customers { id orders(order_by: { amount: desc_nulls_last }, limit: 1) { id } } }",
        # distinct_on.
        "{ customers { id orders(distinct_on: [status], limit: 1) { id } } }",
    ],
)
def test_a_pick_cypher_cannot_state_exactly_has_no_export(model, query):
    schema, ctx, lm = model
    (root,) = compile_query(parse(query), ctx)
    assert (
        semantic_sql_to_cypher(
            make_semantic_sql(root.sql, ctx),
            lm,
            ctx,
            override_limit=root.result_limit,
            params=root.params,
        )
        is None
    )
