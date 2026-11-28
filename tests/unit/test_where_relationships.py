# Copyright (c) 2026 Kenneth Stott
# Canary: b16eba2c-cfce-4185-bf93-6886f4e842b8
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2136: a where states a relationship's condition, Hasura's shape -- ``{ rel: { ... } }`` --
read as "a related row matches it" and compiled to the row's key IN the keys of the relationship's
target rows matching it, governed as the rest of the statement is; and ``ilike`` matches with case
ignored."""

from pathlib import Path

import pytest
from graphql import parse, validate

from provisa.compiler.rls import RLSContext
from provisa.compiler.sql_gen import compile_query
from tests.helpers import governed_for_role
from tests.unit.test_sql_gen import _build_schema_and_ctx

ORDERS, CUSTOMERS = 1, 2

# Both ways between orders and customers: each order's customer, each customer's orders.
RELATIONSHIPS = [
    {
        "id": "ord-cust",
        "source_table_id": ORDERS,
        "target_table_id": CUSTOMERS,
        "source_column": "customer_id",
        "target_column": "id",
        "cardinality": "many-to-one",
    },
    {
        "id": "cust-ord",
        "source_table_id": CUSTOMERS,
        "target_table_id": ORDERS,
        "source_column": "id",
        "target_column": "customer_id",
        "cardinality": "one-to-many",
        "graphql_alias": "orders",
    },
]


@pytest.fixture
def schema_and_ctx():
    return _build_schema_and_ctx(relationships=RELATIONSHIPS)


def _compiled(schema_and_ctx, query: str):
    schema, ctx = schema_and_ctx
    doc = parse(query)
    assert not validate(schema, doc)
    return compile_query(doc, ctx)[0]


def test_the_where_offers_each_relationship_as_its_target_s_where(schema_and_ctx):
    schema, _ = schema_and_ctx
    customers_where = schema.get_type("CustomersWhere")
    assert "orders" in customers_where.fields
    # Its condition is the target's own where, relationships and all.
    assert customers_where.fields["orders"].type.name == "OrdersWhere"
    assert "customer" in schema.get_type("OrdersWhere").fields


def test_a_relationship_s_condition_is_its_key_in_the_target_s_keys(schema_and_ctx):
    q = _compiled(
        schema_and_ctx, '{ customers(where: { orders: { region: { eq: "us" } } }) { id name } }'
    )
    assert 'FROM "public"."customers" "t0"' in q.sql
    # Uncorrelated, so PostgreSQL plans it once and hashes it (REQ-2174's reach filter); null keys
    # left out and a null source key FALSE, so it is true exactly where EXISTS is.
    assert (
        'COALESCE("t0"."id" IN (SELECT "w1"."customer_id" FROM "public"."orders" "w1"'
        ' WHERE "w1"."customer_id" IS NOT NULL AND "w1"."region" = $1), FALSE)'
    ) in q.sql
    assert q.params == ["us"]
    # The target is one of the statement's tables: routing and the floor see it.
    assert q.table_ids == frozenset({ORDERS, CUSTOMERS})


def test_an_empty_condition_is_any_related_row(schema_and_ctx):
    q = _compiled(schema_and_ctx, "{ customers(where: { orders: {} }) { id } }")
    assert 'WHERE "w1"."customer_id" IS NOT NULL AND TRUE), FALSE)' in q.sql


def test_conditions_combine_and_nest_each_under_its_own_alias(schema_and_ctx):
    q = _compiled(
        schema_and_ctx,
        """{ orders(where: { _or: [
              { customer: { orders: { status: { eq: "open" } } } },
              { region: { eq: "eu" } }
            ] }) { id } }""",
    )
    assert 'COALESCE("t0"."customer_id" IN (SELECT "w1"."id" FROM' in q.sql
    assert (
        'COALESCE("w1"."id" IN (SELECT "w2"."customer_id" FROM "public"."orders" "w2"'
        ' WHERE "w2"."customer_id" IS NOT NULL AND "w2"."status" = $1), FALSE)'
    ) in q.sql
    assert '"t0"."region" = $2' in q.sql
    assert q.params == ["open", "eu"]


@pytest.mark.parametrize(
    ("where", "kept"),
    [
        ('{ customer: { name: { eq: "ann" } } }', [1]),
        # NOT keeps what NOT EXISTS keeps: the order with no customer, and one whose customer
        # is not ann -- never dropped as an IN over a null would drop it.
        ('{ _not: { customer: { name: { eq: "ann" } } } }', [2, 3]),
        ("{ customer: {} }", [1, 2]),
        ("{ _not: { customer: {} } }", [3]),
    ],
)
def test_a_relationship_s_condition_holds_where_exists_holds(schema_and_ctx, where, kept):
    import duckdb

    q = _compiled(schema_and_ctx, f"{{ orders(where: {where}) {{ id }} }}")
    db = duckdb.connect()
    db.execute("CREATE SCHEMA public")
    db.execute("CREATE TABLE public.customers (id INTEGER, name VARCHAR)")
    db.execute("INSERT INTO public.customers VALUES (10, 'ann'), (11, 'bo'), (NULL, 'ann')")
    db.execute("CREATE TABLE public.orders (id INTEGER, customer_id INTEGER)")
    db.execute("INSERT INTO public.orders VALUES (1, 10), (2, 11), (3, NULL)")
    rows = db.execute(q.sql, q.params).fetchall()
    assert sorted(r[0] for r in rows) == kept


def test_the_exists_body_is_governed_with_the_target_s_row_rules(schema_and_ctx):
    _, ctx = schema_and_ctx
    q = _compiled(
        schema_and_ctx, '{ customers(where: { orders: { status: { eq: "open" } } }) { id } }'
    )
    rls = RLSContext(rules={ORDERS: "\"region\" = 'us-east'"})
    governed = governed_for_role(q.sql, ctx, "admin", rls=rls)
    related = governed[governed.index(" IN (SELECT") :]
    assert "'us-east'" in related


def test_ilike_ignores_case(schema_and_ctx):
    q = _compiled(schema_and_ctx, '{ customers(where: { name: { ilike: "%ann%" } }) { id } }')
    assert 'LOWER("name") LIKE LOWER($1)' in q.sql
    assert q.params == ["%ann%"]


def test_a_relationship_s_condition_in_a_nested_where_correlates_with_the_nested_rows(
    schema_and_ctx,
):
    """A nested field's where states a relationship's condition as a root's does: the nested
    field's own rows' key IN the target's keys (the Ontology View's edges to the terms
    its filters keep)."""
    q = _compiled(
        schema_and_ctx,
        '{ customers { id orders(where: { customer: { name: { eq: "x" } } }) { id } } }',
    )
    # REQ-2174: applied inside the field's grouped read, to the rows it groups.
    assert 'FROM "public"."orders" "t1" WHERE COALESCE(' in q.sql
    assert 'GROUP BY "t1"."customer_id"' in q.sql
    assert (
        'COALESCE("t1"."customer_id" IN (SELECT "w1"."id" FROM "public"."customers" "w1"'
        ' WHERE "w1"."id" IS NOT NULL AND "w1"."name" = $1), FALSE)'
    ) in q.sql
    assert q.params[0] == "x"


def test_a_nested_where_s_exists_body_is_governed_with_the_target_s_row_rules(schema_and_ctx):
    _, ctx = schema_and_ctx
    q = _compiled(
        schema_and_ctx,
        '{ customers { id orders(where: { customer: { name: { eq: "x" } } }) { id } } }',
    )
    rls = RLSContext(rules={CUSTOMERS: "\"name\" = 'ann'"})
    columns = {
        ORDERS: [("id", "integer"), ("customer_id", "integer")],
        CUSTOMERS: [("id", "integer"), ("name", "varchar")],
    }
    governed = governed_for_role(q.sql, ctx, "admin", rls=rls, column_types=columns)
    related = governed[governed.index(" IN (SELECT") :]
    assert '"w1"."name" = \'ann\'' in related


def test_a_nested_where_without_a_relationship_reads_its_rows_under_the_alias(schema_and_ctx):
    q = _compiled(
        schema_and_ctx, '{ customers { id orders(where: { status: { eq: "open" } }) { id } } }'
    )
    # REQ-2174: the grouped read keeps the rows it groups, read under the field's alias.
    assert 'FROM "public"."orders" "t1" WHERE "t1"."status" = $1 GROUP BY' in q.sql


def test_an_empty_or_holds_for_no_row_and_an_empty_and_for_every_row(schema_and_ctx):
    q = _compiled(schema_and_ctx, "{ customers(where: { _or: [] }) { id } }")
    assert "WHERE FALSE" in q.sql
    q = _compiled(schema_and_ctx, "{ customers(where: { _and: [] }) { id } }")
    assert "WHERE TRUE" in q.sql


def test_the_glossary_s_terms_and_edges_reach_each_other_both_ways():
    from provisa.api._meta_seed import META_RELATIONSHIPS

    by_alias = {r[9]: r for r in META_RELATIONSHIPS if r[0].startswith("ontology:_term")}
    assert by_alias["edgesOut"][2:8] == (
        "_terms",
        "id",
        "from_term_id",
        "one-to-many",
        by_alias["edgesOut"][6],
        "_term_edges",
    )
    assert by_alias["edgesIn"][4] == "to_term_id"
    assert by_alias["sourceTerm"][3] == "from_term_id"
    assert by_alias["targetTerm"][3] == "to_term_id"


def test_a_relationship_target_is_always_read_under_the_joins_alias():
    """A star under a lateral is expanded by its table's alias: with none, a table the statement
    renames expanded under a physical name the FROM no longer held (Binder Error)."""
    source = Path("provisa/compiler/sql_selection.py").read_text()
    assert 'f"LEFT JOIN LATERAL (SELECT {distinct}* FROM {target} {_q(join_alias)} "' in source
