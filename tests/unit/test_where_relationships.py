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
read as "a related row matches it" and compiled to EXISTS over the relationship's target, governed
as the rest of the statement is; and ``ilike`` matches with case ignored."""

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


def test_a_relationship_s_condition_is_an_exists_over_its_target(schema_and_ctx):
    q = _compiled(
        schema_and_ctx, '{ customers(where: { orders: { region: { eq: "us" } } }) { id name } }'
    )
    assert 'FROM "public"."customers" "t0"' in q.sql
    assert (
        'EXISTS (SELECT 1 FROM "public"."orders" "w1" WHERE "w1"."customer_id" = "t0"."id"'
        ' AND ("w1"."region" = $1))'
    ) in q.sql
    assert q.params == ["us"]
    # The target is one of the statement's tables: routing and the floor see it.
    assert q.table_ids == frozenset({ORDERS, CUSTOMERS})


def test_an_empty_condition_is_any_related_row(schema_and_ctx):
    q = _compiled(schema_and_ctx, "{ customers(where: { orders: {} }) { id } }")
    assert 'WHERE "w1"."customer_id" = "t0"."id" AND (TRUE))' in q.sql


def test_conditions_combine_and_nest_each_under_its_own_alias(schema_and_ctx):
    q = _compiled(
        schema_and_ctx,
        """{ orders(where: { _or: [
              { customer: { orders: { status: { eq: "open" } } } },
              { region: { eq: "eu" } }
            ] }) { id } }""",
    )
    assert '"w1"."id" = "t0"."customer_id"' in q.sql
    assert '"w2"."customer_id" = "w1"."id" AND ("w2"."status" = $1)' in q.sql
    assert '"t0"."region" = $2' in q.sql
    assert q.params == ["open", "eu"]


def test_the_exists_body_is_governed_with_the_target_s_row_rules(schema_and_ctx):
    _, ctx = schema_and_ctx
    q = _compiled(
        schema_and_ctx, '{ customers(where: { orders: { status: { eq: "open" } } }) { id } }'
    )
    rls = RLSContext(rules={ORDERS: "\"region\" = 'us-east'"})
    governed = governed_for_role(q.sql, ctx, "admin", rls=rls)
    exists = governed[governed.index("EXISTS") :]
    assert "'us-east'" in exists


def test_ilike_ignores_case(schema_and_ctx):
    q = _compiled(schema_and_ctx, '{ customers(where: { name: { ilike: "%ann%" } }) { id } }')
    assert 'LOWER("name") LIKE LOWER($1)' in q.sql
    assert q.params == ["%ann%"]


def test_a_relationship_s_condition_in_a_nested_where_is_refused_not_dropped(schema_and_ctx):
    with pytest.raises(ValueError, match="unsupported condition"):
        _compiled(
            schema_and_ctx,
            '{ customers { id orders(where: { customer: { name: { eq: "x" } } }) { id } } }',
        )


def test_an_empty_or_holds_for_no_row_and_an_empty_and_for_every_row(schema_and_ctx):
    q = _compiled(schema_and_ctx, "{ customers(where: { _or: [] }) { id } }")
    assert "WHERE FALSE" in q.sql
    q = _compiled(schema_and_ctx, "{ customers(where: { _and: [] }) { id } }")
    assert "WHERE TRUE" in q.sql


def test_the_glossary_s_terms_and_edges_reach_each_other_both_ways():
    from provisa.api._meta_seed import META_RELATIONSHIPS

    by_alias = {r[9]: r for r in META_RELATIONSHIPS if r[0].startswith("meta:glossary_term")}
    assert by_alias["edgesOut"][2:8] == (
        "glossary_terms",
        "id",
        "from_term_id",
        "one-to-many",
        by_alias["edgesOut"][6],
        "glossary_term_edges",
    )
    assert by_alias["edgesIn"][4] == "to_term_id"
    assert by_alias["sourceTerm"][3] == "from_term_id"
    assert by_alias["targetTerm"][3] == "to_term_id"
