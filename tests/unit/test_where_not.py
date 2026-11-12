# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A where object's ``_not`` (REQ-2143, PAGE SEARCH USES THE WEB-SEARCH SYNTAX: the page search's
exclusions are read by the Ontology View's query): the rows its condition does not hold for, as
SQL's NOT reads them -- a comparison with a null stays null and keeps no row. It is the caller's
filter inside the one governed statement, and a masked column inside it is refused (REQ-531)."""

from __future__ import annotations

import duckdb
import pytest
import sqlglot
from graphql import GraphQLInputObjectType

from provisa.compiler.context import build_context
from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.parser import parse_query
from provisa.compiler.rls import RLSContext
from provisa.compiler.schema_gen import SchemaInput, generate_schema
from provisa.compiler.sql_gen import compile_query
from provisa.compiler.sql_rewrite import lower_to_catalog_physical
from provisa.compiler.sql_validator import validate_sql
from provisa.compiler.stage2 import apply_governance, build_governance_context
from provisa.security.masking import MaskingRule, MaskType

ROLE = {"id": "clerk", "capabilities": ["query_development"], "domain_access": ["*"]}
MASKED = MaskingRule(mask_type=MaskType.constant, value=None)


def _table(table_id: int, name: str, columns: list[str]) -> dict:
    return {
        "id": table_id,
        "source_id": "pg",
        "domain_id": "shop",
        "schema_name": "public",
        "table_name": name,
        "columns": [{"column_name": c, "visible_to": ["clerk"]} for c in columns],
        "write_ops": [],
        "write_returns_rows": False,
        "write_refused_forms": [],
    }


TABLES = [
    _table(1, "customers", ["id", "name", "email"]),
    _table(2, "orders", ["id", "customer_id"]),
]
RELATIONSHIPS = [
    {
        "id": 1,
        "source_table_id": 1,
        "target_table_id": 2,
        "source_column": "id",
        "target_column": "customer_id",
        "cardinality": "one-to-many",
        "alias": "orders",
    },
]


@pytest.fixture(scope="module")
def model():
    """ann and bob, a customer with no name, and one with no orders; email masked for the clerk."""
    types = {
        1: [
            ColumnMetadata(column_name="id", data_type="integer", is_nullable=False),
            ColumnMetadata(column_name="name", data_type="varchar", is_nullable=True),
            ColumnMetadata(column_name="email", data_type="varchar", is_nullable=True),
        ],
        2: [
            ColumnMetadata(column_name="id", data_type="integer", is_nullable=False),
            ColumnMetadata(column_name="customer_id", data_type="integer", is_nullable=True),
        ],
    }
    si = SchemaInput(
        tables=TABLES,
        relationships=RELATIONSHIPS,
        column_types=types,
        naming_rules=[],
        role=ROLE,
        domains=[{"id": "shop", "description": "Shop"}],
    )
    schema, ctx = generate_schema(si), build_context(si)
    masks = {(1, "clerk"): {"email": (MASKED, "varchar")}}
    gov = build_governance_context(
        "clerk", RLSContext(rules={}), masks, ctx, TABLES, ROLE, RELATIONSHIPS
    )
    con = duckdb.connect()
    con.execute("ATTACH ':memory:' AS pg")
    con.execute("CREATE SCHEMA pg.public")
    con.execute("CREATE TABLE pg.public.customers (id INTEGER, name VARCHAR, email VARCHAR)")
    con.execute("CREATE TABLE pg.public.orders (id INTEGER, customer_id INTEGER)")
    con.execute(
        "INSERT INTO pg.public.customers VALUES "
        "(1, 'ann', 'a@x'), (2, 'bob', 'b@x'), (3, NULL, 'c@x'), (4, 'cy', 'd@x')"
    )
    con.execute("INSERT INTO pg.public.orders VALUES (10, 1), (11, 2), (12, 3)")
    return schema, ctx, gov, con


def _compiled(model, where: str):
    schema, ctx = model[0], model[1]
    (compiled,) = compile_query(
        parse_query(schema, f"{{ customers(where: {where}) {{ id }} }}", ctx=ctx), ctx
    )
    return compiled


def _violations(model, where: str) -> list[str]:
    ctx, gov = model[1], model[2]
    return [v.code for v in validate_sql(_compiled(model, where).sql, ctx, gov, ROLE, TABLES)]


def _ids(model, where: str) -> list[int]:
    ctx, gov, con = model[1], model[2], model[3]
    compiled = _compiled(model, where)
    assert _violations(model, where) == []
    lowered = lower_to_catalog_physical(apply_governance(compiled.sql, gov), ctx)
    duck = sqlglot.transpile(lowered, read="postgres", write="duckdb")[0]
    rows = con.execute(duck, list(compiled.params)).fetchall()
    return sorted(r[0] for r in rows)


def test_every_where_input_offers_not(model):
    schema = model[0]
    where = schema.query_type.fields["customers"].args["where"].type
    assert isinstance(where, GraphQLInputObjectType)
    assert where.fields["_not"].type is where


def test_not_keeps_the_rows_its_condition_does_not_hold_for(model):
    assert _ids(model, '{_not: {name: {eq: "ann"}}}') == [2, 4]
    assert _ids(model, '{_not: {_or: [{name: {eq: "ann"}}, {name: {ilike: "%Y"}}]}}') == [2]
    assert _ids(model, '{id: {lt: 3}, _not: {name: {eq: "ann"}}}') == [2]
    assert _ids(model, "{_not: {_not: {id: {eq: 2}}}}") == [2]


def test_not_reads_a_null_as_sql_does(model):
    """A comparison with a null is neither true nor false: its negation keeps no row either."""
    assert 3 not in _ids(model, '{_not: {name: {eq: "ann"}}}')
    assert _ids(model, "{_not: {name: {is_null: false}}}") == [3]
    # A null text is excluded only when the query says what a null means.
    keep_null = '{_not: {_and: [{name: {is_null: false}}, {name: {ilike: "%n%"}}]}}'
    assert _ids(model, keep_null) == [2, 3, 4]


def test_not_holds_a_relationship_s_condition(model):
    assert _ids(model, "{_not: {orders: {id: {gt: 10}}}}") == [1, 4]


@pytest.mark.parametrize(
    "where",
    [
        '{_not: {email: {eq: "a@x"}}}',
        '{_not: {_or: [{id: {eq: 1}}, {email: {ilike: "%x"}}]}}',
        "{_and: [{id: {gt: 0}}, {_not: {email: {is_null: true}}}]}",
    ],
)
def test_a_masked_column_inside_not_is_refused(model, where):
    assert "V005" in _violations(model, where)
