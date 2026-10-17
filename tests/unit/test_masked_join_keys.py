# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A relationship joins on its real key when the key is masked for the role (REQ-603, REQ-040).

A nested relationship field is compiled to a subquery whose condition equates the two columns the
relationship registers. The engine reads those keys to join; the role reads a masked key, where it
selects one, as its mask. Anywhere else -- a filter of the role's own, a projection -- the masked
key is masked or refused as any masked column is. Run on DuckDB, lowered as every surface lowers a
governed statement.
"""

# Requirements: REQ-603, REQ-040, REQ-263

from __future__ import annotations

import json

import duckdb
import sqlglot

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

ROLE = {"id": "customer", "capabilities": ["query_development"], "domain_access": ["*"]}
ZERO = MaskingRule(mask_type=MaskType.constant, value=0)


def _table(table_id: int, name: str, columns: list[str]) -> dict:
    return {
        "id": table_id,
        "source_id": "pg",
        "domain_id": "shop",
        "schema_name": "public",
        "table_name": name,
        "columns": [{"column_name": c, "visible_to": []} for c in columns],
        "write_ops": [],
        "write_returns_rows": False,
        "write_refused_forms": [],
    }


TABLES = [_table(1, "customers", ["id", "name"]), _table(2, "orders", ["id", "customer_id"])]
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
    {
        "id": 2,
        "source_table_id": 2,
        "target_table_id": 1,
        "source_column": "customer_id",
        "target_column": "id",
        "cardinality": "many-to-one",
        "alias": "customer",
    },
]


def _model(masked: tuple[int, str]):
    """The model with one key masked to 0 for the customer: ``(table id, column)``."""
    types = {
        1: [
            ColumnMetadata(column_name="id", data_type="integer", is_nullable=False),
            ColumnMetadata(column_name="name", data_type="varchar", is_nullable=True),
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
    masks = {(masked[0], "customer"): {masked[1]: (ZERO, "integer")}}
    gov = build_governance_context(
        "customer", RLSContext(rules={}), masks, ctx, TABLES, ROLE, RELATIONSHIPS
    )
    con = duckdb.connect()
    con.execute("ATTACH ':memory:' AS pg")
    con.execute("CREATE SCHEMA pg.public")
    # PostgreSQL's JSON aggregate, as the statement is transpiled for DuckDB.
    con.execute("CREATE MACRO json_arrayagg(x) AS to_json(list(x))")
    con.execute("CREATE TABLE pg.public.customers (id INTEGER, name VARCHAR)")
    con.execute("CREATE TABLE pg.public.orders (id INTEGER, customer_id INTEGER)")
    con.execute("INSERT INTO pg.public.customers VALUES (1, 'ann'), (2, 'bob'), (3, 'cy')")
    con.execute("INSERT INTO pg.public.orders VALUES (10, 1), (11, 1), (12, 2)")
    return schema, ctx, gov, con


def _violations(model, sql: str) -> list[str]:
    _schema, ctx, gov, _con = model
    return [v.code for v in validate_sql(sql, ctx, gov, ROLE, TABLES)]


def _run(model, sql: str) -> list[tuple]:
    _schema, ctx, gov, con = model
    assert _violations(model, sql) == []
    lowered = lower_to_catalog_physical(apply_governance(sql, gov), ctx)
    return con.execute(sqlglot.transpile(lowered, read="postgres", write="duckdb")[0]).fetchall()


def _graphql(model, query: str) -> list[tuple]:
    schema, ctx, _gov, _con = model
    (compiled,) = compile_query(parse_query(schema, query, ctx=ctx), ctx)
    return _run(model, compiled.sql)


def _ids(value) -> list[int]:
    """The ``id`` of each object in a nested field's JSON value."""
    if value is None:
        return []
    parsed = json.loads(value) if isinstance(value, str) else value
    items = parsed if isinstance(parsed, list) else [parsed]
    return sorted(item["id"] for item in items)


def test_a_one_to_many_relationship_joins_on_a_masked_key_and_shows_the_mask():
    model = _model((1, "id"))  # customers.id masked
    rows = _graphql(model, "{ customers { name id orders { id } } }")
    assert sorted((name, key, _ids(orders)) for name, key, orders in rows) == [
        ("ann", 0, [10, 11]),
        ("bob", 0, [12]),
        ("cy", 0, []),
    ]


def test_a_many_to_one_relationship_joins_on_a_masked_key_and_shows_the_mask():
    model = _model((2, "customer_id"))  # orders.customer_id masked
    rows = _graphql(model, "{ orders { id customerId customer { id } } }")
    assert sorted((oid, key, _ids(customer)) for oid, key, customer in rows) == [
        (10, 0, [1]),
        (11, 0, [1]),
        (12, 0, [2]),
    ]


def test_a_masked_key_outside_the_join_condition_is_masked_or_refused():
    model = _model((2, "customer_id"))
    assert "V005" in _violations(model, 'SELECT "id" FROM "shop"."orders" WHERE "customer_id" = 1')
    # An expression over it reads the mask.
    rows = _run(model, 'SELECT "id", "customer_id" + 5 AS "k" FROM "shop"."orders"')
    assert sorted(rows) == [(10, 5), (11, 5), (12, 5)]
    assert sorted(_run(model, 'SELECT "id", "customer_id" FROM "shop"."orders"')) == [
        (10, 0),
        (11, 0),
        (12, 0),
    ]
