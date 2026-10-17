# Copyright (c) 2026 Kenneth Stott
# Canary: 21258a22-778b-4e96-a480-b4ec7d29baec
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Governance reads a column by its published name as it reads it by its physical name, and a
relationship is offered to a role that sees both of its tables.

A column with an alias is written in semantic SQL under that alias (the compiler's
``physical_to_sql``); governance runs on that SQL before the alias is lowered to the physical
name. Its grants and masks are recorded by physical name, and are found under either name. A
relationship's join column is read by the
engine, not returned: the relationship is offered when the role sees both tables, and the
compiled join keeps its key though the role cannot select it.
"""

# Requirements: REQ-040, REQ-263, REQ-265, REQ-603

from __future__ import annotations

import sqlglot
import sqlglot.expressions as exp

from provisa.compiler.context import build_context
from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.parser import parse_query
from provisa.compiler.rls import RLSContext
from provisa.compiler.schema_gen import SchemaInput, generate_schema
from provisa.compiler.sql_gen import compile_query
from provisa.compiler.sql_validator import validate_sql
from provisa.compiler.stage2 import apply_governance, build_governance_context
from provisa.security.masking import MaskingRule, MaskType

ROLE = {"id": "customer", "capabilities": ["query_development"], "domain_access": ["*"]}


def _col(name: str, visible_to: list[str], **extra) -> dict:
    return {"column_name": name, "visible_to": visible_to, **extra}


def _table(table_id: int, name: str, columns: list[dict]) -> dict:
    return {
        "id": table_id,
        "source_id": "pg",
        "domain_id": "shop",
        "schema_name": "public",
        "table_name": name,
        "columns": columns,
        "write_ops": [],
        "write_returns_rows": False,
        "write_refused_forms": [],
    }


def _model(*, email_to: list[str]):
    """customers: first_name published as givenName, email as mail (granted ``email_to``), ssn
    as taxId (masked for the customer). orders: customer_id granted to admin only."""
    tables = [
        _table(
            1,
            "customers",
            [
                _col("id", []),
                _col("first_name", [], alias="givenName"),
                _col("email", email_to, alias="mail"),
                _col("ssn", [], alias="taxId"),
            ],
        ),
        _table(2, "orders", [_col("id", []), _col("customer_id", ["admin"])]),
    ]
    relationships = [
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
    types = {
        1: [
            ColumnMetadata(column_name="id", data_type="integer", is_nullable=False),
            ColumnMetadata(column_name="first_name", data_type="varchar", is_nullable=True),
            ColumnMetadata(column_name="email", data_type="varchar", is_nullable=True),
            ColumnMetadata(column_name="ssn", data_type="varchar", is_nullable=True),
        ],
        2: [
            ColumnMetadata(column_name="id", data_type="integer", is_nullable=False),
            ColumnMetadata(column_name="customer_id", data_type="integer", is_nullable=True),
        ],
    }
    si = SchemaInput(
        tables=tables,
        relationships=relationships,
        column_types=types,
        naming_rules=[],
        role=ROLE,
        domains=[{"id": "shop", "description": "Shop"}],
    )
    schema, ctx = generate_schema(si), build_context(si)
    masks = {
        (1, "customer"): {"ssn": (MaskingRule(mask_type=MaskType.constant, value="***"), "varchar")}
    }
    gov = build_governance_context(
        "customer", RLSContext(rules={}), masks, ctx, tables, ROLE, relationships
    )
    return schema, ctx, gov, tables


def _graphql(model, query: str) -> str:
    schema, ctx, _gov, _tables = model
    (compiled,) = compile_query(parse_query(schema, query, ctx=ctx), ctx)
    return compiled.sql


def _violations(model, sql: str) -> list[str]:
    _schema, ctx, gov, tables = model
    return [f"{v.code} {v.message}" for v in validate_sql(sql, ctx, gov, ROLE, tables)]


def _projection(governed: str) -> dict[str, str]:
    """Output name → the SQL of what the governed statement's answer holds for it."""
    tree = sqlglot.parse_one(governed, read="postgres")
    assert isinstance(tree, exp.Select)
    return {
        e.alias_or_name: (e.this if isinstance(e, exp.Alias) else e).sql() for e in tree.expressions
    }


# --- a published name is governed as the column it names ---------------------------------------


def test_a_visible_aliased_column_is_read_by_its_published_name():
    model = _model(email_to=["admin"])
    sql = _graphql(model, "{ customers { id givenName } }")
    assert _violations(model, sql) == []
    governed = apply_governance(sql, model[2])
    assert list(_projection(governed)) == ["id", "givenName"]


def test_a_hidden_aliased_column_is_refused_by_either_name():
    model = _model(email_to=["admin"])
    for name in ("mail", "email"):
        sql = f'SELECT "id", "{name}" FROM "shop"."customers"'
        assert any(v.startswith("V003") for v in _violations(model, sql)), name
        assert name not in _projection(apply_governance(sql, model[2]))


def test_a_masked_aliased_column_is_masked_when_every_column_is_visible():
    """With every column visible the table has no visibility filter; the mask is found by the
    column's physical name whichever name the statement reads it by."""
    model = _model(email_to=[])
    assert model[2].visible_columns[1] is None
    sql = _graphql(model, "{ customers { id taxId } }")
    assert _violations(model, sql) == []
    projection = _projection(apply_governance(sql, model[2]))
    assert projection["taxId"] == "'***'"
    # The physical name is masked as before.
    raw = _projection(apply_governance('SELECT "ssn" FROM "shop"."customers"', model[2]))
    assert raw["ssn"] == "'***'"


def test_a_masked_aliased_column_may_not_filter():
    model = _model(email_to=[])
    for name in ("taxId", "ssn"):
        sql = f'SELECT "id" FROM "shop"."customers" WHERE "{name}" = \'x\''
        assert any(v.startswith("V005") for v in _violations(model, sql)), name


def test_a_star_reads_the_published_names_masked_and_filtered():
    model = _model(email_to=["admin"])
    sql = 'SELECT * FROM "shop"."customers"'
    assert _violations(model, sql) == []
    projection = _projection(apply_governance(sql, model[2]))
    assert list(projection) == ["id", "givenName", "taxId"]  # mail is not the customer's
    assert projection["taxId"] == "'***'"


# --- a relationship is offered over both tables, not over its join column ----------------------


def test_a_relationship_is_offered_when_the_role_sees_both_tables():
    schema, _ctx, gov, _tables = _model(email_to=[])
    assert "customer_id" not in (gov.visible_columns[2] or ())
    assert schema.query_type is not None
    customers = schema.query_type.fields["customers"].type.of_type.of_type  # pyright: ignore[reportAttributeAccessIssue]
    orders = schema.query_type.fields["orders"].type.of_type.of_type  # pyright: ignore[reportAttributeAccessIssue]
    assert "orders" in customers.fields
    assert "customer" in orders.fields
    assert "customerId" not in orders.fields and "customer_id" not in orders.fields


def test_a_relationship_compiles_and_keeps_its_join_key_under_governance():
    model = _model(email_to=[])
    for query in ("{ customers { id orders { id } } }", "{ orders { id customer { id } } }"):
        sql = _graphql(model, query)
        assert "customer_id" in sql, sql
        assert _violations(model, sql) == [], query
        governed = apply_governance(sql, model[2])
        assert '"customer_id"' in governed, governed
        assert "NULL" not in governed.upper(), governed
        assert "customer_id" not in _projection(governed)


def test_the_join_column_itself_is_still_not_selectable():
    model = _model(email_to=[])
    sql = 'SELECT "o"."customer_id" FROM "shop"."orders" "o"'
    assert any(v.startswith("V003") for v in _violations(model, sql))
    # Outside an approved join condition, a reference to it reads nothing.
    governed = apply_governance(
        'SELECT "o"."id" FROM "shop"."orders" "o" WHERE "o"."customer_id" = 7', model[2]
    )
    assert "customer_id" not in governed
