# Copyright (c) 2026 Kenneth Stott
# Canary: 829d7e39-69f3-44c0-a7ff-5a07226f6008
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A GraphQL mutation on a table that does not take it is refused naming the table and the
operation — also when the schema has a Mutation type for other tables.

The schema offers a table only the mutations its source takes. With another table writable the
Mutation type exists, and validation alone would answer an unnamed "Cannot query field". The
refusal is the one every surface gives such a write: ``data.write_not_supported``.
"""

# Requirements: REQ-209, REQ-1925

from __future__ import annotations

import pytest

from provisa.compiler import naming as _naming
from provisa.compiler.context import build_context
from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.parser import GraphQLValidationError, parse_query
from provisa.compiler.schema_gen import SchemaInput, generate_schema
from provisa.compiler.write_admission import WriteNotSupported


def _col(name: str, data_type: str) -> ColumnMetadata:
    return ColumnMetadata(column_name=name, data_type=data_type, is_nullable=False)


def _table(table_id: int, name: str, write_ops: list[str]) -> dict:
    return {
        "id": table_id,
        "source_id": "sales-pg",
        "domain_id": "sales",
        "schema_name": "public",
        "table_name": name,
        "columns": [
            {"column_name": "id", "visible_to": ["admin"], "writable_by": ["admin"]},
            {"column_name": "region", "visible_to": ["admin"], "writable_by": ["admin"]},
        ],
        "write_ops": write_ops,
        "write_returns_rows": bool(write_ops),
        "write_refused_forms": [],
    }


@pytest.fixture
def schema_and_ctx():
    _naming.configure(gql="snake")
    si = SchemaInput(
        # orders takes every write; events takes inserts only; archive takes none.
        tables=[
            _table(1, "orders", ["delete", "insert", "update"]),
            _table(2, "events", ["insert"]),
            _table(3, "archive", []),
        ],
        relationships=[],
        column_types={
            tid: [_col("id", "integer"), _col("region", "varchar(20)")] for tid in (1, 2, 3)
        },
        naming_rules=[],
        role={"id": "admin", "capabilities": ["write"], "domain_access": ["*"]},
        domains=[{"id": "sales", "description": "Sales"}],
    )
    schema = generate_schema(si)
    assert schema.mutation_type is not None  # orders makes it exist
    return schema, build_context(si)


def _mutation_fields(schema) -> set[str]:
    return set(schema.mutation_type.fields)


def _refused(schema, ctx, gql: str) -> WriteNotSupported:
    with pytest.raises(WriteNotSupported) as refused:
        parse_query(schema, gql, ctx=ctx)
    return refused.value


def test_an_insert_on_a_table_that_takes_no_writes_names_the_table(schema_and_ctx):
    schema, ctx = schema_and_ctx
    insert = next(f for f in _mutation_fields(schema) if "insert" in f and "orders" in f)
    gql = 'mutation { %s(input: {id: 1, region: "east"}) { affected_rows } }' % insert.replace(
        "orders", "archive"
    )
    refused = _refused(schema, ctx, gql)
    assert (refused.table, refused.operation) == ("archive", "insert")


def test_an_update_on_a_table_that_takes_inserts_only_names_the_update(schema_and_ctx):
    schema, ctx = schema_and_ctx
    update = next(f for f in _mutation_fields(schema) if "update" in f and "orders" in f)
    gql = 'mutation { %s(where: {id: {_eq: 1}}, set: {region: "west"}) { affected_rows } }' % (
        update.replace("orders", "events")
    )
    refused = _refused(schema, ctx, gql)
    assert (refused.table, refused.operation) == ("events", "update")


def test_an_offered_mutation_and_an_unknown_field_are_validation_as_before(schema_and_ctx):
    schema, ctx = schema_and_ctx
    insert = next(f for f in _mutation_fields(schema) if "insert" in f and "orders" in f)
    parse_query(
        schema, 'mutation { %s(input: {id: 1, region: "e"}) { affected_rows } }' % insert, ctx=ctx
    )
    with pytest.raises(GraphQLValidationError):
        parse_query(
            schema, "mutation { insert_nothing_here(input: {}) { affected_rows } }", ctx=ctx
        )
