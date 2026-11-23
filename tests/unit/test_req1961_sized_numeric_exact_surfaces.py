# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1961 (amended 2026-11-23): a sized numeric(p,s) is the string of its exact digits on the
surfaces with no exact decimal type -- String in the GraphQL schema (and so in the OpenAPI and
JSON:API descriptions built from it), string in gRPC proto, a string on Bolt -- while an unsized
numeric stays Float / double. GraphQL filters and aggregates still treat a sized numeric column
as an ordered number: a comparison binds the decimal its digits are, never text."""

# Requirements: REQ-1961

from __future__ import annotations

from decimal import Decimal
from typing import Any

import pyarrow as pa
import pytest
import sqlglot
import sqlglot.expressions as exp
from graphql import GraphQLFloat, GraphQLNonNull, GraphQLString

from provisa.compiler.context import build_context
from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.parser import parse_query
from provisa.compiler.schema_gen import SchemaInput, generate_schema
from provisa.compiler.sql_gen import compile_query

_COLUMNS = [("id", "integer"), ("amount", "numeric(18,2)"), ("ratio", "numeric")]


def _model(columns=_COLUMNS) -> tuple[Any, Any]:
    si = SchemaInput(
        tables=[
            {
                "id": 1,
                "source_id": "pg1",
                "domain_id": "sales",
                "schema_name": "public",
                "table_name": "ledger",
                "columns": [{"column_name": c, "visible_to": ["admin"]} for c, _t in columns],
                "enable_aggregates": True,
                "write_ops": [],
                "write_returns_rows": False,
                "write_refused_forms": [],
            }
        ],
        relationships=[],
        column_types={
            1: [ColumnMetadata(column_name=c, data_type=t, is_nullable=True) for c, t in columns]
        },
        naming_rules=[],
        role={"id": "admin", "domain_access": ["*"], "capabilities": []},
        domains=[{"id": "sales", "description": "Sales"}],
    )
    return generate_schema(si), build_context(si)


def _unwrap(t):
    return t.of_type if isinstance(t, GraphQLNonNull) else t


def _row_type(schema):
    (name,) = [n for n in schema.type_map if n.lower().endswith("ledger") and "__" not in n[:2]]
    return schema.type_map[name]


def _root_field(schema, suffix: str = "ledger") -> str:
    (name,) = [n for n in schema.query_type.fields if n.lower().endswith(suffix)]
    return name


def _compiled(query: str) -> tuple[str, list]:
    schema, ctx = _model()
    (compiled,) = compile_query(parse_query(schema, query, {}, ctx=ctx), ctx, {})
    return compiled.sql, list(compiled.params)


# --- GraphQL ------------------------------------------------------------------------------------


def test_a_sized_numeric_is_declared_a_string_and_an_unsized_one_a_float():
    schema, _ = _model()
    fields = _row_type(schema).fields
    assert _unwrap(fields["amount"].type) is GraphQLString
    assert _unwrap(fields["ratio"].type) is GraphQLFloat


def test_a_sized_numeric_is_filtered_by_ordered_number_comparisons_of_its_digits():
    schema, _ = _model()
    where = schema.type_map[f"{_row_type(schema).name}Where"]
    amount_filter = _unwrap(where.fields["amount"].type)
    assert amount_filter.name == "DecimalFilter"
    assert {"eq", "neq", "in", "gt", "gte", "lt", "lte", "is_null"} <= set(amount_filter.fields)
    assert "like" not in amount_filter.fields
    assert _unwrap(amount_filter.fields["gt"].type) is GraphQLString
    assert _unwrap(where.fields["ratio"].type).name == "FloatFilter"


def test_a_gt_on_a_sized_numeric_binds_the_decimal_its_digits_are():
    root = _root_field(_model()[0])
    sql, params = _compiled(f'{{ {root}(where: {{amount: {{gt: "9.75"}}}}) {{ id amount }} }}')
    assert params == [Decimal("9.75")]
    assert all(isinstance(p, Decimal) for p in params)
    where = sqlglot.parse_one(sql, read="postgres").args["where"]
    assert isinstance(where, exp.Where)
    assert '"amount" > $1' in where.sql(dialect="postgres")


def test_a_gt_on_a_sized_numeric_compares_as_a_number_on_duckdb():
    """As text "10.25" sorts before "9.75"; as the numbers they are, it is greater."""
    import duckdb

    from provisa.compiler.params import bind_positionally

    root = _root_field(_model()[0])
    sql, params = _compiled(f'{{ {root}(where: {{amount: {{gt: "9.75"}}}}) {{ id }} }}')
    predicate = sqlglot.parse_one(sql, read="postgres").args["where"].this.sql(dialect="duckdb")
    statement, bound = bind_positionally(
        f"SELECT id FROM ledger WHERE {predicate} ORDER BY id", params, "?"
    )
    con = duckdb.connect()
    try:
        con.execute("CREATE TABLE ledger (id INTEGER, amount DECIMAL(18,2))")
        con.execute("INSERT INTO ledger VALUES (1, 9.50), (2, 10.25)")
        assert con.execute(statement, bound).fetchall() == [(2,)]
    finally:
        con.close()


def test_in_on_a_sized_numeric_binds_decimals():
    root = _root_field(_model()[0])
    _sql, params = _compiled(f'{{ {root}(where: {{amount: {{in: ["1.5", "12.50"]}}}}) {{ id }} }}')
    assert params == [Decimal("1.5"), Decimal("12.50")]


def test_digits_that_are_not_a_number_are_refused():
    root = _root_field(_model()[0])
    with pytest.raises(ValueError, match="not a number"):
        _compiled(f'{{ {root}(where: {{amount: {{gt: "ten"}}}}) {{ id }} }}')


def test_sum_and_avg_still_aggregate_a_sized_numeric_and_min_max_are_its_digits():
    schema, _ = _model()
    agg_fields = schema.type_map[f"{_row_type(schema).name}AggregateFields"].fields
    sum_fields = _unwrap(agg_fields["sum"].type).fields
    avg_fields = _unwrap(agg_fields["avg"].type).fields
    assert _unwrap(sum_fields["amount"].type) is GraphQLFloat
    assert _unwrap(avg_fields["amount"].type) is GraphQLFloat
    assert _unwrap(_unwrap(agg_fields["min"].type).fields["amount"].type) is GraphQLString
    assert _unwrap(_unwrap(agg_fields["max"].type).fields["ratio"].type) is GraphQLFloat
    root = _root_field(schema, "ledgeraggregate")
    sql, _ = _compiled(
        f"{{ {root} {{ aggregate {{ sum {{ amount }} avg {{ amount }} max {{ amount }} }} }} }}"
    )
    assert 'SUM("amount")' in sql and 'AVG("amount")' in sql


# --- OpenAPI and JSON:API ------------------------------------------------------------------------


def test_the_openapi_description_of_a_sized_numeric_is_string():
    from provisa.api.rest.openapi_spec import _gql_to_openapi_schema

    fields = _row_type(_model()[0]).fields
    assert _gql_to_openapi_schema(fields["amount"].type) == {"type": "string"}
    assert _gql_to_openapi_schema(fields["ratio"].type) == {"type": "number", "format": "double"}


def test_the_jsonapi_description_of_a_sized_numeric_is_string():
    from provisa.api.jsonapi.spec import _gql_to_schema

    fields = _row_type(_model()[0]).fields
    assert _gql_to_schema(fields["amount"].type) == {"type": "string"}
    assert _gql_to_schema(fields["ratio"].type) == {"type": "number"}


# --- gRPC ----------------------------------------------------------------------------------------


def test_the_proto_field_of_a_sized_numeric_is_string_and_an_unsized_one_double():
    from provisa.grpc.proto_gen import _physical_to_proto

    assert _physical_to_proto("numeric(18,2)") == "string"
    assert _physical_to_proto("decimal(10,2)") == "string"
    assert _physical_to_proto("numeric") == "double"


@pytest.mark.asyncio
async def test_grpc_carries_a_sized_numerics_exact_digits_in_its_string_field():
    from tests.unit.test_grpc_values_exact import _served_arrow

    digits = [Decimal("12.50"), Decimal("12345678901234567.89")]
    sent = await _served_arrow("payload", pa.array(digits, pa.decimal128(19, 2)))
    assert [m.payload for m in sent] == ["12.50", "12345678901234567.89"]


# --- Bolt ----------------------------------------------------------------------------------------


def test_bolt_sends_a_decimal_as_the_string_of_its_exact_digits():
    from provisa.bolt.packstream import pack, unpack

    assert unpack(pack(Decimal("12.50"))) == "12.50"
    assert unpack(pack(Decimal("12345678901234567.89"))) == "12345678901234567.89"
    assert unpack(pack(0.5)) == 0.5  # an unsized numeric arrives as a float and stays one


def test_a_having_on_the_max_of_a_sized_numeric_binds_a_decimal():
    from provisa.compiler.aggregates import _compile_having
    from provisa.compiler.params import ParamCollector

    collector = ParamCollector()
    sql = _compile_having(
        {"max": {"amount": {"gt": "9.75"}}, "sum": {"amount": {"gt": 20.5}}},
        collector,
        1,
        {},
        {"amount": "numeric(18,2)"},
    )
    assert sql == 'MAX("amount") > $1 AND SUM("amount") > $2'
    assert collector.params == [Decimal("9.75"), 20.5]
