# Copyright (c) 2026 Kenneth Stott
# Canary: f1bc2f71-4ac7-466c-bcac-1b5baa7083d9
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1961: a sized numeric is an exact decimal in Parquet, through the compiler's columns, and a
Decimal with no declared size is written at the precision and scale its values carry, never float.
"""

# Requirements: REQ-1961

from __future__ import annotations

import io
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from graphql import parse

from provisa.compiler.sql_gen import compile_query
from provisa.compiler.sql_types import ColumnRef
from provisa.executor.formats.tabular import rows_to_parquet
from provisa.executor.redirect import _serialize_for_redirect
from provisa.executor.result import QueryResult

# 18 digits: float64 turns it into ...456.8
_EXACT = Decimal("1234567890123456.78")


def _ctx():
    from provisa.compiler import naming as _naming
    from provisa.compiler.context import build_context
    from provisa.compiler.introspect import ColumnMetadata
    from provisa.compiler.schema_gen import SchemaInput, generate_schema

    def col(name, data_type):
        return ColumnMetadata(column_name=name, data_type=data_type, is_nullable=True)

    _naming.configure(gql="snake")
    si = SchemaInput(
        tables=[
            {
                "id": 1,
                "source_id": "sales-pg",
                "domain_id": "sales",
                "schema_name": "public",
                "table_name": "accounts",
                "columns": [
                    {"column_name": c, "visible_to": ["admin"]}
                    for c in ("id", "balance", "ratio", "name")
                ],
                "write_ops": [],
                "write_returns_rows": True,
                "write_refused_forms": [],
            }
        ],
        relationships=[],
        column_types={
            1: [
                col("id", "integer"),
                col("balance", "numeric(18,2)"),
                col("ratio", "numeric"),
                col("name", "text"),
            ]
        },
        naming_rules=[],
        role={"id": "admin", "capabilities": [], "domain_access": ["*"]},
        domains=[{"id": "sales", "description": "Sales"}],
        source_types={"sales-pg": "postgresql"},
    )
    generate_schema(si)
    return build_context(si)


def _read(data: bytes) -> pa.Table:
    return pq.read_table(io.BytesIO(data))


def _redirected(rows, columns) -> pa.Table:
    names = [c.field_name for c in columns]
    body, content_type, ext = _serialize_for_redirect(QueryResult(rows, names), columns, "parquet")
    assert (content_type, ext) == ("application/vnd.apache.parquet", ".parquet")
    return _read(body)


class TestCompilerColumnsCarryTheRegisteredType:
    def test_a_root_query_column_carries_its_registered_type(self):
        (compiled,) = compile_query(parse("{ accounts { id balance ratio name } }"), _ctx(), None)
        assert {c.field_name: c.data_type for c in compiled.columns} == {
            "id": "integer",
            "balance": "numeric(18,2)",
            "ratio": "numeric",
            "name": "text",
        }

    def test_min_and_max_carry_the_column_type_and_sum_does_not(self):
        doc = parse(
            "{ accounts_aggregate { aggregate { count sum { balance } max { balance } "
            "min { balance } avg { balance } } } }"
        )
        (compiled,) = compile_query(doc, _ctx(), None)
        types = {(c.nested_in, c.field_name): c.data_type for c in compiled.columns}
        assert types[("aggregate.max", "balance")] == "numeric(18,2)"
        assert types[("aggregate.min", "balance")] == "numeric(18,2)"
        assert types[("aggregate.sum", "balance")] is None
        assert types[("aggregate.avg", "balance")] is None


class TestRedirectParquetThroughTheCompiler:
    def test_a_sized_numeric_is_its_declared_decimal(self):
        (compiled,) = compile_query(parse("{ accounts { id balance } }"), _ctx(), None)
        table = _redirected([(1, _EXACT)], compiled.columns)
        assert table.schema.field("balance").type == pa.decimal128(18, 2)
        assert table.column("balance").to_pylist() == [_EXACT]

    def test_an_unsized_numeric_is_the_decimal_its_values_carry(self):
        (compiled,) = compile_query(parse("{ accounts { ratio } }"), _ctx(), None)
        table = _redirected([(Decimal("1.5"),), (_EXACT,)], compiled.columns)
        assert table.schema.field("ratio").type == pa.decimal128(18, 2)
        assert table.column("ratio").to_pylist() == [Decimal("1.50"), _EXACT]

    def test_a_sum_is_the_decimal_its_values_carry(self):
        doc = parse("{ accounts_aggregate { aggregate { sum { balance } } } }")
        (compiled,) = compile_query(doc, _ctx(), None)
        big = Decimal("123456789012345678901.25")
        table = _redirected([(big,)], compiled.columns)
        (field,) = table.schema
        assert field.type == pa.decimal128(23, 2)
        assert table.column(0).to_pylist() == [big]


def _expression(name: str) -> ColumnRef:
    """A column the statement computes: no registered type."""
    return ColumnRef(alias=None, column=name, field_name=name, nested_in=None)


class TestExpressionColumnDecimal:
    def test_the_widest_precision_and_scale_among_its_values(self):
        values = [Decimal("1.5"), Decimal("-98765432109876543.123"), None, Decimal("7")]
        table = _read(rows_to_parquet([(v,) for v in values], [_expression("total")]))
        assert table.schema.field("total").type == pa.decimal128(20, 3)
        assert table.column("total").to_pylist() == values

    def test_a_value_beyond_decimal128_is_decimal256(self):
        big = Decimal("1" * 40 + ".5")
        table = _read(rows_to_parquet([(big,)], [_expression("total")]))
        assert table.schema.field("total").type == pa.decimal256(41, 1)
        assert table.column("total").to_pylist() == [big]

    def test_a_fraction_below_one(self):
        table = _read(rows_to_parquet([(Decimal("0.001"),)], [_expression("x")]))
        assert table.schema.field("x").type == pa.decimal128(3, 3)

    def test_a_non_finite_decimal_is_refused_by_name(self):
        with pytest.raises(ValueError, match="'total'"):
            rows_to_parquet([(Decimal("NaN"),)], [_expression("total")])

    def test_a_column_with_no_decimal_is_inferred_as_before(self):
        table = _read(rows_to_parquet([(1.5,), (2.0,)], [_expression("f")]))
        assert table.schema.field("f").type == pa.float64()
