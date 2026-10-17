# Copyright (c) 2026 Kenneth Stott
# Canary: 5e4fb1c1-5c6b-438e-a146-4843382e6247
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A subscription field takes its table's query arguments (REQ-258): every frame is the
equivalent query's answer, so ``where``, ``order_by`` and ``limit`` shape each frame alike."""

# Requirements: REQ-258

from __future__ import annotations

from graphql import parse, validate

from provisa.compiler import naming as _naming
from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.schema_gen import SchemaInput, generate_schema


def _schema():
    _naming.configure(gql="snake")
    columns = {
        1: [
            ColumnMetadata(column_name="order_id", data_type="varchar", is_nullable=False),
            ColumnMetadata(column_name="ordered_at", data_type="timestamp", is_nullable=True),
        ]
    }
    si = SchemaInput(
        tables=[
            {
                "id": 1,
                "source_id": "orders",
                "domain_id": "shop",
                "schema_name": "shop",
                "table_name": "orders",
                "columns": [
                    {"column_name": "order_id", "visible_to": ["admin"]},
                    {"column_name": "ordered_at", "visible_to": ["admin"]},
                ],
                "write_ops": [],
                "write_returns_rows": False,
                "write_refused_forms": [],
            }
        ],
        relationships=[],
        column_types=columns,
        naming_rules=[],
        role={"id": "admin", "capabilities": [], "domain_access": ["*"]},
        domains=[{"id": "shop", "description": "Shop"}],
        source_types={"orders": "kafka"},
    )
    return generate_schema(si)


def test_a_subscription_field_takes_the_query_fields_arguments():
    schema = _schema()
    [name] = schema.subscription_type.fields
    assert set(schema.subscription_type.fields[name].args) == set(
        schema.query_type.fields[name].args
    )
    assert {"where", "order_by", "limit"} <= set(schema.subscription_type.fields[name].args)


def test_the_latest_ten_orders_is_a_valid_subscription():
    schema = _schema()
    [name] = schema.subscription_type.fields
    document = parse(
        f"subscription {{ {name}(limit: 10, order_by: [{{ ordered_at: desc }}]) {{ order_id }} }}"
    )
    assert validate(schema, document) == []
