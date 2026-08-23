# Copyright (c) 2026 Kenneth Stott
# Canary: 2c7bb6da-561d-496d-8b71-177b6ababc2b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The 'deprecated' tag surfaces in the GraphQL SDL as @deprecated(reason:) (REQ-1375).

Column deprecation lands on the object field, table deprecation on the root query field
(the object type itself cannot carry the directive), and relationship deprecation on the
navigational field. The reason text carries the steward's stated reason and, when set,
the planned removal date.
"""

# Requirements: REQ-1375

from __future__ import annotations

from graphql import print_schema

from provisa.compiler import naming as _naming
from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.schema_gen import SchemaInput, generate_schema


def _col(name: str, data_type: str = "varchar(100)") -> ColumnMetadata:
    return ColumnMetadata(column_name=name, data_type=data_type, is_nullable=True)


def _schema(tables, relationships, column_types):
    _naming.configure(gql="snake")
    si = SchemaInput(
        tables=tables,
        relationships=relationships,
        column_types=column_types,
        naming_rules=[],
        role={"id": "admin", "capabilities": [], "domain_access": ["*"]},
        domains=[{"id": "sales", "description": "Sales"}],
    )
    return generate_schema(si)


def _tables(**orders_extra):
    return [
        {
            "id": 1,
            "source_id": "sales-pg",
            "domain_id": "sales",
            "schema_name": "public",
            "table_name": "orders",
            "columns": [
                {"column_name": "id", "visible_to": ["admin"]},
                {"column_name": "customer_id", "visible_to": ["admin"]},
                {
                    "column_name": "legacy_code",
                    "visible_to": ["admin"],
                    "deprecation_reason": "Use id (removal: 2026-12-01)",
                },
            ],
            **orders_extra,
        },
        {
            "id": 2,
            "source_id": "sales-pg",
            "domain_id": "sales",
            "schema_name": "public",
            "table_name": "customers",
            "columns": [{"column_name": "id", "visible_to": ["admin"]}],
        },
    ]


_COLUMN_TYPES = {
    1: [_col("id", "integer"), _col("customer_id", "integer"), _col("legacy_code")],
    2: [_col("id", "integer")],
}


def test_deprecated_column_emits_the_directive():
    schema = _schema(_tables(), [], _COLUMN_TYPES)
    sdl = print_schema(schema)
    assert '@deprecated(reason: "Use id (removal: 2026-12-01)")' in sdl
    field = schema.type_map["Orders"].fields["legacy_code"]
    assert field.deprecation_reason == "Use id (removal: 2026-12-01)"


def test_deprecated_table_emits_on_the_root_query_field():
    schema = _schema(_tables(deprecation_reason="Replaced by order_facts"), [], _COLUMN_TYPES)
    root = schema.query_type.fields["orders"]
    assert root.deprecation_reason == "Replaced by order_facts"
    # The undeprecated table stays clean.
    assert schema.query_type.fields["customers"].deprecation_reason is None


def test_deprecated_relationship_emits_on_the_navigational_field():
    rel = {
        "id": "rel-1",
        "source_table_id": 1,
        "target_table_id": 2,
        "source_column": "customer_id",
        "target_column": "id",
        "cardinality": "many-to-one",
        "deprecation_reason": "Traverse via order_facts",
    }
    schema = _schema(_tables(), [rel], _COLUMN_TYPES)
    orders_fields = schema.type_map["Orders"].fields
    nav = next(
        (
            f
            for name, f in orders_fields.items()
            if f.deprecation_reason is not None and name != "legacy_code"
        ),
        None,
    )
    assert nav is not None and nav.deprecation_reason == "Traverse via order_facts"
