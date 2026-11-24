# Copyright (c) 2026 Kenneth Stott
# Canary: 3f8d2b6e-91a4-4c57-b0e3-6a1d7c9e2f48
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""One pg enum typing columns of two tables gives the schema one filter type (REQ-221).

An enum's filter type is named after the enum, so every where input filtering a column of that
enum, on any table and on any root field (query, aggregate, group-by), takes the same type. A
filter type built per table put two types of one name in the schema, and graphql-core refused
the role's schema.
"""

# Requirements: REQ-221

from __future__ import annotations

from graphql import GraphQLInputObjectType, print_schema

from provisa.compiler.enum_detect import build_enum_types
from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.schema_gen import SchemaInput, generate_schema

_ENUMS = build_enum_types({"mood": ["happy", "sad"]})


def _table(table_id: int, name: str) -> dict:
    return {
        "id": table_id,
        "source_id": "pg",
        "domain_id": "sales",
        "schema_name": "public",
        "table_name": name,
        "enable_aggregates": True,
        "enable_group_by": True,
        "columns": [
            {"column_name": "id", "visible_to": ["admin"]},
            {"column_name": "mood", "visible_to": ["admin"]},
        ],
        "write_ops": [],
        "write_returns_rows": True,
        "write_refused_forms": [],
    }


def _columns() -> list[ColumnMetadata]:
    return [
        ColumnMetadata(column_name="id", data_type="integer", is_nullable=False),
        ColumnMetadata(column_name="mood", data_type="public.mood", is_nullable=True),
    ]


def _schema_input() -> SchemaInput:
    return SchemaInput(
        tables=[_table(1, "people"), _table(2, "pets")],
        relationships=[],
        column_types={1: _columns(), 2: _columns()},
        naming_rules=[],
        role={"id": "admin", "capabilities": [], "domain_access": ["*"]},
        domains=[{"id": "sales", "description": "Sales"}],
        enum_types=_ENUMS,
    )


def test_two_tables_typed_by_one_enum_share_its_filter_type():
    schema = generate_schema(_schema_input())

    filter_type = schema.get_type("mood_EnumFilter")
    assert isinstance(filter_type, GraphQLInputObjectType)
    wheres = [
        t
        for name, t in schema.type_map.items()
        if isinstance(t, GraphQLInputObjectType) and name.endswith("Where") and "mood" in t.fields
    ]
    assert {w.name for w in wheres} >= {"PeopleWhere", "PetsWhere"}, sorted(w.name for w in wheres)
    for where in wheres:
        assert where.fields["mood"].type is filter_type, where.name

    sdl = print_schema(schema)
    assert sdl.count("input mood_EnumFilter ") == 1
