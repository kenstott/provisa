# Copyright (c) 2026 Kenneth Stott
# Canary: 8f2a6c1d-9b3e-4a7f-8d5c-2e6f9a1b3c7d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for the "one-to-one" relationship cardinality.

Covers: the Relationship/Cardinality model accepting "one-to-one" without a validation
error, schema_gen emitting a singular (non-list) SDL field for it (same shape as
many-to-one), and the sql_selection resolver expression taking the head of the matched
rows (LIMIT 1, single json_object) rather than a list — mirroring the many-to-one
resolver shape exactly, per the demo scenario (orders.order_id <-> order_docs.order_id,
both primary keys).
"""

# Requirements: REQ-019

from __future__ import annotations

import pytest
from graphql import GraphQLField, GraphQLList, GraphQLObjectType
from pydantic import ValidationError

from provisa.compiler import naming as _naming
from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.schema_gen import SchemaInput, generate_schema
from provisa.compiler.sql_selection import _build_rel_json_expr
from provisa.compiler.sql_types import CompilationContext, TableMeta
from provisa.core.models import Cardinality, Relationship
from graphql import FieldNode, parse


# ---------------------------------------------------------------------------
# 1. config_loader / core model: "one-to-one" is a valid cardinality
# ---------------------------------------------------------------------------


def test_cardinality_enum_has_one_to_one():
    assert Cardinality.one_to_one == "one-to-one"


def test_relationship_model_accepts_one_to_one():
    rel = Relationship(
        id="orders-docs",
        source_table_id="orders",
        target_table_id="order_docs",
        source_column="order_id",
        target_column="order_id",
        cardinality=Cardinality.one_to_one,
    )
    assert rel.cardinality == "one-to-one"


def test_relationship_model_accepts_one_to_one_string_literal():
    """Pydantic must accept the raw YAML string, not just the enum member."""
    rel = Relationship(
        id="orders-docs",
        source_table_id="orders",
        target_table_id="order_docs",
        source_column="order_id",
        target_column="order_id",
        cardinality="one-to-one",
    )
    assert rel.cardinality == Cardinality.one_to_one


def test_relationship_model_rejects_unknown_cardinality():
    with pytest.raises(ValidationError):
        Relationship(
            id="orders-docs",
            source_table_id="orders",
            target_table_id="order_docs",
            source_column="order_id",
            target_column="order_id",
            cardinality="one-to-one-hundred",
        )


# ---------------------------------------------------------------------------
# 2. schema_gen: one-to-one produces a singular object field, not a list
# ---------------------------------------------------------------------------


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


_TABLES = [
    {
        "id": 1,
        "source_id": "sales-pg",
        "domain_id": "sales",
        "schema_name": "public",
        "table_name": "orders",
        "columns": [
            {"column_name": "order_id", "visible_to": ["admin"]},
        ],
        "write_ops": ["delete", "insert", "update"],
    },
    {
        "id": 2,
        "source_id": "docs-mongo",
        "domain_id": "sales",
        "schema_name": "public",
        "table_name": "order_docs",
        "columns": [
            {"column_name": "order_id", "visible_to": ["admin"]},
        ],
        "write_ops": ["delete", "insert", "update"],
    },
]

_COLUMN_TYPES = {
    1: [_col("order_id", "integer")],
    2: [_col("order_id", "integer")],
}

_ONE_TO_ONE_REL = {
    "id": "orders-docs",
    "source_table_id": 1,
    "target_table_id": 2,
    "source_column": "order_id",
    "target_column": "order_id",
    "cardinality": "one-to-one",
}


def _unwrap_field(field: GraphQLField):
    return field.type.of_type if hasattr(field.type, "of_type") else field.type


def test_one_to_one_emits_singular_object_field_not_list():
    schema = _schema(_TABLES, [_ONE_TO_ONE_REL], _COLUMN_TYPES)
    orders_fields = schema.type_map["Orders"].fields
    nav_name, nav_field = next(
        ((n, f) for n, f in orders_fields.items() if n == "order_doc"),
        (None, None),
    )
    assert nav_field is not None, f"no relationship field found among {list(orders_fields)}"
    # Singular: the field type is the target object type directly (possibly wrapped in
    # NonNull), never a GraphQLList — same shape many-to-one produces.
    field_type = nav_field.type
    while hasattr(field_type, "of_type"):
        assert not isinstance(field_type, GraphQLList), (
            f"one-to-one field {nav_name!r} must not be a list"
        )
        field_type = field_type.of_type
    assert isinstance(field_type, GraphQLObjectType)
    assert field_type.name == "Order_docs"


def test_one_to_one_matches_many_to_one_sdl_shape():
    """Same relationship, only cardinality differs, must produce identical field typing shape."""
    m2o_rel = dict(_ONE_TO_ONE_REL, cardinality="many-to-one")
    schema_o2o = _schema(_TABLES, [_ONE_TO_ONE_REL], _COLUMN_TYPES)
    schema_m2o = _schema(_TABLES, [m2o_rel], _COLUMN_TYPES)

    def _nav_field(schema):
        fields = schema.type_map["Orders"].fields
        return fields["order_doc"]

    assert str(_nav_field(schema_o2o).type) == str(_nav_field(schema_m2o).type)


def test_unhandled_cardinality_raises_not_silently_dropped():
    bad_rel = dict(_ONE_TO_ONE_REL, cardinality="bogus-cardinality")
    # graphql-core lazily resolves the field thunk and wraps whatever it raises in a
    # TypeError ("<Type> fields cannot be resolved. <original message>") — the underlying
    # ValueError message still carries through.
    with pytest.raises(TypeError, match="unhandled relationship cardinality"):
        _schema(_TABLES, [bad_rel], _COLUMN_TYPES)


# ---------------------------------------------------------------------------
# 3. sql_selection resolver: one-to-one takes the head of the matched rows
# ---------------------------------------------------------------------------


def _field(query: str) -> FieldNode:
    doc = parse(query)
    return doc.definitions[0].selection_set.selections[0]


def _table(table_id: int, table_name: str, field_name: str = "widgets") -> TableMeta:
    return TableMeta(
        table_id=table_id,
        field_name=field_name,
        type_name="Widget",
        source_id="src",
        catalog_name="src",
        schema_name="public",
        table_name=table_name,
    )


def test_build_rel_json_expr_one_to_one_matches_many_to_one_shape():
    parent = _table(2, "order_docs")
    ctx = CompilationContext(physical_to_sql={(2, "name"): "name"})
    fn = _field("{ x { name } }")

    expr_o2o, counter_o2o = _build_rel_json_expr(
        fn.selection_set.selections,
        ctx,
        "Widget",
        parent,
        "t1",
        "public.order_docs t1",
        '"t1"."order_id" = "t0"."order_id"',
        "one-to-one",
        None,
        False,
        1,
        set(),
    )
    expr_m2o, _ = _build_rel_json_expr(
        fn.selection_set.selections,
        ctx,
        "Widget",
        parent,
        "t1",
        "public.order_docs t1",
        '"t1"."order_id" = "t0"."order_id"',
        "many-to-one",
        None,
        False,
        1,
        set(),
    )

    # Singular json_object (not json_agg), head-of-matched-rows via LIMIT 1 — identical
    # to the many-to-one resolver shape.
    assert expr_o2o.startswith("(SELECT json_object(")
    assert "LIMIT 1" in expr_o2o
    assert "json_agg" not in expr_o2o
    assert expr_o2o == expr_m2o
    assert counter_o2o == 1


def test_build_rel_json_expr_unhandled_cardinality_raises():
    parent = _table(2, "order_docs")
    ctx = CompilationContext(physical_to_sql={(2, "name"): "name"})
    fn = _field("{ x { name } }")
    with pytest.raises(ValueError, match="unhandled relationship cardinality"):
        _build_rel_json_expr(
            fn.selection_set.selections,
            ctx,
            "Widget",
            parent,
            "t1",
            "public.order_docs t1",
            "TRUE",
            "bogus-cardinality",
            None,
            False,
            1,
            set(),
        )
