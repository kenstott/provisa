# Copyright (c) 2026 Kenneth Stott
# Canary: da4c2824-2461-4886-aecd-c9442f973a55
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-257 — JSON:API `include=` produces compound documents."""

from graphql import parse, validate

from provisa.api._query_helpers import build_graphql_query
from provisa.api.jsonapi.generator import (
    _extract_included,
    _get_relationship_fields,
    _relationship_scalars,
)
from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.schema_gen import SchemaInput, generate_schema


def _col(name, data_type="varchar(100)", nullable=False):
    return ColumnMetadata(column_name=name, data_type=data_type, is_nullable=nullable)


def _schema():
    tables = [
        {
            "id": 1,
            "source_id": "sales-pg",
            "domain_id": "sales",
            "schema_name": "public",
            "table_name": "orders",
            "columns": [
                {"column_name": "id", "visible_to": ["admin"]},
                {"column_name": "customer_id", "visible_to": ["admin"]},
                {"column_name": "amount", "visible_to": ["admin"]},
            ],
        },
        {
            "id": 2,
            "source_id": "sales-pg",
            "domain_id": "sales",
            "schema_name": "public",
            "table_name": "customers",
            "columns": [
                {"column_name": "id", "visible_to": ["admin"]},
                {"column_name": "name", "visible_to": ["admin"]},
            ],
        },
    ]
    relationships = [
        {
            "id": "r1",
            "source_table_id": 1,
            "target_table_id": 2,
            "source_column": "customer_id",
            "target_column": "id",
            "cardinality": "many-to-one",
        },
    ]
    column_types = {
        1: [_col("id", "integer"), _col("customer_id", "integer"), _col("amount", "decimal(10,2)")],
        2: [_col("id", "integer"), _col("name", "varchar(100)")],
    }
    si = SchemaInput(
        tables=tables,
        relationships=relationships,
        column_types=column_types,
        naming_rules=[],
        role={"id": "admin", "capabilities": ["query_development"], "domain_access": ["*"]},
        domains=[{"id": "sales", "description": "Sales"}],
    )
    return generate_schema(si)


class TestRelationshipScalars:
    def test_returns_related_type_scalars(self):
        schema = _schema()
        scalars = _relationship_scalars(schema, "orders", "customer")
        assert "id" in scalars
        assert "name" in scalars
        # no nested object fields
        assert "orders" not in scalars

    def test_unknown_relationship_returns_empty(self):
        assert _relationship_scalars(_schema(), "orders", "nope") == []


class TestRelationshipFieldNames:
    def test_customer_is_a_relationship(self):
        rels = _get_relationship_fields(_schema(), "orders")
        assert "customer" in rels.values()


class TestIncludeQueryValidates:
    def test_include_selection_compiles_against_schema(self):
        schema = _schema()
        fields = ["id", "amount", "customer { id name }"]
        q = build_graphql_query("orders", fields, {}, [], 25, 0)
        doc = parse(q)
        assert not validate(schema, doc)  # the relationship sub-selection is valid


class TestExtractIncluded:
    def test_pulls_nested_object_and_dedupes(self):
        rows = [
            {"id": 1, "customer_id": 7, "customer": {"id": 7, "name": "Alice"}},
            {"id": 2, "customer_id": 7, "customer": {"id": 7, "name": "Alice"}},
            {"id": 3, "customer_id": 8, "customer": {"id": 8, "name": "Bob"}},
        ]
        included = _extract_included(rows, ["customer"])
        # deduped by id
        assert [c["id"] for c in included["customer"]] == [7, 8]
        # nested object removed from the primary rows (kept only the FK)
        assert "customer" not in rows[0]
        assert rows[0]["customer_id"] == 7

    def test_one_to_many_list_flattened(self):
        rows = [{"id": 1, "items": [{"id": 10}, {"id": 11}]}]
        included = _extract_included(rows, ["items"])
        assert [i["id"] for i in included["items"]] == [10, 11]

    def test_no_includes_returns_empty(self):
        rows = [{"id": 1, "customer": {"id": 7}}]
        assert _extract_included(rows, []) == {}
        assert rows[0]["customer"] == {"id": 7}  # untouched
