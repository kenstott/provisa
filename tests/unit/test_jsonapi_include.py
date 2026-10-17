# Copyright (c) 2026 Kenneth Stott
# Canary: da4c2824-2461-4886-aecd-c9442f973a55
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-257 — JSON:API `include=` produces compound documents."""

from graphql import parse, validate

from provisa.api._query_helpers import NO_VARIABLES, build_graphql_query
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
            "write_ops": ["delete", "insert", "update"],
            "write_returns_rows": True,
            "write_refused_forms": [],
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
            "write_ops": ["delete", "insert", "update"],
            "write_returns_rows": True,
            "write_refused_forms": [],
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
        q = build_graphql_query("orders", fields, NO_VARIABLES, [], 25, 0)
        doc = parse(q)
        assert not validate(schema, doc)  # the relationship sub-selection is valid


class TestExtractIncluded:
    def test_pulls_nested_object_and_dedupes(self):
        rows = [
            {"id": 1, "customer_id": 7, "customer": {"id": 7, "name": "Alice"}},
            {"id": 2, "customer_id": 7, "customer": {"id": 7, "name": "Alice"}},
            {"id": 3, "customer_id": 8, "customer": {"id": 8, "name": "Bob"}},
        ]
        included, _ = _extract_included(rows, ["customer"], {"customer": ["id"]})
        # deduped by id
        assert [c["id"] for c in included["customer"]] == [7, 8]
        # nested object removed from the primary rows (kept only the FK)
        assert "customer" not in rows[0]
        assert rows[0]["customer_id"] == 7

    def test_one_to_many_list_flattened(self):
        rows = [{"id": 1, "items": [{"id": 10}, {"id": 11}]}]
        included, _ = _extract_included(rows, ["items"], {"items": ["id"]})
        assert [i["id"] for i in included["items"]] == [10, 11]

    def test_no_includes_returns_empty(self):
        rows = [{"id": 1, "customer": {"id": 7}}]
        assert _extract_included(rows, [], {}) == ({}, [{}])
        assert rows[0]["customer"] == {"id": 7}  # untouched

    def test_a_composite_key_identifies_each_included_resource(self):
        """An order's lines are keyed by (order_id, line): no line is lost to another's empty id."""
        rows = [
            {"id": 1, "order_items": [{"order_id": 1, "line": 1}, {"order_id": 1, "line": 2}]},
            {"id": 2, "order_items": [{"order_id": 2, "line": 1}]},
        ]
        included, links = _extract_included(
            rows, ["order_items"], {"order_items": ["order_id", "line"]}
        )
        assert [(i["order_id"], i["line"]) for i in included["order_items"]] == [
            (1, 1),
            (1, 2),
            (2, 1),
        ]
        assert links == [
            {
                "order_items": {
                    "data": [
                        {"type": "order_items", "id": "1:1"},
                        {"type": "order_items", "id": "1:2"},
                    ]
                }
            },
            {"order_items": {"data": [{"type": "order_items", "id": "2:1"}]}},
        ]

    def test_a_to_one_include_links_one_identifier_or_none(self):
        rows = [{"id": 1, "shipment": {"order_id": 1}}, {"id": 2, "shipment": None}]
        _, links = _extract_included(rows, ["shipment"], {"shipment": ["order_id"]})
        assert links == [
            {"shipment": {"data": {"type": "shipment", "id": "1"}}},
            {"shipment": {"data": None}},
        ]


class TestResourceId:
    def test_a_single_key_is_its_value(self):
        from provisa.api.jsonapi.serializer import resource_id

        assert resource_id({"order_id": 7, "carrier": "x"}, ["order_id"]) == "7"

    def test_a_composite_key_joins_its_values_escaping_the_separator(self):
        from provisa.api.jsonapi.serializer import resource_id

        assert resource_id({"order_id": 7, "line": 2}, ["order_id", "line"]) == "7:2"
        assert resource_id({"a": "x:y", "b": "z\\"}, ["a", "b"]) == "x\\:y:z\\\\"

    def test_a_document_carries_ids_and_links_what_it_includes(self):
        from provisa.api.jsonapi.serializer import rows_to_jsonapi

        doc = rows_to_jsonapi(
            [{"id": 1, "status": "shipped"}],
            "orders",
            id_columns=["id"],
            included_rows={"order_items": [{"order_id": 1, "line": 1, "quantity": 2}]},
            included_id_columns={"order_items": ["order_id", "line"]},
            row_links=[{"order_items": {"data": [{"type": "order_items", "id": "1:1"}]}}],
        )
        assert doc["data"][0]["id"] == "1"
        assert doc["data"][0]["relationships"]["order_items"]["data"] == [
            {"type": "order_items", "id": "1:1"}
        ]
        assert [(r["type"], r["id"]) for r in doc["included"]] == [("order_items", "1:1")]
