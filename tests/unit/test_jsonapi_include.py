# Copyright (c) 2026 Kenneth Stott
# Canary: da4c2824-2461-4886-aecd-c9442f973a55
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-257 — JSON:API `include=` produces compound documents."""

from graphql import parse, validate

from provisa.api._query_helpers import NO_VARIABLES, build_graphql_query
from provisa.api.jsonapi.generator import (
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


def _document(table, columns, *, includes, include_keys, id_columns=("id",), rels=None):
    """The JSON:API document the Rust encoder writes for ``table`` (REQ-257, REQ-2194): the
    included resources moved out of the rows, deduplicated by their ids, and linked."""
    import json

    import pyarrow as pa

    from provisa.executor import json_encoders as je

    body, _ = je.jsonapi(
        [pa.record_batch(table)],
        columns,
        resource_type="orders",
        page_size=100,
        page_number=1,
        probe=True,
        total=None,
        id_columns=list(id_columns),
        scalar_names={},
        rel_names=[(r, r) for r in (includes if rels is None else rels)],
        rel_scalar_names={},
        includes=list(includes),
        include_keys=include_keys,
        base_path="/data/jsonapi/sales/orders",
        link_params=[],
    )
    return json.loads(body)


def _c(field, nested_in=None, cardinality=None):
    from provisa.compiler.sql_types import ColumnRef

    return ColumnRef(
        None, field if nested_in is None else f"{nested_in}_{field}", field, nested_in, cardinality
    )


_CUSTOMER = [_c("id"), _c("id", "customer", "many-to-one"), _c("name", "customer", "many-to-one")]


class TestIncluded:
    def test_pulls_nested_object_and_dedupes(self):
        doc = _document(
            {"id": [1, 2, 3], "customer_id": [7, 7, 8], "customer_name": ["Alice", "Alice", "Bob"]},
            _CUSTOMER,
            includes=["customer"],
            include_keys={"customer": ["id"]},
        )
        # deduped by id
        assert [(r["type"], r["id"]) for r in doc["included"]] == [
            ("customer", "7"),
            ("customer", "8"),
        ]
        assert doc["included"][0]["attributes"] == {"name": "Alice"}
        # the nested object is out of the primary resource, linked from it
        first = doc["data"][0]
        assert "customer" not in first["attributes"]
        assert first["relationships"]["customer"] == {"data": {"type": "customer", "id": "7"}}

    def test_one_to_many_list_flattened(self):
        doc = _document(
            {"id": [1, 1], "items_id": [10, 11]},
            [_c("id"), _c("id", "items", "one-to-many")],
            includes=["items"],
            include_keys={"items": ["id"]},
        )
        assert [r["id"] for r in doc["included"]] == ["10", "11"]
        assert doc["data"][0]["relationships"]["items"]["data"] == [
            {"type": "items", "id": "10"},
            {"type": "items", "id": "11"},
        ]

    def test_no_includes_leaves_the_nested_object(self):
        doc = _document(
            {"id": [1], "customer_id": [7], "customer_name": ["Alice"]},
            _CUSTOMER,
            includes=[],
            include_keys={},
            rels=[],
        )
        assert "included" not in doc
        assert doc["data"][0]["attributes"]["customer"] == {"id": 7, "name": "Alice"}

    def test_a_composite_key_identifies_each_included_resource(self):
        """An order's lines are keyed by (order_id, line): no line is lost to another's empty id."""
        doc = _document(
            {
                "id": [1, 1, 2],
                "order_items_order_id": [1, 1, 2],
                "order_items_line": [1, 2, 1],
            },
            [
                _c("id"),
                _c("order_id", "order_items", "one-to-many"),
                _c("line", "order_items", "one-to-many"),
            ],
            includes=["order_items"],
            include_keys={"order_items": ["order_id", "line"]},
        )
        assert [r["id"] for r in doc["included"]] == ["1:1", "1:2", "2:1"]
        assert [d["relationships"]["order_items"] for d in doc["data"]] == [
            {
                "data": [
                    {"type": "order_items", "id": "1:1"},
                    {"type": "order_items", "id": "1:2"},
                ]
            },
            {"data": [{"type": "order_items", "id": "2:1"}]},
        ]

    def test_a_to_one_include_links_one_identifier_or_none(self):
        doc = _document(
            {"id": [1, 2], "shipment_order_id": [1, None]},
            [_c("id"), _c("order_id", "shipment", "many-to-one")],
            includes=["shipment"],
            include_keys={"shipment": ["order_id"]},
        )
        assert [d["relationships"]["shipment"] for d in doc["data"]] == [
            {"data": {"type": "shipment", "id": "1"}},
            {"data": None},
        ]


class TestResourceId:
    def test_a_single_key_is_its_value(self):
        doc = _document(
            {"order_id": [7], "carrier": ["x"]},
            [_c("order_id"), _c("carrier")],
            includes=[],
            include_keys={},
            id_columns=["order_id"],
        )
        assert doc["data"][0]["id"] == "7"

    def test_a_composite_key_joins_its_values_escaping_the_separator(self):
        def rid(table, keys):
            columns = [_c(k) for k in table]
            return _document(table, columns, includes=[], include_keys={}, id_columns=keys)["data"][
                0
            ]["id"]

        assert rid({"order_id": [7], "line": [2]}, ["order_id", "line"]) == "7:2"
        assert rid({"a": ["x:y"], "b": ["z\\"]}, ["a", "b"]) == "x\\:y:z\\\\"

    def test_a_document_carries_ids_and_links_what_it_includes(self):
        doc = _document(
            {
                "id": [1],
                "status": ["shipped"],
                "order_items_order_id": [1],
                "order_items_line": [1],
                "order_items_quantity": [2],
            },
            [
                _c("id"),
                _c("status"),
                _c("order_id", "order_items", "one-to-many"),
                _c("line", "order_items", "one-to-many"),
                _c("quantity", "order_items", "one-to-many"),
            ],
            includes=["order_items"],
            include_keys={"order_items": ["order_id", "line"]},
        )
        assert doc["data"][0]["id"] == "1"
        assert doc["data"][0]["relationships"]["order_items"]["data"] == [
            {"type": "order_items", "id": "1:1"}
        ]
        assert [(r["type"], r["id"]) for r in doc["included"]] == [("order_items", "1:1")]
