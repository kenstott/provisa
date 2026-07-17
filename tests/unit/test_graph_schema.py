# Copyright (c) 2026 Kenneth Stott
# Canary: abb0b5c0-963a-4b74-bbff-c8b7a79c425b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-398/392 — graph-schema exposes pk_columns per node label.

The /data/graph-schema endpoint serializes each node's `pk_columns` straight from the
CypherLabelMap node mappings, which carry the user-designated primary key from the
compilation context. This covers that data flow.
"""

from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.schema_gen import SchemaInput, generate_schema
from provisa.compiler.context import build_context
from provisa.cypher.label_map import CypherLabelMap


def _col(name, data_type="varchar(100)", nullable=False):
    return ColumnMetadata(column_name=name, data_type=data_type, is_nullable=nullable)


def _ctx():
    tables = [
        {
            "id": 1,
            "source_id": "sales-pg",
            "domain_id": "sales",
            "schema_name": "public",
            "table_name": "orders",
            "governance": "pre-approved",
            "columns": [
                {"column_name": "id", "visible_to": ["admin"], "is_primary_key": True},
                {"column_name": "amount", "visible_to": ["admin"]},
            ],
        },
        {
            "id": 2,
            "source_id": "sales-pg",
            "domain_id": "sales",
            "schema_name": "public",
            "table_name": "customers",
            "governance": "pre-approved",
            "columns": [
                {"column_name": "id", "visible_to": ["admin"], "is_primary_key": True},
                {"column_name": "name", "visible_to": ["admin"]},
            ],
        },
    ]
    column_types = {
        1: [_col("id", "integer"), _col("amount", "decimal(10,2)")],
        2: [_col("id", "integer"), _col("name", "varchar(100)")],
    }
    si = SchemaInput(
        tables=tables,
        relationships=[],
        column_types=column_types,
        naming_rules=[],
        role={"id": "admin", "capabilities": ["query_development"], "domain_access": ["*"]},
        domains=[{"id": "sales", "description": "Sales"}],
    )
    generate_schema(si)
    return build_context(si)


def _nodes_by_table_label(lm: CypherLabelMap):
    return {n.table_label: n for n in lm.nodes.values()}


def test_pk_columns_populated_per_node():
    lm = CypherLabelMap.from_schema(_ctx())
    nodes = _nodes_by_table_label(lm)
    assert "id" in nodes["Orders"].pk_columns
    assert "id" in nodes["Customers"].pk_columns


def test_pk_singular_first_designated():
    lm = CypherLabelMap.from_schema(_ctx())
    node = _nodes_by_table_label(lm)["Orders"]
    # the endpoint emits pk = pk_columns[0]
    assert node.pk_columns[0] == "id"


def test_endpoint_serializes_pk_columns_shape():
    # mirror the /data/graph-schema per-node dict for pk fields
    lm = CypherLabelMap.from_schema(_ctx())
    node = _nodes_by_table_label(lm)["Customers"]
    serialized = {
        "pk": node.pk_columns[0] if node.pk_columns else None,
        "pk_columns": list(node.pk_columns),
    }
    assert serialized["pk"] == "id"
    assert serialized["pk_columns"] == ["id"]
