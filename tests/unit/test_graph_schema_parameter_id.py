# Copyright (c) 2026 Kenneth Stott
# Canary: 5e8a2c47-1f3b-4d96-b0a7-9c6e2d18f4b3
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A join onto a table's opaque parameter binds it; it is no key of the table's node (REQ-1986).

The pet-store shape: ``pet_by_status.id`` joins many-to-one onto ``pet_by_id._nf_petId``, the path
parameter of get_pet_by_id. A many-to-one join's target column is its target's key, except where
that column is a parameter: an argument passed to the source, never an attribute of the rows
returned (REQ-1986), so never a node's identity. The node's id is a column of its rows, and
/data/graph-schema names every node's id, pk and pk_columns by the properties it carries.
"""

# Requirements: REQ-1986, REQ-392, REQ-394

from __future__ import annotations

import json
import types
from typing import Any, cast

import pytest

import provisa.api.app as appmod
import provisa.api.rest.cypher_router as cr
from provisa.compiler.context import build_context
from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.schema_gen import SchemaInput, generate_schema
from provisa.gql.label_map import CypherLabelMap

ROLE = {"id": "admin", "capabilities": ["query_development"], "domain_access": ["*"]}


def _columns(*names: str) -> list[dict]:
    return [{"column_name": n, "visible_to": ["admin"]} for n in names]


def _table(table_id: int, name: str, columns: list[dict]) -> dict:
    return {
        "id": table_id,
        "source_id": "petstore",
        "domain_id": "pet-store",
        "schema_name": "public",
        "table_name": name,
        "columns": columns,
        "write_ops": [],
        "write_returns_rows": False,
        "write_refused_forms": [],
    }


def _meta(*names: tuple[str, str]) -> list[ColumnMetadata]:
    return [ColumnMetadata(column_name=n, data_type=t, is_nullable=True) for n, t in names]


TABLES = [
    _table(1, "pet_by_status", _columns("id", "name", "status")),
    _table(
        2,
        "pet_by_id",
        [
            {
                "column_name": "_nf_petId",
                "visible_to": ["admin"],
                "data_type": "integer",
                "native_filter_type": "path_param",
                "native_filter_required": True,
            },
            *_columns("id", "name", "status"),
        ],
    ),
]

#: config/provisa.yaml's find-pets-by-status-to-get-pet-by-id: each listed pet's id fills the
#: path parameter of get_pet_by_id.
RELATIONSHIPS = [
    {
        "id": 1,
        "source_table_id": 1,
        "target_table_id": 2,
        "source_column": "id",
        "target_column": "_nf_petId",
        "cardinality": "many-to-one",
        "alias": "DETAIL",
    },
]


@pytest.fixture
def graph() -> CypherLabelMap:
    # An opaque parameter has no column of the source's result: no column metadata of its own.
    real = _meta(("id", "integer"), ("name", "varchar"), ("status", "varchar"))
    si = SchemaInput(
        tables=TABLES,
        relationships=RELATIONSHIPS,
        column_types={1: real, 2: real},
        naming_rules=[],
        role=ROLE,
        domains=[{"id": "pet-store", "description": "Pet store"}],
    )
    generate_schema(si)
    ctx = build_context(si)
    return CypherLabelMap.from_schema(ctx, domain_access=["*"], all_tables=TABLES)


def _pet_by_id(graph: CypherLabelMap):
    (node,) = [n for n in graph.nodes.values() if n.table_name.endswith("pet_by_id")]
    return node


def test_a_parameter_a_join_binds_is_no_node_id(graph):
    node = _pet_by_id(graph)
    assert node.parameterized
    assert node.id_column == "id"
    assert node.property_of(node.id_column) == "id"


def test_every_nodes_identity_is_a_property_it_carries(graph):
    for node in graph.nodes.values():
        node.property_of(node.id_column)
        for column in node.pk_columns:
            node.property_of(column)


@pytest.mark.asyncio(loop_scope="session")
async def test_graph_schema_answers_for_a_parameterized_table(graph, monkeypatch):
    monkeypatch.setattr(cr, "_resolve_role_id", lambda *_: "admin")
    monkeypatch.setattr(cr, "_build_label_map", lambda *_: graph)
    monkeypatch.setattr(appmod.state, "contexts", {"admin": object()})
    monkeypatch.setattr(appmod.state, "table_path_maps", {"admin": {}})
    monkeypatch.setattr(appmod.state, "schema_build_cache", {"tables": []})

    resp = await cr.graph_schema(cast(Any, types.SimpleNamespace()))

    assert resp.status_code == 200
    body = json.loads(bytes(resp.body))
    (pet,) = [n for n in body["node_labels"] if n["table_id"] == 2]
    assert pet["id_column"] == "id"
    assert "id" in pet["properties"]
    assert [p["name"] for p in pet["parameters"]] == ["petId"]
