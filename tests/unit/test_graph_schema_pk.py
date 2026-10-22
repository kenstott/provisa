# Copyright (c) 2026 Kenneth Stott
# Canary: 986c7d3b-2d7c-4c05-a186-4b67d51c8a15
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-392: /data/graph-schema returns a singular `pk: string|null` per node label."""

from __future__ import annotations

import json
import types
from typing import Any, cast

import pytest

import provisa.api.app as appmod
import provisa.api.rest.cypher_router as cr

pytestmark = [pytest.mark.asyncio(loop_scope="session")]


def _node(label: str, pk_columns: list[str]):
    from provisa.cypher.label_map import NodeMapping

    # Each column read and written by its published name (the convention's: customerId).
    return NodeMapping(
        label=label,
        type_name=label,
        domain_label=None,
        table_label=label,
        table_id=0,
        source_id="pg",
        id_column="id",
        pk_columns=pk_columns,
        catalog_name="pg",
        schema_name="public",
        table_name=label.lower(),
        properties={"id": "id", "customerId": "customer_id"},
        physical_properties={"id": "id", "customerId": "customer_id"},
    )


async def test_graph_schema_includes_singular_pk(monkeypatch):
    label_map = types.SimpleNamespace(
        nodes={
            "Orders": _node("Orders", ["customer_id"]),
            "Events": _node("Events", []),  # no designated PK → pk is null
        },
        relationships={},
    )
    monkeypatch.setattr(cr, "_resolve_role_id", lambda *_: "admin")
    monkeypatch.setattr(cr, "_build_label_map", lambda *_: label_map)
    monkeypatch.setattr(appmod.state, "contexts", {"admin": object()})
    # A built surface has its root fields too (REQ-2128's graphql_field); none are named here.
    monkeypatch.setattr(appmod.state, "table_path_maps", {"admin": {}})
    monkeypatch.setattr(appmod.state, "schema_build_cache", {"tables": []})

    resp = await cr.graph_schema(cast(Any, types.SimpleNamespace()))
    body = json.loads(bytes(resp.body))
    by_label = {n["label"]: n for n in body["node_labels"]}

    assert "pk" in by_label["Orders"]
    # The property the column is read and written by.
    assert by_label["Orders"]["pk"] == "customerId"
    assert by_label["Events"]["pk"] is None
