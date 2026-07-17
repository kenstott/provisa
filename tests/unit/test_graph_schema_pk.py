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
    return types.SimpleNamespace(
        label=label,
        domain_label=None,
        domain_id=None,
        table_label=label,
        table_id=0,
        properties={"id": "id"},
        physical_properties={"id": "id"},
        pk_columns=pk_columns,
        id_column="id",
        native_filter_columns={},
        traversal_only=False,
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
    monkeypatch.setattr(appmod.state, "schema_build_cache", {"tables": []})

    resp = await cr.graph_schema(cast(Any, types.SimpleNamespace()))
    body = json.loads(bytes(resp.body))
    by_label = {n["label"]: n for n in body["node_labels"]}

    assert "pk" in by_label["Orders"]
    assert by_label["Orders"]["pk"] == cr._cql_prop("customer_id")
    assert by_label["Events"]["pk"] is None
