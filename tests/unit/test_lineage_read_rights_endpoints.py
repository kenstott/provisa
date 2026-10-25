# Copyright (c) 2026 Kenneth Stott
# Canary: 8e1a6c3d-5b27-4f90-9d14-c3b72e0a5f61
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1161, REQ-1625: every lineage read is capped by its caller's own read rights.

The federation graph's roles lens narrows within what the caller reads and never past it; a
governance user with no grant on a field never sees it, through any lens; the statement graph
answers the same way.
"""

from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace
from typing import Any

import pytest

from provisa.api import app as app_module
from provisa.api.admin import lineage_router
from provisa.api.admin.lineage_router import LineageGraphRequest, federation_graph, lineage_graph

# orders (1) → v_orders (2, B) → mart (3); secret (4) is read by the lens role alone.
VIEWS = [
    {"domain_id": "sales", "table_name": "v_orders", "view_sql": "SELECT amount FROM sales.orders"},
    {"domain_id": "sales", "table_name": "mart", "view_sql": "SELECT amount FROM sales.v_orders"},
    {"domain_id": "sales", "table_name": "leak", "view_sql": "SELECT x FROM sales.secret"},
]


def _column(table_id: int, table: str, column: str) -> dict:
    return {
        "table_id": table_id,
        "domain_id": "sales",
        "table_name": table,
        "column_name": column,
        "alias": None,
    }


COLUMNS = [
    _column(1, "orders", "amount"),
    _column(2, "v_orders", "amount"),
    _column(3, "mart", "amount"),
    _column(4, "secret", "x"),
    _column(5, "leak", "x"),
]


def _context(served: dict[int, list[str]]) -> SimpleNamespace:
    return SimpleNamespace(
        tables={tid: SimpleNamespace(table_id=tid) for tid in served},
        physical_to_sql={(tid, c): c for tid, cols in served.items() for c in cols},
    )


@pytest.fixture
def wired(monkeypatch):
    """The analyst reads orders and mart, not v_orders (B) or secret; the auditor reads all."""
    contexts = {
        "analyst": _context({1: ["amount"], 3: ["amount"]}),
        "auditor": _context({1: ["amount"], 2: ["amount"], 3: ["amount"], 4: ["x"], 5: ["x"]}),
    }

    async def view_rows(state):
        return VIEWS

    async def columns(state):
        return COLUMNS

    async def natives(state):
        return []

    monkeypatch.setattr(lineage_router, "_fetch_view_rows", view_rows)
    monkeypatch.setattr(lineage_router, "_fetch_registry_columns", columns)
    monkeypatch.setattr(lineage_router, "_native_tables", natives)
    monkeypatch.setattr(app_module.state, "contexts", contexts, raising=False)


def _as(role: str | None) -> Any:
    # No identity: the capability gates admit it (the no-auth exemption); the read rights still
    # come from the role the request acts as.
    return SimpleNamespace(state=SimpleNamespace(identity=None, role=role))


def _federation(role: str, lens: str | None) -> dict:
    return asyncio.run(federation_graph(_as(role), roles=lens))


@pytest.mark.parametrize("lens", [None, "analyst", "auditor", "analyst,auditor"])
def test_a_governance_user_never_sees_a_field_they_cannot_read_through_any_lens(
    wired, lens
) -> None:
    out = _federation("analyst", lens)
    text = json.dumps(out)
    assert "v_orders" not in text
    nodes = {n["id"] for n in out["nodes"]}
    assert {"sales.orders.amount", "sales.mart.amount"} <= nodes
    edge = next(e for e in out["edges"] if e["target"] == "sales.mart.amount")
    assert (edge["source"], edge["transform"], edge["ops"]) == ("sales.orders.amount", "", [])


def test_a_lens_broader_than_the_caller_is_capped(wired) -> None:
    out = _federation("analyst", "auditor")
    text = json.dumps(out)
    assert "secret" not in text and "leak" not in text


def test_the_lens_still_narrows_within_what_the_caller_reads(wired) -> None:
    whole = {n["id"] for n in _federation("auditor", None)["nodes"]}
    assert {"sales.v_orders.amount", "sales.secret.x"} <= whole
    narrowed = {n["id"] for n in _federation("auditor", "analyst")["nodes"]}
    assert "sales.secret.x" not in narrowed
    assert "sales.mart.amount" in narrowed


def test_a_caller_acting_as_no_role_reads_no_field(wired) -> None:
    out = asyncio.run(federation_graph(_as(None)))
    assert [n for n in out["nodes"] if n["relation"] and n["relation"].startswith("sales.")] == []


def test_focusing_a_hidden_field_answers_as_an_unknown_one(wired) -> None:
    from fastapi import HTTPException

    with pytest.raises(HTTPException) as hidden:
        asyncio.run(federation_graph(_as("analyst"), focus="sales.v_orders.amount"))
    with pytest.raises(HTTPException) as unknown:
        asyncio.run(federation_graph(_as("analyst"), focus="sales.nowhere.amount"))
    assert hidden.value.status_code == unknown.value.status_code == 404
    assert "v_orders" in str(hidden.value.detail) and "nowhere" in str(unknown.value.detail)


def test_the_statement_graph_is_capped_the_same_way(wired) -> None:
    out = asyncio.run(
        lineage_graph(_as("analyst"), LineageGraphRequest(sql="SELECT amount FROM sales.mart"))
    )
    assert "v_orders" not in json.dumps(out)
    assert {n["id"] for n in out["nodes"]} >= {"sales.orders.amount", "sales.mart.amount"}
