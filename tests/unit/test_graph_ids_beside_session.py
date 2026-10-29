# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A model node read while the principal's graph session holds ephemeral objects keeps its stable
id (REQ-394): only what a label or type the session overlays names is the session's, never
registered (REQ-2176).

Once a session held any ephemeral object, every read of it skipped the registration, so
``MATCH (n:Orders) RETURN n`` answered ``SalesAnalytics:Orders|1`` where it answered an integer
before, and ``id(n) IN [<that id>]`` could not be written (test_cypher_router_api's round trip).
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from types import SimpleNamespace
from typing import Any

from provisa.ephemeral.read import SESSION_CATALOG, overlaid
from provisa.gql.assembler import register_node_ids, register_rel_ids


class _Conn:
    def __init__(self, ids: dict[str, int]) -> None:
        self.ids = ids
        self.upserted: list[str] = []

    async def execute_core(self, _stmt: Any) -> Any:
        return SimpleNamespace(fetchone=lambda: None)

    async def upsert_returning(self, _table: Any, values: dict, **_kw: Any) -> int:
        self.upserted.append(values["composite_id"])
        return self.ids.setdefault(values["composite_id"], 100 + len(self.ids))


class _ModelDb:
    def __init__(self) -> None:
        self.conn = _Conn({})

    @asynccontextmanager
    async def acquire(self):
        yield self.conn


def _node(label: str, key: str) -> dict:
    return {"id": f"{label}|{key}", "label": label, "tableLabel": label, "properties": {"k": key}}


async def test_a_model_node_beside_the_session_is_registered():
    db = _ModelDb()
    rows = [{"n": _node("SalesAnalytics:Orders", "1"), "m": _node("Person", "e:1")}]
    await register_node_ids(rows, db, kept_labels=frozenset({"Person"}))
    assert db.conn.upserted == ["SalesAnalytics:Orders|1"]
    assert isinstance(rows[0]["n"]["id"], int)
    assert rows[0]["m"]["id"] == "Person|e:1"


async def test_with_no_session_every_node_is_registered():
    db = _ModelDb()
    rows = [{"n": _node("SalesAnalytics:Orders", "1")}]
    await register_node_ids(rows, db)
    assert isinstance(rows[0]["n"]["id"], int)


def _edge(rel_type: str, start: dict, end: dict) -> dict:
    return {
        "identity": f"{rel_type}:{start['id']}-{end['id']}",
        "type": rel_type,
        "start": start["id"],
        "end": end["id"],
        "properties": {},
        "startNode": start,
        "endNode": end,
    }


async def test_a_session_relationship_keeps_its_identity():
    db = _ModelDb()
    order, customer, note = (
        _node("SalesAnalytics:Orders", "1"),
        _node("SalesAnalytics:Customers", "7"),
        _node("Note", "e:2"),
    )
    rows = [
        {
            "model": _edge("PLACED_BY", order, customer),
            "typed": _edge("ABOUT", order, customer),
            "ended": _edge("PLACED_BY", order, note),
        }
    ]
    await register_rel_ids(
        rows, db, kept_types=frozenset({"ABOUT"}), kept_labels=frozenset({"Note"})
    )
    assert db.conn.upserted == ["PLACED_BY:SalesAnalytics:Orders|1-SalesAnalytics:Customers|7"]
    assert isinstance(rows[0]["model"]["identity"], int)
    assert isinstance(rows[0]["typed"]["identity"], str)
    assert isinstance(rows[0]["ended"]["identity"], str)


def test_overlaid_names_what_the_session_reads():
    def nm(label: str, catalog: str) -> Any:
        return SimpleNamespace(label=label, catalog_name=catalog)

    def rm(rel_type: str, via: Any) -> Any:
        return SimpleNamespace(rel_type=rel_type, via=via)

    label_map = SimpleNamespace(
        nodes={
            "SalesAnalytics:Orders": nm("SalesAnalytics:Orders", "sales_pg"),
            "Person": nm("Person", SESSION_CATALOG),
        },
        relationships={
            "placed_by": rm("PLACED_BY", None),
            "knows": rm("KNOWS", SimpleNamespace(catalog_name=SESSION_CATALOG)),
            "junction": rm("TAGGED", SimpleNamespace(catalog_name="sales_pg")),
        },
    )
    assert overlaid(label_map) == (frozenset({"Person"}), frozenset({"KNOWS"}))
