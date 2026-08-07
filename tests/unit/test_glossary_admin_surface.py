# Copyright (c) 2026 Kenneth Stott
# Canary: 6a3e9d15-2b7c-4f8a-9c0e-4d5b1f8a2c66
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1387: the glossary admin router and the MCP term-lookup surface.

Router handlers are awaited directly with a fake request (the repo's admin-surface
pattern); the DB is a real SQLite tenant store seeded through the table repository so
the terms under test came from the real derivation path. Every mutation must queue a
metadata publish — the term graph exports — so the notify seam is asserted, not stubbed
silent.
"""

# Requirements: REQ-1387

from __future__ import annotations

import types
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, cast

import pytest
from sqlalchemy.ext.asyncio import create_async_engine

if TYPE_CHECKING:
    from fastapi import Request

from provisa.api.admin import glossary_router
from provisa.api.errors import ApiError
from provisa.core.database import Database
from provisa.core.models import Column, Table
from provisa.core.repositories import table as table_repo
from provisa.core.schema_org import (
    glossary_term_edges,
    glossary_term_experts,
    glossary_term_refs,
    glossary_terms,
    registered_tables,
    roles,
    table_columns,
)

pytestmark = pytest.mark.asyncio

_TABLES = [
    registered_tables,
    table_columns,
    roles,
    glossary_terms,
    glossary_term_refs,
    glossary_term_edges,
    glossary_term_experts,
]


def _request(org_id: str = "acme") -> "Request":
    fake = types.SimpleNamespace(
        state=types.SimpleNamespace(
            identity=types.SimpleNamespace(user_id="alice", roles=[]),
            active_org_id=org_id,
        )
    )
    return cast("Request", fake)


def _with_json(request: "Request", body: dict) -> "Request":
    async def _json():
        return body

    cast(Any, request).json = _json
    return request


@asynccontextmanager
async def _surface(tmp_path, monkeypatch):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'gl.db'}")
    async with engine.begin() as c:
        await c.run_sync(lambda s: registered_tables.metadata.create_all(s, tables=_TABLES))
    db = Database(engine, name="gl")
    notified: list[str] = []

    async def _pool():
        return db

    async def _notify(org_id: str, reason: str) -> None:
        notified.append(f"{org_id}:{reason}")

    monkeypatch.setattr(glossary_router, "_pool", _pool)
    monkeypatch.setattr(glossary_router, "_notify", _notify)
    # The org-settings gate resolves capabilities through app state; the repo's admin-surface
    # tests exercise the gate itself elsewhere — here it must simply not block the handler.
    monkeypatch.setattr(glossary_router, "require_org_settings", lambda request: None)
    monkeypatch.setattr(glossary_router, "_require_table_registration", lambda request: None)
    try:
        async with db.acquire() as conn:
            await table_repo.upsert(
                conn,
                Table(
                    source_id="__derived__",
                    domain_id="d",
                    schema_name="s",
                    table_name="orders",
                    columns=[
                        Column(name="cust_id", visible_to=[]),
                        Column(name="order_dt", visible_to=[]),
                    ],
                    view_sql="SELECT 1",
                ),
            )
        yield db, notified
    finally:
        await engine.dispose()


async def _term_id(name: str) -> int:
    terms = await glossary_router.list_terms(_request())
    return next(t["id"] for t in terms if t["name"] == name)


async def test_list_search_and_detail(tmp_path, monkeypatch):
    async with _surface(tmp_path, monkeypatch):
        terms = await glossary_router.list_terms(_request())
        assert {t["name"] for t in terms} == {"customer", "order date"}
        filtered = await glossary_router.list_terms(_request(), q="custom")
        assert [t["name"] for t in filtered] == ["customer"]
        detail = await glossary_router.get_term(_request(), await _term_id("customer"))
        assert detail["refs"][0]["column_name"] == "cust_id"


async def test_curation_mutations_notify_the_exporter(tmp_path, monkeypatch):
    async with _surface(tmp_path, monkeypatch) as (_db, notified):
        customer = await _term_id("customer")
        created = await glossary_router.create_abstract_term(
            _with_json(_request(), {"name": "party", "definition": "Any actor."})
        )
        await glossary_router.update_term(
            _with_json(_request(), {"definition": "The buyer."}), customer
        )
        await glossary_router.add_edge(
            _with_json(_request(), {"to_term_id": created["id"], "rel_type": "KIND_OF"}),
            customer,
        )
        await glossary_router.add_expert(
            _with_json(_request(), {"user_id": "bob", "kind": "author"}), customer
        )
        detail = await glossary_router.get_term(_request(), customer)
        assert detail["definition"] == "The buyer."
        assert detail["edges_out"] == [
            {"term_id": created["id"], "rel_type": "KIND_OF", "name": "party"}
        ]
        assert detail["experts"] == [{"user_id": "bob", "kind": "author"}]
        assert len(notified) == 4  # every mutation queued a publish


async def test_export_excluded_toggle_round_trips_and_notifies(tmp_path, monkeypatch):
    async with _surface(tmp_path, monkeypatch) as (_db, notified):
        customer = await _term_id("customer")
        await glossary_router.update_term(
            _with_json(_request(), {"export_excluded": True}), customer
        )
        detail = await glossary_router.get_term(_request(), customer)
        assert bool(detail["export_excluded"]) is True
        await glossary_router.update_term(
            _with_json(_request(), {"export_excluded": False}), customer
        )
        detail = await glossary_router.get_term(_request(), customer)
        assert bool(detail["export_excluded"]) is False
        assert len(notified) == 2


async def test_free_form_edge_type_is_refused(tmp_path, monkeypatch):
    async with _surface(tmp_path, monkeypatch) as (_db, notified):
        customer = await _term_id("customer")
        order_date = await _term_id("order date")
        with pytest.raises(ApiError) as err:
            await glossary_router.add_edge(
                _with_json(_request(), {"to_term_id": order_date, "rel_type": "VIBES_WITH"}),
                customer,
            )
        assert err.value.status_code == 400
        assert notified == []  # a refused mutation must not queue a publish


async def test_rooted_term_delete_is_refused(tmp_path, monkeypatch):
    async with _surface(tmp_path, monkeypatch) as (_db, notified):
        with pytest.raises(ApiError) as err:
            await glossary_router.delete_term(_request(), await _term_id("customer"))
        assert err.value.status_code == 400
        assert notified == []


async def test_move_ref_and_missing_term_404(tmp_path, monkeypatch):
    async with _surface(tmp_path, monkeypatch) as (db, _notified):
        customer = await _term_id("customer")
        async with db.acquire() as conn:
            row = (
                await conn.execute_core(
                    registered_tables.select().where(registered_tables.c.table_name == "orders")
                )
            ).fetchone()
        moved = await glossary_router.move_ref(
            _with_json(
                _request(),
                {"table_id": row.id, "column_name": "order_dt", "to_term_id": customer},
            )
        )
        assert moved == {"ok": True}
        terms = await glossary_router.list_terms(_request())
        assert {t["name"] for t in terms} == {"customer"}
        with pytest.raises(ApiError) as err:
            await glossary_router.get_term(_request(), 99999)
        assert err.value.status_code == 404


async def test_ref_lookup_serves_the_hover_summary(tmp_path, monkeypatch):
    async with _surface(tmp_path, monkeypatch) as (db, _notified):
        async with db.acquire() as conn:
            row = (
                await conn.execute_core(
                    registered_tables.select().where(registered_tables.c.table_name == "orders")
                )
            ).fetchone()
        term = await glossary_router.term_for_ref(_request(), row.id, "cust_id")
        assert term["name"] == "customer"
        assert term["refs"][0]["column_name"] == "cust_id"
        with pytest.raises(ApiError) as err:
            await glossary_router.term_for_ref(_request(), row.id, "no_such_column")
        assert err.value.status_code == 404


async def test_generate_definition_drafts_without_persisting(tmp_path, monkeypatch):
    from provisa.api.admin import schema_helpers

    prompts: list[str] = []

    async def _fake_llm(prompt: str, operation: str, max_tokens: int = 256) -> str:
        prompts.append(prompt)
        assert operation == "glossary_definition"
        return "A paying party."

    monkeypatch.setattr(schema_helpers, "_call_llm", _fake_llm)
    async with _surface(tmp_path, monkeypatch) as (_db, notified):
        customer = await _term_id("customer")
        result = await glossary_router.generate_definition(_request(), customer)
        assert result == {"definition": "A paying party."}
        # The prompt grounds the model in the term's physical binding.
        assert "customer" in prompts[0] and "orders.cust_id" in prompts[0]
        # Generation is a draft, not a mutation: nothing persisted, no publish queued.
        detail = await glossary_router.get_term(_request(), customer)
        assert detail["definition"] is None
        assert notified == []
        with pytest.raises(ApiError) as err:
            await glossary_router.generate_definition(_request(), 99999)
        assert err.value.status_code == 404


async def test_mcp_search_terms_requires_role_and_returns_refs(tmp_path, monkeypatch):
    from provisa.api.mcp import tools

    async with _surface(tmp_path, monkeypatch) as (db, _notified):
        state = types.SimpleNamespace(contexts={"analyst": object()}, tenant_db=db)
        hits = await tools.search_terms(state, "analyst", "customer")
        assert [t["name"] for t in hits] == ["customer"]
        assert hits[0]["refs"][0]["column_name"] == "cust_id"
        with pytest.raises(PermissionError):
            await tools.search_terms(state, "nobody", "customer")
        with pytest.raises(ValueError):
            await tools.search_terms(state, "analyst", "   ")
