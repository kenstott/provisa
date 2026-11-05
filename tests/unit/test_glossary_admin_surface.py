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
from sqlalchemy import insert

if TYPE_CHECKING:
    from fastapi import Request

from provisa.api.admin import glossary_router
from provisa.api.errors import ApiError
from provisa.core.models import Column, Table
from provisa.core.repositories import table as table_repo
from provisa.core.schema_org import (
    domains,
    sources,
    registered_tables,
)
from tests.role_state import RoleState
from tests.unit.gate_identity import serve_every_column

pytestmark = pytest.mark.asyncio


@pytest.fixture(autouse=True)
def _served(monkeypatch):
    """Every column is served here: which dependents a caller may be told of is REQ-2152's own
    tests (test_refusal_read_rights)."""
    serve_every_column(monkeypatch)


def _request(org_id: str = "acme") -> "Request":
    fake = types.SimpleNamespace(
        state=types.SimpleNamespace(
            identity=types.SimpleNamespace(user_id="alice", roles=[], acting_roles=[]),
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
async def _surface(control_plane, monkeypatch):
    with control_plane.engine().begin() as c:
        c.exec_driver_sql('SET LOCAL search_path TO "org_default"')
        c.execute(insert(domains).values(id="d", description=""))
        c.execute(insert(sources).values(id="__derived__", type="derived"))
    db = control_plane.org()
    notified: list[str] = []

    async def _pool():
        return db

    async def _notify(org_id: str, reason: str) -> None:
        notified.append(f"{org_id}:{reason}")

    monkeypatch.setattr(glossary_router, "_pool", _pool)
    monkeypatch.setattr(glossary_router, "_notify", _notify)
    # The gates resolve capabilities through app state; which right each endpoint names is asserted
    # in test_glossary_rights.py (REQ-1590) — here they must simply not block the handler.
    monkeypatch.setattr(glossary_router, "_require_glossary_read", lambda request: None)
    monkeypatch.setattr(glossary_router, "_require_glossary_rw", lambda request: None)
    monkeypatch.setattr(glossary_router, "_require_table_registration", lambda request: None)
    # Likewise the domain half: it too reads app state, and which endpoints narrow by domain is
    # asserted in test_glossary_domain_scope.py (REQ-1591). None is the unlimited caller.
    monkeypatch.setattr(glossary_router, "_authority", lambda request: None)
    # REQ-1592 split curation off from reach: the mutating gate asks _curate_authority, which reads
    # app state of its own. Which endpoints narrow by curation scope is asserted in
    # test_glossary_stewardship.py; here it must not block either.
    monkeypatch.setattr(glossary_router, "_curate_authority", lambda request: None)
    async with db.acquire() as conn:
        await table_repo.upsert(
            conn,
            Table(
                source_id="__derived__",
                domain_id="d",
                schema_name="s",
                table_name="orders",
                columns=[
                    Column(name="cust_id", data_type="text", visible_to=[]),
                    Column(name="order_dt", data_type="timestamp", visible_to=[]),
                ],
                view_sql="SELECT 1",
            ),
        )
    yield db, notified


async def _term_id(name: str) -> int:
    terms = await glossary_router.list_terms(_request(), domains=None)
    return next(t["id"] for t in terms if t["name"] == name)


async def test_list_search_and_detail(control_plane, monkeypatch):
    async with _surface(control_plane, monkeypatch):
        terms = await glossary_router.list_terms(_request(), domains=None)
        assert {t["name"] for t in terms} == {"customer", "order date"}
        filtered = await glossary_router.list_terms(_request(), q="custom", domains=None)
        assert [t["name"] for t in filtered] == ["customer"]
        detail = await glossary_router.get_term(_request(), await _term_id("customer"))
        assert detail["refs"][0]["column_name"] == "cust_id"


async def test_curation_mutations_notify_the_exporter(control_plane, monkeypatch):
    async with _surface(control_plane, monkeypatch) as (_db, notified):
        customer = await _term_id("customer")
        created = await glossary_router.create_abstract_term(
            _with_json(_request(), {"name": "party", "definition": "Any actor.", "domains": ["d"]})
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


async def test_export_excluded_toggle_round_trips_and_notifies(control_plane, monkeypatch):
    async with _surface(control_plane, monkeypatch) as (_db, notified):
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


async def test_retiring_moves_the_columns_to_the_successor_and_hides_the_term_from_mcp_search(
    control_plane, monkeypatch
):
    from provisa.api.mcp import tools

    async with _surface(control_plane, monkeypatch) as (db, notified):
        state = RoleState(
            contexts={"analyst": object()},
            model_db=db,
            tenant_db=db,
            # REQ-1591: the MCP surface narrows the vocabulary to the domains the ROLE reaches;
            # "*" is the seeded unlimited scope, so the search sees every term here.
            roles={"analyst": {"domain_access": ["*"]}},
        )
        customer = await _term_id("customer")
        await glossary_router.update_term(
            _with_json(_request(), {"definition": "The buyer."}), customer
        )
        client = (
            await glossary_router.create_abstract_term(
                _with_json(
                    _request(), {"name": "client", "definition": "One who buys.", "domains": ["d"]}
                )
            )
        )["id"]
        # REQ-2144: the PATCH flag is gone; retire is its own action, with a reason
        with pytest.raises(ApiError) as err:
            await glossary_router.update_term(_with_json(_request(), {"retired": True}), customer)
        assert err.value.code == "glossary.status_by_action"
        retired = await glossary_router.retire_term(
            _with_json(_request(), {"reason": "Renamed.", "successor": client}), customer
        )
        assert retired == {"ok": True, "term_id": customer, "status": "retired", "tag_changes": []}
        detail = await glossary_router.get_term(_request(), customer)
        assert detail["status"] == "retired" and detail["refs"] == []
        assert detail["status_reason"] == "Renamed."
        assert await tools.search_terms(state, "analyst", "customer") == []
        await glossary_router.approve_term(_request(), client)  # a steward's choice (REQ-1387)
        assert [h["name"] for h in await tools.search_terms(state, "analyst", "client")] == [
            "client"
        ]
        restored = await glossary_router.restore_term(_request(), customer)
        assert restored["status"] == "draft"  # an empty Concept: its columns stayed on client
        assert notified[-3:] == [
            "acme:ontology term retired",
            "acme:ontology term approved",
            "acme:ontology term restored",
        ]


async def test_edge_type_is_correctable_in_place(control_plane, monkeypatch):
    """Retyping is one act on one relationship, not a withdrawal plus a new assertion."""
    async with _surface(control_plane, monkeypatch) as (_db, notified):
        customer = await _term_id("customer")
        party = (
            await glossary_router.create_abstract_term(
                _with_json(
                    _request(), {"name": "party", "definition": "Any actor.", "domains": ["d"]}
                )
            )
        )["id"]
        await glossary_router.add_edge(
            _with_json(_request(), {"to_term_id": party, "rel_type": "RELATED_TO"}), customer
        )
        await glossary_router.retype_edge(
            _with_json(
                _request(),
                {"to_term_id": party, "rel_type": "RELATED_TO", "new_rel_type": "KIND_OF"},
            ),
            customer,
        )
        detail = await glossary_router.get_term(_request(), customer)
        # One edge, same endpoints and direction, corrected type.
        assert detail["edges_out"] == [{"term_id": party, "rel_type": "KIND_OF", "name": "party"}]

        with pytest.raises(ApiError) as unknown:
            await glossary_router.retype_edge(
                _with_json(
                    _request(),
                    {"to_term_id": party, "rel_type": "PART_OF", "new_rel_type": "KIND_OF"},
                ),
                customer,
            )
        assert unknown.value.status_code == 404
        with pytest.raises(ApiError) as bad:
            await glossary_router.retype_edge(
                _with_json(
                    _request(),
                    {"to_term_id": party, "rel_type": "KIND_OF", "new_rel_type": "VIBES_WITH"},
                ),
                customer,
            )
        assert bad.value.status_code == 400
        # create + add + retype; neither refusal published.
        assert len(notified) == 3


async def test_free_form_edge_type_is_refused(control_plane, monkeypatch):
    async with _surface(control_plane, monkeypatch) as (_db, notified):
        customer = await _term_id("customer")
        order_date = await _term_id("order date")
        with pytest.raises(ApiError) as err:
            await glossary_router.add_edge(
                _with_json(_request(), {"to_term_id": order_date, "rel_type": "VIBES_WITH"}),
                customer,
            )
        assert err.value.status_code == 400
        assert notified == []  # a refused mutation must not queue a publish


async def test_rooted_term_delete_is_refused(control_plane, monkeypatch):
    async with _surface(control_plane, monkeypatch) as (_db, notified):
        with pytest.raises(ApiError) as err:
            await glossary_router.delete_term(_request(), await _term_id("customer"))
        assert err.value.status_code == 400
        assert notified == []


async def test_move_ref_and_missing_term_404(control_plane, monkeypatch):
    async with _surface(control_plane, monkeypatch) as (db, _notified):
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
        # REQ-2145: no column carried a tag through the term it left, so no tag changed.
        assert moved == {"ok": True, "source_term_removed": True, "tag_changes": []}
        terms = await glossary_router.list_terms(_request(), domains=None)
        assert {t["name"] for t in terms} == {"customer"}
        with pytest.raises(ApiError) as err:
            await glossary_router.get_term(_request(), 99999)
        assert err.value.status_code == 404


async def test_ref_lookup_serves_the_hover_summary(control_plane, monkeypatch):
    async with _surface(control_plane, monkeypatch) as (db, _notified):
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


async def test_generate_definition_drafts_without_persisting(control_plane, monkeypatch):
    from provisa.api.admin import schema_helpers

    prompts: list[str] = []

    async def _fake_llm(prompt: str, operation: str, max_tokens: int = 256) -> str:
        prompts.append(prompt)
        assert operation == "glossary_definition"
        return "A paying party."

    monkeypatch.setattr(schema_helpers, "_call_llm", _fake_llm)
    async with _surface(control_plane, monkeypatch) as (_db, notified):
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


async def test_mcp_search_terms_requires_role_and_returns_refs(control_plane, monkeypatch):
    from provisa.api.mcp import tools

    async with _surface(control_plane, monkeypatch) as (db, _notified):
        state = RoleState(
            contexts={"analyst": object()},
            model_db=db,
            tenant_db=db,
            # REQ-1591: the MCP surface narrows the vocabulary to the domains the ROLE reaches;
            # "*" is the seeded unlimited scope, so the search sees every term here.
            roles={"analyst": {"domain_access": ["*"]}},
        )
        customer = await _term_id("customer")
        # Draft terms are proposals; the search surface offers what a steward approved.
        assert await tools.search_terms(state, "analyst", "customer") == []
        await glossary_router.update_term(
            _with_json(_request(), {"definition": "The buyer."}), customer
        )
        assert await tools.search_terms(state, "analyst", "customer") == []
        await glossary_router.approve_term(_request(), customer)
        hits = await tools.search_terms(state, "analyst", "customer")
        assert [t["name"] for t in hits] == ["customer"]
        assert hits[0]["refs"][0]["column_name"] == "cust_id"
        with pytest.raises(PermissionError):
            await tools.search_terms(state, "nobody", "customer")
        with pytest.raises(ValueError):
            await tools.search_terms(state, "analyst", "   ")


async def test_relationship_assistant_steers_to_grounding_types(control_plane, monkeypatch):
    """REQ-1387 (RELATED_TO IS THE LAST RESORT; THE SHAPES THAT RESOLVE A RELATED PAIR): the prompt
    carries each type's test and the five shapes tightest first; a RELATED_TO the model adds comes
    back with its shapes in that order, whatever order the model sent, and no term is added."""
    from provisa.api.admin import schema_helpers

    prompts: list[str] = []
    reply = (
        '[{"from": "order date", "to": "customer", "rel_type": "PART_OF"},'
        ' {"from": "order date", "to": "customer", "rel_type": "RELATED_TO",'
        '  "resolve": {"common_genus": ["sales fact"], "bogus": ["x"],'
        '              "component_type": ["  ", "purchase moment"], "common_whole": "party"}}]'
    )

    async def _fake_llm(prompt: str, operation: str, max_tokens: int = 256) -> str:
        prompts.append(prompt)
        assert operation == "glossary_relationships"
        return reply

    async def _no_jev(_pool):
        return None

    from provisa.core import org_secrets

    monkeypatch.setattr(schema_helpers, "_call_llm", _fake_llm)
    monkeypatch.setattr(org_secrets, "resolve_jev_api_key", _no_jev)
    async with _surface(control_plane, monkeypatch) as (_db, notified):
        result = await glossary_router.generate_relationships(_request())
        prompt = prompts[0]
        for test in (
            "KIND_OF: every X is a Y",
            "PART_OF: a Y has an X",
            "VALID_VALUE_OF: X is one of Y's allowed values",
            "RELATED_TO is the last resort",
            "It does not ground",
        ):
            assert test in prompt, test
        # Tightest first, as the guidance orders them.
        order = [
            prompt.index(f'"{s}"')
            for s in (
                "component_type",
                "specialisation",
                "common_whole",
                "common_genus",
                "common_enumeration",
            )
        ]
        assert order == sorted(order)
        # Each term rides with its role, so a concept's properties can be named as its parts.
        assert "- customer [property]" in prompt
        assert result["added"] == 2
        customer = await _term_id("customer")
        order_date = await _term_id("order date")
        assert result["review"] == [
            {
                "from_term_id": order_date,
                "from": "order date",
                "to_term_id": customer,
                "to": "customer",
                "resolve": [
                    {"shape": "component_type", "candidates": ["purchase moment"]},
                    {"shape": "common_genus", "candidates": ["sales fact"]},
                ],
            }
        ]
        terms = await glossary_router.list_terms(_request(), domains=None)
        # The candidates are suggestions: nothing named in them became a term.
        assert {t["name"] for t in terms} == {"customer", "order date"}
        # The list counts each term's RELATED_TO edges, the review filter's input.
        assert {t["name"]: t["related_to"] for t in terms} == {"customer": 1, "order date": 1}
        assert notified == ["acme:ontology relationships generated"]


async def test_resolving_terms_for_one_pair_are_suggested_not_written(control_plane, monkeypatch):
    """REQ-1387 (CHOOSING RELATED_TO OPENS THE ALTERNATIVES): a curator choosing RELATED_TO is
    offered candidate names per shape, through the assistant's own model path, tightest first;
    nothing is written."""
    from provisa.api.admin import schema_helpers

    prompts: list[str] = []

    async def _fake_llm(prompt: str, operation: str, max_tokens: int = 256) -> str:
        prompts.append(prompt)
        assert operation == "glossary_relationships"
        return '{"common_whole": ["purchase"], "component_type": ["order moment"], "x": ["y"]}'

    monkeypatch.setattr(schema_helpers, "_call_llm", _fake_llm)
    async with _surface(control_plane, monkeypatch) as (_db, notified):
        customer = await _term_id("customer")
        order_date = await _term_id("order date")
        result = await glossary_router.suggest_resolving_terms(
            _with_json(_request(), {"from_term_id": order_date, "to_term_id": customer})
        )
        assert result == {
            "resolve": [
                {"shape": "component_type", "candidates": ["order moment"]},
                {"shape": "common_whole", "candidates": ["purchase"]},
            ]
        }
        assert "X: order date [property]" in prompts[0] and "Y: customer [property]" in prompts[0]
        assert "does not ground" in prompts[0]
        terms = await glossary_router.list_terms(_request(), domains=None)
        assert {t["name"] for t in terms} == {"customer", "order date"}
        assert notified == []
        with pytest.raises(ApiError) as err:
            await glossary_router.suggest_resolving_terms(
                _with_json(_request(), {"from_term_id": 99999, "to_term_id": customer})
            )
        assert err.value.status_code == 404
