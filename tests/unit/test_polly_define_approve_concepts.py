# Copyright (c) 2026 Kenneth Stott
# Canary: 4b1f7c2e-93a6-4d58-b0e1-6a2c9d8f3e17
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""REQ-1835, REQ-2144: Polly adds a concept KIND_OF an existing term, defines both and approves
both -- through the same tool dispatch the chat turn uses -- and the Ontology page's reads (the
term list it filters by status, and the open term's detail) show the definitions and the
approved status."""

from __future__ import annotations

import pytest

from provisa.api.admin import glossary_router
from provisa.api.mcp import chat
from provisa.api.mcp import tools as mcp_tools
from tests.role_state import RoleState
from tests.unit.gate_identity import serve_every_column
from tests.unit.test_glossary_class_grounding import _request, _surface


@pytest.fixture(autouse=True)
def _served(monkeypatch):
    serve_every_column(monkeypatch)


async def _call(state, name: str, args: dict):
    return await chat._execute_tool(state, "analyst", name, args, scope=None, request=_request())


@pytest.mark.asyncio
async def test_polly_defines_and_approves_a_concept_and_its_broader_term(
    control_plane, monkeypatch
):
    async with _surface(control_plane, monkeypatch) as (db, ids, _notified):

        async def _no_selection(conn, term_id):
            return None

        monkeypatch.setattr(mcp_tools, "_refuse_term_outside_selection", _no_selection)
        state = RoleState(
            contexts={"analyst": object()}, roles={"analyst": {"id": "analyst"}}, model_db=db
        )
        party = ids["party"]
        made = await _call(state, "create_glossary_term", {"name": "patron", "domains": ["sales"]})
        customer = made["id"]
        await _call(
            state,
            "add_glossary_term_edge",
            {"term_id": customer, "to_term_id": party, "rel_type": "KIND_OF"},
        )
        await _call(
            state,
            "update_glossary_term",
            {"term_id": party, "definition": "Anyone the business deals with."},
        )
        await _call(
            state,
            "update_glossary_term",
            {"term_id": customer, "definition": "A party that buys from the business."},
        )
        for term_id in (party, customer):
            done = await _call(state, "approve_glossary_term", {"term_id": term_id})
            assert done["status"] == "approved"

        # What the Ontology page reads: GET /admin/glossary/terms (filtered by status in the
        # browser) and GET /admin/glossary/terms/{id} for the open term.
        listed = {t["id"]: t for t in await glossary_router.list_terms(_request(), domains=None)}
        approved = {tid for tid, t in listed.items() if t["status"] == "approved"}
        assert {party, customer} <= approved
        assert listed[party]["definition"] == "Anyone the business deals with."
        assert listed[customer]["definition"] == "A party that buys from the business."
        for term_id, text in (
            (party, "Anyone the business deals with."),
            (customer, "A party that buys from the business."),
        ):
            detail = await glossary_router.get_term(_request(), term_id)
            assert detail["definition"] == text
            assert detail["status"] == "approved"
