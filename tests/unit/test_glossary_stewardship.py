# Copyright (c) 2026 Kenneth Stott
# Canary: 9e3b6a14-27d8-4c5f-b0a2-61f4d8c7e925
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""REQ-2144 (A STEWARD SEES THE IMPACT OF EACH ACTION): before a steward approves, retires or binds
a term the page previews what it changes; a stewardship summary shows the terms agents see, the
terms waiting on one approval and the approved terms that reach no data; defined, grounded drafts
are approved together."""

# Requirements: REQ-2144, REQ-1387

from __future__ import annotations

import pytest

from provisa.api.admin import glossary_router
from provisa.api.errors import ApiError
from provisa.core.repositories import glossary as glossary_repo
from tests.unit.gate_identity import serve_every_column
from tests.unit.test_glossary_class_grounding import (
    _request,
    _surface,
    _with_json,
)


@pytest.fixture(autouse=True)
def _served(monkeypatch):
    serve_every_column(monkeypatch)


async def _note(db, ids) -> int:
    async with db.acquire() as conn:
        note = await glossary_repo.get_term_by_ref(conn, ids["customers"], "note")
    assert note is not None
    return note["id"]


async def _chain(db, ids, *, approve_ledger: bool = True) -> int:
    """ledger (approved) - party (defined draft) - note (the data end, on customers): note is a
    kind of party, a kind of ledger, so grounding runs down from ledger to note."""
    note = await _note(db, ids)
    async with db.acquire() as conn:
        await glossary_repo.set_definition(conn, ids["party"], "Someone we deal with.")
        await glossary_repo.set_definition(conn, ids["ledger"], "What the books record.")
        if approve_ledger:
            await glossary_repo.approve_term(conn, ids["ledger"], by=None)
        await glossary_repo.add_edge(conn, ids["party"], ids["ledger"], "KIND_OF")
        await glossary_repo.add_edge(conn, note, ids["party"], "KIND_OF")
    return note


def _names(items: list[dict]) -> list[str]:
    return [i["name"] for i in items]


async def _tables_of(db, term_id: int) -> int:
    """How many tables the term holds a column of."""
    async with db.acquire() as conn:
        term = await glossary_repo.get_term(conn, term_id)
    assert term is not None
    return len({r["table_id"] for r in term["refs"]})


@pytest.mark.asyncio
async def test_approving_a_draft_between_previews_the_terms_it_grounds(control_plane, monkeypatch):
    async with _surface(control_plane, monkeypatch) as (db, ids, notified):
        await _chain(db, ids)
        preview = await glossary_router.preview_impact(_request(), ids["party"], action="approve")
        assert preview["term"] == "party"
        assert _names(preview["becomes_grounded"]) == ["ledger"]
        assert preview["becomes_pending"] == preview["becomes_not_grounded"] == []
        # the agents already reach customers, through the approved ledger
        assert (preview["tables_gained"], preview["tables_lost"]) == (0, 0)
        # a preview changes nothing and tells no one
        async with db.acquire() as conn:
            assert (await glossary_repo.term_statuses(conn))[ids["party"]] == "draft"
            assert (await glossary_repo.grounding(conn))[ids["ledger"]] == "grounded_pending"
        assert notified == []


@pytest.mark.asyncio
async def test_approving_the_first_term_on_a_path_gives_agents_its_tables(
    control_plane, monkeypatch
):
    async with _surface(control_plane, monkeypatch) as (db, ids, _notified):
        note = await _chain(db, ids, approve_ledger=False)
        preview = await glossary_router.preview_impact(_request(), ids["party"], action="approve")
        # ledger, a draft, reached data only through draft party; party was grounded already
        assert _names(preview["becomes_grounded"]) == ["ledger"]
        # no approved term was on the path, so agents reached none of note's tables
        assert (preview["tables_gained"], preview["tables_lost"]) == (await _tables_of(db, note), 0)


@pytest.mark.asyncio
async def test_retiring_a_term_on_the_only_path_previews_what_loses_data(
    control_plane, monkeypatch
):
    async with _surface(control_plane, monkeypatch) as (db, ids, _notified):
        note = await _chain(db, ids)
        preview = await glossary_router.preview_impact(_request(), ids["party"], action="retire")
        assert _names(preview["becomes_not_grounded"]) == ["ledger", "party"]
        assert (preview["tables_gained"], preview["tables_lost"]) == (0, await _tables_of(db, note))


@pytest.mark.asyncio
async def test_binding_an_approved_concept_to_a_table_previews_the_gain(control_plane, monkeypatch):
    async with _surface(control_plane, monkeypatch) as (db, ids, _notified):
        async with db.acquire() as conn:
            await glossary_repo.set_definition(conn, ids["ledger"], "What the books record.")
            await glossary_repo.approve_term(conn, ids["ledger"], by=None)
        preview = await glossary_router.preview_impact(
            _request(), ids["ledger"], action="bind", table_id=ids["accounts"]
        )
        assert _names(preview["becomes_grounded"]) == ["ledger"]
        assert (preview["tables_gained"], preview["tables_lost"]) == (1, 0)


@pytest.mark.asyncio
async def test_a_preview_names_its_refusals(control_plane, monkeypatch):
    async with _surface(control_plane, monkeypatch) as (_db, ids, _notified):
        with pytest.raises(ApiError) as err:
            await glossary_router.preview_impact(_request(), ids["party"], action="delete")
        assert err.value.code == "glossary.impact_action"
        with pytest.raises(ApiError) as err:
            await glossary_router.preview_impact(_request(), ids["party"], action="bind")
        assert err.value.code == "glossary.impact_table_required"


@pytest.mark.asyncio
async def test_the_stewardship_summary(control_plane, monkeypatch):
    async with _surface(control_plane, monkeypatch) as (db, ids, _notified):
        note = await _chain(db, ids)
        async with db.acquire() as conn:
            spare = await glossary_repo.create_abstract_term(
                conn, "spare", definition="Wired to nothing.", domains={"sales"}
            )
            await glossary_repo.approve_term(conn, spare, by=None)
        summary = await glossary_router.stewardship(_request())
        # ledger is seen, and so is party, a defined draft on its path; note is an undefined hop
        assert summary["agent_visible"] == [
            {"term_id": ids["ledger"], "name": "ledger", "status": "approved"},
            {"term_id": ids["party"], "name": "party", "status": "draft"},
        ]
        assert note not in {t["term_id"] for t in summary["agent_visible"]}
        assert summary["waiting_on_one_approval"] == [
            {
                "term_id": ids["ledger"],
                "name": "ledger",
                "approve": [{"term_id": ids["party"], "name": "party"}],
            }
        ]
        assert summary["approved_not_grounded"] == [{"term_id": spare, "name": "spare"}]


@pytest.mark.asyncio
async def test_defined_grounded_drafts_are_approved_together(control_plane, monkeypatch):
    async with _surface(control_plane, monkeypatch) as (db, ids, notified):
        note = await _chain(db, ids, approve_ledger=False)
        async with db.acquire() as conn:
            await glossary_repo.set_definition(conn, note, "A free-text remark.")
        approvable = await glossary_router.list_approvable(_request())
        # ledger is defined and grounded too: party is a draft, but nothing stands between
        # party and note, and only one term stands between ledger and data
        names = _names(approvable)
        assert {"note", "party"} <= set(names) and "ledger" not in names
        # an undefined or ungrounded draft refuses the whole batch
        with pytest.raises(ApiError) as err:
            await glossary_router.bulk_approve(
                _with_json(_request(), {"term_ids": [note, ids["ledger"]]})
            )
        assert err.value.code == "glossary.not_bulk_approvable"
        async with db.acquire() as conn:
            assert (await glossary_repo.term_statuses(conn))[note] == "draft"
        done = await glossary_router.bulk_approve(
            _with_json(_request(), {"term_ids": [note, ids["party"]]})
        )
        assert done == {"ok": True, "approved": sorted([note, ids["party"]])}
        assert notified[-1] == "ontology terms approved"
        async with db.acquire() as conn:
            statuses = await glossary_repo.term_statuses(conn)
        assert statuses[note] == statuses[ids["party"]] == "approved"
