# Copyright (c) 2026 Kenneth Stott
# Canary: 9e4b2a17-5c8d-4f63-a0e1-7d2c9b5f8a36
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""REQ-2146: one findings endpoint says, per term, what a curator should look at -- each finding
a {kind, code, message, params}, one function per kind. A plain draft is not a finding."""

from __future__ import annotations

import datetime

import pytest

from provisa.api.admin import glossary_router
from provisa.core import glossary_findings
from provisa.core.repositories import glossary as glossary_repo
from provisa.core.schema_org import tag_assignments
from tests.unit.test_glossary_class_grounding import _ground, _request, _surface


async def _found(kind: str) -> dict[int, list[dict]]:
    """The findings of ``kind`` the route returns, by term id."""
    out: dict[int, list[dict]] = {}
    for entry in await glossary_router.list_findings(_request()):
        kinds = [f for f in entry["findings"] if f["kind"] == kind]
        if kinds:
            out[entry["term_id"]] = kinds
    return out


async def _note(db, ids) -> int:
    async with db.acquire() as conn:
        note = await glossary_repo.get_term_by_ref(conn, ids["customers"], "note")
    assert note is not None
    return note["id"]


def test_the_registry_holds_one_finder_per_kind():
    assert list(glossary_findings.FINDERS) == [
        "unlinked_tables",
        "class_and_attribute",
        "tag_wrong_kind",
        "retirement_blocked",
        "no_data",
    ]


def test_role_conflicts_is_folded_into_findings():
    paths = {getattr(r, "path", None) for r in glossary_router.router.routes}
    assert "/admin/glossary/findings" in paths
    assert "/admin/glossary/role-conflicts" not in paths


@pytest.mark.asyncio
async def test_unlinked_tables(control_plane, monkeypatch):
    async with _surface(control_plane, monkeypatch) as (_db, ids, _notified):
        await _ground(ids["party"], ids["customers"])
        assert ids["party"] not in await _found("unlinked_tables")  # one table is no gap
        await _ground(ids["party"], ids["accounts"])
        [finding] = (await _found("unlinked_tables"))[ids["party"]]
        assert finding["code"] == "glossary.finding.unlinked_tables"
        assert finding["params"] == {"tables": "__derived__.s.accounts, __derived__.s.customers"}


@pytest.mark.asyncio
async def test_tag_wrong_kind(control_plane, monkeypatch):
    async with _surface(control_plane, monkeypatch) as (db, ids, _notified):
        async with db.acquire() as conn:
            # planted past the tag rules, which refuse it: a tag on a Concept passes to nothing
            await conn.execute_core(
                tag_assignments.insert().values(
                    tag_id="pii",
                    base_tag_id="pii",
                    object_type="term",
                    term_id=ids["ledger"],
                    object_key=f"term:{ids['ledger']}",
                )
            )
        [finding] = (await _found("tag_wrong_kind"))[ids["ledger"]]
        assert finding["params"] == {"tags": "pii", "role": "concept"}


@pytest.mark.asyncio
async def test_retirement_blocked(control_plane, monkeypatch):
    async with _surface(control_plane, monkeypatch) as (db, ids, _notified):
        note = await _note(db, ids)
        today = datetime.date.today()
        async with db.acquire() as conn:
            await glossary_repo.set_definition(conn, note, "A remark.")
            await glossary_repo.deprecate_term(
                conn, note, reason="Old.", retire_on=today, by=None, today=today
            )
            # the day comes: the Property still holds its columns, so it is not retired
            assert await glossary_repo.retire_due(conn, today) == []
        [finding] = (await _found("retirement_blocked"))[note]
        assert finding["params"] == {"retire_on": today.isoformat(), "role": "property"}


@pytest.mark.asyncio
async def test_no_data_and_a_plain_draft_is_no_finding(control_plane, monkeypatch):
    async with _surface(control_plane, monkeypatch) as (db, ids, _notified):
        note = await _note(db, ids)
        found = await _found("no_data")
        # two Concepts that reach no data; the derived "note" is a plain draft, no finding
        assert {ids["party"], ids["ledger"]} <= set(found)
        everything = {e["term_id"] for e in await glossary_router.list_findings(_request())}
        assert note not in everything
        await _ground(ids["party"], ids["customers"])
        async with db.acquire() as conn:
            await glossary_repo.add_edge(conn, ids["ledger"], ids["party"], "RELATED_TO")
        assert not {ids["party"], ids["ledger"]} & set(await _found("no_data"))


@pytest.mark.asyncio
async def test_the_term_list_counts_each_terms_findings(control_plane, monkeypatch):
    async with _surface(control_plane, monkeypatch) as (_db, ids, _notified):
        listed = {
            t["name"]: t["findings"]
            for t in await glossary_router.list_terms(_request(), domains=None)
        }
        assert listed["ledger"] == 1 and listed["note"] == 0


@pytest.mark.asyncio
async def test_polly_lists_the_same_findings(control_plane, monkeypatch):
    from types import SimpleNamespace

    from provisa.api.mcp import tools

    async with _surface(control_plane, monkeypatch) as (db, _ids, _notified):
        monkeypatch.setattr(tools, "require_role", lambda role, state: None)
        state = SimpleNamespace(model_db=db)
        assert await tools.list_glossary_findings(state, "analyst", _request()) == (
            await glossary_router.list_findings(_request())
        )
