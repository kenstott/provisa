# Copyright (c) 2026 Kenneth Stott
# Canary: 4c7e2a91-b3d5-4f86-9e10-7a2d5c8b6f43
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""REQ-2259: the Glossary page reads the approved terms the caller may read, with their names,
definitions and domains and the a-kind-of, see-also and also-called edges between them; no draft,
deprecated or retired term, and no edge reaching one."""

# Requirements: REQ-2259, REQ-1592

from __future__ import annotations

import inspect
from datetime import date

import pytest
from sqlalchemy import insert

from provisa.api.admin import glossary_router
from provisa.core.repositories import glossary as glossary_repo
from provisa.core.schema_org import domains
from tests.unit.gate_identity import serve_every_column
from tests.unit.test_glossary_class_grounding import SCHEMA, _request, _surface


@pytest.fixture(autouse=True)
def _served(monkeypatch):
    serve_every_column(monkeypatch)


async def _approved(conn, name: str, definition: str, scope: set[str]) -> int:
    term = await glossary_repo.create_abstract_term(
        conn, name, definition=definition, domains=scope
    )
    await glossary_repo.approve_term(conn, term, by=None)
    return term


def test_the_entries_endpoint_asks_the_glossary_read_right():
    assert "_require_glossary_read(request)" in inspect.getsource(glossary_router.glossary_entries)


@pytest.mark.asyncio
async def test_only_approved_terms_and_the_edges_between_them(control_plane, monkeypatch):
    async with _surface(control_plane, monkeypatch) as (db, ids, _notified):
        async with db.acquire() as conn:
            account = await _approved(conn, "fund", "A pool of money.", {"sales"})
            savings = await _approved(conn, "savings fund", "A fund earning interest.", {"sales"})
            client = await _approved(conn, "client", "Someone we serve.", {"sales"})
            patron = await _approved(conn, "patron", "Someone we serve.", {"sales"})
            # party: a defined draft -- not on the page, nor any edge to it
            await glossary_repo.set_definition(conn, ids["party"], "Someone we deal with.")
            await glossary_repo.add_edge(conn, savings, account, "KIND_OF")
            await glossary_repo.add_edge(conn, account, client, "RELATED_TO")
            await glossary_repo.add_edge(conn, client, patron, "SYNONYM_OF")
            await glossary_repo.add_edge(conn, client, ids["party"], "KIND_OF")
            await glossary_repo.add_edge(conn, savings, client, "PART_OF")
        page = await glossary_router.glossary_entries(_request())
        assert [t["name"] for t in page["terms"]] == [
            "client",
            "fund",
            "patron",
            "savings fund",
        ]
        assert page["terms"][1] == {
            "id": account,
            "name": "fund",
            "definition": "A pool of money.",
            "domains": ["sales"],
        }
        assert sorted(
            (e["from_term_id"], e["to_term_id"], e["rel_type"]) for e in page["edges"]
        ) == sorted(
            [
                (savings, account, "KIND_OF"),
                (account, client, "RELATED_TO"),
                (client, patron, "SYNONYM_OF"),
            ]
        )


@pytest.mark.asyncio
async def test_a_deprecated_or_retired_term_leaves_the_page(control_plane, monkeypatch):
    async with _surface(control_plane, monkeypatch) as (db, _ids, _notified):
        async with db.acquire() as conn:
            kept = await _approved(conn, "kept", "Still in service.", {"sales"})
            old = await _approved(conn, "old", "Going away.", {"sales"})
            await glossary_repo.deprecate_term(
                conn, old, reason="superseded", retire_on=None, by=None, today=date.today()
            )
        page = await glossary_router.glossary_entries(_request())
        assert [t["id"] for t in page["terms"]] == [kept]


@pytest.mark.asyncio
async def test_a_term_outside_the_callers_domains_is_not_shown(control_plane, monkeypatch):
    with control_plane.engine().begin() as c:
        c.exec_driver_sql(f'SET LOCAL search_path TO "{SCHEMA}"')
        c.execute(insert(domains).values(id="finance", description=""))
    async with _surface(control_plane, monkeypatch, authority=frozenset({"sales"})) as (
        db,
        _ids,
        _notified,
    ):
        async with db.acquire() as conn:
            mine = await _approved(conn, "order", "A request to buy.", {"sales"})
            theirs = await _approved(conn, "accrual", "Earned, not yet paid.", {"finance"})
            shared = await _approved(conn, "company", "The whole firm.", {"*"})
            await glossary_repo.add_edge(conn, mine, theirs, "RELATED_TO")
        page = await glossary_router.glossary_entries(_request())
        assert sorted(t["id"] for t in page["terms"]) == sorted([mine, shared])
        assert page["edges"] == []
