# Copyright (c) 2026 Kenneth Stott
# Canary: 3b9e6d14-7a2c-4f58-b0e3-91c4d2a7f605
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""REQ-2259 (RECENT CHANGE IS THE TERM'S LAST WRITE): every glossary term records when it last
changed. Any write to its name, definition, calculation method, domains, relations on either end or
status sets that time. Each test backdates the term, makes one write, and reads the time again."""

# Requirements: REQ-2259

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

import pytest
from sqlalchemy import select, update

from provisa.core.repositories import glossary as glossary_repo
from provisa.core.schema_org import glossary_terms
from tests.unit.gate_identity import serve_every_column
from tests.unit.test_glossary_class_grounding import _surface

_LONG_AGO = datetime(2020, 1, 1, tzinfo=UTC)


@pytest.fixture(autouse=True)
def _served(monkeypatch):
    serve_every_column(monkeypatch)


async def _changed_at(conn, term_id: int) -> datetime:
    row = (
        await conn.execute_core(
            select(glossary_terms.c.changed_at).where(glossary_terms.c.id == term_id)
        )
    ).fetchone()
    assert row is not None
    return row.changed_at


async def _backdate(conn, *term_ids: int) -> None:
    await conn.execute_core(
        update(glossary_terms).where(glossary_terms.c.id.in_(term_ids)).values(changed_at=_LONG_AGO)
    )


async def _moved(conn, term_id: int) -> bool:
    return await _changed_at(conn, term_id) > _LONG_AGO


async def _term(conn, name: str, definition: str | None = "Defined.") -> int:
    return await glossary_repo.create_abstract_term(
        conn, name, definition=definition, domains={"sales"}
    )


@pytest.mark.asyncio
async def test_a_created_term_records_when(control_plane, monkeypatch):
    before = datetime.now(UTC) - timedelta(seconds=5)
    async with _surface(control_plane, monkeypatch) as (db, _ids, _notified):
        async with db.acquire() as conn:
            abstract = await _term(conn, "pool")
            declared = await glossary_repo.upsert_declared_term(
                conn, "ledger", definition="A book.", domains={"sales"}
            )
            derived = await glossary_repo._find_or_create_term(conn, "sku", "sales")
            for tid in (abstract, declared, derived):
                assert await _changed_at(conn, tid) >= before


@pytest.mark.asyncio
async def test_a_rename_sets_it(control_plane, monkeypatch):
    async with _surface(control_plane, monkeypatch) as (db, _ids, _notified):
        async with db.acquire() as conn:
            tid = await _term(conn, "pool")
            await _backdate(conn, tid)
            assert await glossary_repo.rename_term(conn, tid, "fund")
            assert await _moved(conn, tid)


@pytest.mark.asyncio
async def test_a_definition_sets_it(control_plane, monkeypatch):
    async with _surface(control_plane, monkeypatch) as (db, _ids, _notified):
        async with db.acquire() as conn:
            tid = await _term(conn, "pool")
            await _backdate(conn, tid)
            assert await glossary_repo.set_definition(conn, tid, "Money held together.")
            assert await _moved(conn, tid)


@pytest.mark.asyncio
async def test_a_redeclared_term_sets_it(control_plane, monkeypatch):
    async with _surface(control_plane, monkeypatch) as (db, _ids, _notified):
        async with db.acquire() as conn:
            tid = await glossary_repo.upsert_declared_term(
                conn, "ledger", definition="A book.", domains={"sales"}
            )
            await _backdate(conn, tid)
            again = await glossary_repo.upsert_declared_term(
                conn, "ledger", definition="The books.", domains={"sales"}
            )
            assert again == tid
            assert await _moved(conn, tid)


@pytest.mark.asyncio
async def test_a_calculation_method_sets_it(control_plane, monkeypatch):
    async with _surface(control_plane, monkeypatch) as (db, _ids, _notified):
        async with db.acquire() as conn:
            tid = await _term(conn, "margin")
            await _backdate(conn, tid)
            assert await glossary_repo.set_calculation_method(conn, tid, "Revenue less cost.")
            assert await _moved(conn, tid)


@pytest.mark.asyncio
async def test_declared_domains_set_it(control_plane, monkeypatch):
    async with _surface(control_plane, monkeypatch) as (db, _ids, _notified):
        async with db.acquire() as conn:
            tid = await _term(conn, "pool")
            await _backdate(conn, tid)
            await glossary_repo.set_declared_domains(conn, tid, {"*"})
            assert await _moved(conn, tid)


@pytest.mark.asyncio
async def test_an_approval_sets_it(control_plane, monkeypatch):
    async with _surface(control_plane, monkeypatch) as (db, _ids, _notified):
        async with db.acquire() as conn:
            tid = await _term(conn, "pool")
            await _backdate(conn, tid)
            await glossary_repo.approve_term(conn, tid, by=None)
            assert await _moved(conn, tid)


@pytest.mark.asyncio
async def test_a_deprecation_sets_it(control_plane, monkeypatch):
    async with _surface(control_plane, monkeypatch) as (db, _ids, _notified):
        async with db.acquire() as conn:
            tid = await _term(conn, "pool")
            await glossary_repo.approve_term(conn, tid, by=None)
            await _backdate(conn, tid)
            await glossary_repo.deprecate_term(
                conn, tid, reason="superseded", retire_on=None, by=None, today=date.today()
            )
            assert await _moved(conn, tid)


@pytest.mark.asyncio
async def test_a_retirement_sets_it_on_the_term_and_its_successor(control_plane, monkeypatch):
    async with _surface(control_plane, monkeypatch) as (db, _ids, _notified):
        async with db.acquire() as conn:
            old = await _term(conn, "pool")
            new = await _term(conn, "fund")
            await _backdate(conn, old, new)
            await glossary_repo.retire_term(conn, old, reason="merged", successor=new, by=None)
            assert await _moved(conn, old)
            assert await _moved(conn, new)  # it gained the REPLACES edge


@pytest.mark.asyncio
async def test_a_restore_sets_it_on_the_term_and_its_old_successor(control_plane, monkeypatch):
    async with _surface(control_plane, monkeypatch) as (db, _ids, _notified):
        async with db.acquire() as conn:
            old = await _term(conn, "pool")
            new = await _term(conn, "fund")
            await glossary_repo.retire_term(conn, old, reason="merged", successor=new, by=None)
            await _backdate(conn, old, new)
            assert await glossary_repo.restore_term(conn, old, by=None)
            assert await _moved(conn, old)
            assert await _moved(conn, new)  # it lost the REPLACES edge


@pytest.mark.asyncio
async def test_a_scheduled_retirement_sets_it(control_plane, monkeypatch):
    async with _surface(control_plane, monkeypatch) as (db, _ids, _notified):
        async with db.acquire() as conn:
            tid = await _term(conn, "pool")
            await glossary_repo.approve_term(conn, tid, by=None)
            today = date.today()
            await glossary_repo.deprecate_term(
                conn, tid, reason="superseded", retire_on=today, by=None, today=today
            )
            await _backdate(conn, tid)
            assert await glossary_repo.retire_due(conn, today) == [tid]
            assert await _moved(conn, tid)


@pytest.mark.asyncio
async def test_a_derived_deprecation_and_revival_set_it(control_plane, monkeypatch):
    async with _surface(control_plane, monkeypatch) as (db, _ids, _notified):
        async with db.acquire() as conn:
            tid = await glossary_repo._find_or_create_term(conn, "sku", "sales")
            await glossary_repo.set_definition(conn, tid, "A stock unit.")  # curated: kept
            await _backdate(conn, tid)
            removed = await glossary_repo._settle_terms(conn, {tid}, domains_before={})
            assert removed == set()
            assert await _moved(conn, tid)
            await _backdate(conn, tid)
            assert await glossary_repo._find_or_create_term(conn, "sku", "sales") == tid  # revived
            assert await _moved(conn, tid)


@pytest.mark.asyncio
async def test_adding_an_edge_sets_it_on_both_ends(control_plane, monkeypatch):
    async with _surface(control_plane, monkeypatch) as (db, _ids, _notified):
        async with db.acquire() as conn:
            a = await _term(conn, "pool")
            b = await _term(conn, "fund")
            await _backdate(conn, a, b)
            await glossary_repo.add_edge(conn, a, b, "RELATED_TO")
            assert await _moved(conn, a)
            assert await _moved(conn, b)


@pytest.mark.asyncio
async def test_removing_an_edge_sets_it_on_both_ends(control_plane, monkeypatch):
    async with _surface(control_plane, monkeypatch) as (db, _ids, _notified):
        async with db.acquire() as conn:
            a = await _term(conn, "pool")
            b = await _term(conn, "fund")
            await glossary_repo.add_edge(conn, a, b, "RELATED_TO")
            await _backdate(conn, a, b)
            assert await glossary_repo.remove_edge(conn, a, b, "RELATED_TO")
            assert await _moved(conn, a)
            assert await _moved(conn, b)


@pytest.mark.asyncio
async def test_retyping_an_edge_sets_it_on_both_ends(control_plane, monkeypatch):
    async with _surface(control_plane, monkeypatch) as (db, _ids, _notified):
        async with db.acquire() as conn:
            a = await _term(conn, "pool")
            b = await _term(conn, "fund")
            await glossary_repo.add_edge(conn, a, b, "RELATED_TO")
            await _backdate(conn, a, b)
            assert await glossary_repo.retype_edge(conn, a, b, "RELATED_TO", "KIND_OF")
            assert await _moved(conn, a)
            assert await _moved(conn, b)


@pytest.mark.asyncio
async def test_a_removed_edge_that_was_not_there_changes_nothing(control_plane, monkeypatch):
    async with _surface(control_plane, monkeypatch) as (db, _ids, _notified):
        async with db.acquire() as conn:
            a = await _term(conn, "pool")
            b = await _term(conn, "fund")
            await _backdate(conn, a, b)
            assert not await glossary_repo.remove_edge(conn, a, b, "RELATED_TO")
            assert not await _moved(conn, a)
            assert not await _moved(conn, b)
