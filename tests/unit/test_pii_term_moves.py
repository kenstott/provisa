# Copyright (c) 2026 Kenneth Stott
# Canary: 2040d1b2-e990-4929-b23f-abf875e9e5b6
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2145: a move a person makes of a column's ref, and the tags the column carries through its
term.

A move that takes a restricting tag (pii) off a column -- one it carried through the term it
leaves -- is refused with each column's change, unless every lost tag is confirmed or carried to
the term the column moves to. A move the model makes by derivation (sync, key re-homing on ground
and unground) changes no inherited tag. A column's own tags never change on a move.
"""

# Requirements: REQ-2145

from __future__ import annotations

import types
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, cast

import pytest
from sqlalchemy import insert, select, update

if TYPE_CHECKING:
    from fastapi import Request

from provisa.api.admin import glossary_router
from provisa.api.errors import ApiError
from provisa.core.models import Column, Table, TagAssignment
from provisa.core.repositories import glossary as glossary_repo
from provisa.core.repositories import table as table_repo
from provisa.core.repositories import tag as tag_repo
from provisa.core.schema_org import domains, glossary_terms, registered_tables, sources
from provisa.core.term_tags import (
    TagLossRefused,
    TermTagRefused,
    effective_column_tags,
    inherited_snapshot,
    tag_changes_of_moves,
)

SCHEMA = "org_default"


def _request(body: dict | None = None) -> "Request":
    fake = types.SimpleNamespace(
        state=types.SimpleNamespace(
            identity=types.SimpleNamespace(user_id="alice", roles=[], acting_roles=[]),
            active_org_id="acme",
        )
    )

    async def _json():
        return body or {}

    cast(Any, fake).json = _json
    return cast("Request", fake)


def _tbl(name: str, column: str, *, keyed: bool) -> Table:
    return Table(
        source_id="__derived__",
        domain_id="sales",
        schema_name="s",
        table_name=name,
        columns=[
            Column(name="id", data_type="integer", visible_to=[], is_primary_key=keyed),
            Column(name=column, data_type="text", visible_to=[]),
        ],
        view_sql="SELECT 1",
    )


async def _table_id(conn, name: str) -> int:
    return (
        await conn.execute_core(
            select(registered_tables.c.id).where(registered_tables.c.table_name == name)
        )
    ).scalar_one()


async def _term_of(conn, table_id: int, column: str) -> int:
    term = await glossary_repo.get_term_by_ref(conn, table_id, column)
    assert term is not None
    return term["id"]


async def _define(conn, term_id: int, definition: str = "Where a person lives") -> None:
    await conn.execute_core(
        update(glossary_terms).where(glossary_terms.c.id == term_id).values(definition=definition)
    )


async def _tag_term(conn, term_id: int) -> None:
    await tag_repo.assign(
        conn,
        TagAssignment(tag_id="pii", object_type="term", term_id=term_id, reason="a home address"),
    )


@asynccontextmanager
async def _model(control_plane, monkeypatch):
    """customers and accounts (keyed) each hold home_address, one live Property tagged pii;
    notes (unkeyed) holds body, a second Property, defined and untagged."""
    with control_plane.engine().begin() as c:
        c.exec_driver_sql(f'SET LOCAL search_path TO "{SCHEMA}"')
        c.execute(insert(domains).values(id="sales", description=""))
        c.execute(insert(sources).values(id="__derived__", type="derived"))
    db = control_plane.org()

    async def _pool():
        return db

    async def _notify(org_id: str, reason: str) -> None:
        return None

    monkeypatch.setattr(glossary_router, "_pool", _pool)
    monkeypatch.setattr(glossary_router, "_notify", _notify)
    monkeypatch.setattr(glossary_router, "_require_glossary_rw", lambda request: None)
    monkeypatch.setattr(glossary_router, "_authority", lambda request: None)
    monkeypatch.setattr(glossary_router, "_curate_authority", lambda request: None)
    monkeypatch.setattr(glossary_router, "_is_org_curator", lambda request: True)
    ids: dict[str, int] = {}
    async with db.acquire() as conn:
        await table_repo.upsert(conn, _tbl("customers", "home_address", keyed=True))
        await table_repo.upsert(conn, _tbl("accounts", "home_address", keyed=True))
        await table_repo.upsert(conn, _tbl("notes", "body", keyed=False))
        for name in ("customers", "accounts", "notes"):
            ids[name] = await _table_id(conn, name)
        ids["address"] = await _term_of(conn, ids["customers"], "home_address")
        assert ids["address"] == await _term_of(conn, ids["accounts"], "home_address")
        ids["body"] = await _term_of(conn, ids["notes"], "body")
        await _define(conn, ids["address"])
        await _define(conn, ids["body"], "The text of a note")
        await _tag_term(conn, ids["address"])
    yield db, ids


async def _move(ids: dict[str, int], **options) -> dict:
    body = {
        "table_id": ids["customers"],
        "column_name": "home_address",
        "to_term_id": ids["body"],
        **options,
    }
    return await glossary_router.move_ref(_request(body))


async def _pii_columns(db) -> set[tuple[int, str]]:
    async with db.acquire() as conn:
        return {
            key
            for key, tags in (await effective_column_tags(conn)).items()
            if any(t["base_tag_id"] == "pii" for t in tags)
        }


@pytest.mark.asyncio
async def test_a_move_that_takes_pii_off_a_column_is_refused_with_its_summary(
    control_plane, monkeypatch
):
    async with _model(control_plane, monkeypatch) as (db, ids):
        with pytest.raises(ApiError) as refused:
            await _move(ids)
        assert refused.value.status_code == 409
        assert refused.value.code == "tag.move_loses"
        assert refused.value.params["columns"] == [
            {
                "column": "home_address",
                "table": "__derived__.s.customers",
                "loses": ["pii"],
                "gains": [],
            }
        ]
        async with db.acquire() as conn:  # nothing moved
            assert await _term_of(conn, ids["customers"], "home_address") == ids["address"]
        assert (ids["customers"], "home_address") in await _pii_columns(db)


@pytest.mark.asyncio
async def test_a_confirmed_loss_moves_the_column_and_it_is_no_longer_pii(
    control_plane, monkeypatch
):
    async with _model(control_plane, monkeypatch) as (db, ids):
        moved = await _move(ids, confirm_tag_loss=["pii"])
        assert moved["tag_changes"] == [
            {
                "column": "home_address",
                "table": "__derived__.s.customers",
                "loses": ["pii"],
                "gains": [],
            }
        ]
        assert await _pii_columns(db) == {(ids["accounts"], "home_address")}


@pytest.mark.asyncio
async def test_a_move_never_changes_a_columns_own_tags(control_plane, monkeypatch):
    async with _model(control_plane, monkeypatch) as (db, ids):
        async with db.acquire() as conn:
            await tag_repo.assign(
                conn,
                TagAssignment(
                    tag_id="pii",
                    object_type="column",
                    table_id=ids["customers"],
                    column_name="home_address",
                    reason="marked by hand",
                ),
            )
        # It still carries pii, its own: the inherited copy going is listed, and needs no
        # confirming.
        moved = await _move(ids)
        assert moved["tag_changes"] == [
            {
                "column": "home_address",
                "table": "__derived__.s.customers",
                "loses": ["pii"],
                "gains": [],
            }
        ]
        async with db.acquire() as conn:
            tags = (await effective_column_tags(conn, {ids["customers"]}))[
                (ids["customers"], "home_address")
            ]
        assert tags == [
            {"tag_id": "pii", "base_tag_id": "pii", "via": "column", "reason": "marked by hand"}
        ]


@pytest.mark.asyncio
async def test_carrying_the_tags_marks_the_term_the_column_moves_to(control_plane, monkeypatch):
    async with _model(control_plane, monkeypatch) as (db, ids):
        moved = await _move(ids, carry_tags=True)
        assert moved["tag_changes"] == []
        async with db.acquire() as conn:
            assert await glossary_repo.term_tags(conn, ids["body"]) == [
                {"tag_id": "pii", "reason": "a home address"}
            ]
        assert await _pii_columns(db) == {
            (ids["customers"], "home_address"),
            (ids["accounts"], "home_address"),
            (ids["notes"], "body"),
        }


@pytest.mark.asyncio
async def test_carrying_to_a_term_that_is_not_live_is_refused_and_nothing_moves(
    control_plane, monkeypatch
):
    async with _model(control_plane, monkeypatch) as (db, ids):
        async with db.acquire() as conn:
            await _define(conn, ids["body"], "")
        with pytest.raises(ApiError) as refused:
            await _move(ids, carry_tags=True)
        assert refused.value.code == "tag.term_not_live"
        async with db.acquire() as conn:
            assert await _term_of(conn, ids["customers"], "home_address") == ids["address"]
            assert await glossary_repo.term_tags(conn, ids["body"]) == []


@pytest.mark.asyncio
async def test_the_check_is_one_function_any_move_calls(control_plane, monkeypatch):
    """Retire's move of a term's columns calls it as any move does: snapshot, move, check."""
    async with _model(control_plane, monkeypatch) as (db, ids):
        async with db.acquire() as conn:
            with pytest.raises(TagLossRefused) as refused:
                async with conn.transaction():
                    before = await inherited_snapshot(conn)
                    for table in ("customers", "accounts"):
                        await glossary_repo.move_ref(conn, ids[table], "home_address", ids["body"])
                    await tag_changes_of_moves(conn, before)
            assert sorted((c["table"], c["loses"]) for c in refused.value.columns) == [
                ("__derived__.s.accounts", ["pii"]),
                ("__derived__.s.customers", ["pii"]),
            ]
            # undone with the transaction
            assert await _term_of(conn, ids["accounts"], "home_address") == ids["address"]
            await _define(conn, ids["body"], "")
            with pytest.raises(TermTagRefused):
                async with conn.transaction():
                    before = await inherited_snapshot(conn)
                    await glossary_repo.move_ref(
                        conn, ids["customers"], "home_address", ids["body"]
                    )
                    await tag_changes_of_moves(conn, before, carry_tags=True)


@pytest.mark.asyncio
async def test_moves_the_model_derives_change_no_inherited_tag(control_plane, monkeypatch):
    """Sync re-deriving refs and key refs re-homed on ground and unground leave every column's
    inherited tags as they were."""
    async with _model(control_plane, monkeypatch) as (db, ids):
        async with db.acquire() as conn:
            before = await inherited_snapshot(conn)
            await table_repo.upsert(conn, _tbl("customers", "home_address", keyed=True))
            await table_repo.upsert(conn, _tbl("notes", "body", keyed=False))
            assert await inherited_snapshot(conn) == before
            party = await glossary_repo.create_abstract_term(conn, "party", domains={"sales"})
            await glossary_repo.ground_table(conn, party, ids["customers"])
            assert await inherited_snapshot(conn) == before
            await glossary_repo.unground_table(conn, party, ids["customers"])
            assert await inherited_snapshot(conn) == before
            assert await tag_changes_of_moves(conn, before) == []
