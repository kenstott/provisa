# Copyright (c) 2026 Kenneth Stott
# Canary: 2c7e9a51-6f3b-4d08-9e14-b5a8c3d7f210
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""REQ-2144: a term's status is one of draft, live, deprecated and retired -- draft and live
computed, deprecated and retired asserted by a curator with a reason -- and retiring a term moves
what it holds to a successor."""

from __future__ import annotations

import datetime

import pytest
from sqlalchemy import select

from provisa.api.admin import glossary_router
from provisa.api.errors import ApiError
from provisa.core.repositories import glossary as glossary_repo
from provisa.core.schema_org import glossary_term_edges, glossary_terms
from tests.unit.test_glossary_class_grounding import (
    _ground,
    _request,
    _surface,
    _tbl,
    _with_json,
    cfg_db,  # noqa: F401  (a fixture)
    _config,
)

TODAY = datetime.date.today()


async def _status(db, term_id: int) -> str:
    async with db.acquire() as conn:
        return (await glossary_repo.term_statuses(conn))[term_id]


async def _act(action: str, term_id: int, body: dict) -> dict:
    route = getattr(glossary_router, f"{action}_term")
    request = _request() if action == "restore" else _with_json(_request(), body)
    return await route(request, term_id)


async def _note(db, ids) -> int:
    async with db.acquire() as conn:
        note = await glossary_repo.get_term_by_ref(conn, ids["customers"], "note")
    assert note is not None
    return note["id"]


async def _define(db, term_id: int, text: str = "Defined.") -> None:
    async with db.acquire() as conn:
        await glossary_repo.set_definition(conn, term_id, text)


# --- draft and live are computed ---------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_derived_term_is_a_draft_until_defined_then_live(control_plane, monkeypatch):
    async with _surface(control_plane, monkeypatch) as (db, ids, _notified):
        note = await _note(db, ids)
        assert await _status(db, note) == "draft"
        await _define(db, note, "A remark.")
        assert await _status(db, note) == "live"
        listed = {
            t["name"]: t["status"]
            for t in await glossary_router.list_terms(_request(), domains=None)
        }
        assert listed["note"] == "live" and listed["ledger"] == "draft"  # ledger grounds nothing
        assert (await glossary_router.get_term(_request(), note))["status"] == "live"


# --- deprecate --------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_deprecated_term_stays_in_service_until_its_planned_retirement(
    control_plane, monkeypatch
):
    async with _surface(control_plane, monkeypatch) as (db, ids, notified):
        note = await _note(db, ids)
        await _define(db, note)
        on = (TODAY + datetime.timedelta(days=30)).isoformat()
        done = await _act("deprecate", note, {"reason": "Merging into remark.", "retire_on": on})
        assert done["status"] == "deprecated" and notified[-1] == "ontology term deprecated"
        detail = await glossary_router.get_term(_request(), note)
        assert (detail["status_reason"], str(detail["retire_on"]), detail["status_by"]) == (
            "Merging into remark.",
            on,
            "alice",
        )
        async with db.acquire() as conn:
            assert note in await glossary_repo.live_ids(conn)  # still conducts, still admitted


@pytest.mark.asyncio
async def test_deprecation_refusals_are_named(control_plane, monkeypatch):
    async with _surface(control_plane, monkeypatch) as (db, ids, _notified):
        note = await _note(db, ids)
        with pytest.raises(ApiError) as err:
            await _act("deprecate", note, {"reason": "Old."})
        assert (err.value.code, err.value.params) == ("glossary.term_not_live", {"status": "draft"})
        await _define(db, note)
        for body, code in (
            ({}, "glossary.reason_required"),
            ({"reason": "  "}, "glossary.reason_required"),
            ({"reason": "Old.", "retire_on": "2001-01-01"}, "glossary.retire_on_past"),
            ({"reason": "Old.", "retire_on": "soon"}, "glossary.retire_on_invalid"),
        ):
            with pytest.raises(ApiError) as err:
                await _act("deprecate", note, body)
            assert err.value.code == code, body
        assert await _status(db, note) == "live"


@pytest.mark.asyncio
async def test_patch_cannot_set_a_status(control_plane, monkeypatch):
    async with _surface(control_plane, monkeypatch) as (_db, ids, _notified):
        for body in ({"retired": True}, {"status": "retired"}):
            with pytest.raises(ApiError) as err:
                await glossary_router.update_term(_with_json(_request(), body), ids["ledger"])
            assert err.value.code == "glossary.status_by_action"


# --- retire -----------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_retiring_a_property_moves_its_columns_to_the_successor(control_plane, monkeypatch):
    async with _surface(control_plane, monkeypatch) as (db, ids, _notified):
        note = await _note(db, ids)
        await _define(db, note, "A remark.")
        async with db.acquire() as conn:
            remark = await glossary_repo.create_abstract_term(
                conn, "remark", definition="What someone wrote.", domains={"sales"}
            )
        done = await _act("retire", note, {"reason": "Renamed.", "successor": remark})
        assert done["status"] == "retired"
        async with db.acquire() as conn:
            moved = await glossary_repo.get_term(conn, remark)
            kept = await glossary_repo.get_term(conn, note)
            edges = (
                await conn.execute_core(
                    select(
                        glossary_term_edges.c.from_term_id, glossary_term_edges.c.rel_type
                    ).where(glossary_term_edges.c.to_term_id == note)
                )
            ).fetchall()
        assert sorted(r["column_name"] for r in moved["refs"]) == ["id", "note", "note", "note"]
        assert kept["refs"] == [] and kept["definition"] == "A remark."  # an inert record
        assert [(e.from_term_id, e.rel_type) for e in edges] == [(remark, "REPLACES")]
        assert await _status(db, remark) == "live"


@pytest.mark.asyncio
async def test_retiring_a_class_moves_its_tables_and_their_keys(control_plane, monkeypatch):
    async with _surface(control_plane, monkeypatch) as (db, ids, _notified):
        await _ground(ids["party"], ids["customers"])
        async with db.acquire() as conn:
            client = await glossary_repo.create_abstract_term(conn, "client", domains={"sales"})
        await _act("retire", ids["party"], {"reason": "Renamed.", "successor": client})
        async with db.acquire() as conn:
            assert [t["table_id"] for t in await glossary_repo.term_tables(conn, client)] == [
                ids["customers"]
            ]
            assert (await glossary_repo.get_term_by_ref(conn, ids["customers"], "id"))[
                "name"
            ] == "client"
            assert await glossary_repo.term_tables(conn, ids["party"]) == []


@pytest.mark.asyncio
async def test_retirement_refusals_are_named(control_plane, monkeypatch):
    async with _surface(control_plane, monkeypatch) as (db, ids, _notified):
        note = await _note(db, ids)
        await _ground(ids["party"], ids["customers"])
        async with db.acquire() as conn:
            spent = await glossary_repo.create_abstract_term(conn, "spent", domains={"sales"})
            await glossary_repo.retire_term(conn, spent, reason="Unused.", successor=None, by=None)
        for body, code in (
            ({"reason": "Old."}, "glossary.successor_required"),
            ({"successor": ids["ledger"]}, "glossary.reason_required"),
            ({"reason": "Old.", "successor": note}, "glossary.successor_is_self"),
            ({"reason": "Old.", "successor": spent}, "glossary.successor_retired"),
            # a Property's successor is a Property or a Concept, never a Class
            ({"reason": "Old.", "successor": ids["party"]}, "glossary.successor_role"),
        ):
            with pytest.raises(ApiError) as err:
                await _act("retire", note, body)
            assert err.value.code == code, body
        assert await _status(db, note) == "draft"
        # a term with nothing attached retires with a reason alone
        assert (await _act("retire", ids["ledger"], {"reason": "Unused."}))["status"] == "retired"


@pytest.mark.asyncio
async def test_a_tag_loss_refused_undoes_the_retirement(control_plane, monkeypatch):
    """Retire's move is checked by the tag move check (term_tags); its refusal rolls the move
    and the retirement back together."""
    from provisa.core import term_tags

    async def refuse(*_a, **_k):
        raise term_tags.TagLossRefused(
            [{"column": "note", "table": "__derived__.s.customers", "loses": ["pii"], "gains": []}]
        )

    async with _surface(control_plane, monkeypatch) as (db, ids, _notified):
        note = await _note(db, ids)
        async with db.acquire() as conn:
            remark = await glossary_repo.create_abstract_term(conn, "remark", domains={"sales"})
        monkeypatch.setattr(term_tags, "tag_changes_of_moves", refuse)
        with pytest.raises(ApiError) as err:
            await _act("retire", note, {"reason": "Renamed.", "successor": remark})
        assert (err.value.status_code, err.value.code) == (409, "tag.move_loses")
        assert err.value.params["columns"][0]["loses"] == ["pii"]
        assert await _status(db, note) == "draft"
        async with db.acquire() as conn:
            assert (await glossary_repo.get_term_by_ref(conn, ids["customers"], "note"))[
                "name"
            ] == "note"


# --- restore ----------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_restore_brings_back_an_empty_draft_and_clears_a_deprecation(
    control_plane, monkeypatch
):
    async with _surface(control_plane, monkeypatch) as (db, ids, _notified):
        note = await _note(db, ids)
        await _define(db, note, "A remark.")
        async with db.acquire() as conn:
            remark = await glossary_repo.create_abstract_term(
                conn, "remark", definition="What someone wrote.", domains={"sales"}
            )
        await _act("retire", note, {"reason": "Renamed.", "successor": remark})
        assert (await _act("restore", note, {}))["status"] == "draft"
        detail = await glossary_router.get_term(_request(), note)
        assert detail["refs"] == [] and detail["definition"] == "A remark."
        assert detail["status_reason"] is None
        await _define(db, ids["ledger"])
        async with db.acquire() as conn:
            await glossary_repo.add_edge(conn, ids["ledger"], remark, "RELATED_TO")
        await _act("deprecate", ids["ledger"], {"reason": "Old."})
        assert (await _act("restore", ids["ledger"], {}))["status"] == "live"


# --- scheduled retirement ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_the_scheduler_retires_a_deprecated_term_on_its_date(control_plane, monkeypatch):
    from provisa.scheduler.planned_retirement import retire_due_terms

    async with _surface(control_plane, monkeypatch) as (db, ids, _notified):
        note = await _note(db, ids)
        await _define(db, ids["ledger"])
        await _define(db, note)
        async with db.acquire() as conn:
            remark = await glossary_repo.create_abstract_term(
                conn, "remark", definition="What someone wrote.", domains={"sales"}
            )
            await glossary_repo.add_edge(conn, ids["ledger"], remark, "RELATED_TO")
            await glossary_repo.add_edge(conn, remark, note, "RELATED_TO")
            on = TODAY + datetime.timedelta(days=2)
            for term in (ids["ledger"], note):
                await glossary_repo.deprecate_term(
                    conn, term, reason="Old.", retire_on=on, by=None, today=TODAY
                )
        assert await retire_due_terms(db, TODAY) == []  # not yet
        # due: the Concept retires; the Property, still holding columns, waits for a successor
        assert await retire_due_terms(db, on) == [ids["ledger"]]
        assert await _status(db, ids["ledger"]) == "retired"
        assert await _status(db, note) == "deprecated"
        async with db.acquire() as conn:
            row = (
                await conn.execute_core(
                    select(glossary_terms.c.status_reason).where(
                        glossary_terms.c.id == ids["ledger"]
                    )
                )
            ).fetchone()
        assert row.status_reason == "Old."


def test_each_orgs_scheduler_runs_the_retirement_daily():
    from apscheduler.schedulers.asyncio import AsyncIOScheduler

    from provisa.scheduler import planned_retirement

    scheduler = AsyncIOScheduler()
    planned_retirement.register_org(scheduler, "acme")
    planned_retirement.register_org(scheduler, "acme")  # kept, not doubled
    [job] = scheduler.get_jobs()
    assert job.id == "planned_retirement:org_acme" and job.args == ("acme",)


# --- the export snapshot, the meta view, config -----------------------------------------------


def test_the_export_carries_the_status():
    from provisa.api.metadata_export.builder import _glossary_assets

    orders = _tbl("orders", keyed=True)
    term = {
        "id": 1,
        "name": "booking",
        "definition": "A sale.",
        "is_abstract": True,
        "deprecated": False,
        "deprecation_asserted": True,
        "status_reason": "Use sale.",
        "retire_on": datetime.date(2027, 1, 1),
        "retired": False,
    }
    glossary = {
        "terms": [term],
        "refs": [],
        "table_refs": [
            {"term_id": 1, "source_id": "__derived__", "schema_name": "s", "table_name": "orders"}
        ],
        "edges": [],
        "experts": [],
    }
    [asset], _edges = _glossary_assets(glossary, [orders], set(), "acme")
    assert (asset.status, asset.status_reason, asset.retire_on) == (
        "deprecated",
        "Use sale.",
        "2027-01-01",
    )


@pytest.mark.asyncio
async def test_the_meta_view_states_the_status(control_plane, monkeypatch):
    from provisa.api._meta_views import _META_TABLE_VIEWS

    async with _surface(control_plane, monkeypatch) as (db, ids, _notified):
        note = await _note(db, ids)
        await _define(db, note)
        async with db.acquire() as conn:
            await glossary_repo.deprecate_term(
                conn, note, reason="Old.", retire_on=None, by=None, today=TODAY
            )
            await conn.execute(_META_TABLE_VIEWS["glossary_term_domains"])
            await conn.execute(_META_TABLE_VIEWS["glossary_terms"])
            rows = {
                r[0]: (r[1], r[2])
                for r in await conn.fetch(
                    "SELECT name, status, status_reason FROM glossary_terms_meta"
                )
            }
        assert rows["note"] == ("deprecated", "Old.")
        assert rows["ledger"] == ("draft", None)


@pytest.mark.asyncio
async def test_a_config_states_a_terms_status_and_the_store_exports_it(cfg_db):  # noqa: F811
    from provisa.core.config_loader import apply_config, parse_config_dict
    from provisa.core.store_config import store_model

    def config(**status):
        base = _config(class_term=None).model_dump(by_alias=True, exclude_none=True)
        base["glossary_terms"][0] = {**base["glossary_terms"][0], **status}
        base["glossary_terms"][0]["edges"] = [{"to": "order", "rel_type": "RELATED_TO"}]
        return parse_config_dict(base)

    async with cfg_db.acquire() as conn:
        await apply_config(config(deprecate={"reason": "Use sale."}), conn)
        [booking] = [t for t in await glossary_repo.list_terms(conn) if t["name"] == "booking"]
        assert booking["status"] == "deprecated"
        [stated] = [
            t for t in (await store_model(conn))["glossary_terms"] if t["name"] == "booking"
        ]
        assert stated["deprecate"] == {"reason": "Use sale."}
        await apply_config(config(deprecate={"reason": "Use sale."}), conn)  # changes nothing
        await apply_config(config(restore=True), conn)
        [booking] = [t for t in await glossary_repo.list_terms(conn) if t["name"] == "booking"]
        assert booking["status"] == "live"
        await apply_config(config(retire={"reason": "Gone.", "successor": "sale"}), conn)
        [booking] = [t for t in await glossary_repo.list_terms(conn) if t["name"] == "booking"]
        assert booking["status"] == "retired"


@pytest.mark.asyncio
async def test_a_config_deprecation_dated_in_the_past_is_refused(cfg_db):  # noqa: F811
    from provisa.core.config_loader import apply_config, parse_config_dict

    base = _config(class_term=None).model_dump(by_alias=True, exclude_none=True)
    base["glossary_terms"][0] = {
        **base["glossary_terms"][0],
        "edges": [{"to": "order", "rel_type": "RELATED_TO"}],
        "deprecate": {"reason": "Use sale.", "retire_on": "2001-01-01"},
    }
    async with cfg_db.acquire() as conn:
        with pytest.raises(ValueError, match="retirement date 2001-01-01 is in the past"):
            await apply_config(parse_config_dict(base), conn)


def test_a_config_states_one_status_action_at_most():
    from provisa.core.models import GlossaryTermConfig

    with pytest.raises(ValueError, match="more than one of deprecate, retire and restore"):
        GlossaryTermConfig.model_validate(
            {"name": "x", "domains": ["d"], "restore": True, "retire": {"reason": "Gone."}}
        )
    with pytest.raises(ValueError):
        GlossaryTermConfig.model_validate({"name": "x", "domains": ["d"], "retire": {"reason": ""}})


# --- applied history --------------------------------------------------------------------------


def test_applied_history_reads_the_commit_before_the_retirement(monkeypatch):
    from provisa.core import env_files, env_repo, term_history

    before = env_files.dump(
        {
            "domains/sales/tables/orders.yaml": {
                "table_name": "orders",
                "glossary": [{"column_name": "note", "term": "terms/note.yaml"}],
                "class_term": [{"term": "terms/note.yaml"}],
            },
            "domains/sales/tables/lines.yaml": {
                "table_name": "lines",
                "glossary": [{"column_name": "memo", "term": "terms/remark.yaml"}],
            },
        }
    )
    monkeypatch.setattr(
        env_repo,
        "history",
        lambda org, ref, limit: [
            {"sha": "c3", "message": "upsert table orders"},
            {"sha": "c2", "message": "boot\n\n- retire glossary term 7\n- add edge"},
            {"sha": "c1", "message": "upsert table lines"},
        ],
    )
    monkeypatch.setattr(env_repo, "parent_of", lambda org, sha: {"c2": "c1"}[sha])
    monkeypatch.setattr(env_repo, "files_at", lambda org, ref: before)
    monkeypatch.setattr(env_files, "kind_path", lambda kind_dir, name: f"{kind_dir}/{name}.yaml")
    found = term_history.applied_history("acme", "prod", 7, "note")
    assert found == {
        "retired_in": "c2",
        "as_of": "c1",
        "columns": [{"table": "domains/sales/tables/orders.yaml", "column": "note"}],
        "tables": ["domains/sales/tables/orders.yaml"],
    }
    assert term_history.applied_history("acme", "prod", 8, "other") is None
