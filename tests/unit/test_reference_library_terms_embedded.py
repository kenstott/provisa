# Copyright (c) 2026 Kenneth Stott
# Canary: 5d0c7a1e-9b3f-4e62-8c41-2f7a9e6d1b83
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2185 (TERMS ARE EMBEDDED AS THEY CHANGE): an in-service glossary term is embedded when it
enters service and when its name or definition changes, and loses its embedding when it leaves
service, in the background of the save. Every path that changes a term's service state, name or
definition calls the one trigger (reference_library_router.embed_terms) with the org; an org with
no vector model schedules nothing; a failing embed is logged and does not fail the save."""

# Requirements: REQ-2185

from __future__ import annotations

import asyncio
import concurrent.futures
import logging
import types
from pathlib import Path
from typing import Any, cast

import pytest
from sqlalchemy import select

from provisa.api.admin import environments_router, glossary_router
from provisa.api.admin import reference_library_router as library_api
from provisa.core import connection_loop
from provisa.core.repositories import glossary as glossary_repo
from provisa.core.request_context import current_env, current_org
from provisa.core.schema_org import glossary_term_vectors
from provisa.reference_library import library
from tests.unit.gate_identity import serve_every_column
from tests.unit.test_glossary_class_grounding import (
    _request,
    _surface,
    _with_json,
    cfg_db,  # noqa: F401  (a fixture)
)

ORG = "acme"  # the org _request is bound to


def record_embeds(monkeypatch) -> list[str]:
    """The orgs the one trigger is called with, in order; nothing is embedded."""
    called: list[str] = []

    async def embed(org_id: str) -> None:
        called.append(org_id)

    monkeypatch.setattr(library_api, "embed_terms", embed)
    return called


@pytest.fixture(autouse=True)
def _served(monkeypatch):
    serve_every_column(monkeypatch)


calls: list[list[str]] = []


async def _embed(texts: list[str]) -> list[list[float]]:
    calls.append(list(texts))
    return [[float(len(t)), 1.0, 0.5] for t in texts]


EMBEDDER = library.Embedder(model="test-embed", embed=_embed)


async def _held(db) -> set[int]:
    async with db.acquire() as conn:
        rows = (await conn.execute_core(select(glossary_term_vectors.c.term_id))).fetchall()
    return {r.term_id for r in rows}


# -- each path that changes a term calls the trigger with the org ------------------------------


async def test_creating_a_term_embeds_it(control_plane, monkeypatch):
    async with _surface(control_plane, monkeypatch) as (_db, _ids, _notified):
        embedded = record_embeds(monkeypatch)
        await glossary_router.create_abstract_term(
            _with_json(_request(), {"name": "vault", "domains": ["sales"]})
        )
        assert embedded == [ORG]


async def test_renaming_or_redefining_a_term_embeds_it_and_no_other_edit_does(
    control_plane, monkeypatch
):
    async with _surface(control_plane, monkeypatch) as (_db, ids, _notified):
        embedded = record_embeds(monkeypatch)
        party = ids["party"]
        await glossary_router.update_term(_with_json(_request(), {"name": "counterparty"}), party)
        assert embedded == [ORG]
        await glossary_router.update_term(
            _with_json(_request(), {"definition": "One side of an agreement."}), party
        )
        assert embedded == [ORG, ORG]
        await glossary_router.update_term(_with_json(_request(), {"export_excluded": True}), party)
        assert embedded == [ORG, ORG]


async def test_each_change_of_service_embeds(control_plane, monkeypatch):
    async with _surface(control_plane, monkeypatch) as (db, ids, _notified):
        embedded = record_embeds(monkeypatch)
        ledger, party = ids["ledger"], ids["party"]
        async with db.acquire() as conn:
            await glossary_repo.set_definition(conn, ledger, "The books.")
        await glossary_router.approve_term(_request(), ledger)
        assert embedded == [ORG]
        await glossary_router.deprecate_term(_with_json(_request(), {"reason": "Old."}), ledger)
        assert embedded == [ORG] * 2
        await glossary_router.restore_term(_request(), ledger)
        assert embedded == [ORG] * 3
        await glossary_router.retire_term(_with_json(_request(), {"reason": "Gone."}), ledger)
        assert embedded == [ORG] * 4
        await glossary_router.delete_term(_request(), party)
        assert embedded == [ORG] * 5


async def test_a_bulk_approval_embeds(control_plane, monkeypatch):
    async with _surface(control_plane, monkeypatch) as (db, ids, _notified):
        async with db.acquire() as conn:
            note = await glossary_repo.get_term_by_ref(conn, ids["customers"], "note")
            assert note is not None
            await glossary_repo.set_definition(conn, note["id"], "A remark.")
        embedded = record_embeds(monkeypatch)
        await glossary_router.bulk_approve(_with_json(_request(), {"term_ids": [note["id"]]}))
        assert embedded == [ORG]


async def test_the_assistant_s_glossary_tools_embed(control_plane, monkeypatch):
    from provisa.api.mcp import tools

    async with _surface(control_plane, monkeypatch) as (db, ids, _notified):
        embedded = record_embeds(monkeypatch)
        monkeypatch.setattr(tools, "require_role", lambda role, state: role)
        state = types.SimpleNamespace(model_db=db)
        created = await tools.create_glossary_term(
            state, "steward", _request(), "vault", domains=["sales"]
        )
        assert embedded == [ORG]
        await tools.update_glossary_term(
            state, "steward", _request(), created["id"], definition="Where it is kept."
        )
        assert embedded == [ORG] * 2
        await tools.update_glossary_term(
            state, "steward", _request(), created["id"], export_excluded=True
        )
        assert embedded == [ORG] * 2
        await tools.change_glossary_term_status(
            state, "steward", _request(), created["id"], "approve", {}
        )
        assert embedded == [ORG] * 3
        await tools.delete_glossary_term(state, "steward", _request(), ids["party"])
        assert embedded == [ORG] * 4


async def test_a_configuration_applied_embeds(monkeypatch):
    from provisa.api import app, app_loaders, startup_seed
    from provisa.core import config_loader, secrets_store

    embedded = record_embeds(monkeypatch)

    async def nothing(*args: Any, **kwargs: Any) -> None:
        return None

    class _Pool:
        def acquire(self):
            return self

        async def __aenter__(self):
            return None

        async def __aexit__(self, *exc):
            return None

    class _Bound:
        async def __aenter__(self):
            return None

        async def __aexit__(self, *exc):
            return None

    monkeypatch.setattr(
        app, "state", types.SimpleNamespace(model_db=_Pool(), federation_engine=None)
    )
    monkeypatch.setattr(app, "_rebuild_schemas", nothing)
    monkeypatch.setattr(startup_seed, "_resolve_pk_from_sources", nothing)
    monkeypatch.setattr(config_loader, "apply_config", nothing)
    monkeypatch.setattr(config_loader, "register_sources", lambda *a, **k: None)
    monkeypatch.setattr(secrets_store, "bound_to_request_org", _Bound)
    monkeypatch.setattr(app_loaders, "_populate_source_catalog_names", lambda config: None)
    monkeypatch.setattr(app_loaders, "_build_source_pools_and_enums", nothing)
    token = current_org.set(ORG)
    try:
        await app_loaders.apply_configuration(cast(Any, types.SimpleNamespace(sources=[])))
    finally:
        current_org.reset(token)
    assert embedded == [ORG]


@pytest.mark.parametrize("cached", [None, "runtime"])
async def test_a_load_into_an_environment_embeds_there(monkeypatch, cached):
    """Deploy, merge, undo, redo, pull and an approved merge request each end in _refresh."""
    seen: list[tuple[str, str | None, str | None]] = []

    async def embed(org_id: str) -> None:
        seen.append((org_id, current_org.get(), current_env.get()))

    monkeypatch.setattr(library_api, "embed_terms", embed)
    registry = types.SimpleNamespace(get=lambda key: cached, invalidate=lambda key: None)
    monkeypatch.setattr(
        environments_router, "_state", lambda: types.SimpleNamespace(org_registry=registry)
    )
    await environments_router._refresh(ORG, "dev", connectivity=True)
    assert seen == [(ORG, ORG, "dev")]
    source = Path(environments_router.__file__).read_text(encoding="utf-8")
    for route in (
        "merge_into_environment",
        "deploy_into_environment",
        "_move",
        "decide_merge_request",
        "pull_environment",
    ):
        body = source.split(f"def {route}(", 1)[1].split("\n@", 1)[0]
        assert "await _refresh(" in body, route


# -- what the trigger does ---------------------------------------------------------------------


@pytest.fixture
def background(monkeypatch) -> list[Any]:
    """What the trigger hands the background worker, run by the test instead."""
    spawned: list[Any] = []

    def spawn(coro, *, name=None):
        spawned.append((coro, name))
        done: concurrent.futures.Future[None] = concurrent.futures.Future()
        done.set_result(None)
        return done

    monkeypatch.setattr(library_api, "spawn_background", spawn)
    return spawned


def _vector_model(monkeypatch, db, model: str | None) -> None:
    async def current() -> str | None:
        return model

    async def embedder() -> library.Embedder:
        return EMBEDDER

    monkeypatch.setattr(library_api, "_current_model", current)
    monkeypatch.setattr(library_api, "_embedder", embedder)
    monkeypatch.setattr(library_api, "_pool", lambda: db)


async def test_an_org_without_a_vector_model_schedules_nothing(
    control_plane, monkeypatch, background
):
    async with _surface(control_plane, monkeypatch) as (db, _ids, _notified):
        _vector_model(monkeypatch, db, None)
        await glossary_router.create_abstract_term(
            _with_json(_request(), {"name": "vault", "domains": ["sales"]})
        )
        assert background == []
        assert await _held(db) == set()


async def test_a_term_is_embedded_as_it_changes_and_loses_its_embedding_leaving_service(
    control_plane, monkeypatch, background
):
    async with _surface(control_plane, monkeypatch) as (db, ids, _notified):
        _vector_model(monkeypatch, db, "test-embed")
        created = await glossary_router.create_abstract_term(
            _with_json(_request(), {"name": "vault", "domains": ["sales"]})
        )
        ((work, name),) = background
        assert name == f"glossary term embeddings of {ORG}"
        await work
        vault = created["id"]
        assert vault in await _held(db)

        background.clear()
        calls.clear()
        await glossary_router.update_term(
            _with_json(_request(), {"definition": "Where it is kept."}), vault
        )
        for work, _name in background:
            await work
        assert calls == [["vault\nWhere it is kept."]]  # only the changed term

        background.clear()
        await glossary_router.retire_term(_with_json(_request(), {"reason": "Gone."}), vault)
        for work, _name in background:
            await work
        held = await _held(db)
        assert vault not in held
        assert ids["party"] in held  # in service, so still embedded


async def test_a_failing_embed_does_not_fail_the_save_and_is_logged(
    control_plane, monkeypatch, caplog
):
    async with _surface(control_plane, monkeypatch) as (db, _ids, _notified):
        _vector_model(monkeypatch, db, "test-embed")

        async def down() -> library.Embedder:
            raise RuntimeError("the vector model is down")

        monkeypatch.setattr(library_api, "_embedder", down)
        futures: list[concurrent.futures.Future[None]] = []
        real = connection_loop.spawn_background

        def spawn(coro, *, name=None):
            futures.append(real(coro, name=name))
            return futures[-1]

        monkeypatch.setattr(library_api, "spawn_background", spawn)
        with caplog.at_level(logging.ERROR, logger="provisa.core.connection_loop"):
            created = await glossary_router.create_abstract_term(
                _with_json(_request(), {"name": "vault", "domains": ["sales"]})
            )
            assert created["id"]  # the save is answered
            (future,) = futures
            await asyncio.wrap_future(future)
        (failed,) = [
            r for r in caplog.records if r.getMessage().endswith(f"embeddings of {ORG} failed")
        ]
        assert failed.getMessage() == f"background task glossary term embeddings of {ORG} failed"
        assert failed.exc_info is not None
        assert str(failed.exc_info[1]) == "the vector model is down"
