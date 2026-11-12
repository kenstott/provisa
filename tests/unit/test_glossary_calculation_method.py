# Copyright (c) 2026 Kenneth Stott
# Canary: 4e9a1c37-6b2d-4f80-a3e5-1d7c9b2f0a64
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2243: a glossary term carries its calculation method beside its definition, saved, read
back and versioned with the term exactly as the definition is."""

# Requirements: REQ-2243

from __future__ import annotations

import pytest

from provisa.api.admin import glossary_router
from provisa.core.env_project import project
from provisa.core.repositories import glossary as glossary_repo
from tests.unit.gate_identity import serve_every_column
from tests.unit.test_glossary_class_grounding import (
    SCHEMA,
    _config,
    _request,
    _surface,
    _with_json,
    cfg_db,  # noqa: F401  (a fixture)
)

METHOD = "Gross order value less refunds and discounts, on the order date."


@pytest.fixture(autouse=True)
def _served(monkeypatch):
    serve_every_column(monkeypatch)


@pytest.mark.asyncio
async def test_the_method_saves_and_reads_back_through_the_term_api(control_plane, monkeypatch):
    async with _surface(control_plane, monkeypatch) as (db, ids, notified):
        term_id = ids["party"]
        await glossary_router.update_term(
            _with_json(_request(), {"calculation_method": METHOD}), term_id
        )
        assert notified[-1] == "ontology term updated"
        detail = await glossary_router.get_term(_request(), term_id)
        assert detail["calculation_method"] == METHOD
        listed = {t["id"]: t for t in await glossary_router.list_terms(_request(), domains=None)}
        assert listed[term_id]["calculation_method"] == METHOD
        await glossary_router.update_term(
            _with_json(_request(), {"calculation_method": None}), term_id
        )
        assert (await glossary_router.get_term(_request(), term_id))["calculation_method"] is None


@pytest.mark.asyncio
async def test_the_method_is_versioned_with_the_term(control_plane, monkeypatch):
    """The model store's projection of the term -- what each commit records -- carries it."""
    async with _surface(control_plane, monkeypatch) as (db, ids, _notified):
        async with db.acquire() as conn:
            assert await glossary_repo.set_calculation_method(conn, ids["party"], METHOD)
            tree = await project(conn, SCHEMA)
        [term] = [b for b in tree.values() if b.get("name") == "party"]
        assert term["calculation_method"] == METHOD


@pytest.mark.asyncio
async def test_a_config_states_the_method_and_the_store_exports_it(cfg_db):  # noqa: F811
    from provisa.core.config_loader import apply_config, parse_config_dict
    from provisa.core.store_config import store_model

    base = _config(class_term=None).model_dump(by_alias=True, exclude_none=True)
    base["glossary_terms"][0] = {
        **base["glossary_terms"][0],
        "definition": "A sale.",
        "calculation_method": METHOD,
        "approve": True,
    }
    async with cfg_db.acquire() as conn:
        await apply_config(parse_config_dict(base), conn)
        [booking] = [t for t in await glossary_repo.list_terms(conn) if t["name"] == "booking"]
        assert (booking["calculation_method"], booking["status"]) == (METHOD, "approved")
        [stated] = [
            t for t in (await store_model(conn))["glossary_terms"] if t["name"] == "booking"
        ]
        assert stated["calculation_method"] == METHOD


@pytest.mark.asyncio
async def test_the_meta_view_reads_the_method(control_plane, monkeypatch):
    """The catalog's terms view -- what SQL and GraphQL read a term through -- carries it."""
    from provisa.api._ontology_views import ONTOLOGY_TABLE_VIEWS

    async with _surface(control_plane, monkeypatch) as (db, ids, _notified):
        async with db.acquire() as conn:
            await glossary_repo.set_calculation_method(conn, ids["party"], METHOD)
            await conn.execute(ONTOLOGY_TABLE_VIEWS["_domains"])
            await conn.execute(ONTOLOGY_TABLE_VIEWS["_terms"])
            rows = dict(await conn.fetch("SELECT name, calculation_method FROM _terms_ontology"))
        assert rows["party"] == METHOD
