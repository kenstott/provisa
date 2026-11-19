# Copyright (c) 2026 Kenneth Stott
# Canary: 6b1e9f3a-27d4-4c85-9a60-d3f2b8e14c77
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1591 (amended 2026-11-18): a term's name is unique within a domain, not across domains.

Two terms may carry the same name when they belong to different domains. Creating or renaming a
term is refused only when a term of that name already belongs to one of its domains, and the
refusal names that domain. Everywhere a term is found by name it is told apart by its domain.
"""

# Requirements: REQ-1591

from __future__ import annotations

import types
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, cast
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import insert

from provisa.api.admin import glossary_router
from provisa.api.errors import ApiError
from provisa.core.models import Column, Table
from provisa.core.repositories import glossary as glossary_repo
from provisa.core.repositories import table as table_repo
from provisa.core.schema_org import domains, sources

if TYPE_CHECKING:
    from fastapi import Request

pytestmark = pytest.mark.asyncio


@asynccontextmanager
async def _conn(control_plane):
    with control_plane.engine().begin() as c:
        c.exec_driver_sql('SET LOCAL search_path TO "org_default"')
        for d in ("sales", "pet-store", "hr"):
            c.execute(insert(domains).values(id=d, description=""))
        c.execute(insert(sources).values(id="__derived__", type="postgresql"))
    async with control_plane.org().acquire() as conn:
        yield conn


def _tbl(domain: str, name: str, columns: list[str]) -> Table:
    return Table(
        source_id="__derived__",
        domain_id=domain,
        schema_name="s",
        table_name=name,
        columns=[Column(name=c, data_type="text", visible_to=[]) for c in columns],
        view_sql="SELECT 1",
    )


async def test_a_name_held_in_another_domain_is_created(control_plane):
    async with _conn(control_plane) as conn:
        sales = await glossary_repo.create_abstract_term(conn, "client", domains={"sales"})
        pets = await glossary_repo.create_abstract_term(conn, "Client", domains={"pet-store"})
        assert sales != pets
        assert await glossary_repo.terms_named(conn, "client") == {
            sales: {"sales"},
            pets: {"pet-store"},
        }


async def test_a_name_held_in_the_same_domain_is_refused_naming_the_domain(control_plane):
    async with _conn(control_plane) as conn:
        await glossary_repo.create_abstract_term(conn, "client", domains={"sales"})
        with pytest.raises(glossary_repo.TermNameTaken) as err:
            await glossary_repo.create_abstract_term(conn, "client", domains={"hr", "sales"})
        assert err.value.domain == "sales"
        assert "'sales'" in str(err.value)


async def test_an_unscoped_name_collides_with_every_domain(control_plane):
    # A term with no domains is reachable from every domain, so it shares one with any term.
    async with _conn(control_plane) as conn:
        await glossary_repo.create_abstract_term(conn, "party", domains=set())
        with pytest.raises(glossary_repo.TermNameTaken):
            await glossary_repo.create_abstract_term(conn, "party", domains={"sales"})
        await glossary_repo.create_abstract_term(conn, "actor", domains={"sales"})
        with pytest.raises(glossary_repo.TermNameTaken):
            await glossary_repo.create_abstract_term(conn, "actor", domains=set())


async def test_a_name_held_by_a_derived_term_is_refused_in_its_tables_domain(control_plane):
    async with _conn(control_plane) as conn:
        await table_repo.upsert(conn, _tbl("sales", "orders", ["order_dt"]))
        with pytest.raises(glossary_repo.TermNameTaken) as err:
            await glossary_repo.create_abstract_term(conn, "order date", domains={"sales"})
        assert err.value.domain == "sales"
        assert await glossary_repo.create_abstract_term(conn, "order date", domains={"hr"})


async def test_rename_is_refused_only_within_a_shared_domain(control_plane):
    async with _conn(control_plane) as conn:
        await glossary_repo.create_abstract_term(conn, "client", domains={"sales"})
        buyer = await glossary_repo.create_abstract_term(conn, "buyer", domains={"sales"})
        patron = await glossary_repo.create_abstract_term(conn, "patron", domains={"pet-store"})
        with pytest.raises(glossary_repo.TermNameTaken) as err:
            await glossary_repo.rename_term(conn, buyer, "Client")
        assert err.value.domain == "sales"
        assert await glossary_repo.rename_term(conn, patron, "client") is True
        # renaming a term to its own name is no collision
        assert await glossary_repo.rename_term(conn, buyer, "buyer") is True


async def test_resolve_tells_same_named_terms_apart_by_domain(control_plane):
    async with _conn(control_plane) as conn:
        sales = await glossary_repo.create_abstract_term(conn, "client", domains={"sales"})
        pets = await glossary_repo.create_abstract_term(conn, "client", domains={"pet-store"})
        party = await glossary_repo.create_abstract_term(conn, "party", domains=set())
        assert await glossary_repo.resolve_term(conn, "client", domains={"sales"}) == sales
        assert await glossary_repo.resolve_term(conn, "client", domains={"pet-store"}) == pets
        assert await glossary_repo.resolve_term(conn, "client", domains={"hr"}) is None
        # an unscoped term is reachable from every domain
        assert await glossary_repo.resolve_term(conn, "party", domains={"hr"}) == party
        with pytest.raises(glossary_repo.TermNameAmbiguous) as err:
            await glossary_repo.resolve_term(conn, "client", domains=None)
        assert err.value.domains == ["pet-store", "sales"]


async def test_a_derived_ref_joins_the_same_named_term_of_its_own_domain(control_plane):
    async with _conn(control_plane) as conn:
        sales = await glossary_repo.create_abstract_term(conn, "client", domains={"sales"})
        pets = await glossary_repo.create_abstract_term(conn, "client", domains={"pet-store"})
        await table_repo.upsert(conn, _tbl("pet-store", "visits", ["client"]))
        detail = await glossary_repo.get_term(conn, pets)
        assert detail is not None and [r["column_name"] for r in detail["refs"]] == ["client"]
        untouched = await glossary_repo.get_term(conn, sales)
        assert untouched is not None and untouched["refs"] == []


async def test_a_derived_ref_creates_its_own_term_beside_several_elsewhere(control_plane):
    async with _conn(control_plane) as conn:
        sales = await glossary_repo.create_abstract_term(conn, "client", domains={"sales"})
        pets = await glossary_repo.create_abstract_term(conn, "client", domains={"pet-store"})
        await table_repo.upsert(conn, _tbl("hr", "staff", ["client"]))
        named = await glossary_repo.terms_named(conn, "client")
        (hr,) = set(named) - {sales, pets}
        assert named[hr] == {"hr"}


async def test_a_declared_term_upserts_the_term_of_its_own_domain(control_plane):
    async with _conn(control_plane) as conn:
        sales = await glossary_repo.create_abstract_term(conn, "client", domains={"sales"})
        pets = await glossary_repo.upsert_declared_term(
            conn, "client", definition="A pet owner.", domains={"pet-store"}
        )
        assert pets != sales
        again = await glossary_repo.upsert_declared_term(
            conn, "client", definition="Owner of a pet.", domains={"pet-store"}
        )
        assert again == pets
        kept = await glossary_repo.get_term(conn, sales)
        assert kept is not None and kept["definition"] is None


def _request() -> "Request":
    async def _json():
        return {}

    fake = types.SimpleNamespace(
        headers={},
        state=types.SimpleNamespace(
            identity=types.SimpleNamespace(user_id="alice", roles=[], acting_roles=[]),
            active_org_id="acme",
        ),
    )
    return cast("Request", fake)


def _with_json(body: dict) -> "Request":
    request = _request()

    async def _json():
        return body

    cast(Any, request).json = _json
    return request


@asynccontextmanager
async def _router(control_plane, monkeypatch):
    async with _conn(control_plane):
        pass
    db = control_plane.org()

    async def _pool():
        return db

    monkeypatch.setattr(glossary_router, "_pool", _pool)
    monkeypatch.setattr(glossary_router, "_notify", AsyncMock())
    monkeypatch.setattr("provisa.api.admin.reference_library_router.embed_terms", AsyncMock())
    # Which right and which domain authority each endpoint names is asserted in
    # test_glossary_rights.py and test_glossary_stewardship.py; here they must not block.
    monkeypatch.setattr(glossary_router, "_require_glossary_rw", lambda request: None)
    monkeypatch.setattr(glossary_router, "_authority", lambda request: None)
    monkeypatch.setattr(glossary_router, "_curate_authority", lambda request: None)
    yield db


async def test_the_router_creates_across_domains_and_refuses_within_one(control_plane, monkeypatch):
    async with _router(control_plane, monkeypatch):
        first = await glossary_router.create_abstract_term(
            _with_json({"name": "client", "domains": ["sales"]})
        )
        second = await glossary_router.create_abstract_term(
            _with_json({"name": "client", "domains": ["pet-store"]})
        )
        assert first["id"] != second["id"]
        with pytest.raises(ApiError) as err:
            await glossary_router.create_abstract_term(
                _with_json({"name": "client", "domains": ["sales"]})
            )
        assert err.value.status_code == 409
        assert err.value.code == "glossary.name_taken"
        assert err.value.params == {"name": "client", "domain": "sales"}


async def test_the_router_refuses_a_rename_or_rescope_into_a_domain_holding_the_name(
    control_plane, monkeypatch
):
    async with _router(control_plane, monkeypatch):
        await glossary_router.create_abstract_term(
            _with_json({"name": "client", "domains": ["sales"]})
        )
        patron = (
            await glossary_router.create_abstract_term(
                _with_json({"name": "client", "domains": ["pet-store"]})
            )
        )["id"]
        with pytest.raises(ApiError) as err:
            await glossary_router.update_term(_with_json({"domains": ["sales"]}), patron)
        assert err.value.code == "glossary.name_taken"
        assert err.value.params["domain"] == "sales"
        buyer = (
            await glossary_router.create_abstract_term(
                _with_json({"name": "buyer", "domains": ["sales"]})
            )
        )["id"]
        with pytest.raises(ApiError) as err:
            await glossary_router.update_term(_with_json({"name": "client"}), buyer)
        assert err.value.code == "glossary.name_taken"
