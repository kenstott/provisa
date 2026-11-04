# Copyright (c) 2026 Kenneth Stott
# Canary: 5a1d7e92-c84b-4f06-9e3a-b26f0d81c7e5
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2189: a saved ERD layout is stored in the model, holds only the arrangement, and is read
under the caller's read rights.

Places are held by id, so a renamed table keeps its own; a deleted table goes from the layout with
no error. A reader is answered only the places of what it reads, and its save keeps the places of
what it cannot see. The layout is written into the environment's tree with its tables as paths,
and a deploy of that tree gives them back their ids.
"""

# Requirements: REQ-2189

from __future__ import annotations

import types
from contextlib import asynccontextmanager
from typing import Any, cast

import pytest
from sqlalchemy import insert, select, update

from provisa.api.admin import capabilities as capmod
from provisa.api.admin import erd_layouts_router as router_mod
from provisa.api.errors import ApiError
from provisa.core.env_deploy import _decompose, _existing_ids, _roles_after
from provisa.core.env_project import project
from provisa.core.models import Column, Table
from provisa.core.repositories import table as table_repo
from provisa.core.repositories.erd_layout import Readable
from provisa.core.schema_org import domains, registered_tables, sources

SCHEMA = "org_default"
EVERYTHING = Readable(table=lambda table_id: True, domain=lambda domain_id: True)


def _request() -> Any:
    return types.SimpleNamespace(
        state=types.SimpleNamespace(
            identity=types.SimpleNamespace(user_id="sam", roles=[], acting_roles=[]),
            active_org_id="acme",
        )
    )


def _table(name: str, domain: str) -> Table:
    return Table(
        source_id="__derived__",
        domain_id=domain,
        schema_name="s",
        table_name=name,
        columns=[Column(name="id", data_type="integer", visible_to=[], is_primary_key=True)],
        view_sql="SELECT 1",
    )


@asynccontextmanager
async def _model(control_plane, monkeypatch, held=frozenset({"table_registration"})):
    """customers in sales and ledger in finance; the router bound to the test's own plane, its
    caller reading what ``reads[0]`` says (everything until a test narrows it)."""
    with control_plane.engine().begin() as c:
        c.exec_driver_sql(f'SET LOCAL search_path TO "{SCHEMA}"')
        c.execute(insert(domains).values(id="sales", description=""))
        c.execute(insert(domains).values(id="finance", description=""))
        c.execute(insert(sources).values(id="__derived__", type="derived"))
    db = control_plane.org()
    reads = [EVERYTHING]

    async def _readable(conn, request):
        return reads[0]

    # The caller models the environment: it holds the right the Tables page is opened with.
    monkeypatch.setattr(capmod, "_resolved_capabilities", lambda _identity, _state: held)
    monkeypatch.setattr(router_mod, "_db", lambda: db)
    monkeypatch.setattr(router_mod, "_readable", _readable)
    ids: dict[str, int] = {}
    async with db.acquire() as conn:
        for name, domain in (("customers", "sales"), ("ledger", "finance")):
            await table_repo.upsert(conn, _table(name, domain))
            ids[name] = (
                await conn.execute_core(
                    select(registered_tables.c.id).where(registered_tables.c.table_name == name)
                )
            ).scalar_one()
    yield db, ids, reads


def _body(ids: dict[str, int], **moved: tuple[float, float]) -> Any:
    at = {"customers": (100.0, 40.0), "ledger": (420.0, 40.0), **moved}
    return router_mod.LayoutBody(
        options=router_mod.LayoutOptions(
            columnDetail="all", edgeRouting="taxi", gridSnap=10, showOrphans=False
        ),
        domains=[
            router_mod.DomainPlace(
                domainId="sales", selected=True, collapsed=False, x=100, y=40, w=200, h=120
            ),
            router_mod.DomainPlace(
                domainId="finance", selected=True, collapsed=True, x=420, y=40, w=130, h=90
            ),
        ],
        tables=[
            router_mod.TablePlace(tableId=ids[name], selected=name == "customers", x=x, y=y)
            for name, (x, y) in at.items()
        ],
    )


def _places(answer: dict) -> dict[int, tuple[float, float]]:
    return {t["tableId"]: (t["x"], t["y"]) for t in answer["tables"]}


@pytest.mark.asyncio
async def test_a_saved_layout_reloads_with_every_place_and_option(control_plane, monkeypatch):
    async with _model(control_plane, monkeypatch) as (_db, ids, _reads):
        await router_mod.save_layout(
            cast(Any, _request()), "Sales overview", _body(ids, customers=(160.0, 80.0))
        )
        answer = await router_mod.get_layout(cast(Any, _request()), "Sales overview")
        assert answer["options"] == {
            "columnDetail": "all",
            "edgeRouting": "taxi",
            "gridSnap": 10,
            "showOrphans": False,
        }
        assert _places(answer) == {ids["customers"]: (160.0, 80.0), ids["ledger"]: (420.0, 40.0)}
        finance = next(d for d in answer["domains"] if d["domainId"] == "finance")
        assert (finance["collapsed"], finance["w"], finance["h"]) == (True, 130.0, 90.0)
        assert answer["updatedBy"] == "sam"
        listed = await router_mod.list_layouts(cast(Any, _request()))
        assert [r["name"] for r in listed["layouts"]] == ["Sales overview"]


@pytest.mark.asyncio
async def test_saving_again_revises_the_layout(control_plane, monkeypatch):
    async with _model(control_plane, monkeypatch) as (_db, ids, _reads):
        await router_mod.save_layout(cast(Any, _request()), "L", _body(ids))
        await router_mod.save_layout(cast(Any, _request()), "L", _body(ids, ledger=(0.0, 300.0)))
        answer = await router_mod.get_layout(cast(Any, _request()), "L")
        assert _places(answer)[ids["ledger"]] == (0.0, 300.0)
        assert len(answer["tables"]) == 2


@pytest.mark.asyncio
async def test_a_renamed_table_keeps_its_place(control_plane, monkeypatch):
    async with _model(control_plane, monkeypatch) as (db, ids, _reads):
        await router_mod.save_layout(cast(Any, _request()), "L", _body(ids))
        async with db.acquire() as conn:
            await conn.execute_core(
                update(registered_tables)
                .where(registered_tables.c.id == ids["customers"])
                .values(table_name="clients")
            )
        answer = await router_mod.get_layout(cast(Any, _request()), "L")
        assert _places(answer)[ids["customers"]] == (100.0, 40.0)


@pytest.mark.asyncio
async def test_a_deleted_table_disappears_from_the_layout(control_plane, monkeypatch):
    async with _model(control_plane, monkeypatch) as (db, ids, _reads):
        await router_mod.save_layout(cast(Any, _request()), "L", _body(ids))
        async with db.acquire() as conn:
            async with conn.transaction():
                assert await table_repo.delete(conn, ids["ledger"])
        answer = await router_mod.get_layout(cast(Any, _request()), "L")
        assert list(_places(answer)) == [ids["customers"]]


@pytest.mark.asyncio
async def test_a_reader_sees_only_what_it_reads_and_keeps_the_rest_on_save(
    control_plane, monkeypatch
):
    async with _model(control_plane, monkeypatch) as (_db, ids, reads):
        await router_mod.save_layout(cast(Any, _request()), "L", _body(ids))
        reads[0] = Readable(
            table=lambda table_id: table_id == ids["customers"],
            domain=lambda domain_id: domain_id == "sales",
        )
        answer = await router_mod.get_layout(cast(Any, _request()), "L")
        assert list(_places(answer)) == [ids["customers"]]
        assert [d["domainId"] for d in answer["domains"]] == ["sales"]
        narrowed = router_mod.LayoutBody(
            options=_body(ids).options,
            domains=[d for d in _body(ids).domains if d.domainId == "sales"],
            tables=[router_mod.TablePlace(tableId=ids["customers"], selected=True, x=5.0, y=6.0)],
        )
        await router_mod.save_layout(cast(Any, _request()), "L", narrowed)
        reads[0] = EVERYTHING
        answer = await router_mod.get_layout(cast(Any, _request()), "L")
        assert _places(answer) == {ids["customers"]: (5.0, 6.0), ids["ledger"]: (420.0, 40.0)}
        assert sorted(d["domainId"] for d in answer["domains"]) == ["finance", "sales"]


@pytest.mark.asyncio
async def test_a_save_placing_what_the_caller_cannot_read_is_refused(control_plane, monkeypatch):
    async with _model(control_plane, monkeypatch) as (_db, ids, reads):
        reads[0] = Readable(
            table=lambda table_id: table_id == ids["customers"], domain=lambda domain_id: True
        )
        with pytest.raises(ApiError) as refused:
            await router_mod.save_layout(cast(Any, _request()), "L", _body(ids))
        assert refused.value.status_code == 403
        assert refused.value.code == "erd.layout_place_unreadable"
        assert (await router_mod.list_layouts(cast(Any, _request())))["layouts"] == []


@pytest.mark.asyncio
async def test_a_layout_is_deleted_and_a_missing_one_is_not_found(control_plane, monkeypatch):
    async with _model(control_plane, monkeypatch) as (_db, ids, _reads):
        await router_mod.save_layout(cast(Any, _request()), "L", _body(ids))
        assert await router_mod.delete_layout(cast(Any, _request()), "L") == {"deleted": "L"}
        for missing in (
            router_mod.delete_layout(cast(Any, _request()), "L"),
            router_mod.get_layout(cast(Any, _request()), "L"),
        ):
            with pytest.raises(ApiError) as refused:
                await missing
            assert refused.value.status_code == 404
            assert refused.value.code == "erd.layout_not_found"


@pytest.mark.asyncio
async def test_a_caller_without_the_modelling_right_cannot_read_or_change_layouts(
    control_plane, monkeypatch
):
    async with _model(control_plane, monkeypatch, held=frozenset({"query_development"})) as (
        _db,
        ids,
        _reads,
    ):
        request = cast(Any, _request())
        for refused_call in (
            router_mod.list_layouts(request),
            router_mod.get_layout(request, "L"),
            router_mod.save_layout(request, "L", _body(ids)),
            router_mod.delete_layout(request, "L"),
        ):
            with pytest.raises(ApiError) as refused:
                await refused_call
            assert refused.value.status_code == 403
            assert refused.value.code == "auth.missing_capability"


@pytest.mark.asyncio
async def test_the_relationships_right_reads_layouts_but_does_not_change_them(
    control_plane, monkeypatch
):
    async with _model(control_plane, monkeypatch) as (_db, ids, _reads):
        request = cast(Any, _request())
        await router_mod.save_layout(request, "L", _body(ids))
        monkeypatch.setattr(
            capmod, "_resolved_capabilities", lambda _identity, _state: {"create_relationship"}
        )
        assert [r["name"] for r in (await router_mod.list_layouts(request))["layouts"]] == ["L"]
        with pytest.raises(ApiError) as refused:
            await router_mod.delete_layout(request, "L")
        assert refused.value.code == "auth.missing_capability"


@pytest.mark.asyncio
async def test_the_layout_versions_with_the_environment_its_tables_by_path(
    control_plane, monkeypatch
):
    async with _model(control_plane, monkeypatch) as (db, ids, _reads):
        await router_mod.save_layout(cast(Any, _request()), "Sales overview", _body(ids))
        async with db.acquire() as conn:
            tree = await project(conn, SCHEMA)
            body = tree["erd-layouts/Sales%20overview.yaml"]
            assert body["options"]["edgeRouting"] == "taxi"
            assert [t["table"] for t in body["tables"]] == [
                "finance/tables/ledger.yaml",
                "sales/tables/customers.yaml",
            ]
            assert all("table_id" not in t and "layout_name" not in t for t in body["tables"])
            rows = _decompose(
                tree, await _existing_ids(conn, SCHEMA), await _roles_after(conn, SCHEMA, tree)
            )
        assert rows["erd_layouts"][0]["name"] == "Sales overview"
        assert {(t["table_id"], t["x"]) for t in rows["erd_layout_tables"]} == {
            (ids["customers"], 100.0),
            (ids["ledger"], 420.0),
        }
        assert {d["domain_id"] for d in rows["erd_layout_domains"]} == {"sales", "finance"}
