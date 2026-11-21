# Copyright (c) 2026 Kenneth Stott
# Canary: 6a2e9c14-7b38-4d51-8f0a-3c5d1e7b9f26
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2219: one stored effective row per person, role and capability, with the mask's bits, the
version of the bit positions they are over and the readable expansion; written when a sign-in or
a Provisa write changes the inputs, for every person Provisa knows to hold a role, and never read
against another model version."""

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any, cast

import httpx
import pytest
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from sqlalchemy import insert, select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from provisa.api.admin.entitlements_router import router
from provisa.api.errors import ApiError
from provisa.auth import middleware
from provisa.core.environments import PROD
from provisa.core.schema_org import effective_entitlements, user_role_assignments
from provisa.core.schema_org import roles as roles_table
from provisa.entitlements import effective_store, runtime, store
from provisa.entitlements.index import DATA_PLANE_CAPABILITIES
from provisa.entitlements.mask import index_version
from tests.unit.test_entitlement_lists import _ctx
from tests.pg_control_plane import ORG
from tests.unit.test_entitlements_api import _identity, _set, _state

pytestmark = pytest.mark.asyncio

_NOW = datetime(2026, 11, 19, tzinfo=UTC)
_FROM_PROVISA = {"provider": "oidc", "assignments_source": "provisa"}


@pytest.fixture
async def api(control_plane, monkeypatch):
    state = _state(control_plane.org())
    state.auth_config = dict(_FROM_PROVISA)
    monkeypatch.setattr("provisa.api.app.state", state, raising=False)
    monkeypatch.setattr(runtime, "_role_context", lambda *_: _ctx())
    identity = _identity()

    app = FastAPI()
    app.include_router(router)

    @app.exception_handler(ApiError)
    async def _coded(_request, exc):  # pyright: ignore[reportUnusedFunction]
        return JSONResponse({"detail": exc.detail, "code": exc.code}, status_code=exc.status_code)

    @app.middleware("http")
    async def _bind(request, call_next):  # pyright: ignore[reportUnusedFunction]
        request.state.identity = identity
        request.state.active_org_id = ORG
        with runtime.entitlements_scope(identity):
            return await call_next(request)

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as client:
        yield SimpleNamespace(client=client, state=state, db=state.model_db)


async def _assign(api_, user: str, role: str) -> None:
    async with api_.db.acquire() as conn:
        await conn.execute_core(
            pg_insert(roles_table)
            .values(id=role, capabilities=[], domain_access=["*"])
            .on_conflict_do_nothing()
        )
        await conn.execute_core(
            insert(user_role_assignments).values(user_id=user, role_id=role, domain_id="*")
        )


async def _rows(api_, principal: str) -> list:
    t = effective_entitlements
    async with api_.db.acquire() as conn:
        rows = await conn.execute_core(select(t).where(t.c.principal == principal).order_by(t.c.id))
        return rows.fetchall()


async def _write(api_, principal: str) -> int:
    async with api_.db.acquire() as conn:
        return await effective_store.write_person(conn, api_.state, principal, _NOW)


async def _hold(api_, principal: str, role: str, kind: str, identifiers: list[str]) -> None:
    async with api_.db.acquire() as conn:
        await store.set_list(conn, principal, role, kind, identifiers, set_by="ops", reason="r")
        api_.state.held_entitlements = await store.load_held(conn)


async def _stored_read(api_, principal: str) -> tuple[str, ...]:
    """The expansion of ``principal``'s stored read row through viewer."""
    async with api_.db.acquire() as conn:
        now = await effective_store.stored_rows(conn, principal)
    expansion = now[("viewer", "read")].expansion
    assert expansion is not None
    return expansion


def _kind_bits(mask, capability: str) -> int:
    kinds = DATA_PLANE_CAPABILITIES[capability][1]
    return sum(1 << o.bit for o in mask.index.objects if o.kind in kinds)


async def test_a_write_appends_one_row_per_capability_with_the_masks_bits(api):
    await _assign(api, "alice", "viewer")
    await _hold(api, "alice", "viewer", "tables", ["sales.*"])
    appended = await _write(api, "alice")
    rows = await _rows(api, "alice")
    assert appended == len(rows)
    assert {r.capability for r in rows} == set(DATA_PLANE_CAPABILITIES)
    assert all(r.role_id == "viewer" for r in rows)
    held = api.state.held_entitlements["alice"]
    mask = runtime.principal_mask(api.state, "alice", "viewer", held, _NOW)
    assert mask is not None
    version = index_version(mask.index)
    for row in rows:
        assert int(row.bits, 16) == mask.bits & _kind_bits(mask, row.capability)
        assert row.model_version == version
    read = next(r for r in rows if r.capability == "read")
    assert "finance.revenue" not in read.expansion
    assert "sales.orders" in read.expansion


async def test_the_same_write_again_appends_nothing(api):
    await _assign(api, "alice", "viewer")
    await _hold(api, "alice", "viewer", "tables", ["sales.*"])
    await _write(api, "alice")
    before = len(await _rows(api, "alice"))
    assert await _write(api, "alice") == 0
    assert len(await _rows(api, "alice")) == before


async def test_a_changed_list_closes_the_row_and_appends_one(api):
    await _assign(api, "alice", "viewer")
    await _hold(api, "alice", "viewer", "tables", ["sales.*"])
    await _write(api, "alice")
    await _hold(api, "alice", "viewer", "tables", ["finance.*"])
    assert await _write(api, "alice") >= 1
    reads = await _stored_read(api, "alice")
    assert "finance.revenue" in reads
    assert "sales.orders" not in reads
    reads = [r for r in await _rows(api, "alice") if r.capability == "read"]
    assert len(reads) == 2
    assert reads[0].valid_from <= reads[1].valid_from


async def test_a_sign_in_with_changed_idp_lists_closes_and_appends(api, monkeypatch):
    monkeypatch.setattr(middleware, "_recorded_idp_lists", {})
    await _assign(api, "carol", "viewer")

    async def sign_in(tables: list[str]) -> None:
        identity = _identity(
            "carol", "viewer", claims={"entitlements": {"viewer": {"tables": tables}}}
        )
        identity.entitlements_claim = "entitlements"
        await middleware._record_idp_entitlements(api.db, identity)  # pyright: ignore[reportPrivateUsage]

    await sign_in(["finance.revenue"])
    first = await _rows(api, "carol")
    assert {r.capability for r in first} == set(DATA_PLANE_CAPABILITIES)
    read = next(r for r in first if r.capability == "read")
    assert "finance.revenue" in read.expansion
    assert "sales.orders" not in read.expansion

    await sign_in(["sales.orders"])
    reads = [r for r in await _rows(api, "carol") if r.capability == "read"]
    assert len(reads) == 2
    assert "sales.orders" in reads[1].expansion
    assert "finance.revenue" not in reads[1].expansion


async def test_a_list_write_through_the_api_updates_the_rows(api):
    await _assign(api, "alice", "viewer")
    await _write(api, "alice")
    response = await api.client.put(
        "/admin/entitlements/lists",
        json=_set(principal="alice", ids=["finance.revenue"]),
    )
    assert response.status_code == 200, response.text
    reads = await _stored_read(api, "alice")
    assert "finance.revenue" in reads
    assert "sales.orders" not in reads


async def test_a_role_no_longer_held_has_its_rows_closed(api):
    await _assign(api, "alice", "viewer")
    await _write(api, "alice")
    async with api.db.acquire() as conn:
        await conn.execute_core(
            user_role_assignments.delete().where(user_role_assignments.c.user_id == "alice")
        )
    api.state.auth_config = {**_FROM_PROVISA, "default_assignments": []}
    assert await _write(api, "alice") == len(DATA_PLANE_CAPABILITIES)
    async with api.db.acquire() as conn:
        assert await effective_store.stored_rows(conn, "alice") == {}
    assert sum(r.removed for r in await _rows(api, "alice")) == len(DATA_PLANE_CAPABILITIES)


async def test_every_role_holder_gets_rows(api):
    await _assign(api, "alice", "viewer")
    await _assign(api, "bob", "analyst")
    await _hold(api, "dora", "viewer", "tables", ["sales.*"])  # holds a list, no grant
    async with api.db.acquire() as conn:
        await effective_store.write_every_holder(conn, api.state, _NOW)
    assert {r.role_id for r in await _rows(api, "alice")} == {"viewer"}
    assert {r.role_id for r in await _rows(api, "bob")} == {"analyst"}
    assert await _rows(api, "dora") == []


async def test_a_row_is_never_read_against_another_model_version(api, monkeypatch):
    await _assign(api, "alice", "viewer")
    await _write(api, "alice")
    async with api.db.acquire() as conn:
        read = await effective_store.current_rows(conn, api.state, "alice")
    assert ("alice", "viewer", "read") in read
    # A build that drops a table from the role's schema moves the bit positions.
    monkeypatch.setattr(runtime, "_role_context", lambda *_: _ctx((1, 2)))
    api.state.schema_version += 1
    async with api.db.acquire() as conn:
        assert await effective_store.current_rows(conn, api.state, "alice") == {}
        assert ("viewer", "read") in await effective_store.stored_rows(conn, "alice")


async def test_a_build_that_moves_no_bit_keeps_the_version(api):
    await _assign(api, "alice", "viewer")
    await _write(api, "alice")
    api.state.schema_version += 1
    assert await _write(api, "alice") == 0
    async with api.db.acquire() as conn:
        assert ("alice", "viewer", "read") in await effective_store.current_rows(
            conn, api.state, "alice"
        )


async def _build(monkeypatch, env: str = PROD) -> None:
    """The model build's step that recomputes the effective rows: its after-answer pass's derived
    writes, with the lineage edges and checker targets it also writes left out."""
    from provisa.api import app as app_module
    from provisa.api import rebuild_trace

    monkeypatch.setattr("provisa.api.admin.lineage_router.write_lineage_edges", _no_write)
    monkeypatch.setattr("provisa.dq.registration.write_checker_targets", _no_write)
    steps = app_module._after_answer_steps(  # pyright: ignore[reportPrivateUsage]
        runtime=cast("Any", SimpleNamespace(role_build_inputs=None)),
        env=env,
        announce=False,
        checked={},
        names=None,
        build=cast("Any", None),
    )
    await steps[rebuild_trace.DERIVED_WRITES]()


async def _no_write(*_args: object) -> None:
    return None


async def test_a_build_that_changes_a_roles_schema_moves_every_holder_to_its_version(
    api, monkeypatch
):
    await _assign(api, "alice", "viewer")
    await _assign(api, "bob", "analyst")
    await _build(monkeypatch)
    before = {p: await _rows(api, p) for p in ("alice", "bob")}
    assert {r.role_id for r in before["alice"]} == {"viewer"}
    assert {r.role_id for r in before["bob"]} == {"analyst"}
    # The build drops a table from the roles' schema: every bit position moves.
    monkeypatch.setattr(runtime, "_role_context", lambda *_: _ctx((1, 2)))
    api.state.schema_version += 1
    await _build(monkeypatch)
    for principal in ("alice", "bob"):
        rows = await _rows(api, principal)
        old, new = rows[: len(before[principal])], rows[len(before[principal]) :]
        role = before[principal][0].role_id
        version = effective_store.current_version(api.state, role)
        assert {r.model_version for r in old} != {version}
        # One new row per key, at the new version: each closes the key's old row.
        assert {(r.role_id, r.capability) for r in new} == {(r.role_id, r.capability) for r in old}
        assert {r.model_version for r in new} == {version}
        assert not any(r.removed for r in new)
        async with api.db.acquire() as conn:
            current = await effective_store.current_rows(conn, api.state, principal)
        assert {(r, c) for _p, r, c in current} == {(r.role_id, r.capability) for r in new}


async def test_a_build_that_moves_no_bit_appends_nothing(api, monkeypatch):
    await _assign(api, "alice", "viewer")
    await _assign(api, "bob", "analyst")
    await _build(monkeypatch)
    before = [await _rows(api, p) for p in ("alice", "bob")]
    api.state.schema_version += 1
    await _build(monkeypatch)
    assert [await _rows(api, p) for p in ("alice", "bob")] == before


async def test_a_write_with_no_model_built_is_made_by_the_first_build(api, monkeypatch):
    await _assign(api, "alice", "viewer")
    names = api.state.names_table
    api.state.names_table = None
    assert await _write(api, "alice") == 0
    assert await _rows(api, "alice") == []
    api.state.names_table = names
    await _build(monkeypatch)
    assert {r.role_id for r in await _rows(api, "alice")} == {"viewer"}


async def test_an_environment_build_leaves_the_rows_to_the_prod_build(api, monkeypatch):
    await _assign(api, "alice", "viewer")
    await _build(monkeypatch, env="sandbox")
    assert await _rows(api, "alice") == []


async def test_every_holder_is_written_a_batch_at_a_time(api, monkeypatch):
    monkeypatch.setattr(effective_store, "_HOLDER_BATCH", 2)
    for user in ("alice", "bob", "carol"):
        await _assign(api, user, "viewer")
    await _build(monkeypatch)
    for user in ("alice", "bob", "carol"):
        assert {r.role_id for r in await _rows(api, user)} == {"viewer"}
