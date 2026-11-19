# Copyright (c) 2026 Kenneth Stott
# Canary: 9228575c-0917-4490-829d-e20678301c7b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2219: a role grant written or removed on any path recomputes the person's stored effective
rows: an offer accepted or cancelled, an org membership granted or removed, an invitation
redeemed, an account removed, and the role grants sweep."""

# Requirements: REQ-2219

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any

import pytest
from sqlalchemy import delete, insert, select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from provisa.api.admin import data_product_offers_router as offers_router
from provisa.auth import role_grants
from provisa.core.org_membership import JOINED_VIA_ADMIN, grant_membership, tombstone_id
from provisa.core.schema_org import effective_entitlements, user_role_assignments
from provisa.core.schema_org import roles as roles_table
from provisa.entitlements import effective_store, runtime
from provisa.entitlements.index import DATA_PLANE_CAPABILITIES
from tests.pg_control_plane import ORG
from tests.unit.test_entitlement_lists import _ctx
from tests.unit.test_entitlements_api import _state

pytestmark = pytest.mark.asyncio

_FROM_PROVISA = {"provider": "oidc", "assignments_source": "provisa"}


@pytest.fixture
async def org(control_plane, monkeypatch):
    """The org's runtime stood in by the entitlements API's state, Provisa holding role grants,
    and ``viewer`` a role of the org's store."""
    state = _state(control_plane.org())
    state.auth_config = dict(_FROM_PROVISA)
    state.admin_db = control_plane.platform
    monkeypatch.setattr("provisa.api.app.state", state, raising=False)
    monkeypatch.setattr(runtime, "_role_context", lambda *_: _ctx())

    async def ensure_org_runtime(org_id: str, env: str | None = None, **_: Any) -> Any:
        assert (org_id, env) == (ORG, None)
        return SimpleNamespace(model_db=state.model_db)

    monkeypatch.setattr("provisa.api.app.ensure_org_runtime", ensure_org_runtime)
    async with state.model_db.acquire() as conn:
        await conn.execute_core(
            pg_insert(roles_table)
            .values(id="viewer", capabilities=[], domain_access=["*"])
            .on_conflict_do_nothing()
        )
    return state


async def _assign(state, user: str, **cols: Any) -> None:
    async with state.model_db.acquire() as conn:
        await conn.execute_core(
            insert(user_role_assignments).values(
                user_id=user, role_id="viewer", domain_id="*", **cols
            )
        )


async def _unassign(conn, user: str) -> None:
    await conn.execute_core(
        delete(user_role_assignments).where(user_role_assignments.c.user_id == user)
    )


async def _held(state, user: str) -> set[tuple[str, str]]:
    """The (role, capability) keys of ``user``'s stored effective rows now."""
    async with state.model_db.acquire() as conn:
        return set(await effective_store.stored_rows(conn, user))


async def _any_rows(state, user: str) -> bool:
    t = effective_entitlements
    async with state.model_db.acquire() as conn:
        rows = await conn.execute_core(select(t.c.id).where(t.c.principal == user))
        return rows.first() is not None


async def _recorded(state, user: str) -> None:
    async with state.model_db.acquire() as conn:
        await effective_store.write_person(conn, state, user)


_VIEWER = {("viewer", c) for c in DATA_PLANE_CAPABILITIES}


# Offers


async def test_an_accepted_offer_records_the_requesters_rows(org, monkeypatch):
    async def decide(conn, _decision, **_k) -> dict:
        await conn.execute_core(
            insert(user_role_assignments).values(user_id="bob", role_id="viewer", domain_id="*")
        )
        return {"requester": "bob"}

    monkeypatch.setattr(offers_router.offer_hooks, "decide", decide)
    monkeypatch.setattr(offers_router.offer_hooks, "held_by_idp", lambda _a: True)
    monkeypatch.setattr(offers_router, "_bid_view", lambda row: row)
    async with org.model_db.acquire() as conn:
        await offers_router._decide(conn, org, SimpleNamespace(request_id=1), None)  # pyright: ignore[reportPrivateUsage]
    assert await _held(org, "bob") == _VIEWER


async def test_a_cancelled_offer_closes_the_requesters_rows(org, monkeypatch):
    await _assign(org, "bob")
    await _recorded(org, "bob")
    assert await _held(org, "bob") == _VIEWER

    async def own_bid(*_a, **_k) -> dict:
        return {"id": 7}

    async def product(*_a, **_k) -> dict:
        return {}

    async def destination(_state) -> tuple:
        return offers_router.offer_hooks.LOCAL, None

    async def cancel(_conn, held, **_k) -> dict:
        return {"cancels": held["id"]}

    async def apply_cancel(conn, cancel_row, **_k) -> dict:
        await _unassign(conn, "bob")
        return cancel_row

    monkeypatch.setattr(offers_router, "_state", lambda: org)
    monkeypatch.setattr(offers_router, "_product", product)
    monkeypatch.setattr(offers_router, "_destination", destination)
    monkeypatch.setattr(offers_router, "_bid_view", lambda row: row)
    monkeypatch.setattr(offers_router.offers, "own_bid", own_bid)
    monkeypatch.setattr(offers_router.offers, "cancel", cancel)
    monkeypatch.setattr(offers_router.offer_hooks, "cancel_payload", lambda *_a, **_k: {})
    monkeypatch.setattr(offers_router.offer_hooks, "apply_cancel", apply_cancel)
    request = SimpleNamespace(state=SimpleNamespace(identity=SimpleNamespace(user_id="bob")))
    await offers_router._cancel("p", 7, "done", request, "accepted")  # pyright: ignore[reportPrivateUsage]
    assert await _held(org, "bob") == set()


# Org membership


async def test_an_auto_join_records_the_members_rows(org, monkeypatch):
    from provisa.api.auto_join import join_org_automatically

    async def nothing(*_a, **_k) -> None:
        return None

    monkeypatch.setattr("provisa.core.commerce.bind_member_to_org_trial", nothing)
    await join_org_automatically(org.admin_db, "dana", None, ORG, "viewer")
    assert await _held(org, "dana") == _VIEWER


async def test_leaving_an_org_closes_the_members_rows(org, monkeypatch):
    from provisa.api.admin import orgs_router

    await grant_membership(org.admin_db, "erin", ORG, joined_via=JOINED_VIA_ADMIN)
    await _assign(org, "erin")
    await _recorded(org, "erin")
    assert await _held(org, "erin") == _VIEWER

    async def model_db_of(_org_id: str) -> Any:
        return org.model_db

    monkeypatch.setattr(orgs_router, "_org_model_db", model_db_of)
    request = SimpleNamespace(state=SimpleNamespace(identity=SimpleNamespace(user_id="erin")))
    await orgs_router.leave_org(ORG, request)  # pyright: ignore[reportArgumentType]
    assert await _held(org, "erin") == set()


async def test_a_removed_account_closes_its_rows_in_every_org_it_left(org):
    await _assign(org, "fay")
    await _recorded(org, "fay")
    async with org.model_db.acquire() as conn:
        await _unassign(conn, "fay")
    from provisa.api.effective_rows import record_removed_account

    await record_removed_account(_removed("fay"))
    assert await _held(org, "fay") == set()


def _removed(user: str) -> dict:
    """What ``org_membership.remove_account`` returns for ``user``, a member of the one org."""
    return {"deleted": user, "tombstone": tombstone_id(user), "left_orgs": [ORG]}


async def test_a_removed_account_leaves_no_effective_row_under_its_name(org):
    await _assign(org, "fay")
    await _recorded(org, "fay")
    async with org.model_db.acquire() as conn:
        await _unassign(conn, "fay")
    from provisa.api.effective_rows import record_removed_account

    await record_removed_account(_removed("fay"))
    assert not await _any_rows(org, "fay")
    # The history stays, closed, under the tombstone.
    assert await _any_rows(org, tombstone_id("fay"))
    assert await _held(org, tombstone_id("fay")) == set()


async def test_a_removed_account_given_a_default_role_gets_no_row(org):
    org.auth_config = {
        **_FROM_PROVISA,
        "default_assignments": [{"role_id": "viewer", "domain_id": "*"}],
    }
    from provisa.api.effective_rows import record_removed_account

    await record_removed_account(_removed("gil"))
    assert not await _any_rows(org, "gil")


async def test_a_recompute_after_removal_gives_no_role_from_cached_idp_lists(org):
    from provisa.api.effective_rows import record_removed_account
    from provisa.entitlements import store

    org.auth_config = {"provider": "oidc"}  # role grants are the identity provider's claims
    lists = {"viewer": {"tables": ("finance.revenue",)}}
    async with org.model_db.acquire() as conn:
        await store.mirror_idp(conn, "hana", lists)
    org.idp_entitlements = {**org.idp_entitlements, "hana": lists}
    await _recorded(org, "hana")
    assert await _held(org, "hana")

    await record_removed_account(_removed("hana"))
    assert "hana" not in org.idp_entitlements
    async with org.model_db.acquire() as conn:
        assert await effective_store.write_person(conn, org, "hana") == 0
        # A runtime rebuilt from the store gets no lists back for it either.
        assert "hana" not in await store.load_idp(conn)
        assert await effective_store.write_every_holder(conn, org) == 0
    assert not await _any_rows(org, "hana")


# Invitations


async def test_a_redeemed_invitation_records_the_redeemers_rows(org):
    from provisa.api.auth_router import _record_redeemed  # pyright: ignore[reportPrivateUsage]
    from provisa.api.invite_env import seat_redeemed_roles

    await seat_redeemed_roles(org.model_db, "gus", "viewer")
    await _record_redeemed(ORG, None, "gus")
    assert await _held(org, "gus") == _VIEWER


async def test_an_invitation_pinned_to_an_environment_records_no_prod_row(org):
    from provisa.api.auth_router import _record_redeemed  # pyright: ignore[reportPrivateUsage]

    await _assign(org, "hal")
    await _record_redeemed(ORG, "dev", "hal")
    assert not await _any_rows(org, "hal")


# The role grants sweep


async def test_the_sweep_closes_the_rows_of_an_expired_grant(org):
    await _assign(org, "ivy", expires_at=datetime.now(UTC) + timedelta(days=1))
    await _recorded(org, "ivy")
    assert await _held(org, "ivy") == _VIEWER
    async with org.model_db.acquire() as conn:
        await conn.execute_core(
            user_role_assignments.update()
            .where(user_role_assignments.c.user_id == "ivy")
            .values(expires_at=datetime.now(UTC) - timedelta(minutes=1))
        )
    swept = await role_grants.sweep(org.model_db, org_id=ORG, notify=False)
    assert [g.user_id for g in swept.expired] == ["ivy"]
    assert await _held(org, "ivy") == set()
