# Copyright (c) 2026 Kenneth Stott
# Canary: 8cfadd2e-aa89-46ff-8b20-2a8e16a4599c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2219, ONE API, THREE WAYS TO USE IT: one write API sets a principal's lists per role grant
and kind, with reason, expiry and audit; where the identity provider sends a list for a role and
kind, that list wins and a write there is refused by name."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import httpx
import pytest
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from sqlalchemy import insert, select

from provisa.api.admin.entitlements_router import router
from provisa.api.errors import ApiError
from provisa.auth import role_grants
from provisa.auth.models import AuthIdentity
from provisa.core.schema_org import (
    admin_audit_log,
    entitlement_expiries,
    entitlements,
    roles,
    user_role_assignments,
)
from provisa.entitlements import runtime, store
from provisa.entitlements.mask import MaskCache
from provisa.security.rights import Capability
from tests.unit.test_entitlement_lists import _ctx, _names

pytestmark = pytest.mark.asyncio

ASSIGN = Capability.ASSIGN_ENTITLEMENTS.value
_TOMORROW = (datetime.now(UTC) + timedelta(days=1)).isoformat()


def _role(capabilities=(), parents=()):
    return {
        "id": "x",
        "capabilities": list(capabilities),
        "parents": list(parents),
        "domain_access": ["*"],
    }


def _state(model_db):
    roles = {
        "viewer": _role(),
        "analyst": _role(parents=["viewer"]),
        "granter": _role([ASSIGN]),
        "scoped_granter": _role([ASSIGN]),
        "people": _role([Capability.USER_MANAGEMENT.value]),
        "nobody": _role(),
    }
    chains = {r: [r] for r in roles}
    chains["analyst"] = ["analyst", "viewer"]
    return SimpleNamespace(
        model_db=model_db,
        roles=roles,
        role_chains=chains,
        held_entitlements={},
        idp_entitlements={},
        names_table=_names(),
        data_product_refs=(),
        entitlement_masks=MaskCache(),
        schema_boot_id="boot",
        schema_version=1,
    )


def _identity(user="admin", role="granter", claims=None):
    identity = AuthIdentity(
        user_id=user, email=None, display_name=None, roles=[role], raw_claims=claims or {}
    )
    identity.acting_roles = [role]
    return identity


@pytest.fixture
async def api(control_plane, monkeypatch):
    state = _state(control_plane.org())
    monkeypatch.setattr("provisa.api.app.state", state, raising=False)
    monkeypatch.setattr(runtime, "_role_context", lambda _state, _role: _ctx())
    caller: dict = {"identity": _identity()}

    app = FastAPI()
    app.include_router(router)

    @app.exception_handler(ApiError)
    async def _coded(_request, exc):  # pyright: ignore[reportUnusedFunction]
        return JSONResponse(
            {"detail": exc.detail, "code": exc.code, "params": exc.params},
            status_code=exc.status_code,
        )

    @app.middleware("http")
    async def _bind(request, call_next):  # pyright: ignore[reportUnusedFunction]
        identity = caller["identity"]
        request.state.identity = identity
        with runtime.entitlements_scope(identity):
            return await call_next(request)

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as client:
        yield SimpleNamespace(client=client, state=state, caller=caller, db=state.model_db)


def _set(principal="alice", role="viewer", kind="tables", ids=("finance.revenue",), **over):
    return {
        "principal": principal,
        "role_id": role,
        "kind": kind,
        "identifiers": list(ids),
        "reason": "quarter close",
        **over,
    }


def _as(api_, user, role="viewer"):
    """The mask ``user``'s next request is served, in ``role``."""
    with runtime.entitlements_scope(_identity(user, role)):
        return runtime.request_mask(api_.state, role)


async def _idp(api_, principal, lists):
    async with api_.db.acquire() as conn:
        await store.mirror_idp(conn, principal, lists)
        api_.state.idp_entitlements = await store.load_idp(conn)


# -- the write takes effect ------------------------------------------------------------------


async def test_a_list_set_through_the_api_narrows_the_principals_next_request(api):
    assert _as(api, "alice") is None
    response = await api.client.put("/admin/entitlements/lists", json=_set())
    assert response.status_code == 200, response.text
    mask = _as(api, "alice")
    assert mask.has_table(1)
    assert not mask.has_table(2)
    other = _as(api, "bob")
    assert other is None


async def test_setting_again_replaces_the_list_on_the_next_request(api):
    await api.client.put("/admin/entitlements/lists", json=_set())
    await api.client.put("/admin/entitlements/lists", json=_set(ids=["sales.orders"]))
    mask = _as(api, "alice")
    assert mask.has_table(2) and not mask.has_table(1)


async def test_ending_a_list_restores_the_role_on_the_next_request(api):
    await api.client.put("/admin/entitlements/lists", json=_set())
    response = await api.client.post(
        "/admin/entitlements/lists/end",
        json={"principal": "alice", "role_id": "viewer", "kind": "tables", "reason": "done"},
    )
    assert response.status_code == 200, response.text
    assert _as(api, "alice") is None


async def test_an_empty_list_is_nothing_of_that_kind(api):
    response = await api.client.put("/admin/entitlements/lists", json=_set(ids=[]))
    assert response.status_code == 200, response.text
    mask = _as(api, "alice")
    assert not mask.has_table(1) and not mask.has_table(2)


# -- the record ------------------------------------------------------------------------------


async def test_a_set_records_who_why_and_the_audit_trail(api):
    await api.client.put("/admin/entitlements/lists", json=_set())
    async with api.db.acquire() as conn:
        row = (
            await conn.execute_core(select(entitlements).where(entitlements.c.principal == "alice"))
        ).fetchone()
        audit = (
            await conn.execute_core(
                select(admin_audit_log).where(admin_audit_log.c.subject_id == "alice")
            )
        ).fetchone()
    assert (row.set_by, row.reason, row.source) == ("admin", "quarter close", "provisa")
    assert audit.action == "entitlements.set"
    assert audit.actor_id == "admin"
    assert audit.detail["role_id"] == "viewer"
    assert audit.detail["kind"] == "tables"
    assert audit.detail["identifiers"] == ["finance.revenue"]
    assert audit.detail["reason"] == "quarter close"


async def test_ending_a_list_is_audited(api):
    await api.client.put("/admin/entitlements/lists", json=_set())
    await api.client.post(
        "/admin/entitlements/lists/end",
        json={"principal": "alice", "role_id": "viewer", "kind": "tables", "reason": "left"},
    )
    async with api.db.acquire() as conn:
        rows = (
            await conn.execute_core(
                select(admin_audit_log.c.action).where(admin_audit_log.c.subject_id == "alice")
            )
        ).fetchall()
    assert [r.action for r in rows] == ["entitlements.set", "entitlements.end"]


async def test_the_lists_of_a_principal_are_listed_with_their_record(api):
    await api.client.put("/admin/entitlements/lists", json=_set(expires_at=_TOMORROW))
    body = (await api.client.get("/admin/entitlements/lists?principal=alice")).json()
    assert body["lists"] == [
        {
            "role_id": "viewer",
            "kind": "tables",
            "identifiers": ["finance.revenue"],
            "capability": None,
            "set_by": "admin",
            "reason": "quarter close",
        }
    ]
    assert body["expiries"][0]["list_kind"] == "tables"
    assert body["expiries"][0]["ends"] == {"*": ["finance.revenue"]}


# -- refused, by name ------------------------------------------------------------------------


async def test_a_write_the_identity_provider_supersedes_is_refused_by_name(api):
    await _idp(api, "alice", {"viewer": {"tables": ("sales.orders",)}})
    response = await api.client.put("/admin/entitlements/lists", json=_set())
    assert response.status_code == 409
    body = response.json()
    assert body["code"] == "entitlements.idp_supersedes"
    assert body["params"] == {"principal": "alice", "role_id": "viewer", "kind": "tables"}
    assert "alice" in body["detail"] and "viewer" in body["detail"] and "tables" in body["detail"]
    async with api.db.acquire() as conn:
        assert "alice" not in await store.load_held(conn)


async def test_an_empty_idp_list_is_still_the_identity_providers_list(api):
    await _idp(api, "alice", {"viewer": {"tables": ()}})
    response = await api.client.put("/admin/entitlements/lists", json=_set())
    assert response.json()["code"] == "entitlements.idp_supersedes"


async def test_ending_a_list_the_identity_provider_supersedes_is_refused_by_name(api):
    await api.client.put("/admin/entitlements/lists", json=_set())
    await _idp(api, "alice", {"viewer": {"tables": ("sales.orders",)}})
    response = await api.client.post(
        "/admin/entitlements/lists/end",
        json={"principal": "alice", "role_id": "viewer", "kind": "tables", "reason": "x"},
    )
    assert response.status_code == 409
    assert response.json()["code"] == "entitlements.idp_supersedes"


async def test_the_identity_provider_is_superseding_only_its_role_and_kind(api):
    await _idp(api, "alice", {"viewer": {"columns": ("sales.orders.id",)}})
    assert (await api.client.put("/admin/entitlements/lists", json=_set())).status_code == 200
    other_role = _set(role="analyst", kind="columns", ids=["sales.orders.id"])
    assert (await api.client.put("/admin/entitlements/lists", json=other_role)).status_code == 200


@pytest.mark.parametrize(
    ("over", "code"),
    [
        ({"reason": "  "}, "entitlements.reason_required"),
        ({"kind": "planets"}, "entitlements.unknown_kind"),
        ({"role_id": "ghost"}, "entitlements.role_not_found"),
        ({"principal": " "}, "entitlements.principal_required"),
        ({"identifiers": ["finance.revenue", " "]}, "entitlements.blank_identifier"),
        ({"expires_at": "2020-01-01T00:00:00+00:00"}, "entitlements.expiry_in_past"),
        ({"identifiers": [], "expires_at": _TOMORROW}, "entitlements.expiry_removes_nothing"),
        (
            {"kind": "sources", "identifiers": ["pg"], "expires_at": _TOMORROW},
            "entitlements.expiry_removes_nothing",
        ),
    ],
)
async def test_a_write_that_cannot_stand_is_refused_by_name(api, over, code):
    response = await api.client.put("/admin/entitlements/lists", json={**_set(), **over})
    assert response.status_code in (404, 422), response.text
    assert response.json()["code"] == code
    async with api.db.acquire() as conn:
        assert (await conn.execute_core(select(entitlements))).fetchall() == []


async def test_an_expiry_with_no_timezone_is_refused(api):
    response = await api.client.put(
        "/admin/entitlements/lists", json=_set(expires_at="2031-01-01T00:00:00")
    )
    assert response.status_code == 422


# -- the gate --------------------------------------------------------------------------------


async def test_a_write_needs_assign_entitlements(api):
    api.caller["identity"] = _identity(role="people")
    response = await api.client.put("/admin/entitlements/lists", json=_set())
    assert response.status_code == 403
    assert response.json()["code"] == "auth.missing_capability"
    assert ASSIGN in response.json()["detail"]


async def test_a_write_is_scoped_by_the_roles_list(api):
    async with api.db.acquire() as conn:
        await store.set_list(
            conn, "scoped", "scoped_granter", "roles", ["viewer"], set_by="o", reason="r"
        )
        api.state.held_entitlements = await store.load_held(conn)
    api.caller["identity"] = _identity("scoped", "scoped_granter")

    # viewer itself, and analyst, which has viewer as an ancestor
    assert (await api.client.put("/admin/entitlements/lists", json=_set())).status_code == 200
    beneath = _set(role="analyst")
    assert (await api.client.put("/admin/entitlements/lists", json=beneath)).status_code == 200

    outside = await api.client.put("/admin/entitlements/lists", json=_set(role="nobody"))
    assert outside.status_code == 403
    assert outside.json()["code"] == "entitlements.role_out_of_reach"
    assert outside.json()["params"] == {"role_id": "nobody"}


async def test_ending_a_list_is_gated_as_a_write(api):
    api.caller["identity"] = _identity(role="nobody")
    response = await api.client.post(
        "/admin/entitlements/lists/end",
        json={"principal": "alice", "role_id": "viewer", "kind": "tables", "reason": "x"},
    )
    assert response.status_code == 403


async def test_everyone_reads_their_own(api):
    await api.client.put("/admin/entitlements/lists", json=_set())
    api.caller["identity"] = _identity("alice", "nobody")
    assert (await api.client.get("/admin/entitlements/lists?principal=alice")).status_code == 200
    own = await api.client.get("/admin/entitlements/effective?principal=alice&role_id=viewer")
    assert own.status_code == 200


async def test_reading_someone_elses_needs_a_right(api):
    api.caller["identity"] = _identity("bob", "nobody")
    response = await api.client.get("/admin/entitlements/lists?principal=alice")
    assert response.status_code == 403
    response = await api.client.get("/admin/entitlements/effective?principal=alice&role_id=viewer")
    assert response.status_code == 403


async def test_user_management_reads_others_within_the_roles_it_administers(api):
    await api.client.put("/admin/entitlements/lists", json=_set())
    await api.client.put(
        "/admin/entitlements/lists", json=_set(role="nobody", ids=["sales.orders"])
    )
    async with api.db.acquire() as conn:
        await store.set_list(conn, "hr", "people", "roles", ["viewer"], set_by="o", reason="r")
        api.state.held_entitlements = await store.load_held(conn)
    api.caller["identity"] = _identity("hr", "people")

    body = (await api.client.get("/admin/entitlements/lists?principal=alice")).json()
    assert [row["role_id"] for row in body["lists"]] == ["viewer"]
    ok = await api.client.get("/admin/entitlements/effective?principal=alice&role_id=viewer")
    assert ok.status_code == 200
    out = await api.client.get("/admin/entitlements/effective?principal=alice&role_id=nobody")
    assert out.status_code == 403
    assert out.json()["code"] == "entitlements.role_out_of_reach"


# -- effective access ------------------------------------------------------------------------


async def test_effective_access_names_where_each_list_came_from(api):
    await api.client.put("/admin/entitlements/lists", json=_set())
    await api.client.put(
        "/admin/entitlements/lists", json=_set(kind="columns", ids=["finance.revenue.id"])
    )
    await _idp(api, "alice", {"viewer": {"columns": ("finance.revenue.amount",)}})
    api.state.roles["viewer"]["default_entitlements"] = {"relationships": ["orders_customer"]}

    body = (
        await api.client.get("/admin/entitlements/effective?principal=alice&role_id=viewer")
    ).json()
    by_kind = {row["kind"]: row for row in body["lists"]}
    assert by_kind["tables"]["origin"] == "provisa"
    assert by_kind["tables"]["identifiers"] == ["finance.revenue"]
    assert by_kind["tables"]["set_by"] == "admin"
    assert by_kind["tables"]["reason"] == "quarter close"
    assert by_kind["columns"]["origin"] == "idp"
    assert by_kind["columns"]["identifiers"] == ["finance.revenue.amount"]
    assert by_kind["columns"]["superseded"] == ["finance.revenue.id"]
    assert by_kind["relationships"]["origin"] == "role"


async def test_an_identifier_naming_nothing_is_flagged_on_the_write(api):
    response = await api.client.put("/admin/entitlements/lists", json=_set(ids=["no.such"]))
    assert response.status_code == 200, response.text
    findings = response.json()["findings"]
    assert [(f["code"], f["identifier"]) for f in findings] == [("unknown_identifier", "no.such")]


async def test_effective_access_of_an_unknown_role_is_refused_by_name(api):
    response = await api.client.get("/admin/entitlements/effective?principal=alice&role_id=ghost")
    assert response.status_code == 404
    assert response.json()["code"] == "entitlements.role_not_found"


# -- expiry ----------------------------------------------------------------------------------


class _Clock:
    """``datetime`` as the runtime reads it, at a time the test sets."""

    def __init__(self, at):
        self.at = at

    def now(self, tz=None):
        return self.at.astimezone(tz)


@pytest.fixture
def clock(monkeypatch):
    def _at(at):
        monkeypatch.setattr(runtime, "datetime", _Clock(at))

    return _at


async def test_an_expired_grant_stops_applying_at_its_expiry(api, clock):
    await api.client.put(
        "/admin/entitlements/lists",
        json=_set(ids=["finance.revenue", "sales.orders"], expires_at=_TOMORROW),
    )
    before = _as(api, "alice")
    assert before.has_table(1) and before.has_table(2)

    clock(datetime.now(UTC) + timedelta(days=2))
    after = _as(api, "alice")
    assert not after.has_table(1) and not after.has_table(2)


async def test_an_expired_list_never_widens_to_the_whole_role(api, clock):
    await api.client.put("/admin/entitlements/lists", json=_set(expires_at=_TOMORROW))
    clock(datetime.now(UTC) + timedelta(days=2))
    mask = _as(api, "alice")
    assert mask is not None
    assert not mask.has_table(2) and not mask.has_table(3)


async def test_effective_access_shows_what_an_expiry_ended(api):
    await api.client.put("/admin/entitlements/lists", json=_set(expires_at=_TOMORROW))
    async with api.db.acquire() as conn:
        await store.add_expiry(
            conn,
            "alice",
            "viewer",
            datetime.now(UTC) - timedelta(hours=1),
            {"*": ["finance.revenue"]},
            set_by="admin",
            reason="ended",
        )
        api.state.held_entitlements = await store.load_held(conn)
    body = (
        await api.client.get("/admin/entitlements/effective?principal=alice&role_id=viewer")
    ).json()
    tables = next(row for row in body["lists"] if row["kind"] == "tables")
    assert tables["ended"] == ["finance.revenue"]
    assert sorted(rule["passed"] for rule in body["expiries"]) == [False, True]
    assert body["grant_ended"] is False


async def _hold(api_, principal, role="viewer", expires_at=None, domain="*"):
    """``principal`` holds a grant of ``role`` Provisa made, until ``expires_at``."""
    async with api_.db.acquire() as conn:
        await conn.upsert(
            roles,
            {"id": role, "capabilities": [], "domain_access": ["*"]},
            index_elements=["id"],
            update_columns=[],
        )
        await conn.execute_core(
            insert(user_role_assignments).values(
                user_id=principal, role_id=role, domain_id=domain, expires_at=expires_at
            )
        )


async def test_a_grant_whose_own_expiry_passed_is_reported_as_ended(api):
    # REQ-2219, EXPIRY IS ON THE GRANT AND ON ITS ENTRIES: what ends a whole grant is its expiry.
    await _hold(api, "alice")
    async with api.db.acquire() as conn:
        await role_grants.end_grant(conn, "alice", "viewer", datetime.now(UTC) - timedelta(hours=1))
    body = (
        await api.client.get("/admin/entitlements/effective?principal=alice&role_id=viewer")
    ).json()
    assert body["grant_ended"] is True


async def test_an_entry_that_names_nothing_is_refused_by_the_store(api):
    async with api.db.acquire() as conn:
        with pytest.raises(ValueError, match="the whole grant's is its own"):
            await store.add_expiry(
                conn,
                "alice",
                "viewer",
                datetime.now(UTC) + timedelta(days=1),
                {},
                set_by="admin",
                reason="left",
            )
        with pytest.raises(ValueError, match="the whole grant's is its own"):
            await store.add_expiry(
                conn,
                "alice",
                "viewer",
                datetime.now(UTC) + timedelta(days=1),
                {"read": []},
                set_by="admin",
                reason="left",
            )
        assert await store.load_held(conn) == {}


async def test_resetting_a_list_withdraws_the_expiry_it_was_set_with(api, clock):
    await api.client.put("/admin/entitlements/lists", json=_set(expires_at=_TOMORROW))
    await api.client.put("/admin/entitlements/lists", json=_set())  # now open-ended
    clock(datetime.now(UTC) + timedelta(days=2))
    assert _as(api, "alice").has_table(1)
    async with api.db.acquire() as conn:
        rows = (
            await conn.execute_core(
                select(entitlement_expiries.c.removed).order_by(entitlement_expiries.c.id)
            )
        ).fetchall()
    assert [r.removed for r in rows] == [False, True]  # append-only: the rule's history stays


async def test_ending_a_list_withdraws_the_expiry_it_was_set_with(api, clock):
    await api.client.put("/admin/entitlements/lists", json=_set(expires_at=_TOMORROW))
    await api.client.post(
        "/admin/entitlements/lists/end",
        json={"principal": "alice", "role_id": "viewer", "kind": "tables", "reason": "x"},
    )
    clock(datetime.now(UTC) + timedelta(days=2))
    assert _as(api, "alice") is None


async def test_a_rule_set_on_another_kind_survives_a_reset(api, clock):
    await api.client.put("/admin/entitlements/lists", json=_set(expires_at=_TOMORROW))
    await api.client.put(
        "/admin/entitlements/lists", json=_set(kind="columns", ids=["sales.orders.id"])
    )
    async with api.db.acquire() as conn:
        held = await store.load_held(conn)
    assert len(held["alice"].expiries) == 1


# -- the identity provider's mirror ----------------------------------------------------------


async def test_the_mirror_holds_the_providers_last_lists(api):
    await _idp(api, "alice", {"viewer": {"tables": ("a.b",), "columns": ("a.b.c",)}})
    await _idp(api, "alice", {"viewer": {"tables": ("x.y",)}})
    assert api.state.idp_entitlements == {"alice": {"viewer": {"tables": ("x.y",)}}}


async def test_a_mirror_with_nothing_changed_appends_nothing(api):
    lists = {"viewer": {"tables": ("a.b",)}}
    await _idp(api, "alice", lists)
    await _idp(api, "alice", lists)
    async with api.db.acquire() as conn:
        rows = (await conn.execute_core(select(entitlements))).fetchall()
    assert len(rows) == 1


async def test_the_mirror_is_not_read_as_what_provisa_holds(api):
    await _idp(api, "alice", {"viewer": {"tables": ("a.b",)}})
    async with api.db.acquire() as conn:
        assert await store.load_held(conn) == {}


async def test_a_sign_in_records_the_tokens_lists(api, monkeypatch):
    from provisa.auth import middleware

    monkeypatch.setattr(middleware, "_recorded_idp_lists", {})
    identity = _identity(
        "carol",
        "viewer",
        claims={"entitlements": {"viewer": {"tables": ["finance.revenue"]}}},
    )
    identity.entitlements_claim = "entitlements"
    await middleware._record_idp_entitlements(api.db, identity)
    assert api.state.idp_entitlements["carol"] == {"viewer": {"tables": ("finance.revenue",)}}
    response = await api.client.put("/admin/entitlements/lists", json=_set(principal="carol"))
    assert response.json()["code"] == "entitlements.idp_supersedes"


async def test_a_sign_in_with_unreadable_lists_records_nothing(api, monkeypatch):
    """The request is refused where the lists are bound; recording must not fail it first."""
    from provisa.auth import middleware

    monkeypatch.setattr(middleware, "_recorded_idp_lists", {})
    identity = _identity("carol", "viewer", claims={"entitlements": "not a map"})
    identity.entitlements_claim = "entitlements"
    await middleware._record_idp_entitlements(api.db, identity)
    assert "carol" not in api.state.idp_entitlements


async def test_the_capability_reads_the_roles_kind(api):
    """A roles list on a role holding only assign_entitlements is used, so it is not flagged."""
    response = await api.client.put(
        "/admin/entitlements/lists",
        json=_set(role="granter", kind="roles", ids=["viewer"]),
    )
    assert response.status_code == 200, response.text
    assert response.json()["findings"] == []
