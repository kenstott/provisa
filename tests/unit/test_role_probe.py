# Copyright (c) 2026 Kenneth Stott
# Canary: 78662e79-445e-4427-a7a0-20396726941b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Naming a role the caller does not hold is a high-severity security event (REQ-2076)."""

# Requirements: REQ-2076

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from sqlalchemy import insert, select

from provisa.auth.pat import PersonalAccessTokenStore
from provisa.core import org_membership, schema_admin
from provisa.core.database import Database, create_engine_from_url
from provisa.core.schema_admin import orgs, user_org_memberships
from provisa.security import role_probe
from provisa.security.meta_role import RoleNotHeld, resolve_requested_role
from provisa.security.role_probe import NOT_PERMITTED, Probe

pytestmark = pytest.mark.real_role_probe


def _probe(credential=None, transport="http", **kw) -> Probe:
    return Probe(
        org_id=kw.get("org_id", "acme"),
        user_id=kw.get("user_id", "ada"),
        credential=credential,
        credential_id=kw.get("credential_id", "h1" if credential == "pat" else None),
        transport=transport,
        endpoint="POST /data/graphql",
        requested="admin",
        held=("analyst",),
    )


# --- what the event suspends ----------------------------------------------------------------


@pytest.mark.parametrize("transport", ["http", "mcp", "pgwire", "bolt", "flight", "grpc"])
def test_a_personal_access_token_is_suspended_on_every_transport(transport):
    assert role_probe.suspension(_probe("pat", transport)) == "token"


@pytest.mark.parametrize("transport", ["mcp", "pgwire", "bolt", "flight", "airport", "grpc"])
def test_an_idp_principal_is_suspended_where_no_browser_calls(transport):
    assert role_probe.suspension(_probe("idp", transport)) == "principal"


def test_an_idp_bearer_on_the_http_api_is_not_suspended():
    # The browser presents the same bearer; a tab still naming a lost role is routine.
    assert role_probe.suspension(_probe("idp", "http")) is None


@pytest.mark.parametrize("transport", ["http", "pgwire", "bolt"])
def test_a_session_or_password_is_never_suspended(transport):
    assert role_probe.suspension(_probe(None, transport)) is None


def test_a_local_stdio_session_is_never_suspended():
    assert role_probe.suspension(_probe("pat", "stdio")) is None


# --- what the caller is told ----------------------------------------------------------------


def test_a_signed_in_session_is_told_what_it_named():
    said = "Role 'admin' is not assigned to this user"
    assert role_probe.refusal(_probe(None, "http"), said) == said


@pytest.mark.parametrize(
    ("credential", "transport"), [("pat", "http"), ("idp", "http"), ("idp", "mcp"), (None, "bolt")]
)
def test_any_other_caller_is_told_nothing_about_the_role(credential, transport):
    assert role_probe.refusal(_probe(credential, transport), "Role 'admin' ...") == NOT_PERMITTED


# --- what leaves the org --------------------------------------------------------------------


def test_the_notice_names_no_statement_and_no_held_role():
    sent = role_probe.notice(
        _probe("pat"), datetime(2026, 10, 18, tzinfo=timezone.utc), "token", "https://x/team"
    )
    assert sent["requested_role"] == "admin" and sent["severity"] == "high"
    assert "held_roles" not in sent and "recent_statement_ids" not in sent
    assert "analyst" not in str(sent)


# --- detection ------------------------------------------------------------------------------


def test_naming_an_unheld_role_raises_the_event_with_what_is_held():
    with pytest.raises(RoleNotHeld) as refused:
        resolve_requested_role(SimpleNamespace(), {"analyst"}, "analyst,admin")
    assert refused.value.role == "admin" and refused.value.held == ("analyst",)
    assert isinstance(refused.value, PermissionError)


def test_bolt_reads_an_unheld_role_in_the_database_name_as_the_event():
    from provisa.bolt.session import BoltSession

    session = BoltSession.__new__(BoltSession)
    session.roles = ["analyst"]
    with pytest.raises(RoleNotHeld) as refused:
        session._resolve_db("provisa_admin")
    assert refused.value.role == "admin"
    assert session._resolve_db("provisa_analyst") == ("analyst", False)


def test_the_shared_authorizer_records_the_event_and_says_nothing(monkeypatch):
    from provisa.auth import bearer

    recorded: list[Probe] = []

    async def _record(probe):
        recorded.append(probe)
        return "token"

    async def _not_suspended(*_a):
        return False

    def _refuse(*_a, **_k):
        raise RoleNotHeld("admin", {"analyst"})

    monkeypatch.setattr(role_probe, "record", _record)
    monkeypatch.setattr(role_probe, "suspended", _not_suspended)
    monkeypatch.setattr(bearer, "authorize_role", _refuse)
    identity = SimpleNamespace(user_id="ada", credential="pat", credential_id="h1")
    import asyncio

    from provisa.core.request_context import reset_current_org, set_current_org

    token = set_current_org("acme")  # the org bound to the call
    try:
        with pytest.raises(PermissionError, match=f"^{NOT_PERMITTED}$"):
            bearer.authorize_role_or_probe(
                SimpleNamespace(multitenancy=True, org_id="root"),
                identity,
                "admin",
                transport="flight",
                endpoint="do_get",
                run=asyncio.run,
            )
    finally:
        reset_current_org(token)
    [probe] = recorded
    assert (probe.transport, probe.requested, probe.credential_id, probe.org_id) == (
        "flight",
        "admin",
        "h1",
        "acme",
    )


def test_the_shared_authorizer_refuses_a_suspended_principal(monkeypatch):
    from provisa.auth import bearer

    async def _suspended(*_a):
        return True

    monkeypatch.setattr(role_probe, "suspended", _suspended)
    import asyncio

    with pytest.raises(PermissionError, match=f"^{NOT_PERMITTED}$"):
        bearer.authorize_role_or_probe(
            SimpleNamespace(multitenancy=True, org_id="root"),
            SimpleNamespace(user_id="ada", credential="idp", active_org_id="acme"),
            None,
            transport="grpc",
            endpoint="/x",
            run=asyncio.run,
        )


# --- the stores ------------------------------------------------------------------------------


@pytest.fixture
async def admin_db() -> Database:
    db = Database(create_engine_from_url("sqlite+pysqlite:///:memory:"), name="role-probe")
    with db.engine.begin() as conn:
        schema_admin.metadata.create_all(conn)
    async with db.acquire() as conn:
        await conn.execute_core(insert(orgs).values(id="acme", name="acme"))
        await conn.execute_core(insert(user_org_memberships).values(user_id="ada", org_id="acme"))
    return db


@pytest.mark.asyncio
async def test_a_suspended_token_is_refused_until_an_admin_lifts_it(admin_db):
    pats = PersonalAccessTokenStore(admin_db)
    secret, _ = await pats.issue(user_id="ada", org_id="acme", env_name="prod", name="ci")
    identity = await pats.validate(secret)
    assert (identity.credential, identity.credential_id is not None) == ("pat", True)
    assert await pats.suspend(token_hash=identity.credential_id, reason="named 'admin'")
    with pytest.raises(ValueError, match="Invalid token"):
        await pats.validate(secret)
    [listed] = await pats.list_suspended("acme")
    assert listed["suspended_reason"] == "named 'admin'" and listed["user_id"] == "ada"
    assert await pats.lift_suspension(token_hash=identity.credential_id, org_id="acme")
    assert (await pats.validate(secret)).user_id == "ada"
    assert await pats.list_suspended("acme") == []


@pytest.mark.asyncio
async def test_a_principals_token_access_is_suspended_until_an_admin_lifts_it(admin_db):
    assert not await org_membership.token_access_suspended(admin_db, user_id="ada", org_id="acme")
    assert await org_membership.suspend_token_access(
        admin_db, user_id="ada", org_id="acme", reason="named 'admin' over pgwire"
    )
    assert await org_membership.token_access_suspended(admin_db, user_id="ada", org_id="acme")
    [row] = await org_membership.suspended_principals(admin_db, "acme")
    assert row["user_id"] == "ada"
    assert await org_membership.lift_token_access_suspension(admin_db, user_id="ada", org_id="acme")
    async with admin_db.acquire() as conn:
        held = (
            await conn.execute_core(
                select(user_org_memberships.c.token_access_suspended_at).where(
                    user_org_memberships.c.user_id == "ada"
                )
            )
        ).scalar_one()
    assert held is None


# --- the event -------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_the_event_suspends_writes_the_trail_and_notifies(admin_db, monkeypatch):
    trail: list[dict] = []
    posted: list[dict] = []

    async def _record_admin_action(_db, **kw):
        trail.append(kw)

    async def _runtime(_org):
        return SimpleNamespace(model_db=object(), record_db=None)

    class _Client:
        def __init__(self, **_k):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_a):
            return False

        async def post(self, url, json):
            posted.append(json)
            return SimpleNamespace(raise_for_status=lambda: None)

    import httpx

    from provisa.api import app as app_module
    from provisa.core import settings_registry

    monkeypatch.setattr(app_module, "state", SimpleNamespace(admin_db=admin_db, config=None))
    monkeypatch.setattr(app_module, "ensure_org_runtime", _runtime)
    monkeypatch.setattr(org_membership, "record_admin_action", _record_admin_action)
    monkeypatch.setattr(httpx, "AsyncClient", _Client)
    monkeypatch.setattr(
        settings_registry,
        "value",
        lambda key: "https://hooks.example/sec" if key == "security.webhook_url" else None,
    )
    suspended = await role_probe.record(_probe("idp", "pgwire"))
    assert suspended == "principal"
    assert await org_membership.token_access_suspended(admin_db, user_id="ada", org_id="acme")
    [entry] = trail
    assert entry["action"] == "security.role_probe"
    assert entry["detail"]["severity"] == "high"
    assert entry["detail"]["held_roles"] == ["analyst"]
    assert entry["detail"]["recent_statement_ids"] == []
    [sent] = posted
    assert sent["transport"] == "pgwire" and "held_roles" not in sent


@pytest.mark.asyncio
async def test_a_local_stdio_event_goes_to_the_operators_log_alone(caplog):
    import logging

    with caplog.at_level(logging.CRITICAL, logger="provisa.security.role_probe"):
        assert await role_probe.record(_probe(None, "stdio")) is None
    assert "security.role_probe severity=high transport=stdio" in caplog.text
