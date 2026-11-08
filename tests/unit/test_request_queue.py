# Copyright (c) 2026 Kenneth Stott
# Canary: 3f8a1c62-9d4e-4b07-a5c2-6e0b7d9f1a38
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2215, REQ-2216, REQ-2217: raising a request.

Every request names its environment, is addressed to the roles holding the right it names with
their members resolved, keeps the roles none could be resolved for by name, and is told by email
only when the org's delivery setting says Provisa sends notices: events only sends none.
"""

from __future__ import annotations

import types
from unittest.mock import AsyncMock

import pytest

from provisa.api.admin import request_queue as rq
from provisa.core.role_members import Principal, RoleMembers

ROLES = {
    "org_admin": {"capabilities": ["create_view", "org_settings"]},
    "steward": {"capabilities": ["create_view"]},
    "analyst": {"capabilities": ["query"]},
    "auditor": {"capabilities": ["create_view"]},
}

MEMBERS = {
    "org_admin": [Principal("root", "Root", "root@x.test")],
    "steward": [Principal("alice", "Alice", "alice@x.test"), Principal("root", "Root", None)],
    "auditor": [],
}


def test_a_request_is_addressed_to_every_role_holding_its_right():
    assert rq.addressed_roles("create_view", ROLES) == ["auditor", "org_admin", "steward"]
    assert rq.addressed_roles("nobody_holds_this", ROLES) == []


def test_members_are_listed_once_with_the_roles_they_are_reached_through():
    resolved = [
        RoleMembers("org_admin", tuple(MEMBERS["org_admin"]), "provisa"),
        RoleMembers("steward", tuple(MEMBERS["steward"]), "provisa"),
        RoleMembers("auditor", (), "provisa"),
    ]
    assert rq.members_of(resolved) == [
        {"id": "alice", "display_name": "Alice", "email": "alice@x.test", "roles": ["steward"]},
        {
            "id": "root",
            "display_name": "Root",
            "email": "root@x.test",
            "roles": ["org_admin", "steward"],
        },
    ]
    assert rq.unresolved_of(resolved) == ["auditor"]


def test_delivery_is_email_unless_the_org_says_otherwise():
    assert rq.delivery_of({}) == "email"
    assert rq.delivery_of({"requests": {"delivery": "events"}}) == "events"
    assert rq.delivery_of({"requests": {"delivery": "both"}}) == "both"
    assert rq.sends_email("email") and rq.sends_email("both")
    assert not rq.sends_email("events")


def test_a_stored_delivery_outside_the_three_is_refused_by_name():
    with pytest.raises(ValueError, match="'sms'"):
        rq.delivery_of({"requests": {"delivery": "sms"}})


class _Resolver:
    name = "fake"
    time_limit_s = 1.0

    async def members(self, org_id: str, role_id: str) -> list[Principal]:
        return MEMBERS.get(role_id, [])


@pytest.fixture
def raised(monkeypatch):
    """raise_request against a recorded store, the fake resolver and recorded sends."""
    import provisa.core.org_settings as org_settings_mod
    from provisa.core.repositories import creation_request as cr_repo
    from provisa.core.request_context import (
        reset_current_env,
        reset_current_org,
        set_current_env,
        set_current_org,
    )

    stored: dict = {}
    sent: list[tuple[str, str, str]] = []
    settings: dict = {}

    async def _create(conn, request_type, capability, payload, requested_by, **addressed):
        stored.update(
            request_type=request_type,
            capability=capability,
            payload=payload,
            requested_by=requested_by,
            **addressed,
        )
        return 17

    async def _send(_state, org_id, to, subject, body):
        sent.append((to, subject, body))

    async def _resolve(_db):
        return dict(settings)

    state = types.SimpleNamespace(
        model_db=object(),
        roles=ROLES,
        config=types.SimpleNamespace(mail=types.SimpleNamespace(provider="smtp")),
    )
    monkeypatch.setattr(cr_repo, "create", _create)
    monkeypatch.setattr(rq, "_send", _send)
    monkeypatch.setattr(rq, "ProvisaAssignmentsResolver", lambda _db: _Resolver())
    monkeypatch.setattr(org_settings_mod, "resolve_org_config", _resolve)
    org = set_current_org("acme")
    env = set_current_env("dev")
    yield types.SimpleNamespace(stored=stored, sent=sent, settings=settings, state=state)
    reset_current_env(env)
    reset_current_org(org)


async def test_a_request_records_its_environment_roles_members_and_unresolved_roles(raised):
    rid = await rq.raise_request(
        AsyncMock(), "view", "create_view", {"name": "v"}, "alice", state=raised.state
    )
    assert rid == 17
    assert raised.stored["environment"] == "dev"
    assert raised.stored["addressed_roles"] == ["auditor", "org_admin", "steward"]
    assert [m["id"] for m in raised.stored["addressed_members"]] == ["alice", "root"]
    assert raised.stored["unresolved_roles"] == ["auditor"]


async def test_email_delivery_tells_each_member_with_an_address_but_the_requester(raised):
    await rq.raise_request(
        AsyncMock(), "view", "create_view", {"name": "v"}, "alice", state=raised.state
    )
    assert [to for to, _, _ in raised.sent] == ["root@x.test"]
    _, subject, body = raised.sent[0]
    assert "#17" in subject
    assert "dev environment" in body
    assert "auditor, org_admin, steward" in body


async def test_events_only_sends_no_email(raised):
    raised.settings["requests"] = {"delivery": "events"}
    await rq.raise_request(
        AsyncMock(), "view", "create_view", {"name": "v"}, "bob", state=raised.state
    )
    assert raised.sent == []
    assert raised.stored["addressed_members"]  # the row still names who may decide


async def test_both_sends_email(raised):
    raised.settings["requests"] = {"delivery": "both"}
    await rq.raise_request(
        AsyncMock(), "view", "create_view", {"name": "v"}, "bob", state=raised.state
    )
    assert sorted(to for to, _, _ in raised.sent) == ["alice@x.test", "root@x.test"]


async def test_no_mail_provider_sends_nothing(raised):
    raised.state.config.mail.provider = ""
    await rq.raise_request(
        AsyncMock(), "view", "create_view", {"name": "v"}, "bob", state=raised.state
    )
    assert raised.sent == []
