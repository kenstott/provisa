# Copyright (c) 2026 Kenneth Stott
# Canary: 2a67f31d-5c08-4e94-b7a2-60d9384ec157
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1287: onboarding's second question — does this person have an invitation?

Onboarding asks three independent things: do you have an account, do you have an invitation, do
you have a membership. Without an answer to the middle one an invited user who arrived without
their token is indistinguishable from a stranger, and the only thing the UI can offer is "create
an org" — which is the wrong answer for someone whose team already has one.

What the query has to get right is who an invitation is FOR: matching too loosely discloses one
org's pending invitations to another person, and matching too strictly leaves the invitee looking
like a stranger again. Both are asserted against a recorded admin plane, since the point is the
predicate rather than the transport.
"""

# Requirements: REQ-516, REQ-1287

from __future__ import annotations

import datetime
import types

from datetime import timezone

import pytest


class _Row:
    def __init__(self, mapping):
        self._mapping = mapping


class _Result:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return self._rows


class _Conn:
    def __init__(self, plane):
        self._plane = plane

    async def execute_core(self, stmt):
        self._plane.statements.append(str(stmt))
        return _Result(self._plane.rows)


class _Db:
    def __init__(self, plane):
        self._plane = plane

    def acquire(self):
        conn = _Conn(self._plane)

        class _Ctx:
            async def __aenter__(self_inner):
                return conn

            async def __aexit__(self_inner, *exc):
                return False

        return _Ctx()


@pytest.fixture
def plane(monkeypatch):
    expires = datetime.datetime(2026, 9, 1, tzinfo=timezone.utc)
    state = types.SimpleNamespace(
        statements=[],
        rows=[
            _Row(
                {
                    "token": "tok-1",
                    "org_id": "carolco",
                    "org_name": "Carolco",
                    "role_id": "analyst",
                    "expires_at": expires,
                }
            )
        ],
    )
    monkeypatch.setattr(
        "provisa.api.app.state", types.SimpleNamespace(admin_db=_Db(state)), raising=False
    )
    return state


def _request(email: str | None, *, authenticated: bool = True):
    identity = types.SimpleNamespace(user_id="carol", email=email) if authenticated else None
    return types.SimpleNamespace(state=types.SimpleNamespace(identity=identity))


@pytest.mark.asyncio
async def test_a_pending_invitation_is_reported_with_what_the_ui_needs_to_accept_it(plane):
    from provisa.api.auth_router import my_invites

    body = await my_invites(_request("carol@example.com"))

    assert body["invites"] == [
        {
            "token": "tok-1",
            "org_id": "carolco",
            "org_name": "Carolco",
            "role_id": "analyst",
            "expires_at": "2026-09-01T00:00:00+00:00",
        }
    ]


@pytest.mark.asyncio
async def test_the_org_name_comes_back_so_the_invitation_is_recognisable(plane):
    """ "Join carolco" means nothing to an invitee; "Join Carolco" is the org they were told about."""
    from provisa.api.auth_router import my_invites

    body = await my_invites(_request("carol@example.com"))

    assert body["invites"][0]["org_name"] == "Carolco"


@pytest.mark.asyncio
async def test_only_unused_unexpired_invitations_addressed_to_the_caller_are_returned(plane):
    from provisa.api.auth_router import my_invites

    await my_invites(_request("carol@example.com"))

    where = plane.statements[0]
    assert "lower(org_invites.email) = " in where.lower().replace("\n", " ")
    assert "used_at IS NULL" in where
    assert "expires_at >" in where


@pytest.mark.asyncio
async def test_the_email_match_is_case_and_whitespace_insensitive(plane):
    """An invitation typed as Carol@Example.com must be found by carol@example.com — the invitee
    otherwise sees "create an org" while their invitation sits unredeemed."""
    from provisa.api.auth_router import my_invites

    body = await my_invites(_request("  Carol@Example.COM  "))

    assert len(body["invites"]) == 1


@pytest.mark.asyncio
async def test_a_caller_with_no_email_is_told_nothing_rather_than_everything(plane):
    """A dev principal or an OIDC identity with no email claim has no invitations addressed to
    it. Querying anyway would match on an empty string and hand back somebody else's."""
    from provisa.api.auth_router import my_invites

    body = await my_invites(_request(None))

    assert body == {"invites": []}
    assert plane.statements == []


@pytest.mark.asyncio
async def test_an_unauthenticated_caller_gets_no_invitations(plane):
    from provisa.api.auth_router import my_invites

    body = await my_invites(_request(None, authenticated=False))

    assert body == {"invites": []}
    assert plane.statements == []


@pytest.mark.asyncio
async def test_a_link_only_invitation_is_not_reported_to_anyone(plane):
    """A link invite carries no email; it is shareable by whoever holds it, so it must not show
    up as addressed to a person who merely signed in."""
    from provisa.api.auth_router import my_invites

    plane.rows = []
    body = await my_invites(_request("carol@example.com"))

    assert body["invites"] == []
