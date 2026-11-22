# Copyright (c) 2026 Kenneth Stott
# Canary: 5e2a9c71-3b84-4f06-a1d7-c94e0b6f3a18
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1250: a request to join an org says nothing about the org, and is bounded.

An invite_only org (the default) refuses a request silently, and so does a name no org holds:
the requester gets the same answer either way. An open org logs it for its admins. One open
request per (user, org); a requester is rate-limited across every org alike; a pending request
expires with the invitation TTL.
"""

# Requirements: REQ-1250

from __future__ import annotations

import datetime
import types
from datetime import timezone

import pytest
from sqlalchemy import insert, select

from provisa.core.repositories import org_join_request as jr
from provisa.core.schema_admin import (
    org_join_requests,
    orgs,
    user_org_memberships,
    user_profiles,
)

TTL = datetime.timedelta(days=7)
T0 = datetime.datetime(2026, 11, 20, 12, 0, tzinfo=timezone.utc)
ASKER = "fb-asker"


async def _org(plane, org_id: str, *, open_to_requests: bool | None) -> None:
    values: dict = {"id": org_id, "name": org_id, "created_by": "founder"}
    if open_to_requests is not None:
        values["open_to_requests"] = open_to_requests
    async with plane.acquire() as conn:
        await conn.execute_core(insert(orgs).values(**values))


async def _statuses(plane, org_id: str) -> list[str]:
    async with plane.acquire() as conn:
        rows = (
            await conn.execute_core(
                select(org_join_requests.c.status)
                .where(org_join_requests.c.org_id == org_id)
                .order_by(org_join_requests.c.requested_at)
            )
        ).fetchall()
    return [r[0] for r in rows]


@pytest.fixture
def plane(control_plane):
    return control_plane.platform


async def test_an_org_is_invite_only_unless_opened(plane):
    await _org(plane, "quiet", open_to_requests=None)
    async with plane.acquire() as conn:
        row = (
            await conn.execute_core(select(orgs.c.open_to_requests).where(orgs.c.id == "quiet"))
        ).one()
    assert row[0] is False


async def test_an_invite_only_org_refuses_silently(plane):
    await _org(plane, "quiet", open_to_requests=False)
    result = await jr.submit_join_request(plane, user_id=ASKER, org_id="quiet", ttl=TTL, now=T0)
    assert result is None
    assert await _statuses(plane, "quiet") == [jr.REJECTED]
    assert await jr.list_open_requests(plane, "quiet", now=T0) == []


async def test_a_missing_org_answers_as_an_invite_only_one_does(plane):
    result = await jr.submit_join_request(plane, user_id=ASKER, org_id="nobody", ttl=TTL, now=T0)
    assert result is None
    assert await _statuses(plane, "nobody") == [jr.REJECTED]


async def test_an_open_org_logs_the_request_for_its_admins(plane):
    await _org(plane, "open", open_to_requests=True)
    async with plane.acquire() as conn:
        await conn.execute_core(
            insert(user_profiles).values(user_id=ASKER, email="asker@example.com")
        )
    await jr.submit_join_request(plane, user_id=ASKER, org_id="open", ttl=TTL, now=T0)
    listed = await jr.list_open_requests(plane, "open", now=T0)
    assert [(r["user_id"], r["email"]) for r in listed] == [(ASKER, "asker@example.com")]
    assert listed[0]["expires_at"] == T0 + TTL


async def test_one_open_request_per_user_and_org(plane):
    await _org(plane, "open", open_to_requests=True)
    await jr.submit_join_request(plane, user_id=ASKER, org_id="open", ttl=TTL, now=T0)
    later = T0 + datetime.timedelta(minutes=1)
    assert (
        await jr.submit_join_request(plane, user_id=ASKER, org_id="open", ttl=TTL, now=later)
        is None
    )
    assert await _statuses(plane, "open") == [jr.PENDING, jr.REJECTED]
    assert len(await jr.list_open_requests(plane, "open", now=later)) == 1


async def test_the_schema_refuses_a_second_open_request(plane):
    await _org(plane, "open", open_to_requests=True)
    await jr.submit_join_request(plane, user_id=ASKER, org_id="open", ttl=TTL, now=T0)
    with pytest.raises(Exception, match="uq_org_join_requests_open"):
        async with plane.acquire() as conn:
            await conn.execute_core(
                insert(org_join_requests).values(
                    id="dup",
                    user_id=ASKER,
                    org_id="open",
                    status=jr.PENDING,
                    requested_at=T0,
                    expires_at=T0 + TTL,
                )
            )


async def test_a_member_asking_is_refused_silently(plane):
    await _org(plane, "open", open_to_requests=True)
    async with plane.acquire() as conn:
        await conn.execute_core(insert(user_org_memberships).values(user_id=ASKER, org_id="open"))
    await jr.submit_join_request(plane, user_id=ASKER, org_id="open", ttl=TTL, now=T0)
    assert await _statuses(plane, "open") == [jr.REJECTED]


async def test_the_rate_limit_counts_every_attempt_alike(plane):
    await _org(plane, "open", open_to_requests=True)
    await _org(plane, "quiet", open_to_requests=False)
    targets = ["open", "quiet", "nobody"]
    for i in range(jr.RATE_LIMIT):
        await jr.submit_join_request(
            plane,
            user_id=ASKER,
            org_id=targets[i % 3],
            ttl=TTL,
            now=T0 + datetime.timedelta(seconds=i),
        )
    for target in targets:
        with pytest.raises(jr.JoinRequestRateLimited) as limited:
            await jr.submit_join_request(
                plane, user_id=ASKER, org_id=target, ttl=TTL, now=T0 + datetime.timedelta(minutes=5)
            )
        assert limited.value.retry_after == jr.RATE_WINDOW - datetime.timedelta(minutes=5)
    # Another requester is not held back by this one.
    await jr.submit_join_request(
        plane, user_id="fb-other", org_id="open", ttl=TTL, now=T0 + datetime.timedelta(minutes=5)
    )
    # And this one may ask again once the window has passed.
    await jr.submit_join_request(
        plane, user_id=ASKER, org_id="quiet", ttl=TTL, now=T0 + jr.RATE_WINDOW + jr.RATE_WINDOW
    )


async def test_a_pending_request_expires_with_the_invite_ttl(plane):
    await _org(plane, "open", open_to_requests=True)
    await jr.submit_join_request(plane, user_id=ASKER, org_id="open", ttl=TTL, now=T0)
    just_before = T0 + TTL - datetime.timedelta(seconds=1)
    assert len(await jr.list_open_requests(plane, "open", now=just_before)) == 1
    assert await jr.list_open_requests(plane, "open", now=T0 + TTL) == []
    # Once expired, asking again opens a new request.
    again = T0 + TTL + datetime.timedelta(days=1)
    await jr.submit_join_request(plane, user_id=ASKER, org_id="open", ttl=TTL, now=again)
    assert await _statuses(plane, "open") == [jr.EXPIRED, jr.PENDING]
    assert len(await jr.list_open_requests(plane, "open", now=again)) == 1


def test_a_request_lives_as_long_as_a_default_invitation():
    from provisa.api.admin.invites_router import CreateInviteBody
    from provisa.api.admin.orgs_router import _join_request_ttl

    days = CreateInviteBody.model_fields["expires_in_days"].default
    assert _join_request_ttl() == datetime.timedelta(days=days)


def _request(user_id: str, credential: str | None):
    identity = types.SimpleNamespace(user_id=user_id, credential=credential)
    return types.SimpleNamespace(state=types.SimpleNamespace(identity=identity))


@pytest.fixture
def routed(plane, monkeypatch):
    from provisa.api.admin import orgs_router

    monkeypatch.setattr(orgs_router, "_admin_pool", lambda: plane)
    return orgs_router


async def _firebase_profile(plane, user_id: str, provider: str = "firebase") -> None:
    async with plane.acquire() as conn:
        await conn.execute_core(insert(user_profiles).values(user_id=user_id, provider=provider))


async def test_the_endpoint_answers_the_same_for_open_invite_only_and_missing(plane, routed):
    await _org(plane, "open", open_to_requests=True)
    await _org(plane, "quiet", open_to_requests=False)
    await _firebase_profile(plane, ASKER)
    answers = [
        await routed.request_to_join(org, _request(ASKER, "idp"))  # type: ignore[arg-type]
        for org in ("open", "quiet", "nobody")
    ]
    assert answers == [{"status": "received"}] * 3
    assert await _statuses(plane, "open") == [jr.PENDING]


async def test_the_endpoint_requires_a_firebase_sign_in(plane, routed):
    from provisa.api.errors import ApiError

    await _org(plane, "open", open_to_requests=True)
    await _firebase_profile(plane, "basic-user", provider="basic")
    await _firebase_profile(plane, ASKER)
    for request in (_request("basic-user", None), _request(ASKER, "pat")):
        with pytest.raises(ApiError) as refused:
            await routed.request_to_join("open", request)  # type: ignore[arg-type]
        assert refused.value.status_code == 403
    assert await _statuses(plane, "open") == []
    with pytest.raises(ApiError) as anonymous:
        await routed.request_to_join("open", _request("anonymous", None))  # type: ignore[arg-type]
    assert anonymous.value.status_code == 401


async def test_the_endpoint_answers_429_over_the_rate(plane, routed, monkeypatch):
    from provisa.api.errors import ApiError

    monkeypatch.setattr(jr, "RATE_LIMIT", 1)
    await _firebase_profile(plane, ASKER)
    await routed.request_to_join("nobody", _request(ASKER, "idp"))  # type: ignore[arg-type]
    with pytest.raises(ApiError) as limited:
        await routed.request_to_join("nobody", _request(ASKER, "idp"))  # type: ignore[arg-type]
    assert limited.value.status_code == 429


async def test_the_gate_refuses_a_caller_without_a_firebase_sign_in(plane, routed):
    """The handler's gate (the one test_admin_routes_gated recognises) refuses on its own."""
    from provisa.api.errors import ApiError

    await _firebase_profile(plane, "basic-user", provider="basic")
    await _firebase_profile(plane, ASKER)
    for request, status in (
        (_request("anonymous", None), 401),
        (_request("basic-user", "idp"), 403),
        (_request(ASKER, "pat"), 403),
    ):
        with pytest.raises(ApiError) as refused:
            await routed._require_join_requester(request)  # type: ignore[arg-type]
        assert refused.value.status_code == status
    assert await routed._require_join_requester(_request(ASKER, "idp")) == ASKER  # type: ignore[arg-type]


def test_a_request_names_no_requester_but_the_caller():
    """Nothing the client sends can name whom a request is for: the handler takes only the org id
    from the path and the request, and the requester comes from the caller's identity."""
    import inspect

    from provisa.api.admin.orgs_router import request_to_join

    assert list(inspect.signature(request_to_join).parameters) == ["org_id", "request"]


async def test_the_request_is_filed_for_the_signed_in_caller_only(plane, routed):
    await _org(plane, "open", open_to_requests=True)
    await _firebase_profile(plane, ASKER)
    await _firebase_profile(plane, "fb-someone-else")
    await routed.request_to_join("open", _request(ASKER, "idp"))  # type: ignore[arg-type]
    async with plane.acquire() as conn:
        rows = (
            await conn.execute_core(
                select(org_join_requests.c.user_id).where(org_join_requests.c.org_id == "open")
            )
        ).fetchall()
    assert [r[0] for r in rows] == [ASKER]
