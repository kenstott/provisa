# Copyright (c) 2026 Kenneth Stott
# Canary: 8c90d6a7-4028-4006-9fb3-5d731539b9e9
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A visitor's demo sandbox is released soon after its tab closes (REQ-2065): the tab says it is
closing, the sandbox is reaped unless a request reaches it within the grace period, a request
clears the mark, and a named sandbox is never marked."""

# Requirements: REQ-2065

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
import sqlalchemy as sa

from provisa.api import demo_sandbox, demo_sandbox_router
from provisa.api.errors import ApiError
from provisa.core import schema_admin

VISITOR_ORG = "sbx-visitor"
NAMED_ORG = "sbx-named"


@pytest.fixture
def plane(control_plane, monkeypatch):
    monkeypatch.setattr("provisa.api.admin.orgs_router._admin_pool", lambda: control_plane.platform)
    monkeypatch.setattr(demo_sandbox, "_touched", {})
    with control_plane.engine().begin() as conn:
        for org, kind, user in (
            (VISITOR_ORG, "visitor", demo_sandbox.visitor_user_id(VISITOR_ORG)),
            (NAMED_ORG, "named", "alice"),
        ):
            conn.execute(sa.insert(schema_admin.orgs).values(id=org, name=org))
            conn.execute(
                sa.insert(schema_admin.demo_sandboxes).values(
                    org_id=org, demo="sonnets", kind=kind, user_id=user
                )
            )
    return control_plane


def _closing_at(plane, org: str):
    with plane.engine().connect() as conn:
        return conn.execute(
            sa.select(schema_admin.demo_sandboxes.c.closing_at).where(
                schema_admin.demo_sandboxes.c.org_id == org
            )
        ).scalar_one()


async def test_closing_marks_a_visitors_sandbox_and_never_a_named_one(plane):
    await demo_sandbox.mark_closing(VISITOR_ORG)
    await demo_sandbox.mark_closing(NAMED_ORG)
    assert _closing_at(plane, VISITOR_ORG) is not None
    assert _closing_at(plane, NAMED_ORG) is None


async def test_a_request_after_closing_clears_the_mark_without_waiting_out_the_throttle(plane):
    await demo_sandbox.touch(VISITOR_ORG)  # a request just before: the next is throttled...
    await demo_sandbox.mark_closing(VISITOR_ORG)
    await demo_sandbox.touch(VISITOR_ORG)  # ...unless the tab said it was closing
    assert _closing_at(plane, VISITOR_ORG) is None


async def test_a_marked_sandbox_is_reaped_after_the_grace_and_not_within_it(plane):
    await demo_sandbox.mark_closing(VISITOR_ORG)
    now = datetime.now(UTC)
    assert VISITOR_ORG not in await demo_sandbox.expired(now)
    later = now + timedelta(seconds=demo_sandbox.CLOSE_GRACE_S + 1)
    assert VISITOR_ORG in await demo_sandbox.expired(later)
    assert NAMED_ORG not in await demo_sandbox.expired(later)  # idle_ttl still its backstop


@pytest.fixture
def endpoint(plane, monkeypatch):
    monkeypatch.setattr("provisa.api.sandbox_tier.serves_sandboxes", lambda: True)
    monkeypatch.setattr("provisa.api.sandbox_tier.is_sandbox_tier_org", lambda _org: True)
    monkeypatch.setattr("provisa.api.sandbox_tier.is_template_org", lambda _org: False)
    bound = {"org": VISITOR_ORG}
    monkeypatch.setattr("provisa.core.request_context.require_current_org", lambda: bound["org"])

    def call(org: str, user_id: str):
        bound["org"] = org
        request = SimpleNamespace(state=SimpleNamespace(identity=SimpleNamespace(user_id=user_id)))
        return demo_sandbox_router.closing(request)  # type: ignore[arg-type]

    return call


async def test_the_endpoint_marks_the_sandbox_its_visitor_closes(plane, endpoint):
    await endpoint(VISITOR_ORG, demo_sandbox.visitor_user_id(VISITOR_ORG))
    assert _closing_at(plane, VISITOR_ORG) is not None


async def test_the_endpoint_refuses_anyone_but_the_visitor_of_a_visitors_sandbox(plane, endpoint):
    with pytest.raises(ApiError) as refused:
        await endpoint(VISITOR_ORG, "someone-else")
    assert refused.value.status_code == 403
    assert _closing_at(plane, VISITOR_ORG) is None


async def test_the_endpoint_marks_nothing_on_a_named_sandbox(plane, endpoint):
    await endpoint(NAMED_ORG, "alice")
    assert _closing_at(plane, NAMED_ORG) is None
