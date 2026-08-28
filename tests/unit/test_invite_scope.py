# Copyright (c) 2026 Kenneth Stott
# Canary: 43524bc9-5ea3-4758-ae95-bc257bdf0cec
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1604: whose invitations an invite listing shows.

An invite token is a live credential -- it grants an account and a role in the org that issued it.
The listing is therefore scoped to the org the request is bound to, for every caller. cross_org
(REQ-1318) is the right to act in any org, one at a time, and not a right to read every org's
invitations at once: with it unscoped, the operator's page showed another org's live tokens.
"""

# Requirements: REQ-1604, REQ-1266, REQ-1318

from types import SimpleNamespace

import pytest

from provisa.api.admin import invites_router
from provisa.api.errors import ApiError


def _request(caps, active_org, monkeypatch, user_id="u1"):
    monkeypatch.setattr(invites_router, "can_act_cross_org", lambda c: "cross_org" in c)
    monkeypatch.setattr(
        "provisa.api.admin.capabilities._resolved_capabilities", lambda _i, _s: caps
    )
    return SimpleNamespace(
        state=SimpleNamespace(identity=SimpleNamespace(user_id=user_id), active_org_id=active_org)
    )


class TestScope:
    async def test_a_cross_org_caller_sees_the_org_they_are_bound_to(self, monkeypatch):
        req = _request({"cross_org", "user_management"}, "default", monkeypatch)
        assert await invites_router._administered_org_scope(req) == "default"

    async def test_a_cross_org_caller_with_no_bound_org_scopes_to_nothing(self, monkeypatch):
        # The platform plane (/auth, /admin/orgs) binds no org; there is no org to scope to yet.
        req = _request({"cross_org"}, None, monkeypatch)
        assert await invites_router._administered_org_scope(req) is None

    async def test_an_org_admin_sees_their_own_org(self, monkeypatch):
        req = _request({"user_management"}, "acme", monkeypatch)
        assert await invites_router._administered_org_scope(req) == "acme"

    async def test_an_org_admin_with_no_bound_org_is_refused(self, monkeypatch):
        req = _request({"user_management"}, None, monkeypatch)
        with pytest.raises(ApiError) as exc:
            await invites_router._administered_org_scope(req)
        assert exc.value.status_code == 403

    async def test_a_member_without_user_management_is_refused(self, monkeypatch):
        req = _request({"read"}, "acme", monkeypatch)
        with pytest.raises(ApiError) as exc:
            await invites_router._administered_org_scope(req)
        assert exc.value.status_code == 403

    async def test_an_unauthenticated_dev_request_is_unscoped(self, monkeypatch):
        req = _request(set(), "acme", monkeypatch, user_id="anonymous")
        assert await invites_router._administered_org_scope(req) is None
