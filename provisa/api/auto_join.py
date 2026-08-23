# Copyright (c) 2026 Kenneth Stott
# Canary: efa04005-f955-4739-958a-a95b983a449e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Performing an auto-join, from either the sign-in path or the person's own choice.

REQ-1269 grants membership the moment a rule matches, which is right while exactly ONE org claims
the address. REQ-1568 splits the two halves apart because more than one claim is not something a
sign-in can settle: the address is evidence for both orgs, and whichever the resolver read first
would take the person while the other never saw them. So the sign-in path joins one match and stops
at several, and the endpoints below carry the choice the person then makes.

The act itself is the same either way and lives here once: membership on the control plane, the
org's default role inside the org's own schema, and the org's running trial bound to the newcomer.
"""

# Requirements: REQ-1269, REQ-1306, REQ-1474, REQ-1568

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from provisa.core.database import Database


async def join_org_automatically(
    admin_pool: "Database", user_id: str, email: str | None, org_id: str, role_id: str
) -> None:
    """Grant ``user_id`` membership of ``org_id`` and its auto-join role.

    The role is granted inside the ORG's schema, which is why the org's runtime is ensured first:
    membership on the control plane alone would seat someone in an org where they can do nothing.
    """
    from provisa.api.app import ensure_org_runtime
    from provisa.api.org_runtime import reset_current_org, set_current_org
    from provisa.core.commerce import bind_member_to_org_trial
    from provisa.core.org_membership import JOINED_VIA_AUTO_JOIN, grant_membership, grant_org_role

    await grant_membership(admin_pool, user_id, org_id, joined_via=JOINED_VIA_AUTO_JOIN)
    # REQ-1474: an auto-joined member works under the org's trial if it is running, so the trial is
    # spent for them too.
    await bind_member_to_org_trial(admin_pool, org_id, email)
    rt = await ensure_org_runtime(org_id)
    if rt.tenant_db is not None:
        org_token = set_current_org(org_id)
        try:
            await grant_org_role(rt.tenant_db, user_id, role_id)
        finally:
            reset_current_org(org_token)
