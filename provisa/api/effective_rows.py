# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A person's effective rows recomputed in a named org, after a role grant there changed
(REQ-2219).

A membership, an invitation, an org's creation or a person's removal writes role grants into an
org that is often not the one the request is bound to (an auto-join at sign-in, a platform
administrator acting on another org, a claim that acts in no org). The rows are computed from that
org's prod runtime, so it is bound here for the computation, and written to its model store.
"""

# Requirements: REQ-2219

from __future__ import annotations

from collections.abc import Iterable


async def record_people(org_id: str, principals: Iterable[str]) -> None:
    """Recompute each of ``principals``' effective rows in ``org_id``'s prod model, after a role
    grant there was written or removed."""
    from provisa.api.app import ensure_org_runtime, state
    from provisa.core.request_context import (
        reset_current_env,
        reset_current_org,
        set_current_env,
        set_current_org,
    )
    from provisa.entitlements import effective_store

    people = sorted(set(principals))
    if not people:
        return
    rt = await ensure_org_runtime(org_id)
    if rt.model_db is None:
        raise RuntimeError(f"org {org_id!r} has no model store to record effective rows in")
    org_token = set_current_org(org_id)
    env_token = set_current_env(None)
    try:
        async with rt.model_db.acquire() as conn:
            for principal in people:
                await effective_store.write_person(conn, state, principal)
    finally:
        reset_current_env(env_token)
        reset_current_org(org_token)


async def record_removed_account(removed: dict) -> None:
    """After an account's removal (``removed``, as ``org_membership.remove_account`` returns it):
    it left every org it was in, its role grants with it, and its effective rows there are
    recomputed."""
    for org_id in removed["left_orgs"]:
        await record_people(org_id, [removed["deleted"]])
