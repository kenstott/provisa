# Copyright (c) 2026 Kenneth Stott
# Canary: 6f2b04d8-91ce-4a37-8be5-0c73d215aa9f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The environment a redeemed invitation seats its redeemer in (REQ-1595).

Both redemption paths -- ``/register``, where the account is being created, and ``/redeem-invite``,
where it already exists -- ask the same two questions in the same order: what am I to mint for this
person, and if the claim then fails, what do I take back. So both are here, called from both, rather
than being written twice and diverging the first time one of them is fixed.

WHY A RELEASE EXISTS AT ALL. Minting comes BEFORE the membership names the environment, because a
membership written first is unpinned until the provisioning returns -- and an unpinned member is
served production, which for a stranger holding a "Try it Out" link is the one outcome the sandbox
exists to prevent. Paying for that ordering means the loser of a race for the last redemption has
already provisioned a schema, and this is what returns it.
"""

# Requirements: REQ-1594, REQ-1595, REQ-1596

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from provisa.core.org_invite import (
    ENV_POLICY_NONE,
    ENV_POLICY_PER_VISITOR,
    ENV_POLICY_SHARED,
    sandbox_env_name,
)


async def redeem_env(invite: dict, user_id: str) -> str | None:
    """The environment this redemption is to be pinned to, minting it if the invite says so.

    ``none`` pins nothing and the member is served by the org as any member is. ``shared`` names an
    environment the org already published, so there is nothing to create -- every redeemer is seated
    in the one it names. ``per_visitor`` mints a fresh one whose deadline is
    ``env_ttl_seconds`` of DISUSE away (REQ-1600): the routing pushes it out on every request the
    environment serves, the reaper (REQ-1523) drops it on the sweep once it has passed, and
    ``select_environment`` refuses it from the deadline itself rather than from the next sweep.

    Both env-bearing policies deploy from ``prod``, so what the visitor is handed is the org's real
    model rather than an empty schema -- a sandbox with nothing in it demonstrates nothing.
    """
    policy = invite["env_policy"]
    if policy == ENV_POLICY_NONE:
        return None
    if policy == ENV_POLICY_SHARED:
        return invite["env_name"]
    if policy != ENV_POLICY_PER_VISITOR:
        raise ValueError(f"unknown invite env_policy {policy!r}")

    from provisa.api.admin.orgs_router import _org_tenant_db
    from provisa.api.app import state
    from provisa.core.env_create import create_environment
    from provisa.core.environments import PROD

    org_id = invite["org_id"]
    name = sandbox_env_name()
    assert state.admin_db is not None and state.tenant_db is not None
    await create_environment(
        state,
        state.admin_db,
        state.tenant_db,
        await _org_tenant_db(org_id),
        org_id,
        name,
        from_env=PROD,
        created_by=user_id,
        expires_at=datetime.now(tz=timezone.utc) + timedelta(seconds=invite["env_ttl_seconds"]),
        # REQ-1600: and the same span is the environment's idle allowance, so the deadline is
        # measured from the last request it served rather than from this moment. A visitor still
        # working an hour in keeps their environment; one who walked away loses it on schedule.
        idle_ttl_seconds=invite["env_ttl_seconds"],
        branched_from=None,
        note=f"provisioned for {user_id}",
    )
    return name


async def release_env(invite: dict, env_name: str | None) -> None:
    """Undo what :func:`redeem_env` minted, for a redemption that did not complete.

    Only a ``per_visitor`` environment is retired: ``shared`` names one the org published and other
    people are working in, and a failed redemption must not take it down with it. The branch goes
    too -- a visitor's hour of history is not something anyone will come back for, and leaving the
    ref behind accumulates one dead branch per lost race.
    """
    if env_name is None or invite["env_policy"] != ENV_POLICY_PER_VISITOR:
        return

    from provisa.api.app import state
    from provisa.core.env_retire import retire_environment

    assert state.admin_db is not None and state.tenant_db is not None
    await retire_environment(
        state.tenant_db, state.admin_db, invite["org_id"], env_name, drop_branch=True
    )
