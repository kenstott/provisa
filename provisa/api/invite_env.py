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

# Requirements: REQ-1594, REQ-1595, REQ-1596, REQ-1602, REQ-1615

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from provisa.core.org_invite import (
    ENV_POLICY_NONE,
    ENV_POLICY_PER_VISITOR,
    ENV_POLICY_SHARED,
    SANDBOX_ROLE,
    sandbox_env_name,
)


@dataclass(frozen=True)
class RedeemedEnv:
    """The environment a redemption resolved to, and whether THIS redemption is what made it.

    REQ-1615: the two are not the same question. A sandbox visitor's environment is named after the
    visitor (REQ-1602), so a returning one resolves to an environment that already exists and was
    not created here -- and :func:`release_env` must not take that environment down when a later
    step of the redemption fails, because what it would be destroying is the visitor's own work
    from a previous session rather than the half-finished product of this call.
    """

    name: str | None
    minted: bool


async def redeem_env(invite: dict, user_id: str) -> RedeemedEnv:
    """The environment this redemption is to be pinned to, minting it if the invite says so.

    ``none`` pins nothing and the member is served by the org as any member is. ``shared`` names an
    environment the org already published, so there is nothing to create -- every redeemer is seated
    in the one it names. ``per_visitor`` mints a fresh one whose deadline is
    ``env_ttl_seconds`` of DISUSE away (REQ-1600): the routing pushes it out on every request the
    environment serves, the reaper (REQ-1523) drops it on the sweep once it has passed, and
    ``select_environment`` refuses it from the deadline itself rather than from the next sweep.

    A ``per_visitor`` environment deploys from the env the invite names (``env_name``): the invite is
    addressed to whatever the inviter was looking at when they minted the link (REQ-1602), and
    ``create_invite`` captures that from the inviter's own ``active_env()`` unconditionally, so the
    column is never empty for this policy. ``shared`` deploys from nowhere: it names an environment
    that already exists.

    REQ-1602: the sandbox org creates per-user ephemeral environments (named ephemeral_<user_id>)
    so auth middleware can auto-select based on user_id without UI/routing complexity.
    """
    from provisa.api.sandbox_org import SANDBOX_ORG_ID

    policy = invite["env_policy"]
    # REQ-1602: sandbox org always creates per-user ephemeral environments
    org_id = invite["org_id"]
    if org_id == SANDBOX_ORG_ID:
        policy = ENV_POLICY_PER_VISITOR
    if policy == ENV_POLICY_NONE:
        return RedeemedEnv(None, minted=False)
    if policy == ENV_POLICY_SHARED:
        return RedeemedEnv(invite["env_name"], minted=False)
    if policy != ENV_POLICY_PER_VISITOR:
        raise ValueError(f"unknown invite env_policy {policy!r}")

    from provisa.api.admin.orgs_router import _org_tenant_db
    from provisa.api.app import state
    from provisa.core.env_create import create_environment
    from provisa.core.env_store import get_env, set_expiry

    org_id = invite["org_id"]
    if org_id == SANDBOX_ORG_ID:
        # Hash user_id to fit 32-char env name limit: ephemeral_<8-char-hash>
        import hashlib

        user_hash = hashlib.md5(user_id.encode(), usedforsecurity=False).hexdigest()[:8]
        name = f"ephemeral_{user_hash}"
    else:
        name = sandbox_env_name()
    # REQ-1602: the invite is addressed to whatever the inviter was looking at when they minted it --
    # ``create_invite`` captures that into ``env_name`` unconditionally for a per_visitor invite, so
    # it is never absent here.
    source_env = invite["env_name"]
    import logging

    logger = logging.getLogger(__name__)
    logger.info(f"redeem_env: org_id={org_id}, user_id={user_id}, name={name}, policy={policy}")
    assert state.admin_db is not None and state.tenant_db is not None

    # REQ-1615: a sandbox name IS its visitor (REQ-1602), so a visitor who redeems a second link --
    # or reopens the first -- resolves to the environment they already have, and there is nothing
    # to mint. Creating it unconditionally made that a name collision, and the collision reached
    # the visitor as a 500 on sign-in: their second visit failed on the existence of their first.
    # Reuse rather than replace, because the environment holds whatever they did last session, and
    # the deadline moves out by the invite's TTL because the visit that just arrived is use
    # (REQ-1600) -- an environment resolved here is one about to be served.
    existing = await get_env(state.admin_db, org_id, name)
    if existing is not None:
        deadline = datetime.now(tz=timezone.utc) + timedelta(seconds=invite["env_ttl_seconds"])
        await set_expiry(state.admin_db, org_id, name, deadline)
        logger.info(f"redeem_env: reusing existing environment {name} for {user_id}")
        return RedeemedEnv(name, minted=False)

    try:
        logger.info(f"Creating environment {name}...")
        await create_environment(
            state,
            state.admin_db,
            state.tenant_db,
            await _org_tenant_db(org_id),
            org_id,
            name,
            from_env=source_env,
            created_by=user_id,
            expires_at=datetime.now(tz=timezone.utc) + timedelta(seconds=invite["env_ttl_seconds"]),
            # REQ-1600: and the same span is the environment's idle allowance, so the deadline is
            # measured from the last request it served rather than from this moment. A visitor still
            # working an hour in keeps their environment; one who walked away loses it on schedule.
            idle_ttl_seconds=invite["env_ttl_seconds"],
            branched_from=source_env,
            note=f"provisioned for {user_id}",
            # REQ-1602: a visitor's environment never binds its own sources, so it needs the source
            # env's real connections copied in at creation, not stripped -- the ordinary REQ-1491
            # convenience would otherwise leave every source unbound and every table unreachable.
            strip_identities=False,
            # REQ-1597: a `sandbox` invitation's redeemer also holds `org_admin` here (see
            # `seat_redeemed_roles`), so the withholding is applied to THAT name, in this
            # environment only, by reading it off this environment's own `sandbox` row. Union of
            # the two assignments is then the sandbox definition and nothing wider.
            define_role_from=(
                ("org_admin", SANDBOX_ROLE) if invite["role_id"] == SANDBOX_ROLE else None
            ),
        )
        logger.info(f"Environment {name} created successfully")
    except Exception as e:
        logger.error(f"Failed to create environment {name} for {org_id}: {e}", exc_info=True)
        raise
    return RedeemedEnv(name, minted=True)


async def seat_redeemed_roles(tenant_db, user_id: str, role_id: str) -> None:
    """Write the role assignments a redemption confers, into the environment it was pinned to.

    ``tenant_db`` is the redeemer's OWN environment's runtime, not prod's -- an env-bearing invite
    (REQ-1595) resolves roles out of the schema it pinned the member to, so an assignment written to
    prod is one the redeemer never has.

    The invitation's own role is always granted. ``sandbox`` grants a SECOND assignment, ``org_admin``,
    and that is the whole of REQ-1597 that could not be expressed as a role id: a column is visible to
    a role when ``table_columns.visible_to`` NAMES it (``schema_gen._build_visible_tables``), and an
    authored model's grants name the roles it was authored against. ``sandbox`` is named by none of
    them, so on its own it is shown no column on any table -- and ``schema_gen`` then drops every
    table left with none, leaving a visitor the tables whose API path/query parameters are exempt
    from the gate and nothing else. Not a smaller sandbox: an empty one.

    So the visitor also holds the name the grants carry. It is not a widening, because
    ``create_environment`` has already reduced ``org_admin`` IN THIS ENVIRONMENT to the ``sandbox``
    row's own capabilities (``define_role_from``), and capabilities resolve as the union over the
    holder's roles -- so the union here is the sandbox definition exactly. Prod's ``org_admin`` is
    untouched, and REQ-1596's pin is what keeps the member out of any environment where it is not.
    """
    from provisa.core.org_membership import grant_org_role

    await grant_org_role(tenant_db, user_id, role_id)
    if role_id == SANDBOX_ROLE:
        await grant_org_role(tenant_db, user_id, "org_admin")


async def release_env(invite: dict, redeemed: RedeemedEnv) -> None:
    """Undo what :func:`redeem_env` minted, for a redemption that did not complete.

    ``minted`` is the whole test, and it is exactly the environments this may take down. A
    ``shared`` invite names one the org published and other people are working in; a returning
    sandbox visitor (REQ-1615) resolves to the environment they already had, which this call did
    not create and whose contents predate it. Neither was minted here, and retiring either would
    answer a failed redemption by deleting something that was never this redemption's to make.

    What it does retire is the per_visitor environment this call provisioned. The branch goes too
    -- a visitor's hour of history is not something anyone will come back for, and leaving the ref
    behind accumulates one dead branch per lost race.
    """
    if not redeemed.minted:
        return
    assert redeemed.name is not None  # minted is set only where a name was created
    env_name = redeemed.name

    from provisa.api.app import state
    from provisa.core.env_retire import retire_environment

    assert state.admin_db is not None and state.tenant_db is not None
    await retire_environment(
        state.tenant_db, state.admin_db, invite["org_id"], env_name, drop_branch=True
    )
