# Copyright (c) 2026 Kenneth Stott
# Canary: 9c4a71e8-5b60-42df-a0d7-3f18b6e2c541
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The hosted platform's own sandbox org, built at startup (REQ-1598).

The public "Try it Out" invite (REQ-1594/REQ-1595) is an invite INTO an org, and the org it names is
``sandbox``. An org a visitor is admitted to cannot be one an operator remembered to create by hand:
the invite is on the sign-in page of every deployment of the hosted platform, and a deployment whose
sandbox org is missing serves a redemption that fails. So it is seeded the way the bootstrap org is
-- by startup, idempotently, on every boot.

It is an ORDINARY org otherwise: the same ``provision_org`` build every self-service org gets, the
demo data set seeded into it (an empty sandbox is nothing to try), the shared engine lane, and the
Starter plan's ceilings. What it is NOT is a sold one -- nobody buys the sandbox, so the checkout
gate a commercial deployment puts in front of org creation (REQ-1476) is not in this path, and the
plan is set through the commerce seam rather than left at the trial a subscription would open.

REQ-1599 adds the operator's own way in. The sandbox is a tenant org like any other, and
platform_admin confers NOTHING inside a tenant org -- it is stripped there (REQ-1297) and everyone,
operators included, is confined to orgs they are a member of (REQ-1327). So the deployment's
administrators are seated in it the ordinary way, with a membership and a role granted inside its
schema.
"""

# Requirements: REQ-1598, REQ-1599

from __future__ import annotations

import asyncio
import logging

from sqlalchemy import select, update

from provisa.core.schema_admin import orgs

log = logging.getLogger(__name__)

SANDBOX_ORG_ID = "sandbox"
SANDBOX_ORG_NAME = "Sandbox"


async def ensure_sandbox_org(pool) -> str:
    """Seed and build the sandbox org if this boot does not already find it ready.

    Returns what the boot did: ``"ready"`` when the org was already built and only its
    administrators were reconciled (REQ-1599), or ``"building"`` when provisioning was spawned. Any state other than ready is
    rebuilt -- a row left at ``provisioning`` is a process that died mid-build, and a row at
    ``awaiting_checkout`` or ``failed`` is an org the platform still owes its visitors. The
    build is the create path's own background task, so startup does not wait on a Trino shard
    waking; ``provisioning_state`` is where its progress is read.
    """
    from provisa.core.commerce import entitle_starter

    ready = False
    async with pool.acquire() as conn:
        result = await conn.execute_core(
            select(orgs.c.provisioning_state).where(orgs.c.id == SANDBOX_ORG_ID)
        )
        row = result.fetchone()
        if row is None:
            await conn.execute_core(
                orgs.insert().values(
                    id=SANDBOX_ORG_ID,
                    name=SANDBOX_ORG_NAME,
                    created_by=None,
                    provisioning_state="provisioning",
                    seeded_demo=True,
                )
            )
        elif row[0] == "ready":
            ready = True
        else:
            await conn.execute_core(
                update(orgs)
                .where(orgs.c.id == SANDBOX_ORG_ID)
                .values(provisioning_state="provisioning", provisioning_error=None)
            )

    if ready:
        # REQ-1599: nothing to build, but an administrator conferred before this org existed is
        # still owed a seat in it, and this boot is where they get one.
        await seat_platform_admins(pool)
        return "ready"

    await entitle_starter(pool, SANDBOX_ORG_ID)
    _spawn_build(pool)
    log.info("sandbox org %s: provisioning", SANDBOX_ORG_ID)
    return "building"


_build_tasks: set[asyncio.Task] = set()


def _spawn_build(pool) -> None:
    """Run the create path's own build, then seat the deployment's administrators in it.

    The seating is inside the task rather than after it because it needs a schema: an operator who
    claimed the platform-admin slot while this build was still running is seated by the line below
    when it finishes, and one who claims it afterwards is seated by the conferral itself. Between
    them there is no boot on which an administrator is left outside the org.
    """
    from provisa.api.admin.orgs_router import _provision_org_task

    async def _build() -> None:
        # created_by is None: the sandbox org has no owner account to grant org_admin to. Its
        # ordinary members arrive by redeeming the open invite, and the role that invite carries is
        # ``sandbox`` (REQ-1597).
        await _provision_org_task(SANDBOX_ORG_ID, True, None, False)
        await seat_platform_admins(pool)

    task = asyncio.create_task(_build())
    _build_tasks.add(task)
    task.add_done_callback(_build_tasks.discard)


async def seat_platform_admins(pool) -> int:
    """Seat every platform_admin of this deployment in the sandbox org as its org_admin (REQ-1599).

    An operator answering for what the "Try it Out" link shows a stranger could not open the org it
    admits them to: platform_admin is a control-plane role that holds no data capability in any
    tenant org, and the sandbox is a tenant org. The right they need is therefore the ordinary one
    -- membership on the control plane, a role inside the sandbox's own schema -- and the role is
    ``org_admin`` rather than ``sandbox``, because an operator has to reach the environments, the
    members and the settings a visitor's role deliberately withholds (REQ-1597).

    Unpinned, unlike a visitor's membership: the pin (REQ-1596) is what confines a stranger to the
    environment minted for them, and an operator is not a stranger.

    Returns how many administrators were seated. Zero when the sandbox org is not ready -- a
    deployment that has none has no operator to seat there either, and one still building is
    seated by :func:`_spawn_build` the moment its schema exists.
    """
    from provisa.api.app import ensure_org_runtime, state
    from provisa.api.org_runtime import reset_current_org, set_current_org
    from provisa.core.org_membership import JOINED_VIA_ADMIN, grant_org_admin
    from provisa.core.schema_org import user_role_assignments
    from provisa.security.rights import PLATFORM_ADMIN_ROLE

    async with pool.acquire() as conn:
        result = await conn.execute_core(
            select(orgs.c.provisioning_state).where(orgs.c.id == SANDBOX_ORG_ID)
        )
        row = result.fetchone()
    if row is None or row[0] != "ready":
        return 0

    # The assignments live in the ROOT org's schema: platform_admin is only ever conferred there
    # (an invitation carrying it is refused for any other org), so that one schema is the whole
    # list of this deployment's administrators.
    root_db = state.tenant_db
    if root_db is None:
        raise RuntimeError(
            "REQ-1599: the sandbox org is ready, so the root org's tenant plane is up — "
            "reading its platform_admin assignments cannot find it unbound"
        )
    async with root_db.acquire() as conn:
        result = await conn.execute_core(
            select(user_role_assignments.c.user_id).where(
                user_role_assignments.c.role_id == PLATFORM_ADMIN_ROLE
            )
        )
        admins = sorted({r[0] for r in result.fetchall()})
    if not admins:
        return 0

    rt = await ensure_org_runtime(SANDBOX_ORG_ID)
    if rt.tenant_db is None:
        raise RuntimeError(
            f"REQ-1599: the sandbox org is ready but {SANDBOX_ORG_ID} has no runtime"
        )
    token = set_current_org(SANDBOX_ORG_ID)
    try:
        for user_id in admins:
            await grant_org_admin(
                pool, rt.tenant_db, user_id, SANDBOX_ORG_ID, joined_via=JOINED_VIA_ADMIN
            )
    finally:
        reset_current_org(token)
    log.info("sandbox org %s: seated %d platform admin(s)", SANDBOX_ORG_ID, len(admins))
    return len(admins)


async def reseat_after_conferral(pool) -> None:
    """Reconcile the sandbox's administrators after any role conferral (REQ-1599).

    Called from every path that confers a role, so an administrator created between boots reaches
    the sandbox on the conferral rather than on the next restart. It does not ask WHICH role was
    just granted -- that would be a gate on a role name (REQ-1337), and the answer is already in
    the assignments :func:`seat_platform_admins` reads. Idempotent, and a no-op for a deployment
    with no sandbox org.
    """
    await seat_platform_admins(pool)
