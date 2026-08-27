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
"""

# Requirements: REQ-1598

from __future__ import annotations

import logging

from sqlalchemy import select, update

from provisa.core.schema_admin import orgs

log = logging.getLogger(__name__)

SANDBOX_ORG_ID = "sandbox"
SANDBOX_ORG_NAME = "Sandbox"


async def ensure_sandbox_org(pool) -> str:
    """Seed and build the sandbox org if this boot does not already find it ready.

    Returns what the boot did: ``"ready"`` when the org was already built and nothing was
    started, or ``"building"`` when provisioning was spawned. Any state other than ready is
    rebuilt -- a row left at ``provisioning`` is a process that died mid-build, and a row at
    ``awaiting_checkout`` or ``failed`` is an org the platform still owes its visitors. The
    build is the create path's own background task, so startup does not wait on a Trino shard
    waking; ``provisioning_state`` is where its progress is read.
    """
    from provisa.api.admin.orgs_router import _spawn_provisioning
    from provisa.core.commerce import entitle_starter

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
            return "ready"
        else:
            await conn.execute_core(
                update(orgs)
                .where(orgs.c.id == SANDBOX_ORG_ID)
                .values(provisioning_state="provisioning", provisioning_error=None)
            )

    await entitle_starter(pool, SANDBOX_ORG_ID)
    # created_by is None: the sandbox org has no owner account to grant org_admin to. Its members
    # arrive by redeeming the open invite, and the role that invite carries is ``sandbox``.
    _spawn_provisioning(SANDBOX_ORG_ID, True, None, False)
    log.info("sandbox org %s: provisioning", SANDBOX_ORG_ID)
    return "building"
