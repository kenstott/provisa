# Copyright (c) 2026 Kenneth Stott
# Canary: 8d31b70a-45e9-4c62-b1d7-9f0263ea4c15
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Creating an environment by deploying a model into a name (REQ-1488).

There is no create-then-populate step: reserving the name, provisioning the schema and its stores,
and carrying the model in are ONE act, and if any part of it fails the parts that landed are undone
-- an environment that exists and holds half a model is worse than one that does not exist.

WHY IT IS NOT INSIDE THE ROUTER. Two callers now create environments. ``POST /admin/orgs/{id}/
environments`` is one; redeeming an open invitation whose ``env_policy`` is ``per_visitor``
(REQ-1595) is the other, and it reaches this from the auth plane with no request to guard. Leaving
the sequence in the router would have made the second caller a second sequence, and the ordering
here -- reserve before provision, seed before the first ref, rollback on any BaseException -- is
exactly the part that must not be re-derived.

The guard is NOT here. Who may create an environment is a question about the caller, and the two
callers answer it differently: the endpoint requires a right in the org, while the invitation IS
the authorization, granted when the org minted the link.
"""

# Requirements: REQ-1488, REQ-1523, REQ-1526, REQ-1539, REQ-1543, REQ-1595, REQ-1600

from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

from provisa.core.env_copy import REPLACE, CopyReport, copy_model
from provisa.core.env_store import forget_env, reserve_env

if TYPE_CHECKING:
    from provisa.core.database import Database


def _schema_sql() -> str:
    """The tenant DDL a new environment's schema is built from."""
    path = Path(__file__).parent / "schema.sql"
    return path.read_text() if path.exists() else ""


async def create_environment(
    state,
    admin_db: "Database",
    tenant_pool: "Database",
    tenant_db: "Database",
    org_id: str,
    name: str,
    *,
    from_env: str,
    created_by: str | None,
    expires_at: datetime | None,
    idle_ttl_seconds: int | None = None,
    branched_from: str | None,
    note: str,
) -> CopyReport:
    """Reserve ``name``, provision it, and deploy ``from_env``'s model into it.

    Raises through: ``EnvironmentNameError`` and ``EnvironmentLimitError`` from the reservation,
    anything the provisioning or the copy raises. Every caller renders those; none of them is
    swallowed here, and a raised error leaves nothing behind.
    """
    from provisa.core.org_provisioning import deprovision_org, provision_org

    await reserve_env(
        state,
        admin_db,
        org_id,
        name,
        created_by=created_by,
        expires_at=expires_at,
        idle_ttl_seconds=idle_ttl_seconds,
        branched_from=branched_from,
    )
    try:
        await provision_org(
            tenant_pool,
            _schema_sql(),
            org_id=org_id,
            redis_url=os.environ.get("REDIS_URL"),
            redis_password=os.environ.get("PROVISA_REDIS_ORG_PASSWORD"),
            env=name,
        )
        report = await copy_model(
            tenant_db,
            org_id,
            from_env,
            name,
            mode=REPLACE,
            # REQ-1539: the only call that seeds roles and assignments. A new environment needs
            # them to be usable at all; every later copy leaves the target's own answer alone.
            seed=True,
        )
        # REQ-1602: ensure sandbox role is present in ephemeral environments for sandbox org
        if org_id == "sandbox":
            from provisa.core.schema_org import roles
            from sqlalchemy import select

            async with tenant_db.acquire() as conn:
                # Check if sandbox role already exists in this environment
                result = await conn.execute_core(select(roles.c.id).where(roles.c.id == "sandbox"))
                if result.fetchone() is None:
                    # Get sandbox role from prod to copy its capabilities

                    prod_result = await conn.execute_core(
                        select(roles.c.capabilities).where(roles.c.id == "sandbox")
                    )
                    sandbox_caps = prod_result.fetchone()
                    if sandbox_caps is not None:
                        # Insert sandbox role with capabilities from prod
                        await conn.execute_core(
                            roles.insert().values(id="sandbox", capabilities=sandbox_caps[0])
                        )
        # REQ-1543: the environment's history starts HERE, where the source it was created from is
        # standing. Without it the branch has no ref, the first edit somebody makes becomes the
        # first commit of the line, and an undo of that edit has no parent to step back to -- the
        # change would be unundoable precisely because it was the first one.
        #
        # The ref is seeded from the source BEFORE the write-through so the new environment's line
        # continues the source's instead of being a root commit no merge could find a base in. The
        # position is recorded against that same sha: the model was just copied, so an identical
        # tree writes no commit (REQ-1526) and the write-through would otherwise leave the
        # environment standing nowhere while its branch stood somewhere.
        from provisa.core.env_repo import ensure_repo, start_branch, write_through
        from provisa.core.env_store import set_origin, set_position
        from provisa.core.environments import org_schema

        started = start_branch(ensure_repo(org_id), name, from_env)
        if started is not None:
            await set_position(admin_db, org_id, name, deployed_sha=started, redo_sha=None)
            # And that sha is the FLOOR of this environment's history: the commits at and below it
            # are the source's, trees this environment never held. Without it a branch created and
            # then changed once offered two undos -- the change, and then a step onto the source's
            # model wearing this environment's name.
            await set_origin(admin_db, org_id, name, started)
        async with tenant_db.acquire() as conn:
            await write_through(
                conn,
                admin_db,
                org_id,
                name,
                org_schema(org_id, name),
                note,
                created_by,
            )
    except BaseException:
        # Compensating rollback, mirroring provision_org's own: an environment that exists and
        # holds part of a model is worse than one that was never created. The failure is re-raised,
        # never swallowed.
        await deprovision_org(tenant_pool, org_id, env=name)
        await forget_env(admin_db, org_id, name)
        raise
    return report
