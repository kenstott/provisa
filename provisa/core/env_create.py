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

# Requirements: REQ-1488, REQ-1523, REQ-1526, REQ-1539, REQ-1543, REQ-1595, REQ-1600, REQ-1620

from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

from provisa.core.env_copy import REPLACE, CopyReport, adopt_role_definition, copy_model
from provisa.core.env_source_files import fork_file_sources
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
    strip_identities: bool = True,
    define_role_from: tuple[str, str] | None = None,
) -> CopyReport:
    """Reserve ``name``, provision it, and deploy ``from_env``'s model into it.

    Raises through: ``EnvironmentNameError`` and ``EnvironmentLimitError`` from the reservation,
    anything the provisioning or the copy raises. Every caller renders those; none of them is
    swallowed here, and a raised error leaves nothing behind.

    ``strip_identities`` is REQ-1491's convenience by default (an IDENTITY_ONLY row lands unbound,
    stripped of the source's connection details). REQ-1602's sandbox visitor environments pass
    ``strip_identities=False`` so the copy carries the real, already-bound connections verbatim --
    a visitor gets a working demo, not an environment it would first have to bind itself.

    ``expires_at`` is what makes the environment ephemeral, and REQ-1620 hangs off it rather than
    off ``strip_identities``: pointing at any data source, prod's included, is an environment's own
    business, and only an environment that is going to be deleted needs its file-backed sources
    forked into copies that go with it.

    ``define_role_from`` is ``(target, source)``: after the model lands, ``target``'s row in the NEW
    environment takes ``source``'s capabilities and demonstrated list. REQ-1602's sandbox visitor is
    the only caller -- it holds ``org_admin`` alongside its own role because the copied model's
    column grants name that role and not ``sandbox``, and the withholding REQ-1597 describes is
    applied to that name here. Inside the try, so a failure takes the half-made environment with it
    rather than leaving a visitor holding an unrestricted ``org_admin``.
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
            strip_identities=strip_identities,
        )
        if expires_at is not None:
            # REQ-1620: an EPHEMERAL environment, and only that one. Every other environment is a
            # first-class place with its own features, free to point at any data source it likes --
            # including, deliberately, the same file prod reads. What distinguishes this one is that
            # it is thrown away: the copy above carried the bindings verbatim, a file-backed
            # source's binding is a path to a file on this deployment's disk that the connector
            # attaches read-write, and an UPDATE issued here would outlive the environment that
            # issued it. So it gets its own copies, and they go when it does.
            #
            # Inside the try, so the environment is bound to its own files before any session can be
            # handed against it, and a failure takes the half-made environment rather than leaving
            # it pointed at the original. A stripped copy lands unbound (REQ-1491) with no path to
            # fork, so this is a no-op there without needing to ask.
            await fork_file_sources(tenant_db, org_id, name)
        if define_role_from is not None:
            _target, _source = define_role_from
            await adopt_role_definition(tenant_db, org_id, name, target=_target, source=_source)
        # REQ-1602: the sandbox role reaching every ephemeral environment of the sandbox org is
        # already the copy above's job -- seed=True carries SEEDED_AT_CREATION tables (roles among
        # them) from from_env (always PROD for this caller), and PROD's own row is guaranteed
        # current because the invite-redemption callers build PROD's runtime (re-running the
        # idempotent schema.sql seed) before ever reaching this function. A second reconciliation
        # here duplicated that guarantee and got it wrong: it queried through tenant_db, which is
        # bound to PROD's schema (ensure_org_runtime with no env), so both its "does the new
        # environment already have it" check and its "insert" ran against PROD, never the
        # newly-created environment.
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
