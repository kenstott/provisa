# Copyright (c) 2026 Kenneth Stott
# Canary: 1a7f3c05-9e4b-4d2a-8f61-3b0c7d5e9a24
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Where an environment's sources actually point, when it did not bind them itself (REQ-1529).

An org's environments come in two kinds. A BASE is created by an org_admin, bound with its own
credentials and granted membership; a BRANCH is created by a member FROM a base and inherits that
base's bindings BY REFERENCE. Nothing is copied by the inheritance: a branch's ``sources`` row is
the unbound row REQ-1491 produces, and the connection values are read from the base at the moment
they are needed. Rotation on the base is therefore picked up by every branch of it with no action
of their own, revocation revokes for all of them at once, and no secret is ever materialized
anywhere a branch, an export or a repository could carry it away.

WRITING IS NOT A QUESTION THIS MODULE ANSWERS (REQ-1539). An inherited binding once carried a
``branch_writable`` policy set on the environment that supplied it, and a write through the binding
was refused unless that policy allowed it. It is gone. Whether a person may write is decided by the
roles they hold, identically in every environment — an environment is a namespace for the model, not
a second permission system stacked on the one that already answers that question. What made the
policy look necessary was REQ-1528 conferring the ``developer`` role on the creator of an
environment, ``write`` included; that is corrected where it arose, in
:data:`provisa.core.env_authority.ENVIRONMENT_OWNER_CAPABILITIES`, which now confers no data right
at all.

RESOLUTION WALKS UP, one environment at a time, and stops at the first that says it is bound. That
single rule covers both cases without a second code path: an environment that bound a source itself
resolves to itself, and one that did not resolves to whichever ancestor did.
"""

# Requirements: REQ-1489, REQ-1491, REQ-1492, REQ-1528, REQ-1529, REQ-1539

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import MetaData, Table, select

from provisa.core.env_classes import BOUND_COLUMN, IDENTITY_ONLY, binding_columns
from provisa.core.environments import PROD, org_schema
from provisa.core.schema_admin import environments
from provisa.core.schema_org import metadata as org_metadata

if TYPE_CHECKING:
    from provisa.core.database import Database

#: How far a resolution follows ``branched_from`` before it declares the chain broken. The column is
#: acyclic by construction — a branch names an environment that already exists and can never be
#: repointed — so reaching this depth is a corrupted registry, not a deep tree, and it is raised
#: rather than truncated.
MAX_DEPTH = 32


class BindingError(Exception):
    """A binding that cannot be resolved or a write that the binding does not permit."""


async def _row(admin_db: "Database", org_id: str, env: str) -> dict:
    async with admin_db.acquire() as conn:
        result = await conn.execute_core(
            select(environments).where(environments.c.org_id == org_id, environments.c.name == env)
        )
        row = result.fetchone()
    if row is None:
        raise BindingError(f"organization {org_id!r} has no environment {env!r}")
    return dict(row._mapping)


async def lineage(admin_db: "Database", org_id: str, env: str) -> list[str]:
    """``env`` and every base above it, nearest first, ending at the base that branched from none.

    A CYCLE IS NAMED AS A CYCLE. ``branched_from`` is acyclic by construction — a branch names an
    environment that already exists and can never be repointed — so a repeated name means the
    registry has been corrupted, and saying which environment closed the ring is what an operator
    needs to unpick it. Left to the depth limit alone the same corruption would surface as "deeper
    than 32", which describes a tree nobody has and sends the reader looking for one.
    """
    chain = [env]
    seen = {env}
    current = await _row(admin_db, org_id, env)
    while current["branched_from"] is not None:
        parent = current["branched_from"]
        if parent in seen:
            raise BindingError(
                f"environment {env!r} of organization {org_id!r} has a cyclic branch chain: "
                f"{' -> '.join(chain)} -> {parent}. branched_from is written once, at branch time, "
                f"against an environment that already exists, so this is a corrupted registry"
            )
        if len(chain) > MAX_DEPTH:
            raise BindingError(
                f"environment {env!r} of organization {org_id!r} has a branch chain deeper than "
                f"{MAX_DEPTH}, which a registry written by branching cannot produce"
            )
        chain.append(parent)
        seen.add(parent)
        current = await _row(admin_db, org_id, parent)
    return chain


async def base_of(admin_db: "Database", org_id: str, env: str) -> str:
    """The environment at the root of ``env``'s branch chain — itself, when it is a base."""
    return (await lineage(admin_db, org_id, env))[-1]


def _scoped(table: Table, schema: str) -> Table:
    return table.to_metadata(MetaData(schema=schema), schema=schema)


async def resolve(
    admin_db: "Database",
    tenant_db: "Database",
    org_id: str,
    env: str,
    table: str,
    key: str,
) -> tuple[str, dict]:
    """Which environment supplies ``table``/``key``'s connection, and the values it supplies.

    Walks ``env``'s lineage and stops at the first environment whose row says it is bound. The row
    must exist in every environment on the way — an IDENTITY_ONLY row travels with the model
    (REQ-1491), so an environment holding the model holds the row — and a lineage where nobody is
    bound is an unbound source, which is a refusal and not an empty connection.
    """
    if table not in IDENTITY_ONLY:
        raise BindingError(f"{table!r} holds no bindings; only an IDENTITY_ONLY table does")
    source = org_metadata.tables[table]
    wanted = sorted(binding_columns(table) - {BOUND_COLUMN})
    pk = next(iter(source.primary_key.columns)).name
    for candidate in await lineage(admin_db, org_id, env):
        scoped = _scoped(source, org_schema(org_id, None if candidate == PROD else candidate))
        async with tenant_db.acquire() as conn:
            result = await conn.execute_core(
                select(scoped.c[BOUND_COLUMN], *(scoped.c[c] for c in wanted)).where(
                    scoped.c[pk] == key
                )
            )
            row = result.fetchone()
        if row is None:
            continue
        values = dict(row._mapping)
        if values.pop(BOUND_COLUMN):
            return candidate, values
    raise BindingError(
        f"{table}.{key} is unbound in {env!r} and in every environment it branched from; bind it, "
        f"or branch an environment that has"
    )
