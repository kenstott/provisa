# Copyright (c) 2026 Kenneth Stott
# Canary: 9b794957-4784-4391-bb05-25d83c0f5c91
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The seed's own definition of a seeded role or domain, and putting it back (REQ-1919).

A seeded object is the deployment's own: a config file may redefine it, and a load never removes
it. When a later file no longer declares it, the load puts the seed's definition back — the same
way it removes a config object the file dropped — through a check that nothing is left dangling:
a role whose seed reaches fewer domains than the file gave it would strand an assignment in a
domain it no longer reaches, and that is refused with the list.

The seed's definitions are the portable seed's (``provisa.core.db``), which schema.sql mirrors.
"""

# Requirements: REQ-1919, REQ-1297

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from sqlalchemy import delete, select, update

from provisa.core.db import _DEMONSTRATED_ROLES, _SEED_DOMAINS, _SEED_ROLES
from provisa.core.repositories.integrity import Dependent, ObjectRef
from provisa.core.schema_org import domains, roles, seed_redefinitions, user_role_assignments

if TYPE_CHECKING:
    from provisa.core.database import Connection


def seed_role(role_id: str) -> dict[str, Any] | None:
    """The seed's definition of ``role_id``, or None when the seed does not define it."""
    capabilities = dict(_SEED_ROLES).get(role_id)
    if capabilities is None:
        return None
    return {
        "capabilities": list(capabilities),
        "demonstrated": list(_DEMONSTRATED_ROLES.get(role_id, [])),
        # A role holding the cross-org right is the control plane, which reaches no data domain.
        "domain_access": [] if "cross_org" in capabilities else ["*"],
        "parent_role_id": None,
        "rate_limit": None,
    }


def seed_domain(domain_id: str) -> dict[str, Any] | None:
    """The seed's definition of ``domain_id``, or None when the seed does not define it."""
    for seeded_id, description, steward in _SEED_DOMAINS:
        if seeded_id == domain_id:
            return {"description": description, "steward": steward, "graphql_alias": None}
    return None


@dataclass(frozen=True)
class Revert:
    """A seeded object a file had redefined and no longer declares: what it goes back to, and
    whether that narrows what a role reaches (said loudly)."""

    ref: ObjectRef
    definition: dict[str, Any]
    narrows: list[str]


async def redefined(conn: "Connection") -> list[ObjectRef]:
    """Every seeded object a config file has redefined."""
    rows = await conn.execute_core(
        select(seed_redefinitions.c.kind, seed_redefinitions.c.object_id).order_by(
            seed_redefinitions.c.kind, seed_redefinitions.c.object_id
        )
    )
    return [ObjectRef(kind, object_id) for kind, object_id in rows.fetchall()]


async def plan_revert(conn: "Connection", ref: ObjectRef) -> tuple[Revert, list[Dependent]]:
    """What putting ``ref`` back to the seed's definition changes, and what that would strand."""
    if ref.kind == "role":
        definition = seed_role(str(ref.id))
        assert definition is not None  # only a seed-defined role is ever marked
        current = (
            await conn.execute_core(
                select(roles.c.capabilities, roles.c.domain_access).where(roles.c.id == ref.id)
            )
        ).one()
        narrows: list[str] = []
        lost_rights = sorted(set(current.capabilities) - set(definition["capabilities"]))
        if lost_rights:
            narrows.append("rights " + ", ".join(lost_rights))
        reaches = set(definition["domain_access"])
        lost_domains = (
            []
            if "*" in reaches
            else sorted(d for d in (current.domain_access or []) if d == "*" or d not in reaches)
        )
        if lost_domains:
            narrows.append("domains " + ", ".join(lost_domains))
        stranded: list[Dependent] = []
        if "*" not in reaches:
            held = await conn.execute_core(
                select(
                    user_role_assignments.c.id,
                    user_role_assignments.c.user_id,
                    user_role_assignments.c.domain_id,
                ).where(user_role_assignments.c.role_id == ref.id)
            )
            stranded = [
                Dependent(
                    ObjectRef("role_assignment", row.id),
                    ("user_role_assignments.domain_id",),
                    f"{row.user_id} holds {ref.id} in {row.domain_id}",
                )
                for row in held.fetchall()
                if row.domain_id not in reaches
            ]
        return Revert(ref, definition, narrows), stranded
    definition = seed_domain(str(ref.id))
    assert definition is not None  # only a seed-defined domain is ever marked
    return Revert(ref, definition, []), []


async def apply(conn: "Connection", revert: Revert) -> None:
    """Put the seed's definition back and forget that a file had changed it."""
    table = roles if revert.ref.kind == "role" else domains
    await conn.execute_core(
        update(table).where(table.c.id == revert.ref.id).values(**revert.definition)
    )
    await conn.execute_core(
        delete(seed_redefinitions).where(
            seed_redefinitions.c.kind == revert.ref.kind,
            seed_redefinitions.c.object_id == str(revert.ref.id),
        )
    )
