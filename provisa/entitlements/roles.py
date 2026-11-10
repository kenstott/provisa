# Copyright (c) 2026 Kenneth Stott
# Canary: 5b1e7c3a-9d42-4f86-a0e1-6c2f8b4d7a13
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The roles a person holds, with where each came from (REQ-2219, ONE API, THREE WAYS TO USE IT).

Entitlement lists are set per role grant, so the picker offers the roles the person holds:

* ``provisa``: the role assignments the org holds for them, in force now, where the deployment
  reads assignments from Provisa (``auth.assignments_source: provisa``);
* ``default_assignment``: the deployment's ``default_assignments``, which a person holds there
  when the org holds no assignment for them (``provisa.auth.held_assignments.read_assignments``);
* ``idp``: the roles their identity provider sent entitlement lists for at their last sign-in, as
  the store mirrors them (``provisa.entitlements.store.mirror_idp``), among the roles the org
  defines: a role the identity provider names that the org does not define has no lists to set,
  so it is not offered.

A role from more than one source is listed once, with each origin.
"""

# Requirements: REQ-2219

from __future__ import annotations

from collections.abc import Collection, Iterable, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING

from sqlalchemy import select

from provisa.auth.role_grants import in_force
from provisa.core.assignments_source import CLAIMS
from provisa.core.assignments_source import PROVISA as FROM_PROVISA
from provisa.core.schema_org import user_role_assignments
from provisa.entitlements.lists import IDP, PROVISA, IdpLists

if TYPE_CHECKING:
    from provisa.core.database import Connection

DEFAULT_ASSIGNMENT = "default_assignment"


@dataclass(frozen=True)
class HeldRole:
    """A role a person holds: where it came from, and the domains its assignments name (empty for
    a role only the identity provider speaks to)."""

    role_id: str
    origins: tuple[str, ...]
    domains: tuple[str, ...]


async def _assigned(conn: "Connection", principal: str) -> list[tuple[str, str]]:
    rows = await conn.execute_core(
        select(user_role_assignments.c.role_id, user_role_assignments.c.domain_id).where(
            user_role_assignments.c.user_id == principal, in_force()
        )
    )
    return [(r.role_id, r.domain_id) for r in rows.fetchall()]


async def held_roles(
    conn: "Connection",
    principal: str,
    idp: IdpLists,
    auth: Mapping,
    defined: Collection[str],
) -> list[HeldRole]:
    """The roles ``principal`` holds in the org ``conn`` is bound to, ordered by role id. ``idp``
    is what their identity provider last sent; ``auth`` the deployment's auth settings;
    ``defined`` the ids of the roles the org defines."""
    origins: dict[str, list[str]] = {}
    domains: dict[str, list[str]] = {}

    def note(role_id: str, origin: str, domain_ids: Iterable[str] = ()) -> None:
        if origin not in origins.setdefault(role_id, []):
            origins[role_id].append(origin)
        held = domains.setdefault(role_id, [])
        held.extend(d for d in domain_ids if d not in held)

    if auth.get("assignments_source", CLAIMS) == FROM_PROVISA:
        assigned = await _assigned(conn, principal)
        for role_id, domain_id in assigned:
            note(role_id, PROVISA, (domain_id,))
        if not assigned:
            for a in auth.get("default_assignments") or []:
                note(a["role_id"], DEFAULT_ASSIGNMENT, (a.get("domain_id", "*"),))
    for role_id in idp:
        if role_id in defined:
            note(role_id, IDP)
    return [
        HeldRole(role_id, tuple(origins[role_id]), tuple(sorted(domains[role_id])))
        for role_id in sorted(origins)
    ]
