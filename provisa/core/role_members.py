# Copyright (c) 2026 Kenneth Stott
# Canary: 8c0f3e52-6a1d-4f7b-9e2a-3d5c71b0a4e9
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The members of a role in an org, by whichever system owns that role's membership (REQ-2215).

One contract answers "the members of role R in org O" with principals (id, display name, email).
A :class:`RoleMemberResolver` implements it for one membership source: read-only, asked
synchronously by the caller, and bound by the time limit it declares. Provisa's own role
assignments (``user_role_assignments``) are the first resolver; resolvers for an external identity
provider (SCIM, LDAP group membership, Microsoft Graph, Google Directory, a declared command)
implement the same contract and are added later.

A role nobody can be resolved for is answered as such, naming the role
(:attr:`RoleMembers.resolvable` is false), so anything addressed to it says so rather than
notifying no one silently.
"""

# Requirements: REQ-2215

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol, runtime_checkable

from sqlalchemy import func, or_, select

from provisa.core.schema_org import user_directory, user_role_assignments

if TYPE_CHECKING:
    from provisa.core.database import Database


@dataclass(frozen=True)
class Principal:
    """One member of a role, as the membership source names them."""

    id: str
    display_name: str | None
    email: str | None

    def as_dict(self) -> dict[str, str | None]:
        return {"id": self.id, "display_name": self.display_name, "email": self.email}


@dataclass(frozen=True)
class RoleMembers:
    """The answer for one role: its principals, or why it has none that can be named."""

    role_id: str
    principals: tuple[Principal, ...]
    #: The membership source that answered.
    source: str

    @property
    def resolvable(self) -> bool:
        return bool(self.principals)

    @property
    def unresolved_reason(self) -> str | None:
        """What a request addressed to this role says when no member can be named."""
        if self.principals:
            return None
        return f"role {self.role_id!r} has no resolvable members ({self.source})"


class ResolverTimeout(Exception):
    """A resolver did not answer within the time limit it declares (REQ-2215)."""

    def __init__(self, resolver: str, role_id: str, limit_s: float) -> None:
        super().__init__(
            f"membership resolver {resolver!r} did not answer for role {role_id!r} "
            f"within its declared {limit_s}s"
        )
        self.resolver = resolver
        self.role_id = role_id
        self.limit_s = limit_s


@runtime_checkable
class RoleMemberResolver(Protocol):
    """The system-defined contract every membership source implements (REQ-2215).

    Read-only: a resolver never writes to its source. Synchronous from the caller's side: the
    caller awaits the answer. Bounded: :attr:`time_limit_s` is the resolver's declared limit, and
    :func:`resolve_role_members` refuses an answer later than it.
    """

    #: A stable name for the membership source, shown with an unresolved role.
    name: str
    #: The longest this resolver may take to answer, in seconds.
    time_limit_s: float

    async def members(self, org_id: str, role_id: str) -> list[Principal]:
        """The principals holding ``role_id`` in ``org_id``; empty when none are known."""
        ...


class ProvisaAssignmentsResolver:
    """Provisa's own role assignments: the users assigned the role, in any domain, whose grant has
    not expired, named as the org's user directory names them."""

    name = "provisa"
    # A control-plane read; the declared limit bounds a stalled connection.
    time_limit_s = 5.0

    def __init__(self, model_db: "Database") -> None:
        self._db = model_db

    async def members(self, org_id: str, role_id: str) -> list[Principal]:
        # org_id is the org ``model_db`` is bound to: the assignments live in that org's schema.
        stmt = (
            select(
                user_role_assignments.c.user_id,
                user_directory.c.display_name,
                user_directory.c.email,
            )
            .select_from(
                user_role_assignments.outerjoin(
                    user_directory, user_directory.c.user_id == user_role_assignments.c.user_id
                )
            )
            .where(
                user_role_assignments.c.role_id == role_id,
                or_(
                    user_role_assignments.c.expires_at.is_(None),
                    user_role_assignments.c.expires_at > func.now(),
                ),
            )
            .distinct()
            .order_by(user_role_assignments.c.user_id)
        )
        async with self._db.acquire() as conn:
            rows = (await conn.execute_core(stmt)).fetchall()
        return [Principal(id=r[0], display_name=r[1], email=r[2]) for r in rows]


async def resolve_role_members(
    resolver: RoleMemberResolver, org_id: str, role_id: str
) -> RoleMembers:
    """The members of ``role_id`` in ``org_id`` by ``resolver``, within its declared time limit.

    Raises :class:`ResolverTimeout` when the resolver overruns: a membership source that cannot
    answer is a failure to report, not a role with no members."""
    try:
        principals = await asyncio.wait_for(
            resolver.members(org_id, role_id), timeout=resolver.time_limit_s
        )
    except asyncio.TimeoutError as exc:
        raise ResolverTimeout(resolver.name, role_id, resolver.time_limit_s) from exc
    unique = {p.id: p for p in principals}
    return RoleMembers(
        role_id=role_id, principals=tuple(unique[k] for k in sorted(unique)), source=resolver.name
    )
