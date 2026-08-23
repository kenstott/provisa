# Copyright (c) 2026 Kenneth Stott
# Canary: d9fbbf41-30bf-4d01-95cb-fe596753d761
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Authority over a model, scoped to the environment whose creator you are (REQ-1528, REQ-1530).

The ordinary way to acquire model-editing rights is to CREATE AN ENVIRONMENT. A member holding no
such rights in the org can still create one, and within the one they created they hold the seeded
``developer`` role — which is what lets a read-only member build without anyone granting them the
organization.

THE GRANT IS A ROLE THAT ALREADY EXISTS, not org_admin's rights less a withheld list. developer
means "may build the model and query it" and stops short of the surfaces an org_admin keeps:
registering sources and tables, masking, column grants, view governance, access config, org
settings. So a right only reaches a branch owner because somebody decided a developer should hold
it, rather than because nobody remembered to withhold it.

WHAT IT MAY BE DONE TO is a separate question with a separate answer: a developer is limited by
DOMAIN MEMBERSHIP, everywhere and not only inside a branch (REQ-1530). Branching changes what a
member may do; it never changes what they may do it to. That limit is the ordinary
``roles.domain_access`` the org already checks, read here by :func:`domains_within`.

WHAT REACHING DATA DEPENDS ON, AND IT IS NOT THIS (REQ-1539). This grant confers no data right at
all — not ``write``, not ``full_results``, not ``usage``. Whether a person may read or write the
data an environment points at is decided by the ROLES they hold there, exactly as it is decided
everywhere else, and each environment answers it for itself because ``roles`` travels with the model
(REQ-1489): dev's ``developer`` may be unrestricted while prod's holds nothing, and the same
assignment means both things in the two places. Creating an environment therefore lets a member
BUILD, and changes nothing about what they may reach — which is why the earlier construction, a
``branch_writable`` flag bounding a ``write`` this grant had handed out, is gone rather than
retuned. Authority over the model and authority over what the model points at stay separate.

THE GRANT IS DERIVED, never administered. It is read from ``environments.created_by`` at
authorization time rather than written into a grant table, so it cannot drift from the environment
it describes, and deleting the environment removes it in the same act.
"""

# Requirements: REQ-1491, REQ-1492, REQ-1503, REQ-1504, REQ-1528, REQ-1529, REQ-1530, REQ-1539

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import select

from provisa.core.db import _SEED_ROLES
from provisa.core.schema_admin import environments

if TYPE_CHECKING:
    from provisa.core.database import Database

#: What the creator of a branch holds INSIDE it: the seeded ``developer`` role, verbatim (REQ-1528).
#:
#: Not org_admin's rights less a withheld list. That construction had to name every right that must
#: not travel, and would have handed over any right added to org_admin afterwards; naming the role
#: means a right reaches a branch owner only because somebody decided a developer should hold it.
#: developer already means "may build the model and query it", and already excludes the surfaces an
#: org_admin keeps — registering sources and tables, masking, column grants, view governance, access
#: config and org settings. So the answer to "what can a branch owner change?" is "what a developer
#: may change", which needs no second rule.
#:
#: MINUS THE DATA RIGHTS (REQ-1539). ``write``, ``full_results`` and ``usage`` are what a role says
#: a person may do to the DATA, and creating an environment is not a way to acquire them: a member
#: who could not write before still cannot, in their own environment as anywhere else. They arrive
#: through ``user_role_assignments``, which travels into the environment with the ``roles`` rows
#: that define what each name means there (REQ-1489), and what remains here is exactly the ability
#: to build the model and nothing else.
_DATA_RIGHTS: frozenset[str] = frozenset({"write", "full_results", "usage"})
ENVIRONMENT_OWNER_CAPABILITIES: frozenset[str] = (
    frozenset(dict(_SEED_ROLES)["developer"]) - _DATA_RIGHTS
)

#: The domain_access value meaning "every domain" — the state of every seeded role, and therefore
#: what makes a developer unlimited until an org_admin narrows it (REQ-1530).
ALL_DOMAINS = "*"


async def environment_owner(admin_db: "Database", org_id: str, env: str) -> str | None:
    """Who created ``env``, which is who holds authority within it. ``None`` if it has no creator.

    An environment created before there was a user to attribute it to — the ``prod`` row
    provisioning writes (REQ-1487) — has no owner, and confers authority on nobody. prod's rights
    are the org's own, held the ordinary way.
    """
    async with admin_db.acquire() as conn:
        result = await conn.execute_core(
            select(environments.c.created_by).where(
                environments.c.org_id == org_id, environments.c.name == env
            )
        )
        row = result.fetchone()
    return str(row[0]) if row is not None and row[0] is not None else None


async def owns_environment(admin_db: "Database", org_id: str, env: str, user_id: str) -> bool:
    """Whether ``user_id`` is the creator of ``env`` — the anchor the whole grant hangs on."""
    owner = await environment_owner(admin_db, org_id, env)
    return owner is not None and owner == user_id


async def capabilities_within(
    admin_db: "Database", org_id: str, env: str, user_id: str
) -> frozenset[str]:
    """The rights ``user_id`` holds by virtue of owning ``env``, and none if they do not own it.

    Additive: the caller unions this with whatever the principal already holds in the org. It never
    removes a right, because owning an environment is a reason to be able to do more inside it and
    never a reason to be able to do less.
    """
    if not await owns_environment(admin_db, org_id, env, user_id):
        return frozenset()
    return ENVIRONMENT_OWNER_CAPABILITIES


def domains_within(role_domain_access: list[str] | None) -> frozenset[str] | None:
    """The domains a member may CHANGE objects in, or ``None`` when they may change any (REQ-1530).

    A developer is limited by domain membership everywhere — in the org and inside a branch they
    own alike — so this reads the same ``roles.domain_access`` the org already checks rather than
    deriving a branch-only scope. Branching changes what a member may do; it never changes what
    they may do it to.

    ``None`` for ``*`` is the answer and not a missing value: an unnarrowed role is unlimited, which
    is the state of every seeded role.

    THE SCOPE IS THE ROLE'S, not the grant's. ``user_role_assignments`` also carries a domain, and
    enforcement deliberately does not read it: a member's rights come off the role they hold, in one
    place, and a second scope to intersect would give every authorization question two answers that
    can disagree. An org_admin scopes a developer with a role whose domain_access names their
    domains, not by granting a developer role on a domain (REQ-1530).
    """
    if role_domain_access is None:
        raise ValueError(
            "a role with no domain_access cannot be scoped: domain_access is NOT NULL on the roles "
            "table, so this is a caller that never read it, not a role that lacks one"
        )
    if ALL_DOMAINS in role_domain_access:
        return None
    return frozenset(role_domain_access)


def may_change_domain(role_domain_access: list[str] | None, domain_id: str) -> bool:
    """Whether a member holding ``role_domain_access`` may change an object in ``domain_id``."""
    allowed = domains_within(role_domain_access)
    return allowed is None or domain_id in allowed
