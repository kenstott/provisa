# Copyright (c) 2026 Kenneth Stott
# Canary: 9b4e2c71-0d6a-4f38-a5e2-71c8d03f4b96
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The people an entitlement can be granted to (REQ-2219, ONE API, THREE WAYS TO USE IT).

One search by name or email over the org's local accounts and known members, returning the
principal a sign-in token carries (the user id), with name and email for display only.

Who is in the org:

* its members: the platform plane's memberships for the org, with the profile each names;
* the people who have signed in to it: the org's ``user_directory``, written at every sign-in;
* its local accounts: the platform plane's ``local_users`` the org's memberships name. A
  deployment without multitenancy is one org, which every local account signs in to, so there
  every local account is the org's.

Directory lookup in an identity provider comes later, per provider, behind one interface.
"""

# Requirements: REQ-2219

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from sqlalchemy import ColumnElement, func, or_, select

from provisa.core.schema_admin import local_users, user_org_memberships, user_profiles
from provisa.core.schema_org import user_directory

if TYPE_CHECKING:
    from provisa.core.database import Database


@dataclass(frozen=True)
class Person:
    principal: str
    name: str | None
    email: str | None


def _matches(text: str, *columns: ColumnElement) -> ColumnElement[bool]:
    needle = text.strip().lower()
    return or_(*(func.lower(c).contains(needle, autoescape=True) for c in columns))


@dataclass
class _Found:
    names: list[str | None]
    emails: list[str | None]


def _note(found: dict[str, _Found], principal: str, name: str | None, email: str | None) -> None:
    slot = found.setdefault(principal, _Found([], []))
    slot.names.append(name)
    slot.emails.append(email)


def _first(values: list[str | None]) -> str | None:
    return next((v for v in values if v), None)


async def search(
    admin_db: "Database",
    model_db: "Database",
    org_id: str,
    text: str,
    *,
    multitenancy: bool,
) -> list[Person]:
    """The people of ``org_id`` whose name or email contains ``text``, case-insensitively, each
    once, ordered by name then email. ``model_db`` is scoped to ``org_id``'s schema."""
    found: dict[str, _Found] = {}
    members = select(user_org_memberships.c.user_id).where(user_org_memberships.c.org_id == org_id)
    async with admin_db.acquire() as conn:
        rows = await conn.execute_core(
            select(
                user_org_memberships.c.user_id,
                user_profiles.c.display_name,
                user_profiles.c.email,
            )
            .select_from(
                user_org_memberships.join(
                    user_profiles, user_profiles.c.user_id == user_org_memberships.c.user_id
                )
            )
            .where(
                user_org_memberships.c.org_id == org_id,
                _matches(
                    text,
                    user_profiles.c.display_name,
                    user_profiles.c.given_name,
                    user_profiles.c.family_name,
                    user_profiles.c.email,
                ),
            )
        )
        for row in rows.fetchall():
            _note(found, row.user_id, row.display_name, row.email)
        local = select(
            local_users.c.id,
            local_users.c.username,
            local_users.c.display_name,
            local_users.c.email,
        ).where(
            _matches(text, local_users.c.username, local_users.c.display_name, local_users.c.email)
        )
        if multitenancy:
            local = local.where(local_users.c.id.in_(members))
        rows = await conn.execute_core(local)
        for row in rows.fetchall():
            _note(found, row.id, row.display_name or row.username, row.email)
    async with model_db.acquire() as conn:
        rows = await conn.execute_core(
            select(
                user_directory.c.user_id, user_directory.c.display_name, user_directory.c.email
            ).where(_matches(text, user_directory.c.display_name, user_directory.c.email))
        )
        for row in rows.fetchall():
            _note(found, row.user_id, row.display_name, row.email)
    people = [Person(p, _first(f.names), _first(f.emails)) for p, f in found.items()]
    return sorted(people, key=lambda p: ((p.name or "").lower(), (p.email or "").lower()))


async def with_email(
    admin_db: "Database",
    model_db: "Database",
    org_id: str,
    email: str,
    *,
    multitenancy: bool,
) -> list[Person]:
    """The people of ``org_id`` whose email is ``email``, case-insensitively."""
    wanted = email.strip().lower()
    candidates = await search(admin_db, model_db, org_id, wanted, multitenancy=multitenancy)
    return [p for p in candidates if (p.email or "").strip().lower() == wanted]
