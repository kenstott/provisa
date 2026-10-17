# Copyright (c) 2026 Kenneth Stott
# Canary: 55c29699-5ff0-4fa9-bc08-ca61513c077a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""User attribute repository (REQ-1382, REQ-1383), via SQLAlchemy Core.

Two tables of the org's model store. ``user_attributes`` holds what the org records for a member;
``user_idp_attributes`` what the member's identity provider supplied at their last sign-in. How
the two resolve is :mod:`provisa.auth.attributes`; this module only stores.
"""

# Requirements: REQ-1382, REQ-1383

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import delete as _delete
from sqlalchemy import func, select

from provisa.auth.attributes import attribute_name_problem, normalize_name
from provisa.core.schema_org import user_attributes, user_idp_attributes

if TYPE_CHECKING:
    from provisa.core.database import Connection

# Serializes writes of providers' records (record_idp); taken only when a record changes.
_IDP_RECORD_LOCK_KEY = 0x50524F5649534136


async def upsert(conn: "Connection", user_id: str, name: str, value: str) -> str:
    """Record ``name`` = ``value`` for ``user_id``, replacing the value it had. Returns the
    normalized name it was stored under. ``ValueError`` for a name that may not be recorded."""
    problem = attribute_name_problem(name)
    if problem is not None:
        raise ValueError(problem[1])
    if not user_id:
        raise ValueError("An attribute belongs to a user")
    normalized = normalize_name(name)
    await conn.upsert(
        user_attributes,
        {"user_id": user_id, "name": normalized, "value": value, "updated_at": func.now()},
        index_elements=["user_id", "name"],
        update_columns=["value", "updated_at"],
    )
    return normalized


async def delete(conn: "Connection", user_id: str, name: str) -> bool:
    result = await conn.execute_core(
        _delete(user_attributes).where(
            user_attributes.c.user_id == user_id,
            user_attributes.c.name == normalize_name(name),
        )
    )
    return (result.rowcount or 0) > 0


async def org_for_user(conn: "Connection", user_id: str) -> dict[str, str]:
    result = await conn.execute_core(
        select(user_attributes.c.name, user_attributes.c.value).where(
            user_attributes.c.user_id == user_id
        )
    )
    return {r.name: r.value for r in result.fetchall()}


async def org_by_user(conn: "Connection") -> dict[str, dict[str, str]]:
    """user id -> the attributes the org records for that user, for every user who has any. What
    the runtime build holds, so resolving a request's attributes reads nothing."""
    result = await conn.execute_core(
        select(user_attributes.c.user_id, user_attributes.c.name, user_attributes.c.value)
    )
    out: dict[str, dict[str, str]] = {}
    for row in result.fetchall():
        out.setdefault(row.user_id, {})[row.name] = row.value
    return out


async def idp_for_user(conn: "Connection", user_id: str) -> dict[str, str]:
    """What the user's identity provider supplied at their last sign-in. Empty for a user who
    has not signed in to this org since the record began."""
    result = await conn.execute_core(
        select(user_idp_attributes.c.name, user_idp_attributes.c.value).where(
            user_idp_attributes.c.user_id == user_id
        )
    )
    return {r.name: r.value for r in result.fetchall()}


async def idp_names_by_user(conn: "Connection") -> dict[str, set[str]]:
    """user id -> the attribute names their provider supplied, for every user it recorded."""
    result = await conn.execute_core(
        select(user_idp_attributes.c.user_id, user_idp_attributes.c.name)
    )
    out: dict[str, set[str]] = {}
    for row in result.fetchall():
        out.setdefault(row.user_id, set()).add(row.name)
    return out


async def record_idp(conn: "Connection", user_id: str, attributes: dict[str, str]) -> bool:
    """Make the record of what the provider supplies for ``user_id`` equal ``attributes``.
    False, and nothing written, when it already is.

    A user's first requests arrive together (a new sandbox's tour warm-up beside its page): each
    found nothing recorded and wrote, and the second insert was a unique-key violation. The
    comparison and the write are one step under a lock, so the second finds the first's record."""
    if await idp_for_user(conn, user_id) == attributes:
        return False
    async with conn.transaction():
        await conn.advisory_xact_lock(_IDP_RECORD_LOCK_KEY)
        if await idp_for_user(conn, user_id) == attributes:
            return False
        await conn.execute_core(
            _delete(user_idp_attributes).where(user_idp_attributes.c.user_id == user_id)
        )
        for name in sorted(attributes):
            await conn.execute_core(
                user_idp_attributes.insert().values(
                    user_id=user_id, name=name, value=attributes[name]
                )
            )
    return True
