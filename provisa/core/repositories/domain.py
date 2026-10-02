# Copyright (c) 2026 Kenneth Stott
# Canary: 34fca4c5-b906-40e6-8a37-c35eb5334652
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Domain repository — CRUD for domains, via SQLAlchemy Core (dialect-portable)."""

# Requirements: REQ-021, REQ-154, REQ-367, REQ-402

from typing import TYPE_CHECKING

from sqlalchemy import delete as _delete, select

from provisa.core.models import Domain
from provisa.core.repositories.integrity import Dependent, ObjectRef, guard, remove_parts
from provisa.core.schema_org import domains

if TYPE_CHECKING:
    from provisa.core.database import Connection


async def upsert(conn: "Connection", domain: Domain) -> None:  # REQ-021, REQ-367
    await conn.upsert(
        domains,
        {
            "id": domain.id,
            "description": domain.description,
            "steward": domain.steward,  # REQ-609
            "graphql_alias": domain.graphql_alias,
            "org_id": "root",
        },
        index_elements=["id"],
        update_columns=["description", "steward", "graphql_alias"],
    )


async def get(conn: "Connection", domain_id: str) -> dict | None:  # REQ-021, REQ-402
    result = await conn.execute_core(select(domains).where(domains.c.id == domain_id))
    row = result.fetchone()
    return dict(row._mapping) if row is not None else None


async def list_all(conn: "Connection") -> list[dict]:  # REQ-021
    result = await conn.execute_core(select(domains).order_by(domains.c.id))
    return [dict(r._mapping) for r in result.fetchall()]


class DomainDeleteRefused(Exception):
    """A domain that may not be deleted, and why. ``reason`` is ``"system"`` (a domain the
    deployment keeps) or ``"dependents"`` (objects refer to it; ``dependents`` lists them)."""

    def __init__(
        self, domain_id: str, reason: str, dependents: "list[Dependent] | None" = None
    ) -> None:
        self.domain_id = domain_id
        self.reason = reason
        self.dependents = dependents or []
        if reason == "dependents":
            named = ", ".join(f"{d.ref.kind} {d.ref.id}" for d in self.dependents)
            message = f"Domain {domain_id!r} is still referred to by: {named}"
        else:
            message = f"Domain {domain_id!r} is a system domain and cannot be deleted"
        super().__init__(message)


async def delete(conn: "Connection", domain_id: str) -> bool:  # REQ-021, REQ-1917, REQ-1918
    """Delete one domain: THE delete, for every surface. False when there is no such domain.

    A domain is deleted only when nothing refers to it (REQ-1917): refused
    (:class:`DomainDeleteRefused`), naming each dependent, while a table or view sits in it, a
    data product, row filter, command, webhook, glossary term or remote registration is placed
    in it, a role lists it, an assignment is scoped to it, or a source allows it. It has no
    parts, so the delete removes the one row. A domain the deployment keeps is refused.
    """
    from provisa.core import domain_policy

    ref = ObjectRef("domain", domain_id)
    async with conn.transaction():
        if await get(conn, domain_id) is None:
            return False
        if domain_id in domain_policy.system_domain_ids():
            raise DomainDeleteRefused(domain_id, "system")
        blocking = await guard(conn, ref)
        if blocking:
            raise DomainDeleteRefused(domain_id, "dependents", blocking)
        await remove_parts(conn, ref)
        await conn.execute_core(_delete(domains).where(domains.c.id == domain_id))
    return True


async def delete_all_except(conn: "Connection", keep: list[str]) -> None:
    """Remove every domain not named in ``keep``: the config loader's full replace.

    A replace load declares the whole model; it is not a deletion of one object, so it does not
    ask the dependency guard, as the deletion of a whole org does not."""
    await conn.execute_core(_delete(domains).where(domains.c.id.not_in(keep)))
