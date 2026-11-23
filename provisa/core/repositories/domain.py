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

# Requirements: REQ-021, REQ-154, REQ-367, REQ-402, REQ-2248

from provisa.core import model_change, model_uri
from typing import TYPE_CHECKING

from sqlalchemy import delete as _delete, select, update

from provisa.core.models import Domain
from provisa.core.repositories import retirement
from provisa.core.repositories.integrity import Dependent, ObjectRef, guard, rekey, remove_parts
from provisa.core.schema_org import domains, registered_tables

if TYPE_CHECKING:
    from provisa.core.database import Connection


async def upsert(  # REQ-021, REQ-367, REQ-1919
    conn: "Connection", domain: Domain
) -> None:
    """Create the domain, or replace its definition.

    REQ-572: refused (:class:`provisa.compiler.naming.LabelClash`), naming both, when the
    domain's Cypher label is a registered table's. The admin and config load share this path."""
    model_change.name("upsert", model_change.uri(model_uri.domain_uri, domain.id))  # REQ-1524
    await _require_label_not_a_table(conn, domain.id)
    await conn.upsert(
        domains,
        {
            "id": domain.id,
            "description": domain.description,
            "graphql_alias": domain.graphql_alias,
            "parent_domain_id": domain.parent_domain_id,  # REQ-1363
            "org_id": "root",
        },
        index_elements=["id"],
        update_columns=["description", "graphql_alias", "parent_domain_id"],
    )


async def _require_label_not_a_table(conn: "Connection", domain_id: str) -> None:  # REQ-572
    from provisa.compiler.naming import refuse_label_clash
    from provisa.core.repositories.table import naming_rules
    from provisa.core.schema_org import registered_tables as rt

    tables = [
        dict(r._mapping)
        for r in (
            await conn.execute_core(
                select(
                    rt.c.source_id,
                    rt.c.schema_name,
                    rt.c.table_name,
                    rt.c.alias,
                    rt.c.gql_naming_convention,
                )
            )
        ).fetchall()
    ]
    refuse_label_clash(tables, [domain_id], await naming_rules(conn))


async def get(conn: "Connection", domain_id: str) -> dict | None:  # REQ-021, REQ-402
    """The domain at ``domain_id``. A retired domain is under a key of its own (REQ-2248:
    :func:`provisa.core.repositories.retirement.retired_key`), so a live id never finds one."""
    result = await conn.execute_core(select(domains).where(domains.c.id == domain_id))
    row = result.fetchone()
    return dict(row._mapping) if row is not None else None


async def list_all(conn: "Connection", *, retired: bool = False) -> list[dict]:  # REQ-021
    """Every domain in service; ``retired`` lists the retired ones instead (REQ-2248), each with
    who retired it, when and why, and the id it had (``retired_id``)."""
    result = await conn.execute_core(
        select(domains).where(domains.c.retired.is_(retired)).order_by(domains.c.id)
    )
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
        # What a refusal says before naming its dependents (REQ-2152: the API layer names only
        # those its caller may read).
        self.lead = f"Domain {domain_id!r} is still referred to by"
        if reason == "dependents":
            named = ", ".join(f"{d.ref.kind} {d.ref.id}" for d in self.dependents)
            message = f"Domain {domain_id!r} is still referred to by: {named}"
        else:
            message = f"Domain {domain_id!r} is a system domain and cannot be deleted"
        super().__init__(message)


async def delete(  # REQ-021, REQ-1917, REQ-1918, REQ-2248
    conn: "Connection", domain_id: str, *, reason: str | None = None
) -> bool:
    """Delete one domain: THE delete, for every surface. It RETIRES the domain (REQ-2248): the
    row is kept, marked retired with ``reason``, the acting person and the time, together with the
    retired tables in it. False when there is no such domain.

    Refused (:class:`DomainDeleteRefused`), naming each dependent, while anything live refers to
    it (REQ-1917): a table or view sits in it, a data product, row filter, command, webhook,
    glossary term or remote registration is placed in it, a role lists it, an assignment is
    scoped to it, a source allows it, or a domain sits under it. A domain the deployment keeps is
    refused.

    A retired domain is hidden and leaves its id free: its row and the retired rows in it move to
    a key of its own (:func:`retirement.retired_key`), and ``retired_id`` keeps the id it had, for
    :func:`restore`. One transaction; nothing is removed.
    """
    model_change.name("retire", model_change.uri(model_uri.domain_uri, domain_id))  # REQ-1524
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
        key = retirement.retired_key(domain_id)
        await rekey(conn, "domain", domain_id, key)
        await conn.execute_core(
            update(domains)
            .where(domains.c.id == key)
            .values(**retirement.values(reason), retired_id=domain_id)
        )
        # REQ-2219: an object gone has no owners or stewards in object_owners.
        from provisa.entitlements.owners import write_object_owners

        await write_object_owners(conn)
    return True


async def restore(conn: "Connection", key: str) -> bool:  # REQ-2248
    """Restore the retired domain kept at ``key``: back at the id it had, its retirement cleared,
    with the tables in it restored. Refused (:class:`retirement.NameHeld`) while a live domain
    holds that id; refused as a new domain is (REQ-572) while a table's label is that id. False
    when there is no retired domain at ``key``."""
    async with conn.transaction():
        row = await get(conn, key)
        if row is None or not row["retired"]:
            return False
        live_id = row["retired_id"]
        model_change.name("restore", model_change.uri(model_uri.domain_uri, live_id))  # REQ-1524
        if await get(conn, live_id) is not None:
            raise retirement.NameHeld("domain", live_id, f"domain {live_id!r}")
        await _require_label_not_a_table(conn, live_id)
        await rekey(conn, "domain", key, live_id)
        await conn.execute_core(
            update(domains)
            .where(domains.c.id == live_id)
            .values(**retirement.cleared(), retired_id=None)
        )
        await conn.execute_core(
            update(registered_tables)
            .where(registered_tables.c.domain_id == live_id, registered_tables.c.retired.is_(True))
            .values(**retirement.cleared())
        )
        # REQ-2219: a restored object's owners and stewards are back in object_owners.
        from provisa.entitlements.owners import write_object_owners

        await write_object_owners(conn)
    return True


async def discard(conn: "Connection", domain_id: str) -> None:
    """Remove a domain's parts and its row WITHOUT asking the guard: for a caller that has
    already established it may go — :func:`delete`."""
    await remove_parts(conn, ObjectRef("domain", domain_id))
    await conn.execute_core(_delete(domains).where(domains.c.id == domain_id))
