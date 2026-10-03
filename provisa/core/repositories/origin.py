# Copyright (c) 2026 Kenneth Stott
# Canary: 83455bc0-35d3-498b-b019-49ffead9ddad
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Where a model object came from (REQ-1919).

Every source, domain, role and registered table records its origin when it is created: a config
file declared it (``CONFIG``), it was made through the admin (``ADMIN``), or it is the
deployment's own (``SEED``). A load manages only what a config declared: it adds and updates what
its file declares and removes only ``CONFIG`` objects the file no longer declares. Objects made
through the admin are never removed or modified by a load.

The origin is written by the model store's create and by nothing else; a later write to the row
leaves it as it is, with one exception: when a config file declares an object that was made
through the admin, the file takes it over (:func:`take_over`).
"""

# Requirements: REQ-1919

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Literal

from sqlalchemy import select, update

from provisa.core.schema_org import (
    domains,
    registered_tables,
    roles,
    seed_redefinitions,
    sources,
)

if TYPE_CHECKING:
    from provisa.core.database import Connection

log = logging.getLogger(__name__)

Origin = Literal["config", "admin", "seed"]
CONFIG: Origin = "config"
ADMIN: Origin = "admin"
SEED: Origin = "seed"
ORIGINS: tuple[Origin, ...] = (CONFIG, ADMIN, SEED)

#: The kinds the deployment seeds with a definition a config file can redefine and a load can put
#: back: the seeded roles and domains (``provisa.core.db``).
SEED_DEFINED_KINDS = frozenset({"role", "domain"})


def require(origin: str) -> Origin:
    """``origin``, or a ``ValueError`` when it is not one of the three."""
    if origin not in ORIGINS:
        raise ValueError(f"origin must be one of {ORIGINS}, not {origin!r}")
    return origin  # type: ignore[return-value]


async def take_over(
    conn: "Connection", table: Any, where: tuple, *, kind: str, ident: object, origin: str
) -> bool:
    """When a config load writes an object that was made through the admin, the file takes it
    over: its origin becomes ``CONFIG`` and the load says so, naming the object. True when it
    did. A write with any other origin, and a row that is already the config's or the seed's,
    is left as it is."""
    if origin != CONFIG:
        return False
    if kind in SEED_DEFINED_KINDS and _seed_defines(kind, ident):
        seeded = (
            await conn.execute_core(select(table.c.id).where(*where, table.c.origin == SEED))
        ).fetchone()
        if seeded is not None:
            # A seeded object the file redefines stays the deployment's own; the load remembers
            # the file changed it, so a load of a file that no longer declares it puts the seed's
            # definition back (``seed_definitions``).
            await conn.upsert(
                seed_redefinitions,
                {"kind": kind, "object_id": str(ident)},
                index_elements=["kind", "object_id"],
                update_columns=[],
            )
            return False
    result = await conn.execute_core(
        update(table).where(*where, table.c.origin == ADMIN).values(origin=CONFIG)
    )
    taken = (result.rowcount or 0) > 0
    if taken:
        log.info(
            "config load takes over %s %r, which was made through the admin: "
            "it is now declared by the config",
            kind,
            ident,
        )
    return taken


def _seed_defines(kind: str, ident: object) -> bool:
    """Whether the seed has its own definition of this role or domain to put back."""
    from provisa.core.repositories.seed_definitions import seed_domain, seed_role

    return (seed_role if kind == "role" else seed_domain)(str(ident)) is not None


_TABLE_OF_KIND = {"source": sources, "domain": domains, "role": roles, "table": registered_tables}


async def of(conn: "Connection", kind: str, ident: object) -> str | None:
    """The origin of a source, domain, role or table (by its id); None when there is none."""
    table = _TABLE_OF_KIND[kind]
    row = (await conn.execute_core(select(table.c.origin).where(table.c.id == ident))).fetchone()
    return None if row is None else row[0]


async def of_registration(
    conn: "Connection", source_id: str, schema_name: str, table_name: str
) -> str | None:
    """The origin of the table registered under this identity; None when none is."""
    row = (
        await conn.execute_core(
            select(registered_tables.c.origin).where(
                registered_tables.c.source_id == source_id,
                registered_tables.c.schema_name == schema_name,
                registered_tables.c.table_name == table_name,
            )
        )
    ).fetchone()
    return None if row is None else row[0]


_NOTICE = {
    "edited": (
        "origin.config_object_edited",
        "{kind} {name!r} is declared in the config: the next load of the config re-applies "
        "what the file says",
    ),
    "deleted": (
        "origin.config_object_deleted",
        "{kind} {name!r} is declared in the config: the next load of the config creates it "
        "again while the file still declares it",
    ),
}


def config_notices(
    kind: str, name: object, origin: str | None, change: Literal["edited", "deleted"]
) -> list[dict[str, Any]]:
    """What an admin is told after editing or deleting an object a config declared: the change
    was made, and the next load of the config re-applies the file. Empty for an object of any
    other origin, and for one that did not exist before the change (``origin`` None)."""
    if origin != CONFIG:
        return []
    code, text = _NOTICE[change]
    return [
        {
            "code": code,
            "message": text.format(kind=kind, name=name),
            "params": {"kind": kind, "name": str(name)},
        }
    ]
