# Copyright (c) 2026 Kenneth Stott
# Canary: 7c2e94b1-0f6d-4a38-b5e7-3d81c9a06f24
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Saved ERD layouts (REQ-2189): a named arrangement of the ERD, kept in the model store.

A layout is only the arrangement: the bar's options, and per domain and table whether the focus
names it, whether it is collapsed and where it stands. What is drawn comes from the live model;
places are held by id, so a renamed table keeps its own, and a removed table or domain goes from
every layout with it (``repositories.integrity``, as a part).

Reading and saving are both held to what the caller reads (``readable``): a reader is answered
the places of what it reads only, and its save replaces only those, keeping the places of what
another reader arranged and it cannot see.
"""

# Requirements: REQ-2189

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from sqlalchemy import and_, delete, insert, select

from provisa.core import model_change
from provisa.core.repositories.integrity import ObjectRef, discard
from provisa.core.schema_org import erd_layout_domains, erd_layout_tables, erd_layouts

if TYPE_CHECKING:
    from provisa.core.database import Connection

#: What a domain's place holds beside its id, and a table's.
DOMAIN_FIELDS = ("selected", "collapsed", "x", "y", "w", "h")
TABLE_FIELDS = ("selected", "x", "y")


@dataclass(frozen=True)
class Readable:
    """What the caller reads: ``table(id)`` and ``domain(id)``."""

    table: Callable[[int], bool]
    domain: Callable[[str], bool]


class UnreadablePlace(Exception):
    """A save names a table or domain the caller does not read."""

    def __init__(self, kind: str, ident: str | int) -> None:
        self.kind = kind
        self.ident = ident
        super().__init__(f"the layout places the {kind} {ident!r}, which the caller does not read")


async def names(conn: "Connection") -> list[dict[str, Any]]:
    """Every saved layout's name, who saved it last and when, by name."""
    res = await conn.execute_core(
        select(erd_layouts.c.name, erd_layouts.c.updated_by, erd_layouts.c.updated_at).order_by(
            erd_layouts.c.name
        )
    )
    return [dict(r._mapping) for r in res.fetchall()]


async def read(conn: "Connection", name: str, readable: Readable) -> dict[str, Any] | None:
    """The layout ``name`` with the places of what the caller reads; None when there is none."""
    row = (
        await conn.execute_core(select(erd_layouts).where(erd_layouts.c.name == name))
    ).fetchone()
    if row is None:
        return None
    domains = await conn.execute_core(
        select(erd_layout_domains)
        .where(erd_layout_domains.c.layout_name == name)
        .order_by(erd_layout_domains.c.domain_id)
    )
    tables = await conn.execute_core(
        select(erd_layout_tables)
        .where(erd_layout_tables.c.layout_name == name)
        .order_by(erd_layout_tables.c.table_id)
    )
    return {
        **dict(row._mapping),
        "domains": [
            {"domain_id": d.domain_id, **{f: getattr(d, f) for f in DOMAIN_FIELDS}}
            for d in domains.fetchall()
            if readable.domain(d.domain_id)
        ],
        "tables": [
            {"table_id": t.table_id, **{f: getattr(t, f) for f in TABLE_FIELDS}}
            for t in tables.fetchall()
            if readable.table(t.table_id)
        ],
    }


async def save(
    conn: "Connection",
    name: str,
    options: dict[str, Any],
    domains: list[dict[str, Any]],
    tables: list[dict[str, Any]],
    readable: Readable,
    by: str | None,
) -> None:
    """Create ``name``, or replace the places of it the caller reads. The caller holds the
    transaction."""
    for d in domains:
        if not readable.domain(d["domain_id"]):
            raise UnreadablePlace("domain", d["domain_id"])
    for t in tables:
        if not readable.table(t["table_id"]):
            raise UnreadablePlace("table", t["table_id"])
    model_change.name("save", "ERD layout", name)  # REQ-1524
    await conn.upsert(
        erd_layouts,
        {"name": name, "options": options, "updated_by": by},
        index_elements=["name"],
        update_columns=["options", "updated_by", "updated_at"],
    )
    held_domains = await conn.execute_core(
        select(erd_layout_domains.c.domain_id).where(erd_layout_domains.c.layout_name == name)
    )
    gone_domains = [r.domain_id for r in held_domains.fetchall() if readable.domain(r.domain_id)]
    if gone_domains:
        await conn.execute_core(
            delete(erd_layout_domains).where(
                and_(
                    erd_layout_domains.c.layout_name == name,
                    erd_layout_domains.c.domain_id.in_(gone_domains),
                )
            )
        )
    held_tables = await conn.execute_core(
        select(erd_layout_tables.c.table_id).where(erd_layout_tables.c.layout_name == name)
    )
    gone_tables = [r.table_id for r in held_tables.fetchall() if readable.table(r.table_id)]
    if gone_tables:
        await conn.execute_core(
            delete(erd_layout_tables).where(
                and_(
                    erd_layout_tables.c.layout_name == name,
                    erd_layout_tables.c.table_id.in_(gone_tables),
                )
            )
        )
    await conn.execute_core_many(
        insert(erd_layout_domains), [{"layout_name": name, **d} for d in domains]
    )
    await conn.execute_core_many(
        insert(erd_layout_tables), [{"layout_name": name, **t} for t in tables]
    )


async def remove(conn: "Connection", name: str) -> bool:
    """Delete ``name`` and its places; False when there is none. Nothing refers to a layout."""
    found = (
        await conn.execute_core(select(erd_layouts.c.name).where(erd_layouts.c.name == name))
    ).fetchone()
    if found is None:
        return False
    model_change.name("delete", "ERD layout", name)  # REQ-1524
    await discard(conn, ObjectRef("erd_layout", name))
    return True
