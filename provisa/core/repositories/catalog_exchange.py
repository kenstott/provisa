# Copyright (c) 2026 Kenneth Stott
# Canary: 0b7e4a19-5c62-4d83-a9f1-3e8d6c2b7a50
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The value last exchanged with a Syncing entry (REQ-2247, REQ-2122).

One row per (entry, direction, kind, object): ``export`` when Provisa's value reached the system
(a publish that succeeded, or a steward taking Provisa's side), ``import`` when the system's value
reached Provisa (a steward taking the catalog's side). Each row records who decided it: ``publish``
for a publish's own write, or the steward by name. The import review shows the latest of the two
directions beside both current values, so a steward sees which side changed since.
"""

# Requirements: REQ-2247, REQ-2122

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any, Iterable, Union

from sqlalchemy import func, select

from provisa.core.schema_org import catalog_exchanges

if TYPE_CHECKING:
    from provisa.core.database import Connection

DIRECTIONS = ("export", "import")

#: Who decided a value a publish wrote itself.
PUBLISH = "publish"

# A value as Syncing compares it (provisa.api.metadata_export.sync_values): a description, or a
# sorted tuple of names. Stored as JSON, a tuple as a list.
Value = Union[str, tuple[str, ...]]
Key = tuple[str, str]


def to_json(value: Value) -> Any:
    return list(value) if isinstance(value, tuple) else value


def from_json(value: Any) -> Value:
    return tuple(value) if isinstance(value, list) else value


@dataclass(frozen=True)
class Exchange:  # REQ-2247
    """One value last exchanged: its direction, the value, who decided it and when."""

    direction: str
    value: Value
    decided_by: str
    at: datetime


async def _held(conn: "Connection", entry: str, direction: str) -> dict[Key, Value]:
    result = await conn.execute_core(
        select(catalog_exchanges.c.kind, catalog_exchanges.c.object, catalog_exchanges.c.value)
        .where(catalog_exchanges.c.entry == entry)
        .where(catalog_exchanges.c.direction == direction)
    )
    return {(row.kind, row.object): from_json(row.value) for row in result.fetchall()}


async def record(
    conn: "Connection",
    entry: str,
    direction: str,
    values: Iterable[tuple[str, str, Value]],
    *,
    decided_by: str,
    unchanged_too: bool = False,
) -> int:
    """Record ``(kind, object, value)`` as last exchanged with ``entry`` in ``direction``.

    A value already held as last exchanged is left as it is (its time is when it last changed
    hands), unless ``unchanged_too``: a steward's decision is recorded even when the value is the
    one held, so the row names who decided it. Returns how many rows were written.
    """
    if direction not in DIRECTIONS:
        raise ValueError(f"unknown sync direction {direction!r}")
    held = await _held(conn, entry, direction)
    written = 0
    for kind, obj, value in values:
        if not unchanged_too and held.get((kind, obj)) == value:
            continue
        await conn.upsert(
            catalog_exchanges,
            {
                "entry": entry,
                "direction": direction,
                "kind": kind,
                "object": obj,
                "value": to_json(value),
                "decided_by": decided_by,
            },
            index_elements=["entry", "direction", "kind", "object"],
            update_columns=["value", "decided_by"],
            set_extra={"updated_at": func.now()},
        )
        written += 1
    return written


async def latest(conn: "Connection", entry: str) -> dict[Key, Exchange]:
    """The value last exchanged with ``entry`` per (kind, object), whichever direction was later."""
    result = await conn.execute_core(
        select(
            catalog_exchanges.c.direction,
            catalog_exchanges.c.kind,
            catalog_exchanges.c.object,
            catalog_exchanges.c.value,
            catalog_exchanges.c.decided_by,
            catalog_exchanges.c.updated_at,
            catalog_exchanges.c.id,
        )
        .where(catalog_exchanges.c.entry == entry)
        # Later rows win: by time, then by id where one transaction wrote both directions.
        .order_by(catalog_exchanges.c.updated_at, catalog_exchanges.c.id)
    )
    out: dict[Key, Exchange] = {}
    for row in result.fetchall():
        out[(row.kind, row.object)] = Exchange(
            direction=row.direction,
            value=from_json(row.value),
            decided_by=row.decided_by,
            at=row.updated_at,
        )
    return out
