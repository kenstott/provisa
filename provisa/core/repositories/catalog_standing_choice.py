# Copyright (c) 2026 Kenneth Stott
# Canary: 4c9e2b71-8d35-4a06-b7f1-5e3a0d9c6b28
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A Syncing entry's standing choices (REQ-2247).

One row per (entry, direction, kind of difference): ``provisa`` takes Provisa's value, ``catalog``
the other system's, ``hold`` leaves the difference for a steward. A kind with no row has no standing
choice. Each row names who set it.
"""

# Requirements: REQ-2247

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import delete, func, select

from provisa.core.repositories.catalog_exchange import DIRECTIONS
from provisa.core.schema_org import catalog_standing_choices

if TYPE_CHECKING:
    from provisa.core.database import Connection

PROVISA = "provisa"
CATALOG = "catalog"
HOLD = "hold"
#: The choices a steward sets. Asking a webhook is a fourth, added with the entry's webhooks.
CHOICES = (PROVISA, CATALOG, HOLD)


async def held(conn: "Connection", entry: str) -> dict[str, dict[str, str]]:
    """The entry's standing choices: per direction, the choice of each kind that has one."""
    result = await conn.execute_core(
        select(
            catalog_standing_choices.c.direction,
            catalog_standing_choices.c.kind,
            catalog_standing_choices.c.choice,
        ).where(catalog_standing_choices.c.entry == entry)
    )
    out: dict[str, dict[str, str]] = {direction: {} for direction in DIRECTIONS}
    for row in result.fetchall():
        out[row.direction][row.kind] = row.choice
    return out


async def set_choice(
    conn: "Connection",
    entry: str,
    direction: str,
    kind: str,
    choice: str | None,
    *,
    decided_by: str,
) -> None:
    """Set the entry's standing choice for ``direction`` and ``kind``; ``None`` clears it."""
    if direction not in DIRECTIONS:
        raise ValueError(f"unknown sync direction {direction!r}")
    if choice is None:
        await conn.execute_core(
            delete(catalog_standing_choices)
            .where(catalog_standing_choices.c.entry == entry)
            .where(catalog_standing_choices.c.direction == direction)
            .where(catalog_standing_choices.c.kind == kind)
        )
        return
    if choice not in CHOICES:
        raise ValueError(f"unknown standing choice {choice!r}")
    await conn.upsert(
        catalog_standing_choices,
        {
            "entry": entry,
            "direction": direction,
            "kind": kind,
            "choice": choice,
            "decided_by": decided_by,
        },
        index_elements=["entry", "direction", "kind"],
        update_columns=["choice", "decided_by"],
        set_extra={"updated_at": func.now()},
    )
