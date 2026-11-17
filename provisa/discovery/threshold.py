# Copyright (c) 2026 Kenneth Stott
# Canary: 8d4b2f61-0c75-4e93-b1a8-5f7e3c9d2a46
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The score from which a discovery run's proposals start ticked, kept for the person who set it
(REQ-413).

It is kept in the person's row of ``user_directory``, the tenant's per-person store that also
holds their email preference. A person who has set none has the column's default,
:data:`DEFAULT`, the threshold a suggestion is kept at (REQ-413: rows scoring 0.70 or more start
ticked).
"""

# Requirements: REQ-413

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import select

from provisa.core.schema_org import user_directory
from provisa.discovery.context import KEPT_AT

if TYPE_CHECKING:
    from provisa.core.database import Connection

#: The threshold of a person who has set none: the column's default.
DEFAULT = KEPT_AT


async def read(conn: "Connection", user_id: str) -> float:
    """The person's threshold; :data:`DEFAULT` when they have no row (REQ-413: the default
    selection starts at 0.70)."""
    row = (
        await conn.execute_core(
            select(user_directory.c.discovery_threshold).where(user_directory.c.user_id == user_id)
        )
    ).fetchone()
    return DEFAULT if row is None else float(row[0])


async def keep(conn: "Connection", user_id: str, threshold: float) -> float:
    """Keep ``threshold`` as the person's own."""
    await conn.upsert(
        user_directory,
        {"user_id": user_id, "discovery_threshold": threshold},
        index_elements=["user_id"],
        update_columns=["discovery_threshold"],
    )
    return threshold
