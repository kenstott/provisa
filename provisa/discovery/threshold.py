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

import json
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from sqlalchemy import select

from provisa.core.schema_org import user_directory
from provisa.discovery.measure import DISTINCT_BOUND

if TYPE_CHECKING:
    from provisa.core.database import Connection

#: The threshold of a person who has set none: the column's default (REQ-413: 70%, the
#: ``server_default`` of ``user_directory.discovery_threshold``).
DEFAULT = 0.7


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


@dataclass(frozen=True)
class KeyValueSettings:
    """Key value overlap as the person last set it (REQ-413, amended 2026-11-16), remembered as
    the threshold is: whether it is chosen, its measure cutoff, and its sample size."""

    chosen: bool
    cutoff: float
    sample_size: int

    def as_dict(self) -> dict[str, Any]:
        return {"chosen": self.chosen, "cutoff": self.cutoff, "sample_size": self.sample_size}


#: Key value overlap of a person who has set none: the columns' defaults (REQ-413: chosen, a
#: cutoff of 20%, a sample size of 10,000 distinct values).
KEY_VALUES_DEFAULT = KeyValueSettings(chosen=True, cutoff=0.2, sample_size=DISTINCT_BOUND)


async def read_key_values(conn: "Connection", user_id: str) -> KeyValueSettings:
    """The person's Key value overlap; :data:`KEY_VALUES_DEFAULT` when they have no row."""
    d = user_directory.c
    row = (
        await conn.execute_core(
            select(
                d.discovery_key_values, d.discovery_measure_cutoff, d.discovery_sample_size
            ).where(d.user_id == user_id)
        )
    ).fetchone()
    if row is None:
        return KEY_VALUES_DEFAULT
    return KeyValueSettings(bool(row[0]), float(row[1]), int(row[2]))


async def keep_key_values(
    conn: "Connection", user_id: str, settings: KeyValueSettings
) -> KeyValueSettings:
    """Keep ``settings`` as the person's own."""
    await conn.upsert(
        user_directory,
        {
            "user_id": user_id,
            "discovery_key_values": settings.chosen,
            "discovery_measure_cutoff": settings.cutoff,
            "discovery_sample_size": settings.sample_size,
        },
        index_elements=["user_id"],
        update_columns=[
            "discovery_key_values",
            "discovery_measure_cutoff",
            "discovery_sample_size",
        ],
    )
    return settings


#: The Score filter of a person who has set none: Score at least 10% (REQ-413, amended
#: 2026-11-18), the ``server_default`` of ``user_directory.discovery_score_filter``.
SCORE_FILTER_DEFAULT: dict[str, str] = {"op": "ge", "a": "10"}


async def read_score_filter(conn: "Connection", user_id: str) -> dict[str, Any] | None:
    """The person's filter on a run's Score column; None when they cleared it, and
    :data:`SCORE_FILTER_DEFAULT` when they have no row."""
    d = user_directory.c
    row = (
        await conn.execute_core(select(d.discovery_score_filter).where(d.user_id == user_id))
    ).fetchone()
    if row is None:
        return dict(SCORE_FILTER_DEFAULT)
    return None if row[0] is None else json.loads(row[0])


async def keep_score_filter(
    conn: "Connection", user_id: str, spec: dict[str, Any] | None
) -> dict[str, Any] | None:
    """Keep ``spec`` (None: cleared) as the person's filter on a run's Score column."""
    await conn.upsert(
        user_directory,
        {
            "user_id": user_id,
            "discovery_score_filter": None if spec is None else json.dumps(spec),
        },
        index_elements=["user_id"],
        update_columns=["discovery_score_filter"],
    )
    return spec
