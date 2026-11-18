# Copyright (c) 2026 Kenneth Stott
# Canary: 8d4b2f61-0c75-4e93-b1a8-5f7e3c9d2a46
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The score from which a discovery run's proposals start ticked, and every other threshold and
sample size a run uses, kept for the person who set them (REQ-413, amended 2026-11-18).

It is kept in the person's row of ``user_directory``, the tenant's per-person store that also
holds their email preference. A person who has set none has the column's default,
:data:`DEFAULT`, the threshold a suggestion is kept at (REQ-413: rows scoring 0.70 or more start
ticked).
"""

# Requirements: REQ-413

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from sqlalchemy import select

from provisa.core.schema_org import user_directory
from provisa.discovery.context import CONTAINED_AT, SAMPLE_VALUES, SHARED_CEILING
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
    """Key value overlap as the person last set it (REQ-413, amended 2026-11-16 and 2026-11-18),
    remembered as the threshold is: whether it is chosen, its measure cutoff, its sample size, the
    share of values in a key target at which a pair is a key relationship, and the most a pair
    scores when neither side is a key."""

    chosen: bool
    cutoff: float
    sample_size: int
    contained_at: float
    shared_ceiling: float

    def as_dict(self) -> dict[str, Any]:
        return {
            "chosen": self.chosen,
            "cutoff": self.cutoff,
            "sample_size": self.sample_size,
            "contained_at": self.contained_at,
            "shared_ceiling": self.shared_ceiling,
        }


@dataclass(frozen=True)
class RunSettings:
    """Every threshold and sample size a run uses, as the person last set them in the discover
    dialog (REQ-413, amended 2026-11-18): their Tick from, the sample values shown per column,
    and Key value overlap's."""

    threshold: float
    samples_per_column: int
    key_values: KeyValueSettings

    def as_dict(self) -> dict[str, Any]:
        return {
            "threshold": self.threshold,
            "samples_per_column": self.samples_per_column,
            "key_values": self.key_values.as_dict(),
        }


#: The settings of a person who has set none: the columns' defaults, which only prefill the
#: dialog (REQ-413: a threshold of 70%; a cutoff of 20%; a sample size of 10,000 values).
DEFAULTS = RunSettings(
    threshold=DEFAULT,
    samples_per_column=SAMPLE_VALUES,
    key_values=KeyValueSettings(
        chosen=True,
        cutoff=0.2,
        sample_size=DISTINCT_BOUND,
        contained_at=CONTAINED_AT,
        shared_ceiling=SHARED_CEILING,
    ),
)


async def read_settings(conn: "Connection", user_id: str) -> RunSettings:
    """The person's run settings; :data:`DEFAULTS` when they have no row."""
    d = user_directory.c
    row = (
        await conn.execute_core(
            select(
                d.discovery_threshold,
                d.discovery_samples_per_column,
                d.discovery_key_values,
                d.discovery_measure_cutoff,
                d.discovery_sample_size,
                d.discovery_contained_at,
                d.discovery_shared_ceiling,
            ).where(d.user_id == user_id)
        )
    ).fetchone()
    if row is None:
        return DEFAULTS
    return RunSettings(
        float(row[0]),
        int(row[1]),
        KeyValueSettings(bool(row[2]), float(row[3]), int(row[4]), float(row[5]), float(row[6])),
    )


async def keep_settings(conn: "Connection", user_id: str, settings: RunSettings) -> RunSettings:
    """Keep ``settings`` as the person's own."""
    kv = settings.key_values
    values = {
        "discovery_threshold": settings.threshold,
        "discovery_samples_per_column": settings.samples_per_column,
        "discovery_key_values": kv.chosen,
        "discovery_measure_cutoff": kv.cutoff,
        "discovery_sample_size": kv.sample_size,
        "discovery_contained_at": kv.contained_at,
        "discovery_shared_ceiling": kv.shared_ceiling,
    }
    await conn.upsert(
        user_directory,
        {"user_id": user_id, **values},
        index_elements=["user_id"],
        update_columns=list(values),
    )
    return settings
