# Copyright (c) 2026 Kenneth Stott
# Canary: 8d4b2f61-0c75-4e93-b1a8-5f7e3c9d2a46
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A person's discovery settings, kept for the person who set them (REQ-413, amended
2026-11-18): the score from which a run's proposals start ticked, every other threshold and
sample size a run uses, and the filter on a run's Score column.

They are kept in the person's row of ``user_directory``, the tenant's per-person store that also
holds their email preference, read by :func:`read_settings` and written by
:func:`keep_settings`. A person who has set none has the columns' defaults, :data:`DEFAULTS`
(REQ-413: rows scoring 0.70 or more start ticked; Score >= 10% shown).
"""

# Requirements: REQ-413

from __future__ import annotations

import json
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


#: The Score filter of a person who has set none: Score at least 10% (REQ-413, amended
#: 2026-11-18), the ``server_default`` of ``user_directory.discovery_score_filter``.
SCORE_FILTER_DEFAULT: dict[str, str] = {"op": "ge", "a": "10"}


@dataclass(frozen=True)
class RunSettings:
    """Every threshold and sample size a run uses, as the person last set them in the discover
    dialog (REQ-413, amended 2026-11-18): their Tick from, the sample values shown per column,
    and Key value overlap's; and the filter on a run's Score column as they last set it in the
    results (None: they cleared it)."""

    threshold: float
    samples_per_column: int
    key_values: KeyValueSettings
    score_filter: dict[str, Any] | None

    def as_dict(self) -> dict[str, Any]:
        return {
            "threshold": self.threshold,
            "samples_per_column": self.samples_per_column,
            "key_values": self.key_values.as_dict(),
            "score_filter": self.score_filter,
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
    score_filter=SCORE_FILTER_DEFAULT,
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
                d.discovery_score_filter,
            ).where(d.user_id == user_id)
        )
    ).fetchone()
    if row is None:
        return DEFAULTS
    return RunSettings(
        float(row[0]),
        int(row[1]),
        KeyValueSettings(bool(row[2]), float(row[3]), int(row[4]), float(row[5]), float(row[6])),
        None if row[7] is None else json.loads(row[7]),
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
        "discovery_score_filter": (
            None if settings.score_filter is None else json.dumps(settings.score_filter)
        ),
    }
    await conn.upsert(
        user_directory,
        {"user_id": user_id, **values},
        index_elements=["user_id"],
        update_columns=list(values),
    )
    return settings
