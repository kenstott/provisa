# Copyright (c) 2026 Kenneth Stott
# Canary: 5b2e9c71-d04a-4f3e-8a6b-93c1e7f20d48
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Each dataset's latest schema, documentation and ownership facets (REQ-2263).

The OpenLineage receiver (REQ-2229) writes every such facet an event carries; a facet replaces the
stored one unless the stored one came from a later event. The OpenLineage import paths read them.
"""

# Requirements: REQ-2263

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Iterable

from sqlalchemy import and_, select

from provisa.core.schema_org import openlineage_dataset_facets as _table

_KEY = ["dataset_namespace", "dataset_name", "facet"]


@dataclass(frozen=True)
class StoredFacet:
    payload: dict
    run_id: str
    event_time: datetime


@dataclass(frozen=True)
class DatasetFacets:
    namespace: str
    name: str
    #: facet name ("schema", "documentation", "ownership") -> the latest one received
    facets: dict[str, StoredFacet] = field(default_factory=dict)


async def write_dataset_facets(
    conn: Any, rows: Iterable[dict], *, run_id: str, event_time: datetime
) -> None:
    """Keep each facet row (dataset_namespace, dataset_name, facet, payload) as its dataset's
    latest, from ``run_id`` at ``event_time``. A facet stored from a later event stays."""
    async with conn.transaction():
        for row in rows:
            where = and_(*[_table.c[k] == row[k] for k in _KEY])
            stored = await conn.execute_core(select(_table.c.event_time).where(where))
            found = stored.fetchone()
            if found is not None and found.event_time > event_time:
                continue
            await conn.upsert(
                _table,
                {**row, "run_id": run_id, "event_time": event_time},
                index_elements=_KEY,
            )


async def read_dataset_facets(conn: Any, namespace: str | None = None) -> list[DatasetFacets]:
    """Every dataset with a stored facet, or only those of ``namespace``, ordered by namespace
    and name."""
    query = select(_table).order_by(_table.c.dataset_namespace, _table.c.dataset_name)
    if namespace is not None:
        query = query.where(_table.c.dataset_namespace == namespace)
    result = await conn.execute_core(query)
    by_dataset: dict[tuple[str, str], dict[str, StoredFacet]] = {}
    for r in result.fetchall():
        by_dataset.setdefault((r.dataset_namespace, r.dataset_name), {})[r.facet] = StoredFacet(
            r.payload, r.run_id, r.event_time
        )
    return [DatasetFacets(ns, name, facets) for (ns, name), facets in by_dataset.items()]
