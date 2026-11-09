# Copyright (c) 2026 Kenneth Stott
# Canary: b83e5d14-7a2f-4c69-8d03-2f1a9c6e7b48
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The OpenLineage receiver (REQ-2229).

``POST /api/v1/lineage`` is the path every OpenLineage HTTP transport posts to by default, so a
tool's configuration names only Provisa's address and a token. A job's latest run is what is kept:
events of one run add to each other, since a tool may send its datasets on START, on COMPLETE or on
both, and a newer run with datasets replaces the older run's.
"""

# Requirements: REQ-2229

from __future__ import annotations

import json

from fastapi import APIRouter, Request
from sqlalchemy import and_, delete, select

from provisa.api.admin.capabilities import require_capability_request
from provisa.api.errors import ApiError
from provisa.lineage.reported import InvalidRunEvent, RunEvent, dataset_rows, parse_run_event

router = APIRouter()

_DATASET_KEY = [
    "job_namespace",
    "job_name",
    "run_id",
    "direction",
    "dataset_namespace",
    "dataset_name",
]


async def store_run_event(conn, event: RunEvent) -> None:
    """Keep what ``event`` says of its job's latest run. An event naming no dataset says nothing
    to keep; one from a run older than the job's latest is out of date."""
    from provisa.core.schema_org import openlineage_run_datasets as table

    rows = dataset_rows(event)
    if not rows:
        return
    job = and_(table.c.job_namespace == event.job_namespace, table.c.job_name == event.job_name)
    other_run = table.c.run_id != event.run_id
    async with conn.transaction():
        newer = await conn.execute_core(
            select(table.c.id).where(job, other_run, table.c.event_time > event.event_time).limit(1)
        )
        if newer.fetchone() is not None:
            return
        await conn.execute_core(delete(table).where(job, other_run))
        for row in rows:
            await conn.upsert(
                table,
                {
                    "job_namespace": event.job_namespace,
                    "job_name": event.job_name,
                    "run_id": event.run_id,
                    "event_time": event.event_time,
                    **row,
                },
                index_elements=_DATASET_KEY,
                # An event with no facet for a dataset leaves the facet another event sent.
                update_columns=["event_time", "column_lineage"]
                if row["column_lineage"]
                else ["event_time"],
            )


async def _store(state, event: RunEvent) -> None:
    async with state.model_db.acquire() as conn:
        await store_run_event(conn, event)


@router.post("/api/v1/lineage")
async def receive_run_event(request: Request) -> dict:
    """Accept one OpenLineage run event into the lineage graph."""
    require_capability_request(request, "lineage_report")
    from provisa.api.app import state

    try:
        event = parse_run_event(json.loads(await request.body()))
    except (InvalidRunEvent, ValueError) as exc:
        raise ApiError(422, "lineage.invalid_run_event", str(exc)) from exc
    await _store(state, event)
    return {"status": "accepted"}
