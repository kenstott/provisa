# Copyright (c) 2026 Kenneth Stott
# Canary: d9aa39d3-0d9e-4e15-a1c1-48add4077e3c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""POST /events/ingest/{source_id}/{table} — governed HTTP push receiver (Phase AS, REQ-333).

External services (e.g. OTEL Collector, Fluentd) POST JSON to this endpoint.
Provisa extracts column values using steward-declared dot-notation paths,
applies type coercion, and writes one row to the ingest backing table per event.
"""

from __future__ import annotations

import logging
from typing import Any, cast

from fastapi import APIRouter, HTTPException, Request
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

# Requirements: REQ-331, REQ-333, REQ-334, REQ-335

log = logging.getLogger(__name__)

router = APIRouter(prefix="/events/ingest", tags=["ingest"])


@router.post("/{source_id}/{table}", status_code=202)
async def ingest_event(  # REQ-331, REQ-333, REQ-335
    source_id: str,
    table: str,
    request: Request,
) -> dict[str, str]:
    """Accept a JSON payload and write extracted column values to the ingest table.

    The backing table and column definitions must exist in AppState.ingest_tables.
    Returns 404 if the source/table is unknown, 503 if no engine is available.
    """
    from provisa.api.app import state

    # Validate source and table are registered ingest targets
    source_tables = state.ingest_tables.get(source_id)
    if source_tables is None:
        raise HTTPException(status_code=404, detail=f"Ingest source {source_id!r} not found")

    columns = source_tables.get(table)
    if columns is None:
        raise HTTPException(
            status_code=404, detail=f"Ingest table {table!r} not found for source {source_id!r}"
        )

    engine = state.ingest_engines.get(source_id)
    if engine is None:
        raise HTTPException(
            status_code=503, detail=f"No engine available for ingest source {source_id!r}"
        )

    try:
        body: Any = await request.json()
    except Exception as exc:
        raise HTTPException(status_code=400, detail="Request body must be valid JSON") from exc

    # Support both a single event dict and a list of events
    events: list[Any] = body if isinstance(body, list) else [body]

    inserted = 0
    for event in events:
        if not isinstance(event, dict):
            continue
        row_data = _extract_row(event, columns)
        try:
            await _insert_row(cast("AsyncEngine", engine), table, row_data)
            inserted += 1
        except Exception:
            log.warning("Failed to insert ingest row into %s.%s", source_id, table, exc_info=True)

    return {"status": "accepted", "inserted": str(inserted)}


def _extract_row(
    payload: dict[str, Any], columns: list[dict[str, Any]]
) -> dict[str, Any]:  # REQ-334
    """Extract column values from *payload* using dot-notation paths.

    For columns without a ``path``, falls back to top-level key lookup by
    ``column_name``.  Missing paths yield ``None``.
    """
    from provisa.ingest.ddl import extract_value

    row: dict[str, Any] = {}
    for col in columns:
        raw_name = col.get("column_name") or col.get("name", "")
        name = raw_name if isinstance(raw_name, str) else ""
        if not name or name.startswith("_"):
            continue
        raw_path = col.get("path") or name
        path = raw_path if isinstance(raw_path, str) else name
        row[name] = extract_value(payload, path)
    return row


async def _insert_row(engine: AsyncEngine, table: str, data: dict[str, Any]) -> None:
    if not data:
        return
    cols = ", ".join(data.keys())
    placeholders = ", ".join(f":{k}" for k in data.keys())
    stmt = text(
        f"INSERT INTO {table} ({cols}, _received_at, _updated_at) "  # noqa: S608
        f"VALUES ({placeholders}, NOW(), NOW())"
    )
    async with engine.begin() as conn:
        await conn.execute(stmt, data)
