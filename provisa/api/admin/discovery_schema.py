# Copyright (c) 2025 Kenneth Stott
# Canary: 8f2a1b3c-d4e5-6789-abcd-ef0123456789
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Admin router for NoSQL/non-relational schema discovery (Phase AI8).

POST /admin/discover/{source_id} — introspects the source via its adapter's
discover_schema() and returns candidate columns.
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from provisa.source_adapters.registry import get_adapter

log = logging.getLogger(__name__)

router = APIRouter(prefix="/admin/schema-discovery", tags=["schema-discovery"])

# Source types that do not support schema discovery
_NO_DISCOVER = {"redis", "accumulo"}

# Source types that require a live connection (in source_pools) for sampling
_REQUIRES_LIVE_CONNECTION = {"mongodb"}


def _get_source_pool():
    """Return the current source_pools from app state."""
    from provisa.api.app import state
    return state.source_pools


class DiscoveredColumn(BaseModel):
    name: str
    type: str
    nullable: bool = True
    description: str = ""
    source_path: str = ""


class DiscoverResponse(BaseModel):
    source_id: str
    source_type: str
    columns: list[DiscoveredColumn]


class DiscoverRequest(BaseModel):
    """Optional hints for discovery — e.g. collection name, index, keyspace."""
    collection: str | None = None
    index: str | None = None
    keyspace: str | None = None
    table: str | None = None
    metric: str | None = None
    sample_limit: int = 100


@router.post("/discover/{source_id}", response_model=DiscoverResponse)
async def discover_source_schema(source_id: str, body: DiscoverRequest | None = None):
    """Look up source, call adapter.discover_schema(), return columns."""
    from provisa.api.app import state

    if state.pg_pool is None:
        raise HTTPException(status_code=503, detail="Database not connected")

    # Fetch source record from DB
    async with state.pg_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM sources WHERE id = $1", source_id)

    if row is None:
        raise HTTPException(status_code=404, detail=f"Source '{source_id}' not found")

    source_type = row["type"]

    if source_type in _NO_DISCOVER:
        raise HTTPException(
            status_code=400,
            detail=f"Source type '{source_type}' does not support schema discovery. "
            "Define columns manually.",
        )

    try:
        adapter = get_adapter(source_type)
    except KeyError:
        raise HTTPException(
            status_code=400,
            detail=f"No adapter registered for source type '{source_type}'",
        )

    if not hasattr(adapter, "discover_schema"):
        raise HTTPException(
            status_code=400,
            detail=f"Adapter for '{source_type}' does not implement discover_schema",
        )

    hints = body or DiscoverRequest()

    # Build adapter-specific discovery args from source record + hints
    raw_columns = _call_discover(adapter, source_type, row, hints)

    columns = [
        DiscoveredColumn(
            name=col.get("name", ""),
            type=col.get("type", "VARCHAR"),
            nullable=col.get("nullable", True),
            description=col.get("description", ""),
            source_path=col.get("sourcePath", col.get("source_path", "")),
        )
        for col in raw_columns
    ]

    return DiscoverResponse(
        source_id=source_id,
        source_type=source_type,
        columns=columns,
    )



def _call_discover(adapter, source_type: str, row, hints: DiscoverRequest) -> list[dict]:
    """Dispatch to the correct adapter.discover_schema() signature."""
    if source_type == "mongodb":
        # MongoDB discover_schema requires sample documents from a live connection.
        # Check source_pools for an active connection; raise 503 if none exists.
        source_pools = _get_source_pool()
        source_id = row["id"]
        if not source_pools.has(source_id):
            raise HTTPException(
                status_code=503,
                detail=(
                    f"No live connection for source '{source_id}'. "
                    "MongoDB schema discovery requires an active connection in the source pool. "
                    "Verify the source is connected and the server is reachable."
                ),
            )
        driver = source_pools.get(source_id)
        collection = hints.collection or "default"
        sample_docs = driver.sample_documents(
            collection=collection,
            limit=hints.sample_limit,
        )
        return adapter.discover_schema(sample_docs, collection)

    if source_type == "elasticsearch":
        # ES discover_schema expects an index mapping dict.
        # Real deployment would call ES /_mapping API; stub returns empty.
        return adapter.discover_schema({})

    if source_type == "cassandra":
        # Cassandra discover_schema expects keyspace metadata dict.
        return adapter.discover_schema({})

    if source_type == "prometheus":
        # Prometheus discover_schema expects metric_metadata dict + metric_name.
        metric = hints.metric or ""
        return adapter.discover_schema({}, metric)

    # Fallback: try calling with no args
    try:
        return adapter.discover_schema()
    except TypeError:
        return []
