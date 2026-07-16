# Copyright (c) 2026 Kenneth Stott
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

# Requirements: REQ-017, REQ-252

from __future__ import annotations

# complexity-gate: allow-ble=1 reason="the Elasticsearch mapping fetch catches the driver/transport error only to re-raise it as an HTTP 502 with the index name — it translates, never swallows; the exception always propagates to the caller"

import logging
from typing import TYPE_CHECKING, Protocol, cast

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from sqlalchemy import select

from provisa.core.schema_org import sources
from provisa.source_adapters.registry import get_adapter

if TYPE_CHECKING:
    from provisa.core.database import Connection


class _SampleableDriver(Protocol):
    def sample_documents(self, collection: str, limit: int) -> list[dict]: ...


log = logging.getLogger(__name__)

router = APIRouter(prefix="/admin/schema-discovery", tags=["schema-discovery"])

# Source types that do not support schema discovery
_NO_DISCOVER = {"redis", "accumulo"}


class DiscoveredUniqueConstraint(BaseModel):  # REQ-1093
    name: str
    columns: list[str]


class UniqueConstraintsResponse(BaseModel):  # REQ-1093
    source_id: str
    unique_constraints: list[DiscoveredUniqueConstraint]


@router.get("/unique-constraints/{source_id}", response_model=UniqueConstraintsResponse)
async def get_unique_constraints(
    source_id: str, schema: str, table: str
) -> UniqueConstraintsResponse:  # REQ-1093
    """Introspect declared UNIQUE constraints for one (schema, table) on an RDB source.

    Seeds the register/edit "Uniques" panel. Returns an empty list when the source
    exposes none or does not support constraint introspection — uniqueness is never
    inferred from data.
    """
    from provisa.api.app import state
    from provisa.discovery.fk_introspect import introspect_unique_constraints

    if state.tenant_db is None:
        raise HTTPException(status_code=503, detail="Database not connected")
    async with state.tenant_db.acquire() as conn:
        conn = cast("Connection", conn)
        result = await conn.execute_core(select(sources.c.type).where(sources.c.id == source_id))
        fetched = result.fetchone()
    if fetched is None:
        raise HTTPException(status_code=404, detail=f"Source '{source_id}' not found")
    source_type = fetched._mapping["type"]
    raw = await introspect_unique_constraints(
        state.source_pools, source_type, source_id, schema, table
    )
    return UniqueConstraintsResponse(
        source_id=source_id,
        unique_constraints=[
            DiscoveredUniqueConstraint(name=u["name"], columns=u["columns"]) for u in raw
        ],
    )


@router.get("/ir-types", response_model=list[str])
async def list_ir_types() -> list[str]:
    """The canonical IR data-type vocabulary (REQ-846) — the type names the UI offers when a steward
    assigns a column's type during schema discovery, so an assigned type is engine-independent (the
    landing write face maps IR → the store's physical type). Sorted for a stable dropdown order."""
    from provisa.core.ir_types import IR_TYPES

    return sorted(IR_TYPES)


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
    unique_constraints: list[DiscoveredUniqueConstraint] = []  # REQ-1093


class DiscoverRequest(BaseModel):
    """Optional hints for discovery — e.g. collection name, index, keyspace."""

    collection: str | None = None
    index: str | None = None
    keyspace: str | None = None
    table: str | None = None
    schema_name: str | None = None  # REQ-1093: schema for UNIQUE-constraint introspection
    metric: str | None = None
    sample_limit: int = 100


@router.post("/discover/{source_id}", response_model=DiscoverResponse)
async def discover_source_schema(
    source_id: str, body: DiscoverRequest | None = None
):  # REQ-017, REQ-252
    """Look up source, call adapter.discover_schema(), return columns."""
    from provisa.api.app import state

    if state.tenant_db is None:
        raise HTTPException(status_code=503, detail="Database not connected")

    # Fetch source record from DB
    async with state.tenant_db.acquire() as conn:
        conn = cast("Connection", conn)
        result = await conn.execute_core(select(sources).where(sources.c.id == source_id))
        fetched = result.fetchone()
        row = dict(fetched._mapping) if fetched is not None else None

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

    # REQ-1093: seed declared UNIQUE constraints for the register/edit "Uniques" panel.
    # Only for RDB sources with a known table; introspection reads the live source constraints.
    unique_constraints: list[DiscoveredUniqueConstraint] = []
    if hints.table and hints.schema_name:
        from provisa.discovery.fk_introspect import introspect_unique_constraints

        raw_uniques = await introspect_unique_constraints(
            state.source_pools, source_type, source_id, hints.schema_name, hints.table
        )
        unique_constraints = [
            DiscoveredUniqueConstraint(name=u["name"], columns=u["columns"]) for u in raw_uniques
        ]

    return DiscoverResponse(
        source_id=source_id,
        source_type=source_type,
        columns=columns,
        unique_constraints=unique_constraints,
    )


def _call_discover(
    adapter, source_type: str, row, hints: DiscoverRequest
) -> list[dict]:  # REQ-017, REQ-252
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
        driver = cast(_SampleableDriver, source_pools.get(source_id))
        collection = hints.collection or "default"
        sample_docs = driver.sample_documents(
            collection=collection,
            limit=hints.sample_limit,
        )
        return adapter.discover_schema(sample_docs, collection)

    if source_type == "elasticsearch":
        # REQ-252: fetch the live index mapping via GET /<index>/_mapping. No index hint or a
        # transport error raises — discovery must never silently produce empty columns.
        index = hints.index
        if not index:
            raise HTTPException(
                status_code=400,
                detail="Elasticsearch discovery requires an 'index' hint.",
            )
        try:
            properties = adapter.fetch_index_mapping(row["host"], int(row["port"]), index)
        except Exception as e:
            raise HTTPException(
                status_code=502,
                detail=f"Failed to read Elasticsearch mapping for index {index!r}: {e}",
            )
        return adapter.discover_schema(properties)

    if source_type == "cassandra":
        # REQ-252: Cassandra schema lives in system_schema and requires a live CQL session,
        # which Provisa does not maintain. Rather than return empty columns, direct the steward
        # to provide columns explicitly.
        raise HTTPException(
            status_code=501,
            detail=(
                "Cassandra schema discovery requires a live CQL session, which is not "
                "available. Define columns manually for this source."
            ),
        )

    if source_type == "prometheus":
        # Prometheus discover_schema expects metric_metadata dict + metric_name.
        metric = hints.metric or ""
        return adapter.discover_schema({}, metric)

    # Fallback: try calling with no args
    try:
        return adapter.discover_schema()
    except TypeError:
        return []
