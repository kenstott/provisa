# Copyright (c) 2026 Kenneth Stott
# Canary: 6a559b37-323f-4083-b516-6bd16af803e4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Repository for API endpoint candidates (Phase U), via SQLAlchemy Core (dialect-portable)."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from sqlalchemy import case, select, update

from provisa.api_source.models import ApiColumn, ApiEndpoint, ApiEndpointCandidate
from provisa.core.schema_org import api_endpoint_candidates, api_endpoints

if TYPE_CHECKING:
    from provisa.core.database import Connection

# Requirements: REQ-308, REQ-314, REQ-316


async def store_candidates(  # REQ-314, REQ-316
    conn: "Connection",
    source_id: str,
    candidates: list[ApiEndpointCandidate],
) -> list[int]:
    """Store discovered candidates. Returns list of inserted IDs."""
    ids: list[int] = []
    for c in candidates:
        row_id = await conn.upsert_returning(
            api_endpoint_candidates,
            {
                "source_id": source_id,
                "path": c.path,
                "method": c.method,
                "table_name": c.table_name,
                # JSON column takes a Python object directly.
                "columns": [col.model_dump() for col in c.columns],
                "status": "discovered",
            },
            index_elements=["source_id", "path", "method"],
            returning="id",
            update_columns=["table_name", "columns"],
            # A registered candidate stays registered; anything else becomes discovered.
            set_extra={
                "status": case(
                    (api_endpoint_candidates.c.status == "registered", "registered"),
                    else_="discovered",
                )
            },
        )
        ids.append(row_id)
    return ids


async def list_candidates(  # REQ-599
    conn: "Connection",
    source_id: str | None = None,
) -> list[ApiEndpointCandidate]:
    """List pending (discovered) candidates."""
    stmt = select(api_endpoint_candidates).where(api_endpoint_candidates.c.status == "discovered")
    if source_id:
        stmt = stmt.where(api_endpoint_candidates.c.source_id == source_id)
    stmt = stmt.order_by(api_endpoint_candidates.c.id)
    result = await conn.execute_core(stmt)
    rows = [dict(r._mapping) for r in result.fetchall()]

    def _parse_columns(raw) -> list[ApiColumn]:
        data = raw if isinstance(raw, list) else json.loads(raw)
        return [ApiColumn(**c) for c in data]

    return [
        ApiEndpointCandidate(
            id=r["id"],
            source_id=r["source_id"],
            path=r["path"],
            method=r["method"],
            table_name=r["table_name"],
            columns=_parse_columns(r["columns"]),
            status=r["status"],
        )
        for r in rows
    ]


async def accept_candidate(  # REQ-308, REQ-314, REQ-316
    conn: "Connection",
    candidate_id: int,
    overrides: dict | None = None,
) -> ApiEndpoint:
    """Accept a candidate: register it as an endpoint."""
    result = await conn.execute_core(
        select(api_endpoint_candidates).where(api_endpoint_candidates.c.id == candidate_id)
    )
    row = result.fetchone()
    if row is None:
        raise ValueError(f"Candidate {candidate_id} not found")
    row = dict(row._mapping)
    if row["status"] != "discovered":
        raise ValueError(f"Candidate {candidate_id} status is {row['status']!r}, not 'discovered'")

    overrides = overrides or {}
    table_name = overrides.get("table_name", row["table_name"])
    columns = row["columns"]
    ttl = overrides.get("ttl", 300)
    response_root = overrides.get("response_root")
    error_path = overrides.get("error_path")
    pk_column = overrides.get("pk_column")

    # Insert or update endpoint
    ep_id = await conn.upsert_returning(
        api_endpoints,
        {
            "source_id": row["source_id"],
            "path": row["path"],
            "method": row["method"],
            "table_name": table_name,
            # JSON column takes a Python object directly.
            "columns": columns,
            "ttl": ttl,
            "response_root": response_root,
            "error_path": error_path,
            "pk_column": pk_column,
        },
        index_elements=["table_name"],
        returning="id",
        update_columns=[
            "source_id",
            "path",
            "method",
            "columns",
            "ttl",
            "response_root",
            "error_path",
            "pk_column",
        ],
    )

    # Update candidate status
    await conn.execute_core(
        update(api_endpoint_candidates)
        .where(api_endpoint_candidates.c.id == candidate_id)
        .values(status="registered")
    )

    raw_cols = columns if isinstance(columns, list) else json.loads(columns)
    columns_parsed = [ApiColumn(**c) for c in raw_cols]
    return ApiEndpoint(
        id=ep_id,
        source_id=row["source_id"],
        path=row["path"],
        method=row["method"],
        table_name=table_name,
        columns=columns_parsed,
        ttl=ttl,
        response_root=response_root,
        error_path=error_path,
        pk_column=pk_column,
    )


async def reject_candidate(
    conn: "Connection",
    candidate_id: int,
) -> None:
    """Reject a candidate."""
    result = await conn.execute_core(
        update(api_endpoint_candidates)
        .where(api_endpoint_candidates.c.id == candidate_id)
        .where(api_endpoint_candidates.c.status == "discovered")
        .values(status="rejected")
    )
    if (result.rowcount or 0) == 0:
        raise ValueError(f"Candidate {candidate_id} not found or not in 'discovered' status")
