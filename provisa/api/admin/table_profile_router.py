# Copyright (c) 2026 Kenneth Stott
# Canary: 9a2b4c6d-8e0f-4a1b-2c3d-5e6f7a8b9c0d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Admin route: profile a registered table with a sampled SELECT."""

from __future__ import annotations

import logging

from fastapi import APIRouter, Header, Request

from provisa.api.acting_role import header_role
from provisa.api.admin.table_read import read_as_caller, table_for_steward
from provisa.api.admin.viewer_statement import ViewerRequest
from provisa.api.app import state
from provisa.api.errors import ApiError
from provisa.compiler.limits import role_max_query_time_ms
from provisa.core import request_deadline
from provisa.core.limits import request_timeout_for
from provisa.security.rights import require_role

log = logging.getLogger(__name__)
router = APIRouter(prefix="/admin/tables", tags=["admin", "tables"])

# Requirements: REQ-452, REQ-1921

_SAMPLE_LIMIT = 2000


async def _rows(
    request: Request,
    table_id: int,
    x_provisa_role: str | None,
    *,
    limit: int | None = None,
    page: ViewerRequest | None = None,
) -> dict:
    """Up to ``limit`` rows of the table, read as the caller (admin/table_read.py): the right
    to register tables in the table's own domain, then the caller's role — its row filters, its
    masks, the columns it may see."""
    if state.model_db is None:
        raise ApiError(503, "profile.database_unavailable", "Database unavailable")
    if state.federation_engine is None:
        raise ApiError(503, "profile.query_engine_unavailable", "Query engine unavailable")
    table = table_for_steward(request, state, table_id)
    # REQ-273: read as the role the request runs as — the one the auth layer established (a
    # held role, a set's meta-role), never the raw header text.
    role_id = header_role(request, x_provisa_role, None)
    if not role_id:
        raise ApiError(
            400,
            "profile.role_header_required",
            "X-Provisa-Role header required to read a table's rows",
        )
    # The read's time is the role's, as a statement's on /data/sql is (REQ-1905): the deadline
    # is what a read that waits on a replica's build answers before (ReplicaBuilding, which
    # the app answers by code).
    transport_timeout = request_timeout_for("graphql")
    role_ms = role_max_query_time_ms(require_role(state.roles, role_id))
    timeout = transport_timeout if role_ms is None else min(transport_timeout, role_ms / 1000.0)
    with request_deadline.within(timeout):
        result = await read_as_caller(state, table_id, role_id, limit=limit, request=page)
    rows = [dict(zip(result.column_names, row)) for row in result.rows]
    return {
        "columns": list(result.column_names),
        "rows": rows,
        "rowCount": len(rows),
        "withdrawn": bool(table.get("withdrawn")),
    }


@router.post("/{table_id}/profile")
async def profile_table(
    request: Request,
    table_id: int,
    x_provisa_role: str | None = Header(None),
) -> dict:  # REQ-452
    """A sample of the table's rows for its profile, read as the caller."""
    return await _rows(request, table_id, x_provisa_role, limit=_SAMPLE_LIMIT)


@router.post("/{table_id}/preview")
async def preview_table(
    request: Request,
    table_id: int,
    page: ViewerRequest,
    x_provisa_role: str | None = Header(None),
) -> dict:  # REQ-1921
    """One page of the table as the table viewer shows it, read as the caller: the viewer of a
    WITHDRAWN table, which no statement a caller sends may read. The body is what the viewer chose — its
    filters, sorts, grouping and page — and the server writes the statement from it. One row
    more than the page comes back when a next page exists."""
    return await _rows(request, table_id, x_provisa_role, page=page)
