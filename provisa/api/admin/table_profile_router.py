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
import os
from typing import TYPE_CHECKING

from fastapi import APIRouter, Header, HTTPException
from sqlalchemy import select

from provisa.api.app import state
from provisa.compiler.naming import source_to_catalog
from provisa.core.schema_org import registered_tables

if TYPE_CHECKING:
    from provisa.core.database import Connection  # noqa: F401

log = logging.getLogger(__name__)
router = APIRouter(prefix="/admin/tables", tags=["admin", "tables"])

# Requirements: REQ-452

_SAMPLE_LIMIT = 2000
_TABLESAMPLE_PCT = 10  # BERNOULLI(10) — 10% of blocks


@router.post("/{table_id}/profile")
async def profile_table(
    table_id: int,
    x_provisa_role: str | None = Header(None),
) -> dict:  # REQ-452
    if state.tenant_db is None:
        raise HTTPException(503, "Database unavailable")
    if state.federation_engine is None:
        raise HTTPException(503, "Query engine unavailable")
    engine = state.federation_engine

    async with state.tenant_db.acquire() as conn:
        result = await conn.execute_core(
            select(
                registered_tables.c.source_id,
                registered_tables.c.schema_name,
                registered_tables.c.table_name,
                registered_tables.c.view_sql,
            ).where(registered_tables.c.id == table_id)
        )
        row = result.fetchone()
    if row is None:
        raise HTTPException(404, f"Table {table_id} not found")

    m = row._mapping
    source_id: str = m["source_id"]
    schema_name: str = m["schema_name"]
    table_name: str = m["table_name"]
    view_sql: str | None = m["view_sql"]

    # __provisa__ view SQL is semantic (domain.field refs) and must be compiled,
    # governed, and routed exactly like an interactive /data/sql query — handing it
    # raw to the federation engine fails to resolve domain refs (REQ-452).
    if source_id == "__provisa__" and view_sql:
        # ONE pipeline: sample the view through the single governed chokepoint, exactly like /data/sql.
        from provisa.pgwire._pipeline import _execute_plan, _govern_and_route

        if not x_provisa_role:
            raise HTTPException(400, "X-Provisa-Role header required to profile a view")
        sampled = f"SELECT * FROM ({view_sql.rstrip().rstrip(';')}) _pv LIMIT {_SAMPLE_LIMIT}"
        _plan = await _govern_and_route(sampled, x_provisa_role)
        res = await _execute_plan(_plan, state)
        rows = [dict(zip(res.column_names, r)) for r in res.rows]
        return {"columns": res.column_names, "rows": rows, "rowCount": len(rows)}

    view_catalog = os.environ.get("PROVISA_VIEW_CATALOG", "memory")
    catalog = view_catalog if source_id == "__provisa__" else source_to_catalog(source_id)
    fqn = f'"{catalog}"."{schema_name}"."{table_name}"'
    # Try TABLESAMPLE first; fall back to plain LIMIT if unsupported
    sql = f"SELECT * FROM {fqn} TABLESAMPLE BERNOULLI ({_TABLESAMPLE_PCT}) LIMIT {_SAMPLE_LIMIT}"

    try:
        res = await engine.execute_engine(sql)
        raw_rows = res.rows
        columns = res.column_names
    except Exception:
        if "TABLESAMPLE" in sql:
            # Retry without TABLESAMPLE
            try:
                fqn = f'"{source_to_catalog(source_id)}"."{schema_name}"."{table_name}"'
                res = await engine.execute_engine(f"SELECT * FROM {fqn} LIMIT {_SAMPLE_LIMIT}")
                raw_rows = res.rows
                columns = res.column_names
            except Exception as e:
                raise HTTPException(400, str(e))
        else:
            raise

    rows = [dict(zip(columns, r)) for r in raw_rows]
    return {"columns": columns, "rows": rows, "rowCount": len(rows)}
