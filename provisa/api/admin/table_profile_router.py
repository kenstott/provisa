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

from fastapi import APIRouter, HTTPException

from provisa.api.app import state
from provisa.compiler.naming import source_to_catalog

log = logging.getLogger(__name__)
router = APIRouter(prefix="/admin/tables", tags=["admin", "tables"])

# Requirements: REQ-452

_SAMPLE_LIMIT = 2000
_TABLESAMPLE_PCT = 10  # BERNOULLI(10) — 10% of blocks


@router.post("/{table_id}/profile")
async def profile_table(table_id: int) -> dict:  # REQ-452
    if state.tenant_db is None:
        raise HTTPException(503, "Database unavailable")
    if state.federation_engine is None:
        raise HTTPException(503, "Query engine unavailable")
    engine = state.federation_engine

    async with state.tenant_db.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT source_id, schema_name, table_name, view_sql"
            " FROM registered_tables WHERE id = $1",
            table_id,
        )
    if row is None:
        raise HTTPException(404, f"Table {table_id} not found")

    source_id: str = row["source_id"]
    schema_name: str = row["schema_name"]
    table_name: str = row["table_name"]
    view_sql: str | None = row["view_sql"]

    # For __provisa__ view tables, sample the view_sql directly
    if source_id == "__provisa__" and view_sql:
        sql = f"SELECT * FROM ({view_sql.rstrip().rstrip(';')}) _pv LIMIT {_SAMPLE_LIMIT}"
    else:
        view_catalog = os.environ.get("PROVISA_VIEW_CATALOG", "memory")
        catalog = view_catalog if source_id == "__provisa__" else source_to_catalog(source_id)
        fqn = f'"{catalog}"."{schema_name}"."{table_name}"'
        # Try TABLESAMPLE first; fall back to plain LIMIT if unsupported
        sql = (
            f"SELECT * FROM {fqn} TABLESAMPLE BERNOULLI ({_TABLESAMPLE_PCT}) LIMIT {_SAMPLE_LIMIT}"
        )

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
