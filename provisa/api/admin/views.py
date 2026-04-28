# Copyright (c) 2026 Kenneth Stott
# Canary: f5e4b8a7-362e-487f-9423-d63769c7b69b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Admin view management endpoints extracted from app.py."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request

from provisa.api.admin._config_io import config_path as _config_path, read_config, write_config

router = APIRouter(prefix="/admin", tags=["admin"])


@router.get("/views")
async def list_views():
    """List all configured views."""
    path = _config_path()
    if not path.exists():
        return []
    cfg = read_config()
    return cfg.get("views", [])


@router.post("/views")
async def save_view(request: Request):
    """Add or update a view in the config and reload."""
    from provisa.api.app import _load_and_build

    view = await request.json()
    path = _config_path()
    cfg = read_config()

    views = cfg.setdefault("views", [])
    replaced = False
    for i, v in enumerate(views):
        if v["id"] == view["id"]:
            views[i] = view
            replaced = True
            break
    if not replaced:
        views.append(view)

    write_config(path, cfg)

    try:
        await _load_and_build(str(path))
        return {"success": True, "message": f"View '{view['id']}' saved and reloaded"}
    except Exception as e:
        return {"success": False, "message": str(e)}


@router.delete("/views/{view_id}")
async def delete_view(view_id: str):
    """Delete a view from the config and reload."""
    from provisa.api.app import _load_and_build

    path = _config_path()
    cfg = read_config()

    views = cfg.get("views", [])
    cfg["views"] = [v for v in views if v["id"] != view_id]

    write_config(path, cfg)

    try:
        await _load_and_build(str(path))
        return {"success": True, "message": f"View '{view_id}' deleted"}
    except Exception as e:
        return {"success": False, "message": str(e)}


@router.post("/views/{view_id}/sample")
async def sample_view(view_id: str):
    """Execute a view's SQL with LIMIT 20 and return sample rows."""
    from provisa.api.app import state
    from provisa.compiler.sql_gen import rewrite_semantic_to_trino_physical

    cfg = read_config()

    view = None
    for v in cfg.get("views", []):
        if v["id"] == view_id:
            view = v
            break
    if not view:
        raise HTTPException(status_code=404, detail=f"View '{view_id}' not found")

    if not state.contexts:
        raise HTTPException(status_code=503, detail="Schema not yet built")

    ctx = next(iter(state.contexts.values()))
    sql = view["sql"].strip().rstrip(";")
    physical_sql = rewrite_semantic_to_trino_physical(sql, ctx)
    sample_sql = f"SELECT * FROM ({physical_sql}) _v LIMIT 20"

    if state.trino_conn is None:
        raise HTTPException(status_code=503, detail="Trino not connected")

    try:
        cur = state.trino_conn.cursor()
        cur.execute(sample_sql)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description] if cur.description else []
        return {
            "columns": columns,
            "rows": [dict(zip(columns, row)) for row in rows],
            "count": len(rows),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
