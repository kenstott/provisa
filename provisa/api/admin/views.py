# Copyright (c) 2025 Kenneth Stott
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

import os
from pathlib import Path

import yaml
from fastapi import APIRouter, HTTPException, Request

router = APIRouter(prefix="/admin", tags=["admin"])


@router.get("/views")
async def list_views():
    """List all configured views."""
    config_path = os.environ.get("PROVISA_CONFIG", "config/provisa.yaml")
    path = Path(config_path)
    if not path.exists():
        return []
    with open(path) as f:
        cfg = yaml.safe_load(f)
    return cfg.get("views", [])


@router.post("/views")
async def save_view(request: Request):
    """Add or update a view in the config and reload."""
    from provisa.api.app import _load_and_build

    view = await request.json()
    config_path = os.environ.get("PROVISA_CONFIG", "config/provisa.yaml")
    path = Path(config_path)
    with open(path) as f:
        cfg = yaml.safe_load(f)

    views = cfg.setdefault("views", [])
    replaced = False
    for i, v in enumerate(views):
        if v["id"] == view["id"]:
            views[i] = view
            replaced = True
            break
    if not replaced:
        views.append(view)

    backup = path.with_suffix(".yaml.bak")
    backup.write_text(path.read_text())
    with open(path, "w") as f:
        yaml.dump(cfg, f, default_flow_style=False, sort_keys=False)

    try:
        await _load_and_build(config_path)
        return {"success": True, "message": f"View '{view['id']}' saved and reloaded"}
    except Exception as e:
        return {"success": False, "message": str(e)}


@router.delete("/views/{view_id}")
async def delete_view(view_id: str):
    """Delete a view from the config and reload."""
    from provisa.api.app import _load_and_build

    config_path = os.environ.get("PROVISA_CONFIG", "config/provisa.yaml")
    path = Path(config_path)
    with open(path) as f:
        cfg = yaml.safe_load(f)

    views = cfg.get("views", [])
    cfg["views"] = [v for v in views if v["id"] != view_id]

    backup = path.with_suffix(".yaml.bak")
    backup.write_text(path.read_text())
    with open(path, "w") as f:
        yaml.dump(cfg, f, default_flow_style=False, sort_keys=False)

    try:
        await _load_and_build(config_path)
        return {"success": True, "message": f"View '{view_id}' deleted"}
    except Exception as e:
        return {"success": False, "message": str(e)}


@router.post("/views/{view_id}/sample")
async def sample_view(view_id: str):
    """Execute a view's SQL with LIMIT 20 and return sample rows."""
    from provisa.api.app import state

    config_path = os.environ.get("PROVISA_CONFIG", "config/provisa.yaml")
    path = Path(config_path)
    with open(path) as f:
        cfg = yaml.safe_load(f)

    view = None
    for v in cfg.get("views", []):
        if v["id"] == view_id:
            view = v
            break
    if not view:
        raise HTTPException(status_code=404, detail=f"View '{view_id}' not found")

    sql = view["sql"].strip().rstrip(";")
    sample_sql = f"SELECT * FROM ({sql}) _v LIMIT 20"

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
