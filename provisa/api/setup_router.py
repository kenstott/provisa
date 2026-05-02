# Copyright (c) 2026 Kenneth Stott
# Canary: 3de609ff-6421-4f6e-9d77-5c7c93e20416
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""First-run setup wizard endpoints."""

from __future__ import annotations

import os

import bcrypt
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

router = APIRouter(prefix="/setup", tags=["setup"])


def _is_demo() -> bool:
    return os.environ.get("PROVISA_DEMO", "").lower() in ("1", "true", "yes")


def _idp_override() -> str | None:
    v = os.environ.get("PROVISA_IDP", "").strip()
    return v if v else None


async def _seed_demo_admin(pool) -> None:
    import uuid
    from provisa.api.admin._config_io import config_path, read_config, write_config

    async with pool.acquire() as conn:
        count = await conn.fetchval("SELECT COUNT(*) FROM local_users")
        if count == 0:
            pw_hash = bcrypt.hashpw(b"admin", bcrypt.gensalt()).decode("utf-8")
            await conn.execute(
                "INSERT INTO local_users (id, username, password_hash, display_name, is_active) "
                "VALUES ($1, 'admin', $2, 'Admin', true) ON CONFLICT DO NOTHING",
                str(uuid.uuid4()), pw_hash,
            )

    cfg_path = config_path()
    cfg = read_config()
    if "auth" not in cfg:
        cfg["auth"] = {
            "provider": "basic",
            "assignments_source": "provisa",
            "default_assignments": [{"role_id": "admin", "domain_id": "*"}],
        }
        write_config(cfg_path, cfg)
        from provisa.api.app import _load_and_build
        await _load_and_build(str(cfg_path))


async def _auto_configure_idp(provider: str, pool) -> None:
    """Write auth config for provider from env vars — no wizard required."""
    import uuid
    from provisa.api.admin._config_io import config_path, read_config, write_config

    cfg_path = config_path()
    cfg = read_config()
    if "auth" in cfg:
        return  # already configured

    auth_section: dict = {
        "provider": provider,
        "assignments_source": "provisa",
        "default_assignments": [{"role_id": "admin", "domain_id": "*"}],
    }

    if provider == "firebase":
        project_id = os.environ.get("FIREBASE_PROJECT_ID", "")
        if project_id:
            auth_section["firebase"] = {
                "project_id": project_id,
                "service_account_key": "${env:FIREBASE_SERVICE_ACCOUNT_KEY:-}",
            }
    elif provider == "basic" and pool:
        async with pool.acquire() as conn:
            count = await conn.fetchval("SELECT COUNT(*) FROM local_users")
            if count == 0:
                pw_hash = bcrypt.hashpw(b"admin", bcrypt.gensalt()).decode("utf-8")
                await conn.execute(
                    "INSERT INTO local_users (id, username, password_hash, display_name, is_active) "
                    "VALUES ($1, 'admin', $2, 'Admin', true) ON CONFLICT DO NOTHING",
                    str(uuid.uuid4()), pw_hash,
                )

    cfg["auth"] = auth_section
    write_config(cfg_path, cfg)
    from provisa.api.app import _load_and_build
    await _load_and_build(str(cfg_path))


@router.get("/status")
async def setup_status():
    from provisa.api.app import state
    from provisa.api.admin._config_io import read_config

    idp = _idp_override()

    if _is_demo():
        if state.pg_pool:
            if idp and idp != "basic":
                await _auto_configure_idp(idp, state.pg_pool)
            else:
                await _seed_demo_admin(state.pg_pool)
        return {"needs_setup": False, "demo_mode": True}

    if idp and state.pg_pool:
        await _auto_configure_idp(idp, state.pg_pool)
        return {"needs_setup": False, "demo_mode": False}

    cfg = read_config()
    auth_cfg = cfg.get("auth")
    if not auth_cfg:
        return {"needs_setup": True, "demo_mode": False}

    provider = auth_cfg.get("provider") if isinstance(auth_cfg, dict) else None
    if provider == "basic" and state.pg_pool:
        async with state.pg_pool.acquire() as conn:
            count = await conn.fetchval("SELECT COUNT(*) FROM local_users")
        if count == 0:
            return {"needs_setup": True, "demo_mode": False}

    return {"needs_setup": False, "demo_mode": False}


class SetupRequest(BaseModel):
    provider: str  # "basic" or "firebase"
    mode: str  # "single" or "multi"
    admin_username: str | None = None
    admin_password: str | None = None
    firebase_project_id: str | None = None


@router.post("/")
async def run_setup(body: SetupRequest):
    import uuid
    from provisa.api.app import state, _load_and_build
    from provisa.api.admin._config_io import config_path, read_config, write_config

    if body.provider not in ("basic", "firebase"):
        raise HTTPException(status_code=400, detail="provider must be 'basic' or 'firebase'")
    if body.mode not in ("single", "multi"):
        raise HTTPException(status_code=400, detail="mode must be 'single' or 'multi'")

    auth_section: dict = {
        "provider": body.provider,
        "assignments_source": "provisa",
        "default_assignments": [{"role_id": "admin", "domain_id": "*"}],
    }

    if body.provider == "basic":
        if not body.admin_username or not body.admin_password:
            raise HTTPException(status_code=400, detail="admin_username and admin_password required")
        pw_hash = bcrypt.hashpw(body.admin_password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")
        async with state.pg_pool.acquire() as conn:
            existing = await conn.fetchrow("SELECT id FROM local_users WHERE username = $1", body.admin_username)
            if existing:
                raise HTTPException(status_code=409, detail="Username already exists")
            await conn.execute(
                "INSERT INTO local_users (id, username, password_hash, display_name, is_active) "
                "VALUES ($1, $2, $3, 'Admin', true)",
                str(uuid.uuid4()), body.admin_username, pw_hash,
            )

    elif body.provider == "firebase":
        project_id = body.firebase_project_id or os.environ.get("FIREBASE_PROJECT_ID", "")
        if not project_id:
            raise HTTPException(status_code=400, detail="firebase_project_id required")
        auth_section["firebase"] = {
            "project_id": project_id,
            "service_account_key": "${env:FIREBASE_SERVICE_ACCOUNT_KEY:-}",
        }

    cfg_path = config_path()
    cfg = read_config()
    cfg["auth"] = auth_section
    write_config(cfg_path, cfg)
    await _load_and_build(str(cfg_path))

    return {"success": True, "provider": body.provider}
