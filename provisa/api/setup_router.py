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

# Requirements: REQ-120, REQ-121, REQ-124, REQ-125, REQ-471, REQ-472, REQ-539

from __future__ import annotations

import os

import bcrypt
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from sqlalchemy import func, insert, select

from provisa.core.schema_admin import local_users

router = APIRouter(prefix="/setup", tags=["setup"])


def _is_demo() -> bool:
    return os.environ.get("PROVISA_DEMO", "").lower() in ("1", "true", "yes")


def _idp_override() -> str | None:
    v = os.environ.get("PROVISA_IDP", "").strip()
    return v if v else None


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
            count_result = await conn.execute_core(select(func.count()).select_from(local_users))
            count = count_result.scalar()
            if count == 0:
                pw_hash = bcrypt.hashpw(b"admin", bcrypt.gensalt()).decode("utf-8")
                await conn.upsert(
                    local_users,
                    {
                        "id": str(uuid.uuid4()),
                        "username": "admin",
                        "password_hash": pw_hash,
                        "display_name": "Admin",
                        "is_active": True,
                    },
                    index_elements=["username"],
                    update_columns=[],
                )

    cfg["auth"] = auth_section
    write_config(cfg_path, cfg)
    from provisa.api.app import _load_and_build

    await _load_and_build(str(cfg_path))


@router.get("/status")
async def setup_status():  # REQ-539
    from provisa.api.app import state
    from provisa.api.admin._config_io import read_config

    idp = _idp_override()

    # local_users lives in the platform control plane.
    if _is_demo():
        if idp and state.admin_db:
            await _auto_configure_idp(idp, state.admin_db)
            return {"needs_setup": False, "demo_mode": True}
        cfg = read_config()
        auth_cfg = cfg.get("auth")
        if not auth_cfg:
            return {"needs_setup": True, "demo_mode": True}
        return {"needs_setup": False, "demo_mode": True}

    if idp and state.admin_db:
        await _auto_configure_idp(idp, state.admin_db)
        return {"needs_setup": False, "demo_mode": False}

    cfg = read_config()
    auth_cfg = cfg.get("auth")
    if not auth_cfg:
        return {"needs_setup": True, "demo_mode": False}

    provider = auth_cfg.get("provider") if isinstance(auth_cfg, dict) else None
    if provider == "basic" and state.admin_db:
        async with state.admin_db.acquire() as conn:
            count_result = await conn.execute_core(select(func.count()).select_from(local_users))
            count = count_result.scalar()
        if count == 0:
            return {"needs_setup": True, "demo_mode": False}

    return {"needs_setup": False, "demo_mode": False}


class SetupRequest(BaseModel):
    provider: str  # "basic" or "firebase"
    mode: str  # "single" or "multi"
    admin_username: str | None = None
    admin_password: str | None = None
    firebase_project_id: str | None = None
    # Domain policy decision, made once at install. None = legacy/inert (default),
    # False = single-domain (every table under default_domain), True = namespaced.
    use_domains: bool | None = None
    default_domain: str = "default"


@router.post("/")
async def run_setup(body: SetupRequest):  # REQ-120, REQ-121, REQ-124, REQ-125, REQ-471, REQ-472
    import uuid
    from provisa.api.app import state, _load_and_build
    from provisa.api.admin._config_io import config_path, read_config, write_config

    if body.provider not in ("basic", "firebase", "none"):
        raise HTTPException(
            status_code=400, detail="provider must be 'basic', 'firebase', or 'none'"
        )
    if body.mode not in ("single", "multi"):
        raise HTTPException(status_code=400, detail="mode must be 'single' or 'multi'")
    if body.use_domains not in (None, True, False):
        raise HTTPException(status_code=400, detail="use_domains must be true, false, or null")
    if body.use_domains is False and not body.default_domain:
        raise HTTPException(
            status_code=400, detail="default_domain required when use_domains=false"
        )

    def _apply_naming(cfg: dict) -> None:
        # Domain policy is an install-time decision. Only persist when explicitly chosen
        # (use_domains not None) so a legacy install leaves naming untouched.
        if body.use_domains is None:
            return
        naming = cfg.setdefault("naming", {})
        naming["use_domains"] = body.use_domains
        if body.use_domains is False:
            naming["default_domain"] = body.default_domain

    if body.provider == "none":
        cfg_path = config_path()
        cfg = read_config()
        cfg["auth"] = {"provider": "none"}
        _apply_naming(cfg)
        write_config(cfg_path, cfg)
        await _load_and_build(str(cfg_path))
        return {"success": True, "provider": "none"}

    auth_section: dict = {
        "provider": body.provider,
        "assignments_source": "provisa",
        "default_assignments": [{"role_id": "admin", "domain_id": "*"}],
    }

    if body.provider == "basic":
        if not body.admin_username or not body.admin_password:
            raise HTTPException(
                status_code=400, detail="admin_username and admin_password required"
            )
        pw_hash = bcrypt.hashpw(body.admin_password.encode("utf-8"), bcrypt.gensalt()).decode(
            "utf-8"
        )
        admin_db = state.admin_db
        assert admin_db is not None
        async with admin_db.acquire() as conn:
            existing_result = await conn.execute_core(
                select(local_users.c.id).where(local_users.c.username == body.admin_username)
            )
            if existing_result.fetchone() is not None:
                raise HTTPException(status_code=409, detail="Username already exists")
            await conn.execute_core(
                insert(local_users).values(
                    id=str(uuid.uuid4()),
                    username=body.admin_username,
                    password_hash=pw_hash,
                    display_name="Admin",
                    is_active=True,
                )
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
    _apply_naming(cfg)
    write_config(cfg_path, cfg)
    await _load_and_build(str(cfg_path))

    return {"success": True, "provider": body.provider}
