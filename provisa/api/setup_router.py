# Copyright (c) 2026 Kenneth Stott
# Canary: 50865373-50cf-4940-a44d-88cd5817510a
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
import secrets

import bcrypt
from fastapi import APIRouter
from pydantic import BaseModel
from sqlalchemy import func, insert, select

from provisa.api.errors import ApiError
from provisa.auth.scram_store import write_verifier
from provisa.core.schema_admin import local_users

from provisa.core.demo import is_demo as _is_demo

router = APIRouter(prefix="/setup", tags=["setup"])


def _idp_override() -> str | None:
    v = os.environ.get("PROVISA_IDP", "").strip()
    return v if v else None


async def _auto_configure_idp(provider: str, pool) -> None:
    """Write auth config for provider from env vars — no wizard required."""
    import uuid
    from provisa.api.admin._config_io import config_path, read_config_for_setup, write_config

    cfg_path = config_path()
    cfg = read_config_for_setup()
    if "auth" in cfg and cfg["auth"].get("provider") not in (None, "none"):
        # REQ-125: the provider is settled, but the break-glass account may not be — a
        # deployment configured before the operator supplied the two vars keeps an auth block
        # with no superuser, and nothing else ever writes one. Reconcile that one key and
        # leave the rest of the configured block exactly as it stands.
        if (
            "superuser" not in cfg["auth"]
            and os.environ.get("PROVISA_SUPERUSER_USERNAME")
            and os.environ.get("PROVISA_SUPERUSER_PASSWORD")
        ):
            cfg["auth"]["superuser"] = {
                "username": "${env:PROVISA_SUPERUSER_USERNAME}",
                "password": "${env:PROVISA_SUPERUSER_PASSWORD}",
            }
            write_config(cfg_path, cfg)
            from provisa.api.app import _load_and_build

            await _load_and_build(str(cfg_path))
        return  # already configured

    auth_section: dict = {
        "provider": provider,
        "assignments_source": "provisa",
    }

    # REQ-125: break-glass superuser. Written whenever the deployment supplies both vars, for any
    # provider — an operator locked out of a hosted IdP still has to reach the deployment. Stored as
    # ${env:...} placeholders so the secret stays in the systemd env file, never in the config.
    if os.environ.get("PROVISA_SUPERUSER_USERNAME") and os.environ.get(
        "PROVISA_SUPERUSER_PASSWORD"
    ):
        auth_section["superuser"] = {
            "username": "${env:PROVISA_SUPERUSER_USERNAME}",
            "password": "${env:PROVISA_SUPERUSER_PASSWORD}",
        }

    # PROVISA_MULTITENANCY promotes the deployment from single-admin bootstrap to multitenant
    # onboarding: the first authenticated user still claims the platform superadmin slot, but later
    # identities are admitted (not denied) and join an org by redeeming an invite. Off by default so
    # desktop/native firebase stays the single-administrator REQ-1266 mode.
    multitenant = os.environ.get("PROVISA_MULTITENANCY", "").lower() in ("1", "true", "yes")

    if provider == "firebase":
        # REQ-1266: limited Firebase mode — the first authenticated user becomes the sole
        # super-admin and every later user is denied. No blanket admin default_assignments
        # (that would make every Firebase user admin); the bootstrap gate grants the first.
        # With PROVISA_MULTITENANCY the bootstrap gate keeps granting the first superadmin but stops
        # denying later users (middleware), enabling the invite-based multitenant onboarding flow.
        auth_section["bootstrap_superadmin"] = True
        auth_section["default_assignments"] = []
        project_id = os.environ.get("FIREBASE_PROJECT_ID", "")
        if project_id:
            auth_section["firebase"] = {
                "project_id": project_id,
                "service_account_key": "${env:FIREBASE_SERVICE_ACCOUNT_KEY:-}",
            }
    else:
        # basic and every other IdP: the configured principal is the admin.
        auth_section["default_assignments"] = [{"role_id": "admin", "domain_id": "*"}]

    if provider == "basic":
        # REQ-124: same signing key the wizard writes — PROVISA_IDP=basic skips the wizard
        # entirely, and without a secret the browser's /auth/login answers 503.
        auth_section["jwt_secret"] = secrets.token_urlsafe(48)

    if provider == "basic" and pool:
        async with pool.acquire() as conn:
            count_result = await conn.execute_core(select(func.count()).select_from(local_users))
            count = count_result.scalar()
            if count == 0:
                pw_hash = bcrypt.hashpw(b"admin", bcrypt.gensalt()).decode("utf-8")
                admin_id = str(uuid.uuid4())
                await conn.upsert(
                    local_users,
                    {
                        "id": admin_id,
                        "username": "admin",
                        "password_hash": pw_hash,
                        "display_name": "Admin",
                        "is_active": True,
                    },
                    index_elements=["username"],
                    update_columns=[],
                )
                # REQ-1394: the bootstrap password exists in plaintext only here, so the SCRAM
                # verifier is derived alongside the bcrypt hash. Without it the seeded admin could
                # not negotiate SASL over pgwire until the password was changed.
                await write_verifier(pool, admin_id, "admin", "admin")

    cfg["auth"] = auth_section
    # multitenancy is a top-level config field (models.py), not part of the auth section.
    if multitenant:
        cfg["multitenancy"] = True
    write_config(cfg_path, cfg)
    from provisa.api.app import _load_and_build

    await _load_and_build(str(cfg_path))


def _auth_enabled(auth_cfg) -> bool:
    # auth is enforced when a real provider is configured. The SPA's login gate keys off
    # this runtime flag rather than a build-time VITE_AUTH_ENABLED, so a single image serves
    # both unsecured (demo/none) and firebase/basic deploys without a rebuild (REQ-1267).
    provider = auth_cfg.get("provider") if isinstance(auth_cfg, dict) else None
    return bool(provider and provider != "none")


@router.get("/status")
async def setup_status():  # REQ-539
    from provisa.api.app import state
    from provisa.api.admin._config_io import read_config

    idp = _idp_override()
    # Tenancy mode chosen at setup: the SPA hides org-lifecycle affordances (e.g.
    # Delete Organization) on single-tenant deploys.
    multitenancy = bool(read_config().get("multitenancy"))

    # local_users lives in the platform control plane.
    if _is_demo():
        if idp and state.admin_db:
            await _auto_configure_idp(idp, state.admin_db)
            return {
                "needs_setup": False,
                "demo_mode": True,
                "auth_enabled": True,
                "multitenancy": multitenancy,
            }
        cfg = read_config()
        auth_cfg = cfg.get("auth")
        if not auth_cfg:
            return {
                "needs_setup": True,
                "demo_mode": True,
                "auth_enabled": False,
                "multitenancy": multitenancy,
            }
        return {
            "needs_setup": False,
            "demo_mode": True,
            "auth_enabled": _auth_enabled(auth_cfg),
            "multitenancy": multitenancy,
        }

    if idp and state.admin_db:
        await _auto_configure_idp(idp, state.admin_db)
        return {
            "needs_setup": False,
            "demo_mode": False,
            "auth_enabled": True,
            "multitenancy": multitenancy,
        }

    cfg = read_config()
    auth_cfg = cfg.get("auth")
    if not auth_cfg:
        return {
            "needs_setup": True,
            "demo_mode": False,
            "auth_enabled": False,
            "multitenancy": multitenancy,
        }

    provider = auth_cfg.get("provider") if isinstance(auth_cfg, dict) else None
    if provider == "basic" and state.admin_db:
        async with state.admin_db.acquire() as conn:
            count_result = await conn.execute_core(select(func.count()).select_from(local_users))
            count = count_result.scalar()
        if count == 0:
            return {
                "needs_setup": True,
                "demo_mode": False,
                "auth_enabled": True,
                "multitenancy": multitenancy,
            }

    return {
        "needs_setup": False,
        "demo_mode": False,
        "auth_enabled": _auth_enabled(auth_cfg),
        "multitenancy": multitenancy,
    }


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
    from provisa.api.admin._config_io import config_path, read_config_for_setup, write_config

    if body.provider not in ("basic", "firebase", "none"):
        raise ApiError(
            400, "setup.invalid_provider", "provider must be 'basic', 'firebase', or 'none'"
        )
    if body.mode not in ("single", "multi"):
        raise ApiError(400, "setup.invalid_mode", "mode must be 'single' or 'multi'")
    if body.use_domains not in (None, True, False):
        raise ApiError(
            400, "setup.invalid_use_domains", "use_domains must be true, false, or null"
        )
    if body.use_domains is False and not body.default_domain:
        raise ApiError(
            400,
            "setup.default_domain_required",
            "default_domain required when use_domains=false",
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
        cfg = read_config_for_setup()
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
            raise ApiError(
                400,
                "setup.admin_credentials_required",
                "admin_username and admin_password required",
            )
        pw_hash = bcrypt.hashpw(body.admin_password.encode("utf-8"), bcrypt.gensalt()).decode(
            "utf-8"
        )
        admin_db = state.admin_db
        assert admin_db is not None
        admin_id = str(uuid.uuid4())
        async with admin_db.acquire() as conn:
            existing_result = await conn.execute_core(
                select(local_users.c.id).where(local_users.c.username == body.admin_username)
            )
            if existing_result.fetchone() is not None:
                raise ApiError(409, "auth.username_exists", "Username already exists")
            await conn.execute_core(
                insert(local_users).values(
                    id=admin_id,
                    username=body.admin_username,
                    password_hash=pw_hash,
                    display_name="Admin",
                    is_active=True,
                )
            )
        # REQ-1394: the wizard is one of the moments a plaintext password exists, so the account
        # setup creates can negotiate SCRAM over pgwire without first changing its password.
        await write_verifier(admin_db, admin_id, body.admin_username, body.admin_password)
        # REQ-124: the browser exchanges its password for a signed session token at
        # /auth/login, so the basic provider needs a signing key from the moment setup
        # writes the config — otherwise the first sign-in from the UI answers 503.
        auth_section["jwt_secret"] = secrets.token_urlsafe(48)

    elif body.provider == "firebase":
        project_id = body.firebase_project_id or os.environ.get("FIREBASE_PROJECT_ID", "")
        if not project_id:
            raise ApiError(400, "setup.firebase_project_id_required", "firebase_project_id required")
        # REQ-1266: limited Firebase mode — first user → sole super-admin, rest denied.
        # Drop the blanket admin default (it would admit every Firebase user).
        auth_section["bootstrap_superadmin"] = True
        auth_section["default_assignments"] = []
        auth_section["firebase"] = {
            "project_id": project_id,
            "service_account_key": "${env:FIREBASE_SERVICE_ACCOUNT_KEY:-}",
        }

    cfg_path = config_path()
    cfg = read_config_for_setup()
    cfg["auth"] = auth_section
    _apply_naming(cfg)
    write_config(cfg_path, cfg)
    await _load_and_build(str(cfg_path))

    return {"success": True, "provider": body.provider}
