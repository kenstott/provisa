# Copyright (c) 2026 Kenneth Stott
# Canary: 2b8e5c31-9d47-4a06-b7f2-1e83c0a95d64
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1412: the ORG's federation-engine lane — shared, isolated, or external.

Distinct from ``/admin/federation-engine`` (settings_router), which selects the engine KIND for the
whole deployment and is gated on ``platform_settings``. This surface never changes the engine kind;
it decides which coordinator the acting org's queries land on, so it is gated on ``org_settings``
and an org administrator holds it in either tenancy mode.

The three modes are derived from the ``orgs`` row, not stored a second time:

    external  external_engine_host is set — a coordinator the ORG operates
    isolated  isolated_engine is true     — a coordinator SaaS runs for this org alone
    shared    neither                     — the pooled lane every org starts on (REQ-1243)
"""

from __future__ import annotations

import logging
import os

import httpx
from fastapi import APIRouter, Request
from pydantic import BaseModel
from sqlalchemy import select, update

from provisa.api.admin._guards import require_active_org_id
from provisa.api.admin._platform_guard import require_org_settings
from provisa.api.errors import ApiError
from provisa.core.schema_admin import orgs

log = logging.getLogger(__name__)

router = APIRouter(tags=["admin"])

SHARED = "shared"
ISOLATED = "isolated"
EXTERNAL = "external"
MODES = (SHARED, ISOLATED, EXTERNAL)


class OrgEngineBody(BaseModel):
    mode: str
    external_host: str | None = None
    external_port: int | None = None


def _admin_pool():
    from provisa.api.app import state

    assert state.admin_db is not None
    return state.admin_db


def _mode_of(host: str | None, isolated: bool) -> str:
    if host:
        return EXTERNAL
    return ISOLATED if isolated else SHARED


def _isolated_available() -> bool:
    """Whether this deployment can resolve a dedicated coordinator for an org.

    ``isolated_engine_endpoint`` raises without ``PROVISA_ISOLATED_ENGINE_HOST_TEMPLATE`` rather
    than routing to the shared engine, so offering the mode where it is unset would sell isolation
    the deployment cannot deliver. The tab disables the option instead.
    """
    return bool(os.environ.get("PROVISA_ISOLATED_ENGINE_HOST_TEMPLATE"))


def _isolated_provisioned_here() -> bool:
    """Whether SaaS itself creates the coordinator (REQ-1416), as opposed to a deployment whose
    per-org clusters are stood up out of band and only RESOLVED here."""
    from provisa.federation.isolated_provisioner import provisioning_available

    return provisioning_available()


async def _isolated_engine_status(org_id: str) -> dict | None:
    """The dedicated coordinator's container state, or None where SaaS does not create it."""
    if not _isolated_provisioned_here():
        return None
    from provisa.federation.isolated_provisioner import engine_status

    return await engine_status(org_id)


async def _read_row(org_id: str):
    async with _admin_pool().acquire() as conn:
        result = await conn.execute_core(
            select(
                orgs.c.isolated_engine,
                orgs.c.external_engine_host,
                orgs.c.external_engine_port,
            ).where(orgs.c.id == org_id)
        )
        row = result.fetchone()
    if row is None:
        raise ApiError(404, "org_engine.org_not_found", f"org {org_id!r} not found", org=org_id)
    return row


@router.get("/admin/org-engine")
async def get_org_engine(request: Request):  # REQ-1412
    """The acting org's engine lane, plus which lanes this deployment can actually offer."""
    require_org_settings(request)
    from provisa.api.app import state

    org_id = require_active_org_id(request)
    row = await _read_row(org_id)
    return {
        "org_id": org_id,
        "mode": _mode_of(row[1], bool(row[0])),
        "external_host": row[1],
        "external_port": row[2],
        "isolated_available": _isolated_available(),
        # REQ-1416: whether moving onto the isolated lane also CREATES the coordinator here.
        "isolated_provisioned_here": _isolated_provisioned_here(),
        "isolated_engine": await _isolated_engine_status(org_id),
        # The engine KIND is the deployment's choice (platform_settings). Reported read-only so the
        # org administrator knows what an external coordinator has to be — a Trino lane cannot be
        # pointed at something that does not speak Trino.
        "engine_name": state.federation_engine.name,
    }


@router.put("/admin/org-engine")
async def set_org_engine(request: Request, body: OrgEngineBody):  # REQ-1412
    """Move the acting org onto a different engine lane and rebuild its runtime immediately."""
    require_org_settings(request)
    from provisa.api.app import build_org_runtime, state

    org_id = require_active_org_id(request)
    mode = body.mode
    if mode not in MODES:
        raise ApiError(
            400,
            "org_engine.unknown_mode",
            f"unknown mode {mode!r}; valid: {list(MODES)}",
            mode=str(mode),
            valid=list(MODES),
        )
    if mode == ISOLATED and not _isolated_available():
        raise ApiError(
            503,
            "org_engine.isolated_unavailable",
            "this deployment has no PROVISA_ISOLATED_ENGINE_HOST_TEMPLATE, so it cannot resolve a "
            "dedicated coordinator for an org (REQ-1043)",
        )
    host: str | None = None
    port: int | None = None
    if mode == EXTERNAL:
        host = (body.external_host or "").strip()
        port = body.external_port
        if not host or not port:
            raise ApiError(
                400,
                "org_engine.external_endpoint_required",
                "external mode requires both external_host and external_port",
            )

    async with _admin_pool().acquire() as conn:
        await conn.execute_core(
            update(orgs)
            .where(orgs.c.id == org_id)
            .values(
                isolated_engine=(mode == ISOLATED),
                external_engine_host=host,
                external_engine_port=port,
            )
        )

    # REQ-1416: the coordinator has to EXIST before the runtime binds a terminal at its hostname —
    # a rebuild against a name nothing answers leaves the org on a lane that cannot run a query.
    # Ordering matters the other way too: the previous lane's coordinator is removed only after
    # the org has been moved off it.
    if _isolated_provisioned_here():
        from provisa.federation.isolated_provisioner import (
            IsolatedProvisioningError,
            docker_error_detail,
            provision_isolated_engine,
        )

        if mode == ISOLATED:
            try:
                await provision_isolated_engine(org_id)
            except (IsolatedProvisioningError, httpx.HTTPError) as exc:
                raise ApiError(
                    502,
                    "org_engine.provisioning_failed",
                    f"could not provision a dedicated coordinator for {org_id!r}: "
                    f"{docker_error_detail(exc)}",
                    org=org_id,
                ) from exc

    # The lane is bound when the runtime is built (bind_terminal stores the terminal's kwargs), so
    # the change only reaches queries once the runtime is rebuilt. Rebuild under the registry's
    # per-org lock — REQ-1322 — rather than invalidating and letting the next request race.
    from provisa.api.app import _read_org_flags

    seeded_demo = (await _read_org_flags(org_id))[0]
    await state.org_registry.rebuild(
        org_id,
        lambda oid: build_org_runtime(
            oid,
            include_demo=seeded_demo,
            isolated_engine=(mode == ISOLATED),
            external_engine=(host, port) if mode == EXTERNAL and host and port else None,
        ),
    )
    if _isolated_provisioned_here() and mode != ISOLATED:
        # Off the isolated lane, the dedicated coordinator is dead weight — it holds no data (the
        # org's catalogs are issued from its config on every runtime build), so it is removed
        # rather than left running against the org's memory budget. Removal happens AFTER the
        # rebuild moved traffic off it.
        from provisa.federation.isolated_provisioner import deprovision_isolated_engine

        await deprovision_isolated_engine(org_id)

    log.info("org %s moved to %s federation-engine lane", org_id, mode)
    return {"success": True, "mode": mode, "external_host": host, "external_port": port}
