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

    external  an endpoint or a DSN is set — an engine the ORG operates
    isolated  isolated_engine is true     — a coordinator SaaS runs for this org alone
    shared    neither                     — the pooled lane every org starts on (REQ-1243)

REQ-1418: an external engine is not required to be the deployment's kind. The org picks any kind in
the registry that is externally addressable — Trino, Databricks, Snowflake, BigQuery, ClickHouse,
Fabric, Synapse, or any SQLAlchemy URL — and supplies whichever of endpoint/DSN that kind is
addressed by. Which one is required comes from ``engine_addressing(kind)``, never from inspecting
what was typed.
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
    # REQ-1418: which engine kind the org's own engine IS. ``None`` keeps the deployment's kind —
    # the state of an org that only moved its coordinator, and of every non-external org.
    engine_kind: str | None = None
    external_host: str | None = None
    external_port: int | None = None
    # The DSN for a URL-addressed kind. Write-only: the GET reports whether one is set, never its
    # value, because it carries the org's warehouse token.
    external_url: str | None = None


def _admin_pool():
    from provisa.api.app import state

    assert state.admin_db is not None
    return state.admin_db


def _mode_of(host: str | None, url_set: bool, isolated: bool) -> str:
    # REQ-1418: either address puts the org on the external lane — a DSN-addressed engine
    # (databricks://, snowflake://, …) has no host to look at.
    if host or url_set:
        return EXTERNAL
    return ISOLATED if isolated else SHARED


def _external_addressing(kind: str) -> str:
    """How an ORG-operated engine of ``kind`` is addressed.

    Identical to ``engine_addressing`` except for the bundled ``trino`` kind, which carries no
    address in the registry because the DEPLOYMENT manages its coordinator's lifecycle. When an org
    operates that coordinator instead, it is reached exactly as ``trino-byo`` is — host and port —
    which is the state of every org that moved its endpoint before kinds were selectable.
    """
    from provisa.federation.engine import engine_addressing

    return "endpoint" if kind == "trino" else engine_addressing(kind)


# Kinds that run INSIDE the Provisa process. They carry a `federation_engine_url` (a local data
# directory), so addressing alone does not exclude them from the org-operated list.
_IN_PROCESS_KINDS = frozenset({"clickhouse"})


def _external_kinds() -> list[dict]:
    """The engine kinds an org can run ITSELF: every registry kind that is externally addressable.

    Excludes the kinds with no address of their own — the deployment-managed bundled Trino and
    in-process DuckDB, neither of which an org can point at from outside the process — and the
    in-process kinds whose ``federation_engine_url`` is a LOCAL path rather than a network address:
    embedded ClickHouse (chdb) links into the Provisa process and its "URL" is a data directory on
    the deployment's disk, so an org cannot operate one. Its external counterpart is the
    ``clickhouse-server`` kind.
    """
    from provisa.federation.engine import ENGINE_REGISTRY, engine_addressing

    return [
        {
            "key": e["key"],
            "label": e["label"],
            "description": e["description"],
            "addressing": engine_addressing(e["key"]),
        }
        for e in ENGINE_REGISTRY
        if engine_addressing(e["key"]) != "none" and e["key"] not in _IN_PROCESS_KINDS
    ]


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
                orgs.c.engine_kind,
                orgs.c.engine_url_enc,
            ).where(orgs.c.id == org_id)
        )
        row = result.fetchone()
    if row is None:
        raise ApiError(404, "org_engine.org_not_found", f"org {org_id!r} not found", org=org_id)
    return row


async def _stored_engine_url(org_id: str) -> str | None:
    """REQ-1418: the DSN already on file for ``org_id``, decrypted, or ``None`` if it has none."""
    row = await _read_row(org_id)
    if row[4] is None:
        return None
    from provisa.encryption.runtime import encryption_service

    return encryption_service().decrypt(bytes(row[4])).decode("utf-8")


@router.get("/admin/org-engine")
async def get_org_engine(request: Request):  # REQ-1412
    """The acting org's engine lane, plus which lanes this deployment can actually offer."""
    require_org_settings(request)
    from provisa.api.app import state

    from provisa.core import commerce

    org_id = require_active_org_id(request)
    row = await _read_row(org_id)
    return {
        "org_id": org_id,
        "mode": _mode_of(row[1], row[4] is not None, bool(row[0])),
        "external_host": row[1],
        "external_port": row[2],
        # REQ-1418: the org's own kind (``None`` = the deployment's), whether a DSN is on file, and
        # the kinds an org may choose. The DSN itself is never returned — it carries a warehouse
        # token, so the tab shows "set / not set" and re-entry replaces it.
        "engine_kind": row[3],
        "external_url_set": row[4] is not None,
        "external_kinds": _external_kinds(),
        "isolated_available": _isolated_available(),
        # REQ-1412: whether the org's PLAN includes a coordinator of its own, as distinct from
        # whether the deployment can resolve one. The tab disables the lane it cannot buy instead of
        # letting the save fail with a 402.
        "isolated_entitled": await commerce.lane_entitled(state, org_id, ISOLATED),
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
    # REQ-1412: the isolated lane is platform-run compute, so on a hosted deployment it is part of
    # a plan. Checked before anything is written or provisioned.
    from provisa.core import commerce

    await commerce.require_lane_entitlement(state, org_id, mode)
    host: str | None = None
    port: int | None = None
    kind: str | None = None
    url: str | None = None
    if mode == EXTERNAL:
        from provisa.federation.engine import _ENGINE_BUILDERS

        # REQ-1418: an omitted kind means the deployment's kind — the org moved its coordinator, not
        # its engine. ``selected_key`` is what build_engine resolved for this process, so the
        # addressing rule below is the same one that will be applied when the runtime is built.
        kind = body.engine_kind
        effective_kind = kind or state.federation_engine.selected_key
        if effective_kind not in _ENGINE_BUILDERS:
            raise ApiError(
                400,
                "org_engine.unknown_engine_kind",
                f"unknown engine kind {effective_kind!r}; valid: {sorted(_ENGINE_BUILDERS)}",
                engine_kind=str(effective_kind),
                valid=sorted(_ENGINE_BUILDERS),
            )
        addressing = _external_addressing(effective_kind)
        if addressing == "url":
            url = (body.external_url or "").strip() or None
            if url is None:
                # The GET never returns the stored DSN (it carries a token), so a tab that changed
                # only the kind or the lane sends none back. Omitting it means UNCHANGED — the DSN
                # already on file is reused. There is no DSN to reuse on a first move to this lane,
                # which is the error below.
                url = await _stored_engine_url(org_id)
            if url is None:
                raise ApiError(
                    400,
                    "org_engine.external_url_required",
                    f"engine kind {effective_kind!r} is addressed by a URL; external_url is required",
                    engine_kind=effective_kind,
                )
        elif addressing == "endpoint":
            host = (body.external_host or "").strip()
            port = body.external_port
            if not host or not port:
                raise ApiError(
                    400,
                    "org_engine.external_endpoint_required",
                    f"engine kind {effective_kind!r} is addressed by a coordinator endpoint; both "
                    "external_host and external_port are required",
                    engine_kind=effective_kind,
                )
        else:
            raise ApiError(
                400,
                "org_engine.engine_kind_not_external",
                f"engine kind {effective_kind!r} has no external address — it is managed by the "
                "deployment or runs in-process, so an org cannot operate one of its own",
                engine_kind=effective_kind,
            )

    # REQ-1418: the DSN is stored encrypted with the same process-wide service as api_sources.auth
    # (REQ-686), because it carries the org's warehouse token.
    url_enc: bytes | None = None
    if url is not None:
        from provisa.encryption.runtime import encryption_service

        url_enc = encryption_service().encrypt(url.encode("utf-8"))

    async with _admin_pool().acquire() as conn:
        await conn.execute_core(
            update(orgs)
            .where(orgs.c.id == org_id)
            .values(
                isolated_engine=(mode == ISOLATED),
                external_engine_host=host,
                external_engine_port=port,
                engine_kind=kind,
                engine_url_enc=url_enc,
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

    lane = await _read_org_flags(org_id)
    await state.org_registry.rebuild(
        org_id,
        lambda oid: build_org_runtime(
            oid,
            include_demo=lane.seeded_demo,
            # REQ-1450: an org keeps its shard placement across a lane change — moving to the
            # isolated or external lane does not release the shared shard row, and moving back has
            # to land on the same one.
            shard=lane.shard,
            isolated_engine=(mode == ISOLATED),
            external_engine=(host, port) if mode == EXTERNAL and host and port else None,
            engine_kind=kind if mode == EXTERNAL else None,
            engine_url=url if mode == EXTERNAL else None,
        ),
    )
    if _isolated_provisioned_here() and mode != ISOLATED:
        # Off the isolated lane, the dedicated coordinator is dead weight — it holds no data (the
        # org's catalogs are issued from its config on every runtime build), so it is removed
        # rather than left running against the org's memory budget. Removal happens AFTER the
        # rebuild moved traffic off it.
        from provisa.federation.isolated_provisioner import deprovision_isolated_engine

        await deprovision_isolated_engine(org_id)

    log.info("org %s moved to %s federation-engine lane (kind=%s)", org_id, mode, kind or "-")
    return {
        "success": True,
        "mode": mode,
        "engine_kind": kind,
        "external_host": host,
        "external_port": port,
        "external_url_set": url is not None,
    }
