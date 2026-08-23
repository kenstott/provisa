# Copyright (c) 2026 Kenneth Stott
# Canary: 6e0d24b7-1a53-4f8c-9b26-c7e5f0a3d914
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1046/1048/1049: the org's materialization storage — what it uses, and whose disk it is on.

The storage counterpart to ``org_engine_router``: that surface decides which coordinator answers
the org's queries, this one decides where the org's materialized bytes land. Gated on
``org_settings`` for the same reason — an org administrator owns their org's data plane.

GET reports the footprint against the allowance so the org can see the ceiling before it hits it,
and can act (drop something, upgrade, or register a bucket) rather than discovering the limit at
the moment a refresh is refused.

PUT registers a store the ORG owns (REQ-1048). From that point its materializations are written
there and its bytes stop being measured or capped here. The DSN is stored encrypted and is never
returned by the GET — it is a credential to a system the platform does not own, so the surface
reports only whether one is set and re-entry replaces it. Clearing it moves the org back onto the
platform store, where the allowance applies again.
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, Request
from pydantic import BaseModel
from sqlalchemy import select, update

from provisa.api.admin._guards import require_active_org_id
from provisa.api.admin._platform_guard import require_org_settings
from provisa.api.errors import ApiError
from provisa.core.schema_admin import orgs

log = logging.getLogger(__name__)

router = APIRouter(tags=["admin"])


class OrgStorageBody(BaseModel):
    # The org's own materialization store DSN. ``None`` clears it and returns the org to the
    # platform store; a value replaces whatever is on file (the existing one is never readable, so
    # there is nothing to edit in place).
    storage_url: str | None = None


def _admin_pool():
    from provisa.api.app import state

    assert state.admin_db is not None
    return state.admin_db


async def _storage_url_set(org_id: str) -> bool:
    async with _admin_pool().acquire() as conn:
        result = await conn.execute_core(select(orgs.c.storage_url_enc).where(orgs.c.id == org_id))
        row = result.fetchone()
    if row is None:
        raise ApiError(404, "org_storage.org_not_found", f"org {org_id!r} not found", org=org_id)
    return row[0] is not None


@router.get("/admin/org-storage")
async def get_org_storage(request: Request):  # REQ-1046, REQ-1049
    """The acting org's storage footprint, its allowance, and whether it brings its own store."""
    require_org_settings(request)
    from provisa.storage.quota import storage_report

    org_id = require_active_org_id(request)
    # Raises 404 for an unknown org before the report, which reads the store rather than the row.
    await _storage_url_set(org_id)
    return await storage_report(org_id)


@router.put("/admin/org-storage")
async def put_org_storage(body: OrgStorageBody, request: Request):  # REQ-1048
    """Register (or clear) the org's own materialization store."""
    require_org_settings(request)
    from provisa.api.app import _read_org_flags, build_org_runtime, state

    org_id = require_active_org_id(request)
    url = body.storage_url.strip() if body.storage_url else None

    if url is not None:
        # Rejected here rather than at the first landing: an unusable DSN accepted now surfaces as
        # a failed refresh hours later, with nothing pointing at the setting that caused it.
        from sqlalchemy.exc import ArgumentError

        from provisa.federation.store_writer import async_store_url

        try:
            async_store_url(url)
        except (ValueError, ArgumentError) as exc:
            raise ApiError(
                400,
                "org_storage.invalid_url",
                f"not a usable materialization store DSN: {exc}",
                org=org_id,
            ) from exc

    # REQ-1048: encrypted at rest with the same process-wide service as the org's engine DSN — it
    # carries the credentials to the org's own store.
    url_enc: bytes | None = None
    if url is not None:
        from provisa.encryption.runtime import encryption_service

        url_enc = encryption_service().encrypt(url.encode("utf-8"))

    async with _admin_pool().acquire() as conn:
        await conn.execute_core(
            update(orgs).where(orgs.c.id == org_id).values(storage_url_enc=url_enc)
        )

    # The store is resolved off the built runtime (materialize_store reads it there), so the change
    # reaches the write paths only once the runtime is rebuilt. Rebuilt under the registry's per-org
    # lock — REQ-1322 — rather than invalidated, so no request races the build.
    lane = await _read_org_flags(org_id)
    await state.org_registry.rebuild(
        org_id,
        lambda oid: build_org_runtime(
            oid,
            include_demo=lane.seeded_demo,
            isolated_engine=lane.isolated_engine,
            external_engine=lane.external_engine,
            engine_kind=lane.engine_kind,
            engine_url=lane.engine_url,
            shard=lane.shard,
            storage_url=lane.storage_url,
        ),
    )

    log.info("org %s materialization store %s", org_id, "registered" if url else "cleared")
    return {"success": True, "org_id": org_id, "storage_url_set": url is not None}
