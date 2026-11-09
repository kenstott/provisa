# Copyright (c) 2026 Kenneth Stott
# Canary: 6b1e47f8-c293-4e0a-a5d7-3f809c1d62be
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Adding a dbt source, and giving it a new manifest (REQ-2228).

The source's spec is dbt's manifest: from a URL or artifact location, an upload, or dbt Cloud's API
through the source's token. Adding one registers nothing, as with an OpenAPI source (REQ-316): its
models are tables on offer and what dbt can run are commands on offer.
"""

# Requirements: REQ-2228

from __future__ import annotations

import logging
from dataclasses import asdict
from typing import TYPE_CHECKING, Literal, cast

from fastapi import APIRouter, Request
from pydantic import BaseModel
from sqlalchemy import select

from provisa.api.admin.capabilities import require_capability_request
from provisa.api.admin.schema_common import remote_source_counts
from provisa.api.errors import ApiError
from provisa.core.schema_org import sources
from provisa.core.secrets_store import resolved_credential
from provisa.core.source_downloads import read_bytes
from provisa.dbt.cloud import NoCloudRun, latest_manifest
from provisa.dbt.commands import offered_commands
from provisa.dbt.manifest import DbtManifest, NotAManifest, parse_manifest
from provisa.dbt.store import (
    DBT,
    SPEC_CLOUD,
    SPEC_LOCATION,
    SPEC_UPLOAD,
    held_manifest,
    store_manifest,
    warehouse_rows,
)
from provisa.dbt.warehouse import warehouse_offers

if TYPE_CHECKING:
    from provisa.core.database import Connection

log = logging.getLogger(__name__)
router = APIRouter(prefix="/admin/dbt", tags=["admin", "dbt"])

_INLINE = ":inline:"


class DbtSpec(BaseModel):
    spec: Literal["location", "upload", "cloud"] = SPEC_LOCATION
    location: str = ""
    content: str = ""
    host: str = ""
    account_id: str = ""
    job_id: str = ""
    token: str = ""


class DbtRegisterRequest(DbtSpec):
    source_id: str
    description: str = ""
    command_line: str = "dbt"


async def _read_spec(body: DbtSpec, *, token: str = "") -> DbtManifest:
    """The manifest ``body`` points at; ``token`` is the held credential when none was entered."""
    try:
        if body.spec == SPEC_UPLOAD:
            return parse_manifest(body.content)
        if body.spec == SPEC_CLOUD:
            credential = await resolved_credential(body.token or token)
            return parse_manifest(
                await latest_manifest(body.host, body.account_id, body.job_id, credential)
            )
        return parse_manifest(await read_bytes(await resolved_credential(body.location)))
    except NotAManifest as exc:
        raise ApiError(422, "dbt.not_a_manifest", str(exc)) from exc
    except NoCloudRun as exc:
        raise ApiError(422, "dbt.no_cloud_run", str(exc), job_id=body.job_id) from exc
    except Exception as exc:
        raise ApiError(
            422, "dbt.manifest_unreadable", f"The manifest could not be read: {exc}"
        ) from exc


def _summary(manifest: DbtManifest) -> dict:
    return {
        "project": manifest.project,
        "adapter": manifest.adapter,
        "models": len(manifest.models),
        "commands": len(offered_commands(manifest)),
    }


@router.post("/preview")
async def preview_dbt_manifest(request: Request, body: DbtSpec):
    """What the manifest offers and which warehouse reads it. Nothing is persisted."""
    require_capability_request(request, "source_registration")
    from provisa.api.app import state

    manifest = await _read_spec(body)
    assert state.model_db is not None
    async with state.model_db.acquire() as conn:
        held = await warehouse_rows(conn)
    return {
        **_summary(manifest),
        "warehouses": [
            asdict(o) for o in warehouse_offers(manifest.adapter, manifest.databases, held)
        ],
    }


def _spec_mapping(body: DbtRegisterRequest) -> dict:
    return {
        "spec": body.spec,
        "command_line": body.command_line,
        "cloud": {"host": body.host, "account_id": body.account_id, "job_id": body.job_id},
    }


async def _save_source(conn: Connection, body: DbtRegisterRequest) -> None:
    from provisa.core.models import Source, SourceType
    from provisa.core.repositories import source as source_repo

    existing = await source_repo.get(conn, body.source_id)
    token = body.token or (existing["password_ref"] if existing is not None else "")
    path = body.location if body.spec == SPEC_LOCATION else _INLINE
    model = Source(
        id=body.source_id,
        type=SourceType.dbt,
        path=path,
        password=token,
        description=body.description,
        mapping=_spec_mapping(body),
    )
    if existing is not None:
        # A new spec replaces the spec, not the operator's other settings of the source.
        model = source_repo.source_from_row(existing).model_copy(
            update={"path": path, "password": token, "mapping": _spec_mapping(body)}
        )
    await source_repo.upsert(conn, model)


@router.post("/register")
async def register_dbt_source(request: Request, body: DbtRegisterRequest):
    """Add the source and hold its manifest. Nothing is registered until a steward does."""
    require_capability_request(request, "source_registration")
    from provisa.api.app import _rebuild_schemas, state

    if body.spec == SPEC_CLOUD and not body.token:
        assert state.model_db is not None
        async with state.model_db.acquire() as conn:
            held = (
                await conn.execute_core(
                    select(sources.c.password_ref).where(sources.c.id == body.source_id)
                )
            ).fetchone()
        if held is None or not held.password_ref:
            raise ApiError(422, "dbt.token_required", "dbt Cloud needs an API token")
        manifest = await _read_spec(body, token=held.password_ref)
    else:
        manifest = await _read_spec(body)
    assert state.model_db is not None
    async with state.model_db.acquire() as conn:
        await _save_source(cast("Connection", conn), body)
        await store_manifest(conn, body.source_id, manifest)
    await _rebuild_schemas()
    log.info("Added dbt source %s (%d models on offer)", body.source_id, len(manifest.models))
    return {
        "source_id": body.source_id,
        **_summary(manifest),
        **remote_source_counts(0, len(manifest.models), len(offered_commands(manifest))),
    }


class DbtRefreshRequest(BaseModel):
    content: str = ""


@router.post("/refresh/{source_id}")
async def refresh_dbt_source(request: Request, source_id: str, body: DbtRefreshRequest):
    """A new manifest: read again from its spec, or given in place of an upload. What is on offer,
    the lineage and the registered models' descriptions follow from it; nothing is registered."""
    require_capability_request(request, "source_registration")
    from provisa.api.app import _rebuild_schemas, state

    assert state.model_db is not None
    async with state.model_db.acquire() as conn:
        row = (
            await conn.execute_core(
                select(
                    sources.c.type, sources.c.path, sources.c.mapping, sources.c.password_ref
                ).where(sources.c.id == source_id)
            )
        ).fetchone()
    if row is None or row.type != DBT:
        raise ApiError(
            404,
            "dbt.source_not_registered",
            f"dbt source {source_id!r} not registered",
            source_id=source_id,
        )
    mapping = row.mapping or {}
    cloud = mapping.get("cloud") or {}
    spec = DbtSpec(
        spec=mapping.get("spec", SPEC_LOCATION),
        location=row.path if mapping.get("spec") == SPEC_LOCATION else "",
        content=body.content,
        host=cloud.get("host", ""),
        account_id=cloud.get("account_id", ""),
        job_id=cloud.get("job_id", ""),
    )
    if spec.spec == SPEC_UPLOAD and not body.content:
        raise ApiError(
            422,
            "dbt.upload_required",
            "A dbt source given by upload is refreshed with a new manifest",
        )
    manifest = await _read_spec(spec, token=row.password_ref or "")
    async with state.model_db.acquire() as conn:
        await store_manifest(conn, source_id, manifest)
    await _rebuild_schemas()
    return {"source_id": source_id, **_summary(manifest)}


@router.get("/list")
async def list_dbt_sources(request: Request):
    """Each dbt source's setup, without its token, which never leaves the server."""
    require_capability_request(request, "source_registration")
    from provisa.api.app import state

    assert state.model_db is not None
    out = []
    async with state.model_db.acquire() as conn:
        rows = (
            await conn.execute_core(
                select(
                    sources.c.id, sources.c.path, sources.c.mapping, sources.c.password_ref
                ).where(sources.c.type == DBT)
            )
        ).fetchall()
        for row in rows:
            manifest = await held_manifest(conn, row.id)
            mapping = row.mapping or {}
            out.append(
                {
                    "source_id": row.id,
                    "spec": mapping.get("spec", SPEC_LOCATION),
                    "location": "" if row.path == _INLINE else (row.path or ""),
                    "command_line": mapping.get("command_line", ""),
                    "cloud": mapping.get("cloud") or {},
                    "has_token": bool(row.password_ref),
                    "summary": None if manifest is None else _summary(manifest),
                }
            )
    return out
