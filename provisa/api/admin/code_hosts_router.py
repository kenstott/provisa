# Copyright (c) 2026 Kenneth Stott
# Canary: 943840da-522f-4e3f-be32-b8d08e4f4f8c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A code source's image and remote host, as its form drives them (REQ-2098).

Under ``/admin/orgs/{org_id}/code-sources/{source_id}``:

- ``POST /builds`` ``{arch, registry_id, download}``: build the dev-mode source's image, pushed to
  a container registry (``registry_id``, else the default) or kept for download; ``GET /builds``
  and ``GET /builds/{id}`` say how each went, with the compose service to run it; ``GET
  /builds/{id}/archive`` downloads a kept one.
- ``POST /connect`` ``{host, port, pairing_code}``: pair with the remote host running it, check
  it serves the source's code, and switch the source to built mode. ``POST /dev`` switches it back.
- ``GET /exports``: the functions its code exports, each its name and kind (table, command,
  stream), which its form lists.
- ``GET /host``: the source's mode and whether its remote host answers. ``POST /repin``
  ``{code_hash}``: pin the source to the code its host now runs, once reviewed.
- ``POST /validate`` ``{script, inputs, keys}`` (REQ-2225): a Spark source's script -- the one in
  the form, or with none the one it serves -- validated by its host in three steps: the static
  check, a dry run of each function on a sample of the governed tables ``inputs`` names, read as
  the caller's role, and each resolver's two ``keys`` (provisa/codesource/script_validation.py).

Each is opened by ``source_registration`` held in THAT organisation, as the code uploads are.
"""

# Requirements: REQ-2098, REQ-2225
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Request
from fastapi.responses import FileResponse
from pydantic import BaseModel

from provisa.api.admin.code_uploads_router import _guard
from provisa.api.errors import ApiError
from provisa.codesource import builds, remote_sources
from provisa.codesource.bundle import BundleRefused
from provisa.core.container_registries import ContainerRegistryRefused

router = APIRouter(prefix="/admin/orgs/{org_id}/code-sources/{source_id}", tags=["admin"])


class BuildBody(BaseModel):
    arch: str = "amd64"
    registry_id: str | None = None
    download: bool = False


class ConnectBody(BaseModel):
    host: str
    port: int
    pairing_code: str


class RepinBody(BaseModel):
    code_hash: str


class ValidateBody(BaseModel):
    #: The script to validate; None validates the one the source serves.
    script: str | None = None
    #: Each function's inputs by parameter: a table argument's value names a governed table.
    inputs: dict[str, dict[str, Any]] = {}
    #: Each code resolver's two keys, its source key and its target key.
    keys: dict[str, tuple[str, str]] = {}


def _refusal(refused: BundleRefused | ContainerRegistryRefused) -> ApiError:
    status = getattr(refused, "status", 400)
    if refused.code.endswith(("not_found", "build_not_found")):
        status = 404
    return ApiError(status, refused.code, str(refused), None, **refused.params)


def _state():
    from provisa.api.app import state

    return state


@router.post("/builds")
async def build(request: Request, org_id: str, source_id: str, body: BuildBody) -> dict:
    actor = await _guard(request, org_id)
    try:
        return await builds.start(
            _state(), source_id, body.arch, body.registry_id, body.download, actor
        )
    except (BundleRefused, ContainerRegistryRefused) as refused:
        raise _refusal(refused) from None


@router.get("/builds")
async def list_builds(request: Request, org_id: str, source_id: str) -> dict:
    await _guard(request, org_id)
    return {"builds": await builds.listed(source_id)}


@router.get("/builds/{build_id}")
async def read_build(request: Request, org_id: str, source_id: str, build_id: str) -> dict:
    await _guard(request, org_id)
    try:
        return await builds.read(source_id, build_id)
    except BundleRefused as refused:
        raise _refusal(refused) from None


@router.get("/builds/{build_id}/archive")
async def download(request: Request, org_id: str, source_id: str, build_id: str) -> FileResponse:
    await _guard(request, org_id)
    try:
        path = await builds.archive_path(source_id, build_id)
    except BundleRefused as refused:
        raise _refusal(refused) from None
    return FileResponse(
        path, media_type="application/x-tar", filename=f"provisa-code-{source_id}.tar"
    )


@router.post("/connect")
async def connect(request: Request, org_id: str, source_id: str, body: ConnectBody) -> dict:
    await _guard(request, org_id)
    try:
        mapping = await remote_sources.connect(
            _state(), source_id, body.host, body.port, body.pairing_code
        )
    except BundleRefused as refused:
        raise _refusal(refused) from None
    # What the form keeps for the source from now on: an edit of it carries these through.
    return {
        "mode": mapping["mode"],
        "remote": mapping["remote"],
        "content_hash": mapping["content_hash"],
    }


@router.post("/dev")
async def to_dev(request: Request, org_id: str, source_id: str) -> dict:
    await _guard(request, org_id)
    try:
        await remote_sources.to_dev(_state(), source_id)
    except BundleRefused as refused:
        raise _refusal(refused) from None
    return {"mode": remote_sources.DEV}


@router.get("/host")
async def host(request: Request, org_id: str, source_id: str) -> dict:
    await _guard(request, org_id)
    try:
        return await remote_sources.status(_state(), source_id)
    except BundleRefused as refused:
        raise _refusal(refused) from None


@router.post("/repin")
async def repin(request: Request, org_id: str, source_id: str, body: RepinBody) -> dict:
    await _guard(request, org_id)
    try:
        mapping = await remote_sources.repin(_state(), source_id, body.code_hash)
    except BundleRefused as refused:
        raise _refusal(refused) from None
    return {"mode": mapping["mode"], "content_hash": mapping["content_hash"]}


@router.get("/exports")
async def exports(request: Request, org_id: str, source_id: str) -> dict:
    from provisa.codesource import discovery

    await _guard(request, org_id)
    try:
        found = await discovery.exports(_state(), source_id)
    except BundleRefused as refused:
        raise _refusal(refused) from None
    if found is None:
        missing = BundleRefused("not_found", f"There is no source {source_id!r}", source=source_id)
        raise _refusal(missing)
    return {"exports": [{"name": name, "kind": kind} for name, kind in found]}


@router.post("/validate")
async def validate(request: Request, org_id: str, source_id: str, body: ValidateBody) -> dict:
    from provisa.api.admin.capabilities import acting_role_of_request
    from provisa.codesource import script_validation

    await _guard(request, org_id)
    asked = script_validation.Asked(script=body.script, inputs=body.inputs, keys=body.keys)
    try:
        target = await script_validation.target_of(_state(), source_id)
        return await script_validation.validate(
            _state(), target, asked, acting_role_of_request(request)
        )
    except BundleRefused as refused:
        raise _refusal(refused) from None
