# Copyright (c) 2026 Kenneth Stott
# Canary: c7d7c0b8-3467-4317-a07b-cc2d53de4cf0
#
# This source code is licensed under the Business Source License 1.1
"""A file-based source's file, given by upload (REQ-1997): the request's body is the file,
streamed into the org's folder (``provisa.core.source_uploads``). Only the org's source
registrars may upload. A source that exists already is provisioned again, so its next read is of
the new file. A JDBC source's driver jar is given the same way (REQ-2191), where this server runs
a customer's driver at all: elsewhere it is refused by name, as a save of it would be."""

# Requirements: REQ-1997, REQ-2191
from __future__ import annotations

from fastapi import APIRouter, Request

from provisa.api.admin.capabilities import require_capability_request
from provisa.api.errors import ApiError
from provisa.core import source_uploads
from provisa.core.sandbox_limits import SandboxDiskFull
from provisa.jdbc_source import SOURCE_TYPE as JDBC_SOURCE

router = APIRouter(prefix="/admin/sources", tags=["admin", "sources"])


@router.put("/{source_id}/file")
async def upload_file(request: Request, source_id: str, name: str, type: str) -> dict:
    require_capability_request(request, "source_registration")
    if type not in source_uploads.UPLOAD_TYPES:
        raise ApiError(
            400,
            "files.not_a_file_source",
            f"a {type} source is not given a file by upload",
            None,
            type=type,
        )
    if type == JDBC_SOURCE:
        from provisa.jdbc_source.jdbc import JdbcRefused, refuse_where_not_allowed

        try:
            refuse_where_not_allowed()
        except JdbcRefused as refused:
            raise ApiError(
                403,
                "schema.jdbc_driver_refused",
                f"Source {source_id!r}: {refused}",
                None,
                source=source_id,
            ) from None
    length = request.headers.get("content-length")
    try:
        path, size = await source_uploads.receive(
            source_id, name, request.stream(), int(length) if length is not None else None
        )
    except SandboxDiskFull as refused:
        raise ApiError(507, refused.code, str(refused), None, **refused.params) from None
    except source_uploads.UploadTooLarge as refused:
        raise ApiError(413, refused.code, str(refused), None, **refused.params) from None
    except source_uploads.UploadRefused as refused:
        raise ApiError(400, refused.code, str(refused), None, **refused.params) from None
    from provisa.api.admin.schema_helpers import _get_pool
    from provisa.api.admin.schema_mutation_ops import reprovision_source_after_registration
    from provisa.api.app import state

    await reprovision_source_after_registration(await _get_pool(), state, source_id)
    # REQ-1997: the path as the org's tokens write it ({files}/<source>/<name>), never the file's
    # physical location; it is what the source then names.
    from provisa.core.org_files import token_path

    return {"path": token_path(str(path)), "bytes": size}
