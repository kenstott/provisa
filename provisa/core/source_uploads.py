# Copyright (c) 2026 Kenneth Stott
# Canary: d70abd17-25b1-4b59-85c3-3a2ad044861e
#
# This source code is licensed under the Business Source License 1.1
"""The file a file-based source was given by upload (REQ-1997), and a JDBC source's driver jar
(REQ-2191).

An upload is kept in its org's folder (``provisa.core.org_files``), under
``files/<source id>/<file name>``, and the source's path names it there: encrypted when the org's
folder is, seen by every node (one shared mount), kept until the source goes. The file is
streamed to disk a chunk at a time, never held in memory, into a temporary file beside its place
that then takes that place in one rename: a reader sees the old file or the new one, never part
of either. A file larger than the operator's limit (``sources.max_upload_bytes``) is refused,
naming the limit, and nothing of it is kept."""

# Requirements: REQ-1997, REQ-2191
from __future__ import annotations

import asyncio
import os
import re
import shutil
import uuid
from collections.abc import AsyncIterator
from pathlib import Path

from provisa.core import org_files, settings_registry

#: The source types whose path names one file, which an upload can be; and JDBC, whose one file
#: is its driver jar (REQ-2191).
UPLOAD_TYPES = frozenset({"sqlite", "duckdb", "csv", "parquet", "jdbc"})

_AREA = "files"
_TEMP = ".upload-"
_SEGMENT = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")


class UploadRefused(ValueError):
    """An upload that cannot be kept; ``code`` and ``params`` word it for the reader."""

    def __init__(self, message: str, code: str, **params: object) -> None:
        super().__init__(message)
        self.code = code
        self.params = params


class UploadTooLarge(UploadRefused):
    def __init__(self, limit: int) -> None:
        super().__init__(
            f"the file is larger than the {limit} bytes an upload may be",
            "files.too_large",
            limit=limit,
        )


def max_bytes() -> int:
    return int(settings_registry.value("sources.max_upload_bytes"))


def _segment(value: str, what: str) -> str:
    if not _SEGMENT.match(value) or value.startswith(_TEMP):
        raise UploadRefused(
            f"{value!r} cannot be the {what} of an upload: letters, digits, '.', '_' and '-' only",
            "files.bad_name",
            name=value,
        )
    return value


def upload_dir(source_id: str, org_id: str | None = None) -> Path:
    """Where ``source_id``'s uploaded file is kept, made if it is not there."""
    return org_files.org_path(_AREA, _segment(source_id, "source"), org_id=org_id)


def is_upload(source_id: str, path: str | None, org_id: str | None = None) -> bool:
    """Whether ``path`` is ``source_id``'s uploaded file."""
    if not path:
        return False
    here = org_files.org_root(org_id) / _AREA / source_id
    return Path(org_files.resolve_path(path, org_id)).parent == here


async def receive(
    source_id: str,
    name: str,
    chunks: AsyncIterator[bytes],
    declared: int | None,
    org_id: str | None = None,
) -> tuple[Path, int]:
    """Keep the file ``chunks`` give as ``source_id``'s upload named ``name``, in place of any
    earlier one; its path and its size. A file over the limit -- said by its declared length,
    or found while it is read -- is refused and nothing of it is kept."""
    limit = max_bytes()
    if declared is not None and declared > limit:
        raise UploadTooLarge(limit)
    final = upload_dir(source_id, org_id) / _segment(name, "file name")
    # REQ-2006: a demo sandbox's uploads count against its folder's disk cap.
    from provisa.core.sandbox_limits import (
        folder_bytes,
        org_of_store,
        require_room,
        require_room_after,
    )

    owner = org_of_store(str(final))
    # Measured once: what the folder held before this file, which the stream may not outgrow.
    held = folder_bytes(owner) if owner is not None else 0
    if owner is not None:
        require_room(owner, declared or 0)
    temp = final.parent / f"{_TEMP}{uuid.uuid4().hex}"
    size = 0
    try:
        with open(temp, "wb") as out:
            async for chunk in chunks:
                size += len(chunk)
                if size > limit:
                    raise UploadTooLarge(limit)
                if owner is not None:
                    require_room_after(owner, held, size)
                await asyncio.to_thread(out.write, chunk)
            await asyncio.to_thread(out.flush)
            await asyncio.to_thread(os.fsync, out.fileno())
        os.replace(temp, final)
    except BaseException:
        temp.unlink(missing_ok=True)
        raise
    # An earlier upload under another name is no longer the source's file.
    for other in final.parent.iterdir():
        if other != final and not other.name.startswith(_TEMP):
            other.unlink()
    return final, size


def remove(source_id: str, org_id: str | None = None) -> None:
    """Remove ``source_id``'s uploaded file, when it has one."""
    here = org_files.org_root(org_id) / _AREA / _segment(source_id, "source")
    if here.exists():
        shutil.rmtree(here)
