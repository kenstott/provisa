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
naming the limit, and nothing of it is kept.

The own-disk rule (REQ-1997) is here too, one check for every source type. In a held organization
(``sandbox_egress.held``: a sandbox org on any install, and every org but the deployment's own on
a server of many organizations) a source's location on the server's own disk is refused, where
the source is saved and where the location is opened, unless it is inside that organization's
folder or is one its demo declares (the operator's own, ``sandbox_egress.declared_location``)."""

# Requirements: REQ-1997, REQ-2191
from __future__ import annotations

import asyncio
import os
import re
import shutil
import uuid
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any
from urllib.parse import unquote

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


# --- the own-disk rule (REQ-1997) -------------------------------------------------------------

#: Every source type whose ``path`` can name a place on the server's own disk: the upload types'
#: file, a ``files`` source's directory or glob, a lake table's location, an OpenAPI source's
#: specification and a gRPC source's proto file.
PATH_TYPES = UPLOAD_TYPES | {"files", "iceberg", "delta_lake", "openapi", "grpc_remote"}

#: How an OpenAPI source's path names a specification that is no location (a brand's shipped one,
#: REQ-1923; a ServiceNow companion's, REQ-1954).
_SPEC_MARKERS = ("brand:", "servicenow:")

#: Where a source of any type can name a file besides its path: ``(field, key)``, the key of a
#: bag (None for the field itself). A BigQuery service-account file, a SharePoint certificate,
#: a gRPC source's import directories, a sentinel marker (REQ-1148).
_OTHER_LOCATIONS: tuple[tuple[str, str | None], ...] = (
    ("sentinel_path", None),
    ("federation_hints", "credentials_path"),
    ("federation_hints", "import_paths"),
    ("mapping", "certificate_path"),
)

_REMOTE = re.compile(r"^([A-Za-z][A-Za-z0-9+.-]*)://([^/?#]+)")


class LocalPathRefused(PermissionError):
    """A held org's source names a place on the server's own disk outside the org's folder."""

    code = "sources.local_path_refused"

    def __init__(self, path: str) -> None:
        super().__init__(
            f"This organization's sources read no file on the server's own disk outside its own "
            f"folder; {path!r} is one. Upload the file, or name it as {{files}}/... or by a "
            "remote address"
        )
        self.path = path
        self.params = {"path": path}


def on_own_disk(location: str) -> bool:
    """Whether ``location`` can be read from this server's disk: anything but a remote address
    written whole (``scheme://host/...``, the scheme not ``file``). An address with a ``..``
    segment is one too: a reader that takes it for a path climbs out of where it started."""
    found = _REMOTE.match(location)
    if found is None or found.group(1).lower() == "file" or found.group(2) in (".", ".."):
        return True
    return ".." in re.split(r"[/\\]", location)


def _disk_paths(location: str) -> list[str]:
    """``location`` as the paths a reader may open for it: as written, without a ``file:``
    prefix and its authority, and with its percent escapes read."""
    path = location
    if path[:5].lower() == "file:":
        path = path[5:]
        if path.startswith("//"):
            path = "/" + path[2:].partition("/")[2]
    return sorted({path, unquote(path)})


def _inside(real: str, root: str) -> bool:
    return real == root or real.startswith(root + os.sep)


def require_own_disk(location: str | None, org_id: str | None = None) -> None:
    """Refuse, by name, a held org's ``location`` on the server's own disk unless it is inside
    the org's folder (through no ``..`` or symbolic link out of it) or its demo declares it. A
    remote location, and any org that is not held, has nothing to check here. ``org_id`` is the
    bound org when None."""
    from provisa.core.request_context import current_org
    from provisa.core.secrets import resolve_secrets
    from provisa.security import sandbox_egress

    org = org_id if org_id is not None else current_org.get(None)
    if not location or not sandbox_egress.held(org):
        return
    assert org is not None
    # The place read is the resolved one (a path may be written as a secret reference); the one
    # named in the refusal is the one written.
    resolved = str(resolve_secrets(location))
    if not on_own_disk(resolved):
        return
    if org_files.is_org_path(resolved):
        try:
            org_files.resolve_path(resolved, org)
        except org_files.OrgPathRefused as exc:
            raise LocalPathRefused(location) from exc
        return
    root = os.path.realpath(org_files.folder(org))
    for path in _disk_paths(resolved):
        real = os.path.realpath(path)
        if not _inside(real, root) and not sandbox_egress.declared_location(org, real):
            raise LocalPathRefused(location)


def locations(source_type: str, fields: dict[str, Any]) -> list[str]:
    """Every location a source of ``source_type`` names that could be on the server's own disk,
    as written: its path, when its type reads one (:data:`PATH_TYPES`), the files its other
    fields name (:data:`_OTHER_LOCATIONS`), the place its file is downloaded from and its code
    directory."""
    found: list[str] = []
    path = fields.get("path")
    if source_type in PATH_TYPES and isinstance(path, str) and path:
        if not (source_type == "openapi" and path.startswith(_SPEC_MARKERS)):
            found.append(path)
    for field, key in _OTHER_LOCATIONS:
        value = fields.get(field)
        if key is not None:
            value = value.get(key) if isinstance(value, dict) else None
        if isinstance(value, str) and value:
            # A gRPC source's import directories are kept as one comma-separated text.
            found.extend(value.split(",") if key == "import_paths" else [value])
        elif isinstance(value, (list, tuple)):
            found.extend(v for v in value if isinstance(v, str) and v)
    # A CSV or Parquet source's download_from (REQ-2113) and a code source's directory
    # (REQ-2121): each read from where it names, by source_downloads.checked_mount and
    # codesource.source.checked_directory.
    download = fields.get("download_from")
    if isinstance(download, str) and download:
        found.append(download)
    mapping = fields.get("mapping")
    code = mapping.get("code") if isinstance(mapping, dict) else None
    if isinstance(code, dict) and code.get("kind") == "directory":
        directory = code.get("path")
        if isinstance(directory, str) and directory:
            found.append(directory)
    return found


def real_locations(source_type: str, fields: dict[str, Any]) -> frozenset[str]:
    """The real paths of the places on this server's disk a source names: what a demo's own
    source declares (``sandbox_egress.set_demo_destinations``)."""
    from provisa.core.secrets import resolve_secrets

    real: set[str] = set()
    for location in locations(source_type, fields):
        resolved = str(resolve_secrets(location))
        if on_own_disk(resolved) and not org_files.is_org_path(resolved):
            real.update(os.path.realpath(path) for path in _disk_paths(resolved))
    return frozenset(real)


def require_source_own_disk(
    source_type: str, fields: dict[str, Any], org_id: str | None = None
) -> None:
    """:func:`require_own_disk` for every location a source names (:func:`locations`)."""
    for location in locations(source_type, fields):
        require_own_disk(location, org_id)
