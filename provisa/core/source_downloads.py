# Copyright (c) 2026 Kenneth Stott
# Canary: 43fbfb8c-612e-49a4-9217-07ee68f6d594
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A CSV or Parquet source given its file by download (REQ-2113): a third way beside a path and an
upload (REQ-1997). The steward names one file -- a file mount path, an ``https://`` URL or an
``s3://`` URL -- and its bytes are streamed through the upload path (``source_uploads.receive``)
into the org's folder under ``files/<source id>/<name>``: the same size limit, the same folder
(encrypted when the org's folder is) and the same one-rename swap as an upload. From then on the
source reads that file.

A file mount path may name only a file under a directory the operator lists in
``sources.download_roots``, once its symlinks are followed; a held organization
(``sandbox_egress.held``) is refused one outside its own folder that its demo does not declare,
whatever the setting holds (REQ-1997, ``source_uploads.require_own_disk``)."""

# Requirements: REQ-2113

from __future__ import annotations

import os
from collections.abc import AsyncIterator
from pathlib import Path, PurePosixPath
from typing import Any
from urllib.parse import unquote, urlparse

from provisa.core import settings_registry, source_uploads
from provisa.security import sandbox_egress

#: The source types whose file can be downloaded: one CSV or Parquet file.
DOWNLOAD_TYPES = frozenset({"csv", "parquet"})
_CHUNK = 1 << 20
#: How many redirects an https download follows, each hop's host checked as the first one is.
_MAX_HOPS = 5


class DownloadRefused(source_uploads.UploadRefused):
    """An address a source's file may not be downloaded from; the message names it and why."""


def download_roots() -> list[str]:
    """The directories a file mount path may be downloaded from, as the operator listed them,
    each resolved."""
    return [
        os.path.realpath(os.path.expanduser(root))
        for root in settings_registry.value("sources.download_roots")
    ]


def _inside(path: str, roots: list[str]) -> bool:
    return any(path == root or path.startswith(root.rstrip(os.sep) + os.sep) for root in roots)


def _kind(address: str) -> str:
    """``mount``, ``https`` or ``s3``; refused by name for any other kind of address."""
    from provisa.federation.connector_duckdb import location_kind

    try:
        kind = location_kind(address)
    except ValueError:
        kind = "other"
    if kind is None:
        return "mount"
    if kind == "s3":
        return "s3"
    if kind == "http" and address.lower().startswith("https://"):
        return "https"
    raise DownloadRefused(
        f"{address}: a file is downloaded from a file mount path, an https:// URL or an s3:// URL",
        "files.download_bad_address",
        address=address,
    )


def checked_mount(address: str, org_id: str | None = None) -> Path:
    """The file ``address`` names on this server, when it is a file under an allowed directory
    once its symlinks are followed; refused by name otherwise, and for a held org (``org_id``,
    the bound org when None) outside its own folder."""
    try:
        source_uploads.require_own_disk(address, org_id)
    except source_uploads.LocalPathRefused as exc:
        raise DownloadRefused(
            f"{address}: this organization copies no file from the server's own disk outside "
            "its own folder; give an https:// or s3:// address",
            "files.download_mount_refused",
            address=address,
        ) from exc
    path = address.removeprefix("file://")
    expanded = os.path.expanduser(path)
    if not os.path.isabs(expanded):
        raise DownloadRefused(
            f"{address}: name the file by its full path",
            "files.download_relative",
            address=address,
        )
    real = os.path.realpath(expanded)
    roots = download_roots()
    if not _inside(real, roots):
        raise DownloadRefused(
            f"{address} resolves to {real}, outside the directories a source's file may be "
            f"downloaded from: {', '.join(roots) or 'none are listed'}",
            "files.download_outside_roots",
            address=address,
            roots=", ".join(roots),
        )
    if not os.path.isfile(real):
        raise DownloadRefused(
            f"{address}: there is no file there", "files.download_missing", address=address
        )
    return Path(real)


def _not_private(host: str, port: int | None, address: str, org_id: str | None) -> None:
    """For a held org (``org_id``, the bound org when None), refuse by name an address whose host
    is not public, unless its demo declares it (``sandbox_egress.require_allowed``, REQ-1557): a
    held org's download must not reach the server's own network (its cloud metadata server hands
    out the VM's credentials)."""
    try:
        sandbox_egress.require_allowed(host, port, org_id)
    except sandbox_egress.PrivateDestinationRefused as exc:
        raise DownloadRefused(
            f"{address}: {host} is {exc.params['address']}, which is not a public address; "
            "this organization downloads nothing from the server's own network",
            "files.download_internal_address",
            address=address,
            host=host,
        ) from exc


def file_name(address: str) -> str:
    """The name the downloaded file is kept under: the last segment of the address's path, with
    any character an upload's name may not hold made '_' (as the source form names an upload)."""
    import re

    path = urlparse(address).path if "://" in address else address
    name = re.sub(r"[^A-Za-z0-9._-]", "_", PurePosixPath(unquote(path)).name)
    return re.sub(r"^[^A-Za-z0-9]+", "", name) or "file"


async def _mount_chunks(path: Path) -> AsyncIterator[bytes]:
    import asyncio

    with open(path, "rb") as handle:
        while chunk := await asyncio.to_thread(handle.read, _CHUNK):
            yield chunk


async def _https_chunks(address: str, org_id: str | None) -> AsyncIterator[bytes]:
    import httpx

    url = address
    async with httpx.AsyncClient(follow_redirects=False, timeout=120) as client:
        for _hop in range(_MAX_HOPS + 1):
            parsed = urlparse(url)
            if parsed.scheme != "https":
                raise DownloadRefused(
                    f"{address}: redirected to {url}, which is not https://",
                    "files.download_bad_address",
                    address=address,
                )
            _not_private(parsed.hostname or "", parsed.port, address, org_id)
            async with client.stream("GET", url) as response:
                if response.is_redirect:
                    url = str(response.url.join(response.headers["location"]))
                    continue
                if response.status_code != 200:
                    raise DownloadRefused(
                        f"{address}: answered {response.status_code}",
                        "files.download_failed",
                        address=address,
                        status=response.status_code,
                    )
                async for chunk in response.aiter_bytes(_CHUNK):
                    yield chunk
                return
    raise DownloadRefused(
        f"{address}: redirected more than {_MAX_HOPS} times",
        "files.download_failed",
        address=address,
    )


def _s3_body(address: str, hints: dict[str, Any], org_id: str | None) -> tuple[Any, int | None]:
    import boto3
    from botocore.config import Config

    from provisa.core.secrets import resolve_secrets_in_dict

    hints = resolve_secrets_in_dict(dict(hints))
    parsed = urlparse(address)
    endpoint = hints.get("endpoint") or None
    # The host the bytes come from: an S3-compatible store's endpoint, else AWS's own.
    if endpoint:
        host, port = urlparse(endpoint).hostname or "", urlparse(endpoint).port
    else:
        host, port = "s3.amazonaws.com", None
    _not_private(host, port, address, org_id)
    client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=hints.get("access_key_id") or None,
        aws_secret_access_key=hints.get("secret_access_key") or None,
        region_name=hints.get("region") or None,
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path" if hints.get("endpoint") else "virtual"},
        ),
    )
    answer = client.get_object(Bucket=parsed.netloc, Key=parsed.path.lstrip("/"))
    return answer["Body"], answer.get("ContentLength")


async def _s3_chunks(body: Any) -> AsyncIterator[bytes]:
    import asyncio

    try:
        while chunk := await asyncio.to_thread(body.read, _CHUNK):
            yield chunk
    finally:
        body.close()


async def download(
    source_id: str,
    address: str,
    *,
    hints: dict[str, Any] | None = None,
    org_id: str | None = None,
) -> tuple[Path, int]:
    """Keep the file at ``address`` as ``source_id``'s file, in place of any earlier one, through
    the upload path; its path and its size. ``hints`` are the source's object-store credentials
    (federation_hints), used for an s3:// address."""
    import asyncio

    kind = _kind(address)
    name = file_name(address)
    if kind == "mount":
        path = checked_mount(address, org_id)
        return await source_uploads.receive(
            source_id, name, _mount_chunks(path), path.stat().st_size, org_id
        )
    if kind == "s3":
        body, length = await asyncio.to_thread(_s3_body, address, hints or {}, org_id)
        return await source_uploads.receive(source_id, name, _s3_chunks(body), length, org_id)
    # Its length is not taken on the server's word: the limit is held as the bytes arrive.
    return await source_uploads.receive(
        source_id, name, _https_chunks(address, org_id), None, org_id
    )


async def read_bytes(address: str, *, hints: dict[str, Any] | None = None) -> bytes:
    """What is at ``address``, read as :func:`download` reads it and held to the upload limit: for
    a document that is read and kept nowhere as a file (a dbt manifest, REQ-2228)."""
    import asyncio

    kind = _kind(address)
    if kind == "mount":
        chunks = _mount_chunks(checked_mount(address))
    elif kind == "s3":
        body, _length = await asyncio.to_thread(_s3_body, address, hints or {}, None)
        chunks = _s3_chunks(body)
    else:
        chunks = _https_chunks(address, None)
    limit = source_uploads.max_bytes()
    held = bytearray()
    async for chunk in chunks:
        held.extend(chunk)
        if len(held) > limit:
            raise source_uploads.UploadTooLarge(limit)
    return bytes(held)


async def refetched(source: Any, table: Any) -> bool:
    """Fetch ``source``'s file again when its copy has outlived its TTL (``table``'s declared
    cache TTL), before ``table``'s replica is built from it; whether it did. TTL -1, or none,
    keeps the copy it has: it is fetched again only when it is not there. A source read where
    it is has nothing to fetch."""
    from datetime import UTC, datetime

    from provisa.core import org_files
    from provisa.core.cache_ttl import NEVER_EXPIRES
    from provisa.federation.role_ttl import declared_cache_ttl

    if not getattr(source, "download_from", None):
        return False
    kind = getattr(source.type, "value", source.type)
    if kind not in DOWNLOAD_TYPES or not source.path:
        return False
    ttl = declared_cache_ttl(table, source)
    copy = Path(org_files.resolve_path(source.path))
    if copy.is_file():
        if ttl is None or ttl == NEVER_EXPIRES:
            return False
        age = datetime.now(UTC).timestamp() - copy.stat().st_mtime
        if age < ttl:
            return False
    await download(source.id, source.download_from, hints=source.federation_hints or {})
    return True
