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
``sources.download_roots``, once its symlinks are followed; on a server of many organizations a
file mount path is refused whatever the setting holds -- the server's disk is the platform's."""

# Requirements: REQ-2113

from __future__ import annotations

import ipaddress
import os
import socket
from collections.abc import AsyncIterator
from pathlib import Path, PurePosixPath
from typing import Any
from urllib.parse import unquote, urlparse

from provisa.core import settings_registry, source_uploads
from provisa.core.secrets_providers import many_organisations

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


def checked_mount(address: str) -> Path:
    """The file ``address`` names on this server, when it is a file under an allowed directory
    once its symlinks are followed; refused by name otherwise, and always on a server of many
    organizations."""
    if many_organisations():
        raise DownloadRefused(
            f"{address}: a server of many organizations copies no file of its own; "
            "give an https:// or s3:// address",
            "files.download_mount_refused",
            address=address,
        )
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


def _not_private(host: str, address: str) -> None:
    """On a server of many organizations, refuse by name an address whose host resolves to a
    private, loopback, link-local or otherwise internal address: a tenant's download must not
    reach the platform's own network (its cloud metadata server hands out the VM's credentials)."""
    if not many_organisations():
        return
    try:
        found = {str(info[4][0]) for info in socket.getaddrinfo(host, None)}
    except socket.gaierror as exc:
        raise DownloadRefused(
            f"{address}: {host} does not resolve", "files.download_failed", address=address
        ) from exc
    for ip in found:
        parsed = ipaddress.ip_address(ip.split("%", 1)[0])
        if not parsed.is_global or parsed.is_multicast:
            raise DownloadRefused(
                f"{address}: {host} resolves to {ip}, an address inside the server's own "
                "network, which a server of many organizations downloads nothing from",
                "files.download_internal_address",
                address=address,
                host=host,
            )


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


async def _https_chunks(address: str) -> AsyncIterator[bytes]:
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
            _not_private(parsed.hostname or "", address)
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


def _s3_body(address: str, hints: dict[str, Any]) -> tuple[Any, int | None]:
    import boto3
    from botocore.config import Config

    from provisa.core.secrets import resolve_secrets_in_dict

    hints = resolve_secrets_in_dict(dict(hints))
    parsed = urlparse(address)
    endpoint = hints.get("endpoint") or None
    # The host the bytes come from: an S3-compatible store's endpoint, else AWS's own.
    host = (urlparse(endpoint).hostname or "") if endpoint else "s3.amazonaws.com"
    _not_private(host, address)
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
        path = checked_mount(address)
        return await source_uploads.receive(
            source_id, name, _mount_chunks(path), path.stat().st_size, org_id
        )
    if kind == "s3":
        body, length = await asyncio.to_thread(_s3_body, address, hints or {})
        return await source_uploads.receive(source_id, name, _s3_chunks(body), length, org_id)
    # Its length is not taken on the server's word: the limit is held as the bytes arrive.
    return await source_uploads.receive(source_id, name, _https_chunks(address), None, org_id)


async def refetched(source: Any, ttl: int | None) -> bool:
    """Fetch ``source``'s file again when its copy has outlived its TTL (the table's declared
    cache TTL, ``ttl``), before its replica is built from it; whether it did. TTL -1, or none,
    keeps the copy it has: it is fetched again only when it is not there."""
    from datetime import UTC, datetime

    from provisa.core import org_files
    from provisa.core.cache_ttl import NEVER_EXPIRES

    kind = getattr(source.type, "value", source.type)
    if not source.download_from or kind not in DOWNLOAD_TYPES or not source.path:
        return False
    copy = Path(org_files.resolve_path(source.path))
    if copy.is_file():
        if ttl is None or ttl == NEVER_EXPIRES:
            return False
        age = datetime.now(UTC).timestamp() - copy.stat().st_mtime
        if age < ttl:
            return False
    await download(source.id, source.download_from, hints=source.federation_hints or {})
    return True
