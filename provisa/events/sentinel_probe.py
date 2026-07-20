# Copyright (c) 2026 Kenneth Stott
# Canary: 7e3c2a19-8d46-4f15-9b07-2a6e3c4f81d9
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Sentinel freshness probe — a HASH-shaped token from a zero-byte marker, not the data (REQ-1148).

A producer that cannot cheaply expose a change token (a NoSQL store, an opaque API, a batch job) can
instead DROP a sentinel marker when its data changes. The probe reads only that marker, never the
data: a new drop changes the token → CHANGED → re-pull; an unchanged marker keeps the last snapshot.

The marker lives at a ``sentinel_path`` URL whose scheme selects the transport:

    file:///abs/path          -> exists + mtime + size
    ftp://user:pass@host/p     -> exists + mtime (MDTM) + size (SIZE)
    sftp://user:pass@host/p    -> exists + mtime + size (paramiko; fail-loud if absent)
    http(s)://host/p           -> ETag, else Last-Modified (a HEAD request)

The token is opaque and compared by equality only (REQ-855 gate, unchanged) — the same HASH token
shape files already use. A marker that cannot be read this call yields ``None`` (TTL degrade, never a
silent stale fallback), exactly like the other probe transports.
"""

from __future__ import annotations

from typing import Awaitable, Callable
from urllib.parse import urlparse

Transport = Callable[[], Awaitable["str | None"]]


def _stat_token(exists: bool, mtime: float | int | str | None, size: int | None) -> str | None:
    """The HASH token for a file-like marker: ``mtime:size``, or None when the marker is absent.

    A missing marker is None (not a token of "0:0") so an absent sentinel degrades to TTL rather than
    reading as a definite, unchanging state."""
    if not exists:
        return None
    return f"{mtime}:{size}"


def _file_token(path: str) -> str | None:
    import os

    try:
        st = os.stat(path)
    except OSError:
        return None
    return _stat_token(True, int(st.st_mtime), st.st_size)


def _ftp_token(parsed) -> str | None:
    import ftplib

    try:
        ftp = ftplib.FTP()  # nosec B321 - operator-configured sentinel host, control-plane only
        ftp.connect(parsed.hostname, parsed.port or 21, timeout=10)
        ftp.login(parsed.username or "anonymous", parsed.password or "")
        try:
            size = ftp.size(parsed.path)
            mdtm = ftp.sendcmd(f"MDTM {parsed.path}")  # "213 YYYYMMDDhhmmss"
        finally:
            ftp.quit()
    except ftplib.all_errors:  # already includes OSError
        return None
    mtime = mdtm.split(" ", 1)[1].strip() if " " in mdtm else mdtm
    return _stat_token(True, mtime, size)


def _sftp_token(parsed) -> str | None:
    try:
        import paramiko
    except ImportError as exc:  # fail loud — an sftp:// sentinel needs the optional dep
        raise ImportError(
            "sftp:// sentinel_path requires paramiko: pip install provisa[sftp]"
        ) from exc

    transport = None
    try:
        transport = paramiko.Transport((parsed.hostname, parsed.port or 22))
        transport.connect(username=parsed.username, password=parsed.password)
        sftp = paramiko.SFTPClient.from_transport(transport)
        if sftp is None:
            return None
        st = sftp.stat(parsed.path)
    except (OSError, paramiko.SSHException):
        return None
    finally:
        if transport is not None:
            transport.close()
    return _stat_token(True, int(st.st_mtime or 0), st.st_size)


HttpHead = Callable[[str], "dict[str, str] | None"]


def _default_http_head(url: str) -> dict[str, str] | None:
    import urllib.request

    if not url.startswith(("http://", "https://")):
        return None
    req = urllib.request.Request(url, method="HEAD")  # noqa: S310 - scheme checked above
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:  # nosec B310 - https/http validated
            return {k.lower(): v for k, v in resp.headers.items()}
    except OSError:
        return None


def _http_token(url: str, head: HttpHead) -> str | None:
    """ETag (preferred) else Last-Modified as the token; None when neither is exposed/reachable."""
    headers = head(url)
    if not headers:
        return None
    return headers.get("etag") or headers.get("last-modified")


def build_sentinel_probe(sentinel_path: str, *, http_head: HttpHead | None = None) -> Transport:
    """Build the ``freshness_token`` transport for a sentinel marker at ``sentinel_path`` (REQ-1148).

    Dispatches on the URL scheme; an unsupported scheme fails loud at build time (never a silent
    no-op probe). ``http_head`` is injectable so the HTTP transport is testable without a network."""
    parsed = urlparse(sentinel_path)
    scheme = (parsed.scheme or "file").lower()
    if scheme == "file":
        path = parsed.path or sentinel_path

        async def _file() -> str | None:
            return _file_token(path)

        return _file
    if scheme == "ftp":

        async def _ftp() -> str | None:
            return _ftp_token(parsed)

        return _ftp
    if scheme == "sftp":

        async def _sftp() -> str | None:
            return _sftp_token(parsed)

        return _sftp
    if scheme in ("http", "https"):
        head = http_head or _default_http_head

        async def _http() -> str | None:
            return _http_token(sentinel_path, head)

        return _http
    raise ValueError(
        f"unsupported sentinel_path scheme {scheme!r}; expected file/ftp/sftp/http(s)"
    )
