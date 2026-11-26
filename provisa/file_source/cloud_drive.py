# Copyright (c) 2026 Kenneth Stott
# Canary: 3d9e6b1a-0f4c-4e27-8a5d-c71b2f9e4a63
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""OneDrive and Google Drive as File Crawler transports (REQ-2280).

Two fsspec filesystems, registered under ``onedrive://`` and ``gdrive://``, so every place the
File Crawler lists, globs, stats or reads a remote path (``files_glob``, ``crawler``) reaches a
drive exactly as it reaches ``s3://`` or ``sftp://``. Each lists a folder and reads a file by
path; nothing is written.

- ``onedrive://<user>/<folder>/<glob>``: the drive of ``<user>`` (a user principal name or object
  id) through the Microsoft Graph drive API, signed in as the SharePoint source signs in: an app
  registration's client credentials in the source's tenant (OneDrive for Business is a SharePoint
  drive). The app needs the Graph ``Files.Read.All`` application permission.
- ``gdrive://<folder id>/<folder>/<glob>``: the files under a Drive folder (or shared drive)
  through the Google Drive API, signed in as the Google Sheets source signs in: a service
  account, reading as itself, or a person's OAuth approval (a refresh token). A service account
  owns no storage, so the folder must be shared with it.

A source's credentials are its own fields, secrets resolved (:func:`drive_storage_options`): the
filesystem is built with them for the one call, never cached across sources.
"""

# Requirements: REQ-2280
from __future__ import annotations

import io
from collections.abc import Callable, Iterator
from datetime import datetime
from typing import Any, cast
from urllib.parse import quote

import fsspec
import httpx
from fsspec.spec import AbstractFileSystem

from provisa.file_source.drive_operand import (
    _field,
    gdrive_sign_in,
    onedrive_sign_in,
)
from provisa.file_source.drive_schemes import GDRIVE, ONEDRIVE, scheme_of

GRAPH = "https://graph.microsoft.com/v1.0"
MICROSOFT_LOGIN = "https://login.microsoftonline.com"
GRAPH_SCOPE = "https://graph.microsoft.com/.default"
DRIVE_API = "https://www.googleapis.com/drive/v3"
GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
DRIVE_READONLY_SCOPE = "https://www.googleapis.com/auth/drive.readonly"

_FOLDER_MIME = "application/vnd.google-apps.folder"
_GOOGLE_NATIVE = "application/vnd.google-apps."
_DRIVE_FIELDS = "id,name,mimeType,size,modifiedTime"
_TIMEOUT = 30.0

Token = Callable[[], str]


class DriveNameAmbiguous(ValueError):
    """A Google Drive folder holding more than one item of the name a path segment gives."""

    def __init__(self, path: str, name: str) -> None:
        super().__init__(
            f"gdrive path {path!r}: the folder holds more than one item named {name!r}, so the "
            "path names no single file; rename one of them"
        )


class DriveFileNotReadable(ValueError):
    """A Google Docs, Sheets or Slides file: it has no bytes of its own to read as a file."""

    def __init__(self, path: str, mime: str) -> None:
        super().__init__(
            f"gdrive path {path!r} is a Google {mime.removeprefix(_GOOGLE_NATIVE)} file, which "
            "has no file content to read; save it as CSV or Excel in the folder, or register a "
            "Google Sheets source for a sheet"
        )


def _epoch(stamp: str) -> float:
    return datetime.fromisoformat(stamp.replace("Z", "+00:00")).timestamp()


class _DriveFileSystem(AbstractFileSystem):
    """What both drives share: a read-only filesystem over an HTTP API, signed in by ``token``
    (called for each request; the token helpers hold a token until it lapses), sent through
    ``client``."""

    cachable = False  # built per source with its own credentials (module docstring)

    def __init__(self, *, token: Token, client: httpx.Client, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._token = token
        self._client = client

    def _get(self, url: str, path: str, **params: Any) -> httpx.Response:
        resp = self._client.get(
            url,
            params=params or None,
            headers={"Authorization": f"Bearer {self._token()}"},
            follow_redirects=True,
        )
        if resp.status_code == 404:
            raise FileNotFoundError(path)
        resp.raise_for_status()
        return resp

    def _path(self, path: str) -> str:
        """``path`` without its scheme: the filesystem's own name for it."""
        return cast(str, self._strip_protocol(path))

    def _open(
        self,
        path: str,
        mode: str = "rb",
        block_size: Any = None,
        autocommit: bool = True,
        cache_options: Any = None,
        **kwargs: Any,
    ) -> Any:  # a read-only in-memory file, not one of fsspec's buffered files
        if mode != "rb":
            raise PermissionError(
                f"{self.protocol}:// is read-only: cannot open {path!r} as {mode!r}"
            )
        return io.BytesIO(self.cat_file(path))

    def cat_file(
        self, path: str, start: int | None = None, end: int | None = None, **kw: Any
    ) -> bytes:
        data = self._content(self._path(path))
        return data[start:end]

    def _content(self, path: str) -> bytes:
        raise NotImplementedError

    def ls(self, path: str, detail: bool = True, **kwargs: Any) -> list[Any]:
        path = self._path(path)
        entry = self.info(path)
        entries = list(self._children(path)) if entry["type"] == "directory" else [entry]
        return entries if detail else [e["name"] for e in entries]

    def _children(self, path: str) -> Iterator[dict]:
        raise NotImplementedError


class OneDriveFileSystem(_DriveFileSystem):
    """``onedrive://<user>/<path>``: a user's drive through the Microsoft Graph drive API."""

    protocol = ONEDRIVE

    def _item_url(self, path: str) -> str:
        owner, _, rest = path.partition("/")
        if not owner:
            raise ValueError("onedrive:// path names no drive owner: onedrive://<user>/<folder>")
        base = f"{GRAPH}/users/{quote(owner, safe='')}/drive/root"
        return f"{base}:/{quote(rest)}:" if rest else base

    def _entry(self, parent: str, item: dict) -> dict:
        is_folder = "folder" in item
        return {
            "name": f"{parent}/{item['name']}" if parent else item["name"],
            "size": 0 if is_folder else int(item.get("size", 0)),
            "type": "directory" if is_folder else "file",
            "mtime": _epoch(item["lastModifiedDateTime"]),
        }

    def info(self, path: str, **kwargs: Any) -> dict:
        path = self._path(path)
        item = self._get(self._item_url(path), path).json()
        entry = self._entry("", item)
        entry["name"] = path
        return entry

    def _children(self, path: str) -> Iterator[dict]:
        url: str | None = f"{self._item_url(path)}/children"
        while url is not None:
            page = self._get(url, path).json()
            for item in page.get("value", []):
                yield self._entry(path, item)
            url = page.get("@odata.nextLink")

    def _content(self, path: str) -> bytes:
        return self._get(f"{self._item_url(path)}/content", path).content


class GoogleDriveFileSystem(_DriveFileSystem):
    """``gdrive://<folder id>/<path>``: the files under a Drive folder through the Drive API.
    Drive addresses files by id, so each segment of the path is found by name in its folder."""

    protocol = GDRIVE

    def _file(self, file_id: str, path: str) -> dict:
        return self._get(
            f"{DRIVE_API}/files/{quote(file_id, safe='')}",
            path,
            fields=_DRIVE_FIELDS,
            supportsAllDrives="true",
        ).json()

    def _listing(self, parent_id: str, path: str, name: str | None = None) -> Iterator[dict]:
        query = f"'{parent_id}' in parents and trashed = false"
        if name is not None:
            escaped = name.replace("\\", "\\\\").replace("'", "\\'")
            query += f" and name = '{escaped}'"
        token: str | None = None
        while True:
            params: dict[str, Any] = {
                "q": query,
                "fields": f"nextPageToken,files({_DRIVE_FIELDS})",
                "pageSize": 1000,
                "supportsAllDrives": "true",
                "includeItemsFromAllDrives": "true",
            }
            if token is not None:
                params["pageToken"] = token
            page = self._get(f"{DRIVE_API}/files", path, **params).json()
            yield from page.get("files", [])
            token = page.get("nextPageToken")
            if token is None:
                return

    def _resolve(self, path: str) -> dict:
        root, _, rest = path.partition("/")
        if not root:
            raise ValueError("gdrive:// path names no folder: gdrive://<folder id>/<path>")
        item = self._file(root, path)
        for name in (seg for seg in rest.split("/") if seg):
            found = list(self._listing(item["id"], path, name))
            if not found:
                raise FileNotFoundError(path)
            if len(found) > 1:
                raise DriveNameAmbiguous(path, name)
            item = found[0]
        return item

    @staticmethod
    def _entry(name: str, item: dict) -> dict:
        is_folder = item["mimeType"] == _FOLDER_MIME
        return {
            "name": name,
            "size": 0 if is_folder else int(item.get("size", 0)),
            "type": "directory" if is_folder else "file",
            "mtime": _epoch(item["modifiedTime"]),
            "id": item["id"],
            "mimeType": item["mimeType"],
        }

    def info(self, path: str, **kwargs: Any) -> dict:
        path = self._path(path)
        return self._entry(path, self._resolve(path))

    def _children(self, path: str) -> Iterator[dict]:
        folder = self._resolve(path)
        for item in self._listing(folder["id"], path):
            yield self._entry(f"{path}/{item['name']}", item)

    def _content(self, path: str) -> bytes:
        item = self._resolve(path)
        if item["mimeType"].startswith(_GOOGLE_NATIVE):
            raise DriveFileNotReadable(path, item["mimeType"])
        return self._get(
            f"{DRIVE_API}/files/{quote(item['id'], safe='')}",
            path,
            alt="media",
            supportsAllDrives="true",
        ).content


def register() -> None:
    """Register both transports with fsspec, so ``fsspec.core.url_to_fs`` resolves them."""
    fsspec.register_implementation(ONEDRIVE, OneDriveFileSystem, clobber=True)
    fsspec.register_implementation(GDRIVE, GoogleDriveFileSystem, clobber=True)


register()


def _onedrive_token(source: Any) -> Token:
    from provisa.api_source.caller import _fetch_oauth2_token
    from provisa.core.auth_models import ApiAuthOAuth2ClientCredentials

    sign_in = onedrive_sign_in(source)
    auth = ApiAuthOAuth2ClientCredentials(
        client_id=sign_in["clientId"],
        client_secret=sign_in["clientSecret"],
        token_url=f"{MICROSOFT_LOGIN}/{quote(sign_in['tenantId'], safe='')}/oauth2/v2.0/token",
        scope=GRAPH_SCOPE,
    )
    return lambda: _fetch_oauth2_token(auth)


def _gdrive_token(source: Any) -> Token:
    from provisa.api_source.oauth_grants import access_token, service_account_token
    from provisa.core.auth_models import ApiAuthOAuth2RefreshToken

    sign_in = gdrive_sign_in(source)
    if "serviceAccountKey" in sign_in:
        key = sign_in["serviceAccountKey"]
        return lambda: service_account_token(key, [DRIVE_READONLY_SCOPE])
    auth = ApiAuthOAuth2RefreshToken(
        client_id=sign_in["clientId"],
        client_secret=sign_in["clientSecret"],
        refresh_token=sign_in["refreshToken"],
        token_url=GOOGLE_TOKEN_URL,
        scope=DRIVE_READONLY_SCOPE,
    )
    return lambda: access_token(auth)


def drive_storage_options(source: Any) -> dict[str, Any]:
    """The fsspec storage options a files source's drive path is opened with: its sign-in and
    an HTTP client whose requests the organization's egress rules check (REQ-2006). Empty for a
    path in no drive, which needs none here."""
    from provisa.core.secrets import resolve_secrets
    from provisa.security import sandbox_egress

    scheme = scheme_of(resolve_secrets(_field(source, "path") or ""))
    if scheme is None:
        return {}
    token = _onedrive_token(source) if scheme == ONEDRIVE else _gdrive_token(source)
    client = httpx.Client(timeout=_TIMEOUT, event_hooks=sandbox_egress.http_sync_event_hooks())
    return {"token": token, "client": client}
