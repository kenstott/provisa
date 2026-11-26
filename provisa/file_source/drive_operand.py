# Copyright (c) 2026 Kenneth Stott
# Canary: 9691a65d-6127-4efd-a221-3ae927ad1bc1
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""How the Calcite file adapter reads a onedrive:// or gdrive:// source (REQ-2280): its storage
type, its storageConfig (the drive and the source's own sign-in, secrets resolved) and the path
within the drive.

The engines' connectors (the pgwire bundle's operand, the trino-file catalog's storage-config) build
it from here, so they do not import the transports' HTTP sign-in (``cloud_drive``)."""

# Requirements: REQ-2280
from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from provisa.file_source.drive_schemes import GDRIVE, ONEDRIVE, scheme_of

#: How a gdrive:// source signs in (its ``mapping.gdrive_auth``).
GDRIVE_SERVICE_ACCOUNT = "service_account"
GDRIVE_OAUTH = "oauth"
GDRIVE_AUTH_KINDS: tuple[str, ...] = (GDRIVE_SERVICE_ACCOUNT, GDRIVE_OAUTH)


class DriveCredentialsMissing(ValueError):
    """A drive path whose source carries no credential the transport signs in with."""


def _field(source: Any, name: str) -> Any:
    """A source's field, from a Source model or a sources row (a dict)."""
    return source.get(name) if isinstance(source, Mapping) else getattr(source, name, None)


def _required(source: Any, field: str, what: str, scheme: str) -> str:
    from provisa.core.secrets import resolve_secrets

    value = _field(source, field)
    if not value:
        raise DriveCredentialsMissing(
            f"files source {_field(source, 'id')!r}: a {scheme}:// path needs {field} ({what})"
        )
    return resolve_secrets(value, credential=field == "password")


def onedrive_sign_in(source: Any) -> dict[str, str]:
    """A onedrive:// source's app registration: its tenant, client id and client secret."""
    return {
        "tenantId": _required(source, "database", "the tenant id", ONEDRIVE),
        "clientId": _required(source, "username", "the client id", ONEDRIVE),
        "clientSecret": _required(source, "password", "the client secret", ONEDRIVE),
    }


def gdrive_sign_in(source: Any) -> dict[str, str]:
    """A gdrive:// source's sign-in, by its ``mapping.gdrive_auth``: a service account key
    (``serviceAccountKey``), or an OAuth client and its refresh token."""
    from provisa.core.secrets import resolve_secrets

    source_id = _field(source, "id")
    mapping = _field(source, "mapping")
    if not isinstance(mapping, Mapping) or mapping.get("gdrive_auth") not in GDRIVE_AUTH_KINDS:
        raise DriveCredentialsMissing(
            f"files source {source_id!r}: a gdrive:// path needs mapping.gdrive_auth, one of "
            f"{list(GDRIVE_AUTH_KINDS)}"
        )
    if mapping["gdrive_auth"] == GDRIVE_SERVICE_ACCOUNT:
        key = _required(source, "password", "the service account key, JSON", GDRIVE)
        return {"serviceAccountKey": key}
    refresh_token = mapping.get("refresh_token")
    if not refresh_token:
        raise DriveCredentialsMissing(
            f"files source {source_id!r}: gdrive_auth=oauth needs mapping.refresh_token"
        )
    return {
        "clientId": _required(source, "username", "the OAuth client id", GDRIVE),
        "clientSecret": _required(source, "password", "the OAuth client secret", GDRIVE),
        "refreshToken": resolve_secrets(str(refresh_token), credential=True),
    }


def drive_adapter_operand(source: Any, directory: str) -> dict[str, Any] | None:
    """How the Calcite file adapter reads ``directory`` (a files source's path before its glob,
    secrets resolved) live from its drive: the adapter's ``storageType``, its ``storageConfig``
    (the drive and the source's sign-in, secrets resolved) and the ``directory`` within the
    drive. None for a directory in no drive.

    - ``onedrive://<user>/<folder>``: storage ``onedrive``, the drive of ``userPrincipalName``
      ``<user>``; ``<folder>`` is relative to the drive's root.
    - ``gdrive://<folder id>/<folder>``: storage ``gdrive``, rooted at ``folderId``
      ``<folder id>``; ``<folder>`` is found by name below it.
    """
    scheme = scheme_of(directory)
    if scheme is None:
        return None
    root, _, rest = directory.split("://", 1)[1].partition("/")
    if not root:
        raise ValueError(
            f"files source {_field(source, 'id')!r}: {directory!r} names no "
            + (
                "drive owner: onedrive://<user>/<folder>"
                if scheme == ONEDRIVE
                else "folder: gdrive://<folder id>/<folder>"
            )
        )
    if scheme == ONEDRIVE:
        config: dict[str, Any] = {"userPrincipalName": root, **onedrive_sign_in(source)}
    else:
        config = {"folderId": root, **gdrive_sign_in(source)}
    return {"storageType": scheme, "storageConfig": config, "directory": rest.strip("/")}
