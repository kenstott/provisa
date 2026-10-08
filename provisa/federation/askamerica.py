# Copyright (c) 2026 Kenneth Stott
# Canary: 837b8ef4-e858-460c-af05-d02aec3e76e6
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An AskAmerica (US government data) source: a Postgres-wire source like its sibling adapters.

The operator gives one thing, the API key (REQ-540). Everything else follows from it:

* The key is presented to AskAmerica's API, which answers with the short-lived, read-only
  object-store credentials the data is read with (``resolve_storage_credentials``) — the one
  place the key is turned into connection details. What it returns is held in memory and handed
  to the adapter's process; it is never written to config or to a log.
* The Postgres endpoint is the source's bundled ``pgwire-govdata`` server, started and reached
  exactly as ``pgwire-sharepoint`` / ``-salesforce`` / ``-cloudops`` are
  (``federation.pgwire_replica``). The server renews its own credentials before they expire,
  from the key it is started with.

A source lists the schemas it serves (``sec``, ``econ``, ...) in ``database``, as the Sources
form writes them; each is a schema of the adapter's one database, and a registered table names
its own.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from http import HTTPStatus
from typing import Any, Callable

from provisa.core.secrets import resolve_secrets

# Requirements: REQ-492, REQ-540

#: AskAmerica's API: where a key is exchanged for the credentials its data is read with.
CREDENTIALS_URL = "https://api.askamerica.ai/v1/catalog/credentials"
#: The API sits behind a bot filter that refuses a client library's default User-Agent.
USER_AGENT = "provisa-askamerica/1.0"
REQUEST_SECONDS = 10.0


class AskAmericaKeyMissing(ValueError):
    """The source has no API key."""

    def __init__(self, source_id: str) -> None:
        super().__init__(f"AskAmerica source {source_id!r}: an API key is required")
        self.source_id = source_id


class AskAmericaKeyRefused(PermissionError):
    """AskAmerica refused the API key: unknown, expired or revoked."""

    def __init__(self, detail: str) -> None:
        super().__init__(f"AskAmerica refused the API key: {detail}")
        self.detail = detail


class AskAmericaUnavailable(RuntimeError):
    """AskAmerica's API did not answer with credentials (not a refusal of the key)."""


@dataclass(frozen=True)
class StorageCredentials:
    """What an API key resolves to: where the data is and the credentials that read it."""

    access_key_id: str
    secret_access_key: str
    session_token: str
    region: str
    endpoint: str
    bucket: str
    expires_at_millis: int

    def __repr__(self) -> str:  # never printed with its secrets
        return f"StorageCredentials(bucket={self.bucket!r}, expires_at_millis={self.expires_at_millis})"


def _fetch(api_key: str) -> tuple[int, dict]:
    import httpx

    response = httpx.get(
        CREDENTIALS_URL,
        headers={"X-API-Key": api_key, "User-Agent": USER_AGENT},
        timeout=REQUEST_SECONDS,
    )
    return response.status_code, response.json()


def resolve_storage_credentials(
    api_key: str, *, fetch: Callable[[str], tuple[int, dict]] | None = None
) -> StorageCredentials:
    """Exchange ``api_key`` for the credentials its data is read with. A refused key is
    ``AskAmericaKeyRefused``; any other answer without credentials is ``AskAmericaUnavailable``."""
    status, body = (fetch if fetch is not None else _fetch)(api_key)
    if status in (HTTPStatus.UNAUTHORIZED, HTTPStatus.FORBIDDEN):
        raise AskAmericaKeyRefused(str(body.get("error", status)))
    if status != HTTPStatus.OK:
        raise AskAmericaUnavailable(
            f"AskAmerica's credentials API answered {status}: {body.get('error', 'no detail')}"
        )
    return StorageCredentials(
        access_key_id=body["access_key_id"],
        secret_access_key=body["secret_access_key"],
        session_token=body["session_token"],
        region=body["region"],
        endpoint=body["endpoint"],
        bucket=body["bucket"],
        expires_at_millis=int((time.time() + int(body["expires_in"])) * 1000),
    )


def api_key(source: Any) -> str:
    """The source's API key, its secret reference resolved. The Sources form keeps it in
    ``username``."""
    key = resolve_secrets(source.username) if source.username else ""
    if not key:
        raise AskAmericaKeyMissing(source.id)
    return key


def schemas(source: Any) -> list[str]:
    """The adapter schemas the source serves: its comma-separated ``database`` list."""
    return [s.strip().lower() for s in (source.database or "").split(",") if s.strip()]


def server_environment(
    source: Any, *, resolve: Callable[[str], StorageCredentials] | None = None
) -> dict[str, str]:
    """The environment the source's ``pgwire-govdata`` server is started with: the key (the
    server meters with it and renews its credentials from it), the credentials the key resolves
    to now, and where the data is. The bundle's model reads each of these by name."""
    key = api_key(source)
    creds = (resolve if resolve is not None else resolve_storage_credentials)(key)
    return {
        "ASKAMERICA_API_KEY": key,
        "GOVDATA_PARQUET_DIR": f"s3://{creds.bucket}",
        "AWS_ACCESS_KEY_ID": creds.access_key_id,
        "AWS_SECRET_ACCESS_KEY": creds.secret_access_key,
        "AWS_SESSION_TOKEN": creds.session_token,
        "AWS_ENDPOINT_OVERRIDE": creds.endpoint,
        "AWS_REGION": creds.region,
        "AWS_CREDENTIALS_EXPIRES_AT_MILLIS": str(creds.expires_at_millis),
    }


def narrow_model(bundle_model: dict, source: Any) -> dict:
    """The bundle's own model with only the schemas the source serves. A schema the source
    lists that the bundle does not carry is refused by name."""
    wanted = schemas(source)
    if not wanted:
        raise ValueError(f"AskAmerica source {source.id!r}: lists no schema to serve")
    by_name = {entry["name"]: entry for entry in bundle_model["schemas"]}
    unknown = [name for name in wanted if name not in by_name]
    if unknown:
        raise ValueError(
            f"AskAmerica source {source.id!r}: the adapter serves no schema named {unknown} "
            f"(it serves {sorted(by_name)})"
        )
    return {
        **bundle_model,
        "defaultSchema": wanted[0],
        "schemas": [by_name[name] for name in wanted],
    }
