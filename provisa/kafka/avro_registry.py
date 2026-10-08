# Copyright (c) 2026 Kenneth Stott
# Canary: c612160a-d27c-4fa5-afc5-4035ad7306f7
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Reading Avro messages written against a Confluent-compatible schema registry (REQ-1951).

A message in the registry's wire format is one byte ``0x00``, a four-byte big-endian schema id,
and one Avro datum written with the schema that id names. The reader asks the registry for the
schema by id and decodes the datum with it. An id names one schema for ever, so a schema is
fetched once and kept: a topic whose schema evolves simply carries messages with different ids,
each decoded with the schema that wrote it.

How the registry is reached is a setting of the source (``SourceCdcConfig``): one method --
``none``, ``basic``, ``bearer`` or ``mtls`` -- and the fields that method needs. The credentials
are references resolved through the secrets contract when the registry is called.

Nothing here reads an Avro message as anything else. A message in the wire format with no
registry to ask, an id the registry does not know, and a registry that cannot be reached are
each refused by name (``SchemaRegistryRefusal``)."""

from __future__ import annotations

import io
import logging
import struct
from dataclasses import dataclass
from pathlib import PurePath
from typing import Any

log = logging.getLogger(__name__)

# Requirements: REQ-1951

AUTH_NONE, AUTH_BASIC, AUTH_BEARER, AUTH_MTLS = "none", "basic", "bearer", "mtls"
AUTH_METHODS = (AUTH_NONE, AUTH_BASIC, AUTH_BEARER, AUTH_MTLS)

# The fields each method needs, beyond the registry's URL.
REQUIRED_FIELDS: dict[str, tuple[str, ...]] = {
    AUTH_NONE: (),
    AUTH_BASIC: ("schema_registry_username", "schema_registry_password"),
    AUTH_BEARER: ("schema_registry_token",),
    AUTH_MTLS: ("schema_registry_client_cert", "schema_registry_client_key"),
}
# Files the server process opens: named by absolute path, as a source's certificate paths are.
PATH_FIELDS = ("schema_registry_client_cert", "schema_registry_client_key", "schema_registry_ca")
# Credentials: kept in the org vault, the setting holding the reference (REQ-1695).
SECRET_FIELDS = ("schema_registry_password", "schema_registry_token")

_MAGIC = 0
_HEADER = struct.Struct(">bI")

# A registry answers a schema lookup from memory; these bound a registry that does not answer,
# so it cannot hold a subscription's start open. A request's own deadline shortens them.
CONNECT_SECONDS = 5.0
READ_SECONDS = 10.0


class RegistrySettingRefused(ValueError):
    """A source's registry settings do not say how to reach its registry. Raised when the source
    is saved; ``code`` and ``params`` name the refusal for the admin API."""

    def __init__(self, code: str, message: str, **params: Any) -> None:
        super().__init__(message)
        self.code = code
        self.params = params


class SchemaRegistryRefusal(Exception):
    """An Avro message cannot be read, and why. ``status`` is the HTTP answer a caller that has
    not yet opened a stream gives; ``code`` and ``params`` name the refusal."""

    def __init__(self, status: int, code: str, message: str, **params: Any) -> None:
        super().__init__(message)
        self.status = status
        self.code = code
        self.params = params


@dataclass(frozen=True)
class RegistrySettings:
    """How one source's schema registry is reached. Credentials are as the source holds them:
    references, resolved when the registry is called."""

    url: str
    auth: str = AUTH_NONE
    username: str | None = None
    password: str | None = None
    token: str | None = None
    client_cert: str | None = None
    client_key: str | None = None
    ca: str | None = None

    @classmethod
    def of(cls, cdc: Any) -> "RegistrySettings | None":
        """The registry settings of a source's CDC block (a ``SourceCdcConfig`` or the mapping
        built from one), or None when it names no registry."""
        get = cdc.get if isinstance(cdc, dict) else lambda name: getattr(cdc, name, None)
        url = get("schema_registry_url")
        if not url:
            return None
        return cls(
            url=url,
            auth=get("schema_registry_auth") or AUTH_NONE,
            username=get("schema_registry_username"),
            password=get("schema_registry_password"),
            token=get("schema_registry_token"),
            client_cert=get("schema_registry_client_cert"),
            client_key=get("schema_registry_client_key"),
            ca=get("schema_registry_ca"),
        )


def validate_settings(source_id: str, fields: dict[str, Any]) -> None:
    """Refuse, by name, registry settings that do not say how to reach the registry.

    ``fields`` are the CDC block's ``schema_registry_*`` values. The method must be one the
    product knows; a method other than ``none`` needs a registry to apply to; each field the
    method needs must be present; and each file is named by absolute path."""
    method = fields.get("schema_registry_auth") or AUTH_NONE
    if method not in AUTH_METHODS:
        raise RegistrySettingRefused(
            "schema.registry_auth_unknown",
            f"Source {source_id!r}: schema registry authentication {method!r} is not one of "
            f"{', '.join(AUTH_METHODS)}",
            source=source_id,
            method=method,
            methods=", ".join(AUTH_METHODS),
        )
    given = [name for name in (*SECRET_FIELDS, *PATH_FIELDS) if fields.get(name)]
    if not fields.get("schema_registry_url"):
        if method != AUTH_NONE or given or fields.get("schema_registry_username"):
            raise RegistrySettingRefused(
                "schema.registry_url_required",
                f"Source {source_id!r}: schema registry settings are given but no schema "
                "registry URL",
                source=source_id,
            )
        return
    for name in REQUIRED_FIELDS[method]:
        if not fields.get(name):
            raise RegistrySettingRefused(
                "schema.registry_field_required",
                f"Source {source_id!r}: schema registry authentication {method!r} requires {name}",
                source=source_id,
                method=method,
                field=name,
            )
    for name in PATH_FIELDS:
        path = fields.get(name)
        if path and not PurePath(path).is_absolute():
            raise RegistrySettingRefused(
                "schema.registry_path_not_absolute",
                f"Source {source_id!r}: {name} must be an absolute path, got {path!r}",
                source=source_id,
                field=name,
                path=path,
            )


def is_registry_framed(raw: bytes) -> bool:
    """Whether ``raw`` is in the registry's wire format. A JSON document never begins with a
    zero byte, so the first byte tells an Avro message from a JSON one."""
    return len(raw) > _HEADER.size and raw[0] == _MAGIC


def _resolved(value: str | None) -> str:
    from provisa.core.secrets import resolve_secrets

    return resolve_secrets(value) if value else ""


class SchemaRegistry:
    """One source's schema registry: schemas by id, kept once fetched."""

    def __init__(self, settings: RegistrySettings) -> None:
        self._settings = settings
        self._url = settings.url.rstrip("/")
        self._schemas: dict[int, Any] = {}
        self._client: Any = None

    def client_options(self) -> dict[str, Any]:
        """What the HTTP client is built with for this registry's method: the credentials each
        request carries, the certificate the client presents, and the authorities it trusts."""
        s = self._settings
        options: dict[str, Any] = {"headers": {"Accept": "application/vnd.schemaregistry.v1+json"}}
        if s.auth == AUTH_BASIC:
            options["auth"] = (_resolved(s.username), _resolved(s.password))
        elif s.auth == AUTH_BEARER:
            options["headers"]["Authorization"] = f"Bearer {_resolved(s.token)}"
        elif s.auth == AUTH_MTLS:
            options["cert"] = (s.client_cert, s.client_key)
        if s.ca:
            options["verify"] = s.ca
        return options

    def _timeout(self) -> Any:
        """This registry's own bounds, shortened to what the current request has left."""
        import httpx

        from provisa.core import request_deadline

        request_deadline.check()
        budget = request_deadline.remaining()
        connect, read = CONNECT_SECONDS, READ_SECONDS
        if budget is not None:
            connect, read = min(connect, budget), min(read, budget)
        return httpx.Timeout(read, connect=connect)

    async def _get(self, path: str) -> Any:
        import httpx

        if self._client is None:
            self._client = httpx.AsyncClient(**self.client_options())
        try:
            return await self._client.get(f"{self._url}{path}", timeout=self._timeout())
        except (httpx.HTTPError, OSError) as exc:
            raise SchemaRegistryRefusal(
                503,
                "subscribe.schema_registry_unreachable",
                f"Schema registry {self._url} cannot be reached: {type(exc).__name__}: {exc}",
                registry=self._url,
                error=f"{type(exc).__name__}: {exc}",
            ) from exc

    def _refuse_unless_answered(self, response: Any) -> None:
        if response.status_code in (401, 403):
            raise SchemaRegistryRefusal(
                502,
                "subscribe.schema_registry_refused_credentials",
                f"Schema registry {self._url} refused the source's credentials "
                f"(HTTP {response.status_code}, authentication {self._settings.auth!r})",
                registry=self._url,
                http_status=response.status_code,
                method=self._settings.auth,
            )
        if response.status_code >= 400:
            raise SchemaRegistryRefusal(
                502,
                "subscribe.schema_registry_error",
                f"Schema registry {self._url} answered HTTP {response.status_code}: "
                f"{response.text[:200]}",
                registry=self._url,
                http_status=response.status_code,
                error=response.text[:200],
            )

    async def reach(self) -> None:
        """Ask the registry anything, so a registry that is down, hangs, or refuses the source's
        credentials is refused by name when a subscription starts, not at its first message."""
        self._refuse_unless_answered(await self._get("/subjects"))

    async def schema(self, schema_id: int) -> Any:
        """The schema the registry holds under ``schema_id``, parsed."""
        known = self._schemas.get(schema_id)
        if known is not None:
            return known
        import json

        import fastavro

        response = await self._get(f"/schemas/ids/{schema_id}")
        if response.status_code == 404:
            raise SchemaRegistryRefusal(
                502,
                "subscribe.schema_id_unknown",
                f"Schema registry {self._url} holds no schema with id {schema_id}: the message "
                "was written against another registry",
                registry=self._url,
                schema_id=schema_id,
            )
        self._refuse_unless_answered(response)
        body = response.json()
        kind = body.get("schemaType", "AVRO")
        if kind != "AVRO":
            raise SchemaRegistryRefusal(
                502,
                "subscribe.schema_not_avro",
                f"Schema {schema_id} in registry {self._url} is {kind}, not Avro",
                registry=self._url,
                schema_id=schema_id,
                kind=kind,
            )
        parsed = fastavro.parse_schema(json.loads(body["schema"]))
        self._schemas[schema_id] = parsed
        return parsed

    async def decode(self, raw: bytes) -> Any:
        """The value of one message in the registry's wire format."""
        import fastavro

        _magic, schema_id = _HEADER.unpack_from(raw)
        schema = await self.schema(schema_id)
        return fastavro.schemaless_reader(io.BytesIO(raw[_HEADER.size :]), schema)

    async def close(self) -> None:
        client, self._client = self._client, None
        if client is not None:
            await client.aclose()


def refuse_avro_without_registry(topic: str) -> SchemaRegistryRefusal:
    """The refusal for a message in the registry's wire format on a source that names no
    registry: it is never read as JSON."""
    return SchemaRegistryRefusal(
        422,
        "subscribe.avro_topic_without_registry",
        f"Topic {topic!r} carries Avro messages, and its source names no schema registry to "
        "read them with: set the source's schema registry URL",
        topic=topic,
    )
