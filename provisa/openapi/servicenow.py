# Copyright (c) 2026 Kenneth Stott
# Canary: d280573a-ab50-488c-bdc7-9b063e144637
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A ServiceNow instance's own REST APIs, read from what the instance publishes (REQ-1954).

- ``GET /api/now/doc``: the instance's list of its APIs, by namespace: each one's name, route and
  versions, with the HTTP method and route of each of its resources.
- ``GET /api/now/doc/oas_3?namespace=&name=&format=json&version=``: one API's OpenAPI 3
  document, the one the instance's REST API Explorer exports. It declares operations and their
  parameters; its request bodies have no schema (see ``openapi/mapper``: such a body is one
  untyped ``body``).

Both are read with the source's own credential, resolved at use. Nothing here is hand-written:
the APIs offered and their operations are the instance's.
"""

# Requirements: REQ-1954

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import httpx

from provisa.core.secrets import resolve_secrets

#: How long one read of the instance's API list or of one API's document may take.
_TIMEOUT = 60.0


@dataclass(frozen=True)
class InstanceApi:
    """One API the instance publishes."""

    namespace: str
    name: str  # its display name, which the document export is asked for by
    route: str  # "now/import"
    description: str
    versions: tuple[str, ...]
    scripted: bool  # written on the instance (a Scripted REST API), not one of the platform's


class InstanceApiError(Exception):
    """The instance did not answer a read of its APIs: names the call and what it answered."""


def _client(instance_url: str, username: str, password: str) -> httpx.Client:
    return httpx.Client(
        base_url=resolve_secrets(instance_url).rstrip("/"),
        auth=(resolve_secrets(username), resolve_secrets(password)),
        headers={"Accept": "application/json"},
        timeout=_TIMEOUT,
    )


def _get(
    client: httpx.Client,
    path: str,
    params: dict[str, str] | None = None,
    headers: dict[str, str] | None = None,
) -> Any:
    resp = client.get(path, params=params, headers=headers)
    if resp.is_error:
        raise InstanceApiError(f"{path}: HTTP {resp.status_code}: {resp.text[:300]}")
    return resp.json()


def instance_apis(catalog: dict) -> list[InstanceApi]:
    """The APIs of an ``/api/now/doc`` answer, by namespace and name."""
    apis = []
    for namespace, by_name in sorted(catalog["result"].items()):
        for name, api in sorted(by_name.items()):
            apis.append(
                InstanceApi(
                    namespace=namespace,
                    name=name,
                    route=api["route"],
                    description=api.get("description") or "",
                    versions=tuple(api.get("versions") or ()),
                    scripted=bool(api.get("scripted")),
                )
            )
    return apis


def list_instance_apis(instance_url: str, username: str, password: str) -> list[InstanceApi]:
    """Every API the instance publishes (``/api/now/doc``)."""
    with _client(instance_url, username, password) as client:
        return instance_apis(_get(client, "/api/now/doc"))


def fetch_api_spec(
    instance_url: str,
    username: str,
    password: str,
    namespace: str,
    name: str,
    version: str = "latest",
) -> dict:
    """The OpenAPI 3 document the instance exports for one of its APIs."""
    with _client(instance_url, username, password) as client:
        spec = _get(
            client,
            "/api/now/doc/oas_3",
            {"namespace": namespace, "name": name, "format": "json", "version": version},
            # The export is a download: asked for as application/json, the instance answers
            # 406 Not Acceptable. Its body is the JSON document ``format`` names.
            headers={"Accept": "*/*"},
        )
    if "error" in spec or "paths" not in spec:
        raise InstanceApiError(f"{namespace}/{name}: the instance exported no document: {spec}")
    return spec


def merged_spec(title: str, instance_url: str, specs: list[dict]) -> dict:
    """One OpenAPI document holding the operations of every one of ``specs``, addressed at the
    instance: the document an OpenAPI source reads its tables and commands from. Two of them
    declaring the same operation is refused by name: a path is one API's."""
    paths: dict[str, Any] = {}
    for spec in specs:
        for path, item in spec["paths"].items():
            if path in paths:
                raise InstanceApiError(f"{path} is declared by two of the instance's APIs")
            paths[path] = item
    return {
        "openapi": "3.0.1",
        "info": {"title": title, "version": "latest"},
        "servers": [{"url": resolve_secrets(instance_url).rstrip("/")}],
        "paths": paths,
    }


# -- the companion OpenAPI source -------------------------------------------------------------
#
# A ServiceNow source's own APIs are offered through an OpenAPI source kept beside it,
# ``<id>-api``, whose specification is ``servicenow:<id>``: the document assembled here from the
# APIs a steward added, read from the instance when the source is loaded. Everything after that is
# the ordinary OpenAPI path: its GET operations are tables or reads, the rest commands.

#: How a companion source's row names its specification (``sources.path``).
SPEC_SCHEME = "servicenow:"

#: Where a companion source's mapping keeps what its document is assembled from.
MAPPING_KEY = "servicenow"

# An import set's staging columns as JSON Schema, by the PostgreSQL type the adapter names
# (``_servicenow_columns``).
_JSON_TYPES = {
    "character varying": {"type": "string"},
    "integer": {"type": "integer"},
    "bigint": {"type": "integer"},
    "numeric": {"type": "number"},
    "double precision": {"type": "number"},
    "boolean": {"type": "boolean"},
    "timestamp": {"type": "string", "format": "date-time"},
    "date": {"type": "string", "format": "date"},
    "time": {"type": "string", "format": "time"},
}

# The staging columns every import set has, which a caller never writes.
_SYSTEM_PREFIX = "sys_"


def companion_id(source_id: str) -> str:
    """The id of the OpenAPI source kept beside ServiceNow source ``source_id``."""
    return f"{source_id}-api"


def companion_spec_path(source_id: str) -> str:
    return f"{SPEC_SCHEME}{source_id}"


def import_set_path(staging: str) -> dict:
    """The operation that writes one row into import set ``staging``, typed from its columns."""
    return {
        "post": {
            "operationId": f"import_{staging}",
            "summary": f"Insert a row into import set {staging}; its transform map writes it on",
        }
    }


def _import_set_operation(staging: str, columns: list[list[str]]) -> dict:
    properties = {}
    for name, data_type in columns:
        if name.startswith(_SYSTEM_PREFIX):
            continue
        if data_type not in _JSON_TYPES:
            raise InstanceApiError(
                f"import set {staging}: column {name} has a type no JSON type is given for: "
                f"{data_type}"
            )
        properties[name] = _JSON_TYPES[data_type]
    operation = import_set_path(staging)
    operation["post"]["requestBody"] = {
        "content": {"application/json": {"schema": {"type": "object", "properties": properties}}}
    }
    return operation


def companion_spec(mapping: dict) -> dict:
    """The document a companion source reads its tables and commands from: the instance's
    document of each API its steward added, and one typed operation for each import set added
    (``POST /api/now/import/<staging>``, its body the staging table's columns)."""
    held = mapping[MAPPING_KEY]
    url, user, password = held["instance_url"], held["username"], held["password"]
    specs = [
        fetch_api_spec(url, user, password, api["namespace"], api["name"], api["version"])
        for api in held["apis"]
    ]
    spec = merged_spec(f"{held['source']} APIs", url, specs)
    for import_set in held["import_sets"]:
        staging = import_set["staging"]
        path = f"/api/now/import/{staging}"
        if path in spec["paths"]:
            raise InstanceApiError(f"{path} is declared twice")
        spec["paths"][path] = _import_set_operation(staging, import_set["columns"])
    return spec
