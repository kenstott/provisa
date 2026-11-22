# Copyright (c) 2026 Kenneth Stott
# Canary: 7239de29-8635-4ae8-b706-ab6afe19acc7
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Load an OpenAPI spec from a local file or remote URL."""

from __future__ import annotations

from provisa.core import org_files
from provisa.security import sandbox_egress  # REQ-2006
import json
import pathlib
from typing import Any

import yaml

# Requirements: REQ-314, REQ-315, REQ-2265


def parse_text(content: str) -> dict:  # REQ-315, REQ-407
    """Parse an inline YAML or JSON spec string.

    Tries YAML first, falls back to JSON.
    """
    try:
        return yaml.safe_load(content)
    except Exception:
        return json.loads(content)


def _fetch(spec_path: str, auth: Any) -> str:  # REQ-2265
    """The text at ``spec_path`` (http/https), requested with ``auth`` applied exactly as the
    source's data calls apply it (``api_source.caller._apply_auth``): the same header or query
    parameter, its secret references resolved at the call. A refusal names the status and the
    address without the credential, and carries no chained error that could."""
    import httpx
    from pydantic import TypeAdapter

    from provisa.api_source.caller import _apply_auth
    from provisa.core.auth_models import ApiAuth

    headers: dict[str, str] = {}
    params: dict[str, str] = {}
    _apply_auth(
        None if auth is None else TypeAdapter(ApiAuth).validate_python(auth), headers, params
    )
    r = sandbox_egress.checked_get(spec_path, timeout=30, headers=headers, params=params)
    if r.is_success:
        return r.text
    shown = httpx.Request("GET", spec_path)
    raise httpx.HTTPStatusError(
        f"{r.status_code} {r.reason_phrase} reading the OpenAPI spec at {spec_path}",
        request=shown,
        response=httpx.Response(r.status_code, request=shown),
    )


def load_spec(spec_path: str, auth: Any = None) -> dict:  # REQ-314, REQ-315, REQ-2265
    """Load OpenAPI spec from a local file path or remote URL (http/https), or the spec a
    branded source ships with (``brand:<id>``, REQ-1923).

    ``auth``: the source's auth (core.auth_models.ApiAuth, or its stored shape as
    ``api_source.openapi_endpoint.api_auth`` writes it); a remote spec is requested with it as
    the source's data calls are (REQ-2265). None for a source without one. A file or a shipped
    spec is read without it.

    Raises FileNotFoundError for missing local paths.
    Raises httpx.HTTPError for HTTP failures.
    Raises ValueError for parse errors.
    """
    from provisa.openapi.brands import brand_spec, spec_brand

    brand = spec_brand(spec_path)
    if brand is not None:
        return brand_spec(brand)
    from provisa.core.source_uploads import on_own_disk, require_own_disk

    # REQ-1997: what the location is decides how it is read, never whether a file of that name
    # happens to exist; a held org's specification on the server's own disk is in its own folder.
    if not on_own_disk(spec_path) and spec_path.startswith(("http://", "https://")):
        text = _fetch(spec_path, auth)
    else:
        require_own_disk(spec_path)
        p = pathlib.Path(org_files.resolve_path(spec_path))
        if not p.exists():
            raise FileNotFoundError(f"Spec not found: {spec_path!r}")
        text = p.read_text()

    if spec_path.endswith(".yaml") or spec_path.endswith(".yml"):
        return yaml.safe_load(text)
    if spec_path.endswith(".json"):
        return json.loads(text)
    # Heuristic: try YAML first, fall back to JSON
    try:
        return yaml.safe_load(text)
    except Exception:
        return json.loads(text)


def spec_server_url(spec: dict, spec_path: str) -> str:  # REQ-2265
    """Where a spec's calls go: its first server's URL. The OpenAPI specification reads a relative
    server URL against the address the spec was read from, so a spec read over http(s) that
    names its server as a path (``/openapi/``, as DataHub's GMS does) is called at that path on
    the spec's own host. Empty where the spec names no server."""
    from urllib.parse import urljoin

    servers = spec.get("servers") or []
    url = servers[0].get("url", "") if servers else ""
    if url and "://" not in url and spec_path.startswith(("http://", "https://")):
        url = urljoin(spec_path, url)
    return url.rstrip("/") if "://" in url else url
