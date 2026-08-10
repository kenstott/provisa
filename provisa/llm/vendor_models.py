# Copyright (c) 2026 Kenneth Stott
# Canary: 7c3b91de-45a8-4f27-9b16-0d8ea2c47f51
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Vendor model catalogs, read live from each vendor's list-models API.

The admin AI-models surface offers the model names a vendor actually serves rather than a list
baked into the bundle, so a model released after the build is selectable the day it ships. Every
vendor here publishes a list-models endpoint; the vendors aisuite supports that do not (the local
endpoints ollama/lmstudio, and the vendors whose auth needs more than an api_key) are absent, and
asking for one is an error rather than an empty list — an empty catalog would read as "this vendor
has no models".
"""

# Requirements: REQ-1395, REQ-1398, REQ-1409

from __future__ import annotations

from typing import Any, Literal, NamedTuple


class _VendorApi(NamedTuple):
    url: str
    auth: Literal["bearer", "x-api-key"]


# The env var each vendor's key is read from when the org has not set its own — the same name
# aisuite resolves, so the deployment default the UI advertises ("NO KEY SET — USING DEPLOYMENT
# DEFAULT") is the key this listing uses too.
VENDOR_API_KEY_ENV = {vendor: f"{vendor.upper()}_API_KEY" for vendor in ("anthropic", "openai")}

VENDOR_MODEL_APIS: dict[str, _VendorApi] = {
    "anthropic": _VendorApi("https://api.anthropic.com/v1/models", "x-api-key"),
    "openai": _VendorApi("https://api.openai.com/v1/models", "bearer"),
    "cohere": _VendorApi("https://api.cohere.com/v1/models", "bearer"),
    "groq": _VendorApi("https://api.groq.com/openai/v1/models", "bearer"),
    "mistral": _VendorApi("https://api.mistral.ai/v1/models", "bearer"),
    "xai": _VendorApi("https://api.x.ai/v1/models", "bearer"),
    "deepseek": _VendorApi("https://api.deepseek.com/models", "bearer"),
    "together": _VendorApi("https://api.together.xyz/v1/models", "bearer"),
    "fireworks": _VendorApi("https://api.fireworks.ai/inference/v1/models", "bearer"),
    "nebius": _VendorApi("https://api.studio.nebius.com/v1/models", "bearer"),
    "sambanova": _VendorApi("https://api.sambanova.ai/v1/models", "bearer"),
    "inception": _VendorApi("https://api.inceptionlabs.ai/v1/models", "bearer"),
}


def _headers(api: _VendorApi, api_key: str) -> dict[str, str]:
    if api.auth == "x-api-key":
        # Anthropic authenticates with x-api-key and requires the version header on every call.
        return {"x-api-key": api_key, "anthropic-version": "2023-06-01"}
    return {"Authorization": f"Bearer {api_key}"}


def parse_model_ids(payload: Any) -> list[str]:
    """The model identifiers in a list-models response.

    Three shapes are in play across these vendors: OpenAI-style ``{"data": [{"id": ...}]}``,
    Cohere's ``{"models": [{"name": ...}]}``, and a bare list of either. The shape is read from
    the payload rather than recorded per vendor, so a vendor that switches between them keeps
    working; an unrecognized shape raises instead of yielding an empty catalog.
    """
    if isinstance(payload, dict):
        for key in ("data", "models"):
            if key in payload:
                payload = payload[key]
                break
        else:
            raise ValueError(f"list-models response has no 'data' or 'models' key: {payload!r}")
    if not isinstance(payload, list):
        raise ValueError(f"list-models response is not a list: {payload!r}")
    ids: list[str] = []
    for item in payload:
        if isinstance(item, str):
            ids.append(item)
            continue
        if not isinstance(item, dict):
            raise ValueError(f"list-models entry is neither a string nor an object: {item!r}")
        name = item.get("id") if "id" in item else item.get("name")
        if not isinstance(name, str):
            raise ValueError(f"list-models entry has no id/name: {item!r}")
        ids.append(name)
    return sorted(ids)


async def fetch_vendor_models(vendor: str, api_key: str, *, timeout: float = 15.0) -> list[str]:
    """Every model name ``vendor`` serves for ``api_key``, sorted.

    Raises ``KeyError`` for a vendor with no list-models API, and ``httpx.HTTPStatusError`` when
    the vendor rejects the key — the caller turns both into the admin surface's own error.
    """
    import httpx

    api = VENDOR_MODEL_APIS[vendor]
    async with httpx.AsyncClient(timeout=timeout) as client:
        resp = await client.get(api.url, headers=_headers(api, api_key))
    resp.raise_for_status()
    return parse_model_ids(resp.json())
