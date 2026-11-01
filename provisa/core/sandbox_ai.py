# Copyright (c) 2026 Kenneth Stott
# Canary: 5aedb12b-fceb-4c99-893e-36b66f5f8b68
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The AI models a demo sandbox's visitor uses (REQ-2006).

The sandbox tier's operator names one OpenAI-wire endpoint (``sandbox.ai_base_url``, e.g.
OpenRouter's), the model (``sandbox.ai_model``) and the key (``sandbox.ai_api_key``: a
``${secret:NAME}`` reference). Every sandbox-tier org -- a demo's template and each sandbox of it
-- then resolves its configuration with that endpoint and that model for every completion role:
Polly's chat and each AI-model role (``AI_ROLES``). Its own organization settings still layer on
top, as any org's do. No embedding model is named: a sandbox has none, and what needs one
(catalog search) is unavailable there.

The key is the deployment's, held once in the deployment org's vault. It is resolved there --
not in the sandbox's own vault, which holds nothing -- for this endpoint only: one whose id,
base URL and key reference are the setting's own (``key_org``). An endpoint a visitor writes
into their sandbox's settings is resolved in the sandbox's own vault, as any tenant's field is.
"""

# Requirements: REQ-2006

from __future__ import annotations

from typing import Any

#: The id the tier's endpoint goes by in a sandbox's ``ai_endpoints``.
ENDPOINT_ID = "provisa-sandbox-ai"

#: Every completion role the product assigns a model to (api/admin/ai_models_router.py).
AI_ROLES = (
    "table_description",
    "column_description",
    "relationship_inference",
    "sql_generation",
    "table_selection",
    "mcp_chat",
    "glossary_term_finding",
)


def _setting(key: str) -> str:
    from provisa.core import settings_registry

    return str(settings_registry.value(key) or "").strip()


def enabled() -> bool:
    """Whether the operator has named both the endpoint and its key."""
    return bool(_setting("sandbox.ai_base_url") and _setting("sandbox.ai_api_key"))


def _endpoint() -> dict[str, Any]:
    return {
        "id": ENDPOINT_ID,
        "style": "openai",
        "base_url": _setting("sandbox.ai_base_url"),
        "api_key_env": _setting("sandbox.ai_api_key"),
        "enabled": True,
    }


def defaults_for(org_id: str | None) -> dict[str, Any]:
    """The configuration a sandbox-tier org's own settings layer on: the tier's endpoint and model
    for every completion role. None of it for any other org, or when the operator has not set
    the endpoint and key."""
    from provisa.core.org_ids import is_sandbox_tier_org

    if org_id is None or not is_sandbox_tier_org(org_id) or not enabled():
        return {}
    model = _setting("sandbox.ai_model")
    return {
        "ai_endpoints": [_endpoint()],
        "ai_models": {role: {"vendor": ENDPOINT_ID, "model": model} for role in AI_ROLES},
    }


def key_org(endpoint: dict[str, Any], org_id: str | None) -> str | None:
    """The org whose vault ``endpoint``'s key is resolved in, when it is not the acting org's: the
    deployment org, for the tier's own endpoint as the setting names it, in a sandbox-tier org.
    None otherwise -- the key is resolved as any tenant's field is."""
    from provisa.core import settings_registry
    from provisa.core.org_ids import is_sandbox_tier_org

    if org_id is None or not is_sandbox_tier_org(org_id) or not enabled():
        return None
    ours = _endpoint()
    if any(endpoint.get(k) != ours[k] for k in ("id", "base_url", "api_key_env")):
        return None
    return str(settings_registry.value("control_plane.org_id"))


async def endpoint_key(endpoint: dict[str, Any], org_id: str | None) -> str | None:
    """``endpoint``'s key as ``org_id`` uses it: the tier's own endpoint's from the deployment org's
    vault (``key_org``), any other from the acting org's, as provisa.core.secrets resolves every
    credential field."""
    from provisa.core.request_context import current_org
    from provisa.core.secrets import resolve_api_key_field

    owner = key_org(endpoint, org_id)
    if owner is None:
        return await resolve_api_key_field(endpoint.get("api_key_env"))
    token = current_org.set(owner)
    try:
        return await resolve_api_key_field(endpoint["api_key_env"])
    finally:
        current_org.reset(token)


async def vendor_keys(org_id: str | None) -> dict[str, str]:
    """The key of the tier's endpoint, by its id, for a sandbox-tier org that uses it -- what the
    org's own vendor keys (provisa.core.org_secrets.read_org_api_keys) carry alongside theirs, so
    a role's client finds it as it finds any vendor's. Empty for any other org."""
    if not defaults_for(org_id):
        return {}
    key = await endpoint_key(_endpoint(), org_id)
    if not key:
        raise ValueError(
            f"sandbox.ai_api_key {_setting('sandbox.ai_api_key')!r} resolved to nothing in the "
            "deployment org's vault"
        )
    return {ENDPOINT_ID: key}


def pin_model(cfg: dict[str, Any], org_id: str | None) -> dict[str, Any]:
    """``cfg`` with every role that names the tier's endpoint on the tier's model: the endpoint and
    its key are the deployment's, and nothing meters a visitor's use of them, so a sandbox's own
    settings choose whether a role uses the endpoint, never which model it runs there. A role on
    an endpoint the visitor configured keeps the model they chose."""
    if not defaults_for(org_id):
        return cfg
    model = _setting("sandbox.ai_model")
    roles = cfg.get("ai_models") or {}
    pinned = {
        role: (
            {**choice, "model": model}
            if isinstance(choice, dict) and choice.get("vendor") == ENDPOINT_ID
            else choice
        )
        for role, choice in roles.items()
    }
    return {**cfg, "ai_models": pinned}
