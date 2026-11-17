# Copyright (c) 2026 Kenneth Stott
# Canary: 8b41d0c7-6e25-4a93-9f18-3c07b5d2ae64
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Per-org secrets, encrypted at rest (REQ-1395, REQ-1398).

Distinct from :mod:`provisa.core.org_settings`, whose ``org_settings.value`` column is
unencrypted JSON — a raw credential (e.g. an org's own LLM vendor API key) must never land
there. This module stores each secret as an encrypted blob via the process-wide
:func:`provisa.encryption.runtime.encryption_service`, the same pattern already used for
``api_sources.auth`` (REQ-686).

Values are write-only from the caller's perspective in practice: callers that read a secret
use it immediately (e.g. to construct an LLM client) and must not echo it back in an API
response.

REQ-1580: what is stored here may be the credential itself or a REFERENCE to one held in the
deployment's secrets service (``${secret:NAME}``). The reference is stored verbatim and resolved
on the way OUT, so an org that keeps every credential in one vault does not have to keep a second
copy of its vendor key here, and rotating the vault entry rotates what the LLM client is handed.
"""

# Requirements: REQ-1395, REQ-1398, REQ-1580

from __future__ import annotations

import os
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from provisa.core.db import Database

# REQ-1398: aisuite vendors whose provider takes a plain `api_key` string (config `{vendor:
# {"api_key": ...}}`) and nothing else — excludes vendors needing extra fields the org-secret
# model doesn't carry (google: project/region/creds path; aws: region/IAM; azure/watsonx: a
# base_url/service_url alongside the key) and local-endpoint vendors with no key at all
# (ollama, lmstudio).
LLM_VENDORS: frozenset[str] = frozenset(
    {
        "anthropic",
        "openai",
        "cohere",
        "groq",
        "mistral",
        "xai",
        "deepseek",
        "together",
        "fireworks",
        "nebius",
        "sambanova",
        "inception",
    }
)

# Jev (TypeSafe AI) is not an aisuite chat vendor — it takes no model assignment, just a
# credential — so it is kept out of LLM_VENDORS and handled as its own named secret.
JEV_SECRET_KEY = "jev_api_key"

#: REQ-2098: the certificate and key the organisation presents to its code sources' remote hosts
#: (provisa/codesource/remote_sources.py), made once and never shown. Not one an administrator
#: sets (ORG_SECRET_KEYS), and not a vendor's key (read_org_api_keys).
CODEHOST_IDENTITY_KEY = "codehost_identity"

# The only secrets an org administrator may set through this door.
ORG_SECRET_KEYS: frozenset[str] = frozenset(
    f"{vendor}_api_key" for vendor in LLM_VENDORS
) | frozenset({JEV_SECRET_KEY})


async def _resolved(value: str) -> str:
    """The stored value with any ``${secret:NAME}`` resolved against the org's vault (REQ-1580).

    A stored credential contains no ``${``, so the common case never touches the vault at all;
    the check is the reference grammar deciding whether this string IS a reference, not a guard
    around a value that might be missing. A reference that names nothing raises out of
    ``resolve_secrets`` -- the LLM call fails saying which name is unset, rather than sending the
    reference text to the vendor as a key and reading back "invalid x-api-key".
    """
    if "${" not in value:
        return value
    from provisa.core.secrets import resolve_secrets
    from provisa.core.secrets_store import bound_to_request_org

    async with bound_to_request_org():
        return resolve_secrets(value)


async def read_org_secret(model_db: Database, key: str) -> str | None:
    """The org's decrypted secret value, or ``None`` if unset."""
    from sqlalchemy import select

    from provisa.core.schema_org import org_secrets
    from provisa.encryption.runtime import encryption_service

    async with model_db.acquire() as conn:
        result = await conn.execute_core(
            select(org_secrets.c.value_enc).where(org_secrets.c.key == key)
        )
        row = result.fetchone()
    if row is None:
        return None
    return await _resolved(encryption_service().decrypt(bytes(row[0])).decode("utf-8"))


async def read_org_api_keys(model_db: Database) -> dict[str, str]:
    """Every configured `{vendor}_api_key` for the org, keyed by vendor name."""
    from sqlalchemy import select

    from provisa.core.schema_org import org_secrets
    from provisa.encryption.runtime import encryption_service

    async with model_db.acquire() as conn:
        result = await conn.execute_core(
            select(org_secrets.c.key, org_secrets.c.value_enc).where(
                org_secrets.c.key.in_(ORG_SECRET_KEYS)
            )
        )
        rows = result.fetchall()
    service = encryption_service()
    from provisa.core.request_context import current_org
    from provisa.core.sandbox_ai import vendor_keys

    return {
        # REQ-2006: a sandbox-tier org's include the tier's AI endpoint's key, by its id.
        **await vendor_keys(current_org.get()),
        **{
            row[0].removesuffix("_api_key"): await _resolved(
                service.decrypt(bytes(row[1])).decode("utf-8")
            )
            for row in rows
        },
    }


async def write_org_secret(
    model_db: Database, key: str, value: str | None, *, updated_by: str
) -> None:
    """Upsert the encrypted secret; a ``None`` value DELETES the row.

    Raises ValueError on a key outside :data:`ORG_SECRET_KEYS`.
    """
    if key not in ORG_SECRET_KEYS:
        raise ValueError(f"not an org secret key: {key}")

    from datetime import datetime, timezone

    from sqlalchemy import delete as _delete

    from provisa.core.schema_org import org_secrets as _org_secrets_t
    from provisa.encryption.runtime import encryption_service

    async with model_db.acquire() as conn:
        if value is None:
            await conn.execute_core(_delete(_org_secrets_t).where(_org_secrets_t.c.key == key))
            return
        # REQ-1557: checked before it is encrypted, where the write seam cannot read it.
        from provisa.core.secrets_providers import check_stored

        check_stored(value)
        value_enc = encryption_service().encrypt(value.encode("utf-8"))
        await conn.upsert(
            _org_secrets_t,
            {
                "key": key,
                "value_enc": value_enc,
                "updated_at": datetime.now(timezone.utc),
                "updated_by": updated_by,
            },
            index_elements=["key"],
            update_columns=["value_enc", "updated_at", "updated_by"],
        )


#: The name the Jev key is resolved by, beside the model vendors' (it is no chat vendor).
JEV = "jev"

#: The environment variable an install of one organization reads each AI key from when the
#: organization stored none: aisuite's own name for each key-taking vendor, and Jev's. Read
#: here and nowhere else (:func:`ai_key_or_none`).
AI_KEY_ENV: dict[str, str] = {
    "anthropic": "ANTHROPIC_API_KEY",
    "openai": "OPENAI_API_KEY",
    "cohere": "CO_API_KEY",
    "groq": "GROQ_API_KEY",
    "mistral": "MISTRAL_API_KEY",
    "xai": "XAI_API_KEY",
    "deepseek": "DEEPSEEK_API_KEY",
    "together": "TOGETHER_API_KEY",
    "fireworks": "FIREWORKS_API_KEY",
    "nebius": "NEBIUS_API_KEY",
    "sambanova": "SAMBANOVA_API_KEY",
    "inception": "INCEPTION_API_KEY",
    JEV: "TYPESAFEAI_API_KEY",
}

#: aisuite's vendors that take no credential at all: a local server.
KEYLESS_VENDORS: frozenset[str] = frozenset({"ollama", "lmstudio"})

#: Where an organization adds an AI key, as the refusals say it.
WHERE_TO_ADD_AI_KEY = "Admin, AI Models"


class AiKeyRequired(Exception):
    """An AI call was asked with no key to make it with. ``code`` and ``params`` are what the
    caller is answered with (HTTP 409, ``provisa.api.app``); nothing was sent to the vendor."""

    def __init__(self, vendor: str, code: str, message: str, **params: str) -> None:
        super().__init__(message)
        self.vendor = vendor
        self.code = code
        self.params = {"vendor": vendor, **params}


def _no_key(vendor: str) -> AiKeyRequired:
    return AiKeyRequired(
        vendor,
        "ai.key_required",
        f"{vendor} has no API key. Add one under {WHERE_TO_ADD_AI_KEY}.",
    )


def ai_key_or_none(stored: Mapping[str, str], vendor: str) -> str | None:
    """The key ``vendor`` (a model vendor, or :data:`JEV`) is called with, given the keys the
    organization stored (:func:`read_org_api_keys`): the stored one; None when there is none to
    use. Whether a vendor is shown as having a key and whether a call to it is made are both
    this answer, so the two never differ."""
    key = stored.get(vendor)
    if key:
        return key
    from provisa.core.secrets_providers import many_organisations

    if many_organisations():
        return None
    # REQ-1395 as amended 2026-11-16 requires this read: an install of one organization keeps
    # using the deployment's environment variable when the organization stored no key. On a
    # server of many organizations it is never read.
    variable = AI_KEY_ENV.get(vendor)
    return (os.environ.get(variable) or None) if variable else None


def ai_key(stored: Mapping[str, str], vendor: str) -> str:
    """The key ``vendor`` is called with (:func:`ai_key_or_none`); refused when there is none."""
    key = ai_key_or_none(stored, vendor)
    if key is None:
        raise _no_key(vendor)
    return key


def provider_key(stored: Mapping[str, str], vendor: str) -> str | None:
    """The ``api_key`` one of aisuite's named vendors is constructed with. On an install of one
    organization, None for a vendor that takes none from here: a local server
    (:data:`KEYLESS_VENDORS`), and a vendor reached with the deployment's own credentials (Vertex
    AI, Bedrock and the like), which aisuite reads itself. On a server of many organizations the
    local server and those credentials are the server's, so such a vendor is refused."""
    if vendor in AI_KEY_ENV:
        return ai_key(stored, vendor)
    key = stored.get(vendor)
    if key:
        return key
    from provisa.core.secrets_providers import many_organisations

    if not many_organisations():
        return None
    if vendor in KEYLESS_VENDORS:
        # REQ-1395 as amended 2026-11-16: aisuite reaches a local server at the address the
        # server's environment names (OLLAMA_API_URL, LMSTUDIO_API_URL) or the server's own
        # localhost; an organization reaches its own through an AI endpoint it declares.
        raise AiKeyRequired(
            vendor,
            "ai.local_server_from_environment",
            f"{vendor} is reached at the server's own address, which an organization on this "
            f"server does not use. Declare an AI endpoint at your own address under "
            f"{WHERE_TO_ADD_AI_KEY}.",
        )
    raise AiKeyRequired(
        vendor,
        "ai.vendor_uses_server_credentials",
        f"{vendor} is reached with the server's own credentials, which an organization on this "
        f"server does not use. Choose a vendor that takes a key, or an AI endpoint, under "
        f"{WHERE_TO_ADD_AI_KEY}.",
    )


#: What an endpoint that checks no key is constructed with: no credential, and it keeps the
#: vendor's SDK from reading one out of the environment.
NO_KEY = "not-required"


def endpoint_provider_key(key: str | None) -> str | None:
    """The ``api_key`` an AI endpoint's provider is constructed with: its own key. With none, on
    a server of many organizations, :data:`NO_KEY`: a vendor's SDK constructed with no key reads
    the vendor's from the environment, and would send the server's to the endpoint's address."""
    from provisa.core.secrets_providers import many_organisations

    if key or not many_organisations():
        return key
    return NO_KEY


async def _stored_ai_keys(model_db: "Database | None") -> dict[str, str]:
    """The organization's stored AI keys; none where no model store is bound."""
    return await read_org_api_keys(model_db) if model_db is not None else {}


async def org_ai_key_or_none(model_db: "Database | None", vendor: str) -> str | None:
    """:func:`ai_key_or_none` for the organization whose model store this is."""
    return ai_key_or_none(await _stored_ai_keys(model_db), vendor)


async def org_ai_key(model_db: "Database | None", vendor: str) -> str:
    """:func:`ai_key` for the organization whose model store this is."""
    return ai_key(await _stored_ai_keys(model_db), vendor)


# --- AI endpoints (REQ-1790) ---------------------------------------------------------------------


def _many() -> bool:
    from provisa.core.secrets_providers import many_organisations

    return many_organisations()


def _key_field_reads_environment(field: str) -> bool:
    """Whether an endpoint's ``api_key_env`` is read from the server's environment: a bare
    variable name (provisa.core.secrets.resolve_api_key_field), or an ``${env:...}`` reference."""
    return "${" not in field or "${env:" in field


def _refuse_endpoint_key_from_environment(endpoint: Mapping[str, object], field: str) -> None:
    """REQ-1395 as amended 2026-11-16: on a server of many organizations the only AI endpoint that
    takes its key from the deployment is the sandbox tier's (REQ-2006), whose key the
    organization's keys already carry (:func:`read_org_api_keys`). Any other endpoint's key read
    from the server's environment is refused; an organization's own is a ``${secret:...}``."""
    if _many() and _key_field_reads_environment(field):
        endpoint_id = str(endpoint["id"])
        raise AiKeyRequired(
            endpoint_id,
            "ai.endpoint_key_from_server",
            f"The AI endpoint {endpoint_id} takes its key from the server's environment, which an "
            f"organization on this server does not use. Give it a key of your own under "
            f"{WHERE_TO_ADD_AI_KEY}.",
        )


def _endpoint_key_unset(endpoint: Mapping[str, object]) -> AiKeyRequired:
    endpoint_id = str(endpoint["id"])
    return AiKeyRequired(
        endpoint_id,
        "ai.endpoint_key_unset",
        f"The AI endpoint {endpoint_id} has no key: what it names is not set. Set it under "
        f"{WHERE_TO_ADD_AI_KEY}.",
    )


def _endpoint_key_field(endpoint: Mapping[str, object]) -> str | None:
    """The ``api_key_env`` an endpoint's key is resolved from, after the refusals; None when it
    declares none."""
    field = endpoint.get("api_key_env")
    if not field:
        return None
    field = str(field)
    from provisa.core.env_reference_guard import refuse_named_by_org

    refuse_named_by_org(field)  # REQ-1557: a held organization's own key variable is not read
    _refuse_endpoint_key_from_environment(endpoint, field)
    return field


async def endpoint_key(stored: Mapping[str, str], endpoint: Mapping[str, object]) -> str | None:
    """The key an AI endpoint is called with, given the organization's keys
    (:func:`read_org_api_keys`): the one they carry for it by its id (the sandbox tier's); else
    what its ``api_key_env`` resolves to (provisa.core.secrets.resolve_api_key_field), refused
    when that is the server's environment on a server of many organizations, and when it is
    unset. None for an endpoint that declares no key."""
    held = stored.get(str(endpoint["id"]))
    if held:
        return held
    field = _endpoint_key_field(endpoint)
    if field is None:
        return None
    from provisa.core.secrets import resolve_api_key_field

    key = await resolve_api_key_field(field)
    if not key:
        raise _endpoint_key_unset(endpoint)
    return key


def endpoint_key_now(stored: Mapping[str, str], endpoint: Mapping[str, object]) -> str | None:
    """:func:`endpoint_key` where no organization's vault is bound (the model client's call, on a
    thread of its own): a variable is read as :func:`endpoint_key` reads it, a reference resolved
    by provisa.core.secrets.resolve_secrets."""
    held = stored.get(str(endpoint["id"]))
    if held:
        return held
    field = _endpoint_key_field(endpoint)
    if field is None:
        return None
    if "${" in field:
        from provisa.core.secrets import resolve_secrets

        key: str | None = resolve_secrets(field)
    else:
        # REQ-1790: an endpoint's api_key_env names a variable; on a server of many organizations
        # it is refused above, so this read is an install of one organization's own (REQ-1395).
        key = os.environ.get(field)
    if not key:
        raise _endpoint_key_unset(endpoint)
    return key


def endpoint_address(endpoint: Mapping[str, object]) -> str:
    """The ``base_url`` an AI endpoint is reached at, as declared. On a server of many
    organizations an address taken from the server's environment (``${env:...}``) is refused like
    a key from it (REQ-1395 as amended 2026-11-16)."""
    address = str(endpoint["base_url"])
    if _many() and "${env:" in address:
        endpoint_id = str(endpoint["id"])
        raise AiKeyRequired(
            endpoint_id,
            "ai.endpoint_address_from_server",
            f"The AI endpoint {endpoint_id} takes its address from the server's environment, "
            f"which an organization on this server does not use. Give it an address of its own "
            f"under {WHERE_TO_ADD_AI_KEY}.",
        )
    return address


# --- the vendors' own clients ----------------------------------------------------------------------

#: aisuite's providers built on a vendor SDK's client, by the SDK. Each other provider aisuite has
#: speaks to a fixed address over httpx with the key it is given (xai, together, fireworks), is a
#: local server (:data:`KEYLESS_VENDORS`), or is reached with the server's own credentials.
_SDK_OF: dict[str, str] = {
    "anthropic": "anthropic",
    "openai": "openai",
    "deepseek": "openai",
    "nebius": "openai",
    "sambanova": "openai",
    "inception": "openai",
}

#: The address each SDK's client is given when no other is: the vendor's own, which the SDK takes
#: from ANTHROPIC_BASE_URL / OPENAI_BASE_URL when it is given none.
SDK_ADDRESS: dict[str, str] = {
    "anthropic": "https://api.anthropic.com",
    "openai": "https://api.openai.com/v1",
}

#: What each SDK reads from the environment whatever it is given: headers it sends with every
#: request (anthropic and openai ``_client.py``). On a server of many organizations no client of
#: that SDK is built while one is set.
SDK_HEADER_ENV: dict[str, str] = {
    "anthropic": "ANTHROPIC_CUSTOM_HEADERS",
    "openai": "OPENAI_CUSTOM_HEADERS",
}


def sdk_config(provider: str, config: Mapping[str, Any]) -> dict[str, Any]:
    """The configuration aisuite's ``provider`` (or the vendor SDK's client of that name) is built
    with: ``config``, and on a server of many organizations every value its SDK otherwise takes
    from the server's environment given explicitly (REQ-1395 as amended 2026-11-16): the key
    (anthropic's SDK reads ANTHROPIC_AUTH_TOKEN and its profiles only when given none), the
    address (ANTHROPIC_BASE_URL, OPENAI_BASE_URL), and OpenAI's organization and project
    (OPENAI_ORG_ID, OPENAI_PROJECT_ID, sent as headers, which are omitted) and admin key
    (OPENAI_ADMIN_KEY). Refused while the environment sets the headers the SDK sends whatever it
    is given (:data:`SDK_HEADER_ENV`). An install of one organization builds with ``config`` as
    it is."""
    built = dict(config)
    sdk = _SDK_OF.get(provider)
    if sdk is None or not _many():
        return built
    variable = SDK_HEADER_ENV[sdk]
    if variable in os.environ:
        raise AiKeyRequired(
            provider,
            "ai.server_environment_sets",
            f"{provider} cannot be called here: the server's environment sets {variable}, which "
            f"its client would send for every organization.",
            variable=variable,
        )
    if not built.get("api_key"):
        raise _no_key(provider)
    built.setdefault("base_url", SDK_ADDRESS[sdk])
    if sdk == "openai":
        from openai import Omit

        built["admin_api_key"] = ""
        built["default_headers"] = {
            **dict(built.get("default_headers") or {}),
            "OpenAI-Organization": Omit(),
            "OpenAI-Project": Omit(),
        }
    return built
