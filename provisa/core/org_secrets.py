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
from typing import TYPE_CHECKING

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

    def __init__(self, vendor: str, code: str, message: str) -> None:
        super().__init__(message)
        self.vendor = vendor
        self.code = code
        self.params = {"vendor": vendor, "where": WHERE_TO_ADD_AI_KEY}


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
    """The ``api_key`` one of aisuite's named vendors is constructed with. None for a vendor
    that takes none from here: a local server (:data:`KEYLESS_VENDORS`), and on an install of
    one organization a vendor reached with the deployment's own credentials (Vertex AI, Bedrock
    and the like), which aisuite reads itself. On a server of many organizations those
    credentials are the server's, so such a vendor is refused."""
    if vendor in AI_KEY_ENV:
        return ai_key(stored, vendor)
    key = stored.get(vendor)
    if key:
        return key
    from provisa.core.secrets_providers import many_organisations

    if vendor in KEYLESS_VENDORS or not many_organisations():
        return None
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
