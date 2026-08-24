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

# The only secrets an org administrator may set through this door.
ORG_SECRET_KEYS: frozenset[str] = frozenset(f"{vendor}_api_key" for vendor in LLM_VENDORS)


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


async def read_org_secret(tenant_db: Database, key: str) -> str | None:
    """The org's decrypted secret value, or ``None`` if unset."""
    from sqlalchemy import select

    from provisa.core.schema_org import org_secrets
    from provisa.encryption.runtime import encryption_service

    async with tenant_db.acquire() as conn:
        result = await conn.execute_core(
            select(org_secrets.c.value_enc).where(org_secrets.c.key == key)
        )
        row = result.fetchone()
    if row is None:
        return None
    return await _resolved(encryption_service().decrypt(bytes(row[0])).decode("utf-8"))


async def read_org_api_keys(tenant_db: Database) -> dict[str, str]:
    """Every configured `{vendor}_api_key` for the org, keyed by vendor name."""
    from sqlalchemy import select

    from provisa.core.schema_org import org_secrets
    from provisa.encryption.runtime import encryption_service

    async with tenant_db.acquire() as conn:
        result = await conn.execute_core(
            select(org_secrets.c.key, org_secrets.c.value_enc).where(
                org_secrets.c.key.in_(ORG_SECRET_KEYS)
            )
        )
        rows = result.fetchall()
    service = encryption_service()
    return {
        row[0].removesuffix("_api_key"): await _resolved(
            service.decrypt(bytes(row[1])).decode("utf-8")
        )
        for row in rows
    }


async def write_org_secret(
    tenant_db: Database, key: str, value: str | None, *, updated_by: str
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

    async with tenant_db.acquire() as conn:
        if value is None:
            await conn.execute_core(_delete(_org_secrets_t).where(_org_secrets_t.c.key == key))
            return
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
