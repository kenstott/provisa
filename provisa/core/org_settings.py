# Copyright (c) 2026 Kenneth Stott
# Canary: 8b41d0c7-6e25-4a93-9f18-3c07b5d2ae64
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Per-org overrides layered over the deployment config file (REQ-1349).

The deployment config is the BASE. An org may narrow a small, fixed set of top-level keys — the
ones whose subject is the org's own work, not the deployment's infrastructure — by writing a row
into its schema's ``org_settings`` table. Everything else (federation engine, cache storage,
encryption provider, auth provider) has no per-org representation at all and stays gated on
``platform_settings``.

Resolution happens at REQUEST time, not at startup: a read hits the org's table each time, so an
org administrator changing the NL provider sees it on the next query rather than after a restart.
"""

# Requirements: REQ-1349

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from provisa.core.db import Database

# The only top-level config keys an org administrator owns. `ai_models` selects the vendor/model
# per LLM operation (this is what "change the LLM provider for NL" edits — sql_generation);
# `vector_models` is the embedding registry the org's own semantic search uses; `nl` carries the
# NL rate limit. Any key not listed here is deployment-wide by definition and is rejected.
# REQ-1074: metadata_export is org-scoped — the catalog an org publishes to, and the credentials
# it publishes with, belong to that org, not to the deployment.
ORG_OVERRIDABLE_KEYS: frozenset[str] = frozenset(
    {"ai_models", "vector_models", "nl", "metadata_export"}
)


def merge_org_overrides(base: dict[str, Any], overrides: dict[str, Any]) -> dict[str, Any]:
    """The deployment config with the org's overrides layered on top.

    A dict-valued key merges one level deep, so an org that overrides only ``sql_generation``
    keeps the deployment's model for every other operation. Any other shape replaces outright —
    ``vector_models`` is a list and is owned whole or not at all.
    """
    merged = dict(base)
    for key, value in overrides.items():
        current = merged.get(key)
        if isinstance(value, dict) and isinstance(current, dict):
            merged[key] = {**current, **value}
        else:
            merged[key] = value
    return merged


async def read_org_overrides(tenant_db: Database) -> dict[str, Any]:
    """The org's raw override rows as ``{key: value}``. Empty when the org has overridden nothing."""
    from sqlalchemy import select

    from provisa.core.schema_org import org_settings

    async with tenant_db.acquire() as conn:
        result = await conn.execute_core(select(org_settings.c.key, org_settings.c.value))
        rows = result.fetchall()
    return {row[0]: row[1] for row in rows}


async def resolve_org_config(tenant_db: Database) -> dict[str, Any]:
    """The deployment config with this org's overrides applied — the config a request should use."""
    from provisa.api.admin._config_io import read_config

    return merge_org_overrides(read_config(), await read_org_overrides(tenant_db))


async def write_org_overrides(
    tenant_db: Database, updates: dict[str, Any], *, updated_by: str
) -> list[str]:
    """Upsert the given override keys; a ``None`` value DELETES the row (reverting to deployment).

    Returns the keys written. Raises ValueError on a key outside :data:`ORG_OVERRIDABLE_KEYS` —
    an org must not be able to reach a deployment-wide setting through this door.
    """
    unknown = set(updates) - ORG_OVERRIDABLE_KEYS
    if unknown:
        raise ValueError(f"not org-overridable: {', '.join(sorted(unknown))}")

    import json

    written: list[str] = []
    async with tenant_db.acquire() as conn:
        for key, value in updates.items():
            if value is None:
                await conn.execute("DELETE FROM org_settings WHERE key = $1", key)
            else:
                await conn.execute(
                    "INSERT INTO org_settings (key, value, updated_by) VALUES ($1, $2::jsonb, $3)"
                    " ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value,"
                    " updated_at = NOW(), updated_by = EXCLUDED.updated_by",
                    key,
                    json.dumps(value),
                    updated_by,
                )
            written.append(key)
    return written
