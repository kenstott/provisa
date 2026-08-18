# Copyright (c) 2026 Kenneth Stott
# Canary: 19955ff7-c3f9-44b9-8d73-3c62336e21ee
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
# REQ-1266: `naming` carries the org's DOMAIN MODE (use_domains/default_domain) — a tenant setting,
# since one org modelling a single domain says nothing about the next. The rest of the naming block
# (convention, sql_convention, domain_prefix) is NOT org-overridable: those configure the process-
# global `provisa.compiler.naming` module at schema-build time, so they remain deployment-wide and
# the settings router rejects them on this door (see NAMING_ORG_KEYS).
# `redirect` (large-result delivery) and `cache` (default response TTL) govern the org's OWN query
# results, so both are org-owned. The object store they land in — bucket, endpoint, credentials —
# is the deployment's and stays in the platform env, never here.
ORG_OVERRIDABLE_KEYS: frozenset[str] = frozenset(
    {"ai_models", "vector_models", "nl", "metadata_export", "naming", "redirect", "cache"}
)

# The only `naming` sub-keys an org owns. Every other sub-key configures the process-global naming
# module and is deployment-wide.
NAMING_ORG_KEYS: frozenset[str] = frozenset({"use_domains", "default_domain"})


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
    naming = updates.get("naming")
    if isinstance(naming, dict):
        # The guarantee lives at the door, not only in the router: `naming` is overridable for the
        # domain mode alone, so an org can never reach the deployment's naming conventions here.
        extra = set(naming) - NAMING_ORG_KEYS
        if extra:
            raise ValueError(f"not org-overridable: naming.{', naming.'.join(sorted(extra))}")

    from datetime import datetime, timezone

    from sqlalchemy import delete as _delete

    from provisa.core.schema_org import org_settings as _org_settings_t

    written: list[str] = []
    async with tenant_db.acquire() as conn:
        for key, value in updates.items():
            if value is None:
                await conn.execute_core(
                    _delete(_org_settings_t).where(_org_settings_t.c.key == key)
                )
            else:
                # SQLAlchemy Core upsert, not raw PG SQL: NOW()/::jsonb/$n only exist on
                # PostgreSQL, and the demo/dev control plane is SQLite (REQ-1074 settings
                # must save on every dialect the control plane supports).
                await conn.upsert(
                    _org_settings_t,
                    {
                        "key": key,
                        "value": value,
                        "updated_at": datetime.now(timezone.utc),
                        "updated_by": updated_by,
                    },
                    index_elements=["key"],
                    update_columns=["value", "updated_at", "updated_by"],
                )
            written.append(key)
    return written
