# Copyright (c) 2026 Kenneth Stott
# Canary: 4b7e2c90-1d36-4a8f-9e05-6c3f8a1d2b57
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An org's subscription tier, stored with the org (REQ-1066, amended 2026-10-20).

The tier is a column of the org's row in the platform registry (``orgs.tier``): it holds on a
single-org install as on a server of many organizations, and across restarts. A platform
administrator sets it (Admin > Orgs); every tier-gated feature reads it here.
"""

# Requirements: REQ-1066, REQ-1073

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import select, update

from provisa.core.schema_admin import orgs

if TYPE_CHECKING:
    from provisa.core.db import Database


def _admin_db() -> "Database":
    from provisa.api.app import state

    if state.admin_db is None:
        raise RuntimeError("the platform registry is not connected")
    return state.admin_db


async def read_org_tier(org_id: str) -> str | None:
    """``org_id``'s tier, or None when none is set. Raises KeyError for an org with no row."""
    async with _admin_db().acquire() as conn:
        row = (await conn.execute_core(select(orgs.c.tier).where(orgs.c.id == org_id))).fetchone()
    if row is None:
        raise KeyError(f"org {org_id!r} is not in the platform registry")
    return row[0]


async def write_org_tier(org_id: str, tier: str | None) -> bool:
    """Set ``org_id``'s tier (None clears it). False when the org has no row."""
    async with _admin_db().acquire() as conn:
        result = await conn.execute_core(
            update(orgs).where(orgs.c.id == org_id).values(tier=tier).returning(orgs.c.id)
        )
        return result.fetchone() is not None
