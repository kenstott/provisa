# Copyright (c) 2026 Kenneth Stott
# Canary: 8460b7d7-c0bd-4a23-97c4-6baa7679e094
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""CRUD for ``org_config`` — the per-org encrypted entity config in the control plane (REQ-458).

Each row holds one entity's ciphertext together with the data key that encrypts it, wrapped under
the org's own KMS customer key. The table is part of the open-source registry: encrypting an org's
connection secrets is product behaviour, not a commercial feature.
"""

# Requirements: REQ-458, REQ-1355

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING

from sqlalchemy import func, select

from provisa.core.schema_admin import org_config

if TYPE_CHECKING:
    from provisa.core.database import Database


async def upsert_config_entity(  # REQ-458, REQ-1355
    pool: "Database",
    org_id: str,
    entity_type: str,
    entity_id: str,
    encrypted_dek: bytes,
    ciphertext: bytes,
    iv: bytes,
) -> None:
    async with pool.acquire() as conn:
        await conn.upsert(
            org_config,
            {
                "id": uuid.uuid4(),
                "org_id": org_id,
                "entity_type": entity_type,
                "entity_id": entity_id,
                "encrypted_dek": encrypted_dek,
                "ciphertext": ciphertext,
                "iv": iv,
            },
            index_elements=["org_id", "entity_type", "entity_id"],
            update_columns=["encrypted_dek", "ciphertext", "iv"],
            set_extra={"updated_at": func.now()},
        )


async def fetch_config_entities(  # REQ-458, REQ-1355
    pool: "Database", org_id: str, entity_type: str
) -> list[dict]:
    async with pool.acquire() as conn:
        result = await conn.execute_core(
            select(
                org_config.c.entity_id,
                org_config.c.encrypted_dek,
                org_config.c.ciphertext,
                org_config.c.iv,
                org_config.c.updated_at,
            ).where(
                org_config.c.org_id == org_id,
                org_config.c.entity_type == entity_type,
            )
        )
        rows = result.fetchall()
    return [dict(r._mapping) for r in rows]
