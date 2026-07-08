# Copyright (c) 2026 Kenneth Stott
# Canary: 4fceec2b-2f69-43db-b7a6-27ff29e7a9cf
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Domain repository — CRUD for domains, via SQLAlchemy Core (dialect-portable)."""

# Requirements: REQ-021, REQ-154, REQ-367, REQ-402

from typing import TYPE_CHECKING

from sqlalchemy import delete as _delete, select

from provisa.core.models import Domain
from provisa.core.schema_org import domains

if TYPE_CHECKING:
    from provisa.core.database import Connection


async def upsert(conn: "Connection", domain: Domain) -> None:  # REQ-021, REQ-367
    await conn.upsert(
        domains,
        {
            "id": domain.id,
            "description": domain.description,
            "graphql_alias": domain.graphql_alias,
            "org_id": "root",
        },
        index_elements=["id"],
        update_columns=["description", "graphql_alias"],
    )


async def get(conn: "Connection", domain_id: str) -> dict | None:  # REQ-021, REQ-402
    result = await conn.execute_core(select(domains).where(domains.c.id == domain_id))
    row = result.fetchone()
    return dict(row._mapping) if row is not None else None


async def list_all(conn: "Connection") -> list[dict]:  # REQ-021
    result = await conn.execute_core(select(domains).order_by(domains.c.id))
    return [dict(r._mapping) for r in result.fetchall()]


async def delete(conn: "Connection", domain_id: str) -> bool:  # REQ-021
    result = await conn.execute_core(_delete(domains).where(domains.c.id == domain_id))
    return (result.rowcount or 0) > 0
