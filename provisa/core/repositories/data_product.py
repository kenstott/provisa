# Copyright (c) 2026 Kenneth Stott
# Canary: 4fceec2b-2f69-43db-b7a6-27ff29e7a9cf
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""DataProduct repository — CRUD, domain-scoped, via SQLAlchemy Core (dialect-portable)."""

# Requirements: REQ-1634

from typing import TYPE_CHECKING

from sqlalchemy import delete as _delete, select

from provisa.core.models import DataProduct
from provisa.core.schema_org import data_products

if TYPE_CHECKING:
    from provisa.core.database import Connection


async def upsert(conn: "Connection", product: DataProduct) -> None:  # REQ-1634
    await conn.upsert(
        data_products,
        {
            "id": product.id,
            "domain_id": product.domain_id,
            "name": product.name,
            "owner": product.owner,
            "description": product.description,
        },
        index_elements=["id"],
        update_columns=["domain_id", "name", "owner", "description"],
    )


async def get(conn: "Connection", product_id: str) -> dict | None:  # REQ-1634
    result = await conn.execute_core(select(data_products).where(data_products.c.id == product_id))
    row = result.fetchone()
    return dict(row._mapping) if row is not None else None


async def list_all(conn: "Connection") -> list[dict]:  # REQ-1634
    result = await conn.execute_core(select(data_products).order_by(data_products.c.id))
    return [dict(r._mapping) for r in result.fetchall()]


async def list_by_domain(conn: "Connection", domain_id: str) -> list[dict]:  # REQ-1634
    result = await conn.execute_core(
        select(data_products)
        .where(data_products.c.domain_id == domain_id)
        .order_by(data_products.c.id)
    )
    return [dict(r._mapping) for r in result.fetchall()]


async def delete(conn: "Connection", product_id: str) -> bool:  # REQ-1634
    result = await conn.execute_core(_delete(data_products).where(data_products.c.id == product_id))
    return (result.rowcount or 0) > 0
