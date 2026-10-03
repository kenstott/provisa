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
from provisa.core.repositories.integrity import Dependent, ObjectRef, guard, remove_parts
from provisa.core.repositories.origin import require as require_origin
from provisa.core.repositories.origin import take_over
from provisa.core.schema_org import data_products

if TYPE_CHECKING:
    from provisa.core.database import Connection


async def upsert(  # REQ-1634, REQ-1919
    conn: "Connection", product: DataProduct, *, origin: str
) -> None:
    """Create the data product, or replace its definition. ``origin`` says where it comes from
    (``repositories.origin``): written at CREATE, left alone after, except that a config load
    takes over an admin-made one."""
    require_origin(origin)
    await conn.upsert(
        data_products,
        {
            "id": product.id,
            "origin": origin,  # REQ-1919: on INSERT only
            "domain_id": product.domain_id,
            "name": product.name,
            "owner_role": product.owner_role,
            "team_role": product.team_role,
            "purpose": product.purpose,
            "limitations": product.limitations,
            "usage": product.usage,
            "version": product.version,
            "status": product.status,
            "sla": product.sla,
            "support": product.support,
            "support_contact": product.support_contact,
            "publish": product.publish,
            "custom_properties": product.custom_properties,
        },
        index_elements=["id"],
        update_columns=[
            "domain_id",
            "name",
            "owner_role",
            "team_role",
            "purpose",
            "limitations",
            "usage",
            "version",
            "status",
            "sla",
            "support",
            "support_contact",
            "publish",
            "custom_properties",
        ],
    )
    await take_over(
        conn,
        data_products,
        (data_products.c.id == product.id,),
        kind="data product",
        ident=product.id,
        origin=origin,
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


class DataProductDeleteRefused(Exception):
    """A data product that may not be deleted because it has members; ``dependents`` lists
    them."""

    def __init__(self, product_id: str, dependents: list[Dependent]) -> None:
        self.product_id = product_id
        self.dependents = dependents
        named = ", ".join(f"{d.ref.kind} {d.ref.id}" for d in dependents)
        super().__init__(f"Data product {product_id!r} still has members: {named}")


async def delete(conn: "Connection", product_id: str) -> bool:  # REQ-1634, REQ-1918
    """Delete one data product: THE delete, for every surface. False when there is none.

    A data product is blocked by its members (REQ-1918): refused
    (:class:`DataProductDeleteRefused`), naming each, while a table or a command belongs to
    it — the operator takes them out of the product first. (PostgreSQL used to detach them
    silently; SQLite left them naming a product that was gone.) Its tag assignments go with it.
    One transaction."""
    ref = ObjectRef("data_product", product_id)
    async with conn.transaction():
        if await get(conn, product_id) is None:
            return False
        blocking = await guard(conn, ref)
        if blocking:
            raise DataProductDeleteRefused(product_id, blocking)
        await remove_parts(conn, ref)
        await conn.execute_core(_delete(data_products).where(data_products.c.id == product_id))
    return True
