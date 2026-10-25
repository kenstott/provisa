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

from provisa.core import model_change
from typing import TYPE_CHECKING

from sqlalchemy import delete as _delete, select, update

from provisa.core.models import DataContract, DataProduct
from provisa.core.repositories.integrity import Dependent, ObjectRef, guard, remove_parts
from provisa.core.schema_org import data_products, registered_tables, table_columns

if TYPE_CHECKING:
    from provisa.core.database import Connection


async def upsert(  # REQ-1634, REQ-1919
    conn: "Connection", product: DataProduct
) -> None:
    """Create the data product, or replace its definition."""
    model_change.name("upsert", "data product", product.id)  # REQ-1524
    await conn.upsert(
        data_products,
        {
            "id": product.id,
            "domain_id": product.domain_id,
            "name": product.name,
            "owner_role": product.owner_role,
            "team_role": product.team_role,
            "purpose": product.purpose,
            "limitations": product.limitations,
            "usage": product.usage,
            "version": product.version,
            "status": product.status,
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
            "support",
            "support_contact",
            "publish",
            "custom_properties",
        ],
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
        # What a refusal says before naming its dependents (REQ-2152: the API layer names only
        # those its caller may read).
        self.lead = f"Data product {product_id!r} still has members"
        named = ", ".join(f"{d.ref.kind} {d.ref.id}" for d in dependents)
        super().__init__(f"Data product {product_id!r} still has members: {named}")


async def delete(conn: "Connection", product_id: str) -> bool:  # REQ-1634, REQ-1918
    """Delete one data product: THE delete, for every surface. False when there is none.

    A data product is blocked by its members (REQ-1918): refused
    (:class:`DataProductDeleteRefused`), naming each, while a table or a command belongs to
    it — the operator takes them out of the product first. (PostgreSQL used to detach them
    silently; SQLite left them naming a product that was gone.) Its tag assignments go with it.
    One transaction."""
    model_change.name("delete", "data product", product_id)  # REQ-1524
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


async def member_contracts(conn: "Connection", product_id: str | None = None) -> list[dict]:
    """REQ-2158: each product member's contract, with its table's id and address: every product's,
    or ``product_id``'s."""
    query = select(
        registered_tables.c.id,
        registered_tables.c.product_id,
        registered_tables.c.source_id,
        registered_tables.c.schema_name,
        registered_tables.c.table_name,
        registered_tables.c.contract,
    ).where(registered_tables.c.contract.is_not(None))
    if product_id is not None:
        query = query.where(registered_tables.c.product_id == product_id)
    result = await conn.execute_core(query.order_by(registered_tables.c.id))
    return [dict(r._mapping) for r in result.fetchall()]


async def save_contract(  # REQ-2158
    conn: "Connection", table_id: int, contract: DataContract
) -> dict:
    """A member's contract replaced: refused (data_contracts.ContractRefused) when the table is no
    product's output port, when it stops promising a column at the same version, or when it
    promises a column the member does not hold as that type. The member's row, as it is now."""
    from provisa.core import data_contracts

    async with conn.transaction():
        row = (
            await conn.execute_core(
                select(registered_tables).where(registered_tables.c.id == table_id)
            )
        ).fetchone()
        if row is None or row.product_id is None or row.contract is None:
            raise data_contracts.ContractRefused(
                f"table {table_id} is no data product's output port: it has no contract"
            )
        cid = data_contracts.contract_id(row.product_id, row.schema_name, row.table_name)
        model_change.name("upsert", "data contract", cid)  # REQ-1524
        contract = data_contracts.as_stored(cid, contract)
        data_contracts.require_new_version(cid, DataContract.model_validate(row.contract), contract)
        types = {
            r.column_name: r.data_type
            for r in (
                await conn.execute_core(
                    select(table_columns.c.column_name, table_columns.c.data_type).where(
                        table_columns.c.table_id == table_id
                    )
                )
            ).fetchall()
        }
        data_contracts.require_provided(cid, contract, types)
        await conn.execute_core(
            update(registered_tables)
            .where(registered_tables.c.id == table_id)
            .values(contract=contract.model_dump())
        )
    return {**dict(row._mapping), "contract": contract.model_dump()}
