# Copyright (c) 2026 Kenneth Stott
# Canary: 38452509-8588-402b-ad95-2cc597ab0125
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Billing schema init + CRUD for tenants and tenant_config.

Goes through the control-plane ``Database`` abstraction only — portable SQLAlchemy metadata for the
schema and vanilla SQLAlchemy Core for the CRUD — so the platform control plane works on any backend
(PostgreSQL or SQLite). No engine-specific DDL/functions/ON CONFLICT here.
"""

# Requirements: REQ-052

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING

from sqlalchemy import func, insert, select, update

from provisa.api.billing.models import Plan, Tenant
from provisa.core.schema_admin import tenant_config, tenants

if TYPE_CHECKING:
    from provisa.core.database import Database


def _row_to_tenant(row) -> Tenant:
    d = dict(row._mapping)
    return Tenant(
        id=d["id"],
        kms_key_arn=d["kms_key_arn"],
        ls_customer_id=d["ls_customer_id"],
        plan=Plan(d["plan"]),
        source_limit=d["source_limit"],
        created_at=d["created_at"],
    )


async def init_billing_schema(pool: "Database") -> None:  # REQ-592, REQ-696
    """Create the SaaS billing tables via portable SQLAlchemy metadata (dialect-appropriate DDL),
    mirroring ``init_registry_schema``. The ``tenants``/``tenant_config`` Table objects live in the
    shared registry metadata; no PG-only ``CREATE SCHEMA`` / functions are emitted."""
    from provisa.core.schema_admin import metadata

    async with pool.engine.begin() as conn:
        await conn.run_sync(lambda sc: metadata.create_all(sc, tables=[tenants, tenant_config]))


async def create_tenant(pool: "Database", kms_key_arn: str) -> Tenant:  # REQ-592
    async with pool.acquire() as conn:
        # id is generated app-side (portable across dialects); plan/source_limit/created_at fall to
        # the table's server defaults.
        result = await conn.execute_core(
            insert(tenants).values(id=uuid.uuid4(), kms_key_arn=kms_key_arn).returning(tenants)
        )
        row = result.fetchone()
    return _row_to_tenant(row)


async def get_tenant(pool: "Database", tenant_id: str) -> Tenant | None:  # REQ-592
    async with pool.acquire() as conn:
        result = await conn.execute_core(
            select(tenants).where(tenants.c.id == uuid.UUID(tenant_id))
        )
        row = result.fetchone()
    if row is None:
        return None
    return _row_to_tenant(row)


async def get_tenant_by_ls_customer(  # REQ-592, REQ-1015
    pool: "Database", ls_customer_id: str
) -> Tenant | None:
    async with pool.acquire() as conn:
        result = await conn.execute_core(
            select(tenants).where(tenants.c.ls_customer_id == ls_customer_id)
        )
        row = result.fetchone()
    if row is None:
        return None
    return _row_to_tenant(row)


async def update_tenant_plan(  # REQ-592
    pool: "Database", tenant_id: str, plan: str, source_limit: int
) -> None:
    async with pool.acquire() as conn:
        await conn.execute_core(
            update(tenants)
            .where(tenants.c.id == uuid.UUID(tenant_id))
            .values(plan=plan, source_limit=source_limit)
        )


async def update_tenant_ls_customer(  # REQ-592, REQ-1015
    pool: "Database", tenant_id: str, ls_customer_id: str
) -> None:
    async with pool.acquire() as conn:
        await conn.execute_core(
            update(tenants)
            .where(tenants.c.id == uuid.UUID(tenant_id))
            .values(ls_customer_id=ls_customer_id)
        )


async def upsert_config_entity(  # REQ-458
    pool: "Database",
    tenant_id: str,
    entity_type: str,
    entity_id: str,
    encrypted_dek: bytes,
    ciphertext: bytes,
    iv: bytes,
) -> None:
    async with pool.acquire() as conn:
        await conn.upsert(
            tenant_config,
            {
                "id": uuid.uuid4(),
                "tenant_id": uuid.UUID(tenant_id),
                "entity_type": entity_type,
                "entity_id": entity_id,
                "encrypted_dek": encrypted_dek,
                "ciphertext": ciphertext,
                "iv": iv,
            },
            index_elements=["tenant_id", "entity_type", "entity_id"],
            update_columns=["encrypted_dek", "ciphertext", "iv"],
            set_extra={"updated_at": func.now()},
        )


async def fetch_config_entities(
    pool: "Database", tenant_id: str, entity_type: str
) -> list[dict]:  # REQ-458
    async with pool.acquire() as conn:
        result = await conn.execute_core(
            select(
                tenant_config.c.entity_id,
                tenant_config.c.encrypted_dek,
                tenant_config.c.ciphertext,
                tenant_config.c.iv,
                tenant_config.c.updated_at,
            ).where(
                tenant_config.c.tenant_id == uuid.UUID(tenant_id),
                tenant_config.c.entity_type == entity_type,
            )
        )
        rows = result.fetchall()
    return [dict(r._mapping) for r in rows]
