# Copyright (c) 2026 Kenneth Stott
# Canary: 38452509-8588-402b-ad95-2cc597ab0125
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Billing schema DDL and asyncpg CRUD for tenants and tenant_config."""

# Requirements: REQ-052

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING

from provisa.api.billing.models import Plan, Tenant

if TYPE_CHECKING:
    from provisa.core.database import Database

BILLING_SCHEMA_SQL = """
CREATE SCHEMA IF NOT EXISTS platform;

CREATE TABLE IF NOT EXISTS platform.tenants (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    kms_key_arn TEXT NOT NULL,
    stripe_customer_id TEXT,
    plan TEXT NOT NULL DEFAULT 'trial',
    source_limit INT NOT NULL DEFAULT 2,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS platform.tenant_config (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL REFERENCES platform.tenants(id) ON DELETE CASCADE,
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    encrypted_dek BYTEA NOT NULL,
    ciphertext BYTEA NOT NULL,
    iv BYTEA NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, entity_type, entity_id)
);

CREATE INDEX IF NOT EXISTS idx_tenant_config_lookup ON platform.tenant_config (tenant_id, entity_type);
"""


def _row_to_tenant(row) -> Tenant:
    return Tenant(
        id=row["id"],
        kms_key_arn=row["kms_key_arn"],
        stripe_customer_id=row["stripe_customer_id"],
        plan=Plan(row["plan"]),
        source_limit=row["source_limit"],
        created_at=row["created_at"],
    )


async def init_billing_schema(pool: "Database") -> None:  # REQ-592, REQ-696
    async with pool.acquire() as conn:
        await conn.execute("SELECT pg_advisory_lock(7338)")
        try:
            # multi-statement script (CREATE SCHEMA + tables + index); raw asyncpg
            # runs it natively, the Database shim auto-routes to the driver.
            await conn.execute(BILLING_SCHEMA_SQL)
        finally:
            await conn.execute("SELECT pg_advisory_unlock(7338)")


async def create_tenant(pool: "Database", kms_key_arn: str) -> Tenant:  # REQ-592
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO platform.tenants (kms_key_arn)
            VALUES ($1)
            RETURNING id, kms_key_arn, stripe_customer_id, plan, source_limit, created_at
            """,
            kms_key_arn,
        )
    return _row_to_tenant(row)


async def get_tenant(pool: "Database", tenant_id: str) -> Tenant | None:  # REQ-592
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT id, kms_key_arn, stripe_customer_id, plan, source_limit, created_at
            FROM platform.tenants WHERE id = $1
            """,
            uuid.UUID(tenant_id),
        )
    if row is None:
        return None
    return _row_to_tenant(row)


async def get_tenant_by_stripe_customer(  # REQ-592
    pool: "Database", stripe_customer_id: str
) -> Tenant | None:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT id, kms_key_arn, stripe_customer_id, plan, source_limit, created_at
            FROM platform.tenants WHERE stripe_customer_id = $1
            """,
            stripe_customer_id,
        )
    if row is None:
        return None
    return _row_to_tenant(row)


async def update_tenant_plan(  # REQ-592
    pool: "Database", tenant_id: str, plan: str, source_limit: int
) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE platform.tenants SET plan = $1, source_limit = $2 WHERE id = $3",
            plan,
            source_limit,
            uuid.UUID(tenant_id),
        )


async def update_tenant_stripe_customer(  # REQ-592
    pool: "Database", tenant_id: str, stripe_customer_id: str
) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE platform.tenants SET stripe_customer_id = $1 WHERE id = $2",
            stripe_customer_id,
            uuid.UUID(tenant_id),
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
        await conn.execute(
            """
            INSERT INTO platform.tenant_config (tenant_id, entity_type, entity_id, encrypted_dek, ciphertext, iv)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (tenant_id, entity_type, entity_id)
            DO UPDATE SET encrypted_dek = EXCLUDED.encrypted_dek,
                          ciphertext = EXCLUDED.ciphertext,
                          iv = EXCLUDED.iv,
                          updated_at = now()
            """,
            uuid.UUID(tenant_id),
            entity_type,
            entity_id,
            encrypted_dek,
            ciphertext,
            iv,
        )


async def fetch_config_entities(
    pool: "Database", tenant_id: str, entity_type: str
) -> list[dict]:  # REQ-458
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT entity_id, encrypted_dek, ciphertext, iv, updated_at
            FROM platform.tenant_config
            WHERE tenant_id = $1 AND entity_type = $2
            """,
            uuid.UUID(tenant_id),
            entity_type,
        )
    return [dict(r) for r in rows]
