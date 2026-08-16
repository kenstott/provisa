# Copyright (c) 2026 Kenneth Stott
# Canary: 38452509-8588-402b-ad95-2cc597ab0125
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Billing schema init + CRUD against the org registry.

REQ-1355: the org IS the billing subject. This module used to own a ``tenants`` table with its own
UUID primary key, so every billing call site carried a tenant id alongside the org id and nothing
guaranteed the two agreed. The plan/limit/Lemon-Squeezy/KMS columns now live on ``orgs`` and the
encrypted per-entity config in ``org_config`` is keyed by the org slug.

Goes through the control-plane ``Database`` abstraction only — portable SQLAlchemy metadata for the
schema and vanilla SQLAlchemy Core for the CRUD — so the platform control plane works on any backend
(PostgreSQL or SQLite). No engine-specific DDL/functions/ON CONFLICT here.
"""

# Requirements: REQ-052, REQ-1355

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING

from sqlalchemy import func, select, update

from provisa.api.billing.models import OrgBilling, Plan
from provisa.core.schema_admin import org_config, orgs

if TYPE_CHECKING:
    from provisa.core.database import Database


def _row_to_org_billing(row) -> OrgBilling:
    d = dict(row._mapping)
    return OrgBilling(
        org_id=d["id"],
        kms_key_arn=d["kms_key_arn"],
        ls_customer_id=d["ls_customer_id"],
        ls_subscription_id=d["ls_subscription_id"],
        plan=Plan(d["plan"]),
        source_limit=d["source_limit"],
        created_at=d["created_at"],
    )


async def init_billing_schema(pool: "Database") -> None:  # REQ-592, REQ-696, REQ-1355
    """Create ``org_config`` via portable SQLAlchemy metadata (dialect-appropriate DDL), mirroring
    ``init_registry_schema``. The billing columns themselves ride on ``orgs``, which
    ``init_registry_schema`` creates — nothing to do for them here."""
    from provisa.core.schema_admin import metadata

    async with pool.engine.begin() as conn:
        await conn.run_sync(lambda sc: metadata.create_all(sc, tables=[org_config]))


async def get_org_billing(pool: "Database", org_id: str) -> OrgBilling | None:  # REQ-592, REQ-1355
    async with pool.acquire() as conn:
        result = await conn.execute_core(select(orgs).where(orgs.c.id == org_id))
        row = result.fetchone()
    if row is None:
        return None
    return _row_to_org_billing(row)


async def get_org_by_ls_customer(  # REQ-592, REQ-1075, REQ-1355
    pool: "Database", ls_customer_id: str
) -> OrgBilling | None:
    async with pool.acquire() as conn:
        result = await conn.execute_core(
            select(orgs).where(orgs.c.ls_customer_id == ls_customer_id)
        )
        row = result.fetchone()
    if row is None:
        return None
    return _row_to_org_billing(row)


async def set_org_kms_key(pool: "Database", org_id: str, kms_key_arn: str) -> None:  # REQ-1355
    """Bind the org's KMS customer key. Written once, when billing is initialized for the org —
    the caller checks for an existing key first, because overwriting it strands every DEK already
    wrapped under the old one."""
    async with pool.acquire() as conn:
        await conn.execute_core(
            update(orgs).where(orgs.c.id == org_id).values(kms_key_arn=kms_key_arn)
        )


async def update_org_plan(  # REQ-592, REQ-1355
    pool: "Database", org_id: str, plan: str, source_limit: int
) -> None:
    async with pool.acquire() as conn:
        await conn.execute_core(
            update(orgs).where(orgs.c.id == org_id).values(plan=plan, source_limit=source_limit)
        )


async def update_org_ls_customer(  # REQ-592, REQ-1075, REQ-1355
    pool: "Database", org_id: str, ls_customer_id: str, ls_subscription_id: str | None = None
) -> None:
    values: dict[str, str] = {"ls_customer_id": ls_customer_id}
    if ls_subscription_id is not None:
        values["ls_subscription_id"] = ls_subscription_id
    async with pool.acquire() as conn:
        await conn.execute_core(update(orgs).where(orgs.c.id == org_id).values(**values))


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
