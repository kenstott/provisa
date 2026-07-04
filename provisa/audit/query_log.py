# Copyright (c) 2026 Kenneth Stott
# Canary: 8f8ec523-0921-4866-889d-9a3f38256e46
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""SOC2 append-only query audit log."""

# Requirements: REQ-074

from __future__ import annotations

import hashlib
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from provisa.core.database import Database
    from provisa.encryption import EncryptionService

AUDIT_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS query_audit_log (
    id BIGSERIAL PRIMARY KEY,
    tenant_id UUID,
    user_id TEXT NOT NULL,
    role_id TEXT NOT NULL,
    query_hash TEXT NOT NULL,
    query_text_enc BYTEA,
    table_ids TEXT[] NOT NULL DEFAULT '{}',
    source TEXT NOT NULL,
    status_code INT NOT NULL,
    duration_ms INT NOT NULL,
    logged_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

DO $$ BEGIN
    CREATE RULE no_delete_audit AS ON DELETE TO query_audit_log DO INSTEAD NOTHING;
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

DO $$ BEGIN
    CREATE RULE no_update_audit AS ON UPDATE TO query_audit_log DO INSTEAD NOTHING;
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

CREATE INDEX IF NOT EXISTS idx_audit_tenant_time ON query_audit_log (tenant_id, logged_at DESC);
CREATE INDEX IF NOT EXISTS idx_audit_user_time ON query_audit_log (user_id, logged_at DESC);
"""


async def init_audit_schema(pool: "Database", org_id: str = "default") -> None:  # REQ-074
    from provisa.core.db import _validate_org_id

    _validate_org_id(org_id)
    # Non-PG backends already have query_audit_log from schema_org.create_all
    # (init_schema portable path); the append-only RULE is PG-only. On those
    # backends immutability is enforced app-side, so audit init is a no-op.
    if pool.dialect != "postgresql":
        return
    schema_name = f"org_{org_id}"
    async with pool.acquire() as conn:
        await conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
        await conn.execute(f"SET search_path TO {schema_name}")
        # multi-statement script (CREATE TABLE + DO $$ RULEs + indexes); raw
        # asyncpg runs it natively, the Database shim auto-routes to the driver.
        await conn.execute(AUDIT_SCHEMA_SQL)


async def log_query(  # REQ-074, REQ-689
    pool: "Database",
    *,
    tenant_id: str | None,
    user_id: str,
    role_id: str,
    query_text: str,
    table_ids: list[str],
    source: str,
    status_code: int,
    duration_ms: int,
    encryption: "EncryptionService",
) -> None:
    """Append an audit row. The query text is stored ENCRYPTED (REQ-689) — query text
    can reveal schema shape and data intent — and the plaintext SHA hash is kept for
    indexing/dedup. Pass NullEncryption in dev/test; a real provider in production."""
    query_hash = hashlib.sha256(query_text.encode()).hexdigest()
    query_text_enc = encryption.encrypt(query_text.encode("utf-8"))
    await pool.execute(
        "INSERT INTO query_audit_log"
        " (tenant_id, user_id, role_id, query_hash, query_text_enc, table_ids, source,"
        " status_code, duration_ms)"
        " VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)",
        tenant_id,
        user_id,
        role_id,
        query_hash,
        query_text_enc,
        table_ids,
        source,
        status_code,
        duration_ms,
    )


async def read_query_text(  # REQ-689
    pool: "Database", audit_id: int, encryption: "EncryptionService"
) -> str | None:
    """Decrypt and return the query text for one audit row (authorised admin read).

    Callers MUST gate this on an admin capability; decryption is only meaningful with
    the provider/key that wrote the row. Returns None when the row or column is absent.
    """
    row = await pool.fetchrow("SELECT query_text_enc FROM query_audit_log WHERE id = $1", audit_id)
    if row is None or row["query_text_enc"] is None:
        return None
    return encryption.decrypt(bytes(row["query_text_enc"])).decode("utf-8")
