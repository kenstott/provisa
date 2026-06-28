# Copyright (c) 2026 Kenneth Stott
# Canary: ad492cac-4438-4e3a-88d8-315e26a58491
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""asyncpg connection pool factory."""

# Requirements: REQ-052, REQ-040

import asyncio
import json

import asyncpg


def _json_encoder(v):
    return v if isinstance(v, str) else json.dumps(v)


async def _init_conn(conn: asyncpg.Connection) -> None:
    await conn.set_type_codec(
        "jsonb", encoder=_json_encoder, decoder=json.loads, schema="pg_catalog"
    )
    await conn.set_type_codec(
        "json", encoder=_json_encoder, decoder=json.loads, schema="pg_catalog"
    )


def _make_init_conn(org_id: str):
    schema_name = f"org_{org_id}"

    async def _init_conn_for_org(conn: asyncpg.Connection) -> None:
        await conn.execute(f"SET search_path TO {schema_name}")

    return _init_conn_for_org


_make_setup_conn = _make_init_conn


async def create_pool(  # REQ-052
    host: str,
    port: int,
    database: str,
    user: str,
    password: str,
    min_size: int = 2,
    max_size: int = 10,
    org_id: str = "default",
) -> asyncpg.Pool:
    return await asyncio.wait_for(
        asyncpg.create_pool(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password,
            min_size=min_size,
            max_size=max_size,
            init=_init_conn,
            setup=_make_setup_conn(org_id),
        ),
        timeout=10,
    )


async def create_org_role(conn: asyncpg.Connection, org_id: str) -> None:  # REQ-699
    """Create a PG role scoped to org_<org_id> schema only."""
    _validate_org_id(org_id)
    schema_name = f"org_{org_id}"
    role_name = f"role_{org_id}"
    await conn.execute(
        f"DO $$ BEGIN CREATE ROLE {role_name}; EXCEPTION WHEN duplicate_object THEN NULL; END $$"
    )
    await conn.execute(f"GRANT USAGE, CREATE ON SCHEMA {schema_name} TO {role_name}")


def _validate_org_id(org_id: str) -> None:
    import re

    if not re.fullmatch(r"[a-zA-Z0-9_]+", org_id):
        raise ValueError(f"org_id must be alphanumeric/underscore only, got: {org_id!r}")


async def init_schema(pool: asyncpg.Pool, schema_sql: str, org_id: str = "default") -> None:
    """Execute schema SQL scoped to org_<org_id> schema (REQ-697)."""
    _validate_org_id(org_id)
    schema_name = f"org_{org_id}"
    async with pool.acquire() as conn:
        await conn.execute("SELECT pg_advisory_lock(7337)")
        try:
            await conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
            await conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}_mv_cache")
            await conn.execute(f"SET search_path TO {schema_name}")
            await conn.execute(schema_sql)
        finally:
            await conn.execute("SELECT pg_advisory_unlock(7337)")


async def set_tenant_context(
    conn: asyncpg.Connection, tenant_id: str | None
) -> None:  # REQ-040, REQ-041
    """Set app.tenant_id session variable for RLS. Call at start of each transaction."""
    if tenant_id:
        await conn.execute(f"SET LOCAL app.tenant_id = '{tenant_id}'")
