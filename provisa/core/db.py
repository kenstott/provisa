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
from typing import TYPE_CHECKING, Any

import asyncpg

if TYPE_CHECKING:
    from provisa.core.database import Database


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


async def create_org_role(conn: Any, org_id: str) -> None:  # REQ-699, REQ-889
    """Create a PG role scoped to org_<org_id> schema for physical multi-tenant isolation.

    PostgreSQL-only hardening — a NO-OP on every other control-plane backend (REQ-889). Provisa
    governance roles are a Provisa-layer concept living in metadata tables, never a DB role system;
    this only adds defense-in-depth when Postgres is the control plane. Embedded/single-tenant
    homes have no role system to harden, so the metadata home stays portable.
    """
    _validate_org_id(org_id)
    # Default to postgresql for a raw asyncpg connection (which has no capabilities wrapper).
    dialect = getattr(getattr(conn, "capabilities", None), "dialect", "postgresql")
    if dialect != "postgresql":
        return
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


# Default domain rows seeded by schema.sql; FK targets other tenant rows depend
# on (domain_id='' must always resolve). Re-seeded on the portable path.
_SEED_DOMAINS: tuple[tuple[str, str], ...] = (
    ("", "No domain"),
    ("meta", "System metadata"),
    ("ops", "Operational telemetry"),
    ("shelter", "Animal shelter staff and breed management"),
)


async def _init_schema_portable(pool: "Database") -> None:
    """Bootstrap the tenant plane from portable SQLAlchemy metadata.

    ``schema.sql`` is PostgreSQL-only DDL (SERIAL/JSONB/DO $$/advisory locks) and
    does not parse on SQLite/MySQL. The ``schema_org`` metadata is the dialect-
    neutral mirror; ``create_all`` emits per-dialect DDL. Org isolation is the
    default schema on these single-tenant backends (no ``search_path``)."""
    from provisa.core import schema_org

    async with pool.engine.begin() as conn:
        await conn.run_sync(schema_org.metadata.create_all)
    async with pool.acquire() as conn:
        for domain_id, description in _SEED_DOMAINS:
            exists = await conn.fetchval("SELECT id FROM domains WHERE id = $1", domain_id)
            if exists is None:
                await conn.execute(
                    "INSERT INTO domains (id, description) VALUES ($1, $2)", domain_id, description
                )


async def init_schema(pool: "Database", schema_sql: str, org_id: str = "default") -> None:
    """Execute schema SQL scoped to org_<org_id> schema (REQ-697).

    PostgreSQL runs the raw ``schema.sql`` script inside an ``org_<id>`` schema.
    Non-PG backends bootstrap from portable ``schema_org`` metadata instead."""
    _validate_org_id(org_id)
    # A raw asyncpg pool (no Database shim) has no .dialect and is always PostgreSQL — run the
    # native schema.sql path. Only the portable SQLAlchemy Database routes non-PG backends.
    if getattr(pool, "dialect", "postgresql") != "postgresql":
        await _init_schema_portable(pool)
        return
    schema_name = f"org_{org_id}"
    async with pool.acquire() as conn:
        await conn.execute("SELECT pg_advisory_lock(7337)")
        try:
            await conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
            await conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}_mv_cache")
            await conn.execute(f"SET search_path TO {schema_name}")
            # schema_sql is a multi-statement script (DO $$ blocks). Raw asyncpg
            # runs it natively; the control-plane Database shim auto-detects the
            # multi-statement case and routes to the raw driver.
            await conn.execute(schema_sql)
        finally:
            await conn.execute("SELECT pg_advisory_unlock(7337)")


async def set_tenant_context(
    conn: asyncpg.Connection, tenant_id: str | None
) -> None:  # REQ-040, REQ-041
    """Set app.tenant_id session variable for RLS. Call at start of each transaction."""
    if tenant_id:
        await conn.execute(f"SET LOCAL app.tenant_id = '{tenant_id}'")
