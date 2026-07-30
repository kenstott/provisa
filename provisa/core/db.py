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
from sqlalchemy import insert, select
from sqlalchemy.schema import CreateSchema

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
    from provisa.core.schema_org import domains

    async with pool.engine.begin() as conn:
        await conn.run_sync(schema_org.metadata.create_all)
    async with pool.acquire() as conn:
        for domain_id, description in _SEED_DOMAINS:
            result = await conn.execute_core(select(domains.c.id).where(domains.c.id == domain_id))
            if result.fetchone() is None:
                await conn.execute_core(
                    insert(domains).values(id=domain_id, description=description)
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
        # This branch is PostgreSQL-only (non-PG returned above); the advisory lock is taken through
        # the abstraction so no PG-specific lock SQL appears here.
        async with conn.advisory_lock(7337):
            await conn.execute_core(CreateSchema(schema_name, if_not_exists=True))
            await conn.execute_core(CreateSchema(f"{schema_name}_mv_cache", if_not_exists=True))
            await conn.execute(f"SET search_path TO {schema_name}")
            # schema_sql is a multi-statement script (DO $$ blocks). Raw asyncpg
            # runs it natively; the control-plane Database shim auto-detects the
            # multi-statement case and routes to the raw driver.
            await conn.execute(schema_sql)


async def apply_tenancy_role_grants(  # REQ-1337
    pool: "Database", org_id: str, *, multitenancy: bool
) -> None:
    """Assert the tenancy-dependent role grants: ``platform_settings`` and ``cross_org``.

    The deployment-wide settings surface (federation engine, cache storage, encryption, auth
    provider, the config file, query-engine lifecycle) is gated by the ``platform_settings`` RIGHT,
    never by a role name. Which roles hold that right is the only thing tenancy decides:

    * single-tenant — the org administrator IS the deployment operator, so org_admin holds it;
    * multitenant — an org administers only its own data plane, so org_admin must NOT hold it and
      the grant is withdrawn (a deployment flipped to multitenant keeps no stale right).

    Runs on every ``init_schema``, so the seed re-asserts the mode's grant rather than depending on
    when the schema was first created. platform_admin always holds it (seeded in schema.sql).
    """
    _validate_org_id(org_id)
    if getattr(pool, "dialect", "postgresql") != "postgresql":
        return  # portable bootstrap seeds no system roles; nothing to re-assert
    async with pool.acquire() as conn:
        await conn.execute(f"SET search_path TO org_{org_id}")
        # REQ-1337: cross_org is withdrawn in BOTH modes — org authority is confined to the org
        # being acted in, so org_admin never holds it however the deployment is configured. Only
        # platform_admin carries it (schema.sql), and holding it is what marks a role control-plane.
        await conn.execute(
            "UPDATE roles SET capabilities = COALESCE("
            "  (SELECT jsonb_agg(v) FROM jsonb_array_elements(capabilities) v"
            "   WHERE v <> '\"cross_org\"'::jsonb), '[]'::jsonb)"
            " WHERE id <> 'platform_admin' AND capabilities ? 'cross_org'"
        )
        if multitenancy:
            await conn.execute(
                "UPDATE roles SET capabilities = COALESCE("
                "  (SELECT jsonb_agg(v) FROM jsonb_array_elements(capabilities) v"
                "   WHERE v <> '\"platform_settings\"'::jsonb), '[]'::jsonb)"
                " WHERE id = 'org_admin' AND capabilities ? 'platform_settings'"
            )
        else:
            await conn.execute(
                "UPDATE roles SET capabilities = capabilities || '[\"platform_settings\"]'::jsonb"
                " WHERE id = 'org_admin' AND NOT capabilities ? 'platform_settings'"
            )


async def set_tenant_context(
    conn: asyncpg.Connection, tenant_id: str | None
) -> None:  # REQ-040, REQ-041
    """Set app.tenant_id session variable for RLS. Call at start of each transaction."""
    if tenant_id:
        await conn.execute(f"SET LOCAL app.tenant_id = '{tenant_id}'")
