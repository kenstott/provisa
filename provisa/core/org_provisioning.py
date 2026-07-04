# Copyright (c) 2026 Kenneth Stott
# Canary: b3e17f4a-9c2d-4e8b-a1f0-c5d92e74b803
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Atomic org provisioning — PG schema, PG role, Redis ACL, Trino cache schema.

REQ-700, REQ-701
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from provisa.core.database import Database

log = logging.getLogger(__name__)


async def provision_redis_acl(redis_url: str, org_id: str, password: str) -> None:  # REQ-700
    """Create or replace a Redis ACL user scoped to this org's key prefixes."""
    import redis.asyncio as aioredis

    from provisa.core.db import _validate_org_id

    _validate_org_id(org_id)
    r = aioredis.from_url(redis_url, decode_responses=True)
    try:
        key_patterns = [
            f"provisa:cache:{org_id}:*",
            f"provisa:table:{org_id}:*",
            f"provisa:apq:{org_id}:*",
            f"provisa:hot:{org_id}:*",
        ]
        pattern_args = []
        for p in key_patterns:
            pattern_args += ["~" + p]
        await r.execute_command(
            "ACL",
            "SETUSER",
            f"org_{org_id}",
            "on",
            f">{password}",
            *pattern_args,
            "+@read",
            "+@write",
            "+@connection",
            "+@keyspace",
        )
        log.info("Redis ACL user org_%s provisioned", org_id)
    finally:
        await r.aclose()


async def deprovision_redis_acl(redis_url: str, org_id: str) -> None:  # REQ-701
    """Delete the Redis ACL user for this org."""
    import redis.asyncio as aioredis

    r = aioredis.from_url(redis_url, decode_responses=True)
    try:
        await r.execute_command("ACL", "DELUSER", f"org_{org_id}")
        log.info("Redis ACL user org_%s deleted", org_id)
    except Exception as exc:
        log.warning("Redis ACL DELUSER org_%s failed (may not exist): %s", org_id, exc)
    finally:
        await r.aclose()


async def provision_org(  # REQ-701
    pool: "Database",
    schema_sql: str,
    org_id: str,
    redis_url: str | None = None,
    redis_password: str | None = None,
) -> None:
    """Atomically provision all infrastructure for a new org.

    Steps (with compensating rollback on failure):
      1. Create org PG schema + run schema SQL
      2. Create PG role scoped to org schema
      3. Provision Redis ACL user (if redis_url provided)

    Idempotent — safe to call on an existing org.
    """
    from provisa.audit.query_log import init_audit_schema
    from provisa.core.db import _validate_org_id, create_org_role, init_schema

    _validate_org_id(org_id)
    schema_name = f"org_{org_id}"

    provisioned_pg = False
    provisioned_role = False
    provisioned_redis = False

    try:
        await init_schema(pool, schema_sql, org_id=org_id)
        await init_audit_schema(pool, org_id=org_id)
        provisioned_pg = True

        async with pool.acquire() as conn:
            await create_org_role(conn, org_id)  # type: ignore[arg-type]
        provisioned_role = True

        if redis_url and redis_password:
            await provision_redis_acl(redis_url, org_id, redis_password)
            provisioned_redis = True

        log.info(
            "Org %r provisioned: schema=%s role=role_%s redis=%s",
            org_id,
            schema_name,
            org_id,
            provisioned_redis,
        )

    except Exception:
        log.error("Org provisioning failed for %r — rolling back", org_id, exc_info=True)
        # Compensating rollback in reverse order
        if provisioned_redis and redis_url:
            await deprovision_redis_acl(redis_url, org_id)
        if provisioned_role and pool.dialect == "postgresql":  # REQ-889: PG-only role hardening
            try:
                async with pool.acquire() as conn:
                    await conn.execute(f"DROP ROLE IF EXISTS role_{org_id}")
            except Exception as drop_exc:
                log.warning("Rollback: DROP ROLE role_%s failed: %s", org_id, drop_exc)
        if provisioned_pg:
            try:
                async with pool.acquire() as conn:
                    await conn.execute(f"DROP SCHEMA IF EXISTS {schema_name} CASCADE")
            except Exception as drop_exc:
                log.warning("Rollback: DROP SCHEMA %s failed: %s", schema_name, drop_exc)
        raise


async def deprovision_org(  # REQ-701
    pool: "Database",
    org_id: str,
    redis_url: str | None = None,
) -> None:
    """Remove all infrastructure for an org (deprovisioning).

    Drops Redis ACL user, PG role, and PG schema in reverse order.
    """
    from provisa.core.db import _validate_org_id

    _validate_org_id(org_id)
    schema_name = f"org_{org_id}"

    if redis_url:
        await deprovision_redis_acl(redis_url, org_id)

    async with pool.acquire() as conn:
        if pool.dialect == "postgresql":  # REQ-889: PG-only role hardening — no-op elsewhere
            await conn.execute(f"DROP ROLE IF EXISTS role_{org_id}")
        await conn.execute(f"DROP SCHEMA IF EXISTS {schema_name} CASCADE")

    log.info("Org %r deprovisioned", org_id)
