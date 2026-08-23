# Copyright (c) 2026 Kenneth Stott
# Canary: b3e17f4a-9c2d-4e8b-a1f0-c5d92e74b803
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Atomic org provisioning — PG schema, PG role, Redis ACL, the engine cache schema.

REQ-700, REQ-701
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from provisa.core.environments import env_schemas

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
    env: str | None = None,
) -> None:
    """Atomically provision all infrastructure for a new org, or for one of its environments.

    Steps (with compensating rollback on failure):
      1. Create org PG schema + run schema SQL
      2. Create PG role scoped to org schema
      3. Provision Redis ACL user (if redis_url provided)

    REQ-1488: *env* provisions an environment of an already-provisioned org — its own schema, its
    own audit log, and a grant of that schema to the org's ONE role, because REQ-1487 keeps
    identity at the org. Redis is skipped for an environment for the same reason: the ACL user is
    the org's, and it already exists. ``None`` and ``prod`` provision the org itself.

    Idempotent — safe to call on an existing org.
    """
    from provisa.audit.query_log import init_audit_schema
    from provisa.core.db import _validate_org_id, create_org_role, init_schema
    from provisa.core.environments import (
        PROD,
        EnvironmentPlaneError,
        org_schema,
        validate_env_name,
    )

    _validate_org_id(org_id)
    if env is not None and env != PROD:
        # REQ-1523: refused against THIS org's id, before anything is provisioned — the length a
        # name may reach is what PostgreSQL's identifier limit leaves after this org's own id.
        validate_env_name(org_id, env)
        # REQ-1488: the environment IS a schema. A plane without schemas has nowhere to put it,
        # and the portable bootstrap below ignores *env* entirely — so provisioning here would
        # report success while writing the new environment's model into the org's only namespace.
        if getattr(pool, "dialect", "postgresql") != "postgresql":
            raise EnvironmentPlaneError(
                f"environments need a PostgreSQL control plane; this one is "
                f"{getattr(pool, 'dialect', 'postgresql')!r}"
            )
    schema_name = org_schema(org_id, env)

    provisioned_pg = False
    provisioned_role = False
    provisioned_redis = False

    try:
        await init_schema(pool, schema_sql, org_id=org_id, env=env)
        await init_audit_schema(pool, org_id=org_id, env=env)
        provisioned_pg = True

        async with pool.acquire() as conn:
            await create_org_role(conn, org_id, env=env)  # type: ignore[arg-type]
        provisioned_role = True

        if redis_url and redis_password and (env is None or env == PROD):
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
        # An environment rolls back only what it created. The role is the ORG's and predates this
        # environment, so dropping it here would deprovision the org a failed environment was
        # being added to.
        rollback_role = provisioned_role and (env is None or env == PROD)
        if rollback_role and pool.dialect == "postgresql":  # REQ-889: PG-only role hardening
            try:
                async with pool.acquire() as conn:
                    await conn.execute(f"DROP ROLE IF EXISTS role_{org_id}")
            except Exception as drop_exc:
                log.warning("Rollback: DROP ROLE role_%s failed: %s", org_id, drop_exc)
        # Only PostgreSQL put schemas there to drop; the portable bootstrap creates tables in the
        # one namespace the backend has, and DROP SCHEMA does not parse on it at all.
        if provisioned_pg and getattr(pool, "dialect", "postgresql") == "postgresql":
            for schema in env_schemas(org_id, env):
                try:
                    async with pool.acquire() as conn:
                        await conn.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
                except Exception as drop_exc:
                    log.warning("Rollback: DROP SCHEMA %s failed: %s", schema, drop_exc)
        raise


async def deprovision_org(  # REQ-701
    pool: "Database",
    org_id: str,
    redis_url: str | None = None,
    env: str | None = None,
) -> None:
    """Remove all infrastructure for an org, or for one of its environments (REQ-1488).

    Drops Redis ACL user, PG role, and PG schemas in reverse order. Every store schema the
    environment owns is dropped, not only its base one: the caches are what REQ-1046 meters, so a
    cache left behind is a tenant still being billed for an environment that no longer exists.

    *env* deletes one environment of a surviving org, so it keeps the org's role and its Redis ACL
    user. ``prod`` is not accepted here — REQ-1487 gives it to the org at creation and forbids
    deleting it; the org is deleted instead, by calling this with no *env*.
    """
    from provisa.core.db import _validate_org_id
    from provisa.core.environments import PROD, EnvironmentNameError

    _validate_org_id(org_id)
    if env == PROD:
        raise EnvironmentNameError(
            f"{PROD!r} cannot be deleted; delete the organization to remove it"
        )
    org_itself = env is None

    if redis_url and org_itself:
        await deprovision_redis_acl(redis_url, org_id)

    async with pool.acquire() as conn:
        if (
            getattr(pool, "dialect", "postgresql") == "postgresql"
        ):  # REQ-889: PG-only role hardening
            if org_itself:
                await conn.execute(f"DROP ROLE IF EXISTS role_{org_id}")
            # Schemas exist only on PostgreSQL. On a portable backend the org's tables are the
            # backend's own namespace, which this call does not own and must not drop.
            for schema in env_schemas(org_id, env):
                await conn.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")

    if org_itself:
        log.info("Org %r deprovisioned", org_id)
    else:
        log.info("Org %r environment %r deprovisioned", org_id, env)
