# Copyright (c) 2026 Kenneth Stott
# Canary: 4f7c1d90-8b2e-4a63-95d1-6e0837bca42f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An environment's replicas live in a schema the environment owns (REQ-1622, REQ-1912).

A replica is addressed by store DSN, schema, and a table named for its source, schema and table.
The source's identity is the same in every environment, so the table name cannot tell two
environments apart: the SCHEMA does. Every org environment has its own replicas schema,
``org_<org>[_env_<env>]_replicas`` (``replica_address.replica_schema``), so two environments of
one org never write the same replica, and two orgs never share a schema.

What is left here is the other half of that ownership: when an environment is retired, the
replicas schema it wrote in the store goes with it. The store is a different DSN from the tenant
pool the environment's own schemas are dropped from, so nothing else reaches it (REQ-1620: nothing
an expiring environment writes may outlive it).
"""

# Requirements: REQ-1622, REQ-1620, REQ-1487, REQ-1912

from __future__ import annotations

import logging

from provisa.core.environments import PROD, org_schema
from provisa.federation.replica_address import SYNTHETIC_INFIX

log = logging.getLogger(__name__)


async def drop_env_store(dsn: str, org_id: str, env: str) -> str | None:
    """Remove the replicas ``env`` of ``org_id`` wrote in the store at ``dsn``, and the export
    views published over them. Returns the replicas schema dropped, or None.

    Called from the one retire door. ``prod`` is never retired, so its replicas schema is never
    dropped from here: the only schemas this deletes are ones that exist because a non-prod
    environment existed.
    """
    if env == PROD:
        return None
    from sqlalchemy import text

    from provisa.federation.replica_address import EXPORT_SUFFIX, REPLICAS_SUFFIX
    from provisa.federation.store_writer import store_connection

    from provisa.core import process_region

    # REQ-1922: this node's region's stores, named for it.
    region = process_region.region()
    schema = org_schema(org_id, env, REPLICAS_SUFFIX, region=region)
    # The export views first: each selects from a replica the next statement drops.
    export = org_schema(org_id, env, EXPORT_SUFFIX, region=region)
    # REQ-1939: and every synthetic dataset's schema the environment held.
    synthetic_prefix = org_schema(org_id, env) + SYNTHETIC_INFIX
    async with store_connection(dsn) as conn:
        await conn.execute_core(text(f'DROP SCHEMA IF EXISTS "{export}" CASCADE'))
        await conn.execute_core(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        names = (
            await conn.execute_core(text("SELECT schema_name FROM information_schema.schemata"))
        ).fetchall()
        for (name,) in names:
            if name.startswith(synthetic_prefix):
                await conn.execute_core(text(f'DROP SCHEMA IF EXISTS "{name}" CASCADE'))
    log.info("Environment %r dropped its replicas schema %s", env, schema)
    return schema


async def drop_synthetic_schema(dsn: str, schema: str) -> None:
    """Remove a synthetic dataset's schema from the store at ``dsn`` (REQ-1939)."""
    from sqlalchemy import text

    from provisa.federation.replica_address import is_replicas_schema
    from provisa.federation.store_writer import store_connection

    if SYNTHETIC_INFIX not in schema or not is_replicas_schema(schema):
        raise ValueError(f"{schema!r} is not a synthetic dataset's schema")
    async with store_connection(dsn) as conn:
        await conn.execute_core(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
    log.info("Dropped synthetic dataset schema %s", schema)


async def drop_synthetic_table(dsn: str, schema: str, table: str) -> None:
    """Remove one table from a synthetic dataset's schema in the store at ``dsn``: a real sample
    landed for generation only (REQ-1939, NOT TOO CLOSE TO A REAL ROW; maintainer ruling W1)."""
    from sqlalchemy import text

    from provisa.federation.replica_address import is_replicas_schema
    from provisa.federation.store_writer import store_connection

    if SYNTHETIC_INFIX not in schema or not is_replicas_schema(schema):
        raise ValueError(f"{schema!r} is not a synthetic dataset's schema")
    quoted = '"' + table.replace('"', '""') + '"'
    async with store_connection(dsn) as conn:
        await conn.execute_core(text(f'DROP TABLE IF EXISTS "{schema}".{quoted}'))
    log.info("Dropped %s from synthetic dataset schema %s", table, schema)
