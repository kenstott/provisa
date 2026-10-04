# Copyright (c) 2026 Kenneth Stott
# Canary: 6fc1149c-1717-4f8f-97ea-93e4490fad59
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What an org, or one of its environments, keeps in its regions goes with it (REQ-1921,
REQ-1922).

Deleting an org or an environment removes, in every region store the org declares, every schema
of that environment (its state, record, replicas, views, caches and exports, under the
region-qualified names — ``environments.org_schema``), the tables another region's engine imported
from those replicas, and every Redis key of its cache and Hot counts. The deleting node reaches
each store through the model's declarations — the same reach cross-region reads use.

All of it or none: every store is reached first, and a store that cannot be reached refuses the
delete, naming its region, before anything is removed.
"""

# Requirements: REQ-1921, REQ-1922

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from provisa.core.database import Database

log = logging.getLogger(__name__)

#: The stores of a region that hold an environment's schemas.
_SQL_ROLES = ("engine", "state", "record", "replicas", "views")


class RegionStoreUnreachable(RuntimeError):
    """A region store an org's delete must clear cannot be reached: the delete is refused."""

    code = "orgs.region_store_unreachable"

    def __init__(self, org_id: str, region: str, role: str, cause: str) -> None:
        self.params = {"org": org_id, "region": region, "store": role}
        super().__init__(
            f"org {org_id!r} cannot be deleted now: its {role} store in region {region!r} cannot "
            f"be reached ({cause}); nothing was removed"
        )


@dataclass
class _Purge:
    """What one delete removes: SQL schemas by store URL, and Redis patterns by cache URL."""

    schemas: dict[str, set[str]] = field(default_factory=dict)
    keys: dict[str, set[str]] = field(default_factory=dict)
    reach: dict[str, tuple[str, str]] = field(default_factory=dict)  # url -> (region, role)


async def purge_org_regions(control_plane: "Database", org_id: str, envs: list[str]) -> list[str]:
    """Remove what ``org_id``'s environments ``envs`` keep in every region store their models
    declare; returns the schemas dropped. Raises :class:`RegionStoreUnreachable` — having removed
    nothing — when a store cannot be reached."""
    import asyncio

    plan = await _plan(control_plane, org_id, envs)
    if not plan.reach:
        return []
    await asyncio.to_thread(_require_reachable, plan, org_id)
    dropped = await asyncio.to_thread(_drop_schemas, plan)
    await _delete_keys(plan)
    log.info("org %s: removed %d region schema(s) for %s", org_id, len(dropped), envs)
    return dropped


async def _plan(control_plane: "Database", org_id: str, envs: list[str]) -> _Purge:
    from provisa.cache.tenancy import place_key_patterns, place_of
    from provisa.core.database import Database
    from provisa.core.environments import PROD, SCHEMA_SUFFIXES, org_schema
    from provisa.core.repositories.region import list_regions, list_stores
    from provisa.core.secrets import resolve_secrets
    from provisa.federation.replica_address import REPLICAS_SUFFIX
    from provisa.federation.replica_hot import count_key_patterns, count_scope

    from provisa.core import process_region
    from provisa.core.database import Capabilities
    from provisa.core.regions import DEFAULT_REGION

    plan = _Purge()
    if process_region.region() == DEFAULT_REGION:
        return plan  # no platform regions: nothing is kept under a region's name
    if not Capabilities.for_dialect(control_plane.engine.dialect.name).schemas:
        raise RuntimeError(
            "regions are declared, so the control plane must hold each org in a schema of its own "
            f"(it is {control_plane.engine.dialect.name})"
        )
    for env in envs:
        if not await _schema_exists(control_plane, org_schema(org_id, env)):
            continue  # never provisioned (a failed org): it declared nothing
        model = Database(
            control_plane.engine,
            name="org-model-purge",
            search_path=org_schema(org_id, env),
            holds="model",
        )
        async with model.acquire() as conn:
            regions, stores = await list_regions(conn), await list_stores(conn)
        declared = {s.id: resolve_secrets(s.url) for s in stores}
        for region in regions:
            names = {org_schema(org_id, env, s, region=region.id) for s in SCHEMA_SUFFIXES}
            # What the org's other regions' engines imported from this region's replicas.
            imported = {
                f"region_{region.id}__{org_schema(org_id, env, REPLICAS_SUFFIX, region=region.id)}"
            }
            for role in _SQL_ROLES:
                url = declared[getattr(region, role)]
                if not _is_sql(url):
                    continue
                plan.reach.setdefault(url, (region.id, role))
                plan.schemas.setdefault(url, set()).update(names)
            for other in regions:
                url = declared[other.engine]
                if other.id != region.id and _is_sql(url):
                    plan.reach.setdefault(url, (other.id, "engine"))
                    plan.schemas.setdefault(url, set()).update(imported)
            cache = declared[region.cache]
            plan.reach.setdefault(cache, (region.id, "cache"))
            plan.keys.setdefault(cache, set()).update(
                place_key_patterns(place_of(org_id, env, region.id))
                + count_key_patterns(count_scope(org_id, env or PROD, region=region.id))
            )
    return plan


async def _schema_exists(control_plane: "Database", schema: str) -> bool:
    from sqlalchemy import text

    async with control_plane.acquire() as conn:
        found = await conn.execute_core(
            text("SELECT 1 FROM information_schema.schemata WHERE schema_name = :s").bindparams(
                s=schema
            )
        )
        return found.fetchone() is not None


def _is_sql(url: str) -> bool:
    # Every store but a Redis (the cache) and a Trino coordinator (an engine endpoint) holds schemas.
    return not url.split(":", 1)[0].lower().startswith(("redis", "rediss", "trino"))


def _require_reachable(plan: _Purge, org_id: str) -> None:
    """Reach every store before anything is removed (all or none)."""
    for url, (region, role) in plan.reach.items():
        try:
            if url in plan.keys:
                _redis(url).ping()
            else:
                from sqlalchemy import create_engine, text

                engine = create_engine(_sync(url), connect_args=_timeout(url))
                try:
                    with engine.connect() as conn:
                        conn.execute(text("SELECT 1"))
                finally:
                    engine.dispose()
        except Exception as exc:  # allow-ble: any failure to reach the store refuses the delete, naming it; nothing has been removed
            raise RegionStoreUnreachable(org_id, region, role, type(exc).__name__) from exc


def _drop_schemas(plan: _Purge) -> list[str]:
    from sqlalchemy import create_engine, text

    dropped: list[str] = []
    for url, names in plan.schemas.items():
        engine = create_engine(_sync(url), connect_args=_timeout(url))
        try:
            with engine.begin() as conn:
                for name in sorted(names):
                    conn.execute(text(f'DROP SCHEMA IF EXISTS "{name}" CASCADE'))
                    dropped.append(name)
        finally:
            engine.dispose()
    return dropped


async def _delete_keys(plan: _Purge) -> None:
    import asyncio

    def _run(url: str, patterns: set[str]) -> None:
        client = _redis(url)
        for pattern in sorted(patterns):
            keys = list(client.scan_iter(match=pattern))
            if keys:
                client.delete(*keys)

    for url, patterns in plan.keys.items():
        await asyncio.to_thread(_run, url, patterns)


def _redis(url: str):
    from provisa.core.redis_factory import make_redis

    return make_redis(url, decode_responses=True).client()


def _sync(url: str) -> str:
    """A synchronous SQLAlchemy URL for a store DSN (a bare ``postgresql://`` gets psycopg)."""
    scheme, sep, rest = url.partition("://")
    if scheme in ("postgresql", "postgres"):
        return f"postgresql+psycopg://{rest}"
    return url


def _timeout(url: str) -> dict:
    return {"connect_timeout": 5} if url.startswith(("postgresql", "postgres")) else {}
