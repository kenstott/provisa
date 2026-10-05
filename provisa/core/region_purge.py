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
region-qualified names — ``environments.org_schema``); every Redis key of its cache and Hot
counts; everything a region's engine keeps to reach the org's sources and the other regions'
replicas — a Trino coordinator's catalogs (with no platform regions, the deployment's own), a pg
engine's foreign servers (their user mappings and foreign tables with them) and its staging and
live-view schemas. Those are all named after the org's catalog names, ``org_<org>_...`` (an org
id has no underscore, REQ-1309, so the prefix names that org alone; ``naming.org_prefixed_catalog``,
``engine_attach_name``). The deleting node reaches each store through the model's declarations —
the same reach cross-region reads use.

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

#: The engine kinds that are a ClickHouse server or embedded store (``engine_kinds.ENGINE_KINDS``).
_CLICKHOUSE_KINDS = frozenset({"clickhouse", "clickhouse-server"})

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
    #: Trino coordinators (``trino://host:port``) -> the prefixes of the catalogs that go.
    catalogs: dict[str, set[str]] = field(default_factory=dict)
    #: A pg engine's database -> the prefixes of the catalog names whose attach objects go.
    attached: dict[str, set[str]] = field(default_factory=dict)
    #: A ClickHouse engine server -> the same, for its databases and staged tables.
    clickhouse: dict[str, set[str]] = field(default_factory=dict)
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
    dropped += await asyncio.to_thread(_drop_attached, plan)
    dropped += await asyncio.to_thread(_drop_clickhouse, plan)
    dropped += await asyncio.to_thread(_drop_catalogs, plan, org_id)
    await _delete_keys(plan)
    log.info("org %s: removed %d region schema(s) for %s", org_id, len(dropped), envs)
    return dropped


async def _plan(control_plane: "Database", org_id: str, envs: list[str]) -> _Purge:
    from provisa.cache.tenancy import place_key_patterns, place_of
    from provisa.core.database import Database
    from provisa.core.environments import PROD, SCHEMA_SUFFIXES, org_schema
    from provisa.core.repositories.region import list_regions, list_stores
    from provisa.core.secrets import resolve_secrets
    from provisa.federation.replica_hot import count_key_patterns, count_scope

    from provisa.core import process_region
    from provisa.core.database import Capabilities
    from provisa.core.region_stores import ENDPOINT_KINDS
    from provisa.core.regions import DEFAULT_REGION

    plan = _Purge()
    # The org itself goes when its prod does (prod is deleted only with the org, REQ-1487): then
    # every catalog of the org, else each environment's own.
    catalog_prefixes = (
        {f"org_{org_id}_".lower()}
        if PROD in envs
        else {f"org_{org_id}_env_{env}__".lower() for env in envs}
    )
    if process_region.region() == DEFAULT_REGION:
        # No platform regions: nothing is kept under a region's name. A deployment running on
        # Trino keeps the org's catalogs on its own coordinator — they go too.
        from provisa.federation.engine import configured_engine_endpoint, configured_engine_kind

        if configured_engine_kind() == "trino":
            host, port = configured_engine_endpoint()
            coordinator = f"trino://{host}:{port}"
            plan.reach[coordinator] = (DEFAULT_REGION, "engine")
            plan.catalogs[coordinator] = catalog_prefixes
        return plan
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
        kinds = {s.id: s.kind for s in stores}
        for region in regions:
            if kinds[region.engine] in ENDPOINT_KINDS:
                coordinator = _coordinator(declared[region.engine])
                plan.reach.setdefault(coordinator, (region.id, "engine"))
                plan.catalogs.setdefault(coordinator, set()).update(catalog_prefixes)
            elif kinds[region.engine] == "pg":
                # What the engine keeps to reach the org's sources live, and the other regions'
                # replicas: foreign servers (their user mappings and foreign tables with them),
                # staging and live-view schemas — each named after a catalog name of the org.
                engine_url = declared[region.engine]
                plan.reach.setdefault(engine_url, (region.id, "engine"))
                plan.attached.setdefault(engine_url, set()).update(catalog_prefixes)
            elif kinds[region.engine] in _CLICKHOUSE_KINDS:
                # Its databases a source is exposed under and its live views, and the staged
                # engine tables — each named after a catalog name of the org.
                engine_url = declared[region.engine]
                plan.reach.setdefault(engine_url, (region.id, "engine"))
                plan.clickhouse.setdefault(engine_url, set()).update(catalog_prefixes)
            names = {org_schema(org_id, env, s, region=region.id) for s in SCHEMA_SUFFIXES}
            for role in _SQL_ROLES:
                url = declared[getattr(region, role)]
                if not _is_sql(url):
                    continue
                plan.reach.setdefault(url, (region.id, role))
                plan.schemas.setdefault(url, set()).update(names)
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
    # Every store but a Redis (the cache), a Trino coordinator (an engine endpoint) and a ClickHouse
    # engine (its databases, the org's among them, go through drop_attached) holds schemas.
    return (
        not url.split(":", 1)[0]
        .lower()
        .startswith(("redis", "rediss", "trino", "clickhouse", "chdb"))
    )


def _coordinator(url: str) -> str:
    """A Trino engine store's coordinator, as ``trino://host:port``."""
    from sqlalchemy import make_url

    parsed = make_url(url)
    if parsed.host is None or parsed.port is None:  # refused at load (region_stores.region_lane)
        raise ValueError(f"a Trino engine store needs a host and a port in its URL: {url!r}")
    return f"trino://{parsed.host}:{parsed.port}"


def _trino(coordinator: str, org_id: str):
    """A connection to ``coordinator`` as the engine user, its statements the org's (REQ-056)."""
    from provisa.federation.trino_lifecycle import ENGINE_USER, connect, engine_source

    host, port = coordinator.removeprefix("trino://").rsplit(":", 1)
    return connect(
        dict(
            host=host,
            port=int(port),
            user=ENGINE_USER,
            source=engine_source(org_id),
            catalog="system",
            http_scheme="http",
            request_timeout=10,
        )
    )


def _org_catalogs(conn, prefixes: set[str]) -> list[str]:
    cur = conn.cursor()
    cur.execute("SHOW CATALOGS")
    return sorted(row[0] for row in cur.fetchall() if row[0].startswith(tuple(prefixes)))


def _drop_catalogs(plan: _Purge, org_id: str) -> list[str]:
    dropped: list[str] = []
    for coordinator, prefixes in plan.catalogs.items():
        conn = _trino(coordinator, org_id)
        try:
            for name in _org_catalogs(conn, prefixes):
                cur = conn.cursor()
                cur.execute(f'DROP CATALOG IF EXISTS "{name}"')
                cur.fetchall()
                dropped.append(name)
        finally:
            conn.close()
    return dropped


def _require_reachable(plan: _Purge, org_id: str) -> None:
    """Reach every store before anything is removed (all or none)."""
    for url, (region, role) in plan.reach.items():
        try:
            if url in plan.clickhouse:
                runtime = _clickhouse(url)
                try:
                    runtime.run_sync("SELECT 1")
                finally:
                    runtime.close()
            elif url in plan.catalogs:
                conn = _trino(url, org_id)
                try:
                    _org_catalogs(conn, plan.catalogs[url])
                finally:
                    conn.close()
            elif url in plan.keys:
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


def _drop_attached(plan: _Purge) -> list[str]:
    """Drop, in each pg engine database, the foreign servers (with their user mappings and
    foreign tables) and the schemas named after the org's catalog names: ``<kind>_<catalog>``
    (``naming.ATTACH_KINDS``) and the live-view schemas ``<catalog>_<schema>``."""
    from sqlalchemy import create_engine, text

    from provisa.compiler.naming import ATTACH_KINDS

    dropped: list[str] = []
    for url, prefixes in plan.attached.items():
        owned = tuple(f"{kind}_{p}" for kind in ATTACH_KINDS for p in prefixes)
        engine = create_engine(_sync(url), connect_args=_timeout(url))
        try:
            with engine.begin() as conn:
                servers = [
                    r[0] for r in conn.execute(text("SELECT srvname FROM pg_foreign_server"))
                ]
                for name in sorted(s for s in servers if s.startswith(owned)):
                    conn.execute(text(f'DROP SERVER IF EXISTS "{name}" CASCADE'))
                    dropped.append(name)
                schemas = [r[0] for r in conn.execute(text("SELECT nspname FROM pg_namespace"))]
                for name in sorted(s for s in schemas if s.startswith(owned + tuple(prefixes))):
                    conn.execute(text(f'DROP SCHEMA IF EXISTS "{name}" CASCADE'))
                    dropped.append(name)
        finally:
            engine.dispose()
    return dropped


def _clickhouse(url: str):
    from provisa.federation.clickhouse_runtime import ClickHouseFederationRuntime

    return ClickHouseFederationRuntime.from_url(url)


def _drop_clickhouse(plan: _Purge) -> list[str]:
    dropped: list[str] = []
    for url, prefixes in plan.clickhouse.items():
        runtime = _clickhouse(url)
        try:
            dropped += runtime.drop_attached(prefixes)
        finally:
            runtime.close()
    return dropped


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
