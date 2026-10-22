# Copyright (c) 2026 Kenneth Stott
# Canary: 1c6a74fc-eea3-4124-b1a0-e9ee328c12f3
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A demo whose model selects regions, on the regions sandbox tier (REQ-2068).

That tier runs one node, of the demo's visitor region. Each region keeps its replicas, its views
and their record in its own PostgreSQL store; another region's are read there in place, and no
node of that region runs: what it keeps is landed once, by the bundle build.

- The bundle build runs a seeding node per region. Each writes what its region keeps -- its
  replicas, its views and their record -- out of its store, into ``seeded/regions/<region>/``.
- A template is landed from those seeds, each region's into that region's store.
- A sandbox copies its template's, region by region, within each region's store.

The region this node serves records its replicas as built for its own store, from its own model
(as a template of the single-region tier does, demo_replicas). Another region's record is carried
as that region wrote it: no node of it runs to build anything again, and a read here asks only
whether it is built (query_residency.require_home_replica).
"""

# Requirements: REQ-2068, REQ-2006, REQ-1922, REQ-1921

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

#: Where the bundle carries each region's seed, under the demo's ``seeded`` directory.
REGIONS_DIR = "regions"
_STORE = "store.duckdb"
_RECORD = "record.json"
#: The schemas a region's seed keeps its replicas and its views in.
_REPLICAS = "replicas"
_VIEWS = "views"


def regional() -> bool:
    """Whether this node serves a platform region: the regions sandbox tier's does."""
    from provisa.core import process_region
    from provisa.core.regions import DEFAULT_REGION

    return process_region.region() != DEFAULT_REGION


def here() -> str:
    from provisa.core import process_region

    return process_region.region()


@dataclass(frozen=True)
class RegionStore:
    """What one region of an org keeps, and where: its replicas' and views' store URLs (resolved)
    and the store its state is kept in."""

    region: str
    replicas_url: str
    views_url: str
    state_url: str


async def org_region_stores(org_id: str) -> list[RegionStore]:
    """Each region ``org_id``'s model selects, with its stores, read from its model."""
    from provisa.api.admin.orgs_router import _pool
    from provisa.core.database import Database
    from provisa.core.environments import org_schema
    from provisa.core.repositories.region import list_regions, list_stores
    from provisa.core.secrets import resolve_secrets

    model = Database(
        _pool().engine, name="org-model", search_path=org_schema(org_id), holds="model"
    )
    async with model.acquire() as conn:
        regions = await list_regions(conn)
        stores = {s.id: s.url for s in await list_stores(conn)}
    return [
        RegionStore(
            r.id,
            resolve_secrets(stores[r.replicas]),
            resolve_secrets(stores[r.views]),
            resolve_secrets(stores[r.state]),
        )
        for r in regions
    ]


def _schema(org_id: str, suffix: str, region: str) -> str:
    from provisa.core.environments import org_schema

    return org_schema(org_id, None, suffix, region=region)


def replicas_schema(org_id: str, region: str) -> str:
    from provisa.federation.replica_address import REPLICAS_SUFFIX

    return _schema(org_id, REPLICAS_SUFFIX, region)


def views_schema(org_id: str, region: str) -> str:
    from provisa.federation.replica_address import MVS_SUFFIX

    return _schema(org_id, MVS_SUFFIX, region)


def state_db(org_id: str, store: RegionStore) -> Any:
    """``org_id``'s state in ``store``'s region, under the name that region keeps it by."""
    from provisa.core.database import Database
    from provisa.core.environments import org_schema
    from provisa.core.region_stores import _engine_for

    return Database(
        _engine_for(store.state_url, pool_size=2, max_overflow=0),
        name=f"org-state-{store.region}",
        search_path=org_schema(org_id, None, region=store.region),
        holds="state",
    )


async def lay_out_state(org_id: str, store: RegionStore) -> None:
    """Lay out ``org_id``'s state schema in another region's state store, where no node of that
    region runs to (region_stores.bind_region_stores, for the region a node serves)."""
    from provisa.core.database import Database
    from provisa.core.db import init_schema
    from provisa.core.environments import org_schema
    from provisa.core.region_stores import _engine_for

    schema_sql = (Path(__file__).parent.parent / "core" / "schema.sql").read_text()
    layout = Database(
        _engine_for(store.state_url, pool_size=2, max_overflow=0),
        name="org-layout",
        search_path=org_schema(org_id, None, region=store.region),
    )
    await init_schema(layout, schema_sql, org_id=org_id, region=store.region)


# -- moving tables -----------------------------------------------------------------------------


def _attach(con: Any, alias: str, dsn: str, *, read_only: bool) -> None:
    from sqlalchemy import make_url

    url = make_url(dsn)
    mode = ", READ_ONLY" if read_only else ""
    if url.get_backend_name() == "duckdb":
        con.execute(f"ATTACH '{url.database}' AS {alias} (TYPE duckdb{mode})")
        return
    from provisa.federation.duckdb_extensions import install_and_load
    from provisa.federation.native_backend import libpq_dsn

    install_and_load(con, "postgres", from_community=False)
    con.execute(f"ATTACH '{libpq_dsn(url)}' AS {alias} (TYPE postgres{mode})")


def copy_tables(
    src: str, src_schema: str, dst: str, dst_schema: str, tables: list[str] | None = None
) -> list[str]:
    """Copy every table of ``src_schema`` in the store ``src`` -- or those of ``tables`` it holds
    -- into ``dst_schema`` of the store ``dst``, rows and all, each replacing a table of its name
    there; the tables copied. Either store is a DuckDB file (a seed) or a PostgreSQL store (a
    region's); one store may be both, a schema copied within it."""
    import duckdb

    con = duckdb.connect()
    try:
        same = src == dst
        _attach(con, "copy_src", src, read_only=not same)
        target = "copy_src" if same else "copy_dst"
        if not same:
            _attach(con, "copy_dst", dst, read_only=False)
        names = [
            r[0]
            for r in con.execute(
                "SELECT table_name FROM duckdb_tables() WHERE database_name = 'copy_src' "
                "AND schema_name = ? ORDER BY table_name",
                [src_schema],
            ).fetchall()
            if tables is None or r[0] in tables
        ]
        con.execute(f'CREATE SCHEMA IF NOT EXISTS {target}."{dst_schema}"')
        for name in names:
            con.execute(f'DROP TABLE IF EXISTS {target}."{dst_schema}"."{name}"')
            con.execute(
                f'CREATE TABLE {target}."{dst_schema}"."{name}" AS '
                f'SELECT * FROM copy_src."{src_schema}"."{name}"'
            )
        return names
    finally:
        con.close()


def held_rows(dsn: str, schema: str, tables: list[str]) -> dict[str, int]:
    """The rows each of ``tables`` holds in ``schema`` of the store ``dsn``; one not there is left
    out."""
    import duckdb

    con = duckdb.connect()
    try:
        _attach(con, "held", dsn, read_only=True)
        present = {
            r[0]
            for r in con.execute(
                "SELECT table_name FROM duckdb_tables() WHERE database_name = 'held' "
                "AND schema_name = ?",
                [schema],
            ).fetchall()
        }
        return {
            name: int(
                con.execute(f'SELECT count(*) FROM held."{schema}"."{name}"').fetchall()[0][0]
            )
            for name in tables
            if name in present
        }
    finally:
        con.close()


# -- records -----------------------------------------------------------------------------------


async def _records(db: Any) -> tuple[list[dict], list[dict]]:
    """A region's record of an org: its replicas' completed builds and its views' builds."""
    from sqlalchemy import select

    from provisa.core.schema_org import mv_build_state
    from provisa.federation import replica_state

    async with db.acquire() as conn:
        replicas = await replica_state.completed_rows(conn)
        views = [
            dict(r._mapping) for r in (await conn.execute_core(select(mv_build_state))).fetchall()
        ]
    return replicas, views


async def _write_records(
    db: Any, replicas: list[dict], views: list[dict], *, replace_replicas: bool
) -> None:
    from sqlalchemy import delete, insert

    from provisa.core.schema_org import mv_build_state
    from provisa.federation import replica_state

    async with db.acquire() as conn:
        if replace_replicas:
            await replica_state.record_adopted(conn, replicas)
        else:
            await replica_state.record_copied(conn, replicas)
        async with conn.transaction():
            for row in views:
                await conn.execute_core(
                    delete(mv_build_state).where(mv_build_state.c.mv_id == row["mv_id"])
                )
            if views:
                await conn.execute_core_many(insert(mv_build_state), views)


async def _as_built_here(
    template_org: str, org_id: str, rows: list[dict], store: RegionStore, *, bound: bool = False
) -> list[dict]:
    """``rows`` -- records of replicas of this node's region -- as built for ``org_id``'s own
    replicas store there, from the template's model at ``org_id``'s address (demo_replicas).
    ``bound``: within the template's own runtime build, with it bound (adopt_seed), its model is
    read as it is -- asking for that runtime there would wait on the build that is asking."""
    from provisa.api.demo_replicas import _as_built, _bound_definitions, _key, definitions

    keys = [_key(row) for row in rows]
    defined = await (
        _bound_definitions(template_org, keys, org_id)
        if bound
        else definitions(template_org, keys, org_id)
    )
    return _as_built(rows, org_id, defined, datetime.now(UTC), store.replicas_url)


def _parsed(rows: list[dict]) -> list[dict]:
    """Records read back from a seed: their times as times again."""
    return [
        {
            k: datetime.fromisoformat(v) if k.endswith("_at") and isinstance(v, str) else v
            for k, v in row.items()
        }
        for row in rows
    ]


# -- the bundle build, the template, the sandbox -----------------------------------------------


def seed_dir(demo_directory: Path, region: str) -> Path:
    from provisa.api.demo_replicas import SEED_DIR

    return demo_directory / SEED_DIR / REGIONS_DIR / region


async def export_seed(template_org: str, demo_directory: Path) -> None:
    """Write what this node's region keeps of the template out as that region's seed (the
    bundle build's seeding node of each region)."""
    [store] = [s for s in await org_region_stores(template_org) if s.region == here()]
    out = seed_dir(demo_directory, store.region)
    out.mkdir(parents=True, exist_ok=True)
    seed = out / _STORE
    seed.unlink(missing_ok=True)
    dsn = f"duckdb:///{seed}"
    replicas = await asyncio.to_thread(
        copy_tables, store.replicas_url, replicas_schema(template_org, store.region), dsn, _REPLICAS
    )
    views = await asyncio.to_thread(
        copy_tables, store.views_url, views_schema(template_org, store.region), dsn, _VIEWS
    )
    replica_rows, view_rows = await _records(state_db(template_org, store))
    (out / _RECORD).write_text(
        json.dumps(
            {"tables": replicas, "views": views, "replicas": replica_rows, "builds": view_rows},
            indent=2,
            default=str,
        )
        + "\n"
    )


async def adopt_seed(template_org: str, demo_directory: Path) -> bool:
    """Land the template from its bundle's region seeds, each into its region's stores, and record
    each region's builds: this node's as built for its own store, another's as it wrote them.
    Whether the bundle carries any."""
    stores = {s.region: s for s in await org_region_stores(template_org)}
    adopted = False
    for region, store in stores.items():
        directory = seed_dir(demo_directory, region)
        record_path = directory / _RECORD
        if not record_path.exists():
            continue
        adopted = True
        record = json.loads(record_path.read_text())
        dsn = f"duckdb:///{directory / _STORE}"
        await asyncio.to_thread(
            copy_tables, dsn, _REPLICAS, store.replicas_url, replicas_schema(template_org, region)
        )
        await asyncio.to_thread(
            copy_tables, dsn, _VIEWS, store.views_url, views_schema(template_org, region)
        )
        replicas, builds = _parsed(record["replicas"]), _parsed(record["builds"])
        if region == here():
            # Run within the template's runtime build (demo_templates.land_from_seed): bound.
            replicas = await _as_built_here(template_org, template_org, replicas, store, bound=True)
        else:
            await lay_out_state(template_org, store)
        await _write_records(state_db(template_org, store), replicas, builds, replace_replicas=True)
    return adopted


async def landed_elsewhere(template_org: str, demo_directory: Path) -> bool:
    """Whether every other region's seed the bundle carries is landed for the template: that
    region's state for it laid out and recording as many completed replicas as its seed lists.
    A regions tier runs no node of those regions, so this node lands them (adopt_seed) -- and a
    node whose own region keeps nothing of the demo has nothing of its own to judge it by."""
    for store in await org_region_stores(template_org):
        if store.region == here():
            continue
        record_path = seed_dir(demo_directory, store.region) / _RECORD
        if not record_path.exists():
            continue
        expected = len(json.loads(record_path.read_text())["replicas"])
        db = state_db(template_org, store)
        async with db.acquire() as conn:
            laid_out = await conn.fetchval("SELECT to_regclass('replica_state')")
        if laid_out is None:
            return False
        rows, _views = await _records(db)
        if len(rows) < expected:
            return False
    return True


async def landed_here(template_org: str) -> int:
    """How many of the template's replicas in this node's region are landed in its store:
    recorded as built for it, and held there with the rows recorded (demo_replicas.landed_here)."""
    from provisa.api.demo_replicas import store_identity
    from provisa.federation.replica_address import replica_table_name

    [store] = [s for s in await org_region_stores(template_org) if s.region == here()]
    rows, _views = await _records(state_db(template_org, store))
    built = store_identity(template_org, store.replicas_url)
    expected = {
        replica_table_name(r["source_id"], r["schema_name"], r["table_name"]): r["completed_rows"]
        for r in rows
        if r["built_store"] == built
    }
    held = await asyncio.to_thread(
        held_rows, store.replicas_url, replicas_schema(template_org, store.region), list(expected)
    )
    return sum(1 for name, count in expected.items() if held.get(name) == count)


async def copy_to_sandbox(template_org: str, org_id: str) -> None:
    """The template's replicas and views, region by region, into the sandbox's schemas in each
    region's stores, with their record: this node's region's as built for the sandbox's own
    store, another's as that region wrote it."""
    for store in await org_region_stores(template_org):
        region = store.region
        await asyncio.to_thread(
            copy_tables,
            store.replicas_url,
            replicas_schema(template_org, region),
            store.replicas_url,
            replicas_schema(org_id, region),
        )
        await asyncio.to_thread(
            copy_tables,
            store.views_url,
            views_schema(template_org, region),
            store.views_url,
            views_schema(org_id, region),
        )
        replicas, builds = await _records(state_db(template_org, store))
        if region == here():
            replicas = await _as_built_here(template_org, org_id, replicas, store)
        # Laid out before the sandbox's runtime is, which lays out this node's region's again.
        await lay_out_state(org_id, store)
        await _write_records(state_db(org_id, store), replicas, builds, replace_replicas=False)


def kept_here(tables: list[Any]) -> list[Any]:
    """Of a demo's ``tables``, those this node keeps: on a node of a region, those that name it or
    none; everything elsewhere. Another region's are landed by its own seeding node."""
    if not regional():
        return tables
    return [t for t in tables if t.region in (None, here())]


def views_kept_here(config: Any) -> int:
    """How many materialized views of a demo this node's region keeps: those that name it."""
    from provisa.core.models import DERIVED_SOURCE_ID

    return sum(
        1
        for t in config.tables
        if t.source_id == DERIVED_SOURCE_ID and t.materialize and t.region == here()
    )


async def views_built_here(template_org: str) -> int:
    """How many of the template's views this node's region has built."""
    [store] = [s for s in await org_region_stores(template_org) if s.region == here()]
    _replicas, builds = await _records(state_db(template_org, store))
    return sum(1 for b in builds if b["region"] == here() and b["last_refresh_at"] is not None)
