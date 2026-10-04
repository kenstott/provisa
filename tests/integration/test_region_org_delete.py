# Copyright (c) 2026 Kenneth Stott
# Canary: 6af5c916-e03c-4a49-bc69-3378f50a167e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Deleting an org removes everything it keeps, in every environment and every region
(REQ-1921, REQ-1922), against a real PostgreSQL and Redis — with each region's stores in a
database of its own, and with both regions in ONE database: its schemas on the control plane, its
state, record, replicas, views, caches and exports in each region store (under the region's
names), what another region's engine imported from them, its cache and Hot-count keys in
Redis, and every catalog a region's Trino coordinator holds for it (region ``us`` runs on Trino,
``eu`` on its Postgres). Another org's are untouched. A region store that cannot be reached refuses the delete,
naming its region, and nothing is removed."""

# Requirements: REQ-1921, REQ-1922

from __future__ import annotations

import os
import uuid

import pytest
import redis
import sqlalchemy as sa
import trino

from provisa.core.environments import SCHEMA_SUFFIXES, org_schema
from provisa.core.regions import OrgRegion, StoreConfig

pytestmark = [pytest.mark.integration]

_PLATFORM = {
    "regions": [
        {"id": "eu", "address": "https://eu.example.com"},
        {"id": "us", "address": "https://us.example.com"},
    ]
}
_ENVS = ["prod", "dev"]
#: Each region's engine: eu's is its Postgres store, us's a Trino coordinator.
_ENGINE = {"eu": "eu-pg", "us": "us-trino"}


def _ddl(conn, statement: str) -> None:
    cur = conn.cursor()
    cur.execute(statement)
    cur.fetchall()


@pytest.fixture
def node_in_eu():
    from provisa.core import process_region

    was = process_region._region
    process_region.bind_launch(_PLATFORM, requested="eu")
    yield
    process_region._region = was


class _Estate:
    """A control plane, the regions' store databases (one per region, or one shared) and a
    Redis, holding an org (prod + dev, regions eu and us) and a bystander org."""

    def __init__(self, pg: dict, *, shared: bool) -> None:
        self.password = os.environ.get("PG_PASSWORD", "provisa")
        self.host, self.port = pg["host"], pg["port"]
        self.tag = uuid.uuid4().hex[:6]
        self.org = f"gone{self.tag}"
        self.bystander = f"stays{self.tag}"
        self.cp_db = f"cp_{self.tag}"
        self.store_dbs = (
            {"eu": f"st_{self.tag}", "us": f"st_{self.tag}"}
            if shared
            else {"eu": f"eu_{self.tag}", "us": f"us_{self.tag}"}
        )
        self.redis_url = os.environ["REDIS_URL"]
        self.trino_host = os.environ.get("TRINO_HOST", "localhost")
        self.trino_port = int(os.environ.get("TRINO_PORT", "8080"))

    def trino(self):
        return trino.dbapi.connect(
            host=self.trino_host, port=self.trino_port, user="provisa", source="provisa/test"
        )

    def catalogs_of(self, org: str) -> set[str]:
        """Every catalog the coordinator holds for ``org``."""
        conn = self.trino()
        try:
            cur = conn.cursor()
            cur.execute("SHOW CATALOGS")
            return {row[0] for row in cur.fetchall() if row[0].startswith(f"org_{org}_")}
        finally:
            conn.close()

    def url(self, database: str, driver: str = "") -> str:
        return f"postgresql{driver}://provisa:{self.password}@{self.host}:{self.port}/{database}"

    def create(self) -> None:
        admin = sa.create_engine(self.url("provisa", "+psycopg"), isolation_level="AUTOCOMMIT")
        with admin.connect() as conn:
            for db in {self.cp_db, *self.store_dbs.values()}:
                conn.execute(sa.text(f'CREATE DATABASE "{db}"'))
        admin.dispose()

    def drop(self) -> None:
        admin = sa.create_engine(self.url("provisa", "+psycopg"), isolation_level="AUTOCOMMIT")
        with admin.connect() as conn:
            for db in {self.cp_db, *self.store_dbs.values()}:
                conn.execute(sa.text(f'DROP DATABASE IF EXISTS "{db}" WITH (FORCE)'))
        admin.dispose()
        client = redis.Redis.from_url(self.redis_url)
        for org in (self.org, self.bystander):
            keys = list(client.scan_iter(match=f"*{org}*"))
            if keys:
                client.delete(*keys)
        conn = self.trino()
        try:
            for org in (self.org, self.bystander):
                for name in self.catalogs_of(org):
                    _ddl(conn, f'DROP CATALOG IF EXISTS "{name}"')
        finally:
            conn.close()

    def stores(self, *, dead_us: bool = False) -> list[StoreConfig]:
        out = []
        for region, db in self.store_dbs.items():
            url = self.url(db)
            if dead_us and region == "us":
                url = f"postgresql://provisa:{self.password}@{self.host}:1/{db}"
            trino_port = 1 if dead_us and region == "us" else self.trino_port
            out += [
                StoreConfig(id=f"{region}-pg", url=url, kind="pg"),
                StoreConfig(id=f"{region}-redis", url=self.redis_url),
                StoreConfig(
                    id=f"{region}-trino",
                    url=f"trino://{self.trino_host}:{trino_port}",
                    kind="trino",
                ),
            ]
        return out

    async def lay_out(self, *, dead_us: bool = False) -> None:
        """The org's model (prod and dev, each declaring both regions) on the control plane, and
        what each region keeps of each environment in its store, and in Redis."""
        from provisa.core.database import Database, create_engine_from_url
        from provisa.core.repositories import region as region_repo
        from provisa.core.schema_org import metadata

        sync = sa.create_engine(self.url(self.cp_db, "+psycopg"))
        for org in (self.org, self.bystander):
            for env in _ENVS:
                with sync.begin() as conn:
                    conn.execute(sa.text(f'CREATE SCHEMA "{org_schema(org, env)}"'))
                    conn.execute(sa.text(f'SET search_path TO "{org_schema(org, env)}"'))
                    metadata.create_all(conn)
        sync.dispose()
        for org in (self.org, self.bystander):
            for env in _ENVS:
                db = Database(
                    create_engine_from_url(self.url(self.cp_db, "+psycopg")),
                    "model",
                    search_path=org_schema(org, env),
                )
                async with db.acquire() as conn:
                    for store in self.stores(dead_us=dead_us and org == self.org):
                        await region_repo.upsert_store(conn, store, origin="admin")
                    for region in ("eu", "us"):
                        await region_repo.upsert_region(
                            conn,
                            OrgRegion(
                                id=region,
                                engine=_ENGINE[region],
                                replicas=f"{region}-pg",
                                views=f"{region}-pg",
                                cache=f"{region}-redis",
                                state=f"{region}-pg",
                                record=f"{region}-pg",
                            ),
                            origin="admin",
                        )
                await db.close()
        client = redis.Redis.from_url(self.redis_url)
        for org in (self.org, self.bystander):
            for region, store_db in self.store_dbs.items():
                other = "us" if region == "eu" else "eu"
                engine = sa.create_engine(self.url(store_db, "+psycopg"))
                with engine.begin() as conn:
                    for env in _ENVS:
                        for suffix in SCHEMA_SUFFIXES:
                            name = org_schema(org, env, suffix, region=region)
                            conn.execute(sa.text(f'CREATE SCHEMA IF NOT EXISTS "{name}"'))
                            conn.execute(sa.text(f'CREATE TABLE "{name}".t (id int)'))
                        if _ENGINE[region] == f"{region}-pg":  # a Postgres engine imports
                            imported = org_schema(org, env, "_replicas", region=other)
                            conn.execute(
                                sa.text(f'CREATE SCHEMA IF NOT EXISTS "region_{other}__{imported}"')
                            )
                engine.dispose()
                for env in _ENVS:
                    from provisa.cache.tenancy import place_of
                    from provisa.federation.replica_hot import count_scope

                    place = place_of(org, env, region)
                    scope = count_scope(org, env, region=region)
                    client.set(f"provisa:cache:{place}:m1:k", "v")
                    client.set(f"provisa:table:{place}:m1:t1", "v")
                    client.set(f"provisa:hot:{place}:m1:t1:blob", "v")
                    client.set(f"provisa:replica_hot:{scope}:1:60:0", "1")
                    client.set(f"provisa:replica_hot:too_large:{scope}:a/b/c", "1")

        # What us's Trino holds for each org: its sources' catalogs (prod's and dev's), its own
        # store's, and eu's replicas store's (REQ-1922).
        conn = self.trino()
        try:
            for org in (self.org, self.bystander):
                for name in (
                    f"org_{org}__sales",
                    f"org_{org}_env_dev__sales",
                    f"org_{org}__store",
                    f"org_{org}__region_eu",
                ):
                    _ddl(
                        conn,
                        f'CREATE CATALOG "{name}" USING postgresql WITH ('
                        f"\"connection-url\" = 'jdbc:postgresql://postgres:5432/provisa', "
                        "\"connection-user\" = 'provisa', \"connection-password\" = 'provisa')",
                    )
        finally:
            conn.close()

    def schemas_of(self, org: str) -> set[str]:
        """Every schema of ``org`` left in any database."""
        found: set[str] = set()
        for db in {self.cp_db, *self.store_dbs.values()}:
            engine = sa.create_engine(self.url(db, "+psycopg"))
            with engine.connect() as conn:
                found |= {
                    r[0]
                    for r in conn.execute(sa.text("SELECT nspname FROM pg_namespace"))
                    if f"org_{org}" in r[0]
                }
            engine.dispose()
        return found

    def keys_of(self, org: str) -> list[bytes]:
        return list(redis.Redis.from_url(self.redis_url).scan_iter(match=f"*{org}*"))


@pytest.fixture(params=[False, True], ids=["a-database-per-region", "one-shared-instance"])
def estate(request, docker_postgres, node_in_eu):
    e = _Estate(docker_postgres, shared=request.param)
    e.create()
    yield e
    e.drop()


async def _delete(estate: _Estate) -> None:
    """The org delete's sequence (orgs_router.delete_org): every region store first, then every
    environment's schemas on the control plane, then the org's own."""
    from provisa.core.database import Database, create_engine_from_url
    from provisa.core.org_provisioning import deprovision_org
    from provisa.core.region_purge import purge_org_regions

    control_plane = Database(create_engine_from_url(estate.url(estate.cp_db, "+psycopg")), "cp")
    try:
        await purge_org_regions(control_plane, estate.org, _ENVS)
        await deprovision_org(control_plane, estate.org, env="dev")
        await deprovision_org(control_plane, estate.org)
    finally:
        await control_plane.close()


async def test_an_org_delete_leaves_nothing_of_it_in_any_region_or_environment(estate):
    await estate.lay_out()
    before = estate.schemas_of(estate.org)
    assert len(before) > 20 and estate.keys_of(estate.org)
    assert len(estate.catalogs_of(estate.org)) == 4
    bystander = (
        estate.schemas_of(estate.bystander),
        sorted(estate.keys_of(estate.bystander)),
        estate.catalogs_of(estate.bystander),
    )

    await _delete(estate)

    assert estate.schemas_of(estate.org) == set()
    assert estate.keys_of(estate.org) == []
    assert estate.catalogs_of(estate.org) == set()
    assert (
        estate.schemas_of(estate.bystander),
        sorted(estate.keys_of(estate.bystander)),
        estate.catalogs_of(estate.bystander),
    ) == bystander


async def test_an_unreachable_region_store_refuses_the_delete_and_nothing_is_removed(estate):
    from provisa.core.region_purge import RegionStoreUnreachable

    await estate.lay_out(dead_us=True)
    before = (
        estate.schemas_of(estate.org),
        sorted(estate.keys_of(estate.org)),
        estate.catalogs_of(estate.org),
    )
    with pytest.raises(RegionStoreUnreachable) as refused:
        await _delete(estate)
    assert refused.value.params["region"] == "us"
    assert refused.value.code == "orgs.region_store_unreachable"
    assert (
        estate.schemas_of(estate.org),
        sorted(estate.keys_of(estate.org)),
        estate.catalogs_of(estate.org),
    ) == before
