# Copyright (c) 2026 Kenneth Stott
# Canary: 0b14927a-0529-4b1c-bb1a-9575e8636ab5
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Saving refuses what loading refuses (REQ-1922), against a real PostgreSQL model store: data
the org keeps in one region must stay readable in place by its other regions — so a source
naming a region, another region's engine, and the region's replicas store are each refused by
name when the save would leave it unreadable."""

# Requirements: REQ-1922

from __future__ import annotations

import os
import uuid

import pytest
import sqlalchemy as sa

from provisa.core.regions import OrgRegion, StoreConfig

pytestmark = [pytest.mark.integration]

_STORES = [
    StoreConfig(id="eu-pg", url="postgresql://eu/db"),
    StoreConfig(id="eu-redis", url="redis://eu:6379/0"),
    StoreConfig(id="eu-trino", url="trino://eu:8080", kind="trino-byo"),
    StoreConfig(id="us-pg", url="postgresql://us/db"),
    StoreConfig(id="us-redis", url="redis://us:6379/0"),
    StoreConfig(id="us-trino", url="trino://us:8080", kind="trino-byo"),
    StoreConfig(id="us-snow", url="snowflake://acct/db", kind="snowflake"),
]


def _region(rid: str, engine: str | None = None) -> OrgRegion:
    return OrgRegion(
        id=rid,
        engine=engine or f"{rid}-trino",
        replicas=f"{rid}-pg",
        views=f"{rid}-pg",
        cache=f"{rid}-redis",
        state=f"{rid}-pg",
        record=f"{rid}-pg",
    )


@pytest.fixture
def model(docker_postgres):
    """The org's model laid out in a schema of a real PostgreSQL."""
    from provisa.core.database import Database, create_engine_from_url
    from provisa.core.schema_org import metadata

    schema = f"test_regsave_{uuid.uuid4().hex[:8]}"
    url = (
        f"postgresql+psycopg://provisa:{os.environ.get('PG_PASSWORD', 'provisa')}@"
        f"{docker_postgres['host']}:{docker_postgres['port']}/provisa"
    )
    sync = sa.create_engine(url)
    with sync.begin() as conn:
        conn.execute(sa.text(f'CREATE SCHEMA "{schema}"'))
        conn.execute(sa.text(f'SET search_path TO "{schema}"'))
        metadata.create_all(conn)
    yield Database(create_engine_from_url(url), "model", search_path=schema)
    with sync.begin() as conn:
        conn.execute(sa.text(f'DROP SCHEMA "{schema}" CASCADE'))
    sync.dispose()


async def test_a_save_that_leaves_a_region_unreadable_by_the_others_is_refused(model):
    from provisa.core.models import Source
    from provisa.core.repositories import region as region_repo
    from provisa.core.repositories import source as source_repo

    crm = Source(
        id="crm", type="postgresql", host="h", database="d", username="u", password="", region="eu"
    )
    async with model.acquire() as conn:
        for store in _STORES:
            await region_repo.upsert_store(conn, store, origin="admin")
        await region_repo.upsert_region(conn, _region("eu"), origin="admin")
        await region_repo.upsert_region(conn, _region("us", "us-snow"), origin="admin")
        with pytest.raises(ValueError, match="region 'us' runs the snowflake engine"):
            await source_repo.upsert(conn, crm, origin="admin")
        await region_repo.upsert_region(conn, _region("us"), origin="admin")
        await source_repo.upsert(conn, crm, origin="admin")
        with pytest.raises(ValueError, match="region 'us' runs the snowflake engine"):
            await region_repo.upsert_region(conn, _region("us", "us-snow"), origin="admin")
        with pytest.raises(ValueError, match="'eu-pg' is not a PostgreSQL store"):
            await region_repo.upsert_store(
                conn, StoreConfig(id="eu-pg", url="mysql://eu/db"), origin="admin"
            )
        # Nothing refused was written.
        assert {r.id: r.engine for r in await region_repo.list_regions(conn)} == {
            "eu": "eu-trino",
            "us": "us-trino",
        }
        stores = {s.id: s.url for s in await region_repo.list_stores(conn)}
        assert stores["eu-pg"] == "postgresql://eu/db"
