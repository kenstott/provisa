# Copyright (c) 2026 Kenneth Stott
# Canary: c0abc1fa-aab0-4811-85e7-a2af5bd06ede
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What an org's delete reaches in its regions (region_purge._plan, REQ-1921, REQ-1922).

A region's schemas are kept in its data stores (state, record, replicas, views). Its engine
holds them only when it is a PostgreSQL database. An in-process engine (DuckDB) keeps none, and
its file belongs to that region's own nodes: the regions sandbox tier, which has no node of the
other region at all, refused every sandbox delete (orgs.region_store_unreachable, store engine)
trying to open eu's engine file from a us node."""

# Requirements: REQ-1921, REQ-1922, REQ-2068

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace
from typing import TYPE_CHECKING, cast

import pytest

from provisa.core import region_purge
from provisa.core.regions import OrgRegion, StoreConfig

if TYPE_CHECKING:
    from provisa.core.database import Database

_EU_DATA = "postgresql://provisa:x@eu-host:5432/eu_store"
_EU_ENGINE_FILE = "duckdb:////var/lib/provisa/regions-tier-eu-engine.duckdb"
_EU_ENGINE_PG = "postgresql://provisa:x@eu-host:5432/eu_engine"


def _plan(monkeypatch, engine_url: str, engine_kind: str) -> region_purge._Purge:
    from provisa.core import database, process_region
    from provisa.core.repositories import region as region_repo

    region = OrgRegion(
        id="eu",
        engine="eu_engine",
        replicas="eu_data",
        views="eu_data",
        cache="cache",
        state="eu_data",
        record="eu_data",
    )
    stores = [
        StoreConfig(id="eu_engine", url=engine_url, kind=engine_kind),
        StoreConfig(id="eu_data", url=_EU_DATA),
        StoreConfig(id="cache", url="redis://cache:6379/0"),
    ]

    class _Model:
        def __init__(self, *_args, **_kwargs) -> None:
            pass

        @asynccontextmanager
        async def acquire(self):
            yield None

    async def regions(_conn):
        return [region]

    async def listed(_conn):
        return stores

    async def exists(_cp, _schema):
        return True

    monkeypatch.setattr(process_region, "region", lambda: "us")
    monkeypatch.setattr(database, "Database", _Model)
    monkeypatch.setattr(
        database.Capabilities, "for_dialect", staticmethod(lambda _d: SimpleNamespace(schemas=True))
    )
    monkeypatch.setattr(region_repo, "list_regions", regions)
    monkeypatch.setattr(region_repo, "list_stores", listed)
    monkeypatch.setattr(region_purge, "_schema_exists", exists)
    control_plane = SimpleNamespace(
        engine=SimpleNamespace(dialect=SimpleNamespace(name="postgresql"))
    )
    return asyncio.run(
        region_purge._plan(cast("Database", control_plane), "sbxr97e695261481", ["prod"])
    )


def test_a_regions_duckdb_engine_file_is_not_reached(monkeypatch):
    plan = _plan(monkeypatch, _EU_ENGINE_FILE, "duckdb")
    assert _EU_ENGINE_FILE not in plan.reach
    assert plan.reach[_EU_DATA] == ("eu", "state")  # its data stores are, as before
    assert plan.schemas[_EU_DATA]


@pytest.mark.parametrize("kind", ["pg"])
def test_a_regions_postgresql_engine_is_still_reached_for_what_it_keeps(monkeypatch, kind):
    plan = _plan(monkeypatch, _EU_ENGINE_PG, kind)
    assert plan.reach[_EU_ENGINE_PG] == ("eu", "engine")
    assert plan.schemas[_EU_ENGINE_PG] and plan.attached[_EU_ENGINE_PG]
