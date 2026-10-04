# Copyright (c) 2026 Kenneth Stott
# Canary: 40ff42bd-86ea-43df-81b1-098891687f9c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A table the org keeps in another region is read from its replica there, never live, and never
built here (REQ-1922). A read is refused, naming the table and its region, while that replica is
not built or that region's stores cannot be reached."""

# Requirements: REQ-1922

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine as sa_create_engine

from provisa.core.database import Database, create_engine_from_url
from provisa.core.region_stores import ForeignRegion, HomeRegionUnavailable
from provisa.core.schema_org import metadata, replica_state
from provisa.federation import replica_state as replica_records
from provisa.federation.query_residency import require_home_replica

_KEY = ("crm", "public", "orders")
_TABLE = SimpleNamespace(source_id="crm", schema_name="public", table_name="orders")


@pytest.fixture
def eu_state(tmp_path) -> Database:
    engine = create_engine_from_url(f"sqlite+pysqlite:///{tmp_path / 'eu-state.db'}")
    with engine.begin() as raw:
        metadata.create_all(raw, tables=[replica_state])
    return Database(engine, "org-state-eu", holds="state")


def _state(eu: Database) -> SimpleNamespace:
    return SimpleNamespace(
        foreign_regions={"eu": ForeignRegion("eu", "postgresql://eu-replicas/db", eu)}
    )


async def _built(db: Database) -> None:
    async with db.acquire() as conn:
        await replica_records.request_build(conn, _KEY, replica_records.REASON_MODEL)
        await replica_records.record_completed(
            conn,
            _KEY,
            rows_copied=3,
            method="stream_batches",
            content_hash=None,
            store="eu-store",
            next_refresh_at=None,
            now=datetime.now(UTC),
        )


async def test_a_read_of_a_table_whose_home_replica_is_built_goes_ahead(eu_state):
    await _built(eu_state)
    await require_home_replica(_state(eu_state), _TABLE, "eu")


async def test_a_read_is_refused_while_the_home_replica_is_not_built(eu_state):
    with pytest.raises(HomeRegionUnavailable) as refused:
        await require_home_replica(_state(eu_state), _TABLE, "eu")
    assert refused.value.code == "query.home_region_unavailable"
    assert refused.value.params == {"table": "orders", "region": "eu"}
    assert "is not built" in str(refused.value)


async def test_a_read_is_refused_when_the_home_region_cannot_be_reached(tmp_path):
    gone = Database(
        sa_create_engine(f"sqlite:///{tmp_path / 'missing' / 'eu-state.db'}"),
        "org-state-eu",
        holds="state",
    )
    with pytest.raises(HomeRegionUnavailable, match="cannot be reached"):
        await require_home_replica(_state(gone), _TABLE, "eu")


def test_duckdb_attaches_only_a_server_store_of_another_region():
    from provisa.federation.duckdb_runtime import DuckDBFederationRuntime

    runtime = DuckDBFederationRuntime.__new__(DuckDBFederationRuntime)
    runtime._region_stores = set()
    with pytest.raises(RuntimeError, match="not a server store"):
        runtime.attach_region_store("eu", "duckdb:///eu.duckdb")


def test_an_engine_without_an_attach_for_another_region_refuses_naming_itself():
    from provisa.federation.backend import EngineBackend, EngineReadsNoOtherRegion

    backend = EngineBackend.__new__(EngineBackend)
    backend.engine = SimpleNamespace(name="snowflake")  # type: ignore[assignment]
    region = ForeignRegion("eu", "postgresql://eu/db", None)  # type: ignore[arg-type]
    with pytest.raises(EngineReadsNoOtherRegion, match="snowflake engine cannot read region 'eu'"):
        backend.region_read_catalog(SimpleNamespace(), region)


# -- the read is routed to the home region's replica -------------------------------------------


class _Acquire:
    async def __aenter__(self):
        return object()

    async def __aexit__(self, *exc):
        return False


async def test_a_table_kept_in_another_region_is_read_at_its_replica_there(monkeypatch):
    """Even on an engine that reads its source live, the read goes to the home region's replica:
    a table kept in another region is never read live from here."""
    from provisa.core import process_region
    from provisa.core.models import Source, SourceType
    from provisa.federation.engine import build_engine
    from provisa.federation.replica_address import address_replicas
    from provisa.federation.replica_routing import replica_routes
    from tests.helpers import no_engine_store, no_promoted_tables

    platform = {
        "regions": [
            {"id": "eu", "address": "https://eu.example.com"},
            {"id": "us", "address": "https://us.example.com"},
        ]
    }
    registered = [
        {
            "id": 1,
            "source_id": "src",
            "schema_name": "public",
            "table_name": "orders",
            "replicate": None,
            "load_protected": None,
            "region": "eu",
            "columns": [{"column_name": "id", "data_type": "bigint", "native_filter_type": None}],
        }
    ]

    async def _fetch_tables(_conn):
        return registered

    async def _no_ui_sources(_conn):
        return []

    monkeypatch.setattr("provisa.api.admin.db_queries.fetch_tables", _fetch_tables)
    monkeypatch.setattr("provisa.federation.replica_state.promotion", no_promoted_tables)
    monkeypatch.setattr("provisa.federation.replica_builds.store_identity", no_engine_store)
    monkeypatch.setattr("provisa.core.repositories.source.list_all", _no_ui_sources)
    engine = build_engine("trino")
    attached: list[str] = []

    def _region_catalog(_self, _state, region):
        attached.append(region.id)
        return f"region_{region.id}"

    monkeypatch.setattr(type(engine.backend), "region_read_catalog", _region_catalog)
    source = Source(
        id="src", type=SourceType.postgresql, host="h", port=5432, database="d", username="u"
    )
    db = SimpleNamespace(acquire=lambda: _Acquire())
    state = SimpleNamespace(
        org_id="acme",
        config=SimpleNamespace(sources=[source], tables=[]),
        model_db=db,
        tenant_db=db,
        federation_engine=SimpleNamespace(engine=engine),
        source_catalogs={"src": "src"},
        foreign_regions={"eu": ForeignRegion("eu", "postgresql://eu/db", None)},  # type: ignore[arg-type]
    )
    was = process_region._region
    try:
        process_region.bind_launch(platform, requested="us")
        routes = await replica_routes(state)
    finally:
        process_region._region = was
    sql = 'SELECT "o"."id" FROM "src"."public"."orders" AS "o"'
    assert address_replicas(sql, routes) == (
        'SELECT "o"."id" FROM "region_eu"."org_acme_replicas"."src__public__orders" AS "o"'
    )
    assert attached == ["eu"]
