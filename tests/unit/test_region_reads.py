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
        foreign_regions={"eu": ForeignRegion("eu", "postgresql://eu-replicas/db", eu, "pg")}
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
    region = ForeignRegion("eu", "postgresql://eu/db", None, "pg")  # type: ignore[arg-type]
    with pytest.raises(EngineReadsNoOtherRegion, match="snowflake engine cannot read region 'eu'"):
        backend.region_read_address(SimpleNamespace(), region, "org_acme_replicas", "orders")


# -- the read is routed to the home region's replica -------------------------------------------


class _Acquire:
    async def __aenter__(self):
        return object()

    async def __aexit__(self, *exc):
        return False


@pytest.mark.parametrize(
    ("replicate", "routed"),
    [
        # Replicate Always: the home region keeps a replica, so the read goes there — even on an
        # engine that reads its source live, a table its region keeps a copy of is never read
        # live from here.
        (0, True),
        # The home region's engine reads the source in place and keeps no replica: the read is
        # the source's here too (REQ-1921), never routed to a replica that was never meant to
        # exist.
        (None, False),
    ],
)
async def test_a_table_kept_in_another_region_is_read_where_that_region_reads_it(
    monkeypatch, replicate, routed
):
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
            "replicate": replicate,
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

    def _region_address(_self, _state, region, schema, table):
        attached.append(region.id)
        return f"region_{region.id}", schema, table

    monkeypatch.setattr(type(engine.backend), "region_read_address", _region_address)
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
        foreign_regions={"eu": ForeignRegion("eu", "postgresql://eu/db", None, "trino")},  # type: ignore[arg-type]
    )
    was = process_region._region
    try:
        process_region.bind_launch(platform, requested="us")
        routes = await replica_routes(state)
    finally:
        process_region._region = was
    sql = 'SELECT "o"."id" FROM "src"."public"."orders" AS "o"'
    if routed:
        assert address_replicas(sql, routes) == (
            # Where eu wrote it: eu's replicas schema (REQ-1922, names carry the region).
            'SELECT "o"."id" FROM "region_eu"."org_acme_rg_eu_replicas"."src__public__orders" '
            'AS "o"'
        )
        assert attached == ["eu"]
    else:
        assert address_replicas(sql, routes) == sql
        assert attached == []


# -- Trino: a store is a catalog of its own ------------------------------------------------------


@pytest.fixture
def acme():
    """The request is acme's (not the boot org's)."""
    from provisa.core.request_context import reset_current_org, set_current_org

    token = set_current_org("acme")
    yield
    reset_current_org(token)


class _TrinoConn:
    def __init__(self) -> None:
        self.statements: list[str] = []

    def cursor(self):
        conn = self

        class _Cursor:
            def execute(self, sql: str) -> None:
                conn.statements.append(sql)

            def fetchall(self) -> list:
                return []

        return _Cursor()


def _trino(monkeypatch):
    import contextlib

    from provisa.federation.engine import build_engine

    engine = build_engine("trino")
    conn = _TrinoConn()

    @contextlib.contextmanager
    def _registrar(url):  # the deployment's registration lock, taken on the control plane
        conn.statements.append(f"-- lock {url}")
        yield

    monkeypatch.setattr("provisa.core.trino_system_catalogs.one_registrar", _registrar)
    state = SimpleNamespace(
        org_id="boot",  # the served org (acme, bound by the test) is not the boot org
        engine_conn=conn,
        engine_conn_kwargs={},
        tenant_engine=SimpleNamespace(url="postgresql://cp/db"),
    )
    return engine.backend, state, conn


def test_trino_reads_another_regions_replicas_through_a_catalog_of_that_store(monkeypatch, acme):
    backend, state, conn = _trino(monkeypatch)
    region = ForeignRegion("eu", "postgresql://reader:pw@eu-replicas:5433/replicas", None, "pg")  # type: ignore[arg-type]
    where = ("org_acme__region_eu", "org_acme_replicas", "orders")
    # The read map names it without dialing anything.
    assert backend.region_read_address(state, region, "org_acme_replicas", "orders") == where
    assert conn.statements == []
    # A read that finds the replica built registers the catalog — once: a second table of that
    # region, or a rebuild, reads through the same catalog.
    backend.attach_region_read(state, region, "org_acme_replicas", "orders", ("h1", "[]"))
    backend.attach_region_read(state, region, "org_acme_replicas", "lines", ("h2", "[]"))
    assert conn.statements == [
        "-- lock postgresql://cp/db",
        "DROP CATALOG IF EXISTS org_acme__region_eu",
        "CREATE CATALOG org_acme__region_eu USING postgresql WITH ("
        "\"connection-url\" = 'jdbc:postgresql://eu-replicas:5433/replicas', "
        "\"connection-user\" = 'reader', \"connection-password\" = 'pw', "
        "\"statistics.enabled\" = 'false')",
    ]


def test_trino_reads_an_orgs_own_store_through_its_catalog_not_the_control_planes(
    monkeypatch, acme
):
    backend, state, conn = _trino(monkeypatch)
    monkeypatch.setattr(
        "provisa.storage.byo.org_store_dsn",
        lambda org: "postgresql://u:p@eu-store/db" if org == "acme" else None,
    )
    catalog, schema = backend.materialize_store_target(state, "acme")
    assert catalog == "org_acme__store" and schema.endswith("_mv_cache")
    assert backend.materialize_store_target(state, "beta")[0] == "provisa_admin"


def test_trino_refuses_a_store_it_cannot_read():
    from provisa.core.trino_system_catalogs import store_catalog_spec

    with pytest.raises(ValueError, match="postgresql connector; store 'region_eu' is 'mysql'"):
        store_catalog_spec("region_eu", "mysql://u:p@eu/db")
    with pytest.raises(ValueError, match="must name a host, a database and a user"):
        store_catalog_spec("region_eu", "postgresql://eu/db")


def test_trino_reaches_a_store_without_a_password_setting_none():
    from provisa.core.trino_system_catalogs import store_catalog_spec

    spec = store_catalog_spec("region_eu", "postgresql://reader@eu/db")
    assert "connection-password" not in spec.properties
    assert spec.properties["connection-url"] == "jdbc:postgresql://eu:5432/db"


def test_the_pg_engine_names_the_import_at_publish_and_imports_once_per_build(monkeypatch, acme):
    """PostgreSQL has no catalog per store: it imports the table through postgres_fdw into a
    schema of its own. The read map names that schema without dialing the other region; a read
    that finds the replica built imports it — again only when that region rebuilt it."""
    from provisa.federation.native_backend import NativeEngineBackend
    from provisa.federation.pg_runtime import PgFederationRuntime

    imported: list[tuple] = []
    runtime = PgFederationRuntime.__new__(PgFederationRuntime)
    runtime._region_imports = {}
    monkeypatch.setattr(runtime, "ensure_materialize_attached", lambda: "provisa")
    monkeypatch.setattr(
        runtime,
        "attach_region_table",
        lambda name, dsn, schema, table: imported.append((name, schema, table)),
    )
    backend = NativeEngineBackend.__new__(NativeEngineBackend)
    backend._attach_errors = (RuntimeError,)
    monkeypatch.setattr(backend, "_store_runtime", lambda: runtime)
    region = ForeignRegion("eu", "postgresql://r@eu/db", None, "pg")  # type: ignore[arg-type]
    state = SimpleNamespace(org_id="boot")
    where = backend.region_read_address(state, region, "org_acme_replicas", "orders")
    # REQ-1266/1529: the foreign server carries the org (and environment) reading it.
    assert where == ("provisa", "fdw_org_acme__region_eu__org_acme_replicas", "orders")
    assert imported == []
    for build in (("h1", "[id]"), ("h1", "[id]"), ("h2", "[id, total]")):
        backend.attach_region_read(state, region, "org_acme_replicas", "orders", build)
    assert imported == [("org_acme__region_eu", "org_acme_replicas", "orders")] * 2


def test_a_store_that_cannot_be_attached_is_unreachable(monkeypatch):
    from provisa.federation.backend import RegionStoreUnreachable
    from provisa.federation.native_backend import NativeEngineBackend

    class _Runtime:
        def attach_region_read(self, *_args):
            raise RuntimeError("could not connect to server")

    backend = NativeEngineBackend.__new__(NativeEngineBackend)
    backend._attach_errors = (RuntimeError,)
    monkeypatch.setattr(backend, "_store_runtime", lambda: _Runtime())
    region = ForeignRegion("eu", "postgresql://r@eu/db", None, "pg")  # type: ignore[arg-type]
    with pytest.raises(RegionStoreUnreachable, match="could not connect"):
        backend.attach_region_read(SimpleNamespace(org_id="acme"), region, "s", "t", ("h", "[]"))


async def test_a_read_of_a_built_home_replica_attaches_it_and_an_unreachable_one_is_refused(
    eu_state,
):
    from provisa.federation.backend import RegionStoreUnreachable
    from provisa.federation.query_residency import read_home_replica

    await _built(eu_state)
    attached: list[tuple] = []

    class _Backend:
        fail = False

        def replica_address(self, state, *, source_id, schema_name, table_name, region):
            # Where the home region wrote it.
            return SimpleNamespace(
                schema=f"org_acme_rg_{region}_replicas", table="crm__public__orders"
            )

        def attach_region_read(self, state, region, schema, table, build):
            if self.fail:
                raise RegionStoreUnreachable("eu store is down")
            attached.append((region.id, schema, table))

    backend = _Backend()
    await read_home_replica(_state(eu_state), backend, _TABLE, "eu")
    assert attached == [("eu", "org_acme_rg_eu_replicas", "crm__public__orders")]
    backend.fail = True
    with pytest.raises(HomeRegionUnavailable, match="cannot be reached"):
        await read_home_replica(_state(eu_state), backend, _TABLE, "eu")


@pytest.mark.parametrize(
    ("dsn", "refusal"),
    [
        ("mysql://u:p@eu/db", "is 'mysql'; the pg engine reads another region's store"),
        ("postgresql://eu/db", "must name a host, a database and a user"),
    ],
)
def test_the_pg_engine_refuses_a_store_it_cannot_import_from(dsn, refusal):
    from provisa.federation.pg_runtime import PgFederationRuntime

    runtime = PgFederationRuntime.__new__(PgFederationRuntime)
    with pytest.raises(RuntimeError, match=refusal):
        runtime.attach_region_table("eu", dsn, "org_acme_replicas", "orders")
