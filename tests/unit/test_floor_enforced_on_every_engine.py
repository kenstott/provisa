# Copyright (c) 2026 Kenneth Stott
# Canary: c4a17e62-3b9d-4f05-a8e2-1d6f0b7c5e39
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A floored source is read from its replica on every engine (REQ-030, REQ-826, REQ-1141,
REQ-1912).

``replicate`` / ``load_protected`` remove the live read of a source: no request may reach
it live on any engine path. These cases take a source the engine CAN reach live and check what a
read of it resolves to once the operator's setting is on:

* the engine holds no live attach of the source — nothing a statement could name reads it;
* the read is addressed to the replica, at exactly the address the writer is given;
* that address is in the replicas schema, never the table's registered name."""

# Requirements: REQ-030, REQ-826, REQ-1141, REQ-1912

from __future__ import annotations

import sqlite3
from types import SimpleNamespace

import pytest

from provisa.core.models import Source, SourceType
from provisa.federation.engine import build_engine
from provisa.federation.replica_address import address_replicas
from provisa.federation.replica_routing import replica_routes
from provisa.federation.strategy import Strategy, federate
from tests.helpers import no_engine_store, no_promoted_tables


@pytest.fixture(autouse=True)
def _bound_to_acme(bind_org):
    """The work is acme's, the org the test state serves, bound as its entrypoint binds it (REQ-1266)."""
    bind_org("acme")


_SOURCE_HOST = "orders-db.internal"
_ORG = "acme"

_FLOORS = pytest.mark.parametrize(
    "settings",
    [{"replicate": 0}, {"load_protected": True, "cache_ttl": 3600}],
    ids=["replicate", "load_protected"],
)


def _postgres_source(**settings) -> Source:
    return Source(
        id="src",
        type=SourceType.postgresql,
        host=_SOURCE_HOST,
        port=5432,
        database="shop",
        username="reader",
        password="secret",
        **settings,
    )


def _registered(source_id: str, schema_name: str, table_name: str) -> dict:
    return {
        "id": 1,
        "source_id": source_id,
        "schema_name": schema_name,
        "table_name": table_name,
        "replicate": None,
        "load_protected": None,
        "region": None,  # REQ-1921: a registration always carries its region
        "columns": [
            {
                "column_name": "id",
                "data_type": "bigint",
                "is_primary_key": True,
                "native_filter_type": None,
            }
        ],
    }


class _Acquire:
    async def __aenter__(self):
        return object()

    async def __aexit__(self, *exc):
        return False


def _state(monkeypatch, engine, source, registered: list[dict], *, catalog: str) -> SimpleNamespace:
    """An org's state as the routes are published from it: the registry holds ``registered``."""

    async def _fetch_tables(_conn):
        return registered

    async def _no_ui_sources(_conn):
        return []

    monkeypatch.setattr("provisa.api.admin.db_queries.fetch_tables", _fetch_tables)
    monkeypatch.setattr("provisa.federation.replica_state.promotion", no_promoted_tables)
    monkeypatch.setattr("provisa.federation.replica_builds.store_identity", no_engine_store)
    monkeypatch.setattr("provisa.core.repositories.source.list_all", _no_ui_sources)
    return SimpleNamespace(
        org_id=_ORG,
        config=SimpleNamespace(sources=[source], tables=[]),
        model_db=(_one_db := SimpleNamespace(acquire=lambda: _Acquire())),
        tenant_db=_one_db,
        federation_engine=SimpleNamespace(engine=engine),
        source_catalogs={source.id: catalog},
    )


# -- Trino ----------------------------------------------------------------------------------------


class _TrinoConn:
    """A Trino connection that records the statements issued on it."""

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


@_FLOORS
def test_trino_registers_no_catalog_for_a_floored_source(settings):
    """A Trino catalog IS the live attach of a source. A floored source has none: a catalog left
    from before the setting was turned on is dropped, and none is created. No statement the
    engine could be sent then dials the source."""
    from provisa.core.operator_floor import floor_setting

    engine = build_engine("trino")
    source = _postgres_source(**settings)
    assert floor_setting(source) is not None
    assert federate(source, engine, replicated=True) is Strategy.MATERIALIZED
    conn = _TrinoConn()
    state = SimpleNamespace(engine_conn=conn, engine_conn_kwargs=None)

    engine.backend.register_source(state, source, "secret", catalog_name="src")

    assert conn.statements == ["DROP CATALOG IF EXISTS src"]
    assert not any(_SOURCE_HOST in sql for sql in conn.statements)


def test_trino_registers_the_catalog_of_a_source_it_reads_live(monkeypatch):
    """The control: with no setting on, the same source's catalog is created and dials it."""
    # The test session redirects every Postgres catalog to its own stack's address; a deployment
    # sets neither, and the catalog then dials the source's own host.
    monkeypatch.delenv("PROVISA_ENGINE_CONTROL_PLANE_HOST", raising=False)
    monkeypatch.delenv("PROVISA_ENGINE_CONTROL_PLANE_PORT", raising=False)
    engine = build_engine("trino")
    conn = _TrinoConn()
    state = SimpleNamespace(engine_conn=conn, engine_conn_kwargs=None)

    engine.backend.register_source(state, _postgres_source(), "secret", catalog_name="src")

    created = [sql for sql in conn.statements if sql.startswith("CREATE CATALOG src ")]
    assert len(created) == 1 and _SOURCE_HOST in created[0]


@_FLOORS
async def test_trino_reads_a_floored_source_at_the_address_its_replica_is_written(
    settings, monkeypatch
):
    """Trino has no view layer between a read and the store, so the name a read is given and the
    name the copy is written to must be one address — in the replicas schema."""
    engine = build_engine("trino")
    monkeypatch.setattr(type(engine), "materialize_store", lambda _self: "postgresql:///store")
    source = _postgres_source(**settings)
    state = _state(
        monkeypatch, engine, source, [_registered("src", "public", "orders")], catalog="src"
    )

    routes = await replica_routes(state)
    written = engine.backend.replica_address(
        state, source_id="src", schema_name="public", table_name="orders"
    )

    assert (written.schema, written.table) == ("org_acme_replicas", "src__public__orders")
    read = address_replicas('SELECT "o"."id" FROM "src"."public"."orders" AS "o"', routes)
    assert read == (
        'SELECT "o"."id" FROM "provisa_admin"."org_acme_replicas"."src__public__orders" AS "o"'
    )
    assert (written.schema, written.table) != ("public", "orders")  # not the registered address


async def test_trino_reads_an_unfloored_source_through_its_own_catalog(monkeypatch):
    """The control: a source Trino reads live is left exactly as the statement names it."""
    engine = build_engine("trino")
    monkeypatch.setattr(type(engine), "materialize_store", lambda _self: "postgresql:///store")
    state = _state(
        monkeypatch,
        engine,
        _postgres_source(),
        [_registered("src", "public", "orders")],
        catalog="src",
    )
    sql = 'SELECT "o"."id" FROM "src"."public"."orders" AS "o"'
    assert address_replicas(sql, await replica_routes(state)) == sql


# -- Postgres engine --------------------------------------------------------------------------------


@_FLOORS
async def test_pg_reads_a_floored_source_at_its_replica_not_at_the_live_views_name(
    settings, monkeypatch
):
    """The engine's live view of the table stands at ``"<catalog>_<schema>"."<table>"`` — over a
    foreign table, so a write to that name is a write into the source. The replica has another
    name, and the read is given that one."""
    engine = build_engine("pg")
    monkeypatch.setattr(type(engine), "materialize_store", lambda _self: "postgresql:///engine")
    source = _postgres_source(**settings)
    state = _state(
        monkeypatch, engine, source, [_registered("src", "public", "orders")], catalog="src"
    )

    routes = await replica_routes(state)
    written = engine.backend.replica_address(
        state, source_id="src", schema_name="public", table_name="orders"
    )

    read = address_replicas('SELECT "o"."id" FROM "src_public"."orders" AS "o"', routes)
    assert read == 'SELECT "o"."id" FROM "org_acme_replicas"."src__public__orders" AS "o"'
    assert (written.schema, written.table) == ("org_acme_replicas", "src__public__orders")
    assert (written.schema, written.table) != ("src_public", "orders")  # not the live view's name


# -- DuckDB ---------------------------------------------------------------------------------------


def _sqlite_shop(tmp_path) -> str:
    database = tmp_path / "shop.sqlite"
    conn = sqlite3.connect(database)
    conn.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, amount REAL)")
    conn.executemany("INSERT INTO orders VALUES (?, ?)", [(i, i * 1.5) for i in range(1, 6)])
    conn.commit()
    conn.close()
    return str(database)


async def test_duckdb_reads_the_replica_once_a_live_attached_table_is_floored(
    tmp_path, monkeypatch
):
    """A table attached live on a running engine, then floored (the operator's setting turned
    on): its live view is removed, and the statement the engine runs reads the replica."""
    import duckdb

    from provisa.federation.duckdb_runtime import DuckDBFederationRuntime

    engine = build_engine("duckdb")
    backend = engine.backend
    store = f"duckdb:///{tmp_path / 'mat.duckdb'}"
    monkeypatch.setattr(type(engine), "materialize_store", lambda _self: store)
    runtime = DuckDBFederationRuntime(materialize_dsn=store)
    backend._runtime = runtime
    registered = [_registered("src", "main", "orders")]

    shop = _sqlite_shop(tmp_path)

    def _source(**settings) -> Source:
        return Source(id="src", type=SourceType.sqlite, path=shop, **settings)

    def _walk(source: Source) -> SimpleNamespace:
        """The engine's attach walk over the registry, as a query triggers it."""
        state = _state(monkeypatch, engine, source, registered, catalog="src")
        state.runtime_sources, state.tables = {}, registered
        backend._runtime_for(state)
        return state

    live_name = '"src"."main"."orders"'

    async def _count(pg_sql: str, state: SimpleNamespace) -> int:
        lowered = address_replicas(pg_sql, await replica_routes(state))
        return (await runtime.run(backend.transpile_physical(lowered))).rows[0][0]

    statement = f'SELECT COUNT(*) FROM {live_name} AS "o"'
    state = _walk(_source())
    assert await _count(statement, state) == 5  # live: the source's five rows

    # The operator turns the setting on. The replica is reconciled and filled at its address —
    # with one row, so a read says which of the two answered.
    state = _walk(_source(replicate=0))
    address = backend.replica_address(
        state, source_id="src", schema_name="main", table_name="orders"
    )
    columns = [("id", "integer"), ("amount", "double")]
    await runtime.reconcile_replica(
        schema=address.schema, table=address.table, columns=columns, pk_columns=["id"]
    )
    await runtime.land_table(
        schema=address.schema,
        table=address.table,
        columns=columns,
        rows=[{"id": 1, "amount": 1.5}],
        pk_columns=["id"],
    )

    assert (address.schema, address.table) == ("org_acme_replicas", "src__main__orders")
    assert await _count(statement, state) == 1  # the same statement now reads the replica
    with pytest.raises(duckdb.Error):
        await runtime.run(f"SELECT COUNT(*) FROM {live_name}")  # no live view is left to read


def test_a_state_whose_replica_routes_are_not_replica_routes_is_rejected():
    """A stand-in state (a bare MagicMock) answers ``replica_routes.floored.get`` with another
    mock, which the floor read unpacked into a confusing "expected 2, got 0" deep in routing. The
    read names what is wrong instead."""
    from unittest.mock import MagicMock

    import pytest

    from provisa.federation.registry_view import operator_floor

    with pytest.raises(TypeError, match="replica_routes is a MagicMock, not ReplicaRoutes"):
        operator_floor(MagicMock(), [1])
