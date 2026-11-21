# Copyright (c) 2026 Kenneth Stott
# Canary: 7c41e9d2-3b85-4a06-9f1e-d2a6b8c05e74
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A JDBC source's live attach (REQ-2191): a SQLAlchemy URL's scheme names the built-in source
type of the same database, and with attach turned on for the active engine the source attaches
exactly as that type does there, through that type's own attach connector. A jdbc: URL never
attaches."""

# Requirements: REQ-2191

from __future__ import annotations

import threading
from types import SimpleNamespace

import pytest

import provisa.core.settings_catalog  # noqa: F401 -- declares the settings read here
from provisa.core.models import Source, SourceType
from provisa.federation.engine import build_duckdb_engine, build_trino_engine
from provisa.federation.strategy import Strategy, engine_attaches, federate
from provisa.jdbc_source import attach as ja
from provisa.jdbc_source import settings as gs

pytestmark = pytest.mark.unit

_PG_URL = "postgresql+psycopg2://alice@pg.example:5433/shop"


@pytest.fixture(autouse=True)
def one_organization(monkeypatch):
    from provisa.core import secrets_providers

    monkeypatch.setattr(secrets_providers, "_many_organisations", lambda: False)


@pytest.fixture(scope="module")
def duckdb_engine():
    return build_duckdb_engine().complete_reach()


@pytest.fixture(scope="module")
def trino_engine():
    return build_trino_engine().complete_reach()


def _source(url: str, dialect: str = "postgres", **caps: str) -> Source:
    hints = {
        gs.URL: url,
        gs.SQL_DIALECT: dialect,
        **{gs.capabilities_key(engine): on for engine, on in caps.items()},
    }
    return Source(id="shop", type=SourceType.jdbc, federation_hints=hints, password="pw")


def test_every_named_type_is_a_built_in_source_type():
    for named in ja.NAMED_TYPES.values():
        assert SourceType(named).value == named


@pytest.mark.parametrize(
    ("url", "named"),
    [
        ("postgresql://h/db", "postgresql"),
        ("postgresql+psycopg2://h/db", "postgresql"),
        ("mysql+pymysql://h/db", "mysql"),
        ("mariadb+pymysql://h/db", "mariadb"),
        ("mssql+pyodbc://h/db", "sqlserver"),
        ("oracle+oracledb://h/?service_name=s", "oracle"),
        ("sqlite:///{files}/a.db", "sqlite"),
        ("duckdb:///{files}/a.duckdb", "duckdb"),
        ("clickhouse+native://h/db", "clickhouse"),
        ("snowflake://acct/db", "snowflake"),
        ("hana+hdbcli://h/db", "saphana"),
        ("exa+websocket://h/db", "exasol"),
        ("vertica+vertica_python://h/db", None),
        ("jdbc:postgresql://h/db", None),
    ],
)
def test_the_url_scheme_names_the_built_in_type(url, named):
    assert ja.named_type(url) == named


@pytest.mark.parametrize(
    ("url", "on_duckdb", "on_trino"),
    [
        (_PG_URL, "postgresql", "postgresql"),
        ("mssql+pyodbc://h/db", "sqlserver", "sqlserver"),
        # DuckDB has no MySQL or Oracle attach; Trino has.
        ("mysql+pymysql://h/db", None, "mysql"),
        ("oracle+oracledb://h/?service_name=s", None, "oracle"),
        # Trino attaches no SQLite or DuckDB file.
        ("sqlite:///{files}/a.db", "sqlite", None),
        ("duckdb:///{files}/a.duckdb", "duckdb", None),
    ],
)
def test_the_effective_attach_type_is_the_named_type_where_the_engine_attaches_it(
    url, on_duckdb, on_trino, duckdb_engine, trino_engine
):
    source = _source(url, duckdb="attach", trino="attach")
    assert ja.attach_type(source, duckdb_engine) == on_duckdb
    assert ja.attach_type(source, trino_engine) == on_trino
    for engine, expected in ((duckdb_engine, on_duckdb), (trino_engine, on_trino)):
        # Every caller of engine_attaches judges the source by it.
        assert ja.reach_type(source, engine) == (expected or "jdbc")
        assert engine_attaches(engine, ja.reach_type(source, engine)) is (expected is not None)


def test_attach_is_held_per_engine(duckdb_engine, trino_engine):
    source = _source(_PG_URL, duckdb="attach")
    assert ja.attach_type(source, duckdb_engine) == "postgresql"
    # After an engine swap it is off on the new engine until set there.
    assert ja.attach_type(source, trino_engine) is None
    # Off on every engine to start.
    assert ja.attach_type(_source(_PG_URL), duckdb_engine) is None
    # Another source type has none.
    pg = Source(id="pg", type=SourceType.postgresql, host="h", database="d")
    assert ja.attach_type(pg, duckdb_engine) is None
    assert ja.reach_type(pg, duckdb_engine) == "postgresql"


def test_a_jdbc_url_never_attaches(duckdb_engine):
    from provisa.jdbc_source import jdbc

    url = "jdbc:postgresql://h/db"
    hints = {
        gs.URL: url,
        gs.SQL_DIALECT: "postgres",
        jdbc.DRIVER_JAR: "{files}/pg.jar",
        jdbc.DRIVER_CLASS: "org.postgresql.Driver",
    }
    assert ja.named_type(url) is None
    with pytest.raises(gs.InvalidJdbcSource, match="jdbc URL has no engine attach"):
        gs.parse({**hints, gs.capabilities_key("duckdb"): "attach"})
    source = Source(id="shop", type=SourceType.jdbc, federation_hints=hints)
    assert ja.attach_type(source, duckdb_engine) is None
    assert federate(source, duckdb_engine) is Strategy.MATERIALIZED


def test_the_attach_connector_is_fed_the_urls_host_port_database_and_user(duckdb_engine):
    view = ja.attach_view(_source(_PG_URL, duckdb="attach"), "postgresql")
    assert view.type == SourceType.postgresql
    assert (view.id, view.host, view.port, view.database, view.username, view.password) == (
        "shop",
        "pg.example",
        5433,
        "shop",
        "alice",
        "pw",
    )
    entry = duckdb_engine.resolve(view)
    assert entry.details["attach"] == (
        "ATTACH 'host=pg.example port=5433 dbname=shop user=alice password=pw' "
        'AS "_src_shop" (TYPE postgres)'
    )
    # A file database is named by its path, as written.
    sqlite = ja.attach_view(_source("sqlite:///{files}/a.db", "sqlite"), "sqlite")
    assert (sqlite.type, sqlite.path) == (SourceType.sqlite, "{files}/a.db")


def test_an_attached_source_federates_live_and_lands_where_it_is_not(duckdb_engine, trino_engine):
    source = _source(_PG_URL, duckdb="attach,predicate")
    assert federate(source, duckdb_engine) is Strategy.VIRTUAL
    assert federate(source, trino_engine) is Strategy.MATERIALIZED
    # Its pushdown is what its settings for the engine turn on.
    cap = duckdb_engine.connector_pushdown(source)
    assert cap.predicate_pushdown and not cap.join_pushdown


# -- the engine's attach walk ------------------------------------------------------------------------


class _Runtime:
    def __init__(self) -> None:
        self.attached: list = []
        self.detached: list = []

    def attach_source(self, source) -> None:  # noqa: ANN001
        self.attached.append(source)

    def detach_source(self, source) -> None:  # noqa: ANN001
        self.detached.append(source)


def _walk(engine, source: Source) -> _Runtime:
    """One walk of a registry holding ``source`` and its one table, on ``engine``."""
    from provisa.federation import native_backend

    backend = object.__new__(native_backend.NativeEngineBackend)
    native_backend.NativeEngineBackend.__init__(backend, engine)
    runtime = _Runtime()
    backend._runtime = runtime
    backend._walk_lock = threading.Lock()
    table = SimpleNamespace(
        source_id=source.id,
        schema_name="public",
        table_name="orders",
        row_materialize=False,
        region=None,
    )
    state = SimpleNamespace(
        config=SimpleNamespace(sources=[source], tables=[table]),
        runtime_sources={},
        tables=[],
        source_types={source.id: source.type.value},
        source_catalogs={source.id: "shop"},
        replica_routes=SimpleNamespace(kept_for_good=frozenset()),
        foreign_regions={None: None},
        model_db=None,
    )
    backend._walk_registry(state, state.config)
    return runtime


def test_an_attached_jdbc_postgresql_source_is_attached_through_the_postgres_attach(
    duckdb_engine,
):
    runtime = _walk(duckdb_engine, _source(_PG_URL, duckdb="attach"))
    [merged] = runtime.attached
    assert merged.type.value == "postgresql"
    assert (merged.host, merged.port, merged.database, merged.username) == (
        "pg.example",
        5433,
        "shop",
        "alice",
    )
    assert (merged.catalog, merged.schema_name, merged.table_name) == ("shop", "public", "orders")
    # The engine reads it through its Postgres attach connector.
    assert duckdb_engine.connector_for_source(merged) is duckdb_engine.connectors["postgresql"]


def test_after_an_engine_swap_the_source_is_not_attached_until_set_there():
    from provisa.federation.engine import FederationEngine

    swapped = FederationEngine("duckdb2", list(build_duckdb_engine()._candidates))
    # Attach is on for the engine it was set on, not for the one swapped in.
    assert _walk(swapped, _source(_PG_URL, duckdb="attach")).attached == []
    assert federate(_source(_PG_URL, duckdb="attach"), swapped.complete_reach()) is (
        Strategy.MATERIALIZED
    )
