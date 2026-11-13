# Copyright (c) 2026 Kenneth Stott
# Canary: ac5bd640-ce8d-4535-8178-04c1b894c413
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1867: a Databricks source is read in place only when the steward asks for it.

On the DuckDB and pg engines a Databricks source lands through its native driver by default
(complete_reach keeps the land reach for the type); with ``federation_hints['reach'] = 'scan'`` it
is read in place through the engine's opt-in iceberg_scan connector. Each runtime re-points the
table's view, with fresh vended credentials, before every statement that names it. The resolver is
replaced by a stub here; tests/unit/test_databricks_iceberg_resolver.py covers it."""

from __future__ import annotations

import threading
from types import SimpleNamespace

import pytest

from provisa.core.models import Source, SourceType
from provisa.federation.connector import Mechanism
from provisa.federation.databricks_iceberg_resolver import DatabricksIcebergTable, StorageCredential
from provisa.federation.engine import UnreachableSource, build_engine
from provisa.federation.strategy import Strategy, federate

pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def one_organisation(monkeypatch):
    """The deployment's answer to whether it holds many organisations, which resolve() asks when
    it reads a source's secrets: here, one. The API layer gives it at import; a test that does not
    import it gives it itself, so it passes on its own as well as beside other files."""
    from provisa.core import secrets_providers

    monkeypatch.setattr(secrets_providers, "_many_organisations", lambda: False)


_ENGINES = ["duckdb", "pg"]


def _engine(name: str):
    return build_engine(name).complete_reach()


def _src(hints: dict | None = None, **kw) -> Source:
    return Source(
        id=kw.pop("id", "dbx"),
        type=SourceType.databricks,
        host="acme.cloud.databricks.com",
        password="dapiTOKEN",
        database="ext",
        federation_hints={"schema": "itest", "table": "people", **(hints or {})},
        **kw,
    )


# --- reach selection ------------------------------------------------------------------------


@pytest.mark.parametrize("engine", _ENGINES)
def test_without_the_hint_a_databricks_source_lands(engine):
    e = _engine(engine)
    connector = e.connector_for_source(_src())
    assert connector.mechanism is Mechanism.DIRECT
    assert not connector.reads_in_place
    assert federate(_src(), e) is Strategy.MATERIALIZED
    # The type stays a landed type, so the kind's tables need a landing clock (REQ-1907)
    assert not e.connectors["databricks"].reads_in_place


@pytest.mark.parametrize("engine", _ENGINES)
def test_with_reach_replica_a_databricks_source_lands(engine):
    e = _engine(engine)
    assert e.connector_for_source(_src({"reach": "replica"})).mechanism is Mechanism.DIRECT
    assert federate(_src({"reach": "replica"}), e) is Strategy.MATERIALIZED


@pytest.mark.parametrize("engine", _ENGINES)
def test_with_reach_scan_a_databricks_source_is_read_in_place(engine):
    e = _engine(engine)
    connector = e.connector_for_source(_src({"reach": "scan"}))
    assert connector.opt_in_reach and connector.mechanism is Mechanism.SCAN
    assert connector.reads_in_place
    assert federate(_src({"reach": "scan"}), e) is Strategy.SCAN
    assert e.resolve(_src({"reach": "scan"})).mechanism is Mechanism.SCAN


@pytest.mark.parametrize("engine", _ENGINES)
def test_an_unknown_reach_is_refused_naming_it(engine):
    with pytest.raises(ValueError, match=r"reach='live' is not one of 'replica', 'scan'"):
        _engine(engine).connector_for_source(_src({"reach": "live"}))


def test_an_engine_that_cannot_scan_the_type_refuses_the_hint_by_name():
    e = _engine("duckdb")
    pg = Source(id="pgs", type=SourceType.postgresql, host="h", federation_hints={"reach": "scan"})
    with pytest.raises(UnreachableSource, match=r"pgs.*reach: scan.*cannot read 'postgresql'"):
        e.connector_for_source(pg)


@pytest.mark.parametrize("engine", _ENGINES)
def test_the_opt_in_reach_is_kept_apart_from_the_per_type_reach(engine):
    e = build_engine(engine)  # builds with the complete reach
    assert e.scan_connectors["databricks"].opt_in_reach
    # the per-type reach stays the land reach complete_reach gives a type nothing else claims
    assert e.connectors["databricks"].mechanism is Mechanism.DIRECT
    assert not e.connectors["databricks"].opt_in_reach


# --- per-query re-pointing: DuckDB runtime --------------------------------------------------

_TABLE = DatabricksIcebergTable(
    "s3://bucket/uc/ext/itest/people/metadata/00002-a'b.metadata.json",
    (
        StorageCredential(
            "s3://bucket/uc/ext",
            {
                "s3.access-key-id": "ASIAKEY",
                "s3.secret-access-key": "sec'ret",
                "s3.session-token": "token",
                "client.region": "us-east-2",
            },
        ),
    ),
)


class _RecordingCon:
    def __init__(self) -> None:
        self.sql: list[str] = []

    def execute(self, sql: str, *args):
        self.sql.append(sql)
        return self

    def cursor(self):  # the runtime runs each statement on a cursor of its own (DuckDB 2.0)
        return self

    def close(self) -> None:
        pass


@pytest.fixture
def stub_resolver(monkeypatch):
    calls: list[str] = []

    def _resolve(source):
        calls.append(source.id)
        return _TABLE

    monkeypatch.setattr(
        "provisa.federation.databricks_iceberg_resolver.resolve_databricks_iceberg_table", _resolve
    )
    return calls


def _duckdb_runtime():
    from provisa.federation.duckdb_runtime import DuckDBFederationRuntime

    rt = object.__new__(DuckDBFederationRuntime)
    rt._con = _RecordingCon()
    rt._dbx_relations = {}
    rt._dbx_lock = threading.Lock()
    rt._ch_relations = {}
    rt._ch_lock = threading.Lock()
    rt._ext_loaded = set()
    rt._httpfs_loaded = False
    return rt


def test_duckdb_attach_loads_iceberg_issues_the_vended_secret_and_points_the_view(stub_resolver):
    rt = _duckdb_runtime()
    phys = '"dbx"."itest"."people"'
    rt._attach_databricks(_pg_source({"reach": "scan"}), ", allow_moved_paths := true", phys)
    sql = rt._con.sql
    assert "INSTALL iceberg" in sql and "LOAD iceberg" in sql and "LOAD httpfs" in sql
    secret = next(s for s in sql if s.startswith("CREATE OR REPLACE SECRET"))
    assert secret == (
        "CREATE OR REPLACE SECRET \"_dbx_dbx_0\" (TYPE s3, KEY_ID 'ASIAKEY', SECRET 'sec''ret', "
        "SCOPE 's3://bucket/uc/ext', SESSION_TOKEN 'token', REGION 'us-east-2')"
    )
    assert sql[-1] == (
        f"CREATE OR REPLACE VIEW {phys} AS SELECT * FROM iceberg_scan("
        "'s3://bucket/uc/ext/itest/people/metadata/00002-a''b.metadata.json', "
        "allow_moved_paths := true)"
    )
    assert stub_resolver == ["dbx"]


def test_duckdb_re_points_the_view_before_each_statement_that_names_it(stub_resolver):
    rt = _duckdb_runtime()
    phys = '"dbx"."itest"."people"'
    rt._attach_databricks(_pg_source({"reach": "scan"}), ", allow_moved_paths := true", phys)
    rt._refresh_databricks_relations(f"SELECT count(*) FROM {phys.upper()}")
    rt._refresh_databricks_relations('SELECT 1 FROM "other"."t"')
    rt._refresh_databricks_relations(f"SELECT * FROM {phys}")
    # once at attach, once per statement naming it; never for a statement that does not
    assert stub_resolver == ["dbx", "dbx", "dbx"]


def test_duckdb_detach_stops_the_re_pointing(stub_resolver):
    rt = _duckdb_runtime()
    rt._catalog_of = lambda source: "dbx"  # type: ignore[method-assign]
    rt._phys_catalogs = set()
    phys = '"dbx"."itest"."people"'
    source = SimpleNamespace(**_src({"reach": "scan"}).model_dump(), schema_name="itest")
    source.table_name = "people"
    rt._attach_databricks(source, "", phys)
    rt.detach_source(source)
    rt._refresh_databricks_relations(f"SELECT * FROM {phys}")
    assert stub_resolver == ["dbx"]


# --- per-query re-pointing and #244: pg runtime ----------------------------------------------


class _AutocommitConnection:
    """What a store cursor's ``connection`` says: schema creation asks it for autocommit."""

    autocommit = True


class _PgCursor:
    connection = _AutocommitConnection()

    def __init__(self, log: list) -> None:
        self.log = log
        self._last = ""

    def execute(self, sql, params=None):
        self.log.append(sql if isinstance(sql, str) else sql.as_string(None))
        self._last = self.log[-1]

    def fetchone(self):
        if "pg_foreign_server" in self._last:
            return (["updatable=false"],)
        if "pg_user_mappings" in self._last:
            return ([],)
        if "pg_class" in self._last or "relkind" in self._last:
            return None
        return None


def _pg_runtime(monkeypatch):
    from provisa.federation import pg_runtime as mod

    rt = object.__new__(mod.PgFederationRuntime)
    log: list = []
    rt._con = SimpleNamespace(cursor=lambda: _PgCursor(log))
    rt._dbx_relations = {}
    rt._dbx_lock = threading.Lock()
    # psycopg2.sql composition needs a live connection to render; record the statement shape
    monkeypatch.setattr(
        mod.PgFederationRuntime,
        "_alter_options",
        staticmethod(lambda cur, statement, have, options: log.append((statement, dict(options)))),
    )
    monkeypatch.setattr(
        "provisa.federation.replica_guard.pg_relation_kind", lambda cur, schema, table: None
    )
    return rt, log


def _pg_source(hints=None):
    s = _src(hints)
    return SimpleNamespace(
        id=s.id,
        type=s.type,
        host=s.host,
        password=s.password,
        database=s.database,
        federation_hints=s.federation_hints,
        catalog="dbx",
        schema_name="itest",
        table_name="people",
        columns=[("id", "integer"), ("name", "text")],
    )


def test_pg_a_scan_reader_becomes_a_named_column_live_view(monkeypatch):
    rt, log = _pg_runtime(monkeypatch)
    relation = rt._scan_relation("read_parquet('s3://b/x.parquet')", _pg_source())
    assert relation == (
        "(SELECT r['id']::integer AS \"id\", r['name']::text AS \"name\" FROM "
        "read_parquet('s3://b/x.parquet') r) AS _scan"
    )


def test_pg_a_scan_needs_the_tables_registered_columns(monkeypatch):
    rt, _ = _pg_runtime(monkeypatch)
    source = _pg_source()
    source.columns = []
    with pytest.raises(ValueError, match="needs the table's registered columns"):
        rt._scan_relation("read_csv('x.csv')", source)


def test_pg_databricks_stores_the_vended_secret_and_points_the_view(monkeypatch, stub_resolver):
    rt, log = _pg_runtime(monkeypatch)
    source = _pg_source({"reach": "scan"})
    rt._point_databricks_view(source, ", allow_moved_paths := true")
    assert "CREATE SERVER IF NOT EXISTS \"_dbx_dbx_0\" TYPE 's3' FOREIGN DATA WRAPPER duckdb" in log
    assert (
        'ALTER SERVER "_dbx_dbx_0"',
        {"scope": "s3://bucket/uc/ext", "region": "us-east-2"},
    ) in log
    assert (
        'ALTER USER MAPPING FOR CURRENT_USER SERVER "_dbx_dbx_0"',
        {"key_id": "ASIAKEY", "secret": "sec'ret", "session_token": "token"},
    ) in log
    view = next(s for s in log if isinstance(s, str) and s.startswith("CREATE OR REPLACE VIEW"))
    assert view.startswith('CREATE OR REPLACE VIEW "dbx_itest"."people" AS SELECT * FROM (SELECT')
    assert (
        "FROM iceberg_scan('s3://bucket/uc/ext/itest/people/metadata/00002-a''b.metadata.json', "
        "allow_moved_paths := true) r) AS _scan"
    ) in view


def test_pg_re_points_before_each_statement_that_names_the_view(monkeypatch, stub_resolver):
    rt, _ = _pg_runtime(monkeypatch)
    source = _pg_source({"reach": "scan"})
    rt._dbx_relations[rt._live_view_name(source)] = (source, "")
    rt.refresh_live_relations('SELECT * FROM "dbx_itest"."people" WHERE id = 1')
    rt.refresh_live_relations('SELECT * FROM "dbx_itest"."other"')
    assert stub_resolver == ["dbx"]
