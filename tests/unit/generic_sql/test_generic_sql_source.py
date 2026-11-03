# Copyright (c) 2026 Kenneth Stott
# Canary: 47a5aed3-7bf2-457a-af7a-5a83e64644d9
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Generic SQL sources (REQ-2191): a URL whose scheme names the SQLAlchemy dialect, a SQL dialect
Provisa writes, capability settings held per engine and all off to start, a file URL held to the
org's folder (REQ-1997), and pushdown read from the source."""

# Requirements: REQ-2191

from __future__ import annotations

import json
import sqlite3
from types import SimpleNamespace

import pytest
import sqlglot

import provisa.core.settings_catalog  # noqa: F401 -- declares the settings read here
from provisa.core import org_files, secrets_providers
from provisa.core.models import Source, SourceType
from provisa.generic_sql import settings as gs
from provisa.generic_sql import url as gurl

pytestmark = pytest.mark.unit


@pytest.fixture
def org(tmp_path, monkeypatch):
    monkeypatch.setenv("PROVISA_DATA_DIR", str(tmp_path / "data"))
    folder = org_files.folder("acme")
    (folder / "files").mkdir(parents=True)
    monkeypatch.setattr("provisa.core.org_files.current_org_segment", lambda: "acme")
    monkeypatch.setattr(secrets_providers, "_many_organisations", lambda: False)
    return folder


def _hints(url: str, dialect: str = "sqlite", **caps: str) -> dict[str, str]:
    return {
        gs.URL: url,
        gs.SQL_DIALECT: dialect,
        **{gs.capabilities_key(engine): on for engine, on in caps.items()},
    }


def _input(hints: dict[str, str]) -> SimpleNamespace:
    return SimpleNamespace(id="shop", type="generic_sql", federation_hints_json=json.dumps(hints))


# -- settings ------------------------------------------------------------------------------------


def test_the_sql_dialects_are_sqlglots_and_ansi():
    dialects = gs.sql_dialects()
    assert {"postgres", "tsql", "sqlite", "duckdb", "ansi"} <= set(dialects)
    assert "" not in dialects
    # The ansi dialect is one SQLGlot writes.
    assert sqlglot.transpile("SELECT a FROM t LIMIT 1", write="ansi") == ["SELECT a FROM t LIMIT 1"]


def test_every_capability_is_off_to_start():
    settings = gs.parse(_hints("sqlite:///{files}/shop.db"))
    for capability in gs.CAPABILITIES:
        assert not settings.on("duckdb", capability)


def test_a_capability_is_held_per_engine():
    settings = gs.parse(_hints("postgresql://h/db", "postgres", duckdb="attach,predicate"))
    assert settings.on("duckdb", gs.ATTACH) and settings.on("duckdb", gs.PREDICATE)
    # After an engine swap nothing is on for the new engine until it is set there.
    assert not settings.on("trino", gs.ATTACH) and not settings.on("trino", gs.PREDICATE)


@pytest.mark.parametrize(
    ("hints", "said"),
    [
        ({gs.SQL_DIALECT: "sqlite"}, "needs its connection URL"),
        ({gs.URL: "sqlite:///a.db"}, "needs the SQL dialect"),
        ({gs.URL: "sqlite:///a.db", gs.SQL_DIALECT: "klingon"}, "'klingon' is not a SQL dialect"),
        ({gs.URL: "no-scheme", gs.SQL_DIALECT: "ansi"}, "names no scheme"),
        (_hints("sqlite:///a.db", duckdb="predicate,functions"), "functions is not a capability"),
        (_hints("sqlite:///a.db", duckdb="attach"), "a sqlite URL has no engine attach"),
    ],
)
def test_settings_that_cannot_be_a_source_are_refused_by_name(hints, said):
    with pytest.raises(gs.InvalidGenericSqlSource, match=said):
        gs.parse(hints)


def test_the_attach_an_engine_can_try_follows_from_the_url_scheme():
    assert gs.attach_kind("postgresql+psycopg2://h/db") == "postgres"
    assert gs.attach_kind("postgresql://h/db") == "postgres"
    assert gs.attach_kind("mysql+pymysql://h/db") == "mysql"
    assert gs.attach_kind("mssql+pyodbc://h/db") is None
    assert gs.attach_kind("sqlite:///{files}/a.db") is None


def test_the_sources_dialect_is_the_one_its_admin_chose():
    source = Source(
        id="shop", type=SourceType.generic_sql, federation_hints=_hints("sqlite:///a.db", "ansi")
    )
    assert source.dialect == "ansi"


# -- the URL -------------------------------------------------------------------------------------


def test_a_url_carrying_a_password_is_refused():
    with pytest.raises(gs.InvalidGenericSqlSource, match="password field"):
        gurl.check("postgresql://u:secret@h/db")


def test_a_file_url_opens_its_token_path_under_the_org_folder(org):
    url, shown = gurl.connection_url("sqlite:///{files}/shop.db", "", "")
    assert url.database == str(org / "files" / "shop.db")
    assert shown == "{files}/shop.db"  # named as written, never its physical place


def test_a_file_url_that_leaves_the_org_folder_is_refused_naming_the_path(org):
    with pytest.raises(gurl.FileUrlRefused, match=r"\{files\}/\.\./\.\./x\.db") as refused:
        gurl.check("sqlite:///{files}/../../x.db")
    assert refused.value.path == "{files}/../../x.db"


def test_on_a_server_of_many_organizations_only_the_org_files_area_may_be_named(org, monkeypatch):
    monkeypatch.setattr(secrets_providers, "_many_organisations", lambda: True)
    gurl.check("sqlite:///{files}/shop.db")
    gurl.check("duckdb:///{org}/shop.duckdb")
    for elsewhere in ("sqlite:////data/shop.db", "duckdb:///:memory:", "sqlite://"):
        with pytest.raises(gurl.FileUrlRefused, match="files area"):
            gurl.check(elsewhere)
    # On a server of one organization a file elsewhere stands.
    monkeypatch.setattr(secrets_providers, "_many_organisations", lambda: False)
    gurl.check("sqlite:////data/shop.db")


def test_a_held_orgs_url_gives_only_the_allowed_driver_options(monkeypatch):
    """A driver option can name another address than the URL's host: psycopg2 connects to a
    ``?host=`` instead of it, past the check the URL's host was given."""
    from sqlalchemy.dialects.postgresql.psycopg2 import PGDialect_psycopg2
    from sqlalchemy.engine import make_url

    url = "postgresql+psycopg2://db.example.com/sales?host=10.0.0.5&sslmode=require"
    _, kwargs = PGDialect_psycopg2().create_connect_args(make_url(url))
    assert kwargs["host"] == "10.0.0.5"
    monkeypatch.setattr("provisa.security.sandbox_egress.held", lambda _org: True)
    with pytest.raises(gurl.DriverOptionsRefused, match="option host;") as refused:
        gurl.check(url)
    assert refused.value.options == ["host"]  # sslmode is one it may give
    with pytest.raises(gurl.DriverOptionsRefused):
        gurl.connection_url(url, "u", "p")
    gurl.check("postgresql+psycopg2://db.example.com/sales")
    gurl.check(
        "postgresql://db.example.com/sales?sslmode=verify-full&connect_timeout=5"
        "&application_name=provisa&target_session_attrs=read-write"
    )
    gurl.check("mysql+pymysql://db.example.com/sales?ssl_mode=REQUIRED&charset=utf8mb4")
    gurl.check("mssql+pyodbc://db.example.com/sales?driver=ODBC+Driver+18&Encrypt=yes")
    for refused_url, option in [
        ("mysql+pymysql://db.example.com/s?local_infile=1", "local_infile"),
        ("mysql+mysqldb://db.example.com/s?read_default_file=/etc/my.cnf", "read_default_file"),
        ("postgresql://db.example.com/s?sslkey=/etc/key", "sslkey"),
        ("postgresql://db.example.com/s?service=prod", "service"),
        ("mssql+pyodbc://db.example.com/s?odbc_connect=Server%3D10.0.0.5", "odbc_connect"),
        ("oracle+oracledb://db.example.com/s?dsn=x", "dsn"),
    ]:
        with pytest.raises(gurl.DriverOptionsRefused, match=option):
            gurl.check(refused_url)
    # An org whose sources are not held gives the options its driver takes.
    monkeypatch.setattr("provisa.security.sandbox_egress.held", lambda _org: False)
    gurl.check(url)


def test_a_scheme_provisa_does_not_ship_fails_naming_what_is_missing():
    from sqlalchemy.engine import make_url

    with pytest.raises(gurl.MissingDialect, match="'nosuchdb'"):
        gurl.create_engine(make_url("nosuchdb://h/db"))
    with pytest.raises(gurl.MissingDialect, match="'cx_Oracle'"):
        gurl.create_engine(make_url("oracle+cx_oracle://h/db"))


def test_the_url_host_is_a_destination_the_egress_check_sees():
    from provisa.security.sandbox_egress import destinations

    hints = _hints("mssql+pyodbc://db.example.com:1433/sales", "tsql")
    assert ("db.example.com", 1433) in destinations({"federation_hints": hints})
    # A file database points nowhere.
    assert destinations({"federation_hints": _hints("sqlite:///{files}/a.db")}) == []


# -- the save --------------------------------------------------------------------------------------


def test_the_save_refuses_by_name(org, monkeypatch):
    from provisa.api.admin.schema_common import generic_sql_refusal

    assert generic_sql_refusal(_input(_hints("sqlite:///{files}/shop.db"))) is None
    assert generic_sql_refusal(SimpleNamespace(id="x", type="sqlite")) is None
    invalid = generic_sql_refusal(_input({gs.URL: "sqlite:///a.db", gs.SQL_DIALECT: "nope"}))
    assert invalid is not None and invalid.code == "schema.generic_sql_invalid"
    leaves = generic_sql_refusal(_input(_hints("sqlite:///{files}/../../x.db")))
    assert leaves is not None and leaves.code == "schema.generic_sql_file_refused"
    assert leaves.params["path"] == "{files}/../../x.db"
    attach = generic_sql_refusal(_input(_hints("postgresql://h/db", "postgres", duckdb="attach")))
    assert attach is not None and attach.code == "schema.generic_sql_attach_unbuilt"
    monkeypatch.setattr("provisa.security.sandbox_egress.held", lambda _org: True)
    options = generic_sql_refusal(_input(_hints("postgresql://h/db?host=10.0.0.5", "postgres")))
    assert options is not None and options.code == "schema.generic_sql_options_refused"
    assert options.params["options"] == "host"


# -- the driver ------------------------------------------------------------------------------------


async def test_a_sqlite_file_url_reads_through_the_pool_in_its_chosen_dialect(org):
    from provisa.executor.pool import SourcePool

    db = sqlite3.connect(org / "files" / "shop.db")
    db.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, amount REAL)")
    db.execute("INSERT INTO orders VALUES (1, 9.5), (2, 3.25)")
    db.commit()
    db.close()
    pool = SourcePool()
    await pool.add(
        "shop", "generic_sql", "localhost", 0, "", "", "", extra=_hints("sqlite:///{files}/shop.db")
    )
    try:
        assert pool.dialect_for("shop") == "sqlite"
        assert pool.vector_capability("shop") is None  # nothing is checked on connect
        result = await pool.execute("shop", "SELECT id, amount FROM orders WHERE id = $1", [2])
        assert result.rows == [(2, 3.25)]
        driver = pool.get("shop")
        assert await driver.schema_names() == ["main"]
        assert await driver.table_names("main") == [("orders", None)]
        assert await driver.column_types("main", "orders") == [
            ("id", "INTEGER"),
            ("amount", "REAL"),
        ]
    finally:
        await pool.close_all()


async def test_a_duckdb_file_on_a_server_of_many_organizations_reaches_no_other_file(
    org, monkeypatch, tmp_path
):
    import duckdb

    from provisa.executor.drivers.generic_sql import GenericSQLDriver

    con = duckdb.connect(str(org / "files" / "shop.duckdb"))
    con.execute("CREATE TABLE t AS SELECT 1 AS a")
    con.close()
    outside = tmp_path / "outside.csv"
    outside.write_text("a\n1\n")
    monkeypatch.setattr(secrets_providers, "_many_organisations", lambda: True)
    driver = GenericSQLDriver()
    driver.configure(_hints("duckdb:///{files}/shop.duckdb", "duckdb"))
    await driver.connect("", 0, "", "", "")
    try:
        assert (await driver.execute("SELECT a FROM t")).rows == [(1,)]
        with pytest.raises(Exception, match="disabled"):
            await driver.execute(f"SELECT * FROM read_csv('{outside}')")
    finally:
        await driver.close()


# -- pushdown, read from the source ------------------------------------------------------------------


def test_connector_pushdown_reads_the_sources_settings_for_the_engine():
    from provisa.federation.engine import build_duckdb_engine

    engine = build_duckdb_engine().complete_reach()

    def source(**caps: str) -> Source:
        hints = _hints("postgresql://h/db", "postgres", **caps)
        return Source(id="shop", type=SourceType.generic_sql, federation_hints=hints)

    # Not attached on this engine: its rows land, and the replica is read as any landed table.
    landed = engine.connector_pushdown(source())
    assert landed == engine.connector_for("generic_sql").capability()
    # Attached here: only what its settings for this engine turn on.
    attached = engine.connector_pushdown(source(**{engine.name: "attach,predicate"}))
    assert attached.predicate_pushdown and not attached.join_pushdown
    assert not attached.aggregate_pushdown and not attached.write
    # Settings for another engine say nothing about this one.
    assert engine.connector_pushdown(source(trino="attach,predicate,join")) == landed


# -- a customer's JDBC driver ----------------------------------------------------------------------


def test_a_jdbc_url_needs_its_driver_jar_and_class_and_has_no_attach():
    from provisa.generic_sql import jdbc

    url = "jdbc:sqlite:{files}/shop.db"
    with pytest.raises(gs.InvalidGenericSqlSource, match="jdbc_driver_jar and jdbc_driver_class"):
        gs.parse(_hints(url))
    hints = {**_hints(url), jdbc.DRIVER_JAR: "{files}/sqlite-jdbc.jar", jdbc.DRIVER_CLASS: "x.D"}
    assert gs.parse(hints).url == url
    assert gs.attach_kind(url) is None
    with pytest.raises(gs.InvalidGenericSqlSource, match="jdbc URL has no engine attach"):
        gs.parse({**hints, gs.capabilities_key("duckdb"): "attach"})


def test_a_jdbc_driver_is_refused_by_name_on_a_server_of_many_organizations(org, monkeypatch):
    from provisa.api.admin.schema_common import generic_sql_refusal
    from provisa.generic_sql import jdbc

    hints = {
        **_hints("jdbc:sqlite:{files}/shop.db"),
        jdbc.DRIVER_JAR: "{files}/sqlite-jdbc.jar",
        jdbc.DRIVER_CLASS: "org.sqlite.JDBC",
    }
    assert generic_sql_refusal(_input(hints)) is None  # a self-hosted install runs it
    monkeypatch.setattr(secrets_providers, "_many_organisations", lambda: True)
    refused = generic_sql_refusal(_input(hints))
    assert refused is not None and refused.code == "schema.generic_sql_jdbc_refused"
    assert "self-hosted" in refused.message
    with pytest.raises(jdbc.JdbcRefused):
        jdbc.connect("jdbc:sqlite::memory:", "{files}/sqlite-jdbc.jar", "org.sqlite.JDBC", "", "")
    # Nor for a held org (a sandbox), whose destination a JDBC URL cannot be checked for.
    monkeypatch.setattr(secrets_providers, "_many_organisations", lambda: False)
    monkeypatch.setattr("provisa.security.sandbox_egress.held", lambda _org: True)
    with pytest.raises(jdbc.JdbcRefused):
        jdbc.refuse_where_not_allowed()


def test_jdbc_binds_by_position_in_the_order_the_placeholders_appear():
    from provisa.generic_sql.jdbc import qmark

    assert qmark("a = $2 AND b = $1 OR c = $2", [10, 20]) == (
        "a = ? AND b = ? OR c = ?",
        [20, 10, 20],
    )
    assert qmark("SELECT 1", None) == ("SELECT 1", [])
