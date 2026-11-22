# Copyright (c) 2026 Kenneth Stott
# Canary: 5a2e9c17-4b8d-4f63-9e0a-1c7d3b6f8e42
#
# This source code is licensed under the Business Source License 1.1
"""No registered value reaches the PostgreSQL or ClickHouse engine's DDL unquoted, and no value
adds a setting to a connection string a DuckDB extension parses again.

A role holding source or table registration writes a source's host, port, database, user,
credentials and path, a table's schema and name and a column's name. Each site that puts one of
them into a statement is fed values built to end the literal or the identifier it sits in; the
statement must stay one statement (the engine's own grammar, through sqlglot's tokenizer for that
dialect, finds no ``;`` outside a literal) and the value must come back whole as a literal or a
name. The connection strings are read back by ports of the extensions' own parsers (the mssql
extension's TokenizeConnectionString, the firebird extension's FirebirdConnectionInfo::Parse);
tests/integration/test_duckdb_connection_string_grammar_e2e.py checks them against the real
extensions and servers.

The last tests hold every federation module to the quoting helpers: a statement text built from
an interpolated value that is not written by one of them fails.
"""

from __future__ import annotations

import ast
import re
import types
from pathlib import Path
from typing import Any
from urllib.parse import unquote_plus

import pytest
from sqlglot.dialects.clickhouse import ClickHouse
from sqlglot.dialects.duckdb import DuckDB
from sqlglot.dialects.postgres import Postgres
from sqlglot.tokens import TokenType

import provisa.api.app  # noqa: F401 -- the API layer says whether the deployment holds many orgs, which resolving a source asks
from provisa.core.models import SourceType
from provisa.federation import clickhouse_connectors as chc
from provisa.federation import connector_duckdb as cd
from provisa.federation import pg_motherduck
from provisa.federation.clickhouse_quoting import bind_table, ch_ident, ch_lit
from provisa.federation.clickhouse_runtime import ClickHouseFederationRuntime
from provisa.federation.duckdb_quoting import ado_dsn, ado_value, bigquery_dsn, firebird_url
from provisa.federation.pg_quoting import pg_ident, pg_lit
from provisa.federation.pg_runtime import PgFederationRuntime

HOSTILE = [
    pytest.param("a'b", id="single-quote"),
    pytest.param('a"b', id="double-quote"),
    pytest.param("a;b", id="semicolon"),
    pytest.param("x')", id="quote-paren"),
    pytest.param("a\\b", id="backslash"),
    pytest.param("a\\'b", id="backslash-quote"),
    pytest.param("a\nb", id="newline"),
    pytest.param("'); DROP SCHEMA public CASCADE; --", id="stacked-literal"),
    pytest.param('"; DROP SCHEMA public CASCADE; --', id="stacked-identifier"),
]


def _source(**fields) -> Any:
    base = {
        "id": "src-1",
        "type": SourceType.postgresql,
        "host": "db.internal",
        "port": 5432,
        "database": "shop",
        "username": "reader",
        "password": "pw",
        "path": "/data/x.db",
        "base_url": None,
        "federation_hints": {},
        "mapping": {},
        "max_live_concurrency": None,
        "download_from": None,
        "schema_name": "public",
        "table_name": "orders",
        "catalog": "src_1",
        "columns": [("id", "bigint")],
    }
    return types.SimpleNamespace(**{**base, **fields})


def _tokens(sql: str, dialect: Any) -> list:
    return list(dialect().tokenize(sql))


def _one_statement(sql: str, dialect: Any, value: str) -> None:
    """``sql`` holds no statement separator outside a literal or a name, and ``value`` is whole
    in one of its literals or names, as ``dialect`` reads them."""
    tokens = _tokens(sql, dialect)
    assert not [t for t in tokens if t.token_type == TokenType.SEMICOLON], sql
    held = {t.text for t in tokens if t.token_type in (TokenType.STRING, TokenType.IDENTIFIER)}
    assert value in held, (value, sql)


def _pg_one(sql: str, value: str) -> None:
    _one_statement(sql, Postgres, value)


def _ch_one(sql: str, value: str) -> None:
    _one_statement(sql, ClickHouse, value)


# -- the helpers ---------------------------------------------------------------------------------


@pytest.mark.parametrize("value", HOSTILE)
def test_pg_lit_and_pg_ident_read_back_as_written(value):
    _pg_one(f"SELECT {pg_lit(value)} AS {pg_ident(value)}", value)


@pytest.mark.parametrize("value", HOSTILE)
def test_ch_lit_and_ch_ident_read_back_as_written(value):
    _ch_one(f"SELECT {ch_lit(value)} AS {ch_ident(value)}", value)


def test_a_name_with_a_nul_is_refused():
    with pytest.raises(ValueError):
        pg_ident("a\x00b")
    with pytest.raises(ValueError):
        ch_ident("a\x00b")


# -- PostgreSQL engine connectors (connector_duckdb.py, engine = "postgres") ----------------------

_PG_SERVER_CONNECTORS = [
    pytest.param(cd.PostgresFdwConnector, id="postgres_fdw"),
    pytest.param(cd.PgClickHouseFdwConnector, id="pg_clickhouse"),
    pytest.param(cd.MysqlFdwConnector, id="mysql_fdw"),
    pytest.param(cd.TdsFdwConnector, id="tds_fdw"),
    pytest.param(cd.OracleFdwConnector, id="oracle_fdw"),
]


@pytest.mark.parametrize("connector", _PG_SERVER_CONNECTORS)
@pytest.mark.parametrize("field", ["host", "database", "username", "password"])
@pytest.mark.parametrize("value", HOSTILE)
def test_a_pg_engine_server_value_stays_a_literal(connector, field, value):
    details = connector().details(_source(**{field: value}))
    if connector is cd.MysqlFdwConnector and field == "database":
        # mysql_fdw's server takes no database: it is the remote schema imported, a name.
        imports = [s for s in details["attach_ddl"] if s.startswith("IMPORT FOREIGN SCHEMA")]
        _pg_one(imports[0], value)
        return
    held = [s for s in details["attach_ddl"] if value in "".join(_strings(s, Postgres))]
    assert held, f"{field} is not in any statement"
    for statement in details["attach_ddl"]:
        _pg_statement(statement)
    if connector is cd.OracleFdwConnector and field in ("host", "database"):
        assert any("//" in s and value in s for s in _all_strings(details, Postgres))
    else:
        assert value in _all_strings(details, Postgres)


@pytest.mark.parametrize("connector", _PG_SERVER_CONNECTORS)
@pytest.mark.parametrize("value", HOSTILE)
def test_a_pg_engine_remote_schema_stays_a_name(connector, value):
    if connector is cd.OracleFdwConnector:
        value = value.upper()  # Oracle schema names are upper-cased
    if connector is cd.PgClickHouseFdwConnector:
        source = _source(database=value)
    else:
        source = _source(federation_hints={"schema": value})
    details = connector().details(source)
    imports = [s for s in details["attach_ddl"] if s.startswith("IMPORT FOREIGN SCHEMA")]
    assert len(imports) == 1
    _pg_one(imports[0], value)


def _strings(sql: str, dialect: Any) -> list[str]:
    return [t.text for t in _tokens(sql, dialect) if t.token_type == TokenType.STRING]


def _all_strings(details: dict, dialect: Any) -> list[str]:
    return [s for stmt in details["attach_ddl"] for s in _strings(stmt, dialect)]


def _pg_statement(sql: str) -> None:
    assert not [t for t in _tokens(sql, Postgres) if t.token_type == TokenType.SEMICOLON], sql


@pytest.mark.parametrize("value", HOSTILE)
def test_the_postgres_fdw_copy_ddl_quotes_as_the_attach_does(value):
    details = cd.PostgresFdwConnector().details(_source(password=value))
    for statement in details["server_ddl_for_copy"]:
        _pg_statement(statement)
    assert value in [s for stmt in details["server_ddl_for_copy"] for s in _strings(stmt, Postgres)]


@pytest.mark.parametrize("value", HOSTILE)
def test_a_sqlite_fdw_path_stays_a_literal(value):
    details = cd.SqliteFdwConnector().details(_source(type=SourceType.sqlite, path=f"/d/{value}"))
    server = [s for s in details["attach_ddl"] if "sqlite_fdw OPTIONS" in s]
    _pg_one(server[0], f"/d/{value}")


@pytest.mark.parametrize("value", HOSTILE)
def test_a_file_fdw_path_stays_a_literal(value):
    details = cd.FileFdwConnector().details(_source(type=SourceType.csv, path=f"/d/{value}"))
    _pg_one(f"CREATE FOREIGN TABLE t (a int) SERVER s {details['table_options']}", f"/d/{value}")


@pytest.mark.parametrize("value", HOSTILE)
def test_a_pg_duckdb_scan_path_stays_a_literal(value):
    details = cd.PgDuckdbCsvConnector().details(_source(type=SourceType.csv, path=f"/d/{value}"))
    _pg_one(f"SELECT * FROM {details['scan']} r", f"/d/{value}")


@pytest.mark.parametrize("value", HOSTILE)
def test_the_pgwire_bridge_password_stays_a_literal(value, monkeypatch):
    from provisa.federation import pgwire_endpoint, pgwire_replica

    monkeypatch.setattr(
        pgwire_replica,
        "ensure_endpoint",
        lambda source: types.SimpleNamespace(pgwire_host="127.0.0.1", pgwire_port=5999),
    )
    monkeypatch.setattr(pgwire_endpoint, "sign_in_password", lambda source: value)
    details = cd.PgFilesConnector().details(_source(type=SourceType.files, id="files-1"))
    mapping = [s for s in details["attach_ddl"] if "USER MAPPING" in s]
    _pg_one(mapping[0], value)


@pytest.mark.parametrize("value", HOSTILE)
def test_a_motherduck_synced_table_is_named_quoted(value):
    class _Cur:
        def execute(self, sql, params=None):
            pass

        def fetchone(self):
            return ("ddb$shop",)

    located = pg_motherduck._located(_Cur(), "ddb$shop", value)
    assert located is not None
    _pg_one(f"SELECT * FROM {located}", value)


# -- PostgreSQL engine runtime (pg_runtime.py) ---------------------------------------------------


class _AutocommitConnection:
    autocommit = True


class _Cur:
    connection = _AutocommitConnection()

    def __init__(self, log: list[str], fetch: Any = None) -> None:
        self._log = log
        self._fetch = fetch

    def execute(self, sql: str, params=None) -> None:
        self._log.append(sql)

    def fetchone(self):
        return self._fetch


class _Con:
    def __init__(self, log: list[str], fetch: Any = None) -> None:
        self._log = log
        self._fetch = fetch

    def cursor(self):
        return _Cur(self._log, self._fetch)


def _pg_runtime(details: dict, log: list[str], fetch: Any = None) -> PgFederationRuntime:
    import threading

    rt = PgFederationRuntime.__new__(PgFederationRuntime)
    rt._con = _Con(log, fetch)  # type: ignore[assignment] -- a recording connection
    rt._raw_attached = set()
    rt._dbx_lock = threading.Lock()
    rt._dbx_relations = {}
    rt._engine = types.SimpleNamespace(  # type: ignore[assignment] -- resolves to ``details``
        resolve=lambda source: types.SimpleNamespace(details=details)
    )
    return rt


@pytest.mark.parametrize("field", ["table_name", "schema_name"])
@pytest.mark.parametrize("value", HOSTILE)
def test_the_pg_live_view_names_stay_names(field, value):
    log: list[str] = []
    details = {"attach_ddl": [], "local_schema": "fdw_src_1"}
    _pg_runtime(details, log).attach_source(_source(**{field: value}))
    view = [s for s in log if s.startswith("CREATE OR REPLACE VIEW")]
    assert len(view) == 1
    _pg_statement(view[0])
    names = {t.text for t in _tokens(view[0], Postgres) if t.token_type == TokenType.IDENTIFIER}
    assert any(value in n for n in names), view[0]
    for statement in log:
        _pg_statement(statement)


@pytest.mark.parametrize("value", HOSTILE)
def test_a_file_fdw_foreign_table_and_its_columns_stay_names(value):
    log: list[str] = []
    details = cd.FileFdwConnector().details(_source(type=SourceType.csv))
    _pg_runtime(details, log).attach_source(
        _source(type=SourceType.csv, table_name=value, columns=[(value, "text")])
    )
    foreign = [s for s in log if s.startswith("CREATE FOREIGN TABLE")]
    _pg_one(foreign[0], value)
    for statement in log:
        _pg_statement(statement)


@pytest.mark.parametrize("value", HOSTILE)
def test_a_pg_duckdb_scan_column_stays_a_literal_and_a_name(value):
    relation = PgFederationRuntime._scan_relation(
        "read_csv('/d/x.csv')", _source(columns=[(value, "text")])
    )
    sql = f"SELECT * FROM {relation}"
    tokens = _tokens(sql, Postgres)
    assert not [t for t in tokens if t.token_type == TokenType.SEMICOLON]
    assert value in {t.text for t in tokens if t.token_type == TokenType.STRING}
    assert value in {t.text for t in tokens if t.token_type == TokenType.IDENTIFIER}


@pytest.mark.parametrize("value", HOSTILE)
def test_a_pg_replica_table_and_its_columns_stay_names(value):
    rt = _pg_runtime({}, [])
    ddl = rt._create_table_ddl(value, value, [(value, "text"), ("id", "integer")], [value])
    _pg_one(ddl, value)


@pytest.mark.parametrize("value", HOSTILE)
def test_pg_replica_row_writes_name_their_table_and_columns_quoted(value):
    log: list[str] = []

    class _ManyCur(_Cur):
        def executemany(self, sql, rows):
            self._log.append(sql)

    rt = _pg_runtime({}, log)
    cur = _ManyCur(log)
    rt._insert_rows(cur, value, value, [value], [{value: 1}])
    rt._upsert_rows(cur, value, value, [value, "v"], [value], [{value: 1, "v": 2}])
    assert len(log) == 2
    for statement in log:
        _pg_one(statement, value)


@pytest.mark.parametrize("value", HOSTILE)
def test_a_pg_region_import_names_stay_names(value):
    log: list[str] = []
    rt = _pg_runtime({}, log, fetch=(["updatable=false"],))
    rt._region_imports = {}
    rt.ensure_materialize_attached = lambda: "provisa"
    rt.attach_region_table("eu", "postgresql://u:p@h:5432/db", value, value)
    # The options are set through psycopg2.sql (composed, not text): only the text is read here.
    text = [s for s in log if isinstance(s, str)]
    imports = [s for s in text if s.startswith("IMPORT FOREIGN SCHEMA")]
    _pg_one(imports[0], value)
    for statement in text:
        _pg_statement(statement)


@pytest.mark.parametrize("value", HOSTILE)
def test_a_pg_detach_names_the_view_quoted(value, monkeypatch):
    from provisa.federation import replica_guard

    monkeypatch.setattr(replica_guard, "pg_relation_kind", lambda cur, s, t: ("x", "v"))
    log: list[str] = []
    _pg_runtime({}, log).detach_source(_source(table_name=value))
    drops = [s for s in log if s.startswith("DROP VIEW")]
    _pg_one(drops[0], value)


# -- ClickHouse engine connectors (clickhouse_connectors.py) --------------------------------------


@pytest.mark.parametrize(
    "connector",
    [
        pytest.param(chc.ClickHousePostgresConnector, id="postgresql"),
        pytest.param(chc.ClickHouseMysqlConnector, id="mysql"),
    ],
)
@pytest.mark.parametrize("field", ["host", "database", "username", "password"])
@pytest.mark.parametrize("value", HOSTILE)
def test_a_clickhouse_database_engine_value_stays_a_literal(connector, field, value):
    (statement,) = connector().details(_source(**{field: value}))["attach_ddl"]
    held = _strings(statement, ClickHouse)
    assert not [t for t in _tokens(statement, ClickHouse) if t.token_type == TokenType.SEMICOLON]
    assert any(value in s for s in held), statement
    if field != "host":
        assert value in held


@pytest.mark.parametrize("value", HOSTILE)
def test_a_clickhouse_remote_schema_stays_a_literal(value):
    (statement,) = chc.ClickHousePostgresConnector().details(
        _source(federation_hints={"schema": value})
    )["attach_ddl"]
    _ch_one(statement, value)


@pytest.mark.parametrize("value", HOSTILE)
def test_a_clickhouse_sqlite_path_stays_a_literal(value):
    (statement,) = chc.ClickHouseSqliteConnector().details(
        _source(type=SourceType.sqlite, path=f"/d/{value}")
    )["attach_ddl"]
    _ch_one(statement, f"/d/{value}")


@pytest.mark.parametrize("value", HOSTILE)
def test_the_clickhouse_pgwire_bridge_password_stays_a_literal(value, monkeypatch):
    from provisa.federation import pgwire_endpoint, pgwire_replica

    monkeypatch.setattr(
        pgwire_replica,
        "ensure_endpoint",
        lambda source: types.SimpleNamespace(pgwire_host="127.0.0.1", pgwire_port=5999),
    )
    monkeypatch.setattr(pgwire_endpoint, "sign_in_password", lambda source: value)
    (statement,) = chc.ClickHouseFilesConnector().details(
        _source(type=SourceType.files, id="files-1")
    )["attach_ddl"]
    _ch_one(statement, value)


@pytest.mark.parametrize("field", ["host", "database", "username", "password"])
@pytest.mark.parametrize("value", HOSTILE)
def test_a_clickhouse_mongo_value_stays_a_literal_and_the_table_binds_alone(field, value):
    clause = chc.ClickHouseMongoConnector().details(
        _source(type=SourceType.mongodb, **{field: value})
    )["engine_clause"]
    bound = bind_table(clause, "orders")
    sql = f"CREATE TABLE t (id Int64) ENGINE = {bound}"
    held = _strings(sql, ClickHouse)
    assert not [t for t in _tokens(sql, ClickHouse) if t.token_type == TokenType.SEMICOLON]
    assert "orders" in held
    assert any(value in s for s in held), sql


@pytest.mark.parametrize(
    "path",
    [
        pytest.param("s3://b/{v}.parquet", id="s3"),
        pytest.param("https://h/{v}.parquet", id="url"),
        pytest.param("/d/{v}.parquet", id="file"),
    ],
)
@pytest.mark.parametrize("value", HOSTILE)
def test_a_clickhouse_file_engine_path_stays_a_literal(path, value):
    clause = chc.ClickHouseParquetConnector().details(
        _source(type=SourceType.parquet, path=path.format(v=value))
    )["engine_clause"]
    _ch_one(f"CREATE TABLE t ENGINE = {clause}", path.format(v=value))


@pytest.mark.parametrize("value", HOSTILE)
def test_clickhouse_object_store_credentials_stay_literals(value):
    hints = {"aws_key": value, "aws_secret": value}
    for connector in (chc.ClickHouseParquetConnector(), chc.ClickHouseIcebergConnector()):
        clause = connector.details(
            _source(type=SourceType.parquet, path="s3://b/t", federation_hints=hints)
        )["engine_clause"]
        sql = f"CREATE TABLE t ENGINE = {clause}"
        assert _strings(sql, ClickHouse).count(value) == 2, sql


def test_an_access_key_with_no_secret_is_refused():
    with pytest.raises(ValueError, match="no secret"):
        chc.ClickHouseParquetConnector().details(
            _source(type=SourceType.parquet, path="s3://b/t", federation_hints={"aws_key": "k"})
        )


# -- bind_table ----------------------------------------------------------------------------------


@pytest.mark.parametrize("value", HOSTILE)
def test_bind_table_writes_the_table_as_a_literal(value):
    bound = bind_table("MongoDB('h:1', 'db', '{table}', 'u', 'p')", value)
    _ch_one(f"CREATE TABLE t (a Int64) ENGINE = {bound}", value)


def test_bind_table_leaves_a_value_holding_the_placeholder_text():
    clause = f"MongoDB('h:1', 'db', '{{table}}', 'u', {ch_lit(chr(39) + '{table}' + chr(39))})"
    bound = bind_table(clause, "orders")
    assert _strings(bound, ClickHouse) == ["h:1", "db", "orders", "u", "'{table}'"]


# -- ClickHouse engine runtime (clickhouse_runtime.py) --------------------------------------------


class _CHBackend:
    def __init__(self, rows: list | None = None) -> None:
        self.commands: list[str] = []
        self.queries: list[str] = []
        self._rows = rows or []

    def command(self, sql: str) -> None:
        self.commands.append(sql)

    def query(self, sql: str, params=None):
        self.queries.append(sql)
        return self._rows, []


def _ch_runtime(details: dict, backend: _CHBackend) -> ClickHouseFederationRuntime:
    rt = ClickHouseFederationRuntime(backend)  # type: ignore[arg-type] -- a recording backend
    rt._engine = types.SimpleNamespace(  # type: ignore[assignment] -- resolves to ``details``
        resolve=lambda source: types.SimpleNamespace(details=details)
    )
    return rt


@pytest.mark.parametrize("field", ["table_name", "schema_name"])
@pytest.mark.parametrize("value", HOSTILE)
def test_the_clickhouse_live_view_names_stay_names(field, value):
    backend = _CHBackend()
    details = {"attach_ddl": [], "local_schema": "ch_src_1"}
    _ch_runtime(details, backend).attach_source(_source(**{field: value}))
    view = [s for s in backend.commands if s.startswith("CREATE VIEW")]
    assert len(view) == 1
    names = {t.text for t in _tokens(view[0], ClickHouse) if t.token_type == TokenType.IDENTIFIER}
    assert any(value in n for n in names), view[0]
    for statement in backend.commands:
        assert not [
            t for t in _tokens(statement, ClickHouse) if t.token_type == TokenType.SEMICOLON
        ]


@pytest.mark.parametrize("value", HOSTILE)
def test_a_clickhouse_staged_table_its_columns_and_its_collection_stay_quoted(value):
    backend = _CHBackend()
    details = chc.ClickHouseMongoConnector().details(_source(type=SourceType.mongodb))
    _ch_runtime(details, backend).attach_source(
        _source(type=SourceType.mongodb, table_name=value), columns=[(value, "String")]
    )
    staged = [s for s in backend.commands if s.startswith("CREATE TABLE")]
    _ch_one(staged[0], value)
    assert value in _strings(staged[0], ClickHouse)  # the collection, bound as a literal


@pytest.mark.parametrize("value", HOSTILE)
def test_a_clickhouse_detach_names_the_view_quoted(value):
    backend = _CHBackend(rows=[("View",)])
    _ch_runtime({}, backend).detach_source(_source(table_name=value))
    _ch_one(backend.queries[0], value)
    drops = [s for s in backend.commands if s.startswith("DROP VIEW")]
    _ch_one(drops[0], value)


# -- connection strings a DuckDB extension parses again ------------------------------------------


def _ado_tokenize(cs: str) -> list[tuple[str, str]]:
    """A port of the mssql extension's TokenizeConnectionString (src/mssql_storage.cpp,
    hugr-lab/mssql-extension): ``;``-separated ``key=value``, a value in ``{...}`` with ``}}`` a
    literal ``}``, an unquoted value trimmed."""
    out: list[tuple[str, str]] = []
    n, i = len(cs), 0
    while i < n:
        while i < n and cs[i] in "; \t":
            i += 1
        if i >= n:
            break
        start = i
        while i < n and cs[i] not in "=;":
            i += 1
        if i >= n or cs[i] != "=":
            while i < n and cs[i] != ";":
                i += 1
            continue
        key = cs[start:i].strip()
        i += 1
        while i < n and cs[i] in " \t":
            i += 1
        if i < n and cs[i] == "{":
            i += 1
            value = []
            while i < n:
                if cs[i] == "}":
                    if i + 1 < n and cs[i + 1] == "}":
                        value.append("}")
                        i += 2
                        continue
                    i += 1
                    break
                value.append(cs[i])
                i += 1
            while i < n and cs[i] in " \t":
                i += 1
            if i < n and cs[i] == ";":
                i += 1
            text = "".join(value)
        else:
            start = i
            while i < n and cs[i] != ";":
                i += 1
            text = cs[start:i].strip(" \t")
            if i < n and cs[i] == ";":
                i += 1
        if key:
            out.append((key, text))
    return out


ADO_HOSTILE = [
    *HOSTILE,
    pytest.param("p;Database=other", id="adds-a-key"),
    pytest.param("{p}", id="braces"),
    pytest.param("p}}x{", id="brace-runs"),
    pytest.param(" p ", id="spaces"),
    pytest.param("p=q", id="equals"),
    pytest.param("", id="empty"),
]


@pytest.mark.parametrize("value", ADO_HOSTILE)
def test_an_ado_value_is_read_back_whole_and_adds_no_key(value):
    cs = ado_dsn(("Server", "h,1433"), ("Password", value), ("Database", "shop"))
    assert _ado_tokenize(cs) == [("Server", "h,1433"), ("Password", value), ("Database", "shop")]


def test_an_ordinary_ado_value_is_written_bare():
    assert ado_value("shop") == "shop"
    assert ado_value("Provisa_2026!") == "Provisa_2026!"


@pytest.mark.parametrize("field", ["host", "database", "username", "password"])
@pytest.mark.parametrize("value", ADO_HOSTILE)
def test_the_sql_server_attach_string_carries_each_value_whole(field, value):
    source = _source(type=SourceType.sqlserver, port=1433, **{field: value})
    attach = cd.DuckDBMssqlConnector("sqlserver").details(source)["attach"]
    (dsn,) = [t.text for t in _tokens(attach, DuckDB) if t.token_type == TokenType.STRING]
    pairs = dict(_ado_tokenize(dsn))
    assert list(pairs) == ["Server", "Database", "User Id", "Password"]
    want = {
        "host": ("Server", f"{value},1433"),
        "database": ("Database", value),
        "username": ("User Id", value),
        "password": ("Password", value),
    }[field]
    assert pairs[want[0]] == want[1].strip(" \t") or pairs[want[0]] == want[1]


@pytest.mark.parametrize("value", ADO_HOSTILE)
def test_the_entra_attach_string_carries_the_database_whole(value):
    source = _source(type=SourceType.fabric, database=value)
    attach = cd.DuckDBMssqlConnector("fabric").details(source)["attach"]
    dsns = [s for s in _strings(attach, DuckDB) if s.startswith("Server=")]
    assert dict(_ado_tokenize(dsns[0])) == {"Server": "db.internal,5432", "Database": value}


def _firebird_parse(url: str) -> dict[str, str]:
    """A port of the firebird extension's URL form (FirebirdConnectionInfo::Parse,
    src/firebird_client.cpp, flozer/duckdb-firebird): options after the first ``?``, userinfo
    before the first ``@`` split at its first ``:`` and URL-decoded (``+`` a space), host and
    path as written."""
    rest = url.removeprefix("firebird://")
    query = ""
    if "?" in rest:
        rest, query = rest.split("?", 1)
    info: dict[str, str] = {}
    if "@" in rest:
        userinfo, rest = rest.split("@", 1)
        user, _, password = userinfo.partition(":")
        info["user"], info["password"] = unquote_plus(user), unquote_plus(password)
    host, slash, path = rest.partition("/")
    if ":" in host and "]" not in host:
        name, _, port = host.rpartition(":")
        host = f"{name}/{port}"
    info["database"] = f"{host}:{slash}{path}" if host else f"{slash}{path}"
    for kv in filter(None, query.split("&")):
        key, _, val = kv.partition("=")
        info[key] = unquote_plus(val)
    return info


FIREBIRD_HOSTILE = [
    *HOSTILE,
    pytest.param("p@h:1/x", id="at"),
    pytest.param("p:q", id="colon"),
    pytest.param("p?role=RDB$ADMIN", id="adds-an-option"),
    pytest.param("p%41+q", id="percent-plus"),
    pytest.param("p&dialect=1", id="ampersand"),
]


@pytest.mark.parametrize("field", ["username", "password"])
@pytest.mark.parametrize("value", FIREBIRD_HOSTILE)
def test_a_firebird_credential_is_read_back_whole_and_adds_no_option(field, value):
    source = _source(type=SourceType.firebird, port=3050, path="/db/x.fdb", **{field: value})
    attach = cd.DuckDBFirebirdConnector().details(source)["attach"]
    (url,) = _strings(attach, DuckDB)
    info = _firebird_parse(url)
    assert set(info) == {"user", "password", "database"}
    assert info["user" if field == "username" else "password"] == value
    assert info["database"] == "db.internal/3050://db/x.fdb"


@pytest.mark.parametrize("field", ["host", "path"])
def test_a_firebird_host_or_path_with_a_question_mark_is_refused(field):
    source = _source(type=SourceType.firebird, port=3050, path="/db/x.fdb")
    setattr(source, field, getattr(source, field) + "?role=RDB$ADMIN")
    with pytest.raises(ValueError, match="cannot hold '\\?'"):
        cd.DuckDBFirebirdConnector().details(source)


def test_an_ordinary_firebird_url_is_unchanged():
    assert (
        firebird_url("provisa", "provisa", "fb", 3050, "/firebird/data/test.fdb")
        == "firebird://provisa:provisa@fb:3050//firebird/data/test.fdb"
    )


@pytest.mark.parametrize(
    "project", ["p api_endpoint=evil.example", "p\tdataset=x", "p\nx", " p", ""]
)
def test_a_bigquery_project_holding_whitespace_is_refused(project):
    with pytest.raises(ValueError, match="one word"):
        bigquery_dsn(project)
    with pytest.raises(ValueError, match="one word"):
        cd.DuckDBBigQueryConnector().details(
            _source(type=SourceType.bigquery, federation_hints={"project": project})
        )


@pytest.mark.parametrize("project", ["my-project-123", "example.com:proj", "p'x"])
def test_a_bigquery_project_is_the_one_setting(project):
    attach = cd.DuckDBBigQueryConnector().details(
        _source(type=SourceType.bigquery, federation_hints={"project": project})
    )["attach"]
    (dsn,) = _strings(attach, DuckDB)
    assert dsn == f"project={project}"
    assert dsn.split(" ") == [f"project={project}"]


# -- structure: every federation module writes values through a quoter -------------------------

FEDERATION = Path(cd.__file__).parent
#: What may write an interpolated value into statement text.
QUOTERS = frozenset(
    {
        "lit",
        "ident",
        "libpq_dsn",
        "libpq_value",
        "sql_literal",
        "int",
        "pg_lit",
        "pg_ident",
        "ch_lit",
        "ch_ident",
        "bind_table",
        "ado_dsn",
        "ado_value",
        "firebird_url",
        "bigquery_dsn",
    }
)
_SQL_TEXT = re.compile(
    r"\b(ATTACH|DETACH|CREATE|DROP|ALTER|SELECT|INSERT|UPDATE|DELETE|COPY|INSTALL|LOAD|FROM|CALL|"
    r"DESCRIBE|TYPE|SECRET|KEY_ID|ENDPOINT|REGION|SCOPE|FILEPATH|ACCOUNT|USER|PASSWORD|DATABASE|"
    r"WAREHOUSE|ROLE|TENANT_ID|CLIENT_ID|CLIENT_SECRET|CONNECTION_STRING|OPTIONS|SERVER|IMPORT|"
    r"ENGINE)\b"
    # ClickHouse's table and database engines: the name and its argument list.
    r"|\b(PostgreSQL|MySQL|SQLite|MongoDB|S3|URL|File|IcebergS3|DeltaLake)\("
)

#: Interpolations that are not a registered value, each read and found to be one of: statement
#: text already built through the helper (a quoted name, a clause list, a whole scan), a constant
#: of the module, or a name of the engine's own. (module, function, expression).
REVIEWED: frozenset[tuple[str, str, str]] = frozenset(
    {
        # duckdb_extensions.py: an extension name of the registry; the module's repository constant
        ("duckdb_extensions.py", "load_statement", "extension"),
        ("duckdb_extensions.py", "load_statement", "EXTENSION_REPOSITORIES[extension][0]"),
        # connector_duckdb.py
        ("connector_duckdb.py", "_attach_options", "kind"),  # "sqlite" or "duckdb", from the caller
        ("connector_duckdb.py", "details", "_attach_options(source, 'sqlite')"),  # option keywords
        ("connector_duckdb.py", "details", "_attach_options(source, 'duckdb')"),  # option keywords
        ("connector_duckdb.py", "_scan_view_ddl", "scanner"),  # the connector's table function
        ("connector_duckdb.py", "_s3_secret_ddl", "', '.join(clauses)"),  # clauses of lit()
        (
            "connector_duckdb.py",
            "object_store_secret_ddl",
            "', '.join(clauses)",
        ),  # clauses of lit()
        ("connector_duckdb.py", "object_store_secret_ddl", "name"),  # ident()
        ("connector_duckdb.py", "details", "', '.join(options)"),  # snowflake: options of lit()
        ("connector_duckdb.py", "details", "provider"),  # azure: keywords and lit()
        # duckdb_runtime.py
        ("duckdb_runtime.py", "_point_databricks_view", "options"),  # DuckDB's option names, lit()
        ("duckdb_runtime.py", "_point_databricks_view", "phys"),  # _phys_name: ident()
        ("duckdb_runtime.py", "_point_databricks_view", "scan_args"),  # the connector's constant
        ("duckdb_runtime.py", "attach_source", "phys"),  # _phys_name: ident()
        ("duckdb_runtime.py", "attach_source", "remote"),  # ident()
        ("duckdb_runtime.py", "attach_source", "renamed"),  # ident() pairs
        ("duckdb_runtime.py", "attach_source", "scan"),  # the connector's scan: _scan_view_ddl
        ("duckdb_runtime.py", "attach_region_ops", "select"),  # an ops view's own SELECT
        ("duckdb_runtime.py", "attach_region_store", "store_type"),  # _ATTACH_TYPE_BY_SCHEME
        ("duckdb_runtime.py", "ensure_materialize_attached", "store_type"),  # the same
        ("duckdb_runtime.py", "ensure_materialize_attached", "self._MAT_STORE"),  # "mat_store"
        ("duckdb_runtime.py", "_attach_motherduck_store", "self._MAT_STORE"),  # "mat_store"
        ("duckdb_runtime.py", "_copy_store_table", "target"),  # _local_store_name: ident()
        ("duckdb_runtime.py", "_copy_store_table", "_typed_back(arrow_tbl.schema)"),  # ident()
        ("duckdb_runtime.py", "_index_replica_vectors", "target"),  # _local_store_name: ident()
        ("duckdb_runtime.py", "_index_replica_vectors", "quoted"),  # ident()
        ("duckdb_runtime.py", "introspect_columns", "phys"),  # _phys_name: ident()
        ("duckdb_runtime.py", "introspect_columns", "target"),  # phys, or the ClickHouse read
        ("duckdb_runtime.py", "describe_sync", "duck_sql"),  # the governed statement itself
        # connector_duckdb.py, PostgreSQL engine classes
        ("connector_duckdb.py", "details", "pw"),  # pgwire bridge: ", password " + pg_lit()
        # pg_motherduck.py
        ("pg_motherduck.py", "_server", "clause"),  # option names from a fixed list, values bound
        ("pg_motherduck.py", "_server", "f' OPTIONS ({clause})' if clause else ''"),  # the same
        # pg_runtime.py
        ("pg_runtime.py", "_apply_events", "key_where"),  # pg_ident() = %s, values bound
        ("pg_runtime.py", "_apply_events", "where"),  # pg_ident() = %s, values bound
        ("pg_runtime.py", "_held", "key_where"),  # the same
        ("pg_runtime.py", "_create_live_view", "remote"),  # a relation of pg_ident() / pg_lit()
        ("pg_runtime.py", "_create_table_ddl", "', '.join(col_defs)"),  # pg_ident() + to_physical
        ("pg_runtime.py", "_insert_rows", "cols_sql"),  # pg_ident()
        ("pg_runtime.py", "_insert_rows", "placeholders"),  # "%s, %s, ..."
        ("pg_runtime.py", "_upsert_rows", "cols_sql"),  # pg_ident()
        ("pg_runtime.py", "_upsert_rows", "conflict_cols"),  # pg_ident()
        ("pg_runtime.py", "_upsert_rows", "do_update"),  # set_sql, or DO NOTHING
        ("pg_runtime.py", "_upsert_rows", "placeholders"),  # "%s, %s, ..."
        ("pg_runtime.py", "_upsert_rows", "set_sql"),  # pg_ident() = EXCLUDED.pg_ident()
        ("pg_runtime.py", "_scan_relation", "cols"),  # r[pg_lit()]::<column type> AS pg_ident()
        ("pg_runtime.py", "_scan_relation", "scan"),  # the connector's scan: reader(pg_lit())
        ("pg_runtime.py", "attach_source", "cols"),  # pg_ident() <column type>
        ("pg_runtime.py", "attach_source", "ft"),  # pg_ident()
        ("pg_runtime.py", "attach_source", "details['table_options']"),  # file_fdw: pg_lit()
        ("pg_runtime.py", "describe_sync", "sql"),  # the governed statement itself
        ("pg_runtime.py", "write_result_csv", "query"),  # the governed statement itself
        # clickhouse_connectors.py
        ("clickhouse_connectors.py", "_clickhouse_file_engine", "creds"),  # ch_lit() pairs
        # clickhouse_runtime.py
        ("clickhouse_runtime.py", "attach_source", "clause"),  # the connector's, bind_table()
        ("clickhouse_runtime.py", "attach_source", "col_ddl"),  # ch_ident() <column type>
        ("clickhouse_runtime.py", "attach_source", "phys"),  # ch_ident()
        ("clickhouse_runtime.py", "attach_source", "remote"),  # ch_ident()
        ("clickhouse_runtime.py", "attach_source", "staged"),  # ch_ident()
        ("clickhouse_runtime.py", "insert_arrow", "names"),  # ch_ident()
        ("clickhouse_runtime.py", "introspect_columns", "phys"),  # ch_ident()
        # custom_connectors.py: a template takes raw values; each placeholder is quoted by kind
        ("custom_connectors.py", "details", "_pg_options(self._server_options, fields)"),  # pg_lit
        ("custom_connectors.py", "details", "_pg_options(self._table_options, fields)"),  # pg_lit
        ("custom_connectors.py", "details", "_pg_options(self._user_mapping, fields)"),  # pg_lit
        ("custom_connectors.py", "details", "scan"),  # _clause: lit() / ident() / int()
        # _clause: ch_lit() / ch_ident() / int()
        ("custom_connectors.py", "details", "_clause(self._engine_template, 'clickhouse', fields)"),
    }
)


def _is_quoted(value: ast.expr) -> bool:
    if isinstance(value, ast.Constant):
        return True
    if not isinstance(value, ast.Call):
        return False
    func = value.func
    name = func.id if isinstance(func, ast.Name) else getattr(func, "attr", None)
    return name in QUOTERS


def _unquoted_interpolations(module: str, source: str) -> set[tuple[str, str, str]]:
    """Every value written into statement text of ``source`` by something other than a quoter:
    an f-string field, an operand of ``+`` or ``%``, an argument of ``.format``."""
    tree = ast.parse(source)
    found: set[tuple[str, str, str]] = set()

    def text_of(node: ast.expr) -> str | None:
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            return node.value
        if isinstance(node, ast.JoinedStr):
            return "".join(
                v.value
                for v in node.values
                if isinstance(v, ast.Constant) and isinstance(v.value, str)
            )
        return None

    def visit(node: ast.AST, function: str) -> None:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            function = node.name
        values: list[ast.expr] = []
        if isinstance(node, ast.JoinedStr) and _SQL_TEXT.search(text_of(node) or ""):
            values = [v.value for v in node.values if isinstance(v, ast.FormattedValue)]
        elif isinstance(node, ast.BinOp) and isinstance(node.op, (ast.Add, ast.Mod)):
            sides = [node.left, node.right]
            if any(_SQL_TEXT.search(text_of(s) or "") for s in sides):
                values = [s for s in sides if text_of(s) is None]
        elif (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "format"
            and _SQL_TEXT.search(text_of(node.func.value) or "")
        ):
            values = [*node.args, *(k.value for k in node.keywords)]
        for value in values:
            if not _is_quoted(value):
                # Quotes folded to one kind: ast.unparse picks them differently across 3.12
                # patch releases (a nested f-string).
                found.add((module, function, ast.unparse(value).replace('"', "'")))
        for child in ast.iter_child_nodes(node):
            visit(child, function)

    visit(tree, "<module>")
    return found


def test_the_detector_sees_each_way_a_value_is_spliced():
    spliced = """
def f(source):
    a = f"ATTACH '{source.path}' AS x"
    b = "SELECT * FROM " + source.table_name
    c = "DROP VIEW %s" % source.table_name
    d = "CREATE SCHEMA {}".format(source.schema_name)
    e = f"ATTACH {lit(source.path)} AS {ident(source.id)}"
    return f"no statement here: {source.id}"
"""
    assert _unquoted_interpolations("m.py", spliced) == {
        ("m.py", "f", "source.path"),
        ("m.py", "f", "source.table_name"),
        ("m.py", "f", "source.schema_name"),
    }


#: Interpolations in modules outside the three engines' DDL that are NOT yet read: each is to be
#: written through a quoter or moved to ``REVIEWED`` with what it is, by its own job. The set only
#: shrinks: a new interpolation in any federation module fails the structure test, and an entry
#: whose interpolation is gone fails it too, so the list stays exact. (module, function, expression).
PENDING: frozenset[tuple[str, str, str]] = frozenset(
    {
        # backend.py
        ("backend.py", "execute", "self.engine.name"),
        ("backend.py", "execute_sync", "self.engine.name"),
        ("backend.py", "prepare_region_ops", "CATALOG"),
        ("backend.py", "prepare_region_ops", "model_schema"),
        ("backend.py", "prepare_region_ops", "name"),
        ("backend.py", "prepare_region_ops", "select"),
        ("backend.py", "refresh_landed_views", "PROVISA_ADMIN_CATALOG"),
        # bigquery_connectors.py
        ("bigquery_connectors.py", "external_table_ddl", "dataset"),
        ("bigquery_connectors.py", "external_table_ddl", "fmt"),
        ("bigquery_connectors.py", "external_table_ddl", "project"),
        ("bigquery_connectors.py", "external_table_ddl", "table"),
        ("bigquery_connectors.py", "external_table_ddl", "uri"),
        ("bigquery_connectors.py", "external_table_ddl", "with_conn"),
        # bigquery_runtime.py
        ("bigquery_runtime.py", "detach_source", "dataset"),
        ("bigquery_runtime.py", "detach_source", "project"),
        ("bigquery_runtime.py", "detach_source", "table"),
        ("bigquery_runtime.py", "landed_table_exists", "self._location.lower()"),
        ("bigquery_runtime.py", "landed_table_exists", "self._project"),
        # bigquery_store.py
        ("bigquery_store.py", "constraint_statements", "cols"),
        ("bigquery_store.py", "constraint_statements", "edge.name"),
        ("bigquery_store.py", "constraint_statements", "fq"),
        ("bigquery_store.py", "constraint_statements", "name"),
        ("bigquery_store.py", "constraint_statements", "qualified(holder)"),
        ("bigquery_store.py", "constraint_statements", "qualified(referenced)"),
        ("bigquery_store.py", "constraint_statements", "ref_cols"),
        ("bigquery_store.py", "create_ddl", "cols"),
        ("bigquery_store.py", "create_ddl", "constraint"),
        ("bigquery_store.py", "create_ddl", "qualified(parts)"),
        ("bigquery_store.py", "existing_foreign_keys", "dataset"),
        ("bigquery_store.py", "existing_foreign_keys", "project"),
        ("bigquery_store.py", "existing_primary_key", "dataset"),
        ("bigquery_store.py", "existing_primary_key", "project"),
        ("bigquery_store.py", "expose_view", "body"),
        ("bigquery_store.py", "expose_view", "qualified(replica)"),
        ("bigquery_store.py", "expose_view", "qualified(view)"),
        ("bigquery_store.py", "reconcile_bigquery_native", "qualified(parts)"),
        # clickhouse_http_scan.py
        ("clickhouse_http_scan.py", "_replacement", "cols"),
        ("clickhouse_http_scan.py", "_replacement", "scan"),
        ("clickhouse_http_scan.py", "clickhouse_query", "', '.join(exprs)"),
        ("clickhouse_http_scan.py", "clickhouse_query", "_ch_ident(rel.database)"),
        ("clickhouse_http_scan.py", "clickhouse_query", "_ch_ident(rel.table)"),
        (
            "clickhouse_http_scan.py",
            "clickhouse_query",
            "f' WHERE {' AND '.join(predicates)}' if predicates else ''",
        ),
        ("clickhouse_http_scan.py", "columns_query", "_ch_string(database)"),
        ("clickhouse_http_scan.py", "columns_query", "_ch_string(table)"),
        ("clickhouse_http_scan.py", "rewrite", "_duck_ident(alias)"),
        ("clickhouse_http_scan.py", "rewrite", "replacement"),
        ("clickhouse_http_scan.py", "secret_ddl", "source_id"),
        # clickhouse_store.py
        ("clickhouse_store.py", "create_ddl", "cols"),
        ("clickhouse_store.py", "create_ddl", "order_clause"),
        ("clickhouse_store.py", "create_ddl", "qualified(parts)"),
        ("clickhouse_store.py", "ensure_namespace", "database"),
        ("clickhouse_store.py", "existing_columns", "_lit(database)"),
        ("clickhouse_store.py", "existing_columns", "_lit(table)"),
        ("clickhouse_store.py", "land_clickhouse_native", "collist"),
        ("clickhouse_store.py", "land_clickhouse_native", "fq"),
        ("clickhouse_store.py", "land_clickhouse_native", "values"),
        ("clickhouse_store.py", "reconcile_clickhouse_native", "qualified(parts)"),
        # codehost_connectors.py
        ("codehost_connectors.py", "details", "DATABASE"),
        ("codehost_connectors.py", "details", "USER"),
        ("codehost_connectors.py", "details", "alias"),
        ("codehost_connectors.py", "details", "files.certificate"),
        ("codehost_connectors.py", "details", "files.key"),
        ("codehost_connectors.py", "details", "files.root(source.id)"),
        ("codehost_connectors.py", "details", "host"),
        ("codehost_connectors.py", "details", "local_schema"),
        ("codehost_connectors.py", "details", "options"),
        ("codehost_connectors.py", "details", "port"),
        ("codehost_connectors.py", "details", "quoted"),
        ("codehost_connectors.py", "details", "schema"),
        ("codehost_connectors.py", "details", "server"),
        # connector_base.py
        ("connector_base.py", "probe_pg_extension", "ext"),
        # connector_mssql.py
        ("connector_mssql.py", "fetch_database_comment", "database"),
        ("connector_mssql.py", "fetch_database_comment", "host"),
        ("connector_mssql.py", "fetch_database_comment", "password"),
        ("connector_mssql.py", "fetch_database_comment", "port"),
        ("connector_mssql.py", "fetch_database_comment", "username"),
        # connector_sqlite.py
        ("connector_sqlite.py", "fetch_all_rows", "table"),
        # databricks_runtime.py
        ("databricks_runtime.py", "attach_source", "catalog"),
        ("databricks_runtime.py", "attach_source", "d['format']"),
        ("databricks_runtime.py", "attach_source", "d['location']"),
        ("databricks_runtime.py", "attach_source", "schema"),
        ("databricks_runtime.py", "attach_source", "table"),
        ("databricks_runtime.py", "detach_source", "catalog"),
        ("databricks_runtime.py", "detach_source", "schema"),
        ("databricks_runtime.py", "detach_source", "table"),
        # databricks_store.py
        ("databricks_store.py", "_create_ddl", "_qualified(catalog, schema, table)"),
        ("databricks_store.py", "_create_ddl", "cols"),
        ("databricks_store.py", "_create_ddl", "constraint"),
        ("databricks_store.py", "_ensure_namespace", "catalog"),
        ("databricks_store.py", "_ensure_namespace", "schema"),
        ("databricks_store.py", "_existing_columns", "catalog"),
        ("databricks_store.py", "_land_via_copy_into", "collist"),
        ("databricks_store.py", "_land_via_copy_into", "object_url"),
        ("databricks_store.py", "_land_via_copy_into", "qualified"),
        ("databricks_store.py", "comment_statements", "_lit(description)"),
        ("databricks_store.py", "comment_statements", "column"),
        ("databricks_store.py", "comment_statements", "fq"),
        ("databricks_store.py", "constraint_statements", "_pk_name(target.replica[2])"),
        ("databricks_store.py", "constraint_statements", "_q(holder)"),
        ("databricks_store.py", "constraint_statements", "_q(referenced)"),
        ("databricks_store.py", "constraint_statements", "cols"),
        ("databricks_store.py", "constraint_statements", "column"),
        ("databricks_store.py", "constraint_statements", "edge.name"),
        ("databricks_store.py", "constraint_statements", "fq"),
        ("databricks_store.py", "constraint_statements", "name"),
        ("databricks_store.py", "constraint_statements", "ref_cols"),
        ("databricks_store.py", "existing_comments", "_where(parts)"),
        ("databricks_store.py", "existing_comments", "catalog"),
        ("databricks_store.py", "existing_foreign_keys", "_where(parts)"),
        ("databricks_store.py", "existing_foreign_keys", "catalog"),
        ("databricks_store.py", "existing_primary_key", "_where(parts, 'c.')"),
        ("databricks_store.py", "existing_primary_key", "catalog"),
        ("databricks_store.py", "existing_tags", "catalog"),
        ("databricks_store.py", "existing_tags", "where"),
        ("databricks_store.py", "land_databricks_native", "collist"),
        ("databricks_store.py", "land_databricks_native", "placeholders"),
        ("databricks_store.py", "land_databricks_native", "qualified"),
        ("databricks_store.py", "object_exists", "_where(parts)"),
        ("databricks_store.py", "object_exists", "catalog"),
        (
            "databricks_store.py",
            "reconcile_databricks_native",
            "_qualified(catalog, schema, table)",
        ),
        ("databricks_store.py", "tag_statements", "_lit(value)"),
        ("databricks_store.py", "tag_statements", "_q(target.replica)"),
        ("databricks_store.py", "tag_statements", "column"),
        ("databricks_store.py", "tag_statements", "fq"),
        ("databricks_store.py", "tag_statements", "tag"),
        # delta.py
        ("delta.py", "apply_sql_delta", "field_list"),
        ("delta.py", "apply_sql_delta", "src"),
        ("delta.py", "apply_sql_delta", "wm"),
        ("delta.py", "source_max_watermark", "src"),
        ("delta.py", "source_max_watermark", "wm"),
        # duckdb_extensions.py
        ("duckdb_extensions.py", "load_staged", "extension"),
        ("duckdb_extensions.py", "load_staged", "repository"),
        # exact_numeric.py
        ("exact_numeric.py", "_source_select", "_quoted(schema)"),
        ("exact_numeric.py", "_source_select", "_quoted(table)"),
        ("exact_numeric.py", "_source_select", "selected"),
        ("exact_numeric.py", "exact_numeric_reads", "_quoted(table.alias_or_name)"),
        ("exact_numeric.py", "exact_numeric_reads", "alias"),
        ("exact_numeric.py", "exact_numeric_reads", "source_sql"),
        # mail_write.py
        ("mail_write.py", "_insert", "table"),
        ("mail_write.py", "holds", "', '.join(('?' for _ in columns))"),
        (
            "mail_write.py",
            "holds",
            "', '.join((f''{name}' {_DUCK_TYPES[kind]}' for name, kind in columns))",
        ),
        ("mail_write.py", "holds", "predicate"),
        # materialize_broker.py
        ("materialize_broker.py", "__init__", "_MAT_STORE_ALIAS"),
        ("materialize_broker.py", "__init__", "_MEMORY_PATH"),
        ("materialize_broker.py", "_counts", "_qualified(_MAT_STORE_ALIAS, schema, name)"),
        ("materialize_broker.py", "_do", "_MAT_STORE_ALIAS"),
        ("materialize_broker.py", "_do", "_qualified(_MAT_STORE_ALIAS, schema, build)"),
        ("materialize_broker.py", "_do", "_qualified(_MAT_STORE_ALIAS, schema, table)"),
        ("materialize_broker.py", "_do", "collist"),
        ("materialize_broker.py", "_do", "cols"),
        ("materialize_broker.py", "_do", "escaped"),
        ("materialize_broker.py", "_do", "name"),
        ("materialize_broker.py", "_do", "replica"),
        ("materialize_broker.py", "_do", "schema"),
        ("materialize_broker.py", "_do", "schema_part"),
        ("materialize_broker.py", "_do", "table"),
        ("materialize_broker.py", "_into", "_MAT_STORE_ALIAS"),
        ("materialize_broker.py", "_into", "dst_schema"),
        ("materialize_broker.py", "_into", "name"),
        ("materialize_broker.py", "_into", "src_path"),
        ("materialize_broker.py", "_into", "src_schema"),
        ("materialize_broker.py", "_with_store", "_MAT_STORE_ALIAS"),
        ("materialize_broker.py", "_with_store", "db_path"),
        ("materialize_broker.py", "_with_store", "mode"),
        ("materialize_broker.py", "replica_abort", "_qualified(_MAT_STORE_ALIAS, schema, build)"),
        ("materialize_broker.py", "zoned_times_as_text", "as_text"),
        ("materialize_broker.py", "zoned_times_as_text", "relation"),
        # mssql_warehouse_runtime.py
        ("mssql_warehouse_runtime.py", "_ensure_schema", "schema"),
        ("mssql_warehouse_runtime.py", "_land", "collist"),
        ("mssql_warehouse_runtime.py", "_land", "cols_ddl"),
        ("mssql_warehouse_runtime.py", "_land", "fq"),
        ("mssql_warehouse_runtime.py", "_land", "ph"),
        ("mssql_warehouse_runtime.py", "_land", "schema"),
        ("mssql_warehouse_runtime.py", "_land", "table"),
        ("mssql_warehouse_runtime.py", "_open", "driver_clause"),
        ("mssql_warehouse_runtime.py", "_open", "self._database"),
        ("mssql_warehouse_runtime.py", "_open", "self._server"),
        ("mssql_warehouse_runtime.py", "_reconcile", "cols_ddl"),
        ("mssql_warehouse_runtime.py", "_reconcile", "fq"),
        ("mssql_warehouse_runtime.py", "attach_source", "bulk_path"),
        ("mssql_warehouse_runtime.py", "attach_source", "d['format']"),
        ("mssql_warehouse_runtime.py", "attach_source", "fq"),
        ("mssql_warehouse_runtime.py", "detach_source", "schema"),
        ("mssql_warehouse_runtime.py", "detach_source", "table"),
        # pgwire_replica.py
        ("pgwire_replica.py", "adapter_columns", "only"),
        ("pgwire_replica.py", "adapter_columns", "schema"),
        ("pgwire_replica.py", "adapter_tables", "schema"),
        ("pgwire_replica.py", "jira_fields", "FIELD_LIST"),
        ("pgwire_replica.py", "jira_fields", "_quoted(issue_type)"),
        ("pgwire_replica.py", "jira_fields", "_quoted(project_key)"),
        ("pgwire_replica.py", "jira_fields", "schema"),
        ("pgwire_replica.py", "jira_form_list", "FORM_LIST"),
        ("pgwire_replica.py", "jira_form_list", "schema"),
        ("pgwire_replica.py", "land_via_select", "schema"),
        ("pgwire_replica.py", "land_via_select", "select"),
        ("pgwire_replica.py", "land_via_select", "table"),
        ("pgwire_replica.py", "land_via_select", "where"),
        ("pgwire_replica.py", "land_via_select_keys", "col"),
        ("pgwire_replica.py", "land_via_select_keys", "schema"),
        ("pgwire_replica.py", "land_via_select_keys", "table"),
        ("pgwire_replica.py", "servicenow_columns", "COLUMN_LIST"),
        ("pgwire_replica.py", "servicenow_columns", "schema"),
        ("pgwire_replica.py", "servicenow_columns", "table"),
        ("pgwire_replica.py", "servicenow_table_list", "TABLE_LIST"),
        ("pgwire_replica.py", "servicenow_table_list", "schema"),
        ("pgwire_replica.py", "servicenow_transform_maps", "TABLE_LIST"),
        ("pgwire_replica.py", "servicenow_transform_maps", "TRANSFORM_MAP_LIST"),
        ("pgwire_replica.py", "servicenow_transform_maps", "schema"),
        ("pgwire_replica.py", "servicenow_transform_maps", "target"),
        # query_residency.py
        ("query_residency.py", "home_view_read", "quoted_name(address)"),
        # replica_hot.py
        ("replica_hot.py", "_row_count", "ref"),
        # replica_source.py
        ("replica_source.py", "batches", "self._projection"),
        ("replica_source.py", "batches", "self._ref"),
        ("replica_source.py", "batches", "self._where"),
        # replica_target.py
        ("replica_target.py", "__init__", "_lit(schema)"),
        ("replica_target.py", "_begin", "qualified(build)"),
        ("replica_target.py", "_build", "_q(spec.name)"),
        ("replica_target.py", "_build", "cols"),
        ("replica_target.py", "_build", "index"),
        ("replica_target.py", "_build", "mark"),
        ("replica_target.py", "_build", "replica"),
        ("replica_target.py", "_build", "unique"),
        ("replica_target.py", "_drop", "qualified((self._database, name))"),
        ("replica_target.py", "_pg_index_vectors", "_q(_hnsw_index_name(table, column))"),
        ("replica_target.py", "_pg_index_vectors", "_q(column)"),
        ("replica_target.py", "_pg_index_vectors", "replica"),
        ("replica_target.py", "_pg_swap", "_q(build)"),
        ("replica_target.py", "_pg_swap", "_q(schema)"),
        ("replica_target.py", "_pg_swap", "_q(table)"),
        ("replica_target.py", "_pg_swap", "replica"),
        ("replica_target.py", "_swap", "build"),
        ("replica_target.py", "_swap", "previous"),
        ("replica_target.py", "_swap", "quote(self._table)"),
        ("replica_target.py", "_swap", "replica"),
        ("replica_target.py", "_table_engine", "_lit(name)"),
        ("replica_target.py", "_table_engine", "_lit(self._database)"),
        ("replica_target.py", "_write", "', '.join('?' * len(names))"),
        ("replica_target.py", "_write", "', '.join((quote(name) for name in names))"),
        ("replica_target.py", "_write", "quote(self._build)"),
        ("replica_target.py", "_write", "quote(self._schema)"),
        ("replica_target.py", "abort", "build"),
        ("replica_target.py", "begin", "_q(self._build)"),
        ("replica_target.py", "begin", "_q(self._schema)"),
        ("replica_target.py", "begin", "names"),
        ("replica_target.py", "drop", "_q(name)"),
        ("replica_target.py", "drop", "_q(self._schema)"),
        ("replica_target.py", "pg_statement_copy", "_q(schema)"),
        ("replica_target.py", "pg_statement_copy", "names"),
        ("replica_target.py", "pg_statement_copy", "selected"),
        ("replica_target.py", "pg_statement_copy", "source_relation"),
        ("replica_target.py", "pg_statement_copy", "target"),
        ("replica_target.py", "pg_statement_copy", "where"),
        # replica_target_warehouse.py
        (
            "replica_target_warehouse.py",
            "_abort",
            "qualified((self._database, self._schema, self._build))",
        ),
        ("replica_target_warehouse.py", "_abort", "self._ref(self._build)"),
        ("replica_target_warehouse.py", "_begin", "DATABRICKS_BUILD_VOLUME"),
        (
            "replica_target_warehouse.py",
            "_begin",
            "qualified((self._database, self._schema, self._build))",
        ),
        ("replica_target_warehouse.py", "_begin", "self._catalog"),
        ("replica_target_warehouse.py", "_begin", "self._database"),
        ("replica_target_warehouse.py", "_begin", "self._ref(self._build)"),
        ("replica_target_warehouse.py", "_begin", "self._schema"),
        ("replica_target_warehouse.py", "_ddl", "cols"),
        ("replica_target_warehouse.py", "_ddl", "self._ref(table)"),
        (
            "replica_target_warehouse.py",
            "_drop",
            "_qualified(self._catalog, self._schema, self._table)",
        ),
        ("replica_target_warehouse.py", "_drop", "qualified((self._database, self._schema, name))"),
        ("replica_target_warehouse.py", "_drop", "self._ref(name)"),
        ("replica_target_warehouse.py", "_swap", "build"),
        ("replica_target_warehouse.py", "_swap", "collist"),
        ("replica_target_warehouse.py", "_swap", "names"),
        ("replica_target_warehouse.py", "_swap", "projection"),
        ("replica_target_warehouse.py", "_swap", "replica"),
        ("replica_target_warehouse.py", "_swap", "self._dir"),
        ("replica_target_warehouse.py", "_swap", "self._ref(self._build)"),
        ("replica_target_warehouse.py", "_swap", "self._ref(self._table)"),
        ("replica_target_warehouse.py", "_write", "collist"),
        ("replica_target_warehouse.py", "_write", "marks"),
        ("replica_target_warehouse.py", "_write", "self._ref(self._build)"),
        # result_sink.py
        ("result_sink.py", "clickhouse_count", "_CH_S3"),
        ("result_sink.py", "clickhouse_insert", "_CH_S3"),
        ("result_sink.py", "clickhouse_insert", "select_sql"),
        ("result_sink.py", "databricks_count", "target.directory_url"),
        ("result_sink.py", "databricks_insert", "_quoted(target.directory_url)"),
        ("result_sink.py", "databricks_insert", "select_sql"),
        ("result_sink.py", "duckdb_copy", "_quoted(target.object_url)"),
        ("result_sink.py", "duckdb_copy", "select_sql"),
        ("result_sink.py", "duckdb_copy_file", "DUCKDB_FILE_FORMATS[output_format]"),
        ("result_sink.py", "duckdb_copy_file", "_quoted(path)"),
        ("result_sink.py", "duckdb_copy_file", "select_sql"),
        ("result_sink.py", "duckdb_secret", "', '.join(parts)"),
        ("result_sink.py", "duckdb_secret", "DUCKDB_SECRET"),
        ("result_sink.py", "duckdb_secret", "_quoted(config.access_key)"),
        ("result_sink.py", "duckdb_secret", "_quoted(config.region)"),
        ("result_sink.py", "duckdb_secret", "_quoted(config.secret_key)"),
        ("result_sink.py", "duckdb_secret", "_quoted(endpoint.netloc)"),
        ("result_sink.py", "duckdb_secret", "_quoted(f's3://{config.bucket}/results/')"),
        ("result_sink.py", "snowflake_copy", "_identifier(integration)"),
        ("result_sink.py", "snowflake_copy", "_quoted(target.directory_url)"),
        ("result_sink.py", "snowflake_copy", "select_sql"),
        # session_rows.py
        ("session_rows.py", "_do", "_MAT_STORE_ALIAS"),
        ("session_rows.py", "_do", "_ident(name)"),
        ("session_rows.py", "_do", "_ident(schema)"),
        ("session_rows.py", "_do", "names"),
        ("session_rows.py", "_do", "same"),
        ("session_rows.py", "_do", "target"),
        ("session_rows.py", "add_column", "_column_type(kind, _duckdb_dialect())"),
        ("session_rows.py", "add_column", "_ident(column)"),
        ("session_rows.py", "add_column", "_ident(schema)"),
        ("session_rows.py", "add_column", "_ident(table)"),
        ("session_rows.py", "add_column", "kind_sql"),
        ("session_rows.py", "add_column", "self._table(schema, table)"),
        ("session_rows.py", "drop_lookup_index", "_ident(schema)"),
        ("session_rows.py", "drop_lookup_index", "name"),
        ("session_rows.py", "index", "_ident(_index_name(table))"),
        ("session_rows.py", "index", "_ident(schema)"),
        ("session_rows.py", "index", "_ident(table)"),
        ("session_rows.py", "index", "cols"),
        ("session_rows.py", "index", "self._table(schema, table)"),
        ("session_rows.py", "lookup_index", "_ident(_lookup_index_name(table, columns))"),
        ("session_rows.py", "lookup_index", "_ident(schema)"),
        ("session_rows.py", "lookup_index", "_ident(table)"),
        ("session_rows.py", "lookup_index", "cols"),
        ("session_rows.py", "lookup_index", "self._table(schema, table)"),
        ("session_rows.py", "retype", "_column_type(kind, _duckdb_dialect())"),
        ("session_rows.py", "retype", "_ident(column)"),
        ("session_rows.py", "retype", "_ident(schema)"),
        ("session_rows.py", "retype", "_ident(table)"),
        ("session_rows.py", "retype", "kind_sql"),
        ("session_rows.py", "retype", "self._table(schema, table)"),
        # singlestore_pipeline.py
        ("singlestore_pipeline.py", "batches_since_query", "_literal(schema)"),
        ("singlestore_pipeline.py", "batches_since_query", "since"),
        ("singlestore_pipeline.py", "file_pipeline_ddl", "_literal(store_path(origin.location))"),
        ("singlestore_pipeline.py", "file_pipeline_ddl", "_qualified(schema, link)"),
        ("singlestore_pipeline.py", "file_pipeline_ddl", "_qualified(schema, pipeline)"),
        ("singlestore_pipeline.py", "file_pipeline_ddl", "body"),
        ("singlestore_pipeline.py", "file_pipeline_ddl", "source"),
        ("singlestore_pipeline.py", "iceberg_pipeline_ddl", "_json_literal(config)"),
        ("singlestore_pipeline.py", "iceberg_pipeline_ddl", "_json_literal(credentials)"),
        ("singlestore_pipeline.py", "iceberg_pipeline_ddl", "_literal(hints['iceberg_table_id'])"),
        ("singlestore_pipeline.py", "iceberg_pipeline_ddl", "_qualified(schema, into_table)"),
        ("singlestore_pipeline.py", "iceberg_pipeline_ddl", "_qualified(schema, pipeline)"),
        ("singlestore_pipeline.py", "iceberg_pipeline_ddl", "fields"),
        ("singlestore_pipeline.py", "kafka_pipeline_ddl", "', '.join(reads)"),
        ("singlestore_pipeline.py", "kafka_pipeline_ddl", "', '.join(sets)"),
        (
            "singlestore_pipeline.py",
            "kafka_pipeline_ddl",
            "_literal(f'{bootstrap}/{origin.location}')",
        ),
        ("singlestore_pipeline.py", "kafka_pipeline_ddl", "_literal(schema_registry)"),
        ("singlestore_pipeline.py", "kafka_pipeline_ddl", "_qualified(schema, pipeline)"),
        ("singlestore_pipeline.py", "kafka_pipeline_ddl", "_qualified(schema, procedure)"),
        ("singlestore_pipeline.py", "kafka_pipeline_ddl", "batch"),
        ("singlestore_pipeline.py", "kafka_pipeline_ddl", "cols"),
        ("singlestore_pipeline.py", "kafka_pipeline_ddl", "fields"),
        ("singlestore_pipeline.py", "kafka_pipeline_ddl", "key_match"),
        ("singlestore_pipeline.py", "kafka_pipeline_ddl", "source"),
        ("singlestore_pipeline.py", "kafka_pipeline_ddl", "target_table"),
        ("singlestore_pipeline.py", "kafka_pipeline_ddl", "upsert"),
        (
            "singlestore_pipeline.py",
            "kafka_pipelines_query",
            "_literal(KAFKA_PIPELINE_PREFIX.replace('_', chr(92) + '_') + '%')",
        ),
        ("singlestore_pipeline.py", "kafka_pipelines_query", "_literal(schema)"),
        ("singlestore_pipeline.py", "object_store_link_ddl", "_json_literal(credentials)"),
        ("singlestore_pipeline.py", "object_store_link_ddl", "_qualified(schema, link)"),
        ("singlestore_pipeline.py", "object_store_link_ddl", "store"),
        ("singlestore_pipeline.py", "offsets_from", "_json_literal(offsets)"),
        ("singlestore_pipeline.py", "offsets_from", "_qualified(schema, pipeline)"),
        ("singlestore_pipeline.py", "s3_link_ddl", "_json_literal(config)"),
        ("singlestore_pipeline.py", "s3_link_ddl", "_json_literal(credentials)"),
        ("singlestore_pipeline.py", "s3_link_ddl", "_qualified(schema, link)"),
        ("singlestore_pipeline.py", "teardown", "_qualified(schema, link)"),
        ("singlestore_pipeline.py", "teardown", "_qualified(schema, pipeline)"),
        ("singlestore_pipeline.py", "teardown", "_qualified(schema, procedure)"),
        # snowflake_connectors.py
        ("snowflake_connectors.py", "stage_and_external_table_ddl", "bare_endpoint"),
        ("snowflake_connectors.py", "stage_and_external_table_ddl", "cred_clause"),
        ("snowflake_connectors.py", "stage_and_external_table_ddl", "database"),
        ("snowflake_connectors.py", "stage_and_external_table_ddl", "endpoint_clause"),
        ("snowflake_connectors.py", "stage_and_external_table_ddl", "fmt"),
        ("snowflake_connectors.py", "stage_and_external_table_ddl", "fq"),
        ("snowflake_connectors.py", "stage_and_external_table_ddl", "pattern_clause"),
        ("snowflake_connectors.py", "stage_and_external_table_ddl", "schema"),
        ("snowflake_connectors.py", "stage_and_external_table_ddl", "stage_fq"),
        ("snowflake_connectors.py", "stage_and_external_table_ddl", "url"),
        # snowflake_runtime.py
        ("snowflake_runtime.py", "detach_source", "qualified(self._phys_parts(source))"),
        ("snowflake_runtime.py", "resolve_snowflake_iceberg_metadata_location", "table"),
        # snowflake_store.py
        ("snowflake_store.py", "col_tag", "_literal(value)"),
        ("snowflake_store.py", "col_tag", "column"),
        ("snowflake_store.py", "col_tag", "qualified(view)"),
        ("snowflake_store.py", "col_tag", "tag_fq(tag)"),
        ("snowflake_store.py", "comment_statements", "kind"),
        ("snowflake_store.py", "comment_statements", "qualified(parts)"),
        ("snowflake_store.py", "constraint_statements", "cols"),
        ("snowflake_store.py", "constraint_statements", "edge.name"),
        ("snowflake_store.py", "constraint_statements", "fq"),
        (
            "snowflake_store.py",
            "constraint_statements",
            "identifier(OWNED_PK_PREFIX + target.replica[2])",
        ),
        ("snowflake_store.py", "constraint_statements", "name"),
        ("snowflake_store.py", "constraint_statements", "qualified(holder)"),
        ("snowflake_store.py", "constraint_statements", "qualified(referenced)"),
        ("snowflake_store.py", "constraint_statements", "ref_cols"),
        ("snowflake_store.py", "create_ddl", "cols"),
        ("snowflake_store.py", "create_ddl", "pk"),
        ("snowflake_store.py", "create_ddl", "qualified(parts)"),
        ("snowflake_store.py", "ensure_namespace", "database"),
        ("snowflake_store.py", "ensure_namespace", "schema"),
        ("snowflake_store.py", "existing_columns", "_literal(schema)"),
        ("snowflake_store.py", "existing_columns", "_literal(table)"),
        ("snowflake_store.py", "existing_columns", "database"),
        ("snowflake_store.py", "existing_comments", "_literal(schema)"),
        ("snowflake_store.py", "existing_comments", "_literal(table)"),
        ("snowflake_store.py", "existing_comments", "database"),
        ("snowflake_store.py", "existing_tags", "TAGS_SCHEMA"),
        ("snowflake_store.py", "existing_tags", "_literal(qualified(parts))"),
        ("snowflake_store.py", "existing_tags", "_literal(tags_database)"),
        ("snowflake_store.py", "existing_tags", "tags_database"),
        ("snowflake_store.py", "expose_view", "qualified(replica)"),
        ("snowflake_store.py", "expose_view", "qualified(view)"),
        ("snowflake_store.py", "key_tag_statements", "TAGS_SCHEMA"),
        ("snowflake_store.py", "key_tag_statements", "column"),
        ("snowflake_store.py", "key_tag_statements", "qualified(view)"),
        ("snowflake_store.py", "key_tag_statements", "tag_fq(FK_TAG)"),
        ("snowflake_store.py", "key_tag_statements", "tag_fq(PK_TAG)"),
        ("snowflake_store.py", "key_tag_statements", "tag_fq(tag)"),
        ("snowflake_store.py", "key_tag_statements", "tags_database"),
        ("snowflake_store.py", "land_snowflake_native", "', '.join([row_ph] * len(chunk))"),
        ("snowflake_store.py", "land_snowflake_native", "collist"),
        ("snowflake_store.py", "land_snowflake_native", "fq"),
        ("snowflake_store.py", "land_snowflake_native", "projection"),
        ("snowflake_store.py", "land_snowflake_native", "verb"),
        (
            "snowflake_store.py",
            "merge_snowflake_native",
            "f'MERGE INTO {qualified(parts)} t USING (SELECT {placeholders}) s ({src_cols}) ON {on} ' + (f'WHEN MATCHED THEN UPDATE SET {update} ' if update else '')",
        ),
        (
            "snowflake_store.py",
            "merge_snowflake_native",
            "f'WHEN MATCHED THEN UPDATE SET {update} ' if update else ''",
        ),
        ("snowflake_store.py", "merge_snowflake_native", "insert_vals"),
        ("snowflake_store.py", "merge_snowflake_native", "on"),
        ("snowflake_store.py", "merge_snowflake_native", "placeholders"),
        ("snowflake_store.py", "merge_snowflake_native", "qualified(parts)"),
        ("snowflake_store.py", "merge_snowflake_native", "src_cols"),
        ("snowflake_store.py", "merge_snowflake_native", "update"),
        ("snowflake_store.py", "model_tag_statements", "TAGS_SCHEMA"),
        ("snowflake_store.py", "model_tag_statements", "kind"),
        ("snowflake_store.py", "model_tag_statements", "qualified(parts)"),
        ("snowflake_store.py", "model_tag_statements", "tag_fq(tag)"),
        ("snowflake_store.py", "model_tag_statements", "tags_database"),
        ("snowflake_store.py", "reconcile_snowflake_native", "qualified(parts)"),
        ("snowflake_store.py", "view_reads", "_literal(schema)"),
        ("snowflake_store.py", "view_reads", "_literal(table)"),
        ("snowflake_store.py", "view_reads", "database"),
        # sqlalchemy_runtime.py
        ("sqlalchemy_runtime.py", "_ensure_schema", "ident"),
        # store_connection.py
        ("store_connection.py", "_bulk_insert_rows", "collist"),
        ("store_connection.py", "_bulk_insert_rows", "qualified"),
        ("store_connection.py", "_bulk_insert_rows", "view_name"),
        ("store_connection.py", "_unchanged_keys", "c"),
        ("store_connection.py", "_unchanged_keys", "joined"),
        ("store_connection.py", "_unchanged_keys", "keys"),
        ("store_connection.py", "_unchanged_keys", "qualified"),
        ("store_connection.py", "_unchanged_keys", "same"),
        ("store_connection.py", "_unchanged_keys", "staged"),
        ("store_connection.py", "apply_cdc_duckdb_native", "keys_view"),
        ("store_connection.py", "apply_cdc_duckdb_native", "pk_list"),
        ("store_connection.py", "apply_cdc_duckdb_native", "qualified"),
        ("store_connection.py", "delete_where_duckdb_native", "qualified"),
        ("store_connection.py", "delete_where_duckdb_native", "where"),
        (
            "store_connection.py",
            "ensure_row_cache_table_duckdb_native",
            "_qualified(catalog, schema, table)",
        ),
        (
            "store_connection.py",
            "filter_fetched_at_duckdb_native",
            "_qualified(catalog, schema, record)",
        ),
        ("store_connection.py", "land_duckdb_native", "qualified"),
        ("store_connection.py", "persist_duckdb_native", "pk_list"),
        ("store_connection.py", "persist_duckdb_native", "placeholders"),
        ("store_connection.py", "persist_duckdb_native", "qualified"),
        ("store_connection.py", "read_row_cache_duckdb_native", "keys_view"),
        ("store_connection.py", "read_row_cache_duckdb_native", "pk_list"),
        ("store_connection.py", "read_row_cache_duckdb_native", "qualified"),
        ("store_connection.py", "reconcile_duckdb_native", "_qualified(catalog, schema, table)"),
        ("store_connection.py", "replace_filtered_rows_duckdb_native", "collist"),
        ("store_connection.py", "replace_filtered_rows_duckdb_native", "pk_list"),
        ("store_connection.py", "replace_filtered_rows_duckdb_native", "qualified"),
        ("store_connection.py", "replace_filtered_rows_duckdb_native", "record"),
        ("store_connection.py", "replace_filtered_rows_duckdb_native", "view"),
        ("store_connection.py", "replace_filtered_rows_duckdb_native", "where_sql"),
        ("store_connection.py", "tombstone_row_cache_duckdb_native", "qualified"),
        ("store_connection.py", "tombstone_row_cache_duckdb_native", "where"),
        ("store_connection.py", "upsert_arrow_duckdb_native", "collist"),
        ("store_connection.py", "upsert_arrow_duckdb_native", "pk_list"),
        ("store_connection.py", "upsert_arrow_duckdb_native", "qualified"),
        ("store_connection.py", "upsert_arrow_duckdb_native", "view"),
        # store_scope.py
        ("store_scope.py", "drop_env_store", "export"),
        ("store_scope.py", "drop_env_store", "name"),
        ("store_scope.py", "drop_env_store", "schema"),
        ("store_scope.py", "drop_synthetic_schema", "schema"),
        ("store_scope.py", "drop_synthetic_table", "quoted"),
        ("store_scope.py", "drop_synthetic_table", "schema"),
        ("store_scope.py", "drop_temp_schema", "catalog"),
        ("store_scope.py", "drop_temp_schema", "schema"),
        ("store_scope.py", "drop_temp_table", "catalog"),
        ("store_scope.py", "drop_temp_table", "quoted"),
        ("store_scope.py", "drop_temp_table", "schema"),
        # store_writer.py
        ("store_writer.py", "reconcile_table", "quoted_schema"),
        ("store_writer.py", "reconcile_table", "quoted_table"),
        # trino_connectors.py
        ("trino_connectors.py", "_lake_post_create", "catalog_name"),
        ("trino_connectors.py", "_lake_post_create", "location"),
        ("trino_connectors.py", "_lake_post_create", "table"),
    }
)

#: The modules whose statement text has been read in full: none of them may hold a PENDING entry.
READ_IN_FULL = frozenset(
    {
        "connector_duckdb.py",
        "duckdb_runtime.py",
        "pg_runtime.py",
        "pg_motherduck.py",
        "clickhouse_connectors.py",
        "clickhouse_runtime.py",
        "custom_connectors.py",
        "duckdb_quoting.py",
        "pg_quoting.py",
        "clickhouse_quoting.py",
    }
)


def _all_interpolations() -> set[tuple[str, str, str]]:
    found: set[tuple[str, str, str]] = set()
    for module in sorted(FEDERATION.glob("*.py")):
        found |= _unquoted_interpolations(module.name, module.read_text())
    return found


def test_federation_sql_is_built_through_the_quoting_helpers():
    """A new interpolation in any federation module is written with a quoter; one that is not a
    registered value is added to ``REVIEWED`` with what it is."""
    found = _all_interpolations()
    unreviewed = sorted(found - REVIEWED - PENDING)
    assert not unreviewed, "statement text built from an unquoted value:\n" + "\n".join(
        f"  {m}:{f}: {{{e}}}" for m, f, e in unreviewed
    )
    stale = sorted((REVIEWED | PENDING) - found)
    assert not stale, f"REVIEWED or PENDING names interpolations that are gone: {stale}"


def test_the_modules_read_in_full_hold_nothing_pending():
    assert not REVIEWED & PENDING
    assert not [entry for entry in PENDING if entry[0] in READ_IN_FULL]


def test_the_engine_modules_keep_no_quoter_of_their_own():
    for module in READ_IN_FULL - {"duckdb_quoting.py", "pg_quoting.py", "clickhouse_quoting.py"}:
        tree = ast.parse((FEDERATION / module).read_text())
        own = [
            n.name
            for n in ast.walk(tree)
            if isinstance(n, ast.FunctionDef) and n.name in ("_quoted", "_sql_text", "_lit")
        ]
        assert not own, f"{module} defines {own}: use the engine's quoting module"
