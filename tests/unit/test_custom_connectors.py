# Copyright (c) 2026 Kenneth Stott
# Canary: 5d8b3f61-7a24-4e09-9c31-2f6e0b8a41c7
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1177: config-driven custom source connectors — the descriptor → engine-DDL projection.

Proves the generic PG SQL/MED connector and the two DuckDB mechanisms (ATTACH / SCAN) emit the exact
detail shapes the runtimes consume, from the three conformance descriptors (mongo_fdw / ducklake /
excel). Availability probes need a live engine and are exercised by the reachability integration
suite; here we pin the pure descriptor logic."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from provisa.federation.clickhouse_quoting import ch_lit
from provisa.federation.connector_base import Mechanism
from provisa.federation.custom_connectors import (
    GenericClickHouseDatabaseConnector,
    GenericClickHouseScanConnector,
    GenericClickHouseTableConnector,
    GenericDuckDbAttachConnector,
    GenericDuckDbScanConnector,
    GenericPgFdwConnector,
    load_custom_connectors,
)
from provisa.federation.duckdb_quoting import lit
from provisa.federation.pg_quoting import pg_ident

_FIXTURE = "tests/fixtures/custom_connectors.yaml"


def _src(**kw) -> Any:  # a Source's attributes, as the connectors read them
    base = dict(
        id="reviews",
        catalog="reviews",
        host="mongodb",
        port=27017,
        database="test",
        username="admin",
        password="secret",
        path="/data/x",
        schema_name="public",
        table_name="customer_reviews",
        federation_hints={},
    )
    base.update(kw)
    return SimpleNamespace(**base)


# --------------------------------------------------------------------------- #
# PG SQL/MED generic
# --------------------------------------------------------------------------- #
def test_pg_fdw_no_import_emits_server_ddl_and_table_options():
    d = {
        "source_type": "mongodb",
        "kind": "pg_fdw",
        "extension": "mongo_fdw",
        "mechanism": "attach_r",
        "server_options": {"address": "{host}", "port": "{port}"},
        "user_mapping": {"username": "{username}", "password": "{password}"},
        "supports_import": False,
        "table_options": {"database": "{database}", "collection": "{table_name}"},
    }
    det = GenericPgFdwConnector(d).details(_src())
    assert "attach_ddl" not in det  # explicit-foreign-table path
    assert det["server"] == "fdw_reviews"
    assert det["server_ddl"] == [
        'CREATE EXTENSION IF NOT EXISTS "mongo_fdw"',
        'CREATE SERVER IF NOT EXISTS "fdw_reviews" FOREIGN DATA WRAPPER "mongo_fdw" '
        """OPTIONS ("address" 'mongodb', "port" '27017')""",
        'CREATE USER MAPPING IF NOT EXISTS FOR CURRENT_USER SERVER "fdw_reviews" '
        """OPTIONS ("username" 'admin', "password" 'secret')""",
    ]
    assert det["table_options"] == (
        """OPTIONS ("database" 'test', "collection" 'customer_reviews')"""
    )


def test_pg_fdw_bare_user_mapping_when_empty():
    # `user_mapping: {}` ⇒ the mapping must EXIST but carry NO options — the SQL/MED form a no-auth FDW
    # (mongo_fdw against an unauthenticated MongoDB) needs; a `username ''` would make the driver auth.
    d = {
        "source_type": "mongodb",
        "kind": "pg_fdw",
        "extension": "mongo_fdw",
        "mechanism": "attach_r",
        "server_options": {"address": "{host}", "port": "{port}"},
        "user_mapping": {},
        "supports_import": False,
        "table_options": {"database": "{database}", "collection": "{table_name}"},
    }
    det = GenericPgFdwConnector(d).details(_src())
    assert det["server_ddl"][-1] == (
        'CREATE USER MAPPING IF NOT EXISTS FOR CURRENT_USER SERVER "fdw_reviews"'
    )  # no OPTIONS clause


def test_pg_fdw_import_path_emits_attach_ddl_and_local_schema():
    d = {
        "source_type": "widgets",
        "kind": "pg_fdw",
        "extension": "widget_fdw",
        "server_options": {"host": "{host}", "port": "{port}"},
        "supports_import": True,
        "remote_schema": "public",
    }
    det = GenericPgFdwConnector(d).details(_src(id="w", catalog="w"))
    assert det["local_schema"] == "fdw_w"
    assert det["attach_ddl"][0] == 'CREATE EXTENSION IF NOT EXISTS "widget_fdw"'
    assert (
        det["attach_ddl"][-1] == 'IMPORT FOREIGN SCHEMA "public" FROM SERVER "fdw_w" INTO "fdw_w"'
    )
    # no user_mapping declared → no CREATE USER MAPPING statement
    assert not any("USER MAPPING" in s for s in det["attach_ddl"])


def test_pg_fdw_capability_write_follows_mechanism():
    rw = GenericPgFdwConnector({"source_type": "x", "extension": "e", "mechanism": "attach_rw"})
    ro = GenericPgFdwConnector({"source_type": "y", "extension": "e", "mechanism": "attach_r"})
    assert rw.capability().write is True
    assert ro.capability().write is False


# --------------------------------------------------------------------------- #
# DuckDB ATTACH generic
# --------------------------------------------------------------------------- #
def test_duckdb_attach_ducklake_template():
    d = {
        "source_type": "ducklake",
        "kind": "duckdb_attach",
        "extension": "ducklake",
        "install_from_community": False,
        "probe_symbol": "ducklake_snapshots",
        "mechanism": "attach_rw",
        "attach_template": "ATTACH 'ducklake:' || {path} AS {alias:ident} (DATA_PATH {data_path})",
        "remote_schema": "main",
    }
    c = GenericDuckDbAttachConnector(d)
    det = c.details(_src(id="lake", path="/cat.ducklake", federation_hints={"data_path": "/files"}))
    assert det["attach"] == (
        "ATTACH 'ducklake:' || '/cat.ducklake' AS \"_src_lake\" (DATA_PATH '/files')"
    )
    assert det["raw_alias"] == "_src_lake"
    assert det["remote_schema"] == "main"
    assert c.extension == "ducklake" and c.probe_symbol == "ducklake_snapshots"
    assert c.install_from_community is False


# --------------------------------------------------------------------------- #
# DuckDB SCAN generic
# --------------------------------------------------------------------------- #
def test_duckdb_scan_excel_view_ddl():
    d = {
        "source_type": "xlsx",
        "kind": "duckdb_scan",
        "extension": "excel",
        "install_from_community": False,
        "probe_symbol": "read_xlsx",
        "scan_template": "read_xlsx({path})",
    }
    c = GenericDuckDbScanConnector(d)
    det = c.details(_src(id="sales", path="/sales.xlsx"))
    assert det["view_ddl"] == "CREATE VIEW \"sales\" AS SELECT * FROM read_xlsx('/sales.xlsx')"
    assert c.mechanism is Mechanism.SCAN


# --------------------------------------------------------------------------- #
# ClickHouse generic — DATABASE / TABLE / SCAN  (REQ-1178)
# --------------------------------------------------------------------------- #
def test_clickhouse_database_emits_create_database_and_local_schema():
    d = {
        "source_type": "redis",
        "kind": "clickhouse_database",
        "ch_engine": "Redis",
        "engine_template": "Redis(concat({host}, ':', {port}), {db_index:int}, {password})",
    }
    c = GenericClickHouseDatabaseConnector(d)
    det = c.details(
        _src(
            id="cache",
            catalog="cache",
            host="redis",
            port=6379,
            password="pw",
            federation_hints={"db_index": "0"},
        )
    )
    assert det["local_schema"] == "ch_cache"
    assert det["attach_ddl"] == [
        'CREATE DATABASE IF NOT EXISTS "ch_cache" '
        "ENGINE = Redis(concat('redis', ':', '6379'), 0, 'pw')"
    ]
    assert c.mechanism is Mechanism.ATTACH_RW
    assert c.capability().write is True


def test_clickhouse_table_emits_engine_clause_and_requires_columns():
    d = {
        "source_type": "jdbc",
        "kind": "clickhouse_table",
        "ch_engine": "JDBC",
        "engine_template": "JDBC({jdbc_url}, {database}, {table})",
    }
    det = GenericClickHouseTableConnector(d).details(
        _src(id="erp", database="prod", federation_hints={"jdbc_url": "jdbc:oracle:thin:@h:1521"})
    )
    # {table} stays a placeholder the runtime binds from the registry; columns supplied there.
    assert det == {
        "engine_clause": "JDBC('jdbc:oracle:thin:@h:1521', 'prod', '{table}')",
        "requires_columns": True,
    }


def test_clickhouse_scan_emits_inferred_engine_clause():
    d = {
        "source_type": "hdfs",
        "kind": "clickhouse_scan",
        "ch_engine": "HDFS",
        "engine_template": "HDFS({path}, {format})",
    }
    c = GenericClickHouseScanConnector(d)
    det = c.details(
        _src(id="logs", path="hdfs://nn:8020/logs/*", federation_hints={"format": "JSONEachRow"})
    )
    assert det == {
        "engine_clause": "HDFS('hdfs://nn:8020/logs/*', 'JSONEachRow')",
        "infer": True,
        "validate": True,
    }
    assert c.mechanism is Mechanism.SCAN


# --------------------------------------------------------------------------- #
# Loader
# --------------------------------------------------------------------------- #
def test_loader_filters_by_engine(monkeypatch):
    monkeypatch.setenv("PROVISA_CUSTOM_CONNECTORS", _FIXTURE)
    pg = load_custom_connectors("postgres")
    duck = load_custom_connectors("duckdb")
    ch = load_custom_connectors("clickhouse")
    assert [c.source_type for c in pg] == ["mongodb"]
    assert isinstance(pg[0], GenericPgFdwConnector)
    assert sorted(c.source_type for c in duck) == ["ducklake", "xlsx"]
    kinds = {c.source_type: type(c).__name__ for c in duck}
    assert kinds["ducklake"] == "GenericDuckDbAttachConnector"
    assert kinds["xlsx"] == "GenericDuckDbScanConnector"
    ch_kinds = {c.source_type: type(c).__name__ for c in ch}
    assert ch_kinds == {
        "redis": "GenericClickHouseDatabaseConnector",
        "jdbc": "GenericClickHouseTableConnector",
        "hdfs": "GenericClickHouseScanConnector",
    }


def test_loader_absent_config_is_empty(monkeypatch):
    monkeypatch.setenv("PROVISA_CUSTOM_CONNECTORS", "tests/fixtures/does_not_exist.yaml")
    assert load_custom_connectors("postgres") == []


def test_loader_unknown_kind_fails_loud(tmp_path, monkeypatch):
    cfg = tmp_path / "bad.yaml"
    cfg.write_text("connectors:\n  - engine: postgres\n    source_type: z\n    kind: bogus\n")
    monkeypatch.setenv("PROVISA_CUSTOM_CONNECTORS", str(cfg))
    with pytest.raises(ValueError, match="unknown kind 'bogus'"):
        load_custom_connectors("postgres")


# --------------------------------------------------------------------------- #
# SEC-C6b: hostile values cannot leave the literal or name they are written into
# --------------------------------------------------------------------------- #
# Each holds a quote of each kind, a statement separator, a backslash (ClickHouse reads it as an
# escape), a newline, and a trailing backslash that would swallow a closing quote.
_HOSTILE = [
    "x'); DROP TABLE t; --",
    'x"; DROP TABLE t; --',
    "a\\'; DROP TABLE t; --",
    "line1\nline2'; DROP TABLE t; --",
    "trailing\\",
    "`back`tick'",
]


def _tokens(dialect: str, sql: str):
    from sqlglot.dialects import ClickHouse, DuckDB, Postgres

    cls = {"postgres": Postgres, "duckdb": DuckDB, "clickhouse": ClickHouse}[dialect]
    return cls().tokenize(sql)


def _assert_contained(dialect: str, sql: str, value: str, kind: str) -> None:
    """``sql`` is one statement and ``value`` comes back whole as one STRING or IDENTIFIER token,
    read by the engine's own grammar (sqlglot's tokenizer for that dialect)."""
    from sqlglot.tokens import TokenType

    toks = _tokens(dialect, sql)
    assert not [t for t in toks if t.token_type == TokenType.SEMICOLON], sql
    want = TokenType.STRING if kind == "lit" else TokenType.IDENTIFIER
    assert [t for t in toks if t.token_type == want and t.text == value], (sql, value)


def _pg_descriptor(**kw):
    d = {
        "source_type": "hostile",
        "kind": "pg_fdw",
        "extension": "mongo_fdw",
        "server_options": {"address": "{host}", "port": "{port}"},
        "user_mapping": {"username": "{username}", "password": "{password}"},
        "supports_import": False,
        "table_options": {"database": "{database}", "collection": "{table_name}"},
    }
    d.update(kw)
    return d


@pytest.mark.parametrize("value", _HOSTILE)
def test_pg_option_values_are_literals(value):
    src = _src(host=value, password=value, table_name=value)
    det = GenericPgFdwConnector(_pg_descriptor()).details(src)
    server_ddl, mapping_ddl = det["server_ddl"][1], det["server_ddl"][2]
    _assert_contained("postgres", server_ddl, value, "lit")
    _assert_contained("postgres", mapping_ddl, value, "lit")
    _assert_contained("postgres", det["table_options"], value, "lit")


@pytest.mark.parametrize("value", _HOSTILE)
def test_pg_names_are_identifiers(value):
    d = _pg_descriptor(
        extension=value,
        server_options={value: "{host}"},
        supports_import=True,
        remote_schema="{schema_name}",
    )
    det = GenericPgFdwConnector(d).details(_src(catalog=value, schema_name=value))
    extension, server, mapping, schema, imported = det["attach_ddl"]
    _assert_contained("postgres", extension, value, "ident")
    for stmt in (server, mapping, schema, imported):
        _assert_contained("postgres", stmt, f"fdw_{value}", "ident")
    _assert_contained("postgres", server, value, "ident")  # the extension and the option key
    _assert_contained("postgres", imported, value, "ident")
    assert det["attach_ddl"][-1].startswith(f"IMPORT FOREIGN SCHEMA {pg_ident(value)} ")


@pytest.mark.parametrize("value", _HOSTILE)
def test_duckdb_attach_placeholders_are_quoted(value):
    d = {
        "source_type": "lake",
        "kind": "duckdb_attach",
        "extension": "ducklake",
        "probe_symbol": "ducklake_snapshots",
        "attach_template": "ATTACH 'ducklake:' || {path} AS {alias:ident} (DATA_PATH {data_path})",
    }
    det = GenericDuckDbAttachConnector(d).details(
        _src(id=value, path=value, federation_hints={"data_path": value})
    )
    _assert_contained("duckdb", det["attach"], value, "lit")
    _assert_contained("duckdb", det["attach"], f"_src_{value}", "ident")
    assert det["attach"].count(lit(value)) == 2


@pytest.mark.parametrize("value", _HOSTILE)
def test_duckdb_scan_placeholders_and_view_name_are_quoted(value):
    d = {
        "source_type": "xlsx",
        "kind": "duckdb_scan",
        "extension": "excel",
        "probe_symbol": "read_xlsx",
        "scan_template": "read_xlsx({path}, sheet={sheet})",
    }
    det = GenericDuckDbScanConnector(d).details(
        _src(id=value, path=value, federation_hints={"sheet": value})
    )
    _assert_contained("duckdb", det["view_ddl"], value, "lit")
    _assert_contained("duckdb", det["view_ddl"], value, "ident")


def test_duckdb_scan_view_runs_over_a_file_whose_name_holds_a_quote(tmp_path):
    """The engine itself reads the quoted path: a CSV named with a quote and a semicolon."""
    import duckdb

    csv = tmp_path / "it's; here.csv"
    csv.write_text("a\n1\n")
    d = {
        "source_type": "csvx",
        "kind": "duckdb_scan",
        "extension": "excel",
        "probe_symbol": "read_xlsx",
        "scan_template": "read_csv({path}, header=true)",
    }
    det = GenericDuckDbScanConnector(d).details(_src(id='v"1', path=str(csv)))
    con = duckdb.connect()
    con.execute(det["view_ddl"])
    assert con.execute('SELECT a FROM "v""1"').fetchall() == [(1,)]


@pytest.mark.parametrize("value", _HOSTILE)
def test_clickhouse_engine_placeholders_are_quoted(value):
    d = {
        "source_type": "redis",
        "kind": "clickhouse_database",
        "ch_engine": "Redis",
        "engine_template": "Redis(concat({host}, ':', {port}), {db_index:int}, {password})",
    }
    det = GenericClickHouseDatabaseConnector(d).details(
        _src(catalog=value, host=value, password=value, federation_hints={"db_index": "3"})
    )
    (stmt,) = det["attach_ddl"]
    _assert_contained("clickhouse", stmt, value, "lit")
    _assert_contained("clickhouse", stmt, f"ch_{value}", "ident")
    assert stmt.count(ch_lit(value)) == 2


@pytest.mark.parametrize("value", _HOSTILE)
def test_clickhouse_table_clause_binds_the_table_after_quoting(value):
    from provisa.federation.clickhouse_quoting import bind_table

    d = {
        "source_type": "jdbc",
        "kind": "clickhouse_table",
        "ch_engine": "JDBC",
        "engine_template": "JDBC({jdbc_url}, {database}, {table})",
    }
    det = GenericClickHouseTableConnector(d).details(
        _src(database=value, federation_hints={"jdbc_url": value})
    )
    clause = bind_table(det["engine_clause"], value)
    _assert_contained("clickhouse", clause, value, "lit")
    assert clause == f"JDBC({ch_lit(value)}, {ch_lit(value)}, {ch_lit(value)})"


@pytest.mark.parametrize("value", ["1); DROP TABLE t; --", "0x1", "", "1.5"])
def test_an_int_placeholder_refuses_a_value_that_is_not_an_integer(value):
    d = {
        "source_type": "redis",
        "kind": "clickhouse_database",
        "ch_engine": "Redis",
        "engine_template": "Redis({host}, {db_index:int})",
    }
    with pytest.raises(ValueError):
        GenericClickHouseDatabaseConnector(d).details(_src(federation_hints={"db_index": value}))


@pytest.mark.parametrize(
    ("engine", "kind", "key", "template"),
    [
        ("duckdb", "duckdb_scan", "scan_template", "read_xlsx('{path}')"),
        ("duckdb", "duckdb_attach", "attach_template", 'ATTACH {path} AS "{alias}"'),
        ("clickhouse", "clickhouse_scan", "engine_template", "HDFS('{path}', {format})"),
        ("clickhouse", "clickhouse_scan", "engine_template", "URL('a\\'{path}')"),
        ("clickhouse", "clickhouse_table", "engine_template", "JDBC({jdbc_url}, `{database}`)"),
    ],
)
def test_a_placeholder_inside_quotes_is_refused_when_the_descriptor_is_read(
    engine, kind, key, template
):
    """Old-style templates quoted their own placeholders; a value written as a literal inside such
    quotes could end them, so the descriptor is refused when it is read."""
    from provisa.federation import custom_connectors

    d = {
        "source_type": "s",
        "kind": kind,
        "extension": "e",
        "probe_symbol": "p",
        "ch_engine": "E",
        key: template,
    }
    with pytest.raises(ValueError, match="inside"):
        custom_connectors._KINDS[kind](d)


def test_a_quote_before_a_placeholder_that_is_closed_is_accepted():
    d = {
        "source_type": "s",
        "kind": "clickhouse_scan",
        "ch_engine": "URL",
        "engine_template": "URL(concat('it''s \\\\', {path}), 'CSV')",
    }
    det = GenericClickHouseScanConnector(d).details(_src(path="/p"))
    assert det["engine_clause"] == "URL(concat('it''s \\\\', '/p'), 'CSV')"


@pytest.mark.parametrize("template", ["read_xlsx({path!r})", "read_xlsx({path:raw})"])
def test_a_placeholder_conversion_or_unknown_spec_is_refused(template):
    d = {
        "source_type": "x",
        "kind": "duckdb_scan",
        "extension": "excel",
        "probe_symbol": "read_xlsx",
        "scan_template": template,
    }
    with pytest.raises(ValueError):
        GenericDuckDbScanConnector(d).details(_src())


@pytest.mark.parametrize("value", _HOSTILE)
async def test_the_clickhouse_engine_probe_writes_the_engine_name_as_a_literal(value):
    from provisa.federation.custom_connectors import _probe_clickhouse_engine

    seen: list[str] = []

    async def fetch(sql):
        seen.append(sql)
        return []

    await _probe_clickhouse_engine(fetch, value)
    _assert_contained("clickhouse", seen[0], value, "lit")
