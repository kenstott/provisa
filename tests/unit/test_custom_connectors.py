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

import pytest

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

_FIXTURE = "tests/fixtures/custom_connectors.yaml"


def _src(**kw):
    base = dict(
        id="reviews", host="mongodb", port=27017, database="test", username="admin",
        password="secret", path="/data/x", schema_name="public", table_name="customer_reviews",
        federation_hints={},
    )
    base.update(kw)
    return SimpleNamespace(**base)


# --------------------------------------------------------------------------- #
# PG SQL/MED generic
# --------------------------------------------------------------------------- #
def test_pg_fdw_no_import_emits_server_ddl_and_table_options():
    d = {
        "source_type": "mongodb", "kind": "pg_fdw", "extension": "mongo_fdw", "mechanism": "attach_r",
        "server_options": {"address": "{host}", "port": "{port}"},
        "user_mapping": {"username": "{username}", "password": "{password}"},
        "supports_import": False,
        "table_options": {"database": "{database}", "collection": "{table_name}"},
    }
    det = GenericPgFdwConnector(d).details(_src())
    assert "attach_ddl" not in det  # explicit-foreign-table path
    assert det["server"] == "fdw_reviews"
    assert det["server_ddl"] == [
        "CREATE EXTENSION IF NOT EXISTS mongo_fdw",
        "CREATE SERVER IF NOT EXISTS fdw_reviews FOREIGN DATA WRAPPER mongo_fdw "
        "OPTIONS (address 'mongodb', port '27017')",
        "CREATE USER MAPPING IF NOT EXISTS FOR CURRENT_USER SERVER fdw_reviews "
        "OPTIONS (username 'admin', password 'secret')",
    ]
    assert det["table_options"] == "OPTIONS (database 'test', collection 'customer_reviews')"


def test_pg_fdw_import_path_emits_attach_ddl_and_local_schema():
    d = {
        "source_type": "widgets", "kind": "pg_fdw", "extension": "widget_fdw",
        "server_options": {"host": "{host}", "port": "{port}"},
        "supports_import": True, "remote_schema": "public",
    }
    det = GenericPgFdwConnector(d).details(_src(id="w"))
    assert det["local_schema"] == "fdw_w"
    assert det["attach_ddl"][0] == "CREATE EXTENSION IF NOT EXISTS widget_fdw"
    assert det["attach_ddl"][-1] == "IMPORT FOREIGN SCHEMA public FROM SERVER fdw_w INTO fdw_w"
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
        "source_type": "ducklake", "kind": "duckdb_attach", "extension": "ducklake",
        "install_from_community": False, "probe_symbol": "ducklake_snapshots", "mechanism": "attach_rw",
        "attach_template": "ATTACH 'ducklake:{path}' AS \"{alias}\" (DATA_PATH '{data_path}')",
        "remote_schema": "main",
    }
    c = GenericDuckDbAttachConnector(d)
    det = c.details(_src(id="lake", path="/cat.ducklake", federation_hints={"data_path": "/files"}))
    assert det["attach"] == "ATTACH 'ducklake:/cat.ducklake' AS \"_src_lake\" (DATA_PATH '/files')"
    assert det["raw_alias"] == "_src_lake"
    assert det["remote_schema"] == "main"
    assert c.extension == "ducklake" and c.probe_symbol == "ducklake_snapshots"
    assert c.install_from_community is False


# --------------------------------------------------------------------------- #
# DuckDB SCAN generic
# --------------------------------------------------------------------------- #
def test_duckdb_scan_excel_view_ddl():
    d = {
        "source_type": "xlsx", "kind": "duckdb_scan", "extension": "excel",
        "install_from_community": False, "probe_symbol": "read_xlsx",
        "scan_template": "read_xlsx('{path}')",
    }
    c = GenericDuckDbScanConnector(d)
    det = c.details(_src(id="sales", path="/sales.xlsx"))
    assert det["view_ddl"] == "CREATE VIEW sales AS SELECT * FROM read_xlsx('/sales.xlsx')"
    assert c.mechanism is Mechanism.SCAN


# --------------------------------------------------------------------------- #
# ClickHouse generic — DATABASE / TABLE / SCAN  (REQ-1178)
# --------------------------------------------------------------------------- #
def test_clickhouse_database_emits_create_database_and_local_schema():
    d = {
        "source_type": "redis", "kind": "clickhouse_database", "ch_engine": "Redis",
        "engine_template": "Redis('{host}:{port}', {db_index}, '{password}')",
    }
    c = GenericClickHouseDatabaseConnector(d)
    det = c.details(_src(id="cache", host="redis", port=6379, password="pw",
                         federation_hints={"db_index": "0"}))
    assert det["local_schema"] == "ch_cache"
    assert det["attach_ddl"] == [
        'CREATE DATABASE IF NOT EXISTS "ch_cache" ENGINE = Redis(\'redis:6379\', 0, \'pw\')'
    ]
    assert c.mechanism is Mechanism.ATTACH_RW
    assert c.capability().write is True


def test_clickhouse_table_emits_engine_clause_and_requires_columns():
    d = {
        "source_type": "jdbc", "kind": "clickhouse_table", "ch_engine": "JDBC",
        "engine_template": "JDBC('{jdbc_url}', '{database}', '{table}')",
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
        "source_type": "hdfs", "kind": "clickhouse_scan", "ch_engine": "HDFS",
        "engine_template": "HDFS('{path}', '{format}')",
    }
    c = GenericClickHouseScanConnector(d)
    det = c.details(_src(id="logs", path="hdfs://nn:8020/logs/*", federation_hints={"format": "JSONEachRow"}))
    assert det == {
        "engine_clause": "HDFS('hdfs://nn:8020/logs/*', 'JSONEachRow')",
        "infer": True, "validate": True,
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
