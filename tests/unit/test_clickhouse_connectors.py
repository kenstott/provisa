# Copyright (c) 2026 Kenneth Stott
# Canary: 1a9c4e07-3b52-4d68-9f14-6c2b0e5a7d31
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-909: ClickHouse federation connectors — identity, capability, and the attach DDL /
engine-clause each source type produces. Pure logic; no live ClickHouse."""

from __future__ import annotations

import pytest

from provisa.core.models import Source, SourceType
from provisa.federation.connector import (
    ClickHouseCsvConnector,
    ClickHouseMongoConnector,
    ClickHouseMysqlConnector,
    ClickHouseParquetConnector,
    ClickHousePostgresConnector,
    Mechanism,
)
from provisa.federation.engine import build_clickhouse_engine
from provisa.federation.runtime import EngineCapability, EngineRuntime


def _src(sid: str, type_: SourceType, **kw) -> Source:
    fields = {"host": "h", "port": 9000, "database": "db", "username": "u", "password": "p", **kw}
    return Source(id=sid, type=type_, **fields)


# ---- identity ----------------------------------------------------------------


def test_all_clickhouse_connectors_are_attach_on_the_clickhouse_engine():
    for c in (
        ClickHousePostgresConnector(),
        ClickHouseMysqlConnector(),
        ClickHouseMongoConnector(),
        ClickHouseCsvConnector(),
        ClickHouseParquetConnector(),
    ):
        assert c.engine == "clickhouse"
        assert c.mechanism is Mechanism.ATTACH_RW


# ---- relational DATABASE engine (auto-import) --------------------------------


def test_postgres_database_engine_ddl_and_local_schema():
    d = ClickHousePostgresConnector().details(
        _src("shop", SourceType.postgresql, host="pg", port=5432, database="inventory")
    )
    ddl = d["attach_ddl"][0]
    assert "ENGINE = PostgreSQL('pg:5432', 'inventory', 'u', 'p', 'public')" in ddl
    assert 'CREATE DATABASE IF NOT EXISTS "ch_shop"' in ddl
    assert d["local_schema"] == "ch_shop"


def test_postgres_remote_schema_override_rides_on_federation_hints():
    d = ClickHousePostgresConnector().details(
        _src("shop", SourceType.postgresql, federation_hints={"schema": "sales"})
    )
    assert "'sales')" in d["attach_ddl"][0]


def test_mysql_database_engine_ddl():
    d = ClickHouseMysqlConnector().details(
        _src("inv", SourceType.mysql, host="my", port=3306, database="stock")
    )
    ddl = d["attach_ddl"][0]
    assert "ENGINE = MySQL('my:3306', 'stock', 'u', 'p')" in ddl
    assert d["local_schema"] == "ch_inv"


# ---- per-table TABLE engine (mongo needs columns; files infer) ---------------


def test_mongo_engine_clause_carries_table_placeholder_and_requires_columns():
    d = ClickHouseMongoConnector().details(
        _src("docs", SourceType.mongodb, host="mongo", port=27017, database="app")
    )
    assert d["requires_columns"] is True
    assert d["engine_clause"] == "MongoDB('mongo:27017', 'app', '{table}', 'u', 'p')"


def test_csv_local_path_uses_file_engine():
    d = ClickHouseCsvConnector().details(_src("c", SourceType.csv, path="/data/x.csv"))
    assert d == {
        "engine_clause": "File('CSVWithNames', '/data/x.csv')",
        "infer": True,
        "validate": True,  # external attach is probed at attach time (REQ-987 parity)
    }


def test_parquet_s3_path_uses_s3_engine_public_bucket():
    d = ClickHouseParquetConnector().details(
        _src("p", SourceType.parquet, path="s3://bucket/x.parquet")
    )
    assert d["engine_clause"] == "S3('s3://bucket/x.parquet', 'Parquet')"


def test_parquet_s3_private_bucket_binds_credentials_from_hints():
    d = ClickHouseParquetConnector().details(
        _src(
            "p",
            SourceType.parquet,
            path="s3://bucket/x.parquet",
            federation_hints={"aws_key": "AK", "aws_secret": "SK"},
        )
    )
    assert d["engine_clause"] == "S3('s3://bucket/x.parquet', 'AK', 'SK', 'Parquet')"


def test_parquet_http_path_uses_url_engine():
    d = ClickHouseParquetConnector().details(
        _src("p", SourceType.parquet, path="https://host/x.parquet")
    )
    assert d["engine_clause"] == "URL('https://host/x.parquet', 'Parquet')"


# ---- capability --------------------------------------------------------------


def test_capabilities_relational_writable_parquet_pushdown_csv_neither():
    assert ClickHousePostgresConnector().capability().write is True
    assert ClickHouseMysqlConnector().capability().write is True
    assert ClickHouseParquetConnector().capability().predicate_pushdown is True
    assert ClickHouseCsvConnector().capability().predicate_pushdown is False


# ---- engine wiring -----------------------------------------------------------


def test_engine_reaches_the_five_source_types_and_is_clickhouse_native():
    eng = build_clickhouse_engine()
    assert eng.name == "clickhouse"
    assert eng.native_store == "clickhouse"
    for t in ("postgresql", "mysql", "mongodb", "csv", "parquet"):
        assert eng.reachable(t)


def test_engine_advertises_rows_and_arrow_transports():
    from types import SimpleNamespace

    rt = EngineRuntime(build_clickhouse_engine(), SimpleNamespace(trino_conn=None))
    assert rt.supports(EngineCapability.ROWS)
    assert rt.supports(EngineCapability.ARROW)


# ---- backend selection (REQ-912) ---------------------------------------------


def test_from_url_selects_embedded_for_chdb_scheme():
    from provisa.federation.clickhouse_runtime import _EmbeddedBackend
    from provisa.federation.clickhouse_runtime import ClickHouseFederationRuntime as RT

    rt = RT.from_url("chdb://")
    assert isinstance(rt._backend, _EmbeddedBackend)
    rt.close()


def test_from_url_selects_server_for_clickhouse_scheme(monkeypatch):
    # Don't touch a real server: stub the connect client so construction stays offline.
    import clickhouse_connect

    class _Stub:
        def command(self, sql):
            return None

        def query(self, sql):
            raise AssertionError("not called")

        def close(self):
            return None

    monkeypatch.setattr(clickhouse_connect, "get_client", lambda **kw: _Stub())
    from provisa.federation.clickhouse_runtime import _ServerBackend
    from provisa.federation.clickhouse_runtime import ClickHouseFederationRuntime as RT

    rt = RT.from_url("clickhouse://user:pw@ch-host:8123/db")
    assert isinstance(rt._backend, _ServerBackend)
    rt.close()


def test_from_url_selects_native_for_clickhouse_native_scheme(monkeypatch):
    # Stub the native Client so construction never opens a TCP connection.
    import clickhouse_driver

    class _Stub:
        def __init__(self, **kw):
            self.kw = kw

        def execute(self, sql, **kw):
            return None

        def disconnect(self):
            return None

    monkeypatch.setattr(clickhouse_driver, "Client", _Stub)
    from provisa.federation.clickhouse_runtime import _NativeBackend
    from provisa.federation.clickhouse_runtime import ClickHouseFederationRuntime as RT

    rt = RT.from_url("clickhouse+native://user:pw@ch-host:9000/db")
    assert isinstance(rt._backend, _NativeBackend)
    assert rt._backend._client.kw["port"] == 9000
    rt.close()


def test_from_url_rejects_unknown_scheme():
    from provisa.federation.clickhouse_runtime import ClickHouseFederationRuntime as RT

    with pytest.raises(ValueError, match="clickhouse\\+native"):
        RT.from_url("postgresql://x")


if __name__ == "__main__":
    pytest.main([__file__, "-q"])
