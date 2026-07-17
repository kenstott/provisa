# Copyright (c) 2026 Kenneth Stott
# Canary: 587b181a-cdd8-419b-9ea0-afc6eb324c0f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-950: wire-compatible RDBs reuse their base wire's driver / dialect / Trino connector — they
need only registry entries, no new code. cockroach/yugabyte/greenplum are Postgres-wire; tidb MySQL-
wire."""

from __future__ import annotations

import pytest

from provisa.core.models import Source, SourceType
from provisa.core.source_registry import SOURCE_TO_DIALECT
from provisa.federation.trino_connectors import trino_connector_name
from provisa.executor.drivers.registry import has_native_driver
from provisa.federation.engine import build_trino_engine
from provisa.federation.strategy import Strategy, federate

_PG_WIRE = ["cockroachdb", "yugabytedb", "greenplum"]
_MYSQL_WIRE = ["tidb"]


def _src(stype: str) -> Source:
    return Source(
        id=f"s_{stype}", type=SourceType(stype), host="h", port=5432, database="db", username="u"
    )


@pytest.mark.parametrize("stype", _PG_WIRE + _MYSQL_WIRE)
def test_wire_compatible_rdb_is_federated_virtual_on_trino(stype):
    # Trino reaches them via the base wire's connector → federated in place (VIRTUAL), not landed.
    assert federate(_src(stype), build_trino_engine()) is Strategy.VIRTUAL


@pytest.mark.parametrize("stype", _PG_WIRE + _MYSQL_WIRE)
def test_wire_compatible_rdb_has_native_direct_driver(stype):
    assert has_native_driver(stype)  # reuses the base wire's async driver (pg / mysql)


@pytest.mark.parametrize("stype", _PG_WIRE)
def test_pg_wire_maps_to_postgres(stype):
    assert SOURCE_TO_DIALECT[stype] == "postgres"
    assert trino_connector_name(stype) == "postgresql"
    url = _src(stype).jdbc_url()
    assert url == "jdbc:postgresql://h:5432/db?autosave=conservative"


def test_tidb_maps_to_mysql():
    assert SOURCE_TO_DIALECT["tidb"] == "mysql"
    assert trino_connector_name("tidb") == "mysql"
    assert _src("tidb").jdbc_url() == "jdbc:mysql://h:5432/db"


def test_exasol_and_redshift_jdbc_urls_present():
    # REQ-1097 regression: both declare a Trino JDBC connector, so jdbc_url() must emit a
    # non-empty URL — otherwise _TrinoJdbcConnector.details() returns {} and create_catalog()
    # silently no-ops the catalog. Exasol is colon-delimited (jdbc:exa:host:port, no db);
    # redshift is standard pgjdbc-shaped.
    exa = Source(id="s_exa", type=SourceType.exasol, host="h", port=8563, database="db")
    assert exa.jdbc_url() == "jdbc:exa:h:8563"
    rs = Source(id="s_rs", type=SourceType.redshift, host="h", port=5439, database="db")
    assert rs.jdbc_url() == "jdbc:redshift://h:5439/db"


def test_druid_jdbc_url_is_avatica():
    # REQ-1097 regression: TrinoDruidConnector.details() builds connection-url from jdbc_url(), so it
    # must emit the broker Avatica URL Trino's druid connector wraps — not "" (which would make
    # details() return {} and create_catalog() silently no-op the catalog). Druid is NOT a standard
    # jdbc://host:port/db shape and has no database segment.
    dr = Source(id="s_druid", type=SourceType.druid, host="broker", port=8082)
    assert dr.jdbc_url() == "jdbc:avatica:remote:url=http://broker:8082/druid/v2/sql/avatica/"


def test_hive_catalog_emits_metastore_uri_not_empty():
    # REQ-1097 regression: hive/hive_s3 are NOT JDBC connectors — jdbc_url() is empty for them, so
    # the old generic _TrinoJdbcConnector.details() returned {} and create_catalog() silently no-op'd
    # the hive catalog. The dedicated connectors must emit a non-empty catalog with the Thrift
    # metastore uri (thrift://<host>:9083) instead.
    from provisa.federation.trino_connectors import TRINO_CONNECTORS

    assert trino_connector_name("hive") == "hive"
    assert trino_connector_name("hive_s3") == "hive"

    hv = Source(id="s_hive", type=SourceType.hive, host="hive-metastore", port=9083)
    props = TRINO_CONNECTORS["hive"].details(hv)
    assert props["hive.metastore"] == "thrift"
    assert props["hive.metastore.uri"] == "thrift://hive-metastore:9083"
    assert props["fs.hadoop.enabled"] == "true"
    assert props["hive.non-managed-table-writes-enabled"] == "true"
    assert "connection-url" not in props  # not a JDBC connector

    # Default metastore port is the Hive Thrift default (9083) when the source omits it.
    assert (
        TRINO_CONNECTORS["hive"].details(Source(id="s2", type=SourceType.hive, host="m"))[
            "hive.metastore.uri"
        ]
        == "thrift://m:9083"
    )


def test_hive_s3_catalog_emits_native_s3_from_mapping():
    # REQ-1097 regression: hive_s3 reuses the hive metastore but wires the native S3 filesystem from
    # the source mapping; a missing s3 mapping is a misconfiguration that must fail loud (no fallback).
    from provisa.federation.trino_connectors import TRINO_CONNECTORS

    src = Source(
        id="s_hive_s3",
        type=SourceType.hive_s3,
        host="hive-metastore",
        port=9083,
        mapping={
            "endpoint": "http://minio:9000",
            "access_key_id": "minioadmin",
            "secret_access_key": "minioadmin",
            "region": "us-east-1",
        },
    )
    props = TRINO_CONNECTORS["hive_s3"].details(src)
    assert props["hive.metastore.uri"] == "thrift://hive-metastore:9083"
    assert props["fs.native-s3.enabled"] == "true"
    assert props["s3.endpoint"] == "http://minio:9000"
    assert props["s3.aws-access-key"] == "minioadmin"
    assert props["s3.aws-secret-key"] == "minioadmin"
    assert props["s3.region"] == "us-east-1"
    assert props["s3.path-style-access"] == "true"

    with pytest.raises(ValueError, match="hive_s3 requires s3"):
        TRINO_CONNECTORS["hive_s3"].details(
            Source(id="bad", type=SourceType.hive_s3, host="hive-metastore")
        )
