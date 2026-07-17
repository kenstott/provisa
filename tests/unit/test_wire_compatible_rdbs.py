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
