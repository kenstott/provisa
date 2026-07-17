# Copyright (c) 2026 Kenneth Stott
# Canary: 9904f4bf-c8b5-4955-ad79-1a323cfb9482
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-951: a Connector declares its reach mechanism (how rows are OBTAINED) and materializability
(orthogonal). ATTACH_* = engine reads live; DIRECT/FETCH = Provisa reads + lands a refreshed replica
the engine reads (there is no engine 'LAND' — the engine never writes)."""

from __future__ import annotations

from provisa.core.models import Source, SourceType
from provisa.federation.connector import WarehouseNativeConnector
from provisa.federation.connector_base import Mechanism
from provisa.federation.trino_connectors import (
    TrinoFilesConnector,
    TrinoOpenapiConnector,
    TrinoPostgresConnector,
)
from provisa.federation.connector_base import LIVE_IN_PLACE, DriverProvider, RuntimeDep
from provisa.federation.connector_duckdb import DuckDBParquetConnector, MysqlFdwConnector
from provisa.federation.engine import build_trino_engine
from provisa.federation.strategy import Strategy, federate


def test_relational_is_attach_rw_and_materializable():
    c = TrinoPostgresConnector()
    assert c.mechanism is Mechanism.ATTACH_RW  # engine reads the live source, writable
    assert c.reach_modes == frozenset({Mechanism.ATTACH_RW})
    assert c.materializable is True  # can ALSO be cached — orthogonal to the reach mechanism


def test_api_source_is_fetch():
    # openapi: Provisa fetches + lands a replica; the engine reads it → FETCH, not a live attach.
    assert TrinoOpenapiConnector().mechanism is Mechanism.FETCH


def test_self_only_engine_source_is_direct_not_engine_land():
    # A warehouse/self-only engine can't attach an external source live; Provisa reads it (DIRECT)
    # and lands the replica — the engine never lands its own data.
    assert WarehouseNativeConnector("wh", "wh").mechanism is Mechanism.DIRECT


def test_federate_attach_is_virtual_direct_fetch_is_materialized():
    pg = Source(
        id="pg", type=SourceType.postgresql, host="h", port=5432, database="d", username="u"
    )
    api = Source(
        id="api", type=SourceType.openapi, host="h", port=1, database="d", base_url="http://x"
    )
    eng = build_trino_engine()
    assert federate(pg, eng) is Strategy.VIRTUAL  # ATTACH_RW → engine reads live
    assert federate(api, eng) is Strategy.MATERIALIZED  # FETCH → materialized replica


def test_scan_reach_mode_reads_in_place_and_federates_scan():  # REQ-951
    # A file/object connector declares SCAN (read in place as a view) — distinct from ATTACH (live DB)
    # and from DIRECT/FETCH (Provisa reads + lands). SCAN reads in place, so it is never landed.
    for c in (DuckDBParquetConnector(), TrinoFilesConnector()):
        assert c.mechanism is Mechanism.SCAN
        assert c.reads_in_place is True
        assert Mechanism.SCAN in LIVE_IN_PLACE
    files = Source(id="f", type=SourceType.files, path="/data/*.parquet")
    # files SCANs on Trino (file catalog reads the glob in place), not a MATERIALIZED replica.
    assert federate(files, build_trino_engine()) is Strategy.SCAN


def test_scan_and_attach_both_read_in_place_direct_fetch_do_not():  # REQ-951
    assert TrinoPostgresConnector().reads_in_place is True  # ATTACH_RW
    assert DuckDBParquetConnector().reads_in_place is True  # SCAN
    assert TrinoOpenapiConnector().reads_in_place is False  # FETCH → Provisa lands a replica
    assert WarehouseNativeConnector("wh", "wh").reads_in_place is False  # DIRECT


def test_runtime_deps_are_structured_by_provider():  # REQ-948
    # A bundled driver (Provisa ships it) is not operator-provided → not shown disabled/BYO.
    dep = MysqlFdwConnector().runtime_deps[0]
    assert isinstance(dep, RuntimeDep)
    assert dep.provider is DriverProvider.BUNDLED
    assert MysqlFdwConnector().operator_deps == ()
