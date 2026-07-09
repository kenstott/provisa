# Copyright (c) 2026 Kenneth Stott
# Canary: 9a2c4d71-6b08-4f53-8e12-3c7a0d4f9b74
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-826: federate(datasource, table) strategy resolution."""

from __future__ import annotations

import pytest

from provisa.core.models import Source, SourceType
from provisa.federation.engine import (
    UnreachableSource,
    build_duckdb_engine,
    build_sqlalchemy_engine,
    build_trino_engine,
)
from provisa.federation.strategy import Strategy, federate, requires_residency


def _src(sid: str, type_: SourceType, **kw) -> Source:
    return Source(id=sid, type=type_, host="h", port=1, database="d", username="u", **kw)


# ---- strategy per (source, engine) capability -------------------------------


def test_live_db_via_attach_is_virtual():
    assert federate(_src("pg", SourceType.postgresql), build_trino_engine()) is Strategy.VIRTUAL
    assert federate(_src("pg", SourceType.postgresql), build_duckdb_engine()) is Strategy.VIRTUAL


def test_file_via_scanner_view_is_scan():
    assert (
        federate(_src("e", SourceType.csv, path="/e.csv"), build_duckdb_engine()) is Strategy.SCAN
    )


def test_warehouse_native_lands_is_materialized():
    # A self-only sqlalchemy engine lands every source into its own store.
    eng = build_sqlalchemy_engine("mysql://h/db")
    assert federate(_src("pg", SourceType.postgresql), eng) is Strategy.MATERIALIZED


def test_api_source_with_no_connector_is_materialized():
    # OpenAPI has no live/scan representation on Trino → loaded into the store.
    assert federate(_src("api", SourceType.openapi, base_url="http://x"), build_trino_engine()) is (
        Strategy.MATERIALIZED
    )


def test_nosql_source_is_virtual_on_trino():
    # Trino has a native mongodb connector (REQ-842), so mongodb is federated in place (VIRTUAL),
    # not materialized — the engine's connector set is its true reach.
    assert federate(_src("m", SourceType.mongodb), build_trino_engine()) is Strategy.VIRTUAL


def test_same_source_different_strategy_per_engine():
    # csv scans on DuckDB, but Trino (no csv connector here) has no scan → unreachable.
    csv = _src("c", SourceType.csv, path="/c.csv")
    assert federate(csv, build_duckdb_engine()) is Strategy.SCAN
    with pytest.raises(UnreachableSource):
        federate(csv, build_trino_engine())


def test_prefer_materialized_overrides_attachable():
    # A live DB deliberately cached for latency → MATERIALIZED even though attachable.
    strat = federate(
        _src("pg", SourceType.postgresql), build_trino_engine(), prefer_materialized=True
    )
    assert strat is Strategy.MATERIALIZED


def test_unreachable_when_neither_attach_scan_nor_materializable():
    # kudu: no Trino connector and not materialize-only → genuinely unreachable.
    with pytest.raises(UnreachableSource):
        federate(_src("o", SourceType.kudu), build_trino_engine())


def test_engine_federate_method_matches():
    eng = build_duckdb_engine()
    src = _src("pg", SourceType.postgresql)
    assert eng.federate(src) is federate(src, eng)


# ---- residency (REQ-825 stage-4b prep phase) --------------------------------


def test_only_materialized_requires_residency():
    assert requires_residency(Strategy.MATERIALIZED) is True
    assert requires_residency(Strategy.VIRTUAL) is False
    assert requires_residency(Strategy.SCAN) is False
