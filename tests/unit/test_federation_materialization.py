# Copyright (c) 2026 Kenneth Stott
# Canary: 7a2c9d40-3b18-4e75-8f02-1c6a0d4f9b95
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-844/845/846/848: materialization store backend validity, write face, reactive set."""

from __future__ import annotations

import pytest

from provisa.core.models import Source, SourceType
from provisa.federation.engine import (
    build_duckdb_engine,
    build_sqlalchemy_engine,
    build_trino_engine,
)
from provisa.federation.materialization import (
    InvalidMaterializationBackend,
    WriteFace,
    reactive_sources,
    select_write_face,
    validate_materialization_backend,
)


def _src(sid: str, type_: SourceType, **kw) -> Source:
    return Source(id=sid, type=type_, host="h", port=1, database="d", username="u", **kw)


# ---- backend validity (REQ-846) --------------------------------------------


def test_engine_native_store_is_valid():
    validate_materialization_backend(build_duckdb_engine(), "duckdb")  # own store, no raise


def test_attach_reachable_backend_is_valid():
    validate_materialization_backend(build_trino_engine(), "postgresql")  # Trino attaches PG


def test_backend_with_no_connector_rejected():
    with pytest.raises(InvalidMaterializationBackend, match="no connector"):
        validate_materialization_backend(build_trino_engine(), "kudu")


def test_land_only_backend_rejected_as_regress():
    # A self-only (sqlalchemy) engine whose native store is mysql cannot read a separate
    # PG store landed into it — postgresql is a LAND-only connector here, so it regresses.
    with pytest.raises(InvalidMaterializationBackend):
        validate_materialization_backend(build_sqlalchemy_engine("mysql://h/db"), "postgresql")


# ---- write face selection (REQ-848) -----------------------------------------


def test_engine_native_write_face_collapses_into_engine():
    assert select_write_face(build_duckdb_engine(), "duckdb") is WriteFace.ENGINE_NATIVE
    # sqlalchemy engine on a mysql URL materializes into its own (mysql) store.
    assert select_write_face(build_sqlalchemy_engine("mysql://h/db"), "mysql") is (
        WriteFace.ENGINE_NATIVE
    )


def test_separate_relational_store_uses_sqlalchemy_upsert():
    assert select_write_face(build_trino_engine(), "postgresql") is WriteFace.SQLALCHEMY_UPSERT


def test_write_face_validates_backend_first():
    with pytest.raises(InvalidMaterializationBackend):
        select_write_face(build_trino_engine(), "kudu")


# ---- reactive-replica set (REQ-845) -----------------------------------------


def test_reactive_set_is_engine_relative():
    api = _src("api", SourceType.openapi, base_url="http://x")
    pg = _src("pg", SourceType.postgresql)
    mongo = _src("m", SourceType.mongodb)
    sources = [api, pg, mongo]
    # On Trino: pg + mongo are VIRTUAL (both have connectors); only api (openapi, PG-cache LAND) is
    # MATERIALIZED → reactive. The reactive set is engine-relative to the engine's connector reach.
    assert reactive_sources(build_trino_engine(), sources) == {"api"}


def test_reactive_set_excludes_scannable_and_unreachable():
    csv = _src("c", SourceType.csv, path="/c.csv")
    pg = _src("pg", SourceType.postgresql)
    api = _src("api", SourceType.openapi, base_url="http://x")
    # On DuckDB: csv SCANs, pg VIRTUAL → neither reactive; api MATERIALIZED → reactive.
    assert reactive_sources(build_duckdb_engine(), [csv, pg, api]) == {"api"}


# ---- reachable materialized-store set (REQ-846) -----------------------------


def test_materialize_stores_derived_from_connectors():
    # Connector-derived: only backends flagged materialized_store (PG today) are usable stores.
    assert build_duckdb_engine().materialize_stores == frozenset({"postgresql"})
    assert build_trino_engine().materialize_stores == frozenset({"postgresql"})


def test_materialize_stores_excludes_unflagged_reachable_backends():
    # DuckDB reaches iceberg/mongodb/snowflake (connectors) but none is a materialized store yet.
    stores = build_duckdb_engine().materialize_stores
    assert "iceberg" not in stores and "mongodb" not in stores and "snowflake" not in stores
