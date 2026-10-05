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
    build_clickhouse_engine,
    build_duckdb_engine,
    build_pg_engine,
    build_snowflake_engine,
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
    # airport (DuckDB's own airport community extension, REQ-899) has no Trino connector at all —
    # parquet no longer fits this case since Trino gained a real (SCAN-only) parquet connector.
    with pytest.raises(InvalidMaterializationBackend, match="no connector"):
        validate_materialization_backend(build_trino_engine(), "airport")


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
        select_write_face(build_trino_engine(), "parquet")


# REQ-848: select_write_face is the ONE land-face decision, wired into land_source_table
# (native_backend.py). This table asserts the face for every (engine kind, store backend) the code
# supports equals what the runtime does today: a native engine landing into its OWN store collapses
# into the engine (ENGINE_NATIVE, through its runtime's land_table); a separate attach-able
# relational store uses store_writer (SQLALCHEMY_UPSERT). With no origin named, no face is
# PIPELINE_LAND (the origin-keyed cases follow).
@pytest.mark.parametrize(
    ("engine_factory", "store_backend", "expected"),
    [
        (lambda: build_duckdb_engine(), "duckdb", WriteFace.ENGINE_NATIVE),
        (lambda: build_pg_engine(), "postgresql", WriteFace.ENGINE_NATIVE),
        (lambda: build_clickhouse_engine(), "clickhouse", WriteFace.ENGINE_NATIVE),
        (lambda: build_snowflake_engine(), "snowflake", WriteFace.ENGINE_NATIVE),
        (lambda: build_sqlalchemy_engine("mysql://h/db"), "mysql", WriteFace.ENGINE_NATIVE),
        # REQ-990: a SingleStore store-engine lands into its own store (its bulk path there is the
        # streaming LOAD DATA, but the FACE is still engine-native — same as every other own store).
        (
            lambda: build_sqlalchemy_engine("singlestoredb://h/db"),
            "singlestoredb",
            WriteFace.ENGINE_NATIVE,
        ),
        # Trino attaches a separate relational store and lands through store_writer.
        (lambda: build_trino_engine(), "postgresql", WriteFace.SQLALCHEMY_UPSERT),
    ],
)
def test_write_face_table_matches_runtime(engine_factory, store_backend, expected):
    assert select_write_face(engine_factory(), store_backend) is expected


# REQ-990: with SingleStore as the engine's own store, an origin in the pipeline's scope lands
# through a SingleStore PIPELINE; any other origin keeps the engine's own face (decided by type);
# an in-scope origin that cannot land yet is refused by name.
def _origin(source_type: str, location: str, fmt: str):
    from provisa.federation.singlestore_pipeline import LandOrigin

    return LandOrigin(source_type, location, fmt)


@pytest.mark.parametrize(
    ("origin", "expected"),
    [
        (("kafka", "orders", "json"), WriteFace.PIPELINE_LAND),
        (("kafka", "orders", "avro"), WriteFace.PIPELINE_LAND),
        (("kafka", "orders", "protobuf"), WriteFace.ENGINE_NATIVE),
        (("csv", "s3://b/k.csv", "csv"), WriteFace.PIPELINE_LAND),
        (("parquet", "s3://b/k.parquet", "parquet"), WriteFace.PIPELINE_LAND),
        (("iceberg", "s3://b/warehouse/t", "iceberg"), WriteFace.PIPELINE_LAND),
        (("csv", "/data/local.csv", "csv"), WriteFace.ENGINE_NATIVE),
        (("iceberg", "gs://b/t", "iceberg"), WriteFace.ENGINE_NATIVE),
        (("delta_lake", "s3://b/t", "delta_lake"), WriteFace.ENGINE_NATIVE),
        (("hive_s3", "s3://b/t", "hive_s3"), WriteFace.ENGINE_NATIVE),
        (("mysql", "", "mysql"), WriteFace.ENGINE_NATIVE),
        (("mongodb", "", "mongodb"), WriteFace.ENGINE_NATIVE),
    ],
)
def test_singlestore_engine_lands_in_scope_origins_by_pipeline(origin, expected):
    engine = build_sqlalchemy_engine("singlestoredb://h/db")
    assert select_write_face(engine, "singlestoredb", _origin(*origin)) is expected


@pytest.mark.parametrize("location", ["gs://b/k.csv", "abfss://c@acct.dfs.core.windows.net/k.csv"])
def test_singlestore_engine_refuses_gcs_and_azure_files_by_name(location):
    from provisa.federation.singlestore_pipeline import PipelineRefused

    engine = build_sqlalchemy_engine("singlestoredb://h/db")
    with pytest.raises(PipelineRefused, match="no (GCS|Azure) credential model"):
        select_write_face(engine, "singlestoredb", _origin("csv", location, "csv"))


def test_only_a_singlestore_store_lands_by_pipeline():
    origin = _origin("parquet", "s3://b/k.parquet", "parquet")
    assert select_write_face(build_duckdb_engine(), "duckdb", origin) is WriteFace.ENGINE_NATIVE
    assert (
        select_write_face(build_sqlalchemy_engine("mysql://h/db"), "mysql", origin)
        is WriteFace.ENGINE_NATIVE
    )
    assert select_write_face(build_trino_engine(), "postgresql", origin) is (
        WriteFace.SQLALCHEMY_UPSERT
    )


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
