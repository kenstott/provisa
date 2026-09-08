# Copyright (c) 2026 Kenneth Stott
# Canary: b0d51e5b-0f3b-4d37-bfac-19d9f4c3a0b4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Unit tests for DuckDBParquetConnector's S3-compatible-endpoint secret wiring (REQ-1650)."""

from __future__ import annotations

from provisa.core.models import Source, SourceType
from provisa.federation.connector_duckdb import DuckDBParquetConnector, _s3_secret_ddl


def _source(path: str, federation_hints: dict[str, str] | None = None) -> Source:
    return Source(
        id="r2-orders",
        type=SourceType.parquet,
        path=path,
        federation_hints=federation_hints or {},
    )


def test_non_s3_path_has_no_secret_ddl():
    assert _s3_secret_ddl(_source("/local/orders.parquet")) is None


def test_s3_path_without_credentials_has_no_secret_ddl():
    assert _s3_secret_ddl(_source("s3://bucket/orders.parquet")) is None


def test_s3_path_with_credentials_and_endpoint_builds_secret():
    ddl = _s3_secret_ddl(
        _source(
            "s3://bucket/orders.parquet",
            {
                "access_key_id": "AKIDEXAMPLE",
                "secret_access_key": "SECRETEXAMPLE",
                "endpoint": "https://abc123.r2.cloudflarestorage.com",
            },
        )
    )
    assert ddl is not None
    assert 'CREATE OR REPLACE SECRET "_s3_r2-orders"' in ddl
    assert "KEY_ID 'AKIDEXAMPLE'" in ddl
    assert "SECRET 'SECRETEXAMPLE'" in ddl
    assert "ENDPOINT 'abc123.r2.cloudflarestorage.com'" in ddl  # scheme stripped, bare host
    assert "URL_STYLE 'path'" in ddl


def test_s3_path_with_credentials_no_endpoint_omits_endpoint_clause():
    ddl = _s3_secret_ddl(
        _source(
            "s3://bucket/orders.parquet",
            {"access_key_id": "AKIDEXAMPLE", "secret_access_key": "SECRETEXAMPLE"},
        )
    )
    assert ddl is not None
    assert "ENDPOINT" not in ddl
    assert "URL_STYLE" not in ddl


def test_parquet_connector_details_includes_secret_ddl_for_r2_source():
    conn = DuckDBParquetConnector()
    result = conn.details(
        _source(
            "s3://bucket/orders.parquet",
            {
                "access_key_id": "AKIDEXAMPLE",
                "secret_access_key": "SECRETEXAMPLE",
                "endpoint": "https://abc123.r2.cloudflarestorage.com",
            },
        )
    )
    assert "view_ddl" in result
    assert "secret_ddl" in result


def test_parquet_connector_details_omits_secret_ddl_for_local_source():
    conn = DuckDBParquetConnector()
    result = conn.details(_source("/local/orders.parquet"))
    assert "view_ddl" in result
    assert "secret_ddl" not in result
