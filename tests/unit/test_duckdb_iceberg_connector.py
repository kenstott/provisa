# Copyright (c) 2026 Kenneth Stott
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""DuckDBIcebergConnector (REQ-899): iceberg_scan scanner-view details(), including the
S3-SECRET emission fixed by REQ-1736 (previously only DuckDBParquetConnector called
``_s3_secret_ddl`` — DuckDBIcebergConnector's own docstring promised object-store access via a
DuckDB SECRET, but never actually built one)."""

from __future__ import annotations

from provisa.core.models import Source, SourceType
from provisa.federation.connector import Mechanism
from provisa.federation.connector_duckdb import DuckDBIcebergConnector


def _src(sid: str, path: str = "/warehouse/orders", **kw) -> Source:
    return Source(id=sid, type=SourceType.iceberg, path=path, **kw)


def test_iceberg_connector_identity():
    c = DuckDBIcebergConnector()
    assert c.engine == "duckdb"
    assert c.source_type == "iceberg"
    assert c.key == "duckdb_iceberg"
    assert c.extension == "iceberg"
    assert c.install_from_community is False
    assert c.probe_symbol == "iceberg_scan"
    assert c.mechanism is Mechanism.SCAN


def test_details_emits_iceberg_scan_view_over_source_path():
    details = DuckDBIcebergConnector().details(_src("orders", path="s3://bucket/orders"))
    assert details == {
        "view_ddl": "CREATE VIEW orders AS SELECT * FROM iceberg_scan('s3://bucket/orders')"
    }


def test_details_emits_s3_secret_ddl_when_credentials_present():
    src = _src(
        "orders",
        path="s3://bucket/orders",
        federation_hints={"access_key_id": "AKIAFAKE", "secret_access_key": "shh"},
    )
    details = DuckDBIcebergConnector().details(src)
    assert "secret_ddl" in details
    assert "KEY_ID 'AKIAFAKE'" in details["secret_ddl"]
    assert "SECRET 'shh'" in details["secret_ddl"]


def test_details_omits_secret_ddl_without_credentials():
    details = DuckDBIcebergConnector().details(_src("orders", path="s3://bucket/orders"))
    assert "secret_ddl" not in details
