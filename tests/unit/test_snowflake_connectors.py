# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-988: Snowflake object/lake ATTACH connectors (zero-copy external links via external stage +
external table) + the derived warehouse set. Driver-free. NOT live-verified — no Snowflake account is
available; the connector shape/DDL/validation mirror the live-verified Databricks/BigQuery/ClickHouse
external links."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.federation.connector_base import Mechanism
from provisa.federation.engine import build_snowflake_engine
from provisa.federation.snowflake_connectors import (
    snowflake_object_link_connectors,
    stage_and_external_table_ddl,
)
from provisa.federation.strategy import Strategy, federate


def _src(stype, path="s3://b/dir/orders.parquet", hints=None):
    return SimpleNamespace(
        id="x", type=SimpleNamespace(value=stype), path=path, federation_hints=hints or {}
    )


def test_object_link_connectors_are_attach_r():
    conns = {c.source_type: c for c in snowflake_object_link_connectors()}
    assert set(conns) == {"parquet", "csv", "json", "iceberg", "delta_lake"}
    assert all(
        c.mechanism is Mechanism.SCAN for c in conns.values()
    )  # object-link = SCAN (REQ-951)
    assert conns["parquet"].capability().write is False


def test_snowflake_engine_is_partial_attaches_lake_lands_rest():
    e = build_snowflake_engine()
    assert e.driver_class().value == "partial"  # was self_only before the external links
    for t in ("parquet", "csv", "iceberg", "delta_lake"):
        assert federate(_src(t), e) is Strategy.SCAN, t
    for t in ("postgresql", "mongodb", "kafka"):
        assert federate(_src(t), e) is Strategy.MATERIALIZED, t


def test_stage_and_external_table_ddl_stage_creds_and_validate():
    d = {
        "format": "PARQUET",
        "location": "s3://b/dir/orders.parquet",
        "credential": {"access_key_id": "AK", "secret_access_key": "SK", "endpoint": None},
    }
    stmts = stage_and_external_table_ddl("db", "sch", "orders", "stg", d)
    joined = " | ".join(stmts)
    assert 'CREATE STAGE IF NOT EXISTS "db"."sch"."stg"' in joined
    assert "URL = 's3://b/dir/'" in joined  # file URI → directory as the stage URL
    assert "AWS_KEY_ID = 'AK'" in joined and "AWS_SECRET_KEY = 'SK'" in joined
    assert 'CREATE OR REPLACE EXTERNAL TABLE "db"."sch"."orders"' in joined
    assert "FILE_FORMAT = (TYPE = PARQUET)" in joined
    assert "PATTERN = '.*orders.parquet'" in joined  # single-file → PATTERN match
    assert stmts[-1].startswith("SELECT * FROM") and "LIMIT 1" in stmts[-1]  # validation probe


def test_stage_ddl_s3_compatible_endpoint():
    d = {
        "format": "CSV",
        "location": "s3://b/data",
        "credential": {"access_key_id": "AK", "secret_access_key": "SK", "endpoint": "https://r2"},
    }
    joined = " | ".join(stage_and_external_table_ddl("db", "sch", "t", "stg", d))
    assert "ENDPOINT = 'https://r2'" in joined
    assert "URL = 's3://b/data/'" in joined  # directory URI → no PATTERN


def test_stage_ddl_requires_location():
    with pytest.raises(ValueError, match="no 'path'"):
        stage_and_external_table_ddl(
            "db", "sch", "t", "stg", {"format": "PARQUET", "location": None}
        )


if __name__ == "__main__":
    pytest.main([__file__, "-q"])
