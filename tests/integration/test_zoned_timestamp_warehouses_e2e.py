# Copyright (c) 2026 Kenneth Stott
# Canary: f77e7225-85ca-40af-bce2-5ad618c91c41
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E: a cloud warehouse's zoned timestamp is read as the instant it is (REQ-2050) -- Snowflake's
TIMESTAMP_TZ and TIMESTAMP_LTZ, BigQuery's TIMESTAMP -- through the direct SourcePool, each written
at a +02:00 offset and read back as the instant in UTC. The warehouse lane only, where the live
credentials load."""

# Requirements: REQ-2050

from __future__ import annotations

import os
from datetime import datetime, timezone

import pytest

from tests.env_creds import unset

pytestmark = [pytest.mark.integration, pytest.mark.requires_warehouse, pytest.mark.asyncio]

_INSTANT = datetime(2024, 1, 2, 1, 4, 5, tzinfo=timezone.utc)  # 03:04:05 at +02:00
_SNOWFLAKE = ("SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD")
_BIGQUERY = ("GOOGLE_CLOUD_PROJECT", "GOOGLE_APPLICATION_CREDENTIALS")


@pytest.fixture
def snowflake_warehouse_left_as_found():
    """The test's statement resumes the Snowflake warehouse if it is suspended; it is suspended
    again after the test however it ends, unless it was running before (REQ-254)."""
    pytest.importorskip("snowflake.connector", reason="snowflake-connector-python not installed")
    from tests.integration.snowflake_warehouse import warehouse_in_use

    with warehouse_in_use(os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")):
        yield


@pytest.mark.skipif(
    not all(os.environ.get(v) for v in _SNOWFLAKE), reason=f"not set: {unset(*_SNOWFLAKE)}"
)
async def test_snowflake_timestamp_tz_and_ltz(snowflake_warehouse_left_as_found):  # noqa: ARG001 -- holds the warehouse
    pytest.importorskip("snowflake.connector", reason="snowflake-connector-python not installed")
    from provisa.executor.pool import SourcePool

    pool = SourcePool()
    await pool.add(
        source_id="sf-zoned",
        source_type="snowflake",
        host=os.environ["SNOWFLAKE_ACCOUNT"],
        port=443,
        database="SNOWFLAKE",
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        extra={
            "account": os.environ["SNOWFLAKE_ACCOUNT"],
            "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        },
    )
    try:
        rows = (
            await pool.execute(
                "sf-zoned",
                "SELECT '2024-01-02 03:04:05 +02:00'::TIMESTAMP_TZ, "
                "'2024-01-02 03:04:05 +02:00'::TIMESTAMP_TZ::TIMESTAMP_LTZ",
            )
        ).rows
    finally:
        await pool.close_all()
    assert [tuple(v.astimezone(timezone.utc) for v in row) for row in rows] == [
        (_INSTANT, _INSTANT)
    ]


@pytest.mark.skipif(
    not all(os.environ.get(v) for v in _BIGQUERY), reason=f"not set: {unset(*_BIGQUERY)}"
)
async def test_bigquery_timestamp():
    pytest.importorskip("google.cloud.bigquery", reason="google-cloud-bigquery not installed")
    from provisa.executor.pool import SourcePool

    pool = SourcePool()
    await pool.add(
        source_id="bq-zoned",
        source_type="bigquery",
        host="",
        port=0,
        database=os.environ["GOOGLE_CLOUD_PROJECT"],
        user="",
        password="",
    )
    try:
        rows = (
            await pool.execute("bq-zoned", "SELECT TIMESTAMP '2024-01-02 03:04:05+02:00' AS ts")
        ).rows
    finally:
        await pool.close_all()
    assert [tuple(v.astimezone(timezone.utc) for v in row) for row in rows] == [(_INSTANT,)]
