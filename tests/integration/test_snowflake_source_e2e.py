# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""E2E: Snowflake as a first-class NAMED SOURCE (REQ-988), reachable on ANY engine.

Mirrors the Databricks source e2e: register Snowflake through the REAL pool path (``SourcePool.add``
→ registry ``create_driver`` → ``configure`` with federation_hints account/warehouse → ``connect`` →
``execute``) and read a live table. SKIPPED here — snowflake-connector-python is not installed and no
account creds are set (REQ-988). Set SNOWFLAKE_ACCOUNT / SNOWFLAKE_USER / SNOWFLAKE_PASSWORD (and
install the connector) to enable.
"""

from __future__ import annotations

import os

import pytest

pytestmark = [pytest.mark.integration]

pytest.importorskip("snowflake.connector", reason="snowflake-connector-python not installed")

_ENV = ("SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD")
_HAVE_CREDS = all(os.environ.get(v) for v in _ENV)
pytestmark.append(
    pytest.mark.skipif(not _HAVE_CREDS, reason="Snowflake account creds not set (SNOWFLAKE_*)")
)

from provisa.executor.pool import SourcePool  # noqa: E402

_SID = "sf_src_e2e"
_DB = "PROVISA_SRC_E2E"
_SCHEMA = "PUBLIC"
_TABLE = "WIDGETS"


@pytest.fixture
def seeded():
    import snowflake.connector as sf

    conn = sf.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
    )
    cur = conn.cursor()
    cur.execute(f"CREATE DATABASE IF NOT EXISTS {_DB}")
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {_DB}.{_SCHEMA}")
    cur.execute(f"CREATE OR REPLACE TABLE {_DB}.{_SCHEMA}.{_TABLE} (id NUMBER, name STRING)")
    cur.execute(f"INSERT INTO {_DB}.{_SCHEMA}.{_TABLE} VALUES (1,'a'),(2,'b'),(3,'c')")
    cur.close()
    try:
        yield f"{_DB}.{_SCHEMA}.{_TABLE}"
    finally:
        cur = conn.cursor()
        cur.execute(f"DROP TABLE IF EXISTS {_DB}.{_SCHEMA}.{_TABLE}")
        cur.close()
        conn.close()


@pytest.mark.asyncio
async def test_snowflake_named_source_reads_through_source_pool(seeded):
    pool = SourcePool()
    await pool.add(
        source_id=_SID,
        source_type="snowflake",
        host=os.environ["SNOWFLAKE_ACCOUNT"],
        port=443,
        database=_DB,
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        # account/warehouse can't ride the standard args — they come via federation_hints → configure.
        extra={
            "account": os.environ["SNOWFLAKE_ACCOUNT"],
            "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        },
    )
    assert pool.has(_SID)
    assert pool.dialect_for(_SID) == "snowflake"
    driver = pool.get(_SID)
    result = await driver.execute(f"SELECT id, name FROM {seeded} ORDER BY id")
    assert result.column_names == ["ID", "NAME"]  # Snowflake upper-cases unquoted identifiers
    assert result.rows == [(1, "a"), (2, "b"), (3, "c")]
    await driver.close()


if __name__ == "__main__":
    pytest.main([__file__, "-q"])
