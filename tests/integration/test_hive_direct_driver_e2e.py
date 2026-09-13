# Copyright (c) 2026 Kenneth Stott
# Canary: 6ad741ab-17d9-457d-abe8-f0a0e2c563da
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""E2E: HiveDriver — a HiveServer2 endpoint reached DIRECTLY over Thrift, via impyla (REQ-1731).

Distinct from test_hive_source_e2e.py, which drives ``hive``/``hive_s3`` as Trino-scanned lake
STORAGE (Trino's own hive connector reads the warehouse files + metastore; HiveServer2 is never
involved). This is the OTHER shape: SourceType.hiveserver2 is a live SQL endpoint Provisa reads
directly then lands, the same way singlestore/snowflake/databricks are — for when HS2 itself is the
only thing reachable (no direct filesystem/S3 access to the warehouse).

The compose fixture (docker-compose.test.yml, ``requires_hive`` marker) brings up both
``hive-metastore`` and ``hive-server2`` (HS2 pointed at that same metastore via SERVICE_OPTS —
verified live: this image's entrypoint folds SERVICE_OPTS into HADOOP_CLIENT_OPTS, there is no
dedicated metastore-uri env var). Auth: stock HS2's default (``hive.server2.authentication=NONE``)
speaks SASL PLAIN and accepts any username/password including empty; HiveDriver defaults to PLAIN
for exactly this reason (impyla's NOSASL, its Impala-oriented default, makes a stock HS2 drop the
connection immediately — verified live, see HiveDriver.configure's docstring).
"""

from __future__ import annotations

import os

import pytest

from provisa.executor.drivers.hive import HiveDriver

pytestmark = [pytest.mark.integration, pytest.mark.asyncio, pytest.mark.requires_hive]

_HOST = os.environ.get("HIVE_SERVER2_HOST", "localhost")
_PORT = int(os.environ.get("HIVE_SERVER2_PORT", "10000"))
_TABLE = "direct_driver_widgets"
_WIDGETS = [(1, "Widget A"), (2, "Widget B"), (3, "Widget C")]


async def test_hive_direct_driver_ddl_dml_select() -> None:
    """Real CREATE TABLE / INSERT / SELECT through HiveDriver against a real HiveServer2."""
    driver = HiveDriver()
    await driver.connect(host=_HOST, port=_PORT, database="default", user="hive", password="")
    try:
        await driver.execute(f"DROP TABLE IF EXISTS {_TABLE}")
        await driver.execute(f"CREATE TABLE {_TABLE} (id INT, name STRING)")
        values = ", ".join(f"({wid}, '{name}')" for wid, name in _WIDGETS)
        await driver.execute(f"INSERT INTO {_TABLE} VALUES {values}")
        result = await driver.execute(f"SELECT id, name FROM {_TABLE} ORDER BY id")
        assert result.column_names == ["id", "name"]
        assert result.rows == _WIDGETS
    finally:
        await driver.execute(f"DROP TABLE IF EXISTS {_TABLE}")
        await driver.close()
