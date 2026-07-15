# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""E2E: Databricks as a first-class NAMED SOURCE (REQ-987), reachable on ANY engine.

Exercises the REAL source-registration path — ``SourcePool.add`` → registry ``create_driver`` →
``configure`` (federation_hints: http_path) → ``connect`` → ``execute`` — reading a live Databricks
Delta table. This is the read-directly-then-land face: the same databricks-sql-connector connection
the Databricks federation ENGINE uses, proving the engine capability IS the source capability (no
Trino / delta_lake detour). Skipped when creds are absent (CI-safe); needs SSL_CERT_FILE pointing at
a CA bundle incl. the proxy CA in a TLS-intercepting dev environment.
"""

from __future__ import annotations

import os

import pytest

pytestmark = [pytest.mark.integration]

pytest.importorskip("databricks.sql", reason="databricks-sql-connector required")

_ENV = ("DATABRICKS_SERVER_HOSTNAME", "DATABRICKS_HTTP_PATH", "DATABRICKS_TOKEN")
_HAVE_CREDS = all(os.environ.get(v) for v in _ENV)
pytestmark.append(
    pytest.mark.skipif(not _HAVE_CREDS, reason="Databricks warehouse creds not set (DATABRICKS_*)")
)

from provisa.executor.pool import SourcePool  # noqa: E402

_SID = "dbx_src_e2e"
_SCHEMA = "provisa_src_e2e"
_TABLE = "widgets"


@pytest.fixture
def seeded():
    """Seed a Delta table directly, yield a ready-to-read fully-qualified name, then drop it."""
    from databricks import sql as dbsql

    from provisa.federation.databricks_tls import databricks_tls_kwargs

    conn = dbsql.connect(
        server_hostname=os.environ["DATABRICKS_SERVER_HOSTNAME"],
        http_path=os.environ["DATABRICKS_HTTP_PATH"],
        access_token=os.environ["DATABRICKS_TOKEN"],
        _socket_timeout=60,
        _retry_stop_after_attempts_count=2,
        **databricks_tls_kwargs(),
    )
    cur = conn.cursor()
    fq = f"`workspace`.`{_SCHEMA}`.`{_TABLE}`"
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS `workspace`.`{_SCHEMA}`")
    cur.execute(f"DROP TABLE IF EXISTS {fq}")
    cur.execute(f"CREATE TABLE {fq} (id BIGINT, name STRING) USING DELTA")
    cur.execute(f"INSERT INTO {fq} VALUES (1,'a'),(2,'b'),(3,'c')")
    cur.close()
    try:
        yield fq
    finally:
        cur = conn.cursor()
        cur.execute(f"DROP TABLE IF EXISTS {fq}")
        cur.close()
        conn.close()


@pytest.mark.asyncio
async def test_databricks_named_source_reads_through_source_pool(seeded):
    """Register Databricks as a source via the production pool path and read a Delta table."""
    pool = SourcePool()
    await pool.add(
        source_id=_SID,
        source_type="databricks",
        host=os.environ["DATABRICKS_SERVER_HOSTNAME"],
        port=443,
        database="workspace",
        user="token",
        password=os.environ["DATABRICKS_TOKEN"],
        # http_path can't ride the standard args — it comes via federation_hints → configure.
        extra={"http_path": os.environ["DATABRICKS_HTTP_PATH"]},
    )
    assert pool.has(_SID)
    assert pool.dialect_for(_SID) == "databricks"  # drives transpile of the direct-read SQL

    driver = pool.get(_SID)
    result = await driver.execute(f"SELECT id, name FROM {seeded} ORDER BY id")
    assert result.column_names == ["id", "name"]
    assert result.rows == [(1, "a"), (2, "b"), (3, "c")]

    await driver.close()
    assert driver.is_connected is False


if __name__ == "__main__":
    pytest.main([__file__, "-q"])
