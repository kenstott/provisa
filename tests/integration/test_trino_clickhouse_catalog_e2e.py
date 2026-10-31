# Copyright (c) 2026 Kenneth Stott
# Canary: 8b2f4e61-5c3d-4a79-b0e8-1d6a9f3c7e25
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""E2E: a ClickHouse source gets a Trino catalog, and a join runs across it (J285 P1, P3).

``create_catalog`` builds the catalog from ``Source.jdbc_url()`` (the HTTP port, 8123) and sets
``clickhouse.map-string-as-varchar``, so a ClickHouse ``String`` reads as varchar rather than
varbinary. The join is against a Postgres catalog Trino makes through the same path.
"""

from __future__ import annotations

import os
import time

import pytest
import trino.dbapi

pytestmark = [
    pytest.mark.requires_trino,
    pytest.mark.requires_clickhouse,
    pytest.mark.integration,
]

_TRINO_HOST = os.environ.get("TRINO_HOST", "localhost")
_TRINO_PORT = int(os.environ.get("TRINO_PORT", "8080"))
_CH_TABLE = "provisa_j303_widgets"
_PG_TABLE = "provisa_j303_prices"


def _cursor():
    deadline = time.monotonic() + 120
    while True:
        try:
            conn = trino.dbapi.connect(
                host=_TRINO_HOST, port=_TRINO_PORT, user="itest", catalog="system"
            )
            cur = conn.cursor()
            cur.execute("SELECT 1")
            cur.fetchall()
            return conn, cur
        except Exception:
            if time.monotonic() > deadline:
                raise
            time.sleep(2)


def _run(cur, sql):
    cur.execute(sql)
    return cur.fetchall()


def test_federated_join_through_trino_to_clickhouse():
    import clickhouse_connect

    from provisa.core.catalog import create_catalog
    from provisa.core.models import Source, SourceType

    ch_password = os.environ.get("CLICKHOUSE_PASSWORD", "")
    ch_user = os.environ.get("CLICKHOUSE_USER", "default")
    client = clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST", "localhost"),
        port=int(os.environ.get("CLICKHOUSE_PORT", "8123")),
        username=ch_user,
        password=ch_password,
    )
    client.command(f"DROP TABLE IF EXISTS {_CH_TABLE}")
    client.command(
        f"CREATE TABLE {_CH_TABLE} (id Int64, name String) ENGINE = MergeTree ORDER BY id"
    )
    client.command(f"INSERT INTO {_CH_TABLE} VALUES (1,'anvil'),(2,'bolt'),(3,'cog')")

    conn, cur = _cursor()
    ch_catalog, pg_catalog = "ch_j303", "pg_j303"
    try:
        # "clickhouse" is the compose service name; Trino resolves it inside its own network.
        create_catalog(
            conn,
            Source(
                id="ch-j303",
                type=SourceType.clickhouse,
                host="clickhouse",
                port=8123,
                database="default",
                username=ch_user,
                password=ch_password,
            ),
            "",
        )
        create_catalog(
            conn,
            Source(
                id="pg-j303",
                type=SourceType.postgresql,
                host="postgres",
                port=5432,
                database=os.environ["PG_DATABASE"],
                username=os.environ["PG_USER"],
                password=os.environ["PG_PASSWORD"],
            ),
            "",
        )
        assert ch_catalog in {r[0] for r in _run(cur, "SHOW CATALOGS")}

        # String reads as varchar, not varbinary (P3).
        types = {r[0]: r[1] for r in _run(cur, f"DESCRIBE {ch_catalog}.default.{_CH_TABLE}")}
        assert types["name"] == "varchar"

        _run(cur, f"DROP TABLE IF EXISTS {pg_catalog}.public.{_PG_TABLE}")
        _run(cur, f"CREATE TABLE {pg_catalog}.public.{_PG_TABLE} (id bigint, price integer)")
        _run(cur, f"INSERT INTO {pg_catalog}.public.{_PG_TABLE} VALUES (1,10),(2,20),(3,30)")

        rows = _run(
            cur,
            f"SELECT w.name, p.price FROM {ch_catalog}.default.{_CH_TABLE} w "
            f"JOIN {pg_catalog}.public.{_PG_TABLE} p ON w.id = p.id ORDER BY w.id",
        )
        assert rows == [["anvil", 10], ["bolt", 20], ["cog", 30]]
    finally:
        for stmt in (
            f"DROP TABLE IF EXISTS {pg_catalog}.public.{_PG_TABLE}",
            f"DROP CATALOG IF EXISTS {ch_catalog}",
            f"DROP CATALOG IF EXISTS {pg_catalog}",
        ):
            try:
                _run(cur, stmt)
            except Exception:
                pass
        conn.close()
        client.command(f"DROP TABLE IF EXISTS {_CH_TABLE}")
        client.close()
