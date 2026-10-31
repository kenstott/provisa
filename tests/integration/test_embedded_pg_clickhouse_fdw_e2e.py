# Copyright (c) 2026 Kenneth Stott
# Canary: 94dfdc1b-0afa-47d3-a413-b52776545e24
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration: the pg engine reads a live ClickHouse table at the port its source declares
(REQ-1870).

Drives PgFederationRuntime directly: an embedded Postgres with the product's FDW bundle staged, and
a real ClickHouse server self-provisioned by the ``requires_clickhouse`` marker, which publishes
only the HTTP interface, on a port leased for the run. So the read has one way to succeed: the
attach carries the declared port, over the driver that speaks HTTP.
"""

from __future__ import annotations

import datetime
import os
from types import SimpleNamespace

import pytest

pytestmark = [pytest.mark.integration, pytest.mark.requires_clickhouse]

clickhouse_connect = pytest.importorskip("clickhouse_connect")

from provisa.compiler.naming import live_view_schema  # noqa: E402
from provisa.federation.pg_runtime import PgFederationRuntime  # noqa: E402
from tests.embedded_pg import start_local_postgres  # noqa: E402

_TABLE = "itest_pg_fdw_events"
_EVENT_AT = datetime.datetime(2026, 1, 15, 8, 30, 0, tzinfo=datetime.UTC)


def _port() -> int:
    return int(os.environ["CLICKHOUSE_PORT"])


def _password() -> str:
    return os.environ.get("CLICKHOUSE_PASSWORD", "")


def _source() -> SimpleNamespace:
    return SimpleNamespace(
        id="ch-fdw-src",
        catalog="ch_fdw_src",
        type=SimpleNamespace(value="clickhouse"),
        host=os.environ.get("CLICKHOUSE_HOST", "localhost"),
        port=_port(),
        username=os.environ.get("CLICKHOUSE_USER", "default"),
        password=_password(),
        database="default",
        federation_hints={},
        schema_name="default",
        table_name=_TABLE,
    )


@pytest.fixture(scope="module")
def clickhouse_table():
    client = clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST", "localhost"),
        port=_port(),
        username=os.environ.get("CLICKHOUSE_USER", "default"),
        password=_password(),
    )
    client.command(f"DROP TABLE IF EXISTS {_TABLE}")
    client.command(
        f"CREATE TABLE {_TABLE} (id Int32, name String, at DateTime('America/New_York')) "
        "ENGINE = MergeTree ORDER BY id"
    )
    client.insert(
        _TABLE,
        [[1, "first", _EVENT_AT], [2, "second", _EVENT_AT], [3, "third", _EVENT_AT]],
        column_names=["id", "name", "at"],
    )
    yield
    client.command(f"DROP TABLE IF EXISTS {_TABLE}")
    client.close()


@pytest.fixture(scope="module")
def runtime():
    pg = start_local_postgres(fdws=True)
    rt = PgFederationRuntime(engine_dsn=pg.dsn())
    yield rt
    rt.close()
    pg.stop()


def test_the_pg_engine_reads_clickhouse_at_the_declared_port(clickhouse_table, runtime):
    source = _source()
    assert source.port not in (8123, 9000), "a default port would not prove the attach carries it"

    runtime.attach_source(source)

    view = f'"{live_view_schema(source.catalog, source.schema_name)}"."{_TABLE}"'
    assert runtime.run_sync(f"SELECT id, name FROM {view} ORDER BY id").rows == [
        (1, "first"),
        (2, "second"),
        (3, "third"),
    ]
    assert runtime.run_sync(f"SELECT count(*) FROM {view} WHERE id > 1").rows == [(2,)]
    # The instant survives the HTTP driver, in a column zoned other than the session.
    [(at,)] = runtime.run_sync(f"SELECT at FROM {view} WHERE id = 1").rows
    assert at == _EVENT_AT
