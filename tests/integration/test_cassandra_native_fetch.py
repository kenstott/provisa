# Copyright (c) 2026 Kenneth Stott
# Canary: aa2f66b9-ebe3-4505-bed7-99ce340d091f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Cassandra read natively against a live node (REQ-1676): what Register Table lists and types on
a native engine, and the rows the landing loader produces — no Trino anywhere."""

from __future__ import annotations

import os
from types import SimpleNamespace

import pytest

from provisa.cassandra import fetch as cf
from provisa.events.source_loader import make_cassandra_loader

pytestmark = [
    pytest.mark.integration,
    pytest.mark.requires_cassandra,
    pytest.mark.asyncio(loop_scope="session"),
]

_KS = "native_fetch_ks"
_TABLE = "events"
_ROWS = [(1, "intake", "Buddy"), (2, "vet_visit", "Buddy"), (3, "intake", "Rex")]


def _port() -> int:
    return int(os.environ["CASSANDRA_PORT"])


@pytest.fixture(autouse=True)
def _seed():
    from cassandra.cluster import Cluster

    cluster = Cluster(contact_points=["localhost"], port=_port(), connect_timeout=30)
    session = cluster.connect()
    session.execute(
        f"CREATE KEYSPACE IF NOT EXISTS {_KS} "
        "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}"
    )
    session.execute(
        f"CREATE TABLE IF NOT EXISTS {_KS}.{_TABLE} (event_id int PRIMARY KEY, event_type text, animal text)"
    )
    session.execute(f"TRUNCATE {_KS}.{_TABLE}")
    for row in _ROWS:
        session.execute(
            f"INSERT INTO {_KS}.{_TABLE} (event_id, event_type, animal) VALUES (%s, %s, %s)", row
        )
    cluster.shutdown()
    yield


async def test_keyspace_table_and_typed_columns_come_from_the_node():
    conn = cf.CassandraConnection.build("localhost", _port())
    assert _KS in cf.list_keyspaces(conn)
    assert _TABLE in cf.list_tables(conn, _KS)
    cols = {
        c["name"]: (c["type"], c.get("partitionKey", False))
        for c in cf.table_columns(conn, _KS, _TABLE)
    }
    assert cols["event_id"] == ("INTEGER", True)
    assert cols["animal"] == ("VARCHAR", False)


async def test_loader_lands_every_row():
    source = SimpleNamespace(id="c", host="localhost", port=_port(), username="", password="")
    table = SimpleNamespace(
        schema_name=_KS,
        table_name=_TABLE,
        columns=[
            SimpleNamespace(name=n, native_filter_type=None)
            for n in ("event_id", "event_type", "animal")
        ],
    )
    rows = await make_cassandra_loader()(source, table)
    assert sorted((r["event_id"], r["event_type"], r["animal"]) for r in rows) == _ROWS
