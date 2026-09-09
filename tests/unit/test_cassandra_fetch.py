# Copyright (c) 2026 Kenneth Stott
# Canary: 8cadd6c9-6172-4a95-b73f-dc9cb46221b2
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Cassandra over CQL, engine-independently (REQ-1676): keyspace/table listing, typed columns from
the cluster metadata, the paged read, the loader, and the engine-gated wiring — over a fake
cluster, so no node and no network."""

from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace

import pytest

from provisa.cassandra import fetch as cf


class _Row:
    def __init__(self, **kw):
        self.__dict__.update(kw)


class _Session:
    def __init__(self, rows):
        self._rows = rows
        self.statements: list[str] = []

    def execute(self, stmt):
        self.statements.append(stmt.query_string)
        return iter(self._rows)


class _Cluster:
    def __init__(self, rows):
        col = lambda n, t: SimpleNamespace(name=n, cql_type=t)  # noqa: E731
        tbl = SimpleNamespace(
            columns={
                "event_id": col("event_id", "int"),
                "tags": col("tags", "list<text>"),
                "name": col("name", "text"),
            },
            partition_key=[col("event_id", "int")],
            clustering_key=[],
        )
        self.metadata = SimpleNamespace(
            keyspaces={
                "system": SimpleNamespace(tables={}),
                "system_schema": SimpleNamespace(tables={}),
                "shelter_ops": SimpleNamespace(tables={"intake_events": tbl, "aaa": tbl}),
            }
        )
        self.session = _Session(rows)

    def connect(self):
        return self.session


@pytest.fixture
def fake_cluster(monkeypatch):
    cluster = _Cluster(rows=[_Row(event_id=1, name="Buddy"), _Row(event_id=2, name="Rex")])

    @contextmanager
    def _cluster(self):
        yield cluster

    monkeypatch.setattr(cf.CassandraConnection, "cluster", _cluster)
    return cluster


def _conn() -> cf.CassandraConnection:
    return cf.CassandraConnection.build("localhost", 9042)


def test_build_splits_contact_points_and_gates_auth():
    c = cf.CassandraConnection.build("a, b", 1, username="u", password="p")
    assert c.contact_points == ("a", "b") and c.username == "u" and c.password == "p"
    assert cf.CassandraConnection.build("h", 1, username="u").password is None


def test_keyspaces_and_tables_skip_system_keyspaces(fake_cluster):
    assert cf.list_keyspaces(_conn()) == ["shelter_ops"]
    assert cf.list_tables(_conn(), "shelter_ops") == ["aaa", "intake_events"]
    with pytest.raises(ValueError, match="does not exist"):
        cf.list_tables(_conn(), "nope")


def test_columns_are_typed_from_the_metadata_with_partition_keys_marked(fake_cluster):
    cols = cf.table_columns(_conn(), "shelter_ops", "intake_events")
    assert [(c["name"], c["type"], c.get("partitionKey", False)) for c in cols] == [
        ("event_id", "INTEGER", True),
        ("tags", "VARCHAR", False),
        ("name", "VARCHAR", False),
    ]


def test_fetch_rows_projects_the_columns_and_addresses_keyspace_table(fake_cluster):
    rows = cf.fetch_rows(_conn(), "shelter_ops", "intake_events", ["event_id", "name"])
    assert rows == [{"event_id": 1, "name": "Buddy"}, {"event_id": 2, "name": "Rex"}]
    assert fake_cluster.session.statements == [
        'SELECT "event_id", "name" FROM "shelter_ops"."intake_events"'
    ]


@pytest.mark.asyncio
async def test_loader_reads_the_registered_columns_of_keyspace_table(fake_cluster):
    from provisa.events.source_loader import make_cassandra_loader

    source = SimpleNamespace(id="c", host="localhost", port=9042, username="", password="")
    table = SimpleNamespace(
        schema_name="shelter_ops",
        table_name="intake_events",
        columns=[SimpleNamespace(name=n, native_filter_type=None) for n in ("event_id", "name")],
    )
    assert await make_cassandra_loader()(source, table) == [
        {"event_id": 1, "name": "Buddy"},
        {"event_id": 2, "name": "Rex"},
    ]


def test_loader_is_wired_only_when_the_engine_does_not_read_cassandra_live():
    from provisa.events.app_wiring import build_adapter_loaders
    from provisa.federation.connector import Mechanism

    state = SimpleNamespace(config=SimpleNamespace(sources=[]))
    land = SimpleNamespace(
        engine=SimpleNamespace(
            connectors={
                "cassandra": SimpleNamespace(mechanism=Mechanism.FETCH, reads_in_place=False)
            }
        )
    )
    assert "cassandra" in build_adapter_loaders(state, land)
    live = SimpleNamespace(
        engine=SimpleNamespace(
            connectors={
                "cassandra": SimpleNamespace(mechanism=Mechanism.ATTACH_R, reads_in_place=True)
            }
        )
    )
    assert "cassandra" not in build_adapter_loaders(state, live)
