# Copyright (c) 2026 Kenneth Stott
# Canary: 0e2f5150-eb07-4a1d-b624-14e3322048c6
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The commit time a change-data-capture landing carries, source_committed_at (REQ-1163).

A table on the debezium signal, or a PostgreSQL table on the native signal, is registered with it;
a Debezium change lands it from the envelope's source.ts_ms; a build reads PostgreSQL's commit
time of each row through the source's own driver where the source keeps commit timestamps, and
leaves it empty elsewhere; the definition hash follows the source's setting."""

# Requirements: REQ-1163

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from provisa.federation import commit_time
from provisa.federation.commit_time import SOURCE_COMMITTED_AT, carries, with_column


@pytest.fixture(autouse=True)
def _fresh_probe(monkeypatch):
    monkeypatch.setattr(commit_time, "_probed", {})


def test_which_tables_carry_it():
    assert carries("postgresql", "debezium") and carries("mysql", "debezium")
    assert carries("postgresql", "native")
    assert not carries("mongodb", "native")  # a change stream: no commit time read here
    assert not carries("postgresql", "kafka")  # a plain delta topic carries none
    assert not carries("kafka", "debezium")  # a push source is not captured
    assert not carries("postgresql", None)


def test_it_is_granted_as_the_key_and_added_once():
    cols = [{"name": "id", "is_primary_key": True, "visible_to": ["a"]}, {"name": "v"}]
    added = with_column(cols)
    assert added[-1]["name"] == SOURCE_COMMITTED_AT and added[-1]["visible_to"] == ["a"]
    assert with_column(added) == added
    assert with_column([{"name": "v"}]) == [{"name": "v"}]  # no key, no landing by key


def test_a_config_table_on_a_capture_signal_is_registered_with_it():
    from provisa.core.config_loader import commit_time_columns

    def table(name, signal):
        return {
            "source_id": "pg",
            "table": name,
            "change_signal": signal,
            "columns": [{"name": "id", "is_primary_key": True, "visible_to": ["a"]}],
        }

    raw = {
        "sources": [{"id": "pg", "type": "postgresql"}],
        "tables": [table("cdc", "debezium"), table("feed", "native"), table("plain", None)],
    }
    tables = {
        t["table"]: [c["name"] for c in t["columns"]] for t in commit_time_columns(raw)["tables"]
    }
    assert tables == {
        "cdc": ["id", SOURCE_COMMITTED_AT],
        "feed": ["id", SOURCE_COMMITTED_AT],
        "plain": ["id"],
    }


def test_a_debezium_change_carries_the_source_commit_time_not_the_connectors():
    from provisa.subscriptions.debezium_provider import DebeziumNotificationProvider

    provider = DebeziumNotificationProvider(
        bootstrap_servers="localhost:9092",
        topic_prefix="db",
        database="d",
        consumer_group_id="g",
    )
    envelope = {
        "payload": {
            "op": "u",
            "after": {"id": 1},
            "ts_ms": 1_800_000_000_000,  # the connector's processing time
            "source": {"ts_ms": 1_700_000_000_000},  # the database commit
        }
    }
    event = provider._extract_event(envelope, "t")
    assert event is not None
    assert event.committed_at == datetime.fromtimestamp(1_700_000_000, tz=timezone.utc)
    del envelope["payload"]["source"]
    event = provider._extract_event(envelope, "t")
    assert event is not None and event.committed_at is None


def _pg(sid="pg"):
    return SimpleNamespace(id=sid, type=SimpleNamespace(value="postgresql"))


def _table(*names):
    return SimpleNamespace(
        schema_name="public",
        table_name="orders",
        landing_filter=None,
        columns=[SimpleNamespace(name=n, native_type=None) for n in names],
    )


class _Pools:
    def has(self, _sid):
        return True

    def supports_stream(self, _sid):
        return True


def _reads(monkeypatch, table, *, keeps: bool) -> str:
    """The SQL a build of ``table`` reads its rows with, the source keeping commit timestamps
    as ``keeps`` says."""
    from provisa.events.source_loader import SourceRowLoader

    monkeypatch.setattr(commit_time, "tracks_commit_time", lambda _s: keeps)
    seen: list[str] = []
    monkeypatch.setattr(
        "provisa.executor.direct.open_direct_stream", lambda _p, _sid, sql, _a: seen.append(sql)
    )
    state = SimpleNamespace(source_pools=_Pools(), source_dialects={"pg": "postgres"})
    engine = SimpleNamespace(dialect="duckdb")
    source = SourceRowLoader(engine).replica_source(state, _pg(), table, [])
    if hasattr(source, "_projection"):  # read through the engine
        return f"SELECT {source._projection} FROM {source._ref}"
    source._open()
    return seen[0]


def test_a_build_reads_postgresqls_commit_time_through_the_sources_own_driver(monkeypatch):
    sql = _reads(monkeypatch, _table("id", SOURCE_COMMITTED_AT), keeps=True)
    assert f"pg_xact_commit_timestamp(xmin) AS {SOURCE_COMMITTED_AT}" in sql
    assert sql.startswith("SELECT *, ")


def test_a_build_leaves_it_empty_when_the_source_keeps_no_commit_time(monkeypatch):
    sql = _reads(monkeypatch, _table("id", SOURCE_COMMITTED_AT), keeps=False)
    assert f"CAST(NULL AS TIMESTAMP) AS {SOURCE_COMMITTED_AT}" in sql
    assert "xmin" not in sql


def test_a_table_carrying_none_is_read_as_it_was(monkeypatch):
    sql = _reads(monkeypatch, _table("id"), keeps=True)
    assert SOURCE_COMMITTED_AT not in sql


def test_the_definition_follows_whether_the_source_keeps_commit_time(monkeypatch):
    from provisa.federation.replica_converge import definition_hash

    address = SimpleNamespace(schema="s", table="t")
    carrying = [("id", "bigint"), (SOURCE_COMMITTED_AT, "timestamp")]
    monkeypatch.setattr(commit_time, "tracks_commit_time", lambda _s: True)
    on = definition_hash(_pg(), address, carrying, ["id"], landing_filter=None)
    plain_on = definition_hash(_pg(), address, [("id", "bigint")], ["id"], landing_filter=None)
    monkeypatch.setattr(commit_time, "tracks_commit_time", lambda _s: False)
    off = definition_hash(_pg(), address, carrying, ["id"], landing_filter=None)
    plain_off = definition_hash(_pg(), address, [("id", "bigint")], ["id"], landing_filter=None)
    assert on != off
    assert plain_on == plain_off  # a table carrying none keeps the digest it had


def test_the_probe_is_asked_once_per_interval(monkeypatch):
    asked: list[str] = []

    class _Conn:
        def __enter__(self):
            return self

        def __exit__(self, *_a):
            return False

        def execute(self, sql):
            asked.append(sql)
            return SimpleNamespace(fetchone=lambda: ("on",))

    monkeypatch.setattr(
        "provisa.subscriptions.pg_logical.PgConnection.from_source",
        lambda _s: SimpleNamespace(connect=lambda: _Conn()),
    )
    assert commit_time.tracks_commit_time(_pg()) and commit_time.tracks_commit_time(_pg())
    assert asked == ["SELECT current_setting('track_commit_timestamp')"]


@pytest.mark.asyncio
async def test_an_admin_save_of_a_captured_table_registers_it(monkeypatch):
    from provisa.api.admin import _table_ops
    from provisa.core.models import Column

    class _Acquire:
        async def __aenter__(self):
            return None

        async def __aexit__(self, *_a):
            return False

    async def _get(_conn, _sid):
        return {"type": "postgresql", "change_signal": "debezium"}

    monkeypatch.setattr("provisa.core.repositories.source.get", _get)
    pool = SimpleNamespace(acquire=lambda: _Acquire())
    given = [Column(name="id", visible_to=["a"], is_primary_key=True)]
    saved = SimpleNamespace(source_id="pg", change_signal=None)
    [added] = await _table_ops._commit_time_column(pool, saved, given)
    assert (added.name, added.visible_to) == (SOURCE_COMMITTED_AT, ["a"])
    plain = SimpleNamespace(source_id="pg", change_signal="poll")
    assert await _table_ops._commit_time_column(pool, plain, given) == []
