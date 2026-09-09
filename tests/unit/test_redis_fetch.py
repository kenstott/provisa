# Copyright (c) 2026 Kenneth Stott
# Canary: 735934e0-06c6-4fb9-9489-eaacc819f866
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Redis as a table source, engine-independently (REQ-1675): prefix listing, column typing from
hashes and from the mapping DSL, the row read, the loader, and the engine-gated wiring."""

from __future__ import annotations

from types import SimpleNamespace

import fakeredis
import pytest

from provisa.redis import fetch as rf


@pytest.fixture
def server(monkeypatch):
    srv = fakeredis.FakeServer()

    def _client(self):
        return fakeredis.FakeRedis(server=srv, decode_responses=True)

    monkeypatch.setattr(rf.RedisConnection, "client", _client)
    c = fakeredis.FakeRedis(server=srv, decode_responses=True)
    c.hset("support_agent:1", mapping={"agent_id": "1", "name": "Ann", "team": "tier1"})
    c.hset(
        "support_agent:2",
        mapping={"agent_id": "2", "name": "Bo", "team": "tier2", "shift": "night"},
    )
    c.set("greeting:hello", "world")
    c.hset("provisa:cache:x", mapping={"k": "v"})  # Provisa's own entry: never a table
    c.set("loose_key", "1")  # no prefix: never a table
    return srv


def _conn() -> rf.RedisConnection:
    return rf.RedisConnection(host="h", port=1)


def test_prefixes_are_the_tables_and_own_entries_stay_out(server):
    assert rf.list_prefixes(_conn()) == ["greeting", "support_agent"]


def test_hash_prefix_columns_are_the_key_plus_the_union_of_fields(server):
    assert rf.prefix_columns(_conn(), {}, "support_agent") == [
        ("key", "varchar"),
        ("agent_id", "varchar"),
        ("name", "varchar"),
        ("team", "varchar"),
        ("shift", "varchar"),
    ]


def test_mapping_dsl_declares_pattern_key_column_and_columns(server):
    mapping = {
        "tables": [
            {
                "name": "agents",
                "key_pattern": "support_agent:*",
                "key_column": "redis_key",
                "value_type": "hash",
                "columns": [
                    {"name": "agent", "data_type": "VARCHAR", "field": "name"},
                    {"name": "team", "data_type": "VARCHAR"},
                ],
            }
        ]
    }
    assert rf.table_spec(mapping, "agents") == ("support_agent:*", "redis_key", "hash")
    assert rf.prefix_columns(_conn(), mapping, "agents") == [
        ("redis_key", "varchar"),
        ("agent", "varchar"),
        ("team", "varchar"),
    ]
    rows = rf.fetch_rows(_conn(), mapping, "agents", ["redis_key", "agent", "team"])
    assert rows == [
        {"redis_key": "support_agent:1", "agent": "Ann", "team": "tier1"},
        {"redis_key": "support_agent:2", "agent": "Bo", "team": "tier2"},
    ]


def test_rows_of_a_discovered_hash_prefix_and_of_a_string_prefix(server):
    rows = rf.fetch_rows(_conn(), {}, "support_agent", ["key", "name", "shift"])
    assert rows == [
        {"key": "support_agent:1", "name": "Ann", "shift": None},
        {"key": "support_agent:2", "name": "Bo", "shift": "night"},
    ]
    mapping = {"tables": [{"name": "greeting", "value_type": "string"}]}
    assert rf.prefix_columns(_conn(), mapping, "greeting") == [
        ("key", "varchar"),
        ("value", "varchar"),
    ]
    assert rf.fetch_rows(_conn(), mapping, "greeting", ["key", "value"]) == [
        {"key": "greeting:hello", "value": "world"}
    ]


def test_an_unreadable_value_type_is_refused(server):
    mapping = {"tables": [{"name": "q", "value_type": "list"}]}
    with pytest.raises(ValueError, match="needs declared columns"):
        rf.prefix_columns(_conn(), mapping, "q")
    with pytest.raises(ValueError, match="not readable natively"):
        rf.fetch_rows(_conn(), mapping, "q", ["key", "value"])


@pytest.mark.asyncio
async def test_loader_reads_the_registered_columns(server):
    from provisa.events.source_loader import make_redis_loader

    source = SimpleNamespace(id="r", host="h", port=1, password="", mapping={})
    table = SimpleNamespace(
        table_name="support_agent",
        columns=[
            SimpleNamespace(name=n, native_filter_type=None) for n in ("key", "agent_id", "team")
        ],
    )
    rows = await make_redis_loader()(source, table)
    assert [(r["key"], r["agent_id"], r["team"]) for r in rows] == [
        ("support_agent:1", "1", "tier1"),
        ("support_agent:2", "2", "tier2"),
    ]


def test_loader_is_wired_only_when_the_engine_does_not_read_redis_live():
    from provisa.events.app_wiring import build_adapter_loaders
    from provisa.federation.connector import Mechanism

    state = SimpleNamespace(config=SimpleNamespace(sources=[]))
    land = SimpleNamespace(
        engine=SimpleNamespace(
            connectors={"redis": SimpleNamespace(mechanism=Mechanism.FETCH, reads_in_place=False)}
        )
    )
    assert "redis" in build_adapter_loaders(state, land)
    live = SimpleNamespace(
        engine=SimpleNamespace(
            connectors={"redis": SimpleNamespace(mechanism=Mechanism.ATTACH_R, reads_in_place=True)}
        )
    )
    assert "redis" not in build_adapter_loaders(state, live)
