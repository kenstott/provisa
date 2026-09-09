# Copyright (c) 2026 Kenneth Stott
# Canary: 06a03316-8520-41b2-8313-ed1ba88e8d71
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Redis read natively against the stack's live server (REQ-1675): what Register Table lists and
types on a native engine, and the rows the landing loader produces — no Trino anywhere."""

from __future__ import annotations

import os
from types import SimpleNamespace

import pytest
import redis

from provisa.events.source_loader import make_redis_loader
from provisa.redis import fetch as rf

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

_PREFIX = "native_fetch_agent"
_AGENTS = [("1", "Ann", "tier1"), ("2", "Bo", "tier2"), ("3", "Cy", "tier1")]


def _port() -> int:
    return int(os.environ["REDIS_PORT"])


@pytest.fixture(autouse=True)
def _seed():
    c = redis.Redis(host="localhost", port=_port(), decode_responses=True)
    for k in c.scan_iter(match=f"{_PREFIX}:*"):
        c.delete(k)
    for agent_id, name, team in _AGENTS:
        c.hset(f"{_PREFIX}:{agent_id}", mapping={"agent_id": agent_id, "name": name, "team": team})
    yield
    for k in c.scan_iter(match=f"{_PREFIX}:*"):
        c.delete(k)


async def test_prefix_lists_as_a_table_with_its_fields_typed():
    conn = rf.RedisConnection(host="localhost", port=_port())
    assert _PREFIX in rf.list_prefixes(conn)
    assert rf.prefix_columns(conn, {}, _PREFIX) == [
        ("key", "varchar"),
        ("agent_id", "varchar"),
        ("name", "varchar"),
        ("team", "varchar"),
    ]


async def test_loader_lands_every_key_of_the_prefix():
    source = SimpleNamespace(id="r", host="localhost", port=_port(), password="", mapping={})
    table = SimpleNamespace(
        table_name=_PREFIX,
        columns=[
            SimpleNamespace(name=n, native_filter_type=None) for n in ("agent_id", "name", "team")
        ],
    )
    rows = await make_redis_loader()(source, table)
    assert sorted((r["agent_id"], r["name"], r["team"]) for r in rows) == _AGENTS
