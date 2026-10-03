# Copyright (c) 2026 Kenneth Stott
# Canary: 2f8d4b61-9c37-4e15-a7b0-5e1c3d9a6f84
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A write to a table read from its whole-table replica asks for a build of the replica
(REQ-1915, REQ-1924).

The write is made through Provisa -- a table mutation or a command registered as writing the
table -- and the replica it leaves behind is out of date. A build is asked for with the reason
"write"; readers keep the old replica until the new one swaps in. A table read live, one
replicated row by row, and one with a parameter column have no whole replica to rebuild."""

from __future__ import annotations

from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest

from provisa.api.data import table_written
from provisa.federation import replica_state


def _column(name: str, native_filter_type: str | None = None) -> SimpleNamespace:
    return SimpleNamespace(name=name, native_filter_type=native_filter_type)


def _table(tid: int, *, parameterized: bool = False) -> SimpleNamespace:
    columns = [_column("id")]
    if parameterized:
        columns.append(_column("_nf_owner", "query_param"))
    return SimpleNamespace(
        id=tid,
        source_id="shop",
        schema_name="openapi",
        table_name=f"orders_{tid}",
        columns=columns,
        row_materialize=False,
    )


@pytest.fixture
def written(monkeypatch):
    """The build requests made, and the build passes started."""
    asked: list[tuple] = []
    kicked: list[str | None] = []

    async def _request_build(conn, key, reason, **_kw):
        asked.append((key, reason))
        return True

    async def _sources(_state):
        return [SimpleNamespace(id="shop", type=SimpleNamespace(value="openapi"))]

    async def _tables(_state):
        return [_table(1), _table(2, parameterized=True)]

    monkeypatch.setattr(replica_state, "request_build", _request_build)
    monkeypatch.setattr("provisa.federation.replica_builds.kick", kicked.append)
    monkeypatch.setattr("provisa.federation.registry_view.registered_sources", _sources)
    monkeypatch.setattr("provisa.federation.registry_view.registered_tables", _tables)
    return asked, kicked


def _state(*, attaches: bool, floored: frozenset[int] = frozenset()) -> SimpleNamespace:
    @asynccontextmanager
    async def _acquire():
        yield object()

    return SimpleNamespace(
        federation_engine=SimpleNamespace(attaches=attaches),
        tenant_db=SimpleNamespace(acquire=_acquire),
        replica_routes=SimpleNamespace(floored=floored),
    )


@pytest.fixture(autouse=True)
def _attach_rule(monkeypatch):
    monkeypatch.setattr(
        "provisa.federation.strategy.engine_attaches", lambda engine, _type: engine.attaches
    )


async def test_a_table_read_from_its_replica_asks_for_a_build_on_a_write(written):
    asked, kicked = written
    await table_written._request_replica_build(_state(attaches=False), 1, "shop")
    assert asked == [(("shop", "openapi", "orders_1"), replica_state.REASON_WRITE)]
    assert len(kicked) == 1


async def test_a_table_the_operator_puts_on_its_replica_is_rebuilt_too(written):
    asked, _ = written
    await table_written._request_replica_build(
        _state(attaches=True, floored=frozenset({1})), 1, "shop"
    )
    assert asked == [(("shop", "openapi", "orders_1"), replica_state.REASON_WRITE)]


async def test_a_table_read_live_has_no_replica_to_rebuild(written):
    asked, kicked = written
    await table_written._request_replica_build(_state(attaches=True), 1, "shop")
    assert asked == [] and kicked == []


async def test_a_parameterized_table_has_no_whole_replica_to_rebuild(written):
    asked, _ = written
    await table_written._request_replica_build(_state(attaches=False), 2, "shop")
    assert asked == []


async def test_a_built_in_source_is_never_landed(written):
    asked, _ = written
    await table_written._request_replica_build(_state(attaches=False), 1, "provisa-admin")
    assert asked == []


async def test_a_build_already_asked_for_is_joined_and_starts_no_pass(written, monkeypatch):
    asked, kicked = written

    async def _joined(conn, key, reason, **_kw):
        asked.append((key, reason))
        return False

    monkeypatch.setattr(replica_state, "request_build", _joined)
    await table_written._request_replica_build(_state(attaches=False), 1, "shop")
    assert len(asked) == 1 and kicked == []


def test_write_is_a_build_reason():
    assert replica_state.REASON_WRITE in replica_state.REASONS
