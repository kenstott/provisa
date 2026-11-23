# Copyright (c) 2026 Kenneth Stott
# Canary: 5d2f8e17-a6c3-4b90-8e41-0c7b3d9a2f64
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2176 (amended 2026-11-23, sessions cached per worker): each worker keeps the graph sessions
of the principals it serves and hears of every write of them on the control plane's channel
(LISTEN/NOTIFY). A read on a cached session touches no control-plane row; a write on another
worker is seen once its notification arrives; a worker that loses the channel drops its copies and
reads rows until it reconnects, and says so; retention, rights changes and clearing behave as
before.

Two workers are two handles on two engines of one control-plane database: each engine is heard
of on its own listening connection, as each worker process hears of it on its own.
"""

# Requirements: REQ-2176

from __future__ import annotations

import logging
import threading
import time
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import update

from provisa.api import graph_session
from provisa.api import graph_session_cache as cache
from provisa.compiler.temp_tables import TempTable
from provisa.core.schema_org import graph_sessions
from tests.unit.test_cypher_page_session import _in_thread, _row, _state

pytestmark = [pytest.mark.asyncio(loop_scope="session")]


def _worker(control_plane):
    state = _state((control_plane.org(holds="model"), control_plane.org(holds="state")))
    _until(lambda: cache.of(state.tenant_db).connected)
    return state


@pytest.fixture
def worker(control_plane):
    """One worker's state on a control plane."""
    return _worker(control_plane)


@pytest.fixture
def workers(control_plane):
    """Two workers' states on one control plane."""
    return _worker(control_plane), _worker(control_plane)


def _until(holds, timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    while not holds():
        assert time.monotonic() < deadline, "not so within the timeout"
        time.sleep(0.01)


def _key(state, user="ada"):
    return state.tenant_db.search_path, user


def _copy(state, user="ada"):
    return cache.of(state.tenant_db).get(_key(state, user))


async def _write(state, table: str, user="ada", claims=("analyst",)):
    """A statement making a table in ``user``'s session."""
    async with graph_session.session_for(state, user, list(claims), writes=True) as held:
        held.session.tables[table] = TempTable(table, [("a", "INTEGER")], {})
    return held


async def _read(state, user="ada", claims=("analyst",)) -> tuple[set[str], int]:
    """A statement with no write clause: the tables it sees and its generation."""
    async with graph_session.session_for(state, user, list(claims), writes=False) as held:
        return set(held.session.tables), held.session.generation


async def _cached(state, user="ada", claims=("analyst",)) -> tuple[set[str], int]:
    """:func:`_read`, again until the worker keeps a copy: a row read while the notification of
    the write before it is still on its way is not kept."""
    deadline = time.monotonic() + 10
    while True:
        seen = await _read(state, user, claims)
        if _copy(state, user) is not None:
            return seen
        assert time.monotonic() < deadline, "the worker kept no copy"
        time.sleep(0.01)


def _tables(*names: str) -> list[dict]:
    return [{"name": n, "columns": [["a", "INTEGER"]], "serials": {}} for n in names]


async def _set_row_unheard(state, *names: str) -> None:
    """The row's tables changed with no notification: only a worker reading the row sees it."""
    async with state.tenant_db.acquire() as conn:
        await conn.execute_core(
            update(graph_sessions)
            .where(graph_sessions.c.user_id == "ada")
            .values(tables=_tables(*names))
        )


def _no_control_plane(state, monkeypatch) -> None:
    @asynccontextmanager
    async def refused():
        if True:  # an async generator, so ``acquire`` is one
            raise AssertionError("a read on a cached session reached the control plane")
        yield

    monkeypatch.setattr(state.tenant_db, "acquire", refused)


# --- a read on a cached session ------------------------------------------------------------------


async def test_a_read_on_a_cached_session_touches_no_control_plane_row(workers, monkeypatch):
    one, _two = workers
    await _write(one, "t1")
    assert (await _cached(one))[0] == {"t1"}
    _no_control_plane(one, monkeypatch)
    for _ in range(3):
        assert (await _read(one))[0] == {"t1"}


async def test_a_read_on_a_cached_empty_session_touches_no_control_plane_row(workers, monkeypatch):
    one, _two = workers
    async with graph_session.session_for(one, "ada", ["analyst"], writes=True):
        pass  # the row exists, holding nothing
    await _cached(one)
    _no_control_plane(one, monkeypatch)
    assert (await _read(one))[0] == set()


async def test_a_write_on_this_worker_is_read_at_once(workers):
    one, _two = workers
    await _write(one, "t1")
    await _cached(one)
    held = await _write(one, "t2")
    assert held.changed
    # No notification awaited: the writing worker's own copy is dropped as the write commits.
    tables, generation = await _read(one)
    assert tables == {"t1", "t2"} and generation == (await _row(one_dbs(one))).generation


def one_dbs(state):
    return state.model_db, state.tenant_db


# --- a write on another worker -------------------------------------------------------------------


async def test_a_write_on_another_worker_is_seen_once_its_notification_arrives(workers):
    one, two = workers
    await _write(one, "t1")
    assert (await _cached(one))[0] == {"t1"}
    assert (await _cached(two))[0] == {"t1"}
    await _write(two, "t2")
    _until(lambda: _copy(one) is None)
    assert (await _read(one))[0] == {"t1", "t2"}
    assert (await _read(two))[0] == {"t1", "t2"}


async def _notified(state, generation: int | None) -> None:
    """A notification of a write of ada's row at ``generation``, as a write sends it."""
    sessions = cache.of(state.tenant_db)
    heard = sessions._heard.get(_key(state), 0)
    async with state.tenant_db.acquire() as conn:
        await conn.execute_core(cache.notify(state.tenant_db.search_path, "ada", generation))
    _until(lambda: sessions._heard.get(_key(state), 0) > heard)


async def test_a_notification_of_the_generation_a_copy_holds_keeps_it(workers):
    # A worker hears of its own write too, after its next statement read the row anew.
    one, _two = workers
    await _write(one, "t1")
    _, generation = await _cached(one)
    copied = _copy(one)
    await _notified(one, generation)
    assert _copy(one) is copied
    await _notified(one, generation + 1)
    assert _copy(one) is None


async def test_a_cached_read_does_not_wait_for_a_write_on_another_worker(workers):
    one, two = workers
    await _write(one, "t1")
    await _cached(one)
    order: list[str] = []
    writing = threading.Event()
    read_done = threading.Event()

    async def write_on_two():
        async with graph_session.session_for(two, "ada", ["analyst"], writes=True):
            order.append("write in")
            writing.set()
            read_done.wait(timeout=1.5)
            order.append("write out")

    async def read_on_one():
        writing.wait()
        await _read(one)
        order.append("read")
        read_done.set()

    threads = [_in_thread(write_on_two), _in_thread(read_on_one)]
    for t in threads:
        t.join(timeout=30)
    assert order == ["write in", "read", "write out"]


# --- the channel lost ----------------------------------------------------------------------------


async def test_a_worker_that_loses_the_channel_drops_its_copies_and_reads_rows_until_it_reconnects(
    worker, monkeypatch, caplog
):
    caplog.set_level(logging.INFO, logger=cache.__name__)
    one = worker
    sessions = cache.of(one.tenant_db)
    await _write(one, "t1")
    assert (await _cached(one))[0] == {"t1"}
    await _set_row_unheard(one, "t1", "t9")
    assert (await _read(one))[0] == {"t1"}, "the copy is read while the channel is heard"

    def refused():
        raise ConnectionError("control plane unreachable")

    monkeypatch.setattr(sessions, "_connect", refused)
    async with one.model_db.acquire() as conn:
        await conn.execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
            "WHERE application_name = $1 AND datname = current_database()",
            cache.APPLICATION_NAME,
        )
    _until(lambda: not sessions.connected)
    assert _copy(one) is None
    assert (await _read(one))[0] == {"t1", "t9"}
    await _set_row_unheard(one, "t1", "t8")
    assert (await _read(one))[0] == {"t1", "t8"}, "each statement reads the row"
    assert _copy(one) is None, "nothing is copied while the channel is lost"
    lost = [r for r in caplog.records if "graph-session channel lost" in r.getMessage()]
    assert len(lost) == 1 and lost[0].levelno == logging.WARNING

    monkeypatch.delattr(sessions, "_connect")
    _until(lambda: sessions.connected)
    assert any("heard again" in r.getMessage() for r in caplog.records)
    assert (await _read(one))[0] == {"t1", "t8"}
    assert _copy(one) is not None, "copies are kept again once it reconnects"


# --- rights, retention, clearing -----------------------------------------------------------------


async def test_a_change_of_rights_on_one_worker_ends_the_copy_on_the_other(workers):
    one, two = workers
    await _write(one, "t1")
    await _cached(one)
    await _cached(two)
    tables, generation = await _read(one, claims=("analyst", "auditor"))
    assert tables == set()

    def dropped() -> bool:
        copied = _copy(two)
        return copied is None or copied.generation == generation

    _until(dropped)
    assert await _read(two, claims=("analyst", "auditor")) == (set(), generation)


async def test_a_change_of_a_roles_grants_begins_a_cached_session_anew(workers):
    one, _two = workers
    await _write(one, "t1")
    _, before = await _cached(one)
    one.role_build_inputs = {
        **one.role_build_inputs,
        "tables": [
            {
                "id": 1,
                "domain_id": "sales",
                "columns": [
                    {"column_name": "name", "visible_to": ["analyst"]},
                    {"column_name": "salary", "visible_to": ["analyst"]},
                ],
            }
        ],
    }
    one.role_build_inputs.pop("graph_session_role_prints", None)
    tables, after = await _read(one)
    assert tables == set() and after > before


async def test_a_session_past_its_retention_swept_on_one_worker_ends_on_the_other(workers):
    one, two = workers
    await _write(one, "t1")
    _, before = await _cached(one)
    assert await graph_session.sweep(two, datetime.now(UTC) + timedelta(days=8)) == ["ada"]
    _until(lambda: _copy(one) is None)
    tables, after = await _read(one)
    assert tables == set() and after > before


async def test_a_session_read_from_its_copy_is_marked_used(workers):
    one, _two = workers
    await _write(one, "t1")
    await _cached(one)
    stale = datetime.now(UTC) - timedelta(hours=2)
    async with one.tenant_db.acquire() as conn:
        await conn.execute_core(
            update(graph_sessions).where(graph_sessions.c.user_id == "ada").values(used_at=stale)
        )
    # Unheard: the copy still holds the mark it read, fresh, and renews nothing.
    await _read(one)
    assert (await _row(one_dbs(one))).used_at == stale
    # A copy whose mark is stale renews it, as the row read renews it.
    copied = _copy(one)
    assert copied is not None
    cache.of(one.tenant_db).renewed(_key(one), copied, stale)
    await _read(one)
    assert (await _row(one_dbs(one))).used_at > stale + timedelta(hours=1)


async def test_a_clear_on_one_worker_ends_the_copy_on_the_other(workers):
    one, two = workers
    await _write(one, "t1")
    _, before = await _cached(one)
    await graph_session.clear(two, "ada")
    _until(lambda: _copy(one) is None)
    tables, after = await _read(one)
    assert tables == set() and after > before
