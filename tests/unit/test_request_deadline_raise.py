# Copyright (c) 2026 Kenneth Stott
# Canary: 5d2f8a1c-7b46-4e93-a0c8-3f6e9d1b4a75
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""At expiry the watchdog raises the timeout in the request's own thread (REQ-1905).

Inline work — rows being shaped, a response being encoded — is not a driver call a cancel can
reach, so the request's thread is ended where it is. The known hazard of raising into a thread is
where it lands: these tests aim at that. A timeout that lands while the thread holds a lock
leaves the lock released; one that lands in, or at the door of, a release section leaves the
connection returned; and nothing is raised once the request's scope has ended."""

# Requirements: REQ-1905, REQ-1882

from __future__ import annotations

import gc
import random
import threading
import time

import pytest
import sqlalchemy as sa

from provisa.core import request_deadline
from provisa.core.database import bounded_connection
from provisa.core.request_deadline import DeadlinePassed, RequestTimedOut
from provisa.core.sync_pool import BlockingPool


@pytest.fixture(autouse=True)
def _timeouts(monkeypatch):
    budget = {"s": 0.15}
    monkeypatch.setattr("provisa.core.limits.request_timeout_for", lambda transport: budget["s"])
    monkeypatch.setattr(
        "provisa.core.limits.request_timeout_setting",
        lambda transport: f"limits.request_timeouts.{transport}",
    )
    return budget


def _spin(seconds: float) -> int:
    """Inline Python work: no driver call, nothing a cancel reaches."""
    n = 0
    until = time.monotonic() + seconds
    while time.monotonic() < until:
        n += 1
    return n


def _quiet(seconds: float = 0.6) -> None:
    """Keep executing bytecode: a raise still pending for this thread would land here."""
    _spin(seconds)


class _Conn:
    def __init__(self) -> None:
        self.closed = False


def _pool(size: int = 1) -> tuple[BlockingPool, list[_Conn]]:
    made: list[_Conn] = []

    def _create() -> _Conn:
        made.append(_Conn())
        return made[-1]

    def _close(conn: _Conn) -> None:
        conn.closed = True

    return BlockingPool(_create, _close, minsize=1, maxsize=size, wait_s=0.5, name="t"), made


def _free(pool: BlockingPool) -> bool:
    """Whether a connection can be borrowed at once (none is leaked, no slot is stuck)."""
    conn = pool.getconn()
    pool.putconn(conn)
    return True


# --- inline work is ended ----------------------------------------------------------------------


def test_inline_work_is_ended_at_the_deadline_naming_the_transport_and_setting():
    started = time.monotonic()
    with pytest.raises(RequestTimedOut) as raised:
        with request_deadline.request("graphql"):
            _spin(10.0)  # shaping six million rows
    elapsed = time.monotonic() - started
    assert 0.1 < elapsed < 1.0, elapsed
    assert raised.value.transport == "graphql"
    assert raised.value.setting == "limits.request_timeouts.graphql"
    assert isinstance(raised.value.__cause__, DeadlinePassed)
    _quiet()


def test_a_raise_that_is_swallowed_does_not_let_the_request_run_on():
    swallowed = 0
    started = time.monotonic()
    with pytest.raises(RequestTimedOut):
        with request_deadline.request("rest"):
            try:
                _spin(10.0)
            except Exception:  # a broad handler in the request's path eats the first raise
                swallowed += 1
            _spin(10.0)  # ... and the request goes on working
    assert swallowed == 1
    assert time.monotonic() - started < 2.0
    _quiet()


def test_nothing_is_raised_once_the_scope_has_ended():
    for _ in range(20):
        with pytest.raises(RequestTimedOut):
            with request_deadline.request("sql_http"):
                _spin(10.0)
        assert request_deadline.current() is None
    _quiet(1.0)


def test_a_request_that_finishes_in_time_is_never_raised_in(_timeouts):
    _timeouts["s"] = 0.3
    with request_deadline.request("graphql"):
        _spin(0.05)
    _quiet(0.8)  # past where its deadline would have been


def test_the_tighter_budget_inside_a_request_is_what_ends_it(_timeouts):
    _timeouts["s"] = 30.0
    started = time.monotonic()
    with pytest.raises(TimeoutError, match="0.1s budget"):
        with request_deadline.request("graphql"):
            with request_deadline.within(0.1):
                _spin(10.0)
    assert time.monotonic() - started < 1.0
    _quiet()


# --- where it lands: locks ---------------------------------------------------------------------


def test_a_timeout_that_lands_while_the_thread_holds_a_lock_leaves_it_released():
    lock = threading.Lock()
    rlock = threading.RLock()
    cond = threading.Condition()
    with pytest.raises(RequestTimedOut):
        with request_deadline.request("cypher_http"):
            with lock, rlock, cond:
                _spin(10.0)
    assert not lock.locked()
    assert rlock.acquire(blocking=False)
    rlock.release()
    assert cond.acquire(blocking=False)
    cond.release()
    _quiet()


# --- where it lands: release sections ----------------------------------------------------------


def test_a_timeout_during_borrowed_work_returns_the_connection():
    pool, made = _pool()
    with pytest.raises(RequestTimedOut):
        with request_deadline.request("rest"):
            with pool.connection(is_broken=lambda exc: False):
                _spin(10.0)
    assert _free(pool)
    # Its work was cut at an arbitrary point: that connection is closed, a new one serves next.
    assert [c.closed for c in made] == [True, False]
    _quiet()


def test_a_release_section_is_not_interrupted_and_the_timeout_follows_it():
    """The deadline passes while the thread is inside a release section that takes far longer
    than the timeout. The section runs to its end; the timeout is raised after it."""
    steps: list[str] = []
    shield = request_deadline.shielded()
    started = time.monotonic()
    with pytest.raises(RequestTimedOut):
        with request_deadline.request("bolt"):
            try:
                steps.append("work")
            finally:
                with shield.lock:
                    shield.settle()
                    _spin(0.6)  # four timeouts long
                    steps.append("released")
            steps.append("after the section")
            _spin(10.0)
    assert steps[:2] == ["work", "released"]
    assert 0.6 <= time.monotonic() - started < 1.6
    _quiet()


def test_a_raise_set_just_before_a_release_section_does_not_land_inside_it():
    """The watchdog holds the thread's shield and sets its raise at the very moment the thread
    is entering a release section (blocked on that lock). The thread gets the lock with the
    raise still undelivered; entering settles it, and the section is not interrupted."""
    shield = request_deadline.shielded()
    holding = threading.Event()

    def _watchdog_at_the_worst_moment() -> None:
        with shield.lock:
            holding.set()
            time.sleep(0.2)  # the request thread is now blocked entering its release section
            shield.raise_now()

    watchdog = threading.Thread(target=_watchdog_at_the_worst_moment)
    watchdog.start()
    done = []
    holding.wait(5)
    with shield.lock:
        shield.settle()
        _spin(0.1)
        done.append(True)
    watchdog.join(5)
    assert done == [True]
    _quiet(0.3)


def test_without_settling_that_raise_would_land_inside_the_section():
    """The same moment without ``settle()``: the raise lands in the release code. This is the
    hazard the two-step entry exists for."""
    shield = request_deadline.shielded()
    holding = threading.Event()

    def _watchdog_at_the_worst_moment() -> None:
        with shield.lock:
            holding.set()
            time.sleep(0.2)
            shield.raise_now()

    watchdog = threading.Thread(target=_watchdog_at_the_worst_moment)
    watchdog.start()
    holding.wait(5)
    with pytest.raises(DeadlinePassed):
        with shield.lock:
            _spin(0.1)
    watchdog.join(5)
    _quiet(0.3)


def test_timeouts_landing_anywhere_leave_no_lock_held_and_no_connection_out(_timeouts):
    """Three hundred requests, each timing out at a different instant of the same work: borrow
    a connection, take a lock, shape rows, give both back. Wherever each raise lands — in the
    work, at the door of the release, in the pool's own code — the lock is free and the pool
    whole afterwards.

    The cyclic garbage collector is off for the test: a borrow abandoned at a context manager's
    doorstep must be given back when the request's scope ends, not whenever the collector next
    runs (an exception thrown through a generator-based context manager is always in a cycle)."""
    pool, made = _pool(size=2)
    lock = threading.Lock()
    rng = random.Random(1905)
    timed_out = 0
    gc.disable()
    try:
        timed_out = _three_hundred_timeouts(_timeouts, pool, lock, rng)
    finally:
        gc.enable()
    assert timed_out == 300
    assert sum(1 for c in made if not c.closed) <= 2
    _quiet(1.0)


def _three_hundred_timeouts(_timeouts, pool, lock, rng) -> int:
    timed_out = 0
    for _ in range(300):
        _timeouts["s"] = rng.uniform(0.0005, 0.006)
        try:
            with request_deadline.request("graphql"):
                for _ in range(1000):
                    with pool.connection(is_broken=lambda exc: False):
                        with lock:
                            _spin(0.0002)
        except RequestTimedOut:
            timed_out += 1
        assert request_deadline.current() is None
        assert not lock.locked()
        first, second = pool.getconn(), pool.getconn()  # both slots are free
        pool.putconn(first)
        pool.putconn(second)
    return timed_out


@pytest.mark.parametrize("scope", ["request", "within", "bound"])
def test_a_deadline_that_passed_before_its_scope_was_entered_ends_in_the_timeout(
    monkeypatch, _timeouts, scope
):
    """A loaded machine can hold the request's thread between creating its deadline and entering
    its scope for longer than the budget, so the watchdog has already acted on the expiry when
    the thread enters. The raise that follows at once must land inside the scope — ending in the
    timeout, with the scope left — never in the door of the scope (entering it, or between
    entering it and the block), where it would escape as a bare ``DeadlinePassed`` and leave the
    thread inside the scope, raised into from then on."""
    enter = request_deadline.Deadline._enter

    def _held_by_load(dl, shield):
        until = time.monotonic() + 5.0
        while not dl._cancelled and time.monotonic() < until:  # the watchdog's first step
            time.sleep(0.001)
        assert dl._cancelled
        enter(dl, shield)
        _spin(0.3)  # the rest of the door: the raise now set would land here

    monkeypatch.setattr(request_deadline.Deadline, "_enter", _held_by_load)
    _timeouts["s"] = 0.001
    expected: type[BaseException] = RequestTimedOut if scope == "request" else TimeoutError
    with pytest.raises(expected) as raised:
        if scope == "request":
            with request_deadline.request("graphql"):
                _spin(10.0)
        elif scope == "within":
            with request_deadline.within(0.001):
                _spin(10.0)
        else:
            owned = request_deadline.Deadline(0.001)
            try:
                with request_deadline.bound(owned):
                    _spin(10.0)
            finally:
                owned.stop()  # bound() leaves the deadline to its owner
    assert not isinstance(raised.value, DeadlinePassed)
    assert not request_deadline.shielded().inside
    assert request_deadline.current() is None
    _quiet()


# --- the control-plane engine ------------------------------------------------------------------


def test_a_timeout_during_control_plane_work_returns_its_connection(tmp_path):
    engine = sa.create_engine(
        f"sqlite+pysqlite:///{tmp_path / 'cp.db'}", connect_args={"check_same_thread": False}
    )
    for _ in range(5):
        with pytest.raises(RequestTimedOut):
            with request_deadline.request("sql_http"):
                with bounded_connection(engine) as conn:
                    conn.execute(sa.text("SELECT 1"))
                    _spin(10.0)
        assert engine.pool.checkedout() == 0
    with bounded_connection(engine) as conn:
        assert conn.execute(sa.text("SELECT 1")).scalar() == 1
    engine.dispose()
    _quiet()


# --- another thread under the same deadline ----------------------------------------------------


def test_only_the_requests_own_thread_is_raised_in():
    """Work fanned out to another thread shares the deadline (its statements are cancelled
    through it) but is not this request's thread: nothing is raised there."""
    raised_elsewhere: list[BaseException] = []
    deadline_seen: list[object] = []

    def _other(deadline) -> None:
        try:
            with request_deadline.bound(deadline):
                deadline_seen.append(request_deadline.current())
                _spin(0.6)
        except BaseException as exc:  # recorded and asserted on below
            raised_elsewhere.append(exc)

    with pytest.raises(RequestTimedOut):
        with request_deadline.request("grpc") as deadline:
            other = threading.Thread(target=_other, args=(deadline,))
            other.start()
            _spin(10.0)
    other.join(timeout=5)
    assert deadline_seen == [deadline]
    assert raised_elsewhere == []
    _quiet()
