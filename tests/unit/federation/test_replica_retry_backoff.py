# Copyright (c) 2026 Kenneth Stott
# Canary: d55647f2-4418-46c5-8d26-53ea6532426b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A failed replica build is retried with a growing wait, and never given up (REQ-1915).

A table registered against a source that is gone was rebuilt every ``replication.retry_interval``
for as long as it stayed registered, each time with a traceback (the core UI lane's hiveserver2
table: 29 in 83 minutes, run 37918605161). The wait now doubles with each failure in a row up to
``replication.retry_interval_max``; a completed build resets it; an operator's request is tried at
once. The rule is computed from ``failed_attempts`` and ``failed_at``: nothing else is stored."""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from provisa.core import config_stamp
from provisa.core.database import Database, create_engine_from_url
from provisa.core.schema_org import config_stamp as stamps
from provisa.core.schema_org import metadata, replica_state
from provisa.federation import replica_runner
from provisa.federation import replica_state as build_state
from provisa.federation.replica_state import Failure, RetryPolicy

KEY = ("src", "public", "orders")
DEFAULT = RetryPolicy(60, 3600)
T0 = datetime(2026, 10, 9, 12, 0, tzinfo=UTC)


@pytest.fixture
async def conn(tmp_path):
    engine = create_engine_from_url(f"sqlite+pysqlite:///{tmp_path / 'cp.db'}")
    with engine.begin() as raw:
        metadata.create_all(raw, tables=[replica_state, stamps])
        config_stamp.install(raw, {}, advanced=config_stamp.TENANT_ADVANCED)
    async with Database(engine, "test").acquire() as connection:
        yield connection
    engine.dispose()


def test_the_wait_doubles_with_each_failure_in_a_row_up_to_the_ceiling():
    assert [DEFAULT.wait(n) for n in range(1, 10)] == [
        60, 120, 240, 480, 960, 1920, 3600, 3600, 3600
    ]  # fmt: skip
    # A record that is failed with no failure counted waits the interval.
    assert DEFAULT.wait(0) == 60
    # No interval: tried again at once, however often it has failed.
    assert [RetryPolicy(0, 3600).wait(n) for n in (1, 5, 50)] == [0, 0, 0]
    # A ceiling below the interval is no policy: the pair is refused where it is saved and where
    # it is loaded, and never bent into "the wait is the ceiling".
    with pytest.raises(ValueError, match="ceiling 300 s is below the retry interval 600 s"):
        RetryPolicy(600, 300)
    assert RetryPolicy(600, 600).wait(9) == 600
    assert DEFAULT.next_attempt_at(T0, 3) == T0 + timedelta(seconds=240)


async def _fail(conn, at: datetime, error: str = "source is down") -> Failure:
    """One failed build, claimed and recorded at ``at``."""
    await build_state.request_build(conn, KEY, build_state.REASON_OPERATOR, now=at)
    assert await build_state.claim(conn, KEY, holder="h:1", retry=DEFAULT, now=at)
    return await build_state.record_failed(conn, KEY, error=error, now=at)


async def _a_runner_may_build(conn, at: datetime) -> bool:
    return KEY in await build_state.candidates(conn, now=at, limit=10, retry=DEFAULT)


async def test_a_runner_waits_out_the_backoff_and_then_tries_again(conn):
    """The rule a runner selects by (`_claimable`), at each count of failures in a row."""
    at = T0
    for attempt, wait in enumerate([60, 120, 240, 480, 960, 1920, 3600, 3600], start=1):
        failure = await _fail(conn, at)
        assert failure.attempts == attempt
        assert not await _a_runner_may_build(conn, at + timedelta(seconds=wait - 1)), attempt
        assert await _a_runner_may_build(conn, at + timedelta(seconds=wait)), attempt
        # ... and the claim itself holds the same line.
        early = at + timedelta(seconds=wait - 1)
        assert not await build_state.claim(conn, KEY, holder="h:2", retry=DEFAULT, now=early)
        at += timedelta(seconds=wait)


async def test_a_completed_build_puts_the_wait_back_to_the_interval(conn):
    at = T0
    for _ in range(5):
        await _fail(conn, at)
        at += timedelta(hours=2)
    assert (await build_state.read(conn, KEY)).failed_attempts == 5
    await build_state.request_build(conn, KEY, build_state.REASON_OPERATOR, now=at)
    await build_state.claim(conn, KEY, holder="h:1", retry=DEFAULT, now=at)
    await build_state.record_completed(
        conn, KEY, rows_copied=1, method="stream_batches", content_hash="h", store="s",
        next_refresh_at=None, now=at,
    )  # fmt: skip
    assert (await build_state.read(conn, KEY)).failed_attempts == 0

    failure = await _fail(conn, at)
    assert failure == Failure(attempts=1, repeat=False)
    assert not await _a_runner_may_build(conn, at + timedelta(seconds=59))
    assert await _a_runner_may_build(conn, at + timedelta(seconds=60))


async def test_an_operators_request_is_tried_at_once_whatever_the_backoff(conn):
    at = T0
    for _ in range(7):  # the wait is at its ceiling: an hour
        await _fail(conn, at)
        at += timedelta(hours=2)
    failed_at = at
    await _fail(conn, failed_at)
    moment_later = failed_at + timedelta(seconds=1)
    assert not await _a_runner_may_build(conn, moment_later)
    # A read asks the same policy: it does not move the replica either.
    ask = build_state.request_build
    assert await ask(conn, KEY, build_state.REASON_READ, retry=DEFAULT, now=moment_later) is False
    assert (await build_state.read(conn, KEY)).build_state == "failed"
    # The operator's request gives no policy: requested now, and a runner may take it now.
    assert await ask(conn, KEY, build_state.REASON_OPERATOR, now=moment_later) is True
    assert await _a_runner_may_build(conn, moment_later)
    assert await build_state.claim(conn, KEY, holder="h:1", retry=DEFAULT, now=moment_later)


async def test_a_read_asks_again_only_when_the_backoff_has_passed(conn):
    await _fail(conn, T0)
    second = T0 + timedelta(seconds=60)
    await _fail(conn, second)  # two in a row: the wait is 120 s
    ask = build_state.request_build
    read = build_state.REASON_READ
    assert await ask(conn, KEY, read, retry=DEFAULT, now=second + timedelta(seconds=119)) is False
    assert await ask(conn, KEY, read, retry=DEFAULT, now=second + timedelta(seconds=120)) is True


async def test_a_recorded_failure_says_whether_it_repeats_the_one_before(conn):
    assert await _fail(conn, T0, "connection refused") == Failure(attempts=1, repeat=False)
    again = T0 + timedelta(hours=1)
    assert await _fail(conn, again, "connection refused") == Failure(attempts=2, repeat=True)
    changed = again + timedelta(hours=1)
    assert await _fail(conn, changed, "permission denied") == Failure(attempts=3, repeat=False)


def _logged(caplog, failure: Failure, error: str = "connection refused"):
    caplog.clear()
    with caplog.at_level(logging.DEBUG, logger=replica_runner.log.name):
        try:
            raise ConnectionError(error)
        except ConnectionError as exc:
            replica_runner._log_failure(KEY, exc, failure, DEFAULT, T0)  # noqa: SLF001
    (record,) = caplog.records
    return record


def test_the_first_failure_of_a_run_is_logged_with_its_traceback(caplog):
    record = _logged(caplog, Failure(attempts=1, repeat=False))
    assert record.levelno == logging.ERROR and record.exc_info is not None
    assert record.getMessage() == (
        "replica build of src.public.orders failed: connection refused; "
        "next attempt at 2026-10-09T12:01:00+00:00"
    )


def test_a_repeat_of_the_same_failure_is_one_line(caplog):
    record = _logged(caplog, Failure(attempts=4, repeat=True))
    assert record.levelno == logging.WARNING and not record.exc_info
    assert record.getMessage() == (
        "replica build of src.public.orders failed again (4 in a row): connection refused; "
        "next attempt at 2026-10-09T12:08:00+00:00"
    )


def test_a_failure_whose_error_changed_is_logged_with_its_traceback_again(caplog):
    record = _logged(caplog, Failure(attempts=4, repeat=False), "permission denied")
    assert record.levelno == logging.ERROR and record.exc_info is not None
    assert "permission denied; next attempt at 2026-10-09T12:08:00+00:00" in record.getMessage()


def _settings(monkeypatch, **in_force):
    from provisa.core import settings_registry

    values = {"replication.retry_interval": 60, "replication.retry_interval_max": 3600, **in_force}
    monkeypatch.setattr(settings_registry, "value", lambda key: values[key])
    monkeypatch.setattr(
        settings_registry, "prospective", lambda key, saved: saved.get(key, values[key])
    )


def test_a_ceiling_below_the_interval_is_refused_at_save_naming_the_other_setting(monkeypatch):
    from provisa.api.admin import settings_guards

    _settings(monkeypatch)
    with pytest.raises(settings_guards.Refused) as refused:
        settings_guards._pairs({"replication.retry_interval_max": 30})  # noqa: SLF001
    assert (refused.value.field, refused.value.reason, refused.value.params) == (
        "replication.retry_interval_max",
        "retry_max_below_interval",
        {"other": "replication.retry_interval"},
    )
    # The same pair, saved from the other side.
    with pytest.raises(settings_guards.Refused) as refused:
        settings_guards._pairs({"replication.retry_interval": 7200})  # noqa: SLF001
    assert (refused.value.field, refused.value.params) == (
        "replication.retry_interval",
        {"other": "replication.retry_interval_max"},
    )
    # Saved together they are judged together; equal is allowed (no growth, a fixed wait).
    settings_guards._pairs(  # noqa: SLF001
        {"replication.retry_interval": 7200, "replication.retry_interval_max": 7200}
    )
    settings_guards._pairs({"cache.default_ttl": 5})  # noqa: SLF001 -- neither saved: not judged


def test_a_ceiling_below_the_interval_from_the_environment_stops_the_load_by_name(monkeypatch):
    """The same rule at the other entrance. A deployment started with the ceiling below the
    interval does not run on a bent policy: the load is refused, naming both settings, both
    values and where each came from."""
    from provisa.core import settings_registry
    from provisa.core.settings_registry import Resolved, SettingsConflict

    resolved = {
        "replication.retry_interval": Resolved(60, "default"),
        "replication.retry_interval_max": Resolved(30, "env"),
    }
    monkeypatch.setattr(settings_registry, "resolve", lambda key: resolved[key])
    with pytest.raises(SettingsConflict) as refused:
        settings_registry.check_pairs()
    said = str(refused.value)
    assert (refused.value.field, refused.value.other) == (
        "replication.retry_interval_max",
        "replication.retry_interval",
    )
    assert said == (
        "setting replication.retry_interval_max is 30 and setting replication.retry_interval is "
        "60 (replication.retry_interval_max from environment variable "
        "PROVISA_REPLICATION_RETRY_INTERVAL_MAX, replication.retry_interval from its declared "
        "default) — retry_max_below_interval"
    )
    # A pair that works loads.
    resolved["replication.retry_interval_max"] = Resolved(60, "env")
    settings_registry.check_pairs()


def test_the_boot_applies_the_pair_rule(monkeypatch):
    """`freeze` is where a boot fixes its settings; the pair rule runs there."""
    from provisa.core import settings_registry

    called = []
    monkeypatch.setattr(settings_registry, "check_pairs", lambda values=None: called.append(values))
    monkeypatch.setattr(settings_registry, "_frozen", None)
    settings_registry.freeze()
    assert called == [None]


def test_the_ceiling_is_a_live_operator_setting_with_its_environment_variable():
    from provisa.core import settings_registry

    ceiling = settings_registry.setting("replication.retry_interval_max")
    interval = settings_registry.setting("replication.retry_interval")
    # ... and the product's catalog declares the pair the rule judges.
    assert ("replication.retry_interval_max", "replication.retry_interval") in [
        pair[:2]
        for pair in settings_registry._AT_LEAST  # noqa: SLF001
    ]
    assert (ceiling.type, ceiling.effect, ceiling.default, ceiling.unit) == (
        "int", "live", 3600, "seconds"
    )  # fmt: skip
    assert ceiling.env == "PROVISA_REPLICATION_RETRY_INTERVAL_MAX"
    assert (ceiling.card, ceiling.req) == (interval.card, interval.req)


def test_the_admin_record_of_a_failed_replica_says_when_it_is_tried_next(monkeypatch):
    from provisa.api.admin._replica_builds import build_view

    _settings(monkeypatch)
    record = SimpleNamespace(
        key=KEY, build_state="failed", retired_at=None, requested_reason="read",
        build_method=None, load_kind=None, build_started_at=None, rows_copied=None,
        completed_at=None, next_refresh_at=None, last_error="connection refused",
        last_error_code=None, last_error_params=None, failed_attempts=3, failed_at=T0,
        waiting_on=None, feed_down_since=None, feed_error=None, delta_skipped=None,
        delta_cursor=None, build_notes=[],
    )  # fmt: skip
    assert build_view(record, T0)["next_attempt_at"] == "2026-10-09T12:04:00+00:00"
    record.build_state = "idle"
    assert build_view(record, T0)["next_attempt_at"] is None


# --- #203: the wait before a failed build is tried again is never nothing -----------------------


def test_the_retry_interval_refuses_a_value_below_one_second():
    from provisa.core import settings_registry
    from provisa.core.settings_registry import SettingInvalid

    interval = settings_registry.setting("replication.retry_interval")
    assert interval.min == 1
    for refused in (0, -5):
        with pytest.raises(SettingInvalid) as invalid:
            settings_registry.validate({"replication.retry_interval": refused})
        assert invalid.value.reason == "below_min"
    settings_registry.validate({"replication.retry_interval": 1})


async def test_a_build_that_keeps_failing_is_not_attempted_again_until_its_wait_has_passed(
    tmp_path,
):
    """The regression of #203. With no wait a failing build was claimed again the moment it
    failed, hundreds of times a second, and the runner never rested. At the shortest wait the
    setting allows it is attempted once, and the runner comes to rest."""
    import asyncio

    from provisa.core.database import Database, create_engine_from_url
    from provisa.core.schema_org import metadata
    from provisa.core.schema_org import replica_state as replica_state_table
    from provisa.federation import replica_state as build_state
    from provisa.federation.replica_locks import BuildLocks
    from provisa.federation.replica_runner import ReplicaRunner

    url = f"sqlite+pysqlite:///{tmp_path / 'cp.db'}"
    engine = create_engine_from_url(url)
    with engine.begin() as conn:
        metadata.create_all(conn, tables=[replica_state_table])
    db = Database(engine, "test")
    key = ("src", "public", "t1")
    async with db.acquire() as conn:
        assert await build_state.request_build(conn, key, build_state.REASON_MODEL)
    attempts = 0
    tasks: list[asyncio.Task] = []

    async def build(_key, _progress):
        nonlocal attempts
        attempts += 1
        raise RuntimeError("the source is down")

    async def none(*_args):
        return None

    shortest = settings_registry_min()
    locks = BuildLocks(url)
    locks._slots = tmp_path / "slots"  # noqa: SLF001
    runner = ReplicaRunner(
        db=db,
        org_id="org1",
        locks=locks,
        engine_key=lambda: "engine",
        build=build,
        source_cap=none,
        permits=None,
        next_refresh_at=none,
        store=lambda: "store-a",
        retry=lambda: RetryPolicy(shortest, 3600),
        builds_per_node=lambda: 1,
        engine_jobs=lambda: 1,
        spawn=lambda coro, name: tasks.append(asyncio.ensure_future(coro)),
    )
    try:
        await runner.run_pass()
        # Bounded: a runner that spun would never let this finish.
        await asyncio.wait_for(asyncio.gather(*tasks), 5)
    finally:
        for task in tasks:
            task.cancel()
        engine.dispose()
    assert attempts == 1


def settings_registry_min() -> float:
    from provisa.core import settings_registry

    return float(settings_registry.setting("replication.retry_interval").min)
