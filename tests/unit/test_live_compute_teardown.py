# Copyright (c) 2026 Kenneth Stott
# Canary: 9326ea65-2945-4b28-a996-c1f34b091e42
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-254: a test stops the compute it starts, however it ends, and only that.

The live tests' compute guards (Fabric capacity, Databricks SQL warehouse, Snowflake warehouse)
read the state first, start inside the guarded block, and stop or pause after it -- also when the
start or the block fails -- unless the compute was already running when found. Their teardown
steps are each attempted and every failure is raised. No live account is reached here."""

# Requirements: REQ-254

from __future__ import annotations

import pytest

from tests.integration import databricks_warehouse, fabric_capacity, snowflake_warehouse
from tests.integration.live_teardown import run_every

# --- run_every --------------------------------------------------------------------------------


def test_every_teardown_step_runs_and_every_failure_is_raised():
    ran = []

    def fail(what):
        def step():
            ran.append(what)
            raise RuntimeError(what)

        return step

    with pytest.raises(ExceptionGroup) as raised:
        run_every(
            [
                ("drop a", fail("a")),
                ("drop b", lambda: ran.append("b")),
                ("close", fail("close")),
            ]
        )
    assert ran == ["a", "b", "close"]
    assert [str(e) for e in raised.value.exceptions] == ["a", "close"]
    assert raised.value.exceptions[0].__notes__ == ["teardown step: drop a"]


def test_a_teardown_that_succeeds_raises_nothing():
    ran = []
    run_every([("one", lambda: ran.append(1)), ("two", lambda: ran.append(2))])
    assert ran == [1, 2]


# --- the three compute guards, alike ------------------------------------------------------------


def _fabric(monkeypatch, found, *, resume_fails=False):
    calls = []
    monkeypatch.setattr(fabric_capacity, "capacity_state", lambda: found)

    def resume():
        calls.append("resume")
        if resume_fails:
            raise RuntimeError("resume timed out")

    monkeypatch.setattr(fabric_capacity, "ensure_capacity_resumed", resume)
    monkeypatch.setattr(fabric_capacity, "ensure_capacity_paused", lambda: calls.append("pause"))
    return fabric_capacity.capacity_resumed, calls


def _databricks(monkeypatch, found, *, resume_fails=False):
    calls = []
    monkeypatch.setattr(databricks_warehouse, "warehouse_state", lambda: found)

    def start():
        calls.append("resume")
        if resume_fails:
            raise RuntimeError("start timed out")

    monkeypatch.setattr(databricks_warehouse, "ensure_warehouse_running", start)
    monkeypatch.setattr(databricks_warehouse, "stop_warehouse", lambda: calls.append("pause"))
    return databricks_warehouse.warehouse_running, calls


_GUARDS = {
    "fabric": (_fabric, "Paused", ["Active", "Resuming"]),
    "databricks": (_databricks, "STOPPED", ["RUNNING", "STARTING"]),
}


@pytest.mark.parametrize("kind", sorted(_GUARDS))
def test_compute_the_block_started_is_stopped_after_it(monkeypatch, kind):
    build, stopped, _running = _GUARDS[kind]
    guard, calls = build(monkeypatch, stopped)
    with guard():
        calls.append("body")
    assert calls == ["resume", "body", "pause"]


@pytest.mark.parametrize("kind", sorted(_GUARDS))
def test_compute_is_stopped_when_the_block_fails(monkeypatch, kind):
    build, stopped, _running = _GUARDS[kind]
    guard, calls = build(monkeypatch, stopped)
    with pytest.raises(ValueError, match="seed failed"):
        with guard():
            raise ValueError("seed failed")
    assert calls == ["resume", "pause"]


@pytest.mark.parametrize("kind", sorted(_GUARDS))
def test_compute_is_stopped_when_the_start_itself_fails(monkeypatch, kind):
    build, stopped, _running = _GUARDS[kind]
    guard, calls = build(monkeypatch, stopped, resume_fails=True)
    with pytest.raises(RuntimeError, match="timed out"):
        with guard():
            calls.append("body")
    assert calls == ["resume", "pause"]


@pytest.mark.parametrize(
    ("kind", "found"), [(k, s) for k, (_b, _s, running) in sorted(_GUARDS.items()) for s in running]
)
def test_compute_already_running_is_left_as_it_was_found(monkeypatch, kind, found):
    build, _stopped, _running = _GUARDS[kind]
    guard, calls = build(monkeypatch, found)
    with guard():
        calls.append("body")
    assert calls == ["resume", "body"]


@pytest.mark.parametrize("found", ["SUSPENDED", "SUSPENDING"])
def test_a_snowflake_warehouse_the_block_resumed_is_suspended_after_it(monkeypatch, found):
    suspended = []
    monkeypatch.setattr(snowflake_warehouse, "warehouse_state", lambda wh: found)
    monkeypatch.setattr(snowflake_warehouse, "suspend_warehouse", suspended.append)
    with pytest.raises(ValueError):
        with snowflake_warehouse.warehouse_in_use("WH_X"):
            raise ValueError("query failed")
    assert suspended == ["WH_X"]


@pytest.mark.parametrize("found", ["STARTED", "RESUMING", "RESIZING"])
def test_a_snowflake_warehouse_already_running_is_left_running(monkeypatch, found):
    suspended = []
    monkeypatch.setattr(snowflake_warehouse, "warehouse_state", lambda wh: found)
    monkeypatch.setattr(snowflake_warehouse, "suspend_warehouse", suspended.append)
    with snowflake_warehouse.warehouse_in_use("WH_X"):
        pass
    assert suspended == []


# --- the stops themselves ---------------------------------------------------------------------


class _Cursor:
    def __init__(self, states):
        self.states = list(states)
        self.statements = []
        self.description = [("name",), ("state",)]

    def execute(self, sql):
        self.statements.append(sql)
        if sql.startswith("ALTER"):
            self.states.pop(0)

    def fetchall(self):
        return [("Wh_X", self.states[0])]

    def close(self):
        pass


class _Conn:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor

    def close(self):
        pass


def test_a_running_snowflake_warehouse_is_suspended_by_its_own_name(monkeypatch):
    cur = _Cursor(["STARTED", "SUSPENDED"])
    monkeypatch.setattr(snowflake_warehouse, "_connect", lambda: _Conn(cur))
    assert snowflake_warehouse.suspend_warehouse("wh_x") == "SUSPENDED"
    assert cur.statements == [
        "SHOW WAREHOUSES LIKE 'wh_x'",
        'ALTER WAREHOUSE "Wh_X" SUSPEND',
        "SHOW WAREHOUSES LIKE 'wh_x'",
    ]


def test_a_suspended_snowflake_warehouse_is_not_altered(monkeypatch):
    cur = _Cursor(["SUSPENDED"])
    monkeypatch.setattr(snowflake_warehouse, "_connect", lambda: _Conn(cur))
    assert snowflake_warehouse.suspend_warehouse("wh_x") == "SUSPENDED"
    assert not any(s.startswith("ALTER") for s in cur.statements)


def test_a_snowflake_warehouse_still_running_after_the_suspend_raises(monkeypatch):
    cur = _Cursor(["STARTED", "STARTED"])
    monkeypatch.setattr(snowflake_warehouse, "_connect", lambda: _Conn(cur))
    with pytest.raises(RuntimeError, match="may be billing"):
        snowflake_warehouse.suspend_warehouse("wh_x")


class _Response:
    def raise_for_status(self):
        return self


class _Http:
    def __init__(self):
        self.posts = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def post(self, url):
        self.posts.append(url)
        return _Response()


def _dbx_stop(monkeypatch, states):
    http = _Http()
    remaining = list(states)
    clock = [0.0]
    monkeypatch.setenv("DATABRICKS_HTTP_PATH", "/sql/1.0/warehouses/abc123")
    monkeypatch.setenv("DATABRICKS_SERVER_HOSTNAME", "dbx.example")
    monkeypatch.setattr(databricks_warehouse, "_client", lambda: http)
    monkeypatch.setattr(
        databricks_warehouse,
        "_state",
        lambda c, wid: remaining.pop(0) if len(remaining) > 1 else remaining[0],
    )
    monkeypatch.setattr(databricks_warehouse.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(
        databricks_warehouse.time, "sleep", lambda s: clock.__setitem__(0, clock[0] + s)
    )
    return http


def test_a_running_databricks_warehouse_is_stopped_once_and_waited_for(monkeypatch):
    http = _dbx_stop(monkeypatch, ["RUNNING", "STOPPING", "STOPPING", "STOPPED"])
    databricks_warehouse.stop_warehouse()
    assert http.posts == ["https://dbx.example/api/2.0/sql/warehouses/abc123/stop"]


def test_a_databricks_warehouse_that_never_stops_raises(monkeypatch):
    _dbx_stop(monkeypatch, ["RUNNING", "STOPPING"])
    with pytest.raises(RuntimeError, match="may be billing"):
        databricks_warehouse.stop_warehouse()


def test_a_databricks_warehouse_restarted_while_stopping_raises_at_once_naming_it(monkeypatch):
    """Run 40861830409: another run's queries restarted the warehouse this stop had sent STOPPING,
    and the stop waited 600s for a STOPPED that never came. A restart is named at once, and the
    stop is not sent again (it would take the warehouse from under that user)."""
    http = _dbx_stop(monkeypatch, ["RUNNING", "STOPPING", "STARTING", "RUNNING"])
    with pytest.raises(RuntimeError, match="something else restarted it") as raised:
        databricks_warehouse.stop_warehouse()
    assert "may be billing" in str(raised.value)
    assert http.posts == ["https://dbx.example/api/2.0/sql/warehouses/abc123/stop"]
    assert databricks_warehouse.time.monotonic() < databricks_warehouse._STOP_TIMEOUT_S


# --- the warehouse lane's own Snowflake settings (maintainer 2026-10-29) -------------------------


class _SettingsCursor:
    """SHOW WAREHOUSES answers with the warehouse's current settings; an ALTER ... SET changes
    them as Snowflake would, unless the statement is refused."""

    def __init__(self, *, auto_suspend=600, monitor="null", refuse=None):
        self.row = {"name": "Wh_X", "state": "SUSPENDED", "auto_suspend": auto_suspend,
                    "resource_monitor": monitor}  # fmt: skip
        self.refuse = refuse
        self.statements = []
        self.description = [(k,) for k in self.row]

    def execute(self, sql):
        self.statements.append(sql)
        if self.refuse and sql.startswith(self.refuse):
            raise RuntimeError("SQL access control error: Insufficient privileges")
        if sql.startswith("ALTER WAREHOUSE") and "AUTO_SUSPEND" in sql:
            self.row["auto_suspend"] = int(sql.rsplit("=", 1)[1])
        if sql.startswith("ALTER WAREHOUSE") and "RESOURCE_MONITOR" in sql:
            self.row["resource_monitor"] = sql.rsplit("=", 1)[1].strip().strip('"')

    def fetchall(self):
        return [tuple(self.row.values())]

    def close(self):
        pass


def test_the_lane_sets_the_snowflake_warehouse_to_suspend_after_sixty_idle_seconds(monkeypatch):
    cur = _SettingsCursor(auto_suspend=600)
    monkeypatch.setattr(snowflake_warehouse, "_connect", lambda: _Conn(cur))
    assert snowflake_warehouse.set_auto_suspend("wh_x", 60) == 60
    assert 'ALTER WAREHOUSE "Wh_X" SET AUTO_SUSPEND = 60' in cur.statements
    # Nothing resumes or suspends the warehouse to change the setting.
    assert not any(s.endswith(("RESUME", "SUSPEND")) for s in cur.statements)


def test_a_snowflake_auto_suspend_that_does_not_read_back_raises(monkeypatch):
    cur = _SettingsCursor(auto_suspend=600)
    cur.execute = lambda sql, _orig=cur.execute: None if sql.startswith("ALTER") else _orig(sql)
    monkeypatch.setattr(snowflake_warehouse, "_connect", lambda: _Conn(cur))
    with pytest.raises(RuntimeError, match="600s idle after it was set to 60s"):
        snowflake_warehouse.set_auto_suspend("wh_x", 60)


def test_the_snowflake_warehouse_is_put_under_a_monthly_credit_cap(monkeypatch):
    cur = _SettingsCursor()
    monkeypatch.setattr(snowflake_warehouse, "_connect", lambda: _Conn(cur))
    assert snowflake_warehouse.cap_monthly_credits("wh_x", "provisa_ci", 20) == "PROVISA_CI"
    assert cur.statements[1] == (
        'CREATE RESOURCE MONITOR IF NOT EXISTS "PROVISA_CI" WITH CREDIT_QUOTA = 20 '
        "FREQUENCY = MONTHLY START_TIMESTAMP = IMMEDIATELY "
        "TRIGGERS ON 100 PERCENT DO SUSPEND_IMMEDIATE"
    )
    assert cur.statements[2] == 'ALTER WAREHOUSE "Wh_X" SET RESOURCE_MONITOR = "PROVISA_CI"'


def test_a_snowflake_warehouse_already_under_the_cap_is_left_as_it_is(monkeypatch):
    cur = _SettingsCursor(monitor="PROVISA_CI")
    monkeypatch.setattr(snowflake_warehouse, "_connect", lambda: _Conn(cur))
    assert snowflake_warehouse.cap_monthly_credits("wh_x", "provisa_ci", 20) == "PROVISA_CI"
    assert cur.statements == ["SHOW WAREHOUSES LIKE 'wh_x'"]


def test_a_user_who_may_not_create_the_credit_cap_gets_snowflakes_refusal(monkeypatch):
    """Maintainer 2026-10-29: a CI user without the privilege fails the step with Snowflake's own
    message; nothing catches it."""
    cur = _SettingsCursor(refuse="CREATE RESOURCE MONITOR")
    monkeypatch.setattr(snowflake_warehouse, "_connect", lambda: _Conn(cur))
    with pytest.raises(RuntimeError, match="Insufficient privileges"):
        snowflake_warehouse.cap_monthly_credits("wh_x", "provisa_ci", 20)
