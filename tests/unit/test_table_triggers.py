# Copyright (c) 2026 Kenneth Stott
# Canary: 385f865e-b794-4f3a-965e-0ef965e7383f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Triggers on a table's change or event, running commands (REQ-2054).

A trigger says when -- a schedule, or a table's change, error or quarantine -- and what runs:
an ordinary command, given the trigger's arguments, as the trigger's role. It is refused at save
when its command takes an argument it does not give. Each run is recorded with its outcome.
"""

# Requirements: REQ-2054

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from provisa.core.models import ScheduledTrigger, trigger_arguments, trigger_refusal

_BRONZE = "pg.public.bronze"


def _on(event: str = "change", **kw) -> ScheduledTrigger:
    base = {"id": "t", "on": {"table": _BRONZE, "event": event}, "command": "c", "role": "r"}
    return ScheduledTrigger(**{**base, **kw})


# -- what a trigger says ---------------------------------------------------------------------


def test_a_trigger_written_on_a_table_in_a_config_file_loads():
    """A config file says ``on: {table, event}``; YAML reads a bare ``on`` key as true, and the
    trigger is the same trigger."""
    import yaml

    raw = yaml.safe_load(f"id: t\non: {{table: {_BRONZE}, event: change}}\ncommand: c\nrole: r\n")
    assert True in raw  # what YAML made of the key
    trigger = ScheduledTrigger.model_validate(raw)
    assert (trigger.on_table, trigger.on_event) == (_BRONZE, "change")


def test_a_trigger_is_on_a_table_written_on_or_flat():
    trigger = _on()
    assert (trigger.cron, trigger.on_table, trigger.on_event) == (None, _BRONZE, "change")


@pytest.mark.parametrize(
    ("fields", "why"),
    [
        ({"id": "t", "command": "c", "role": "r"}, "neither a schedule nor a table"),
        (
            {
                "id": "t",
                "cron": "* * * * *",
                "on_table": _BRONZE,
                "on_event": "change",
                "command": "c",
                "role": "r",
            },
            "both a schedule and a table",
        ),  # fmt: skip
        (
            {"id": "t", "on_table": _BRONZE, "on_event": "deleted", "command": "c", "role": "r"},
            "an event that is none",
        ),  # fmt: skip
        (
            {"id": "t", "on_table": "bronze", "on_event": "change", "command": "c", "role": "r"},
            "a table that is not source.schema.table",
        ),  # fmt: skip
        ({"id": "t", "cron": "* * * * *", "command": "c"}, "a command with no role"),
        (
            {"id": "t", "cron": "* * * * *", "command": "c", "role": "r", "sql": "SELECT 1"},
            "two things to run",
        ),  # fmt: skip
    ],
)
def test_a_trigger_that_does_not_say_one_when_and_one_what_is_refused(fields, why):
    with pytest.raises(ValueError):
        ScheduledTrigger(**fields)


def test_what_each_firing_gives():
    assert trigger_arguments(None, "change", {}) == {"table", "changes"}
    assert trigger_arguments(None, "error", {}) == {"table", "event", "reason", "error"}
    assert trigger_arguments("0 * * * *", None, {"since": "x"}) == {"since"}


def _refusal(trigger: ScheduledTrigger, args: list[str]) -> str | None:
    return trigger_refusal(trigger, commands={"c": args}, tables={_BRONZE}, roles={"r"})


def test_a_command_that_takes_only_what_the_trigger_gives_is_accepted():
    assert _refusal(_on(), ["table", "changes"]) is None
    assert _refusal(_on(), []) is None
    assert _refusal(_on("quarantine"), ["reason", "table"]) is None


def test_a_command_that_takes_more_than_the_trigger_gives_is_refused():
    refused = _refusal(_on(), ["table", "since"])
    assert refused is not None and "since" in refused
    assert _refusal(_on("error"), ["changes"]) is not None


def test_a_trigger_on_an_unregistered_table_a_missing_command_or_role_is_refused():
    assert "not registered" in (
        trigger_refusal(_on(), commands={"c": []}, tables=set(), roles={"r"}) or ""
    )
    assert "no command" in (_refusal(_on(command="gone"), []) or "")
    assert "no role" in (_refusal(_on(role="nobody"), []) or "")


def test_a_command_trigger_is_stored_as_a_command():
    from provisa.core.repositories.scheduled_trigger import _values

    values = _values(_on())
    assert (values["kind"], values["command"], values["on_table"], values["on_event"]) == (
        "command",
        "c",
        _BRONZE,
        "change",
    )


# -- a run, and its record -------------------------------------------------------------------


def _row(**kw) -> dict:
    return {"id": "t", "kind": "command", "command": "c", "role": "r", "args": {}, **kw}


@pytest.mark.asyncio
async def test_a_command_is_given_the_arguments_it_declares_in_its_order_as_the_role(monkeypatch):
    from provisa.scheduler import trigger_runs

    state = SimpleNamespace(
        tracked_functions={"c": {"arguments": [{"name": "reason"}, {"name": "table"}]}}
    )
    monkeypatch.setattr("provisa.api.app.state", state)
    invoked = AsyncMock()
    recorded = AsyncMock()
    with (
        patch("provisa.api.data.action_exec.invoke_command", invoked),
        patch.object(trigger_runs, "record", recorded),
    ):
        outcome = await trigger_runs.run_trigger(
            _row(),
            "default",
            given={"table": _BRONZE, "event": "error", "reason": "held", "error": None},
            fired_by="error:" + _BRONZE,
        )
    assert outcome == "succeeded"
    name, args, _state, role = invoked.await_args.args
    assert (name, list(args.items()), role) == ("c", [("reason", "held"), ("table", _BRONZE)], "r")
    assert recorded.await_args.args[1:] == ("t", "succeeded", None, "error:" + _BRONZE)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("raised", "outcome"),
    [
        ("refusal", "refused"),
        ("failure", "failed"),
    ],
)
async def test_a_refused_or_failed_run_is_recorded_with_its_cause(monkeypatch, raised, outcome):
    from provisa.api.errors import ApiError
    from provisa.scheduler import trigger_runs

    monkeypatch.setattr(
        "provisa.api.app.state", SimpleNamespace(tracked_functions={"c": {"arguments": []}})
    )
    error = (
        ApiError(403, "authz.mutation_not_permitted", "no write right")
        if raised == "refusal"
        else RuntimeError("the source is down")
    )
    recorded = AsyncMock()
    with (
        patch("provisa.api.data.action_exec.invoke_command", AsyncMock(side_effect=error)),
        patch.object(trigger_runs, "record", recorded),
    ):
        assert await trigger_runs.run_trigger(_row(), "default", given={}, fired_by=None) == outcome
    _state, trigger_id, said, cause, _fired = recorded.await_args.args
    assert (trigger_id, said) == ("t", outcome) and cause


# -- what fires ------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_change_to_a_table_read_live_fires_now_and_a_replicated_ones_waits(monkeypatch):
    from provisa.scheduler import table_triggers

    fired: list = []
    monkeypatch.setattr(table_triggers, "table_changed", lambda state, key: fired.append(key))
    table = SimpleNamespace(id=7, schema_name="public", table_name="bronze", change_signal=None)
    _registry(monkeypatch, table, SimpleNamespace(id="pg", change_signal="ttl"))
    for replicated in (True, False):
        monkeypatch.setattr(
            "provisa.federation.replica_builds.request_if_replicated",
            AsyncMock(return_value=replicated),
        )
        await table_triggers.source_changed(object(), 7, "pg", "write")
    assert fired == [("pg", "public", "bronze")]  # only the live one; the other on its landing


def _registry(monkeypatch, table, source) -> None:
    async def _tables(_state):
        return [table]

    async def _sources(_state):
        return [source]

    monkeypatch.setattr("provisa.federation.registry_view.registered_tables", _tables)
    monkeypatch.setattr("provisa.federation.registry_view.registered_sources", _sources)


@pytest.mark.asyncio
async def test_a_write_to_a_table_its_native_feed_follows_fires_from_the_feed_alone(monkeypatch):
    from provisa.scheduler import table_triggers

    fired: list = []
    monkeypatch.setattr(table_triggers, "table_changed", lambda state, key: fired.append(key))
    monkeypatch.setattr(
        "provisa.federation.replica_builds.request_if_replicated", AsyncMock(return_value=False)
    )
    table = SimpleNamespace(id=7, schema_name="public", table_name="bronze", change_signal=None)
    _registry(monkeypatch, table, SimpleNamespace(id="pg", change_signal="native"))
    await table_triggers.source_changed(object(), 7, "pg", "write")
    assert fired == []  # the write: its feed reports it
    await table_triggers.source_changed(object(), 7, "pg", "refresh")
    assert fired == [("pg", "public", "bronze")]  # the feed's burst


@pytest.mark.asyncio
async def test_only_prod_fires(monkeypatch):
    from provisa.scheduler import table_triggers

    state = SimpleNamespace(_active_runtime=lambda: SimpleNamespace(env="dev"))
    run = AsyncMock()
    monkeypatch.setattr("provisa.scheduler.trigger_runs.run_trigger", run)
    await table_triggers._fire(state, _BRONZE, "change", {})
    run.assert_not_awaited()


def test_a_nodes_error_fires_the_triggers_on_its_table_and_a_views_fires_none(monkeypatch):
    from provisa.events import app_wiring

    events: list = []
    monkeypatch.setattr(
        "provisa.scheduler.table_triggers.table_event",
        lambda state, key, event, payload: events.append((key, event, payload)),
    )
    fired = app_wiring.node_event_triggers(object())
    fired("pg/public.bronze", "error", {"error": "boom"})
    fired("mv.daily", "quarantine", {"reason": "held"})
    assert events == [(("pg", "public", "bronze"), "error", {"error": "boom"})]


def test_a_landing_that_changed_nothing_fires_nothing():
    from provisa.federation.replica_runner import ReplicaRunner

    changed: list = []
    runner = object.__new__(ReplicaRunner)
    runner._changed = changed.append
    key = ("pg", "public", "bronze")
    landed = SimpleNamespace(content_hash="h1")
    runner._landed(key, SimpleNamespace(content_hash="h1"), landed)
    assert changed == []
    runner._landed(key, SimpleNamespace(content_hash="h0"), landed)
    runner._landed(key, None, landed)  # the first landing
    runner._landed(key, SimpleNamespace(content_hash="h1"), SimpleNamespace(content_hash=None))
    assert changed == [key, key, key]
