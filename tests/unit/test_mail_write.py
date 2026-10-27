# Copyright (c) 2026 Kenneth Stott
# Canary: 7c1e4a92-5b3d-4f86-9a20-e8d6c3b1f574
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The one write route of a mail source's events, attendees and tasks (REQ-1994).

A governed statement reaches the driver as SQL. An INSERT's literal rows are created; an UPDATE or
DELETE names its rows by their write key, the rows are found at the provider as they stand, and
each is changed or removed only where the whole WHERE -- the role's row rules with it -- holds.
Then each provider's writer turns a row into the call that makes it so: Google with sendUpdates
none unless the source says to tell attendees; Microsoft refusing, while that is off, a change
Graph would announce to them.
"""

from __future__ import annotations

import datetime as dt
import json
from contextlib import asynccontextmanager
from typing import Any

import httpx
import pytest

from provisa.core import canonical_mail as cm
from provisa.executor.writable import API_WRITTEN, WritePath, resolve_write_path
from provisa.executor.write_capability import (
    table_write_ops,
    table_write_refused_forms,
    table_write_returns_rows,
)
from provisa.federation.mail_write import Found, MailWriteDriver, WriteRefused, holds
from provisa.google_workspace.calendar_write import GoogleWriter
from provisa.google_workspace.workspace_api import WorkspaceApi
from provisa.microsoft365.calendar_write import GraphWriter
from provisa.microsoft365.graph import Answer, Graph

pytestmark = pytest.mark.asyncio

ADA, BO = "ada@example.test", "bo@example.test"


# -- what the route offers -------------------------------------------------------------------


@pytest.mark.parametrize("source_type", sorted(API_WRITTEN))
async def test_events_attendees_and_tasks_take_all_three_writes_and_nothing_else_does(
    source_type,
):
    assert resolve_write_path(source_type) is WritePath.API
    for table in ("events", "event_attendees", "tasks"):
        record = {"table_name": table, "source_id": "s"}
        assert table_write_ops(record, source_type, None, source_path=None) == {
            "insert",
            "update",
            "delete",
        }
        assert table_write_returns_rows(record, source_type, None, source_path=None) is False
        assert "on_conflict" in table_write_refused_forms(
            record, source_type, None, source_path=None
        )
    for table in ("messages", "calendars", "task_lists", "folders"):
        assert (
            table_write_ops(
                {"table_name": table, "source_id": "s"}, source_type, None, source_path=None
            )
            == set()
        )


# -- the driver --------------------------------------------------------------------------------


class _Writer:
    """A provider's writer, as the driver uses it: the rows it holds, and what it was asked."""

    def __init__(self, rows: list[dict]) -> None:
        self.rows = rows
        self.done: list[tuple] = []
        self.pins: list[dict] = []

    async def find_events(self, pins: dict) -> list[Found]:
        self.pins.append(pins)
        return [Found(r, r["id"]) for r in self.rows if r["id"] == pins.get("id")]

    find_tasks = find_events

    async def insert_event(self, values: dict) -> None:
        self.done.append(("insert", values))

    async def update_event(self, handle: Any, values: dict) -> None:
        self.done.append(("update", handle, values))

    async def delete_event(self, handle: Any) -> None:
        self.done.append(("delete", handle))

    insert_task, update_task, delete_task = insert_event, update_event, delete_event


def _event(**values: Any) -> dict:
    row: dict[str, Any] = {c.name: None for c in cm.TABLES["events"].columns}
    row.update(provider=cm.GOOGLE, account=ADA, **values)
    return row


def _driver(writer: _Writer, accounts=(ADA,), provider=cm.GOOGLE) -> MailWriteDriver:
    async def listed() -> tuple[str, ...]:
        return accounts

    @asynccontextmanager
    async def writer_for(account: str):
        writer.account = account  # type: ignore[attr-defined]
        yield writer

    return MailWriteDriver(provider, listed, writer_for)


async def test_an_insert_creates_each_row_as_its_account():
    writer = _Writer([])
    result = await _driver(writer).execute(
        'INSERT INTO "default"."events" (account, calendar_id, title, start_at, end_at) '
        "VALUES ('ada@example.test', 'primary', 'Lunch', '2026-10-13 12:00:00+00', $1)",
        ["2026-10-13 13:00:00+00"],
    )
    assert result.rowcount == 1
    assert writer.done == [
        (
            "insert",
            {
                "calendar_id": "primary",
                "title": "Lunch",
                "start_at": "2026-10-13 12:00:00+00",
                "end_at": "2026-10-13 13:00:00+00",
            },
        )
    ]


async def test_an_update_changes_only_the_rows_its_whole_where_holds_for():
    """The row rule ``title <> 'private'`` came with the statement: the named row is found,
    and is changed only if the rule holds for it as the table gives it."""
    writer = _Writer([_event(id="e1", title="private")])
    sql = (
        'UPDATE "default"."events" SET title = \'renamed\' '
        "WHERE events.id = 'e1' AND events.title <> 'private'"
    )
    assert (await _driver(writer).execute(sql)).rowcount == 0
    assert writer.done == []
    writer.rows[0]["title"] = "standup"
    assert (await _driver(writer).execute(sql)).rowcount == 1
    assert writer.done == [("update", "e1", {"title": "renamed"})]


async def test_a_delete_names_its_row_by_its_key():
    writer = _Writer([_event(id="e1")])
    result = await _driver(writer).execute(
        "DELETE FROM events WHERE id = 'e1' AND account = 'ada@example.test'"
    )
    assert result.rowcount == 1 and writer.done == [("delete", "e1")]
    assert writer.pins == [{"id": "e1", "account": "ada@example.test"}]


async def test_a_column_the_provider_does_not_keep_is_refused_by_name():
    """Google's Tasks API has no reminder (REQ-1994): a task write that sets one is refused."""
    with pytest.raises(WriteRefused, match="tasks.reminder_at is not written"):
        await _driver(_Writer([])).execute(
            "INSERT INTO tasks (task_list_id, title, reminder_at) VALUES ('l', 't', '2026-10-13')"
        )
    with pytest.raises(WriteRefused, match="events.updated_at is not written"):
        await _driver(_Writer([])).execute(
            "UPDATE events SET updated_at = '2026-10-13' WHERE id = 'e'"
        )


@pytest.mark.parametrize(
    ("sql", "said"),
    [
        ("UPDATE messages SET subject = 'x' WHERE id = 'm'", "messages takes no writes"),
        ("DELETE FROM events", "names the rows it changes"),
        ("INSERT INTO events (title) SELECT title FROM events", "states its rows as VALUES"),
        ("UPDATE events SET title = upper(title) WHERE id = 'e'", "not a value"),
        ("UPDATE events SET account = 'b@x.test' WHERE id = 'e'", "not moved to another account"),
    ],
)
async def test_what_the_route_does_not_carry_is_refused_by_name(sql, said):
    with pytest.raises(WriteRefused, match=said):
        await _driver(_Writer([])).execute(sql)


async def test_a_source_of_several_accounts_is_told_which():
    driver = _driver(_Writer([]), accounts=(ADA, BO))
    with pytest.raises(WriteRefused, match="name the account"):
        await driver.execute("DELETE FROM events WHERE id = 'e1'")
    with pytest.raises(WriteRefused, match="not an account of this source"):
        await driver.execute("DELETE FROM events WHERE id = 'e1' AND account = 'cy@example.test'")


async def test_a_row_of_the_other_provider_is_never_written():
    with pytest.raises(WriteRefused, match="this source is google"):
        await _driver(_Writer([])).execute(
            "DELETE FROM events WHERE id = 'e' AND provider = 'microsoft'"
        )


async def test_the_where_is_evaluated_over_the_row_as_the_table_gives_it():
    import sqlglot

    row = _event(
        id="e1",
        start_at=dt.datetime(2026, 10, 13, 12, tzinfo=dt.UTC),
        reminders={"useDefault": True},
        is_all_day=False,
    )

    def where(text: str):
        return sqlglot.parse_one(f"SELECT 1 WHERE {text}", read="postgres").args["where"].this

    assert holds(where("start_at >= '2026-10-13 00:00:00+00' AND NOT is_all_day"), "events", row)
    assert not holds(where("start_at < '2026-10-13 00:00:00+00'"), "events", row)
    assert holds(where("title IS NULL AND account = 'ada@example.test'"), "events", row)


# -- Google ------------------------------------------------------------------------------------


class _Google:
    """Calendar and Tasks as one account's writer calls them."""

    def __init__(self) -> None:
        self.asked: list[tuple[str, str, dict, Any]] = []
        self.answers: dict[tuple[str, str], Any] = {}

    def handle(self, request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content) if request.content else None
        path = request.url.path
        self.asked.append((request.method, path, dict(request.url.params), body))
        answer = self.answers.get((request.method, path))
        if answer is None:
            return httpx.Response(404, json={"error": {"errors": [{"reason": "notFound"}]}})
        return httpx.Response(200, json=answer)


@pytest.fixture
async def google():
    stub = _Google()
    async with httpx.AsyncClient(transport=httpx.MockTransport(stub.handle)) as client:

        async def token() -> str:
            return "token"

        stub.api = WorkspaceApi(ADA, token, client)  # type: ignore[attr-defined]
        yield stub


async def test_a_google_event_is_inserted_telling_nobody(google):
    google.answers[("POST", "/calendar/v3/calendars/primary/events")] = {"id": "new"}
    writer = GoogleWriter(google.api, send_updates=False)
    await writer.insert_event(
        {
            "calendar_id": "primary",
            "title": "Lunch",
            "start_at": "2026-10-13 12:00:00+00",
            "end_at": "2026-10-13T13:00:00Z",
            "start_time_zone": "Europe/London",
            "reminders": '{"useDefault": false, "overrides": [{"method": "popup", "minutes": 5}]}',
        }
    )
    ((method, path, params, body),) = google.asked
    assert (method, params["sendUpdates"]) == ("POST", "none")
    assert body == {
        "summary": "Lunch",
        "start": {"dateTime": "2026-10-13T12:00:00Z", "timeZone": "Europe/London"},
        "end": {"dateTime": "2026-10-13T13:00:00Z"},
        "reminders": {"useDefault": False, "overrides": [{"method": "popup", "minutes": 5}]},
    }


async def test_one_occurrence_of_a_google_series_is_found_by_its_original_start(google):
    instance = {
        "id": "s1_20261014T080000Z",
        "recurringEventId": "s1",
        "originalStartTime": {"dateTime": "2026-10-14T08:00:00Z"},
        "start": {"dateTime": "2026-10-14T08:00:00Z"},
        "end": {"dateTime": "2026-10-14T08:15:00Z"},
        "status": "confirmed",
    }
    google.answers[("GET", "/calendar/v3/calendars/primary/events/s1/instances")] = {
        "items": [instance]
    }
    google.answers[("PATCH", f"/calendar/v3/calendars/primary/events/{instance['id']}")] = {}
    writer = GoogleWriter(google.api, send_updates=True)
    (found,) = await writer.find_events(
        {"calendar_id": "primary", "series_id": "s1", "original_start_at": "2026-10-14 08:00:00+00"}
    )
    assert (found.row["kind"], found.row["series_id"]) == ("exception", "s1")
    assert google.asked[0][2]["originalStart"] == "2026-10-14T08:00:00Z"
    await writer.update_event(found.handle, {"title": "moved"})
    method, _path, params, body = google.asked[-1]
    assert (method, params["sendUpdates"], body) == ("PATCH", "all", {"summary": "moved"})
    with pytest.raises(WriteRefused, match="set on a series"):
        await writer.update_event(found.handle, {"recurrence": "RRULE:FREQ=DAILY"})


async def test_a_google_attendee_is_added_to_the_events_list(google):
    event = {"id": "e1", "attendees": [{"email": ADA}], "status": "confirmed"}
    google.answers[("GET", "/calendar/v3/calendars/primary/events/e1")] = event
    google.answers[("PATCH", "/calendar/v3/calendars/primary/events/e1")] = {}
    writer = GoogleWriter(google.api, send_updates=False)
    await writer.insert_attendee(
        {"event_id": "e1", "calendar_id": "primary", "address": BO, "kind": "optional"}
    )
    _method, _path, params, body = google.asked[-1]
    assert params["sendUpdates"] == "none"
    assert body == {"attendees": [{"email": ADA}, {"email": BO, "optional": True}]}


async def test_a_google_task_is_due_on_a_day(google):
    google.answers[("POST", "/tasks/v1/lists/l1/tasks")] = {"id": "t"}
    await GoogleWriter(google.api, send_updates=False).insert_task(
        {"task_list_id": "l1", "title": "File", "due_date": "2026-10-16", "status": "open"}
    )
    _m, _p, _q, body = google.asked[-1]
    assert body == {"title": "File", "status": "needsAction", "due": "2026-10-16T00:00:00.000Z"}


# -- Microsoft ---------------------------------------------------------------------------------


class _Graph:
    def __init__(self) -> None:
        self.asked: list[tuple[str, str, Any]] = []
        self.answers: dict[tuple[str, str], Any] = {}

    def send(self, method, url, params, headers, body) -> Answer:
        path = url.split("/v1.0", 1)[1]
        self.asked.append((method, path, body))
        found = self.answers.get((method, path))
        if found is None:
            return Answer(404, {}, {"error": {"code": "ErrorItemNotFound", "message": "gone"}})
        return Answer(200, {}, found)


def _graph_writer(stub: _Graph, send_updates: bool) -> GraphWriter:
    return GraphWriter(Graph(stub.send, lambda: "token"), ADA, send_updates)


MEETING = {
    "id": "m1",
    "type": "singleInstance",
    "isOrganizer": True,
    "attendees": [{"emailAddress": {"address": BO}, "type": "required"}],
    "start": {"dateTime": "2026-10-13T12:00:00", "timeZone": "UTC"},
    "end": {"dateTime": "2026-10-13T13:00:00", "timeZone": "UTC"},
    "calendar": {"id": "cal1"},
}


async def test_a_meeting_is_not_changed_silently_on_microsoft():
    """Graph tells the attendees of a change to a meeting; with the setting off it is refused,
    except the account's own reminder, which nobody is told of."""
    stub = _Graph()
    stub.answers[("GET", f"/users/{ADA.replace('@', '%40')}/events/m1")] = MEETING
    stub.answers[("PATCH", f"/users/{ADA.replace('@', '%40')}/events/m1")] = {}
    writer = _graph_writer(stub, send_updates=False)
    (found,) = await writer.find_events({"id": "m1"})
    with pytest.raises(WriteRefused, match="tells the attendees"):
        await writer.update_event(found.handle, {"title": "moved"})
    with pytest.raises(WriteRefused, match="tells the attendees"):
        await writer.delete_event(found.handle)
    await writer.update_event(found.handle, {"is_reminder_on": False})
    assert stub.asked[-1] == (
        "PATCH",
        f"/users/{ADA.replace('@', '%40')}/events/m1",
        {"isReminderOn": False},
    )


async def test_microsoft_attendee_writes_need_the_setting_on():
    stub = _Graph()
    stub.answers[("GET", f"/users/{ADA.replace('@', '%40')}/events/m1")] = MEETING
    stub.answers[("PATCH", f"/users/{ADA.replace('@', '%40')}/events/m1")] = {}
    with pytest.raises(WriteRefused, match="tells the attendees"):
        await _graph_writer(stub, False).insert_attendee(
            {"event_id": "m1", "address": "cy@example.test"}
        )
    await _graph_writer(stub, True).insert_attendee(
        {"event_id": "m1", "address": "cy@example.test", "kind": "optional"}
    )
    _method, _path, body = stub.asked[-1]
    assert [a["emailAddress"]["address"] for a in body["attendees"]] == [BO, "cy@example.test"]
    assert body["attendees"][-1]["type"] == "optional"


async def test_a_microsoft_series_is_written_as_a_graph_pattern():
    stub = _Graph()
    stub.answers[("POST", f"/users/{ADA.replace('@', '%40')}/calendars/cal1/events")] = {"id": "n"}
    await _graph_writer(stub, False).insert_event(
        {
            "calendar_id": "cal1",
            "title": "Review",
            "start_at": "2026-10-13 14:00:00+00",
            "end_at": "2026-10-13 15:00:00+00",
            "start_time_zone": "Europe/London",
            "recurrence": "RRULE:FREQ=WEEKLY;BYDAY=TU;COUNT=4",
        }
    )
    _method, _path, body = stub.asked[-1]
    assert body["start"] == {"dateTime": "2026-10-13T15:00:00", "timeZone": "Europe/London"}
    assert body["recurrence"]["pattern"]["daysOfWeek"] == ["tuesday"]
    assert body["recurrence"]["range"] == {
        "startDate": "2026-10-13",
        "recurrenceTimeZone": "Europe/London",
        "type": "numbered",
        "numberOfOccurrences": 4,
    }


async def test_a_microsoft_task_reminder_turns_the_reminder_on():
    stub = _Graph()
    stub.answers[("POST", f"/users/{ADA.replace('@', '%40')}/todo/lists/l1/tasks")] = {"id": "t"}
    await _graph_writer(stub, False).insert_task(
        {
            "task_list_id": "l1",
            "title": "Call",
            "due_at": "2026-10-16 09:00:00+00",
            "reminder_at": "2026-10-15 09:00:00+00",
        }
    )
    _method, _path, body = stub.asked[-1]
    assert body["dueDateTime"] == {"dateTime": "2026-10-16T09:00:00", "timeZone": "UTC"}
    assert (body["reminderDateTime"]["dateTime"], body["isReminderOn"]) == (
        "2026-10-15T09:00:00",
        True,
    )


# -- the route a statement takes ---------------------------------------------------------------


@pytest.mark.parametrize("source_type", sorted(API_WRITTEN))
async def test_a_read_goes_to_the_engine_and_a_write_direct_to_the_api_driver(source_type):
    from provisa.core.source_registry import SOURCE_TO_DIALECT
    from provisa.transpiler.router import Route, decide_route

    assert SOURCE_TO_DIALECT[source_type] == "postgres"
    common = dict(
        sources={"s"},
        source_types={"s": source_type},
        source_dialects={"s": "postgres"},
        operator_floor={},
        statement="SELECT 1",
    )
    assert decide_route(**common).route is Route.ENGINE  # type: ignore[arg-type]
    write = decide_route(**common, is_mutation=True)  # type: ignore[arg-type]
    assert (write.route, write.source_id) == (Route.DIRECT, "s")


@pytest.mark.parametrize("source_type", sorted(API_WRITTEN))
async def test_the_first_write_holds_the_sources_api_driver_as_its_pool(monkeypatch, source_type):
    from types import SimpleNamespace

    from provisa.api.admin import schema_query
    from provisa.api.data import pgwire_write

    added: list[tuple] = []

    class _Pools:
        def has(self, source_id: str) -> bool:
            return bool(added)

        async def add_driver(self, source_id, driver, dialect) -> None:
            added.append((source_id, type(driver).__name__, dialect, driver.is_connected))

    async def _source(source_id: str):
        return SimpleNamespace(id=source_id, mapping={})

    monkeypatch.setattr(schema_query, "_source_for_introspection", _source)
    state = SimpleNamespace(source_types={"mail": source_type}, source_pools=_Pools())
    await pgwire_write.ensure_write_pool(state, "mail")
    await pgwire_write.ensure_write_pool(state, "mail")
    assert added == [("mail", "MailWriteDriver", "postgres", True)]
