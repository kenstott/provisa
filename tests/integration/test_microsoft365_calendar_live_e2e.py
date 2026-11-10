# Copyright (c) 2026 Kenneth Stott
# Canary: 6d2b8f31-9e4a-4c57-a0b3-f1c7e5d2a948
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Live Microsoft 365 calendars and To Do tasks (REQ-1994), read and written, against a real
tenant.

What it proves that a stand-in Graph cannot: the client's application permissions let it read
and write a mailbox's events and tasks; a write made through the source's write route -- an event
with its reminder, a weekly series written as an RRULE and read back as one, one occurrence moved
and another cancelled, a task with a due time and a reminder, done, then removed -- is what the
source's tables read back, row for row; a meeting change Graph would announce is refused while
the source tells nobody, and made once it does; and the change token of the source's calendars
and task lists moves when they are written.

The tenant has one licensed mailbox, the maintainer's own (maintainer 2026-11-10), and it is the
one written to. The test makes a calendar and a To Do list of its own there, under names unique
to the run, writes only into them -- never the default calendar or list, so the maintainer's own
events and tasks are untouched -- and removes both when it ends, whether it passed or not. A
calendar or list an earlier run that did not end left behind (its name carries the run marker)
is removed first. No server, engine or
container is used, so run it with the stack switched off:

    PYTEST_NO_DOCKER=1 .venv/bin/python3 -m pytest \\
        tests/integration/test_microsoft365_calendar_live_e2e.py -o addopts="" -q

Credentials
-----------
The same as tests/integration/test_microsoft365_live_e2e.py, with the app registration also
granted these application permissions, with admin consent:

    Calendars.ReadWrite
    Tasks.ReadWrite.All

  M365_TENANT_ID, M365_CLIENT_ID, M365_CLIENT_SECRET, M365_TEST_MAILBOXES (the licensed mailbox
  written to; only the first address is used).

A meeting needs a second person, and the tenant has no second licensed mailbox: the attendee is
the tenant's shared mailbox invites@provisa.dev (:data:`ATTENDEE`), which needs no license and
receives the invitation. Every assertion of the meeting test is on the organizer's side -- the
refusal, the attendee row as written, which change is told -- so a shared mailbox suffices.

In CI it runs in the mail test's lane, microsoft_365, when a dispatch asks for it
(include_microsoft_365); a credential that is not set fails it by name.
"""

# Requirements: REQ-1994
from __future__ import annotations

import asyncio
import datetime as dt
import os
import uuid
from contextlib import asynccontextmanager
from types import SimpleNamespace
from urllib.parse import quote

import httpx
import pytest

from tests.env_creds import unset

pytestmark = [
    pytest.mark.integration,
    pytest.mark.requires_warehouse,
    pytest.mark.requires_microsoft_365,
]

_ENV = ("M365_TENANT_ID", "M365_CLIENT_ID", "M365_CLIENT_SECRET", "M365_TEST_MAILBOXES")
_HAVE_CREDS = all(os.environ.get(v) for v in _ENV)
pytestmark.append(pytest.mark.skipif(not _HAVE_CREDS, reason=f"not set: {unset(*_ENV)}"))

from provisa.core import canonical_mail as cm  # noqa: E402
from provisa.federation.mail_write import MailWriteDriver, WriteRefused  # noqa: E402
from provisa.microsoft365 import calendar, directory, loader  # noqa: E402
from provisa.microsoft365 import settings as m365  # noqa: E402
from provisa.microsoft365.calendar_write import GraphWriter  # noqa: E402
from provisa.microsoft365.graph import Graph, http_send  # noqa: E402

_MARK = "provisa-live-calendar"
_RUN = f"{_MARK}-{uuid.uuid4().hex[:8]}"
_SOURCE = SimpleNamespace(id="m365-live-calendar", mapping={})


def _mailboxes() -> tuple[str, ...]:
    return tuple(
        a.strip().lower() for a in os.environ["M365_TEST_MAILBOXES"].split(",") if a.strip()
    )


ADA = _mailboxes()[0] if _HAVE_CREDS else ""
#: The meeting's attendee: the tenant's shared mailbox, which needs no license.
ATTENDEE = "invites@provisa.dev"
ROOT = f"/users/{quote(ADA, safe='')}"


@pytest.fixture(scope="module")
def token() -> directory.ClientToken:
    return directory.ClientToken(
        m365.token_url(os.environ["M365_TENANT_ID"]),
        os.environ["M365_CLIENT_ID"],
        os.environ["M365_CLIENT_SECRET"],
    )


def _graph(client: httpx.Client, token) -> Graph:
    return Graph(http_send(client), token).for_mailbox(ADA)


def _driver(token, send_updates: bool) -> MailWriteDriver:
    async def accounts() -> tuple[str, ...]:
        return (ADA,)

    @asynccontextmanager
    async def writer_for(_address: str):
        with httpx.Client() as client:
            yield GraphWriter(_graph(client, token), ADA, send_updates)

    return MailWriteDriver(cm.MICROSOFT, accounts, writer_for)


def _run(token, sql: str, params: list | None = None, *, send_updates: bool = False) -> int:
    result = asyncio.run(_driver(token, send_updates).execute(sql, params))
    assert result.rowcount is not None
    return result.rowcount


def _rows(token, table: str) -> list[dict]:
    async def connect(_source):
        return loader.Reading((ADA,), token, organisation=True)

    load = loader.make_microsoft365_loader(connect)
    return asyncio.run(load(_SOURCE, SimpleNamespace(table_name=table)))


def _sweep(token, everything: bool) -> None:
    with httpx.Client() as client:
        graph = _graph(client, token)
        for item in calendar.calendars(graph, ADA):
            name = item.get("name") or ""
            if name.startswith(_MARK) and (everything or not name.startswith(_RUN)):
                graph.write("DELETE", f"{ROOT}/calendars/{quote(item['id'], safe='')}")
        for page in graph.pages(f"{ROOT}/todo/lists"):
            for item in page:
                name = item.get("displayName") or ""
                if name.startswith(_MARK) and (everything or not name.startswith(_RUN)):
                    graph.write("DELETE", f"{ROOT}/todo/lists/{quote(item['id'], safe='')}")


@pytest.fixture(scope="module")
def made(token):
    """A calendar and a To Do list of this run's own, removed when the module ends."""
    _sweep(token, everything=False)
    with httpx.Client() as client:
        graph = _graph(client, token)
        made_calendar = graph.write("POST", f"{ROOT}/calendars", {"name": f"{_RUN} calendar"})
        made_list = graph.write("POST", f"{ROOT}/todo/lists", {"displayName": f"{_RUN} list"})
    try:
        yield SimpleNamespace(calendar=made_calendar["id"], task_list=made_list["id"])
    finally:
        _sweep(token, everything=True)


def _ours(rows: list[dict], made) -> list[dict]:
    return [r for r in rows if r.get("calendar_id") == made.calendar]


def _at(hours: int) -> str:
    start = dt.datetime.now(dt.UTC).replace(minute=0, second=0, microsecond=0)
    return (start + dt.timedelta(days=2, hours=hours)).isoformat()


def _token_of(token, table: str) -> str:
    with httpx.Client() as client:
        return calendar.token_of(calendar.change_lines(_graph(client, token), ADA, table, 30))


def test_an_event_with_its_reminder_reads_back(token, made):
    before = _token_of(token, "events")
    assert (
        _run(
            token,
            "INSERT INTO events (calendar_id, title, start_at, end_at, is_reminder_on, "
            "reminder_minutes_before_start) VALUES ($1, $2, $3, $4, true, 7)",
            [made.calendar, f"{_RUN} single", _at(9), _at(10)],
        )
        == 1
    )
    (event,) = _ours(_rows(token, "events"), made)
    assert (event["provider"], event["account"], event["kind"]) == ("microsoft", ADA, "single")
    assert (event["is_reminder_on"], event["reminder_minutes_before_start"]) == (True, 7)
    assert _token_of(token, "events") != before
    assert (
        _run(token, "UPDATE events SET title = $1 WHERE id = $2", [f"{_RUN} renamed", event["id"]])
        == 1
    )
    (renamed,) = _ours(_rows(token, "events"), made)
    assert renamed["title"] == f"{_RUN} renamed"
    assert _run(token, "DELETE FROM events WHERE id = $1", [event["id"]]) == 1
    assert _ours(_rows(token, "events"), made) == []


def test_a_series_is_one_rrule_with_one_occurrence_moved_and_another_cancelled(token, made):
    _run(
        token,
        "INSERT INTO events (calendar_id, title, start_at, end_at, recurrence) "
        "VALUES ($1, $2, $3, $4, 'RRULE:FREQ=DAILY;COUNT=3')",
        [made.calendar, f"{_RUN} series", _at(11), _at(12)],
    )
    (series,) = [r for r in _ours(_rows(token, "events"), made) if r["kind"] == "series_master"]
    assert series["recurrence"] == "RRULE:FREQ=DAILY;COUNT=3"
    second = series["start_at"] + dt.timedelta(days=1)
    third = series["start_at"] + dt.timedelta(days=2)
    assert (
        _run(
            token,
            "UPDATE events SET title = $1 WHERE series_id = $2 AND original_start_at = $3",
            [f"{_RUN} series moved", series["id"], second.isoformat()],
        )
        == 1
    )
    assert (
        _run(
            token,
            "DELETE FROM events WHERE series_id = $1 AND original_start_at = $2",
            [series["id"], third.isoformat()],
        )
        == 1
    )
    rows = _ours(_rows(token, "events"), made)
    (moved,) = [r for r in rows if r["kind"] == "exception"]
    assert (moved["series_id"], moved["original_start_at"], moved["title"]) == (
        series["id"],
        second,
        f"{_RUN} series moved",
    )
    (master,) = [r for r in rows if r["kind"] == "series_master"]
    assert master["cancelled_occurrences"], "Graph names the cancelled occurrence on the master"


def test_a_meeting_is_changed_only_when_the_source_tells_its_attendees(token, made):
    _run(
        token,
        "INSERT INTO events (calendar_id, title, start_at, end_at) VALUES ($1, $2, $3, $4)",
        [made.calendar, f"{_RUN} meeting", _at(14), _at(15)],
    )
    (meeting,) = [r for r in _ours(_rows(token, "events"), made) if r["title"].endswith("meeting")]
    with pytest.raises(WriteRefused, match="tells the attendees"):
        _run(
            token,
            "INSERT INTO event_attendees (event_id, address) VALUES ($1, $2)",
            [meeting["id"], ATTENDEE],
        )
    assert (
        _run(
            token,
            "INSERT INTO event_attendees (event_id, address) VALUES ($1, $2)",
            [meeting["id"], ATTENDEE],
            send_updates=True,
        )
        == 1
    )
    people = [r for r in _rows(token, "event_attendees") if r["event_id"] == meeting["id"]]
    assert [p["address"].lower() for p in people] == [ATTENDEE]
    # Now a meeting: its own reminder changes silently, its title does not.
    assert (
        _run(token, "UPDATE events SET is_reminder_on = false WHERE id = $1", [meeting["id"]]) == 1
    )
    with pytest.raises(WriteRefused, match="tells the attendees"):
        _run(token, "UPDATE events SET title = 'x' WHERE id = $1", [meeting["id"]])


def test_a_task_with_a_due_time_and_reminder_is_done_and_removed(token, made):
    before = _token_of(token, "tasks")
    due = (dt.datetime.now(dt.UTC) + dt.timedelta(days=3)).replace(
        hour=9, minute=0, second=0, microsecond=0
    )
    _run(
        token,
        "INSERT INTO tasks (task_list_id, title, notes, due_at, reminder_at, importance) "
        "VALUES ($1, $2, 'n', $3, $4, 'high')",
        [
            made.task_list,
            f"{_RUN} task",
            due.isoformat(),
            (due - dt.timedelta(hours=1)).isoformat(),
        ],
    )
    (task,) = [r for r in _rows(token, "tasks") if r["task_list_id"] == made.task_list]
    assert (task["status"], task["importance"], task["is_reminder_on"]) == ("open", "high", True)
    assert task["reminder_at"] == due - dt.timedelta(hours=1)
    assert task["due_date"] == due.date()
    assert _token_of(token, "tasks") != before
    assert _run(token, "UPDATE tasks SET status = 'done' WHERE id = $1", [task["id"]]) == 1
    (done,) = [r for r in _rows(token, "tasks") if r["task_list_id"] == made.task_list]
    assert done["status"] == "done"
    assert _run(token, "DELETE FROM tasks WHERE id = $1", [task["id"]]) == 1
    assert not [r for r in _rows(token, "tasks") if r["task_list_id"] == made.task_list]
