# Copyright (c) 2026 Kenneth Stott
# Canary: a3ede9d4-3959-4e60-a3a1-c0ff509a6b30
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Live Microsoft 365 source of the organisation's mailboxes (REQ-1923), against a real tenant.

What it proves that a stand-in Graph cannot: Microsoft issues the client a token by the
client-credentials grant; the directory lists the test mailboxes; ONE read of a mailbox feeds
the five message tables with the canonical columns; a mailbox the client may not read is left
out and named in the build's note; and a delta link can be followed twice.

Nothing is written to the tenant. No server, engine or container is used: the loader is driven
directly, so run it with the stack switched off:

    PYTEST_NO_DOCKER=1 .venv/bin/python3 -m pytest \\
        tests/integration/test_microsoft365_live_e2e.py -o addopts="" -q

Credentials
-----------
There is no Microsoft 365 emulator: a real tenant and an app registration with application
permissions are required. Read from the environment (``.env`` through tests/env_creds.py):

  M365_TENANT_ID             the directory (tenant) id
  M365_CLIENT_ID             the application (client) id
  M365_CLIENT_SECRET         the client secret's value
  M365_TEST_MAILBOXES        comma-separated addresses the client may read; SMALL mailboxes,
                             each holding at least one message -- a whole mailbox is read
  M365_TEST_MAILBOX_OUTSIDE  optional: one address the client may NOT read (outside its
                             Exchange scope, or a user with no mailbox)

In CI it runs only in its own lane, microsoft_365 (scripts/ci/run_lane.py), when a dispatch of
integration-suite.yml asks for it (include_microsoft_365; the tenant is the maintainer's own,
maintainer 2026-11-10); that job is handed the M365_* repo secrets, fails by name when one of the
four required is not set, and runs with PYTEST_NO_DOCKER=1. Like every warehouse test a credential
that is not set fails it by name rather than passing it over (tests/skip_is_failure.py).

Neither the client secret nor an access token is put in an assertion message, a log line or
captured output; the last test asserts it.
"""

# Requirements: REQ-1923
from __future__ import annotations

import asyncio
import logging
import os

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
from provisa.federation.data_replicator import MAILBOXES_LEFT_OUT  # noqa: E402
from provisa.microsoft365 import directory, loader, mail  # noqa: E402
from provisa.microsoft365 import settings as m365  # noqa: E402
from provisa.microsoft365.graph import BASE_URL, Graph, http_send  # noqa: E402

#: How long one whole-mailbox read may take before the test says the mailbox is not a small one.
_READ_SECONDS = 600


def _inside() -> tuple[str, ...]:
    return tuple(
        a.strip().lower() for a in os.environ["M365_TEST_MAILBOXES"].split(",") if a.strip()
    )


def _outside() -> str | None:
    address = os.environ.get("M365_TEST_MAILBOX_OUTSIDE", "").strip().lower()
    return address or None


@pytest.fixture(scope="module")
def token() -> directory.ClientToken:
    """The client's token source, as the product builds it: the secret goes nowhere else."""
    return directory.ClientToken(
        m365.token_url(os.environ["M365_TENANT_ID"]),
        os.environ["M365_CLIENT_ID"],
        os.environ["M365_CLIENT_SECRET"],
    )


def _secrets(token: directory.ClientToken) -> tuple[str, str]:
    return os.environ["M365_CLIENT_SECRET"], token()


def _read(
    accounts: tuple[str, ...],
    token: directory.ClientToken,
    kept: list | None = None,
    since=None,
):
    """One group read of ``accounts``: (rows per table as arrow batches, the build's notes).
    With ``kept``, the read is then told its build completed, and what it keeps is added.
    With ``since`` (a date), nothing received before it is held."""

    async def keep(rows):
        assert kept is not None
        kept.append(rows)

    async def connect(_source):
        return loader.Reading(
            accounts,
            token,
            organisation=True,
            at_once=2,
            keep=None if kept is None else keep,
            since=since,
        )

    load = loader.make_microsoft365_loader(connect)
    columns = {table: cm.ir_columns(table) for table in mail.MESSAGE_TABLES}
    group = load.replica_group_source(None, mail.MESSAGE_TABLES, columns)

    async def read():
        got: dict[str, list] = {table: [] for table in mail.MESSAGE_TABLES}
        async for table, batch in group.batches(1000):
            got[table].append(batch)
        if kept is not None:
            await group.committed()
        return got

    got = asyncio.run(asyncio.wait_for(read(), _READ_SECONDS))
    return got, group.notes()


def test_microsoft_issues_the_client_a_token_naming_its_permissions(token):
    roles = directory.carried(token())
    # Stated without the token: only whether it could be read, and the permissions' names.
    assert roles is not None, "the access token is not one whose permissions can be read"
    # Mail is read either by the Mail.Read permission or by an Exchange role over a scope of
    # mailboxes, which puts no permission in the token: neither is required here. The read
    # below is what shows mail can be read.
    print(f"application permissions in the token: {sorted(roles) or '(none)'}")


def test_the_list_choice_names_its_mailboxes_and_the_directory_knows_them(token):
    chosen = m365.Mailboxes(m365.LIST, addresses=_inside())
    with httpx.Client() as http:
        graph = Graph(http_send(http), token)
        assert directory.mailboxes(graph, chosen) == sorted(set(_inside()))
        everyone = directory.mailboxes(graph, m365.Mailboxes(m365.EVERYONE))
    missing = sorted(set(_inside()) - set(everyone))
    assert not missing, f"not listed by the directory as enabled members: {missing}"


def test_one_read_of_a_mailbox_feeds_the_five_tables_with_the_canonical_columns(token):
    account = _inside()[0]
    got, notes = _read((account,), token)
    for table in mail.MESSAGE_TABLES:
        wanted = [name for name, _type in cm.ir_columns(table)]
        for batch in got[table]:
            assert batch.schema.names == wanted, f"{table}: columns differ from the canonical ones"
            rows = batch.to_pylist()
            assert {row["account"] for row in rows} <= {account}, f"{table}: another account's rows"
            assert {row["provider"] for row in rows} <= {"microsoft_365"}, f"{table}: provider"
    counts = {table: sum(b.num_rows for b in got[table]) for table in mail.MESSAGE_TABLES}
    print(f"rows read: {counts}; notes: {[(n.code, n.params) for n in notes]}")
    # A test mailbox holds at least one message: it has a folder, a thread, and a sender or a
    # recipient. Attachments may be none.
    for table in ("messages", "message_recipients", "message_folders", "threads"):
        assert counts[table] > 0, f"{table}: no rows from a mailbox said to hold a message"
    assert counts["threads"] <= counts["messages"]
    assert not [n for n in notes if n.code == MAILBOXES_LEFT_OUT], "the test mailbox was left out"


def test_nothing_received_before_the_sources_date_is_held(token):
    """``mail_since``: with the date set at the received day of the mailbox's newest message,
    every message held was received on or after it, the older ones are in no table, and a
    date after every message holds none. Written against the reader; not run here."""
    import datetime as dt

    account = _inside()[0]
    everything, _notes = _read((account,), token)
    received = sorted(
        row["received_at"]
        for batch in everything["messages"]
        for row in batch.to_pylist()
        if row["received_at"] is not None
    )
    assert received, "the test mailbox holds no message with a received time"
    newest = received[-1].date()
    held, _notes = _read((account,), token, since=newest)
    rows = [row for batch in held["messages"] for row in batch.to_pylist()]
    assert rows, "the newest message is received on the date itself, and is held"
    assert all(r["received_at"] is None or r["received_at"].date() >= newest for r in rows)
    ids = {row["id"] for row in rows}
    for table in ("message_recipients", "message_folders", "attachments"):
        others = {r["message_id"] for b in held[table] for r in b.to_pylist()}
        assert others <= ids, f"{table}: rows of a message older than the date"
    assert sum(r["message_count"] for b in held["threads"] for r in b.to_pylist()) == len(rows)
    print(f"held with mail_since={newest}: {len(rows)} of {len(received)} messages")
    none, _notes = _read((account,), token, since=newest + dt.timedelta(days=1))
    assert sum(b.num_rows for b in none["messages"]) == 0


# The outside mailbox is optional: the test exists only when one is named. It is not a skip,
# which in this lane would be reported as a credential that was not supplied.
if os.environ.get("M365_TEST_MAILBOX_OUTSIDE", "").strip():

    def test_a_mailbox_the_client_may_not_read_is_left_out_and_named(token):
        outside = _outside()
        assert outside is not None
        inside = _inside()[0]
        got, notes = _read(tuple(sorted({inside, outside})), token)
        left_out = [n for n in notes if n.code == MAILBOXES_LEFT_OUT]
        assert len(left_out) == 1, f"notes were {[n.code for n in notes]}"
        assert left_out[0].params["accounts"] == [outside] and left_out[0].params["count"] == 1
        accounts = {row["account"] for b in got["messages"] for row in b.to_pylist()}
        assert accounts == {inside}


def test_a_completed_read_keeps_its_links_and_nothing_has_changed_right_after(token):
    """The read is made of delta rounds, one for each folder; what it keeps answers "unchanged"
    when asked at once (a quiet test mailbox), with a handful of requests and no listing."""
    from provisa.federation.mail_sync_state import DELTA

    account = _inside()[0]
    kept: list[dict] = []
    _read((account,), token, kept)
    (rows,) = kept
    assert {(a, kind) for a, _scope, kind in rows} == {(account, DELTA)}
    scopes = {scope for _a, scope, _k in rows}
    assert mail.MAILBOX in scopes and len(scopes) > 1, "no folder's link was kept"
    print(f"links kept: {len(rows)} (one for each folder, one for the list of folders)")

    async def kept_rows():
        return rows

    async def connect(_source):
        return loader.Reading((account,), token, organisation=True, kept=kept_rows)

    answer = asyncio.run(loader.unchanged(connect, None))
    assert answer == loader.UNCHANGED, "a mailbox just read was reported as changed"


def test_what_microsofts_pages_do_not_say_about_delta(token):
    """Four facts the reader rests on that the documentation leaves out. Each is asserted, so
    a tenant where one does not hold says which."""
    account = _inside()[0]
    immutable = {"Prefer": 'IdType="ImmutableId", odata.maxpagesize=1'}
    with httpx.Client(timeout=60) as http:

        def get(url: str, params: dict | None = None, headers: dict | None = None):
            return http.get(
                url,
                params=params,
                headers={"Authorization": f"Bearer {token()}", **(headers or {})},
            )

        root = f"{BASE_URL}/users/{account}"
        # 1. The page size asked for by header is honoured on a delta round.
        first = get(f"{root}/mailFolders/inbox/messages/delta", {"$select": "id"}, immutable)
        assert first.status_code == 200, f"{first.status_code} from the first delta page"
        items = [m for m in first.json()["value"] if "@removed" not in m]
        assert len(first.json()["value"]) <= 1, "odata.maxpagesize was not honoured"
        # 2. Ids on a delta round are the immutable ones when they are asked for: the same id
        #    names the message on the plain route under the same preference.
        if items:
            same = get(f"{root}/messages/{items[0]['id']}", {"$select": "id"}, immutable)
            assert same.status_code == 200, "a delta id is not an id of the message route"
            assert same.json()["id"] == items[0]["id"], "delta ids are not the immutable ids"
        # 3. The folder delta lists every folder the walk of the tree finds, hidden included.
        with_token = Graph(http_send(http), token)
        walked = {f["id"] for page in mail.Mailbox(with_token, account)._folders() for f in page}
        listed: set[str] = set()
        page = get(
            f"{root}/mailFolders/delta", {"$select": "id"}, {"Prefer": 'IdType="ImmutableId"'}
        )
        while True:
            assert page.status_code == 200, f"{page.status_code} from the folder delta"
            body = page.json()
            listed |= {f["id"] for f in body["value"] if "@removed" not in f}
            if "@odata.nextLink" not in body:
                break
            page = get(body["@odata.nextLink"], None, {"Prefer": 'IdType="ImmutableId"'})
        print(f"folders: {len(walked)} by the walk, {len(listed)} by the folder delta")
        missing = len(walked - listed)
        assert not missing, f"{missing} folder(s) the walk finds are not in the folder delta"
        # 4. A link Graph does not honour is refused, not answered as "nothing changed".
        bogus = get(f"{root}/mailFolders/inbox/messages/delta", {"$deltatoken": "not-a-token"})
        code = (bogus.json().get("error") or {}).get("code")
        print(f"a link Graph does not honour answers: {bogus.status_code} {code}")
        assert 400 <= bogus.status_code < 500, "an invalid link was not refused"


def test_a_delta_link_can_be_followed_twice(token):
    """The probe follows a stored delta link without storing a new one, so the same link must
    answer a second time (Microsoft's pages do not say; this is where it is seen)."""
    account = _inside()[0]
    with httpx.Client(timeout=60) as http:

        def get(url: str, params: dict | None = None) -> dict:
            reply = http.get(url, params=params, headers={"Authorization": f"Bearer {token()}"})
            assert reply.status_code == 200, f"{reply.status_code} from the delta call"
            return reply.json()

        page = get(
            f"{BASE_URL}/users/{account}/mailFolders/inbox/messages/delta", {"$select": "id"}
        )
        rounds = 0
        while "@odata.nextLink" in page:
            rounds += 1
            assert rounds < 500, "the test mailbox's inbox is not a small one"
            page = get(page["@odata.nextLink"])
        link = page["@odata.deltaLink"]
        first, second = get(link), get(link)
    assert "@odata.deltaLink" in first and "@odata.deltaLink" in second
    assert [m["id"] for m in first["value"]] == [m["id"] for m in second["value"]]


def test_no_secret_or_token_is_logged_or_printed(token, caplog, capsys):
    secret, access = _secrets(token)
    with caplog.at_level(logging.DEBUG):
        accounts = tuple(sorted({_inside()[0], *([_outside()] if _outside() else [])}))
        _read(accounts, token)
        with httpx.Client() as http:
            directory.mailboxes(Graph(http_send(http), token), m365.Mailboxes(m365.EVERYONE))
    shown = capsys.readouterr()
    said = caplog.text + shown.out + shown.err + repr(token)
    # Each named on its own, with nothing of it in the message.
    assert secret not in said, "the client secret is in a log line or in printed output"
    assert access not in said, "an access token is in a log line or in printed output"
