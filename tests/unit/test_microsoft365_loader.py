# Copyright (c) 2026 Kenneth Stott
# Canary: 8aa538cd-a543-4c93-ba6d-498a1bf96118
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""The Microsoft 365 source as the product reads it: the six canonical mail tables, offered
under one schema, fetched whole through Graph off the event loop and handed to the replica
write face with exactly the canonical columns; messages kept without a body are the build's
note. Graph is the stand-in of ``test_microsoft365_mail``."""

# Requirements: REQ-1923

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from provisa.core import canonical_mail as cm
from provisa.core.ir_arrow import arrow_schema
from provisa.core.models import SourceType
from provisa.microsoft365 import SOURCE_TYPE, loader
from tests.unit.test_microsoft365_mail import ACCOUNT, ROOT, FakeGraph, _html, _message

SOURCE = SimpleNamespace(id="m365", type=SOURCE_TYPE, mapping={})


def _table(name: str) -> SimpleNamespace:
    return SimpleNamespace(table_name=name)


@pytest.fixture
def fetch(monkeypatch):
    """The loader over a stand-in Graph: ``fetch.graph.table`` is what the mailbox holds."""
    fake = FakeGraph({})
    monkeypatch.setattr(loader, "http_send", lambda _client: fake.send)
    tokens: list[str] = []

    async def connect(source):
        assert source is SOURCE

        def token() -> str:
            tokens.append("asked")
            return "access-token"

        return ACCOUNT, token

    made = loader.make_microsoft365_loader(connect)
    made.graph = fake
    made.tokens = tokens
    return made


def test_the_source_type_is_one_the_product_knows():
    assert SourceType(SOURCE_TYPE) is SourceType.microsoft_365
    from provisa.events.source_loader import _ADAPTER_FETCH_ONLY as fetched
    from provisa.federation import strategy

    assert SOURCE_TYPE in fetched  # read by its loader and landed: no engine reaches it
    assert SOURCE_TYPE in strategy._MATERIALIZE_ONLY


def test_it_offers_the_six_canonical_mail_tables_as_google_workspace_does():
    from provisa.google_workspace import loader as google

    assert loader.TABLES == google.TABLES
    assert loader.SCHEMA == google.SCHEMA
    assert set(loader.TABLES) <= set(cm.TABLES)


def test_a_table_it_does_not_offer_is_refused_by_name(fetch):
    with pytest.raises(loader.UnknownMailTable, match="'events'.*messages, message_recipients"):
        asyncio.run(fetch(SOURCE, _table("events")))
    assert fetch.tokens == []  # refused before the source is signed in


def test_a_table_is_fetched_whole(fetch):
    fetch.graph.table.update(
        {f"{ROOT}/messages": [[_message(1), _message(2)], [_message(3)]], **_html(1, 2, 3)}
    )
    rows = asyncio.run(fetch(SOURCE, _table("messages")))
    assert [r["id"] for r in rows] == ["m1", "m2", "m3"]
    assert list(rows[0]) == [name for name, _ in cm.ir_columns("messages")]
    assert fetch.tokens  # each call to Graph asked for the token


@pytest.mark.parametrize("table", loader.TABLES)
def test_the_replica_is_given_exactly_the_canonical_columns(fetch, table):
    fetch.graph.table.update(
        {
            f"{ROOT}/messages": [[_message(1, hasAttachments=True)]],
            **_html(1),
            f"{ROOT}/mailFolders": [[{"id": "f-inbox", "displayName": "Inbox"}]],
            f"{ROOT}/messages/m1/attachments": [
                [{"@odata.type": "#microsoft.graph.fileAttachment", "id": "a1"}]
            ],
        }
    )
    columns = cm.ir_columns(table)
    source = fetch.replica_source(SOURCE, _table(table), columns)

    async def read():
        return [batch async for batch in source.batches(1000)]

    batches = asyncio.run(read())
    assert batches and all(batch.schema == arrow_schema(columns) for batch in batches)
    assert sum(batch.num_rows for batch in batches) >= 1
    assert source.note() is None  # every message was read whole


def test_messages_kept_without_a_body_are_the_builds_note(fetch):
    fetch.graph.table.update({f"{ROOT}/messages": [[_message(1), _message(2)]], **_html(1)})
    columns = cm.ir_columns("messages")
    source = fetch.replica_source(SOURCE, _table("messages"), columns)
    assert source.note() is None  # nothing read yet

    async def read():
        return sum([batch.num_rows async for batch in source.batches(1000)])

    assert asyncio.run(read()) == 2  # the row is kept
    note = source.note()
    assert note.code == "replication.unreadable_messages"
    assert note.params == {"count": 1, "ids": ["m2"], "more": 0}
    assert asyncio.run(read()) == 2  # a build begun again counts again, not twice
    assert source.note().params["count"] == 1


def test_a_refusal_by_graph_fails_the_read_and_is_not_a_shorter_table(fetch):
    from provisa.microsoft365.graph import Answer, GraphRefused

    fetch.graph.table.update({f"{ROOT}/messages": [[_message(1)]], **_html(1)})
    fetch.graph.queued = [
        Answer(403, {}, {"error": {"code": "ErrorAccessDenied", "message": "Access is denied."}})
    ]
    with pytest.raises(GraphRefused, match="ErrorAccessDenied"):
        asyncio.run(fetch(SOURCE, _table("messages")))
