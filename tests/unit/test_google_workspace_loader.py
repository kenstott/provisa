# Copyright (c) 2026 Kenneth Stott
# Canary: 3823d841-2a61-4a89-9fb7-223c5e670d4d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The mail tables of a Google Workspace source (REQ-1923): each is exactly its canonical
table, read whole from a mailbox. Gmail is a stand-in answering made-up mail."""

# Requirements: REQ-1923, REQ-1943
from __future__ import annotations

import base64
import datetime as dt
import logging
from types import SimpleNamespace

import pytest

from provisa.core import canonical_mail as cm
from provisa.core.declared_sensitive import CANONICAL_MAIL, declared_for
from provisa.core.ir_arrow import arrow_schema, rows_to_batch
from provisa.core.models import SourceType
from provisa.google_workspace import SOURCE_TYPE, loader
from provisa.google_workspace.gmail import GmailNotFound
from provisa.google_workspace.loader import TABLES, UnknownMailTable, table_rows
from provisa.google_workspace.settings import SECRET_KEYS, MailSettings

pytestmark = pytest.mark.asyncio

ACCOUNT = "ada@example.test"
FULL = MailSettings("full", None, (), None, False)


def _b64(text: str) -> str:
    return base64.urlsafe_b64encode(text.encode()).decode().rstrip("=")


def _mail(message_id: str, thread: str, when_ms: int, subject: str, **more) -> dict:
    headers = [
        {"name": "Subject", "value": subject},
        {"name": "From", "value": "Bo <bo@example.test>"},
        {"name": "To", "value": "ada@example.test, cy@example.test"},
        {"name": "Date", "value": "Thu, 1 Jan 2026 12:00:00 +0000"},
        {"name": "Message-ID", "value": f"<{message_id}@mail.example.test>"},
    ]
    answered = {
        "id": message_id,
        "threadId": thread,
        "labelIds": ["INBOX", "Label_7"],
        "snippet": f"snippet of {message_id}",
        "historyId": "9",
        "internalDate": str(when_ms),
        "sizeEstimate": 100,
        "payload": {
            "mimeType": "multipart/mixed",
            "filename": "",
            "headers": headers,
            "parts": [
                {
                    "partId": "0",
                    "mimeType": "text/plain",
                    "filename": "",
                    "headers": [{"name": "Content-Type", "value": "text/plain; charset=utf-8"}],
                    "body": {"size": 5, "data": _b64(f"text of {message_id}")},
                },
                {
                    "partId": "1",
                    "mimeType": "application/pdf",
                    "filename": "menu.pdf",
                    "headers": [],
                    "body": {"size": 10, "attachmentId": "h"},
                },
            ],
        },
    }
    answered.update(more)
    return answered


T0 = 1767268800000  # 2026-01-01T12:00:00Z
MAILBOX = {
    "m1": _mail("m1", "t1", T0, "Lunch?"),
    "m2": _mail("m2", "t1", T0 + 3_600_000, "Re: Lunch?"),
    "m3": _mail("m3", "t2", T0 + 60_000, "Invoice"),
}
LABELS = {
    "INBOX": {
        "id": "INBOX",
        "name": "INBOX",
        "type": "system",
        "messagesTotal": 3,
        "messagesUnread": 1,
        "threadsTotal": 2,
        "threadsUnread": 1,
        "labelListVisibility": "labelShow",
        "messageListVisibility": "show",
    },
    "CHAT": {"id": "CHAT", "name": "CHAT", "type": "system"},
    "Label_7": {
        "id": "Label_7",
        "name": "Clients",
        "type": "user",
        "labelListVisibility": "labelHide",
        "messageListVisibility": "hide",
        "color": {"textColor": "#ffffff", "backgroundColor": "#000000"},
        "messagesTotal": 2,
        "messagesUnread": 0,
        "threadsTotal": 1,
        "threadsUnread": 0,
    },
}


class FakeGmail:
    account = ACCOUNT

    def __init__(self, mailbox=None, labels=None, pages=None) -> None:
        self.mailbox = dict(MAILBOX if mailbox is None else mailbox)
        self.label_rows = dict(LABELS if labels is None else labels)
        self.pages = pages or [list(self.mailbox)]
        self.asked: list[tuple[str, str]] = []

    async def labels(self) -> list[dict]:
        return [{"id": label["id"], "name": label["name"]} for label in self.label_rows.values()]

    async def label(self, label_id: str) -> dict:
        return self.label_rows[label_id]

    async def message_ids(self, mail: MailSettings):
        for page in self.pages:
            yield page

    async def message(self, message_id: str, mail: MailSettings) -> dict:
        self.asked.append((message_id, mail.content))
        if message_id not in self.mailbox:
            raise GmailNotFound(ACCOUNT, "messages.get", 404, "notFound")
        return self.mailbox[message_id]


async def _rows(
    table: str, gmail: FakeGmail | None = None, mail: MailSettings = FULL
) -> list[dict]:
    return [row async for batch in table_rows(gmail or FakeGmail(), mail, table) for row in batch]


class TestEveryTableIsItsCanonicalTable:
    @pytest.mark.parametrize("table", TABLES)
    async def test_a_row_has_exactly_the_canonical_columns_in_their_order(self, table):
        rows = await _rows(table)
        assert rows, table
        canonical = [name for name, _ in cm.ir_columns(table)]
        for row in rows:
            assert list(row) == canonical

    @pytest.mark.parametrize("table", TABLES)
    async def test_its_values_are_of_the_canonical_types(self, table):
        columns = cm.ir_columns(table)
        batch = rows_to_batch(await _rows(table), columns, arrow_schema(columns))
        assert batch.num_rows > 0 and batch.schema.names == [name for name, _ in columns]

    @pytest.mark.parametrize("table", TABLES)
    async def test_every_row_says_whose_mailbox_and_which_provider(self, table):
        assert {(r["provider"], r["account"]) for r in await _rows(table)} == {("google", ACCOUNT)}

    @pytest.mark.parametrize("table", TABLES)
    async def test_a_column_google_does_not_populate_is_null(self, table):
        for row in await _rows(table):
            for column in cm.TABLES[table].columns:
                if not column.populated_by(cm.GOOGLE):
                    assert row[column.name] is None, (table, column.name)

    async def test_the_source_offers_the_six_mail_tables_and_they_are_canonical(self):
        assert set(TABLES) == {
            "messages",
            "message_recipients",
            "folders",
            "message_folders",
            "threads",
            "attachments",
        }
        assert set(TABLES) <= set(cm.TABLES)

    async def test_a_table_it_does_not_offer_is_refused_by_name(self):
        with pytest.raises(UnknownMailTable, match="no table 'events'"):
            await _rows("events")


class TestMessages:
    async def test_each_message_is_a_row_with_its_text(self):
        rows = await _rows("messages")
        assert [(r["id"], r["subject"], r["body_text"]) for r in rows] == [
            ("m1", "Lunch?", "text of m1"),
            ("m2", "Re: Lunch?", "text of m2"),
            ("m3", "Invoice", "text of m3"),
        ]
        assert rows[0]["received_at"] == dt.datetime(2026, 1, 1, 12, 0)
        assert rows[0]["flag_status"] == cm.canonical("flag_status", "none")

    async def test_every_page_of_the_mailbox_is_read(self):
        gmail = FakeGmail(pages=[["m1"], ["m2", "m3"]])
        assert [r["id"] for r in await _rows("messages", gmail)] == ["m1", "m2", "m3"]

    async def test_only_the_tables_that_need_a_messages_text_ask_for_it(self):
        asked = {}
        for table in (
            "messages",
            "attachments",
            "message_recipients",
            "message_folders",
            "threads",
        ):
            gmail = FakeGmail()
            await _rows(table, gmail)
            asked[table] = {content for _, content in gmail.asked}
        assert asked == {
            "messages": {"full"},
            "attachments": {"full"},
            "message_recipients": {"headers"},
            "message_folders": {"headers"},
            "threads": {"headers"},
        }

    async def test_a_source_read_as_headers_only_never_asks_for_text(self):
        gmail = FakeGmail()
        await _rows("messages", gmail, MailSettings("headers", None, (), None, False))
        assert {content for _, content in gmail.asked} == {"headers"}

    async def test_a_message_deleted_since_it_was_listed_is_not_a_row(self):
        gmail = FakeGmail(pages=[["m1", "gone", "m3"]])
        assert [r["id"] for r in await _rows("messages", gmail)] == ["m1", "m3"]

    async def test_a_message_with_an_unreadable_part_keeps_its_row_and_is_named(self, caplog):
        broken = _mail("m9", "t9", T0, "Kept")
        broken["payload"]["parts"][0]["headers"] = [
            {"name": "Content-Type", "value": "text/plain; charset=x-made-up"}
        ]
        gmail = FakeGmail(mailbox={**MAILBOX, "m9": broken})
        with caplog.at_level(logging.WARNING, logger=loader.__name__):
            rows = await _rows("messages", gmail)
        kept = next(r for r in rows if r["id"] == "m9")
        assert (kept["subject"], kept["body_text"], kept["from_address"]) == (
            "Kept",
            None,
            "bo@example.test",
        )
        assert len(rows) == 4
        (said,) = [r.getMessage() for r in caplog.records]
        assert "1 message(s)" in said and "m9" in said and ACCOUNT in said

    async def test_a_mailbox_that_reads_clean_says_nothing(self, caplog):
        with caplog.at_level(logging.WARNING, logger=loader.__name__):
            await _rows("messages")
        assert caplog.records == []


class TestItsOtherTables:
    async def test_recipients_folders_and_attachments_are_rows_of_each_message(self):
        recipients = await _rows("message_recipients")
        assert [(r["message_id"], r["kind"], r["address"]) for r in recipients][:3] == [
            ("m1", "from", "bo@example.test"),
            ("m1", "to", "ada@example.test"),
            ("m1", "to", "cy@example.test"),
        ]
        assert {r["kind"] for r in recipients} <= set(cm.ENUMS["recipient_kind"].values)
        folders = await _rows("message_folders")
        assert len(folders) == 6 and folders[0]["folder_id"] == "INBOX"
        attachments = await _rows("attachments")
        assert [(a["message_id"], a["id"], a["name"]) for a in attachments] == [
            ("m1", "1", "menu.pdf"),
            ("m2", "1", "menu.pdf"),
            ("m3", "1", "menu.pdf"),
        ]
        assert {a["kind"] for a in attachments} == {cm.canonical("attachment_kind", "file")}

    async def test_a_thread_is_its_first_subject_its_last_snippet_and_its_span(self):
        threads = {t["id"]: t for t in await _rows("threads")}
        assert set(threads) == {"t1", "t2"}
        t1 = threads["t1"]
        assert (t1["subject"], t1["snippet"], t1["message_count"]) == ("Lunch?", "snippet of m2", 2)
        assert t1["first_message_at"] == dt.datetime(2026, 1, 1, 12, 0)
        assert t1["last_message_at"] == dt.datetime(2026, 1, 1, 13, 0)
        assert threads["t2"]["message_count"] == 1

    async def test_a_thread_is_the_same_whatever_order_its_messages_arrive_in(self):
        forwards = await _rows("threads", FakeGmail(pages=[["m1", "m2", "m3"]]))
        backwards = await _rows("threads", FakeGmail(pages=[["m3", "m2"], ["m1"]]))
        assert {t["id"]: t for t in forwards} == {t["id"]: t for t in backwards}


class TestFolders:
    async def test_a_label_is_a_folder_with_its_counts(self):
        folders = {f["id"]: f for f in await _rows("folders")}
        inbox = folders["INBOX"]
        assert (inbox["kind"], inbox["role"], inbox["name"]) == ("system", "inbox", "INBOX")
        assert (inbox["total_count"], inbox["unread_count"]) == (3, 1)
        assert (inbox["threads_total"], inbox["threads_unread"]) == (2, 1)
        assert (inbox["is_hidden"], inbox["label_list_visibility"]) == (False, "show")
        assert inbox["parent_id"] is None and inbox["child_count"] is None

    async def test_a_label_of_the_users_own_has_no_role(self):
        clients = {f["id"]: f for f in await _rows("folders")}["Label_7"]
        assert (clients["kind"], clients["role"], clients["name"]) == ("user", None, "Clients")
        assert (clients["is_hidden"], clients["label_list_visibility"]) == (True, "hide")
        assert clients["message_list_visibility"] == "hide"
        assert (clients["color_text"], clients["color_background"]) == ("#ffffff", "#000000")

    async def test_a_system_label_google_never_documented_is_other(self):
        chat = {f["id"]: f for f in await _rows("folders")}["CHAT"]
        assert (chat["kind"], chat["role"]) == ("system", "other")
        assert chat["is_hidden"] is None and chat["total_count"] is None

    async def test_a_value_google_states_that_the_translation_does_not_list_fails_the_read(self):
        odd = {"X": {"id": "X", "name": "X", "type": "user", "labelListVisibility": "labelMaybe"}}
        with pytest.raises(cm.UnmappedValue) as raised:
            await _rows("folders", FakeGmail(labels=odd))
        assert raised.value.enum == "label_list_visibility" and raised.value.value == "labelMaybe"


class TestTheSourceKind:
    async def test_it_is_a_source_type_read_by_its_loader_and_landed(self):
        from provisa.events.source_loader import _ADAPTER_FETCH_ONLY
        from provisa.federation.strategy import _MATERIALIZE_ONLY

        assert SourceType(SOURCE_TYPE) is SourceType.google_workspace
        assert SOURCE_TYPE in _ADAPTER_FETCH_ONLY and SOURCE_TYPE in _MATERIALIZE_ONLY

    async def test_its_credentials_are_the_ones_kept_in_the_vault(self):
        from provisa.api.admin.schema_common import SOURCE_MAPPING_SECRET_KEYS

        assert SOURCE_MAPPING_SECRET_KEYS[SOURCE_TYPE] == SECRET_KEYS

    async def test_what_it_declares_sensitive_are_columns_of_the_canonical_tables(self):
        for table, declared in CANONICAL_MAIL.items():
            assert table in TABLES
            names = {name for name, _ in cm.ir_columns(table)}
            assert declared.columns <= names, (table, declared.columns - names)
        assert declared_for(SOURCE_TYPE, "messages") is CANONICAL_MAIL["messages"]

    async def test_settings_that_cannot_be_a_source_are_refused_in_the_setups_terms(self):
        import json

        from provisa.api.admin.schema_common import google_workspace_refusal

        def source(**mapping) -> SimpleNamespace:
            return SimpleNamespace(id="mail", type=SOURCE_TYPE, mapping_json=json.dumps(mapping))

        refused = google_workspace_refusal(
            source(accounts=["ada@example.test"], resources=["mail"])
        )
        assert refused is not None and refused.code == "schema.google_workspace_invalid"
        assert "sign_in must be one of" in refused.message
        good = source(
            accounts=["ada@example.test"],
            resources=["mail"],
            sign_in="service_account",
            service_account_key="${secret:k}",
            mail_content="full",
        )
        assert google_workspace_refusal(good) is None
        other = SimpleNamespace(id="pg", type="postgresql", mapping_json="{}")
        assert google_workspace_refusal(other) is None


class TestTheLoader:
    @pytest.fixture
    def read(self, monkeypatch):
        seen: dict = {}

        class Recording(FakeGmail):
            def __init__(self, account, token, client) -> None:
                super().__init__()
                seen["account"], seen["token"] = account, token

        def token_for(auth) -> str:
            seen["auth"] = auth
            return "made-up-access-token"

        monkeypatch.setattr(loader, "Gmail", Recording)
        monkeypatch.setattr(
            loader, "resolve_secrets", lambda v: v.replace("${secret:k}", "resolved-key")
        )
        monkeypatch.setattr("provisa.api_source.oauth_grants.access_token", token_for)
        return seen

    @staticmethod
    def _source() -> SimpleNamespace:
        mapping = {
            "accounts": [ACCOUNT],
            "resources": ["mail"],
            "sign_in": "service_account",
            "service_account_key": "${secret:k}",
            "mail_content": "full",
        }
        return SimpleNamespace(id="mail", mapping=mapping)

    async def test_a_table_is_read_with_the_sources_own_credential(self, read):
        fetch = loader.make_google_workspace_loader()
        rows = await fetch(self._source(), SimpleNamespace(table_name="messages"))
        assert [r["id"] for r in rows] == ["m1", "m2", "m3"]
        assert read["account"] == ACCOUNT
        assert await read["token"]() == "made-up-access-token"
        assert (read["auth"].key, read["auth"].subject) == ("resolved-key", ACCOUNT)

    async def test_a_replica_is_built_from_the_same_read_in_the_canonical_columns(self, read):
        fetch = loader.make_google_workspace_loader()
        columns = cm.ir_columns("threads")
        source = fetch.replica_source(
            self._source(), SimpleNamespace(table_name="threads"), columns
        )
        batches = [batch async for batch in source.batches(1000)]
        assert sum(b.num_rows for b in batches) == 2
        assert batches[0].schema.names == [name for name, _ in columns]
