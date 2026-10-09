# Copyright (c) 2026 Kenneth Stott
# Canary: 57e81913-0d77-4f1a-800b-d79bd18d4b66
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A Gmail message read into the canonical mail facts (REQ-1923). Every message here is made
up, in the shape Gmail's discovery document gives ``messages.get``."""

# Requirements: REQ-1923
from __future__ import annotations

import base64
import datetime as dt

import pytest

from provisa.google_workspace.mail_rows import message_facts

ACCOUNT = "ada@example.test"
KEY = {"provider": "google", "account": ACCOUNT}


def _b64(text: str, charset: str = "utf-8") -> str:
    return base64.urlsafe_b64encode(text.encode(charset)).decode().rstrip("=")


def _h(**headers: str) -> list[dict]:
    return [{"name": name.replace("_", "-"), "value": value} for name, value in headers.items()]


def _part(mime_type: str, text: str | None = None, *, part_id="0", charset="utf-8", **more) -> dict:
    part = {
        "partId": part_id,
        "mimeType": mime_type,
        "filename": "",
        "headers": _h(Content_Type=f'{mime_type}; charset="{charset}"'),
        "body": {"size": 0},
    }
    if text is not None:
        part["body"] = {"size": len(text), "data": _b64(text, charset)}
    part.update(more)
    return part


def _message(**changed) -> dict:
    answered = {
        "id": "m1",
        "threadId": "t1",
        "labelIds": ["INBOX", "UNREAD", "Label_7"],
        "snippet": "Lunch on Friday?",
        "historyId": "9001",
        "internalDate": "1767268800000",  # 2026-01-01T12:00:00Z
        "sizeEstimate": 2048,
        "payload": {
            "partId": "",
            "mimeType": "multipart/mixed",
            "filename": "",
            "headers": _h(
                Message_ID="<abc@mail.example.test>",
                Subject="Lunch on Friday?",
                From='"Bo Example" <bo@example.test>',
                To="Ada <ada@example.test>, cy@example.test",
                Cc="dee@example.test",
                Date="Thu, 1 Jan 2026 13:00:00 +0100",
                In_Reply_To="<prev@mail.example.test>",
                References="<first@mail.example.test> <prev@mail.example.test>",
            ),
            "body": {"size": 0},
            "parts": [
                {
                    "partId": "0",
                    "mimeType": "multipart/alternative",
                    "filename": "",
                    "headers": [],
                    "body": {"size": 0},
                    "parts": [
                        _part("text/plain", "Lunch on Friday?\n", part_id="0.0"),
                        _part("text/html", "<p>Lunch on Friday?</p>", part_id="0.1"),
                    ],
                },
                {
                    "partId": "1",
                    "mimeType": "application/pdf",
                    "filename": "menu.pdf",
                    "headers": _h(Content_Disposition='attachment; filename="menu.pdf"'),
                    "body": {"size": 51234, "attachmentId": "made-up-attachment-handle"},
                },
            ],
        },
    }
    answered.update(changed)
    return answered


def _with_headers(**headers: str) -> dict:
    message = _message()
    kept = [
        h
        for h in message["payload"]["headers"]
        if h["name"] not in {n.replace("_", "-") for n in headers}
    ]
    message["payload"]["headers"] = kept + _h(**{k: v for k, v in headers.items() if v is not None})
    return message


class TestTheMessage:
    def test_its_identity_and_what_gmail_states_of_it(self):
        row = message_facts(ACCOUNT, _message()).message
        assert {k: row[k] for k in ("provider", "account", "id", "thread_id")} == {
            **KEY,
            "id": "m1",
            "thread_id": "t1",
        }
        assert (row["snippet"], row["size_bytes"], row["change_key"]) == (
            "Lunch on Friday?",
            2048,
            "9001",
        )
        assert row["internet_message_id"] == "<abc@mail.example.test>"
        assert row["in_reply_to"] == "<prev@mail.example.test>"
        assert row["reference_ids"] == ["<first@mail.example.test>", "<prev@mail.example.test>"]

    def test_who_it_is_from_and_to_is_read_from_its_headers(self):
        row = message_facts(ACCOUNT, _message()).message
        assert (row["from_address"], row["from_name"]) == ("bo@example.test", "Bo Example")
        assert row["to_addresses"] == ["ada@example.test", "cy@example.test"]
        assert row["cc_addresses"] == ["dee@example.test"]
        assert row["bcc_addresses"] == [] and row["reply_to_addresses"] == []
        assert row["sender_address"] is None

    def test_when_it_was_sent_and_received_are_in_utc(self):
        row = message_facts(ACCOUNT, _message()).message
        assert row["sent_at"] == dt.datetime(2026, 1, 1, 12, 0, 0)
        assert row["received_at"] == dt.datetime(2026, 1, 1, 12, 0, 0)
        assert row["sent_at"].tzinfo is None

    @pytest.mark.parametrize("date", ["sometime last week", "", None])
    def test_a_date_header_that_is_not_one_is_no_date(self, date):
        assert message_facts(ACCOUNT, _with_headers(Date=date)).message["sent_at"] is None

    def test_a_header_is_found_whatever_its_case_and_its_first_occurrence_taken(self):
        message = _message()
        message["payload"]["headers"] = [
            {"name": "SUBJECT", "value": "first"},
            {"name": "subject", "value": "second"},
        ]
        assert message_facts(ACCOUNT, message).message["subject"] == "first"

    def test_encoded_words_are_decoded(self):
        row = message_facts(
            ACCOUNT,
            _with_headers(
                Subject="=?UTF-8?B?w4RwZmVsIMOgIGxhIGNhcnRl?=",
                From="=?UTF-8?Q?Zo=C3=AB_Example?= <zoe@example.test>",
            ),
        ).message
        assert row["subject"] == "Äpfel à la carte"
        assert (row["from_name"], row["from_address"]) == ("Zoë Example", "zoe@example.test")

    def test_every_header_is_kept_as_gmail_gave_it(self):
        message = _message()
        row = message_facts(ACCOUNT, message).message
        assert row["headers"] == message["payload"]["headers"]

    def test_labels_say_whether_it_is_read_a_draft_or_starred(self):
        row = message_facts(ACCOUNT, _message()).message
        assert (row["is_read"], row["is_draft"], row["is_flagged"], row["flag_status"]) == (
            False,
            False,
            False,
            "none",
        )
        starred = message_facts(ACCOUNT, _message(labelIds=["DRAFT", "STARRED"])).message
        assert (starred["is_read"], starred["is_draft"], starred["is_flagged"]) == (
            True,
            True,
            True,
        )
        assert starred["flag_status"] == "flagged"
        assert row["folder_ids"] == ["INBOX", "UNREAD", "Label_7"]

    def test_what_google_does_not_offer_is_null(self):
        row = message_facts(ACCOUNT, _message()).message
        for column in (
            "created_at",
            "updated_at",
            "importance",
            "categories",
            "inference_classification",
            "is_read_receipt_requested",
            "is_delivery_receipt_requested",
            "web_link",
            "conversation_index",
            "flag_start_at",
            "flag_due_at",
            "flag_completed_at",
        ):
            assert row[column] is None, column

    def test_a_classification_gmail_states_is_kept_and_none_is_null(self):
        assert message_facts(ACCOUNT, _message()).message["classification_labels"] is None
        labelled = _message(classificationLabelValues=[{"labelId": "L1", "fields": []}])
        assert message_facts(ACCOUNT, labelled).message["classification_labels"] == [
            {"labelId": "L1", "fields": []}
        ]

    def test_it_has_every_column_of_the_canonical_table_and_no_other(self):
        row = message_facts(ACCOUNT, _message()).message
        assert len(row) == 42
        assert list(row)[:4] == ["provider", "account", "id", "thread_id"]


class TestItsText:
    def test_the_text_and_the_html_are_the_first_parts_of_each_kind(self):
        row = message_facts(ACCOUNT, _message()).message
        assert row["body_text"] == "Lunch on Friday?\n"
        assert row["body_html"] == "<p>Lunch on Friday?</p>"

    def test_a_message_of_one_part_has_only_that_part(self):
        only = _message(
            payload={
                **_part("text/plain", "Just text.", part_id=""),
                "headers": _h(Subject="s") + _h(Content_Type="text/plain; charset=utf-8"),
            }
        )
        row = message_facts(ACCOUNT, only).message
        assert (row["body_text"], row["body_html"]) == ("Just text.", None)

    def test_a_part_is_decoded_by_the_charset_it_declares(self):
        latin = _message(payload=_part("text/plain", "Grüße", part_id="", charset="iso-8859-1"))
        assert message_facts(ACCOUNT, latin).message["body_text"] == "Grüße"

    def test_a_part_that_declares_no_charset_is_ascii(self):
        part = _part("text/plain", "plain ascii", part_id="")
        part["headers"] = []
        assert message_facts(ACCOUNT, _message(payload=part)).message["body_text"] == "plain ascii"

    def test_a_text_file_attached_is_not_the_messages_text(self):
        message = _message()
        note = _part("text/plain", "attached notes", part_id="2", filename="notes.txt")
        message["payload"]["parts"].insert(0, note)
        facts = message_facts(ACCOUNT, message)
        assert facts.message["body_text"] == "Lunch on Friday?\n"
        assert [a["name"] for a in facts.attachments] == ["notes.txt", "menu.pdf"]

    def test_read_as_headers_and_labels_only_it_has_no_text_and_says_so(self):
        metadata = _message()
        metadata["payload"] = {"headers": metadata["payload"]["headers"]}
        del metadata["snippet"], metadata["sizeEstimate"]
        facts = message_facts(ACCOUNT, metadata)
        row = facts.message
        assert (row["body_text"], row["body_html"]) == (None, None)
        assert (row["snippet"], row["size_bytes"]) == (None, None)
        assert row["subject"] == "Lunch on Friday?" and facts.attachments == []
        assert row["has_attachments"] is False

    @pytest.mark.parametrize(
        ("charset", "text", "said"),
        [
            ("x-made-up", "abc", "declares charset 'x-made-up'"),
            ("us-ascii", "Grüße", "is not us-ascii"),
        ],
    )
    def test_a_part_that_is_not_what_it_declares_leaves_no_text_and_marks_the_message(
        self, charset, text, said
    ):
        part = _part("text/plain", None, part_id="", charset=charset)
        part["body"] = {"size": 1, "data": _b64(text)}
        part["headers"] += _h(Subject="Kept", From="bo@example.test")
        facts = message_facts(ACCOUNT, _message(payload=part))
        assert facts.message["body_text"] is None
        assert (facts.message["subject"], facts.message["from_address"]) == (
            "Kept",
            "bo@example.test",
        )
        assert facts.message["snippet"] == "Lunch on Friday?"
        assert facts.unreadable is not None and said in facts.unreadable
        assert facts.unreadable.startswith("message m1: ") and text not in facts.unreadable

    def test_a_message_that_reads_is_not_marked(self):
        assert message_facts(ACCOUNT, _message()).unreadable is None


class TestItsOtherRows:
    def test_each_person_is_a_row_with_their_name_in_the_headers_order(self):
        recipients = message_facts(ACCOUNT, _message()).recipients
        assert [(r["kind"], r["position"], r["address"], r["name"]) for r in recipients] == [
            ("from", 0, "bo@example.test", "Bo Example"),
            ("to", 0, "ada@example.test", "Ada"),
            ("to", 1, "cy@example.test", None),
            ("cc", 0, "dee@example.test", None),
        ]
        assert all(r["message_id"] == "m1" and r["provider"] == "google" for r in recipients)

    def test_each_label_is_a_row(self):
        folders = message_facts(ACCOUNT, _message()).folders
        assert folders == [
            {**KEY, "message_id": "m1", "folder_id": label}
            for label in ("INBOX", "UNREAD", "Label_7")
        ]

    def test_an_attachment_is_a_part_with_a_file_name_keyed_by_its_part(self):
        (attachment,) = message_facts(ACCOUNT, _message()).attachments
        assert attachment == {
            **KEY,
            "message_id": "m1",
            "id": "1",
            "name": "menu.pdf",
            "content_type": "application/pdf",
            "size_bytes": 51234,
            "is_inline": False,
            "content_id": None,
            "kind": "file",
            "updated_at": None,
        }
        assert "made-up-attachment-handle" not in repr(attachment)

    def test_an_inline_image_is_an_attachment_row_and_does_not_count_as_one(self):
        message = _message()
        message["payload"]["parts"] = [
            {
                "partId": "1",
                "mimeType": "image/png",
                "filename": "logo.png",
                "headers": _h(
                    Content_Disposition='inline; filename="logo.png"', Content_ID="<logo@x>"
                ),
                "body": {"size": 900, "attachmentId": "h"},
            }
        ]
        facts = message_facts(ACCOUNT, message)
        assert facts.attachments[0]["is_inline"] is True
        assert facts.attachments[0]["content_id"] == "logo@x"
        assert facts.message["has_attachments"] is False

    def test_a_message_with_no_labels_and_no_people_has_no_such_rows(self):
        bare = _message(labelIds=None, payload={"headers": []})
        facts = message_facts(ACCOUNT, bare)
        assert facts.recipients == [] and facts.folders == [] and facts.attachments == []
        assert facts.message["from_address"] is None and facts.message["folder_ids"] == []
