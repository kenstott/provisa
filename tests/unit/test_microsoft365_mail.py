# Copyright (c) 2026 Kenneth Stott
# Canary: 64242961-276b-4b26-b980-082c87845157
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""The Microsoft 365 mail reader: Graph's answers become rows of the canonical mail tables,
column for column; a collection is read to its end; a throttled call is waited out within a
bound and otherwise fails by name; an enumeration value Graph's table does not list fails the
read. Graph is a stand-in answering in the forms Microsoft's reference documents."""

from __future__ import annotations

from urllib.parse import parse_qs, urlsplit

import pyarrow as pa
import pytest

from provisa.core import canonical_mail as cm
from provisa.core.ir_arrow import arrow_schema, rows_to_batch
from provisa.microsoft365 import mail
from provisa.microsoft365.graph import (
    BASE_URL,
    Answer,
    Graph,
    GraphRefused,
    GraphThrottled,
)

ACCOUNT = "Megan@Contoso.com"
ROOT = "/users/Megan@Contoso.com"


def _person(name: str, address: str) -> dict:
    return {"emailAddress": {"name": name, "address": address}}


def _message(n: int, **over) -> dict:
    item = {
        "@odata.etag": 'W/"x"',
        "id": f"m{n}",
        "conversationId": "c1",
        "conversationIndex": "AQHa",
        "internetMessageId": f"<{n}@contoso.com>",
        "subject": f"Subject {n}",
        "bodyPreview": f"Preview {n}",
        "from": _person("Dana Swope", "danas@contoso.com"),
        "sender": _person("Dana Swope", "danas@contoso.com"),
        "toRecipients": [_person("Megan", "megan@contoso.com"), _person("Sam", "sam@x.org")],
        "ccRecipients": [],
        "bccRecipients": [],
        "replyTo": [],
        "sentDateTime": f"2026-01-0{n}T10:00:00Z",
        "receivedDateTime": f"2026-01-0{n}T10:00:05Z",
        "createdDateTime": f"2026-01-0{n}T10:00:05Z",
        "lastModifiedDateTime": f"2026-01-0{n}T11:00:00Z",
        "isRead": True,
        "isDraft": False,
        "flag": {"flagStatus": "notFlagged"},
        "importance": "normal",
        "hasAttachments": False,
        "parentFolderId": "f-inbox",
        "categories": ["Blue"],
        "inferenceClassification": "focused",
        "isReadReceiptRequested": False,
        "isDeliveryReceiptRequested": False,
        "body": {"contentType": "text", "content": f"Text {n}"},
        "internetMessageHeaders": [
            {"name": "In-Reply-To", "value": "<0@contoso.com>"},
            {"name": "References", "value": "<a@contoso.com> <0@contoso.com>"},
        ],
        "changeKey": "CQAAAB",
        "webLink": "https://outlook.office365.com/owa/?ItemID=m",
    }
    item.update(over)
    return item


class FakeGraph:
    """Answers GETs from a table of path -> pages (a list of ``value`` lists) or one object,
    and ``$batch`` by answering each request in it from the same table."""

    def __init__(self, table: dict) -> None:
        self.table = table
        self.calls: list[tuple[str, str, dict, dict]] = []
        self.queued: list[Answer] = []  # answers given before the table is consulted

    def _one(self, url: str) -> tuple[int, object]:
        parts = urlsplit(url)
        path = parts.path.removeprefix("/v1.0")
        query = parse_qs(parts.query)
        if path not in self.table:
            return 404, {"error": {"code": "ErrorItemNotFound", "message": "not found"}}
        answer = self.table[path]
        if isinstance(answer, list):
            page = int(query.get("$skiptoken", ["0"])[0])
            body: dict = {"value": answer[page]}
            if page + 1 < len(answer):
                body["@odata.nextLink"] = f"{BASE_URL}{path}?$skiptoken={page + 1}"
            return 200, body
        return 200, answer

    def send(self, method, url, params, headers, body) -> Answer:
        self.calls.append((method, url, dict(params or {}), dict(headers)))
        if self.queued:
            return self.queued.pop(0)
        if method == "POST":
            responses = []
            for request in body["requests"]:
                status, answer = self._one(BASE_URL + request["url"])
                responses.append({"id": request["id"], "status": status, "body": answer})
            return Answer(200, {}, {"responses": list(reversed(responses))})
        status, answer = self._one(url)
        return Answer(status, {}, answer)


def _graph(table: dict, **kw) -> tuple[Graph, FakeGraph, list[float]]:
    fake, slept = FakeGraph(table), []
    return Graph(fake.send, lambda: "access-token", sleep=slept.append, **kw), fake, slept


def _mailbox(table: dict, **kw) -> tuple[mail.Mailbox, FakeGraph]:
    graph, fake, _ = _graph(table)
    return mail.Mailbox(graph, ACCOUNT, **kw), fake


def _all(mailbox: mail.Mailbox, table: str) -> list[dict]:
    return [row for batch in mailbox.rows(table) for row in batch]


def _html(*numbers: int) -> dict:
    return {
        f"{ROOT}/messages/m{n}": {"body": {"contentType": "html", "content": f"<p>{n}</p>"}}
        for n in numbers
    }


# ------------------------------------------------------------------------------ the tables


@pytest.mark.parametrize("table", mail.MAIL_TABLES)
def test_every_row_has_exactly_the_canonical_columns_and_lands_as_them(table):
    """The loader's output schema is the canonical one, column for column, and its values are
    ones the replica write face takes for those types."""
    source = {
        f"{ROOT}/messages": [[_message(1, hasAttachments=True), _message(2)]],
        **_html(1, 2),
        f"{ROOT}/mailFolders": [
            [{"id": "f-inbox", "displayName": "Inbox", "childFolderCount": 0, "isHidden": False}]
        ],
        f"{ROOT}/mailFolders/inbox": {"id": "f-inbox"},
        f"{ROOT}/messages/m1/attachments": [
            [
                {
                    "@odata.type": "#microsoft.graph.fileAttachment",
                    "id": "a1",
                    "name": "plan.pdf",
                    "contentType": "application/pdf",
                    "size": 2048,
                    "isInline": False,
                    "lastModifiedDateTime": "2026-01-01T10:00:00Z",
                }
            ]
        ],
    }
    mailbox, _ = _mailbox(source)
    rows = _all(mailbox, table)
    assert rows
    columns = cm.ir_columns(table)
    for row in rows:
        assert list(row) == [name for name, _ in columns]
        assert (row["provider"], row["account"]) == ("microsoft", "megan@contoso.com")
    batch = rows_to_batch(rows, columns, arrow_schema(columns))
    assert batch.schema == arrow_schema(columns)
    assert batch.num_rows == len(rows)


def test_a_message_row():
    mailbox, fake = _mailbox({f"{ROOT}/messages": [[_message(1)]], **_html(1)})
    (row,) = _all(mailbox, "messages")
    assert row["id"] == "m1" and row["thread_id"] == "c1"
    assert row["from_address"] == "danas@contoso.com" and row["from_name"] == "Dana Swope"
    assert row["to_addresses"] == ["megan@contoso.com", "sam@x.org"]
    assert row["cc_addresses"] == []
    assert row["in_reply_to"] == "<0@contoso.com>"
    assert row["reference_ids"] == ["<a@contoso.com>", "<0@contoso.com>"]
    assert row["flag_status"] == "none" and row["is_flagged"] is False
    assert row["importance"] == "normal" and row["inference_classification"] == "focused"
    assert row["folder_ids"] == ["f-inbox"]
    assert row["body_text"] == "Text 1" and row["body_html"] == "<p>1</p>"
    assert row["received_at"] == "2026-01-01T10:00:05Z"
    # Columns Microsoft has no value for are None, never an empty stand-in.
    assert row["size_bytes"] is None and row["classification_labels"] is None
    listing = fake.calls[0]
    assert listing[2]["$top"] == "50" and "internetMessageHeaders" in listing[2]["$select"]
    assert listing[3]["Prefer"] == 'IdType="ImmutableId", outlook.body-content-type="text"'
    assert listing[3]["Authorization"] == "Bearer access-token"


def test_a_flagged_message_carries_its_flag_times_in_utc():
    flag = {
        "flagStatus": "flagged",
        "startDateTime": {"dateTime": "2026-01-05T08:00:00.0000000", "timeZone": "UTC"},
        "dueDateTime": {"dateTime": "2026-01-06T08:00:00.0000000", "timeZone": "UTC"},
    }
    mailbox, _ = _mailbox({f"{ROOT}/messages": [[_message(1, flag=flag)]], **_html(1)})
    (row,) = _all(mailbox, "messages")
    assert row["is_flagged"] is True and row["flag_status"] == "flagged"
    assert row["flag_start_at"] == "2026-01-05T08:00:00.000000+00:00"
    assert row["flag_completed_at"] is None
    columns = cm.ir_columns("messages")
    landed = rows_to_batch([row], columns, arrow_schema(columns))
    assert landed.schema.field("flag_due_at").type == pa.timestamp("us")


def test_a_flag_time_in_another_zone_is_refused():
    flag = {
        "flagStatus": "flagged",
        "startDateTime": {
            "dateTime": "2026-01-05T08:00:00.0000000",
            "timeZone": "Pacific Standard Time",
        },
    }
    mailbox, _ = _mailbox({f"{ROOT}/messages": [[_message(1, flag=flag)]], **_html(1)})
    with pytest.raises(mail.UnexpectedAnswer, match="Pacific Standard Time"):
        _all(mailbox, "messages")


def test_an_importance_graphs_table_does_not_list_fails_the_read_by_name():
    mailbox, _ = _mailbox({f"{ROOT}/messages": [[_message(1, importance="urgent")]], **_html(1)})
    with pytest.raises(cm.UnmappedValue, match="'urgent'.*'importance'"):
        _all(mailbox, "messages")


def test_a_body_not_in_the_form_asked_for_is_refused():
    wrong = _message(1, body={"contentType": "html", "content": "<p>x</p>"})
    mailbox, _ = _mailbox({f"{ROOT}/messages": [[wrong]], **_html(1)})
    with pytest.raises(mail.UnexpectedAnswer, match="text was asked for"):
        _all(mailbox, "messages")


def test_messages_are_read_to_the_last_page_and_html_bodies_match_their_messages():
    pages = [[_message(1), _message(2)], [_message(3)]]
    mailbox, fake = _mailbox({f"{ROOT}/messages": pages, **_html(1, 2, 3)}, page_size=2)
    rows = _all(mailbox, "messages")
    assert [(r["id"], r["body_html"]) for r in rows] == [
        ("m1", "<p>1</p>"),
        ("m2", "<p>2</p>"),
        ("m3", "<p>3</p>"),
    ]
    follow = [c for c in fake.calls if c[0] == "GET"][1]
    assert follow[1].endswith("$skiptoken=1") and follow[2] == {}  # the next link, as given


def test_recipient_rows_name_each_person_in_order():
    mailbox, _ = _mailbox({f"{ROOT}/messages": [[_message(1)]]})
    rows = _all(mailbox, "message_recipients")
    assert [(r["kind"], r["position"], r["address"], r["name"]) for r in rows] == [
        ("from", 1, "danas@contoso.com", "Dana Swope"),
        ("sender", 1, "danas@contoso.com", "Dana Swope"),
        ("to", 1, "megan@contoso.com", "Megan"),
        ("to", 2, "sam@x.org", "Sam"),
    ]


def test_message_folders_has_one_row_a_message():
    mailbox, _ = _mailbox({f"{ROOT}/messages": [[_message(1), _message(2, parentFolderId="f2")]]})
    assert [(r["message_id"], r["folder_id"]) for r in _all(mailbox, "message_folders")] == [
        ("m1", "f-inbox"),
        ("m2", "f2"),
    ]


def test_threads_are_derived_from_the_messages():
    items = [
        _message(2),
        _message(1),
        _message(3, conversationId="c2", subject="Other", bodyPreview="Only"),
    ]
    mailbox, _ = _mailbox({f"{ROOT}/messages": [items]})
    rows = {r["id"]: r for r in _all(mailbox, "threads")}
    assert rows["c1"]["subject"] == "Subject 1"  # the earliest message's
    assert rows["c1"]["snippet"] == "Preview 2"  # the latest message's
    assert rows["c1"]["message_count"] == 2
    assert rows["c1"]["first_message_at"] == "2026-01-01T10:00:05Z"
    assert rows["c1"]["last_message_at"] == "2026-01-02T10:00:05Z"
    assert rows["c2"]["message_count"] == 1


def test_folders_walk_the_tree_and_take_their_role_from_the_well_known_names():
    def folder(fid, name, children=0, parent="root"):
        return {
            "id": fid,
            "displayName": name,
            "parentFolderId": parent,
            "childFolderCount": children,
            "isHidden": False,
            "totalItemCount": 3,
            "unreadItemCount": 1,
        }

    source = {
        f"{ROOT}/mailFolders": [
            [folder("f-inbox", "Posteingang", children=1), folder("f-sent", "Gesendet")],
            [folder("f-conv", "Aufgezeichnete Unterhaltungen")],
        ],
        f"{ROOT}/mailFolders/f-inbox/childFolders": [
            [folder("f-proj", "Projekt", parent="f-inbox")]
        ],
        f"{ROOT}/mailFolders/inbox": {"id": "f-inbox"},
        f"{ROOT}/mailFolders/sentitems": {"id": "f-sent"},
        f"{ROOT}/mailFolders/conversationhistory": {"id": "f-conv"},
    }
    mailbox, fake = _mailbox(source)
    rows = {r["id"]: r for r in _all(mailbox, "folders")}
    assert (rows["f-inbox"]["kind"], rows["f-inbox"]["role"]) == ("system", "inbox")
    assert (rows["f-sent"]["kind"], rows["f-sent"]["role"]) == ("system", "sent")
    # A well-known folder the role table does not list: the declared catch-all.
    assert (rows["f-conv"]["kind"], rows["f-conv"]["role"]) == ("system", "other")
    assert (rows["f-proj"]["kind"], rows["f-proj"]["role"]) == ("user", None)
    assert rows["f-proj"]["parent_id"] == "f-inbox" and rows["f-inbox"]["child_count"] == 1
    assert rows["f-inbox"]["threads_total"] is None  # Google's
    asked = {c[1].removeprefix(BASE_URL + ROOT + "/mailFolders/") for c in fake.calls}
    assert set(mail.WELL_KNOWN_FOLDERS) <= asked  # each name asked for; one it lacks is skipped
    top = next(c for c in fake.calls if c[1] == BASE_URL + ROOT + "/mailFolders")
    assert top[2]["includeHiddenFolders"] == "true"


def test_attachments_are_listed_for_the_messages_that_have_them():
    source = {
        f"{ROOT}/messages": [[_message(1, hasAttachments=True), _message(2)]],
        f"{ROOT}/messages/m1/attachments": [
            [
                {
                    "@odata.type": "#microsoft.graph.fileAttachment",
                    "id": "a1",
                    "name": "plan.pdf",
                    "contentType": "application/pdf",
                    "size": 2048,
                    "isInline": False,
                    "lastModifiedDateTime": "2026-01-01T10:00:00Z",
                },
                {"@odata.type": "#microsoft.graph.itemAttachment", "id": "a2", "name": "Fwd"},
            ]
        ],
    }
    mailbox, fake = _mailbox(source)
    rows = _all(mailbox, "attachments")
    assert [(r["message_id"], r["id"], r["kind"], r["size_bytes"]) for r in rows] == [
        ("m1", "a1", "file", 2048),
        ("m1", "a2", "item", None),
    ]
    listed = [c for c in fake.calls if c[1].endswith("/attachments")]
    assert len(listed) == 1 and "contentBytes" not in listed[0][2]["$select"]


def test_an_attachment_kind_graphs_table_does_not_list_fails_the_read():
    source = {
        f"{ROOT}/messages": [[_message(1, hasAttachments=True)]],
        f"{ROOT}/messages/m1/attachments": [
            [{"@odata.type": "#microsoft.graph.newKind", "id": "a"}]
        ],
    }
    mailbox, _ = _mailbox(source)
    with pytest.raises(cm.UnmappedValue, match="attachment_kind"):
        _all(mailbox, "attachments")


def test_a_table_that_is_not_a_mail_table_is_refused():
    mailbox, _ = _mailbox({})
    with pytest.raises(KeyError, match="not a Microsoft 365 mail table"):
        _all(mailbox, "events")


def test_a_row_cannot_carry_a_column_microsoft_does_not_populate():
    with pytest.raises(KeyError, match="not a column Microsoft populates"):
        mail._row("messages", "a@b.c", size_bytes=1)
    with pytest.raises(KeyError, match="no column"):
        mail._row("messages", "a@b.c", nonsense=1)


# ---------------------------------------------------------------------------------- Graph


def _throttled(seconds: str | None) -> Answer:
    headers = {} if seconds is None else {"Retry-After": seconds}
    return Answer(429, headers, {"error": {"code": "TooManyRequests", "message": "later"}})


def test_a_throttled_call_waits_as_long_as_graph_asks_and_goes_on():
    graph, fake, slept = _graph({"/x": [[{"id": 1}], [{"id": 2}]]})
    fake.queued = [_throttled("7"), _throttled("3")]
    assert [p for p in graph.pages("/x")] == [[{"id": 1}], [{"id": 2}]]
    assert slept == [7.0, 3.0]


def test_a_call_throttled_past_the_bound_fails_by_name_with_no_part_of_the_answer():
    graph, fake, slept = _graph({"/x": [[{"id": 1}], [{"id": 2}]]}, wait_seconds=10.0)
    got = []
    pages = graph.pages("/x")
    got.append(next(pages))
    fake.queued = [_throttled("6"), _throttled("6")]
    with pytest.raises(GraphThrottled, match="Retry-After: 6") as refused:
        got.append(next(pages))
    assert slept == [6.0] and refused.value.retry_after == "6"
    assert "skiptoken" not in str(refused.value)  # the paging token is not in the message


def test_a_throttled_call_that_names_no_wait_fails_by_name():
    graph, fake, slept = _graph({"/x": {"id": 1}})
    fake.queued = [_throttled(None)]
    with pytest.raises(GraphThrottled):
        graph.get("/x")
    assert slept == []


def test_a_refusal_carries_graphs_code_and_no_token():
    graph, fake, _ = _graph({})
    fake.queued = [
        Answer(403, {}, {"error": {"code": "ErrorAccessDenied", "message": "Access is denied."}})
    ]
    with pytest.raises(GraphRefused, match="403 ErrorAccessDenied: Access is denied") as refused:
        graph.get("/users/a@b.c/messages")
    assert refused.value.code == "ErrorAccessDenied" and "access-token" not in str(refused.value)


def test_find_answers_none_only_for_an_item_graph_says_is_not_there():
    graph, fake, _ = _graph({})
    assert graph.find("/users/a@b.c/mailFolders/archive") is None
    fake.queued = [Answer(404, {}, {"error": {"code": "ResourceNotFound", "message": "no user"}})]
    with pytest.raises(GraphRefused, match="ResourceNotFound"):
        graph.find("/users/nobody@b.c/mailFolders/archive")


def test_a_batch_answers_in_the_order_asked_and_is_cut_at_graphs_limit():
    table = {f"/i/{n}": {"n": n} for n in range(45)}
    graph, fake, _ = _graph(table)
    answers = graph.batch([f"/i/{n}" for n in range(45)], prefer=("a=b",))
    assert [a["n"] for a in answers] == list(range(45))  # the stand-in answers in reverse
    posts = [c for c in fake.calls if c[0] == "POST"]
    assert len(posts) == 3 and all(c[1] == BASE_URL + "/$batch" for c in posts)


def test_a_throttled_request_of_a_batch_is_asked_again_alone():
    graph, fake, slept = _graph({"/i/0": {"n": 0}, "/i/1": {"n": 1}})
    fake.queued = [
        Answer(
            200,
            {},
            {
                "responses": [
                    {"id": "0", "status": 200, "body": {"n": 0}},
                    {"id": "1", "status": 429, "headers": {"Retry-After": "4"}, "body": {}},
                ]
            },
        )
    ]
    assert graph.batch(["/i/0", "/i/1"]) == [{"n": 0}, {"n": 1}]
    assert slept == [4.0]
    assert len(fake.calls) == 2


def test_a_refused_request_of_a_batch_fails_the_batch():
    graph, _, _ = _graph({"/i/0": {"n": 0}})
    with pytest.raises(GraphRefused, match="ErrorItemNotFound"):
        graph.batch(["/i/0", "/i/missing"])


def test_a_message_whose_html_body_graph_will_not_give_keeps_its_row(caplog):
    """The build completes: the row is kept with that body column None and the rest filled,
    and the count and ids are reported once when the table's read ends."""
    import logging

    source = {f"{ROOT}/messages": [[_message(1), _message(2)]], **_html(1)}  # no body for m2
    mailbox, _ = _mailbox(source)
    with caplog.at_level(logging.WARNING, logger="provisa.microsoft365.mail"):
        rows = _all(mailbox, "messages")
    assert [(r["id"], r["body_html"], r["body_text"]) for r in rows] == [
        ("m1", "<p>1</p>", "Text 1"),
        ("m2", None, "Text 2"),
    ]
    assert rows[1]["subject"] == "Subject 2"
    (record,) = caplog.records
    assert "1 message(s)" in record.getMessage() and "m2" in record.getMessage()
    assert "megan@contoso.com" in record.getMessage()


def test_batch_each_gives_each_request_its_answer_or_its_refusal():
    graph, _, _ = _graph({"/i/0": {"n": 0}})
    first, second = graph.batch_each(["/i/0", "/i/missing"])
    assert first == {"n": 0}
    assert isinstance(second, GraphRefused) and second.code == "ErrorItemNotFound"
