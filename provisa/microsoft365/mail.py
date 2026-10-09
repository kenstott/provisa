# Copyright (c) 2026 Kenneth Stott
# Canary: a16da046-3431-4318-a8b8-ef65b5ab8d3d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A Microsoft 365 mailbox as the canonical mail tables (``provisa.core.canonical_mail``).

Each table is read whole. A row holds every column of its canonical table, in its order; a
column Microsoft has no value for is None. Every enumeration value passes through the shared
translator, so a value Graph answers that its table does not list fails the read by name.

What Graph gives and how it is asked:

- messages are listed once with their bodies as text (``Prefer: outlook.body-content-type``);
  the HTML body is a second read of each page's messages, batched;
- a folder's role comes from Graph's well-known folder names, each asked for by name, since a
  v1.0 folder does not carry it; ``/mailFolders`` answers one level, so the tree is walked;
- attachments are listed for the messages Graph marks as having them (``hasAttachments``,
  which does not count inline ones), without their content.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any
from urllib.parse import quote

from provisa.core import canonical_mail as cm
from provisa.microsoft365.graph import IMMUTABLE_IDS, Graph

PROVIDER = cm.MICROSOFT
#: The six mail tables, in the order a registrar is offered them.
MAIL_TABLES = (
    "messages",
    "message_recipients",
    "folders",
    "message_folders",
    "threads",
    "attachments",
)
#: Graph's well-known folder names (the mailFolder resource's table of them).
WELL_KNOWN_FOLDERS = (
    "archive",
    "clutter",
    "conflicts",
    "conversationhistory",
    "deleteditems",
    "drafts",
    "inbox",
    "junkemail",
    "localfailures",
    "msgfolderroot",
    "outbox",
    "recoverableitemsdeletions",
    "scheduled",
    "searchfolders",
    "sentitems",
    "serverfailures",
    "syncissues",
)
PAGE_SIZE = 50
_FOLDER_PAGE = 100
_TEXT_BODY = 'outlook.body-content-type="text"'
_HTML_BODY = 'outlook.body-content-type="html"'
_MESSAGE_FIELDS = (
    "id,conversationId,conversationIndex,internetMessageId,subject,bodyPreview,from,sender,"
    "toRecipients,ccRecipients,bccRecipients,replyTo,sentDateTime,receivedDateTime,"
    "createdDateTime,lastModifiedDateTime,isRead,isDraft,flag,importance,hasAttachments,"
    "parentFolderId,categories,inferenceClassification,isReadReceiptRequested,"
    "isDeliveryReceiptRequested,body,internetMessageHeaders,changeKey,webLink"
)
_THREAD_FIELDS = "id,conversationId,subject,bodyPreview,receivedDateTime"
_ATTACHMENT_FIELDS = "id,name,contentType,size,isInline,lastModifiedDateTime"
_RECIPIENT_LISTS = (
    ("to", "toRecipients"),
    ("cc", "ccRecipients"),
    ("bcc", "bccRecipients"),
    ("reply_to", "replyTo"),
)


class UnexpectedAnswer(RuntimeError):
    """Graph answered in a form the reader did not ask for."""


def _row(table: str, account: str, **values: Any) -> dict[str, Any]:
    """A row of ``table``: every canonical column in order, None where no value is given. A
    value for a column the table does not have, or one Microsoft does not populate, is an
    error of this module, raised here."""
    columns = cm.TABLES[table].columns
    known = {c.name: c for c in columns}
    for name in values:
        if name not in known:
            raise KeyError(f"{table} has no column {name!r}")
        if not known[name].populated_by(PROVIDER):
            raise KeyError(f"{table}.{name} is not a column Microsoft populates")
    row: dict[str, Any] = {c.name: values.get(c.name) for c in columns}
    row["provider"] = PROVIDER
    row["account"] = account
    return row


def _address(recipient: dict | None) -> tuple[str | None, str | None]:
    email = (recipient or {}).get("emailAddress") or {}
    return email.get("address"), email.get("name")


def _addresses(recipients: list[dict] | None) -> list[str]:
    return [a for a, _ in map(_address, recipients or []) if a]


def _utc(moment: dict | None) -> str | None:
    """A Graph ``dateTimeTimeZone`` as an ISO-8601 instant. Graph answers these in UTC unless
    asked otherwise, and this reader never asks otherwise."""
    if moment is None:
        return None
    if moment["timeZone"] != "UTC":
        raise UnexpectedAnswer(
            f"Graph answered a time in the zone {moment['timeZone']!r}; UTC was expected"
        )
    stamp = moment["dateTime"]
    whole, dot, fraction = stamp.partition(".")
    return f"{whole}{dot}{fraction[:6]}+00:00"  # Graph writes seven digits of a second


def _header(headers: list[dict], name: str) -> str | None:
    wanted = name.lower()
    return next((h["value"] for h in headers if h["name"].lower() == wanted), None)


def message_row(account: str, item: dict, html: str | None) -> dict[str, Any]:
    body = item["body"]
    if body["contentType"] != "text":
        raise UnexpectedAnswer(
            f"Graph answered the body of a message as {body['contentType']!r}; text was asked for"
        )
    from_address, from_name = _address(item.get("from"))
    headers = item.get("internetMessageHeaders") or []
    references = _header(headers, "References")
    flag = item["flag"]
    status = cm.translate("flag_status", PROVIDER, flag["flagStatus"])
    return _row(
        "messages",
        account,
        id=item["id"],
        thread_id=item["conversationId"],
        internet_message_id=item.get("internetMessageId"),
        in_reply_to=_header(headers, "In-Reply-To"),
        reference_ids=None if references is None else references.split(),
        subject=item.get("subject"),
        snippet=item.get("bodyPreview"),
        from_address=from_address,
        from_name=from_name,
        sender_address=_address(item.get("sender"))[0],
        to_addresses=_addresses(item.get("toRecipients")),
        cc_addresses=_addresses(item.get("ccRecipients")),
        bcc_addresses=_addresses(item.get("bccRecipients")),
        reply_to_addresses=_addresses(item.get("replyTo")),
        sent_at=item.get("sentDateTime"),
        received_at=item.get("receivedDateTime"),
        created_at=item.get("createdDateTime"),
        updated_at=item.get("lastModifiedDateTime"),
        is_read=item["isRead"],
        is_draft=item["isDraft"],
        is_flagged=status == "flagged",
        flag_status=status,
        importance=cm.translate("importance", PROVIDER, item["importance"]),
        has_attachments=item["hasAttachments"],
        folder_ids=[item["parentFolderId"]],
        categories=item.get("categories") or [],
        inference_classification=cm.translate(
            "inference_classification", PROVIDER, item.get("inferenceClassification")
        ),
        is_read_receipt_requested=item.get("isReadReceiptRequested"),
        is_delivery_receipt_requested=item.get("isDeliveryReceiptRequested"),
        body_text=body["content"],
        body_html=html,
        headers=[{"name": h["name"], "value": h["value"]} for h in headers],
        change_key=item.get("changeKey"),
        web_link=item.get("webLink"),
        conversation_index=item.get("conversationIndex"),
        flag_start_at=_utc(flag.get("startDateTime")),
        flag_due_at=_utc(flag.get("dueDateTime")),
        flag_completed_at=_utc(flag.get("completedDateTime")),
    )


def recipient_rows(account: str, item: dict) -> list[dict[str, Any]]:
    people: list[tuple[str, list[dict]]] = [
        ("from", [item["from"]] if item.get("from") else []),
        ("sender", [item["sender"]] if item.get("sender") else []),
    ]
    people += [(kind, item.get(field) or []) for kind, field in _RECIPIENT_LISTS]
    rows = []
    for kind, recipients in people:
        for position, recipient in enumerate(recipients, 1):
            address, name = _address(recipient)
            rows.append(
                _row(
                    "message_recipients",
                    account,
                    message_id=item["id"],
                    kind=cm.canonical("recipient_kind", kind),
                    position=position,
                    address=address,
                    name=name,
                )
            )
    return rows


def message_folder_row(account: str, item: dict) -> dict[str, Any]:
    return _row("message_folders", account, message_id=item["id"], folder_id=item["parentFolderId"])


def folder_row(account: str, item: dict, well_known: str | None) -> dict[str, Any]:
    role = cm.translate("folder_role", PROVIDER, well_known)
    return _row(
        "folders",
        account,
        id=item["id"],
        name=item["displayName"],
        parent_id=item.get("parentFolderId"),
        kind=cm.canonical("folder_kind", "user" if role is None else "system"),
        role=role,
        is_hidden=item.get("isHidden"),
        total_count=item.get("totalItemCount"),
        unread_count=item.get("unreadItemCount"),
        child_count=item.get("childFolderCount"),
    )


def attachment_row(account: str, message_id: str, item: dict) -> dict[str, Any]:
    return _row(
        "attachments",
        account,
        message_id=message_id,
        id=item["id"],
        name=item.get("name"),
        content_type=item.get("contentType"),
        size_bytes=item.get("size"),
        is_inline=item.get("isInline"),
        content_id=item.get("contentId"),
        kind=cm.translate("attachment_kind", PROVIDER, item["@odata.type"]),
        updated_at=item.get("lastModifiedDateTime"),
    )


def thread_rows(account: str, items: list[dict]) -> list[dict[str, Any]]:
    """The threads of ``items`` (messages): one row for each conversation among them."""
    threads: dict[str, list[dict]] = {}
    for item in items:
        threads.setdefault(item["conversationId"], []).append(item)
    rows = []
    for thread_id, messages in threads.items():
        messages.sort(key=lambda m: (m["receivedDateTime"], m["id"]))
        first, last = messages[0], messages[-1]
        rows.append(
            _row(
                "threads",
                account,
                id=thread_id,
                subject=first.get("subject"),
                snippet=last.get("bodyPreview"),
                first_message_at=first["receivedDateTime"],
                last_message_at=last["receivedDateTime"],
                message_count=len(messages),
            )
        )
    return rows


class Mailbox:
    """One account's mail, read through Graph."""

    def __init__(self, graph: Graph, account: str, *, page_size: int = PAGE_SIZE) -> None:
        self._graph = graph
        self.account = account.lower()
        self._root = f"/users/{quote(account, safe='@')}"
        self._page_size = page_size

    def _messages(self, fields: str, *, prefer: tuple[str, ...]) -> Iterator[list[dict]]:
        yield from self._graph.pages(
            f"{self._root}/messages",
            {"$select": fields, "$top": str(self._page_size)},
            prefer=(IMMUTABLE_IDS, *prefer),
        )

    def _html(self, page: list[dict]) -> dict[str, str]:
        answers = self._graph.batch(
            [f"{self._root}/messages/{quote(m['id'], safe='')}?$select=body" for m in page],
            prefer=(IMMUTABLE_IDS, _HTML_BODY),
        )
        bodies = {}
        for message, answer in zip(page, answers):
            body = answer["body"]
            if body["contentType"] != "html":
                raise UnexpectedAnswer(
                    f"Graph answered the body of a message as {body['contentType']!r}; "
                    "html was asked for"
                )
            bodies[message["id"]] = body["content"]
        return bodies

    def _well_known(self) -> dict[str, str]:
        """Folder id -> the well-known name Graph knows it by, for those this mailbox has."""
        known = {}
        for name in WELL_KNOWN_FOLDERS:
            found = self._graph.find(f"{self._root}/mailFolders/{name}", {"$select": "id"})
            if found is not None:
                known[found["id"]] = name
        return known

    def _folders(self) -> Iterator[list[dict]]:
        query = {"includeHiddenFolders": "true", "$top": str(_FOLDER_PAGE)}
        waiting = [f"{self._root}/mailFolders"]
        while waiting:
            for page in self._graph.pages(waiting.pop(), query):
                yield page
                waiting += [
                    f"{self._root}/mailFolders/{quote(f['id'], safe='')}/childFolders"
                    for f in page
                    if f.get("childFolderCount")
                ]

    def rows(self, table: str) -> Iterator[list[dict[str, Any]]]:
        """Every row of one of the mail tables, a batch at a time."""
        account = self.account
        if table == "messages":
            for page in self._messages(_MESSAGE_FIELDS, prefer=(_TEXT_BODY,)):
                html = self._html(page)
                yield [message_row(account, m, html[m["id"]]) for m in page]
        elif table == "message_recipients":
            fields = "id,from,sender,toRecipients,ccRecipients,bccRecipients,replyTo"
            for page in self._messages(fields, prefer=()):
                yield [row for m in page for row in recipient_rows(account, m)]
        elif table == "message_folders":
            for page in self._messages("id,parentFolderId", prefer=()):
                yield [message_folder_row(account, m) for m in page]
        elif table == "threads":
            # A thread is its messages taken together, so the mailbox is read before any row.
            items = [m for page in self._messages(_THREAD_FIELDS, prefer=()) for m in page]
            rows = thread_rows(account, items)
            for start in range(0, len(rows), self._page_size):
                yield rows[start : start + self._page_size]
        elif table == "folders":
            known = self._well_known()
            for page in self._folders():
                yield [folder_row(account, f, known.get(f["id"])) for f in page]
        elif table == "attachments":
            for page in self._messages("id,hasAttachments", prefer=()):
                for message in page:
                    if not message["hasAttachments"]:
                        continue
                    path = f"{self._root}/messages/{quote(message['id'], safe='')}/attachments"
                    for items in self._graph.pages(
                        path, {"$select": _ATTACHMENT_FIELDS}, prefer=(IMMUTABLE_IDS,)
                    ):
                        yield [attachment_row(account, message["id"], a) for a in items]
        else:
            raise KeyError(f"{table!r} is not a Microsoft 365 mail table")
