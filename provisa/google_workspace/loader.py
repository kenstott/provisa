# Copyright (c) 2026 Kenneth Stott
# Canary: 07e57a0f-f4e1-4794-985c-a4d00d83cdce
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The mail tables of a Google Workspace source (REQ-1923).

One source reads one mailbox and offers the six canonical mail tables
(``provisa.core.canonical_mail``): ``messages``, ``message_recipients``, ``folders``,
``message_folders``, ``threads`` and ``attachments``. Each is read whole, from Gmail, into rows
of exactly the canonical columns, and landed as a replica like any fetched source.

A table is read on its own: the tables that come from messages each list the mailbox and fetch
its messages (as headers and labels only where the table needs no text), so nothing is held in
memory beyond a page and what ``threads`` adds up.

A message with a part that is not what it declares keeps its row without text
(``mail_rows``); the read counts such messages and names them when it ends: every id in the
log, and the count with the first :data:`NOTED_IDS` ids as the build's note on the replica.
"""

# Requirements: REQ-1923
from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from typing import Any

import httpx

from provisa.core import canonical_mail as cm
from provisa.core.secrets import resolve_secrets
from provisa.federation.data_replicator import BuildNote
from provisa.google_workspace import settings as gw_settings
from provisa.google_workspace.gmail import Gmail, GmailNotFound
from provisa.google_workspace.mail_rows import PROVIDER, MessageFacts, message_facts
from provisa.google_workspace.settings import MAIL_HEADERS, MailSettings, Settings

log = logging.getLogger(__name__)

#: The canonical tables this source offers, in the order the table picker lists them.
TABLES: tuple[str, ...] = (
    "messages",
    "message_recipients",
    "folders",
    "message_folders",
    "threads",
    "attachments",
)
#: The schema the table picker shows them under: a mailbox has none of its own.
SCHEMA = "default"

#: The note a build carries when messages were kept without their text.
UNREADABLE_MESSAGES = "replication.unreadable_messages"
#: How many of their ids the note names; the log names them all.
NOTED_IDS = 100

#: Tables whose rows need a message's text and parts; every other is read as headers only.
_NEEDS_WHOLE_MESSAGE = frozenset({"messages", "attachments"})
_PER_MESSAGE = {
    "messages": lambda facts: [facts.message],
    "message_recipients": lambda facts: facts.recipients,
    "message_folders": lambda facts: facts.folders,
    "attachments": lambda facts: facts.attachments,
}


class UnknownMailTable(LookupError):
    def __init__(self, table: str) -> None:
        super().__init__(
            f"A Google Workspace source has no table {table!r}; it offers {', '.join(TABLES)}"
        )


def source_settings(source: Any) -> Settings:
    """The source's settings, as its mapping states them. Credentials stay references."""
    return gw_settings.parse(dict(getattr(source, "mapping", None) or {}))


def _resolved(auth: Any) -> Any:
    """``auth`` with its vault references resolved, in the context that holds the vault: the
    token exchange runs in a worker thread, which holds none."""
    return auth.model_copy(
        update={
            name: resolve_secrets(value)
            for name, value in auth.model_dump().items()
            if isinstance(value, str)
        }
    )


def _folder(account: str, label: dict) -> dict[str, Any]:
    kind = label.get("type")
    color = label.get("color") or {}
    list_visibility = label.get("labelListVisibility")
    return {
        "provider": PROVIDER,
        "account": account,
        "id": label["id"],
        "name": label.get("name"),
        "parent_id": None,
        "kind": cm.translate("folder_kind", cm.GOOGLE, kind),
        # Google names a system label's role by its id; a label of the user's own has none.
        "role": cm.translate("folder_role", cm.GOOGLE, label["id"]) if kind == "system" else None,
        "is_hidden": None if list_visibility is None else list_visibility == "labelHide",
        "total_count": label.get("messagesTotal"),
        "unread_count": label.get("messagesUnread"),
        "child_count": None,
        "threads_total": label.get("threadsTotal"),
        "threads_unread": label.get("threadsUnread"),
        "color_text": color.get("textColor"),
        "color_background": color.get("backgroundColor"),
        "message_list_visibility": cm.translate(
            "message_list_visibility", cm.GOOGLE, label.get("messageListVisibility")
        ),
        "label_list_visibility": cm.translate("label_list_visibility", cm.GOOGLE, list_visibility),
    }


class _Threads:
    """The threads of the messages read: each thread's first subject, last snippet, span and
    count."""

    def __init__(self, account: str) -> None:
        self._account = account
        self._by_id: dict[str, dict[str, Any]] = {}
        self._undated = 0

    def add(self, message: dict[str, Any]) -> None:
        thread_id, at = message["thread_id"], message["received_at"]
        if thread_id is None:
            return
        thread = self._by_id.setdefault(
            thread_id,
            {
                "provider": PROVIDER,
                "account": self._account,
                "id": thread_id,
                "subject": None,
                "snippet": None,
                "first_message_at": None,
                "last_message_at": None,
                "message_count": 0,
            },
        )
        thread["message_count"] += 1
        if at is None:
            return
        if thread["first_message_at"] is None or at < thread["first_message_at"]:
            thread["first_message_at"], thread["subject"] = at, message["subject"]
        if thread["last_message_at"] is None or at >= thread["last_message_at"]:
            thread["last_message_at"], thread["snippet"] = at, message["snippet"]

    def rows(self) -> list[dict[str, Any]]:
        return list(self._by_id.values())


async def _messages(
    gmail: Gmail, mail: MailSettings, unreadable: list[str]
) -> AsyncIterator[list[MessageFacts]]:
    """Every message of the mailbox the source holds, a page of ids at a time. A message
    deleted between its listing and its reading is no longer in the mailbox, and is not a row."""
    async for ids in gmail.message_ids(mail):
        page: list[MessageFacts] = []
        for message_id in ids:
            try:
                answered = await gmail.message(message_id, mail)
            except GmailNotFound:
                continue
            facts = message_facts(gmail.account, answered)
            if facts.unreadable is not None:
                unreadable.append(message_id)
            page.append(facts)
        yield page


def unreadable_note(unreadable: list[str]) -> BuildNote | None:
    """The build's note for the messages kept without text: how many, the first
    :data:`NOTED_IDS` of their ids, and how many more there are."""
    if not unreadable:
        return None
    return BuildNote(
        UNREADABLE_MESSAGES,
        {
            "count": len(unreadable),
            "ids": unreadable[:NOTED_IDS],
            "more": max(len(unreadable) - NOTED_IDS, 0),
        },
    )


async def table_rows(
    gmail: Gmail, mail: MailSettings, table: str, unreadable: list[str] | None = None
) -> AsyncIterator[list[dict]]:
    """The rows of one canonical table, in batches, to the last one. The ids of the messages
    kept without text are added to ``unreadable`` as they are met."""
    if table not in TABLES:
        raise UnknownMailTable(table)
    if table == "folders":
        rows = []
        for label in await gmail.labels():
            rows.append(_folder(gmail.account, await gmail.label(label["id"])))
        yield rows
        return
    if table not in _NEEDS_WHOLE_MESSAGE:
        mail = MailSettings(
            MAIL_HEADERS, mail.search, mail.labels, mail.since, mail.include_spam_trash
        )
    unreadable = [] if unreadable is None else unreadable
    threads = _Threads(gmail.account)
    async for page in _messages(gmail, mail, unreadable):
        if table == "threads":
            for facts in page:
                threads.add(facts.message)
            continue
        rows = [row for facts in page for row in _PER_MESSAGE[table](facts)]
        if rows:
            yield rows
    if table == "threads":
        yield threads.rows()
    if unreadable:
        # Stated, never silent: these rows are in the table without their text.
        log.warning(
            "Google Workspace mailbox %s, table %s: %d message(s) have a part that is not what "
            "it declares and were kept without text: %s",
            gmail.account,
            table,
            len(unreadable),
            ", ".join(unreadable),
        )


def make_google_workspace_loader() -> Any:
    """The row-fetch of a Google Workspace source: one of its tables, whole, read from Gmail
    with the source's own credential."""
    from provisa.api_source.oauth_grants import access_token

    async def _batches(
        source: Any, table: Any, unreadable: list[str] | None = None
    ) -> AsyncIterator[list[dict]]:
        settings = source_settings(source)
        if settings.mail is None:
            raise UnknownMailTable(table.table_name)
        (account,) = settings.accounts
        auth = _resolved(settings.auth(account))

        async def token() -> str:
            return await asyncio.to_thread(access_token, auth)

        async with httpx.AsyncClient() as client:
            gmail = Gmail(account, token, client)
            async for rows in table_rows(gmail, settings.mail, table.table_name, unreadable):
                yield rows

    async def _load(source: Any, table: Any) -> list[dict]:
        return [row async for rows in _batches(source, table) for row in rows]

    def _replica_source(source: Any, table: Any, columns: list[tuple[str, str]]) -> Any:
        from provisa.federation.replica_source import CursorSource

        unreadable: list[str] = []

        def read(_batch_rows: int) -> AsyncIterator[list[dict]]:
            unreadable.clear()  # a build reads once; a read begun again counts again
            return _batches(source, table, unreadable)

        return CursorSource(read, columns, note=lambda: unreadable_note(unreadable))

    _load.replica_source = _replica_source  # type: ignore[attr-defined]
    return _load
