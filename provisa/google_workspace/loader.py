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

The five tables that come from messages are ONE read (:func:`message_table_rows`): the mailbox
is listed once and each message fetched once, whichever of them are being built, as headers
and labels only when none of them needs a message's text. A replica build takes them together
(``replica_group``, ``federation.data_replicator.ReplicaGroupJob``). ``folders`` comes from
the mailbox's labels, on its own. Nothing is held in memory beyond a page and what ``threads``
adds up.

A message with a part that is not what it declares keeps its row without text
(``mail_rows``); the read counts such messages and names them when it ends: every id in the
log, and the count with the first of their ids as the build's note on the replica
(``data_replicator.unreadable_messages_note``).
"""

# Requirements: REQ-1923
from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import httpx

from provisa.core import canonical_mail as cm
from provisa.core.secrets import resolve_secrets
from provisa.federation.data_replicator import noted, unreadable_messages_note
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


#: The tables one read of the mailbox's messages gives; ``folders`` comes from its labels.
MESSAGE_TABLES: tuple[str, ...] = (
    "messages",
    "message_recipients",
    "message_folders",
    "attachments",
    "threads",
)


async def message_table_rows(
    gmail: Gmail, mail: MailSettings, tables: tuple[str, ...], unreadable: list[str]
) -> AsyncIterator[tuple[str, list[dict]]]:
    """The rows of the message tables ``tables``, from ONE pass over the mailbox: each message
    is fetched once and gives its rows to every table asked for. A page of messages yields
    each table's rows of that page; ``threads`` is added up over the whole pass and comes last.
    Whole messages are fetched only when a table asked for needs their text or parts. The ids
    of the messages kept without text are added to ``unreadable`` as they are met."""
    unknown = [table for table in tables if table not in MESSAGE_TABLES]
    if unknown:
        raise UnknownMailTable(unknown[0])
    if not _NEEDS_WHOLE_MESSAGE.intersection(tables):
        mail = MailSettings(
            MAIL_HEADERS, mail.search, mail.labels, mail.since, mail.include_spam_trash
        )
    threads = _Threads(gmail.account) if "threads" in tables else None
    async for page in _messages(gmail, mail, unreadable):
        for table in tables:
            if table == "threads":
                continue
            rows = [row for facts in page for row in _PER_MESSAGE[table](facts)]
            if rows:
                yield table, rows
        if threads is not None:
            for facts in page:
                threads.add(facts.message)
    if threads is not None:
        yield "threads", threads.rows()
    if unreadable:
        # Stated, never silent: these rows are in their tables without their text.
        log.warning(
            "Google Workspace mailbox %s, tables %s: %d message(s) have a part that is not "
            "what it declares and were kept without text: %s",
            gmail.account,
            ", ".join(tables),
            len(unreadable),
            ", ".join(unreadable),
        )


#: The build's note for the messages kept without text: the one every mail source gives.
unreadable_note = unreadable_messages_note


async def table_rows(
    gmail: Gmail, mail: MailSettings, table: str, unreadable: list[str] | None = None
) -> AsyncIterator[list[dict]]:
    """The rows of one canonical table, in batches, to the last one: ``folders`` from the
    mailbox's labels, any other from the one read of its messages, asked for that table alone."""
    if table not in TABLES:
        raise UnknownMailTable(table)
    if table == "folders":
        rows = []
        for label in await gmail.labels():
            rows.append(_folder(gmail.account, await gmail.label(label["id"])))
        yield rows
        return
    unreadable = [] if unreadable is None else unreadable
    async for _table, rows in message_table_rows(gmail, mail, (table,), unreadable):
        yield rows


class _MailboxRead:
    """One read of a source's mailbox as the rows of several message tables: what a group
    build streams (``federation.data_replicator.ReplicaGroupJob``)."""

    def __init__(
        self,
        connect: Any,
        source: Any,
        tables: tuple[str, ...],
        columns: dict[str, list[tuple[str, str]]],
    ) -> None:
        from provisa.federation.data_replicator import SourceCaps, SourceRead

        self.caps = SourceCaps(frozenset({SourceRead.CURSOR}))
        self._connect = connect
        self._source = source
        self._tables = tables
        self._columns = columns
        self._unreadable: list[str] = []

    def notes(self) -> list[Any]:
        return noted(unreadable_note(self._unreadable))

    async def batches(self, batch_rows: int) -> AsyncIterator[tuple[str, Any]]:
        from provisa.core.ir_arrow import arrow_schema, rows_to_batch

        self._unreadable.clear()  # a build reads once; a read begun again counts again
        schemas = {table: arrow_schema(self._columns[table]) for table in self._tables}
        async with self._connect(self._source) as (gmail, mail):
            async for table, rows in message_table_rows(
                gmail, mail, self._tables, self._unreadable
            ):
                for start in range(0, len(rows), batch_rows):
                    yield (
                        table,
                        rows_to_batch(
                            rows[start : start + batch_rows], self._columns[table], schemas[table]
                        ),
                    )


def make_google_workspace_loader(state: Any) -> Any:
    """The row-fetch of a Google Workspace source: one of its tables, whole, read from Gmail
    with the source's own credential -- its owner's approval of the organisation's Google
    client (``core.mail_platforms``, read for the organisation the build runs in), or a service
    account's key."""
    from provisa.api_source.oauth_grants import access_token
    from provisa.core import mail_platforms
    from provisa.core.request_context import require_current_org
    from provisa.google_workspace import SOURCE_TYPE
    from provisa.google_workspace.settings import GOOGLE_ACCOUNT

    @asynccontextmanager
    async def _connect(source: Any) -> AsyncIterator[tuple[Gmail, MailSettings]]:
        """The source's mailbox, read with its own credential, and which mail of it to hold."""
        settings = source_settings(source)
        if settings.mail is None:
            raise UnknownMailTable("messages")
        (account,) = settings.accounts
        client = None
        if settings.sign_in == GOOGLE_ACCOUNT:
            client = await mail_platforms.require(
                state.admin_db, require_current_org(), SOURCE_TYPE
            )
        auth = _resolved(settings.auth(account, client))

        async def token() -> str:
            return await asyncio.to_thread(access_token, auth)

        async with httpx.AsyncClient() as http:
            yield Gmail(account, token, http), settings.mail

    async def _batches(
        source: Any, table: Any, unreadable: list[str] | None = None
    ) -> AsyncIterator[list[dict]]:
        async with _connect(source) as (gmail, mail):
            async for rows in table_rows(gmail, mail, table.table_name, unreadable):
                yield rows

    async def _load(source: Any, table: Any) -> list[dict]:
        return [row async for rows in _batches(source, table) for row in rows]

    def _replica_source(source: Any, table: Any, columns: list[tuple[str, str]]) -> Any:
        from provisa.federation.replica_source import CursorSource

        unreadable: list[str] = []

        def read(_batch_rows: int) -> AsyncIterator[list[dict]]:
            unreadable.clear()  # a build reads once; a read begun again counts again
            return _batches(source, table, unreadable)

        return CursorSource(read, columns, notes=lambda: noted(unreadable_note(unreadable)))

    def _replica_group(_source: Any, table: Any) -> tuple[str, ...] | None:
        """The tables one read of the mailbox gives together with ``table``, or None when
        ``table`` is read on its own (folders)."""
        return MESSAGE_TABLES if table.table_name in MESSAGE_TABLES else None

    def _replica_group_source(
        source: Any, tables: tuple[str, ...], columns: dict[str, list[tuple[str, str]]]
    ) -> Any:
        return _MailboxRead(_connect, source, tuple(tables), columns)

    _load.replica_source = _replica_source  # type: ignore[attr-defined]
    _load.replica_group = _replica_group  # type: ignore[attr-defined]
    _load.replica_group_source = _replica_group_source  # type: ignore[attr-defined]
    return _load
