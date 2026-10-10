# Copyright (c) 2026 Kenneth Stott
# Canary: e8b559bd-68bd-466b-a049-e41c35a0b9db
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A Microsoft 365 source's tables, read and landed (REQ-1923).

One source reads one mailbox and offers the six canonical mail tables
(``provisa.core.canonical_mail``): ``messages``, ``message_recipients``, ``folders``,
``message_folders``, ``threads`` and ``attachments``. Each is read whole, through Microsoft
Graph (``provisa.microsoft365.mail``), into rows of exactly the canonical columns, and landed as
a replica like any fetched source.

Graph is read with a blocking client; each batch is fetched off the event loop and only one is
held. A message whose HTML body Graph will not give keeps its row without it; the read counts
such messages and names them when it ends: every id in the log, and the count with the first
ids as the build's note on the replica, the one every mail source gives.
"""

# Requirements: REQ-1923
from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from dataclasses import dataclass
from typing import Any

import httpx

from provisa.microsoft365 import mail
from provisa.microsoft365.graph import Graph, GraphRefused, http_send

log = logging.getLogger(__name__)

TABLES: tuple[str, ...] = mail.MAIL_TABLES
#: A mailbox has no schema of its own; its tables are listed under this one.
SCHEMA = "default"


@dataclass(frozen=True)
class Reading:
    """What reading a source needs, made where the organisation's vault is bound: the
    mailboxes to read, by address and in address order; a call that gives the access token for
    each request (called off the event loop); and, for a source of the organisation's
    mailboxes, how many are read at once."""

    accounts: tuple[str, ...]
    token: Callable[[], str]
    organisation: bool = False
    at_once: int = 1


Connect = Callable[[Any], Awaitable[Reading]]

#: What Graph answers for a mailbox that is not there or that the client may not read. Such a
#: mailbox of the organisation's is left out of the build and reported; anything else Graph
#: refuses fails the build, and so does throttling it does not lift.
_NOT_READABLE_STATUS = frozenset({403, 404})
_NOT_READABLE_CODES = frozenset({"MailboxNotEnabledForRESTAPI", "MailboxNotSupportedForRESTAPI"})


class NoMailboxRead(RuntimeError):
    """Not one of the organisation's mailboxes could be read: the client's permission, not a
    mailbox, is what is wrong."""

    def __init__(self, count: int, reason: str) -> None:
        super().__init__(
            f"none of the {count} mailboxes could be read ({reason}): the organisation's "
            "Microsoft 365 client has not been granted reading of mail"
        )


_END = object()


class UnknownMailTable(LookupError):
    def __init__(self, table: str) -> None:
        super().__init__(
            f"A Microsoft 365 source has no table {table!r}; it offers {', '.join(TABLES)}"
        )


def table_rows(
    graph: Graph, account: str, table: str, unreadable: list[str] | None = None
) -> Iterator[list[dict]]:
    """The rows of one canonical table, in batches, to the last one."""
    if table not in TABLES:
        raise UnknownMailTable(table)
    return mail.Mailbox(graph, account).rows(table, unreadable)


async def _stepped(make: Callable[[], Iterator[Any]]) -> AsyncIterator[Any]:
    """A blocking iterator's batches, each produced off the event loop."""
    batches = await asyncio.to_thread(make)
    while True:
        rows = await asyncio.to_thread(next, batches, _END)
        if rows is _END:
            return
        yield rows  # type: ignore[misc]  # not the end marker


def _not_readable(refused: BaseException) -> bool:
    return isinstance(refused, GraphRefused) and (
        refused.status in _NOT_READABLE_STATUS or refused.code in _NOT_READABLE_CODES
    )


async def _read_mailboxes(
    reading: Reading,
    read: Callable[[mail.Mailbox], Iterator[Any]],
    left_out: list[str],
) -> AsyncIterator[Any]:
    """What ``read`` gives for each mailbox of ``reading``, as it arrives. One mailbox is read
    as it stands: whatever Graph refuses fails the read. The organisation's mailboxes are
    started in address order, ``at_once`` at a time; one Graph will not let be read at all
    (refused before it gave anything) is added to ``left_out`` and the others go on. Throttling
    Graph does not lift, and any refusal after a mailbox began to be read, fail the read."""
    with httpx.Client() as client:
        send = http_send(client)

        def rows_of(account: str) -> Iterator[Any]:
            return read(mail.Mailbox(Graph(send, reading.token), account))

        if not reading.organisation:
            (account,) = reading.accounts
            async for item in _stepped(lambda: rows_of(account)):
                yield item
            return

        waiting = list(reading.accounts)
        arrived: asyncio.Queue[Any] = asyncio.Queue(maxsize=max(reading.at_once, 1) * 2)
        last_refusal: list[str] = []

        async def worker() -> None:
            while waiting:
                account = waiting.pop(0)
                began = False
                try:
                    async for item in _stepped(lambda: rows_of(account)):  # noqa: B023  # the account of this turn; the loop awaits it to its end before the next
                        began = True
                        await arrived.put(item)
                except GraphRefused as refused:
                    if began or not _not_readable(refused):
                        raise
                    left_out.append(account)
                    last_refusal[:] = [f"{refused.status} {refused.code}"]

        workers = [
            asyncio.ensure_future(worker())
            for _ in range(max(min(reading.at_once, len(waiting)), 1))
        ]
        done = asyncio.ensure_future(asyncio.gather(*workers))
        try:
            while True:
                getter = asyncio.ensure_future(arrived.get())
                finished, _ = await asyncio.wait(
                    {getter, done}, return_when=asyncio.FIRST_COMPLETED
                )
                if getter in finished:
                    yield getter.result()
                    continue
                getter.cancel()
                done.result()  # raises what a worker raised
                while not arrived.empty():
                    yield arrived.get_nowait()
                break
        finally:
            for task in workers:
                task.cancel()
            await asyncio.gather(*workers, return_exceptions=True)
        if reading.accounts and len(left_out) == len(reading.accounts):
            raise NoMailboxRead(len(left_out), last_refusal[0])
        if left_out:
            log.warning(
                "microsoft_365: %d of %d mailbox(es) were left out, not there or not permitted: %s",
                len(left_out),
                len(reading.accounts),
                ", ".join(sorted(left_out)),
            )


def _notes(unreadable: list[str], left_out: list[str]) -> list[Any]:
    from provisa.federation.data_replicator import (
        mailboxes_left_out_note,
        noted,
        unreadable_messages_note,
    )

    return noted(unreadable_messages_note(unreadable), mailboxes_left_out_note(sorted(left_out)))


def make_microsoft365_loader(connect: Connect) -> Any:
    """The row-fetch of a Microsoft 365 source: one of its tables, whole, read through Graph
    as ``connect`` signs the source in."""

    async def _batches(
        source: Any,
        table: Any,
        unreadable: list[str] | None = None,
        left_out: list[str] | None = None,
    ) -> AsyncIterator[list[dict]]:
        name = table.table_name
        if name not in TABLES:
            raise UnknownMailTable(name)
        reading = await connect(source)
        async for rows in _read_mailboxes(
            reading,
            lambda mailbox: mailbox.rows(name, unreadable),
            [] if left_out is None else left_out,
        ):
            if rows:
                yield rows

    async def _load(source: Any, table: Any) -> list[dict]:
        return [row async for rows in _batches(source, table) for row in rows]

    def _replica_source(source: Any, table: Any, columns: list[tuple[str, str]]) -> Any:
        from provisa.federation.replica_source import CursorSource

        unreadable: list[str] = []
        left_out: list[str] = []

        def read(_batch_rows: int) -> AsyncIterator[list[dict]]:
            # A build reads once; a read begun again counts again.
            unreadable.clear()
            left_out.clear()
            return _batches(source, table, unreadable, left_out)

        # The notes are the ones every mail source gives (same codes and particulars).
        return CursorSource(read, columns, notes=lambda: _notes(unreadable, left_out))

    def _replica_group(_source: Any, table: Any) -> tuple[str, ...] | None:
        """The tables one read of the mailbox gives together with ``table``, or None when
        ``table`` is read on its own (folders)."""
        name = table.table_name
        return mail.MESSAGE_TABLES if name in mail.MESSAGE_TABLES else None

    def _replica_group_source(
        source: Any, tables: tuple[str, ...], columns: dict[str, list[tuple[str, str]]]
    ) -> Any:
        return _MessagesRead(connect, source, tuple(tables), columns)

    _load.replica_source = _replica_source  # type: ignore[attr-defined]
    _load.replica_group = _replica_group  # type: ignore[attr-defined]
    _load.replica_group_source = _replica_group_source  # type: ignore[attr-defined]
    return _load


class _MessagesRead:
    """One read of a source's mailboxes' messages as the rows of several canonical tables: what a
    group build streams (``federation.data_replicator.ReplicaGroupJob``)."""

    def __init__(
        self,
        connect: Connect,
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
        self._left_out: list[str] = []

    def notes(self) -> list[Any]:
        return _notes(self._unreadable, self._left_out)

    async def batches(self, batch_rows: int) -> AsyncIterator[tuple[str, Any]]:
        from provisa.core.ir_arrow import arrow_schema, rows_to_batch

        # A build reads once; a read begun again counts again.
        self._unreadable.clear()
        self._left_out.clear()
        schemas = {table: arrow_schema(self._columns[table]) for table in self._tables}
        reading = await self._connect(self._source)
        async for table, rows in _read_mailboxes(
            reading,
            lambda mailbox: mailbox.message_tables(self._tables, self._unreadable),
            self._left_out,
        ):
            for start in range(0, len(rows), batch_rows):
                yield (
                    table,
                    rows_to_batch(
                        rows[start : start + batch_rows], self._columns[table], schemas[table]
                    ),
                )


async def listed_mailboxes(token: Callable[[], str], chosen: Any) -> list[str]:
    """The mailboxes ``chosen`` names, as the directory lists them now, read off the event loop
    with the organisation's own token."""
    from provisa.microsoft365 import directory

    def listed() -> list[str]:
        with httpx.Client() as http:
            return directory.mailboxes(Graph(http_send(http), token), chosen)

    return await asyncio.to_thread(listed)


async def organisation_token(admin_db: Any, org_id: str) -> Callable[[], str]:
    """The token of the organisation's Microsoft client, for reading the organisation's
    mailboxes: refused by name unless its administrator has allowed such sources. The client's
    secret is resolved here, where the organisation's vault is bound, and held only by the
    token's own renewal."""
    from provisa.core import mail_platforms
    from provisa.core.secrets import resolve_secrets
    from provisa.microsoft365 import SOURCE_TYPE, directory
    from provisa.microsoft365 import settings as m365

    client = await mail_platforms.require_organisation(admin_db, org_id, SOURCE_TYPE)
    return directory.ClientToken(
        m365.token_url(client.settings.get("tenant")),
        client.client_id,
        resolve_secrets(client.client_secret),
    )


def make_connect(state: Any) -> Connect:
    """How a source is signed in for a read: the organisation's Microsoft client
    (``core.mail_platforms``, for the organisation the build runs in) and the source's own
    refresh token, exchanged under the lock every refresh of the source takes
    (``api_source.oauth_store``) because Microsoft replaces a refresh token each time it is
    used."""
    from provisa.api_source import oauth_store
    from provisa.api_source.oauth_grants import exchange_refresh_token
    from provisa.core import mail_platforms, settings_registry
    from provisa.core.auth_models import ApiAuthOAuth2RefreshToken
    from provisa.core.config_loader import load_control_plane
    from provisa.core.config_location import config_path_str
    from provisa.core.request_context import require_current_org
    from provisa.core.secrets import resolve_secrets
    from provisa.microsoft365 import SOURCE_TYPE
    from provisa.microsoft365 import settings as m365

    async def connect(source: Any) -> Reading:
        settings = m365.parse(dict(getattr(source, "mapping", None) or {}))
        org_id = require_current_org()
        if settings.organisation:
            assert settings.mailboxes is not None  # the organisation shape names its mailboxes
            # Refused by name unless the organisation's administrator allowed sources that
            # read its mailboxes: asked here, each time a token is about to be asked for.
            token = await organisation_token(state.admin_db, org_id)
            return Reading(
                tuple(await listed_mailboxes(token, settings.mailboxes)),
                token,
                organisation=True,
                at_once=int(settings_registry.value("mail.mailboxes_at_once")),
            )
        client = await mail_platforms.require(state.admin_db, org_id, SOURCE_TYPE)
        # Resolved here, where the organisation's vault is bound: the exchange runs in a
        # worker thread, which holds none.
        client_secret = resolve_secrets(client.client_secret)
        url = m365.token_url(client.settings.get("tenant"))
        platform_url = load_control_plane(config_path_str()).resolved_platform_url()
        loop = asyncio.get_running_loop()
        assert settings.account is not None  # the one-mailbox shape names it (settings.parse)

        def exchange(refresh_token: str) -> oauth_store.Grant:
            answered = exchange_refresh_token(
                ApiAuthOAuth2RefreshToken(
                    client_id=client.client_id,
                    client_secret=client_secret,
                    refresh_token=refresh_token,
                    token_url=url,
                    scope=" ".join(settings.scopes()),
                )
            )
            return oauth_store.Grant(
                answered.access_token, answered.expires_in, answered.replacement
            )

        def signed_in() -> str:
            # Called from the thread that reads Graph; the store is the loop's.
            return asyncio.run_coroutine_threadsafe(
                oauth_store.stored_access_token(
                    state.admin_db,
                    platform_url,
                    org_id,
                    source_id=source.id,
                    secret_name=settings.refresh_token_name,
                    exchange=exchange,
                    replaces=True,
                ),
                loop,
            ).result()

        return Reading((settings.account,), signed_in)

    return connect
