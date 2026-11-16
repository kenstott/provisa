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
from datetime import UTC, date, datetime
from typing import Any

import httpx

from provisa.core import canonical_mail as cm
from provisa.microsoft365 import calendar, directory, mail
from provisa.microsoft365 import settings as m365_settings
from provisa.microsoft365.graph import (
    FOLDER_NOT_THERE,
    WAIT_SECONDS,
    Graph,
    GraphRefused,
    http_send,
)

log = logging.getLogger(__name__)

MAIL_TABLES: tuple[str, ...] = mail.MAIL_TABLES
#: Every table a source can offer: its mail, calendar and task tables (REQ-1994). A source
#: offers those of the resources it reads (``canonical_mail.tables_for``).
TABLES: tuple[str, ...] = cm.tables_for((cm.MAIL, cm.CALENDAR, cm.TASKS))
#: The application permission a source of the organisation's mailboxes needs for a calendar or
#: task table (REQ-1994).
_NEED: dict[str, str] = {
    **dict.fromkeys(cm.RESOURCE_TABLES[cm.CALENDAR], directory.CALENDARS_WRITE),
    **dict.fromkeys(cm.RESOURCE_TABLES[cm.TASKS], directory.TASKS_WRITE),
}
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
    #: Keeps where a completed read of the source reached (``federation.mail_sync_state``
    #: rows): called once the build that read to it is recorded as completed. None where
    #: nothing is kept.
    keep: Callable[[dict[tuple[str, str, str], str]], Awaitable[None]] | None = None
    #: What was kept by the last completed read of the source; None where nothing is kept.
    kept: Callable[[], Awaitable[dict[tuple[str, str, str], str]]] | None = None
    #: Records, for the tables a completed read just fed, that their check for changes has
    #: nothing to report: the check's next answer is then compared with that, and a read just
    #: made is not asked for again. None where no check is kept.
    settle: Callable[[tuple[str, ...]], Awaitable[None]] | None = None
    #: The most the waits one throttled call is asked for may add up to (the operator's
    #: ``mail.throttle_wait_seconds`` for a replica's read).
    wait_seconds: float = WAIT_SECONDS
    #: Told, from the thread that reads Graph, how long a throttled call is about to wait and
    #: 0 when the wait is over, so the build's record can say what it is waiting on.
    throttled: Callable[[float], None] | None = None
    #: Nothing received before this date is held (the source's ``mail_since``); None: any age.
    since: date | None = None

    @property
    def since_marker(self) -> str:
        """The date as it is kept beside what a mailbox's read reached (:data:`SINCE`)."""
        return ANY_AGE if self.since is None else self.since.isoformat()

    #: For a read that lands a part at a time (``federation.mail_units``): keeps where the
    #: read of one mailbox reached, once that part's rows are written; and keeps nothing of
    #: what an earlier read of a mailbox reached. ``kind`` is the state store's (delta, probe).
    keep_part: Callable[[str, dict[str, str], str], Awaitable[None]] | None = None
    forget_mailbox: Callable[[str], Awaitable[None]] | None = None


Connect = Callable[[Any], Awaitable[Reading]]

#: What Graph answers for a mailbox that is not there or that the client may not read. Such a
#: mailbox of the organisation's is left out of the build and reported; anything else Graph
#: refuses fails the build, and so does throttling it does not lift.
_NOT_READABLE_STATUS = frozenset({403, 404})
_NOT_READABLE_CODES = frozenset({"MailboxNotEnabledForRESTAPI", "MailboxNotSupportedForRESTAPI"})
#: What Graph answers for a mailbox at its storage quota when its changes are tracked (the
#: delta rounds a read is made of write their state into the mailbox). It is left out like one
#: that cannot be read, and reported with this reason of its own.
_AT_QUOTA_CODE = "ErrorQuotaExceeded"


#: What is kept for a mailbox the last completed read left out, in place of links.
LEFT_OUT = "left_out"
#: Under this scope the date a mailbox was read with is kept beside where its read reached
#: (as a probe-kind row): a mailbox read with another date than the source states now holds
#: the wrong mail -- too little if the date moved back, too much if it moved on -- and where
#: its read reached says nothing of the difference. It is read again from its start.
SINCE = "mail_since"
#: What is kept under :data:`SINCE` for a source that states no date.
ANY_AGE = "any"
#: What the check for changes answers while nothing changed (``events.check_node`` compares it
#: with the last one it kept; "cannot tell" is None, which is never taken for unchanged).
UNCHANGED = "unchanged"


async def _calendar_token(connect: Connect, source: Any, table: str) -> str | None:
    """The change token of the source's calendars, or its task lists (``calendar.change_lines``
    for every mailbox), or None when Graph cannot be asked: None is never "unchanged", so a read
    is asked for and reports the reason on the replica."""
    try:
        reading = await connect(source)
        settings = m365_settings.parse(dict(getattr(source, "mapping", None) or {}))

        def lines() -> list[str]:
            with httpx.Client() as client:
                graph = Graph(http_send(client), reading.token, wait_seconds=reading.wait_seconds)
                out: list[str] = []
                for account in reading.accounts:
                    try:
                        out.extend(
                            calendar.change_lines(
                                graph.for_mailbox(account),
                                account,
                                table,
                                settings.check_days or 0,
                            )
                        )
                    except GraphRefused as refused:
                        if not (reading.organisation and _not_readable(refused)):
                            raise
                        out.append(f"{account}\x00{LEFT_OUT}")
                return out

        return calendar.token_of(await asyncio.to_thread(lines))
    except Exception as failed:  # allow-ble: a check that cannot be made, for whatever reason, must ask for a read, which reports that reason on the replica; it must never say "unchanged"
        log.warning(
            "microsoft_365 %s: could not check %s for changes, so a read is asked for (%s: %s)",
            getattr(source, "id", "?"),
            table,
            type(failed).__name__,
            failed,
        )
        return None


async def unchanged(connect: Connect, source: Any) -> str | None:
    """:data:`UNCHANGED` when nothing of the source's mail changed since its last completed
    read, else None: something changed, or it cannot be told -- nothing was kept yet, a
    mailbox joined or left the source, a mailbox that was left out can be read now, or Graph
    would not answer. None asks for a read; it never says the copy is current.

    One small batch of requests for each mailbox: every kept link followed for one item. Nothing
    is kept from the answers, so the links stand until a completed read replaces them."""
    try:
        return await _asked_unchanged(connect, source)
    except Exception as failed:  # allow-ble: a check that cannot be made, for whatever reason (the sign-in refused, the organisation's switch off, the directory or the network not answering), must ask for a read, which reports that reason on the replica; it must never end the tick with nothing asked, and never say "unchanged"
        log.warning(
            "microsoft_365 %s: could not check for changes, so a read is asked for (%s: %s)",
            getattr(source, "id", "?"),
            type(failed).__name__,
            failed,
        )
        return None


async def _asked_unchanged(connect: Connect, source: Any) -> str | None:
    from provisa.federation.mail_sync_state import DELTA

    reading = await connect(source)
    if reading.kept is None:
        return None
    links: dict[str, dict[str, str]] = {}
    left_out: set[str] = set()
    dated: dict[str, str] = {}
    for (account, scope, kind), token in (await reading.kept()).items():
        if kind == DELTA:
            links.setdefault(account, {})[scope] = token
        elif scope == SINCE:
            dated[account] = token
        elif token == LEFT_OUT:
            left_out.add(account)
    if not links or set(links) | left_out != {a.lower() for a in reading.accounts}:
        return None
    if any(dated.get(account, ANY_AGE) != reading.since_marker for account in links):
        return None  # read with another date than the source states now: it is read again
    if any(mail.MAILBOX not in scopes for scopes in links.values()):
        return None  # a mailbox whose read did not reach its end: the read goes on

    def asked() -> bool:
        with httpx.Client() as client:
            graph = Graph(http_send(client), reading.token, wait_seconds=reading.wait_seconds)
            if any(mail.Mailbox(graph, account).readable() for account in sorted(left_out)):
                return True
            return any(
                mail.Mailbox(graph, account).changed(kept)
                for account, kept in sorted(links.items())
            )

    return None if await asyncio.to_thread(asked) else UNCHANGED


class NoMailboxRead(RuntimeError):
    """Not one of the organisation's mailboxes could be read: the client's permission, not a
    mailbox, is what is wrong."""

    def __init__(
        self, count: int, reason: str, mail_read: bool | None = None, need: str = "Mail.Read"
    ) -> None:
        """``mail_read``: whether the client's token carries ``need`` (Mail.Read for mail,
        REQ-1994's Calendars.ReadWrite or Tasks.ReadWrite.All for calendars and tasks); None
        when it cannot be told. Without it the client reads only what Exchange lets it (a role
        assignment over a scope of mailboxes), which is a setup that works -- so it is not
        called a fault, and the scope is what is pointed at."""
        if mail_read is True:
            why = (
                f"the organisation's Microsoft 365 client's token carries {need}, so the "
                "permission is granted: check that these addresses are mailboxes of this "
                "tenant, and whether an application access policy in Exchange keeps the "
                "client from them"
            )
        elif mail_read is False:
            why = (
                f"the organisation's Microsoft 365 client's token does not carry {need}. "
                f"Either grant the application permission {need} with admin consent, or, "
                "where the client is given mail through Exchange (a role assignment over a "
                "scope of mailboxes), put these mailboxes in that scope; a new or changed "
                "assignment can take up to two hours to apply"
            )
        else:
            why = f"the organisation's Microsoft 365 client has not been granted {need}"
        super().__init__(f"none of the {count} mailboxes could be read ({reason}): {why}")


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


def _at_quota(refused: BaseException) -> bool:
    return isinstance(refused, GraphRefused) and refused.code == _AT_QUOTA_CODE


def _not_readable(refused: BaseException) -> bool:
    """Whether Graph refused the MAILBOX: it is not there, or may not be read. A FOLDER the
    mailbox says it does not have is the mailbox answering, and is never that."""
    if not isinstance(refused, GraphRefused) or refused.code == FOLDER_NOT_THERE:
        return False
    return refused.status in _NOT_READABLE_STATUS or refused.code in _NOT_READABLE_CODES


async def _read_mailboxes(
    reading: Reading,
    read: Callable[[mail.Mailbox], Iterator[Any]],
    left_out: list[str],
    reached: dict[str, dict[str, str]] | None = None,
    at_quota: list[str] | None = None,
    failed: dict[str, str] | None = None,
    need: str = "Mail.Read",
) -> AsyncIterator[Any]:
    """What ``read`` gives for each mailbox of ``reading``, as it arrives. One mailbox is read
    as it stands: whatever Graph refuses fails the read. The organisation's mailboxes are
    started in address order, ``at_once`` at a time; one Graph will not let be read at all
    (refused before it gave anything) is added to ``left_out`` and the others go on. Throttling
    Graph does not lift, and any refusal after a mailbox began to be read, fail the read."""
    with httpx.Client() as client:
        send = http_send(client)

        def rows_of(account: str) -> Iterator[Any]:
            mailbox = mail.Mailbox(
                Graph(
                    send,
                    reading.token,
                    wait_seconds=reading.wait_seconds,
                    waiting=reading.throttled,
                ),
                account,
                since=reading.since,
            )
            yield from read(mailbox)
            # Where this mailbox's read reached, once it has been read to its end.
            if reached is not None and mailbox.links is not None:
                reached[mailbox.account] = dict(mailbox.links)

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
                    if began and failed is not None:
                        # What it gave before it failed is written and stays; the others go on.
                        failed[account] = f"{refused.status} {refused.code}"
                        continue
                    if began or not (_not_readable(refused) or _at_quota(refused)):
                        raise
                    if _at_quota(refused) and at_quota is not None:
                        at_quota.append(account)
                    else:
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
        if reading.accounts and len(left_out) + len(at_quota or ()) == len(reading.accounts):
            from provisa.microsoft365 import directory

            roles = await asyncio.to_thread(lambda: directory.carried(reading.token()))
            raise NoMailboxRead(
                len(reading.accounts),
                last_refusal[0],
                None if roles is None else not directory.lacks(roles, need),
                need,
            )
        if left_out:
            log.warning(
                "microsoft_365: %d of %d mailbox(es) were left out, not there or not permitted: %s",
                len(left_out),
                len(reading.accounts),
                ", ".join(sorted(left_out)),
            )


def _notes(
    unreadable: list[str], left_out: list[str], at_quota: list[str] | None = None
) -> list[Any]:  # type: ignore[assignment]  # an empty default that is never added to
    from provisa.federation.data_replicator import (
        mailboxes_at_quota_note,
        mailboxes_left_out_note,
        noted,
        unreadable_messages_note,
    )

    return noted(
        unreadable_messages_note(unreadable),
        mailboxes_left_out_note(sorted(left_out)),
        mailboxes_at_quota_note(sorted(at_quota or [])),
    )


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
        if name in MAIL_TABLES:
            read = lambda mailbox: mailbox.rows(name, unreadable)  # noqa: E731
            need = directory.MAIL_READ
        else:
            # REQ-1994: a calendar or task table, read whole for every mailbox.
            read = lambda mailbox: calendar.account_rows(  # noqa: E731
                mailbox.graph, mailbox.account, name
            )
            need = _NEED[name]
        async for rows in _read_mailboxes(
            reading, read, [] if left_out is None else left_out, need=need
        ):
            if rows:
                yield rows

    async def _load(source: Any, table: Any) -> list[dict]:
        return [row async for rows in _batches(source, table) for row in rows]

    def _replica_source(source: Any, table: Any, columns: list[tuple[str, str]]) -> Any:
        from provisa.federation.replica_source import CursorSource

        unreadable: list[str] = []
        left_out: list[str] = []

        async def read(_batch_rows: int) -> AsyncIterator[list[dict]]:
            # A build reads once; a read begun again counts again.
            unreadable.clear()
            left_out.clear()
            at = datetime.now(UTC)  # one whole build lands together, under one stamp
            async for rows in _batches(source, table, unreadable, left_out):
                yield cm.stamped(rows, at)

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

    def _freshness_probe(source: Any, table: Any) -> Any:
        """The table's check for changes (REQ-855, REQ-982), for a table read from the
        mailboxes: any of the message tables, and the folders, whose counts change with every
        message. None for any other table."""
        name = table.table_name
        if name not in TABLES:
            return None
        if name not in MAIL_TABLES:

            async def calendar_token() -> str | None:
                return await _calendar_token(connect, source, name)

            return calendar_token

        async def token() -> str | None:
            return await unchanged(connect, source)

        return token

    _load.replica_source = _replica_source  # type: ignore[attr-defined]
    _load.freshness_probe = _freshness_probe  # type: ignore[attr-defined]
    _load.replica_group = _replica_group  # type: ignore[attr-defined]
    _load.replica_group_source = _replica_group_source  # type: ignore[attr-defined]
    _load.replica_unit_source = lambda source, tables, _columns: _UnitsRead(  # type: ignore[attr-defined]
        connect, source, tuple(tables)
    )
    _load.replica_key_columns = cm.key_columns  # type: ignore[attr-defined]
    _load.replica_stamp_column = cm.LANDED_AT  # type: ignore[attr-defined]
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
        self._at_quota: list[str] = []
        self._reached: dict[str, dict[str, str]] = {}
        self._reading: Reading | None = None

    def notes(self) -> list[Any]:
        return _notes(self._unreadable, self._left_out, self._at_quota)

    async def committed(self) -> None:
        """Keep where this read reached, now that the build it fed is recorded as completed
        (the replica runner awaits this then, and never for a failed build): each read
        mailbox's links, a folder's under the folder's id. What was kept for a mailbox or a
        folder this read did not see is forgotten with the same write.

        Nothing is kept by a read that did not read every one of its mailboxes' messages to
        the end with their links (one that fed no table needing them): what is kept already
        stands, and the next check asks from there."""
        from provisa.federation.mail_sync_state import DELTA, PROBE

        reading = self._reading
        if reading is None or reading.keep is None:
            return
        # A mailbox is kept under its address as the tables' ``account`` column has it.
        gone = {a.lower() for a in (*self._left_out, *self._at_quota)}
        read = {a.lower() for a in reading.accounts} - gone
        if set(self._reached) != read:
            return
        rows = {
            (account, scope, DELTA): link
            for account, links in self._reached.items()
            for scope, link in links.items()
        }
        # A mailbox left out is kept as that: the next check asks whether it can be read now,
        # instead of taking it for one added to the source since.
        rows.update({(account, mail.MAILBOX, PROBE): LEFT_OUT for account in gone})
        # The date each mailbox was read with, beside where its read reached.
        rows.update({(account, SINCE, PROBE): reading.since_marker for account in self._reached})
        await reading.keep(rows)
        if reading.settle is not None:
            await reading.settle(self._tables)

    async def batches(self, batch_rows: int) -> AsyncIterator[tuple[str, Any]]:
        from provisa.core.ir_arrow import arrow_schema, rows_to_batch

        # A build reads once; a read begun again counts again.
        self._unreadable.clear()
        self._left_out.clear()
        self._at_quota.clear()
        self._reached.clear()
        schemas = {table: arrow_schema(self._columns[table]) for table in self._tables}
        # A whole build writes its copy at once: every row of it lands together, under one
        # stamp (a table registered before the stamp existed has no such column and gets none).
        at = datetime.now(UTC)
        reading = self._reading = await self._connect(self._source)
        async for table, rows in _read_mailboxes(
            reading,
            lambda mailbox: mailbox.message_tables(self._tables, self._unreadable),
            self._left_out,
            self._reached,
            self._at_quota,
        ):
            cm.stamped(rows, at)
            for start in range(0, len(rows), batch_rows):
                yield (
                    table,
                    rows_to_batch(
                        rows[start : start + batch_rows], self._columns[table], schemas[table]
                    ),
                )


class _UnitsRead:
    """One pass over a source's mailboxes that gives their messages as units to be written in
    place (``federation.mail_units.ReplicaUnitJob``): each mailbox from where its last read
    reached, or from its start. ``total`` is how many mailboxes the pass reads, known once it
    has begun."""

    #: What a pass counts as it reads them to their end (``replica_state.record_parts``).
    unit = "mailboxes"

    def __init__(self, connect: Connect, source: Any, tables: tuple[str, ...]) -> None:
        self._connect = connect
        self._source = source
        self._tables = tables
        self._unreadable: list[str] = []
        self._left_out: list[str] = []
        self._at_quota: list[str] = []
        self._failed: dict[str, str] = {}
        self._reading: Reading | None = None
        self.total = 0

    def notes(self) -> list[Any]:
        from provisa.federation.data_replicator import mailboxes_failed_note

        return [
            *_notes(self._unreadable, self._left_out, self._at_quota),
            *([note] if (note := mailboxes_failed_note(sorted(self._failed))) else []),
        ]

    async def keep(self, account: str, links: dict[str, str]) -> None:
        from provisa.federation.mail_sync_state import DELTA

        from provisa.federation.mail_sync_state import PROBE

        assert self._reading is not None and self._reading.keep_part is not None
        await self._reading.keep_part(account, links, DELTA)
        if mail.MAILBOX in links:
            # The mailbox was read to its end: the date it was read with is kept with that.
            await self._reading.keep_part(account, {SINCE: self._reading.since_marker}, PROBE)

    async def forget(self, account: str) -> None:
        assert self._reading is not None and self._reading.forget_mailbox is not None
        await self._reading.forget_mailbox(account)

    async def units(self) -> AsyncIterator[cm.Unit]:
        from provisa.federation.mail_sync_state import DELTA

        for said in (self._unreadable, self._left_out, self._at_quota):
            said.clear()
        self._failed.clear()
        reading = self._reading = await self._connect(self._source)
        if reading.kept is None or reading.keep_part is None or reading.forget_mailbox is None:
            raise RuntimeError("a read in units needs somewhere to keep where it reached")
        kept: dict[str, dict[str, str]] = {}
        dated: dict[str, str] = {}
        for (account, scope, kind), token in (await reading.kept()).items():
            if kind == DELTA:
                kept.setdefault(account, {})[scope] = token
            else:
                kept.setdefault(account, {})
                if scope == SINCE:
                    dated[account] = token
        for account, links in kept.items():
            if links and dated.get(account, ANY_AGE) != reading.since_marker:
                # Read with another date than the source states now: what it holds is not
                # what the date asks for, so it is read again from its start, which replaces
                # its rows.
                links.clear()
        accounts = {a.lower() for a in reading.accounts}
        self.total = len(accounts)
        # A mailbox the source no longer names: its rows go, and what was kept for it.
        for account in sorted(set(kept) - accounts):
            yield cm.Unit(
                account,
                removed={
                    t: [{"provider": mail.PROVIDER, "account": account}] for t in self._tables
                },
                forget=True,
            )
        async for unit in _read_mailboxes(
            reading,
            lambda mailbox: mailbox.units(
                self._tables, kept.get(mailbox.account), self._unreadable
            ),
            self._left_out,
            None,
            self._at_quota,
            self._failed,
        ):
            yield unit
        if accounts and len(self._failed) == len(accounts):
            raise RuntimeError(
                f"none of the {len(accounts)} mailbox(es) could be read to its end; the last "
                f"refusal: {next(reversed(self._failed.values()))}"
            )

    async def committed(self) -> None:
        """Once the pass is recorded as completed: a mailbox left out is kept as that, so the
        next check asks whether it can be read now; and the tables' check for changes is told
        it has nothing to report."""
        from provisa.federation.mail_sync_state import PROBE

        reading = self._reading
        if reading is None or reading.keep_part is None or reading.forget_mailbox is None:
            return
        for account in sorted({a.lower() for a in (*self._left_out, *self._at_quota)}):
            await reading.forget_mailbox(account)
            await reading.keep_part(account, {mail.MAILBOX: LEFT_OUT}, PROBE)
        if reading.settle is not None and not self._failed:
            await reading.settle(self._tables)


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
    secret is resolved here, in that organisation's vault, and held only by the
    token's own renewal."""
    from provisa.core import mail_platforms
    from provisa.microsoft365 import SOURCE_TYPE, directory
    from provisa.microsoft365 import settings as m365

    client = await mail_platforms.require_organisation(admin_db, org_id, SOURCE_TYPE)
    return directory.ClientToken(
        m365.token_url(client.settings.get("tenant")),
        client.client,
        # In the entry's own organisation's vault, whatever the caller has bound.
        await mail_platforms.credential(
            admin_db, org_id, SOURCE_TYPE, mail_platforms.CLIENT_SECRET, client.client_secret
        ),
    )


def make_connect(state: Any) -> Connect:
    """How a source is signed in for a read: with the organisation's Microsoft client
    (``core.mail_platforms``, for the organisation the build runs in) and, for one mailbox, the
    approval its owner gave at sign-in, kept in the organisation's state store and renewed under
    the lock every refresh of the source takes (``api_source.oauth_store``), because Microsoft
    replaces it each time it is used."""
    from provisa.api_source import oauth_store
    from provisa.api_source.oauth_grants import exchange_refresh_token
    from provisa.core import mail_platforms, secrets_store, settings_registry
    from provisa.core.auth_models import ApiAuthOAuth2RefreshToken
    from provisa.core.config_loader import load_control_plane
    from provisa.core.config_location import config_path_str
    from provisa.core.secrets import resolve_secrets
    from provisa.core.request_context import require_current_org
    from provisa.microsoft365 import SOURCE_TYPE
    from provisa.microsoft365 import settings as m365

    async def connect(source: Any) -> Reading:
        settings = m365.parse(dict(getattr(source, "mapping", None) or {}))
        org_id = require_current_org()

        async def keep(rows: dict[tuple[str, str, str], str]) -> None:
            from provisa.federation import mail_sync_state

            async with state.tenant_db.acquire() as conn:
                await mail_sync_state.replace(conn, source.id, rows)

        async def kept() -> dict[tuple[str, str, str], str]:
            from provisa.federation import mail_sync_state

            async with state.tenant_db.acquire() as conn:
                return await mail_sync_state.read_all(conn, source.id)

        loop = asyncio.get_running_loop()
        wait_seconds = float(settings_registry.value("mail.throttle_wait_seconds"))

        def throttled(seconds: float) -> None:
            # Called from the thread that reads Graph; the state store is the loop's. The
            # record of every table this source is building says what it waits on.
            from provisa.federation import replica_state

            async def say() -> None:
                async with state.tenant_db.acquire() as conn:
                    await replica_state.record_throttled(
                        conn, source.id, seconds=seconds, by="Microsoft"
                    )

            asyncio.run_coroutine_threadsafe(say(), loop).result()

        async def keep_part(account: str, links: dict[str, str], kind: str) -> None:
            from provisa.federation import mail_sync_state

            async with state.tenant_db.acquire() as conn:
                await mail_sync_state.keep_links(conn, source.id, account, links, kind=kind)

        async def forget_mailbox(account: str) -> None:
            from provisa.federation import mail_sync_state

            async with state.tenant_db.acquire() as conn:
                await mail_sync_state.forget_account(conn, source.id, account)

        async def settle(tables: tuple[str, ...]) -> None:
            # What the poll node of each table compares its check's answer with
            # (events.injector.check_node): set to "unchanged" by the read that just completed,
            # so the first check after it finds nothing new and asks for no second read.
            from provisa.events import queue
            from provisa.events.nodes import source_node

            async with state.tenant_db.acquire() as conn:
                for table in tables:
                    await queue.set_node_state(
                        conn, source_node(source.id, SCHEMA, table), probe_token=UNCHANGED
                    )

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
                keep=keep,
                kept=kept,
                settle=settle,
                wait_seconds=wait_seconds,
                throttled=throttled,
                since=settings.since,
                keep_part=keep_part,
                forget_mailbox=forget_mailbox,
            )
        client = await mail_platforms.require(state.admin_db, org_id, SOURCE_TYPE)
        # Resolved here, in the entry's own organisation's vault whatever the caller has bound
        # (a replica build binds one only for a source whose own fields name a secret): the
        # exchange runs in a worker thread, which holds none.
        client_secret = await mail_platforms.credential(
            state.admin_db, org_id, SOURCE_TYPE, mail_platforms.CLIENT_SECRET, client.client_secret
        )
        assert settings.account is not None  # the one-mailbox shape names it (settings.parse)
        account = settings.account
        url = m365.token_url(client.settings.get("tenant"))
        platform_url = load_control_plane(config_path_str()).resolved_platform_url()
        loop = asyncio.get_running_loop()

        def exchange(refresh_token: str) -> oauth_store.Grant:
            answered = exchange_refresh_token(
                ApiAuthOAuth2RefreshToken(
                    client_id=client.client,
                    client_secret=client_secret,
                    refresh_token=refresh_token,
                    token_url=url,
                    scope=" ".join(settings.scopes()),
                )
            )
            return oauth_store.Grant(
                answered.access_token, answered.expires_in, answered.replacement
            )

        async def earlier() -> str | None:
            # A source connected before approvals were kept in the state store names its
            # owner's approval by a reference in its own settings: resolved once, here, in the
            # organisation's vault, and kept in the state store from then on.
            if settings.refresh_token is None:
                return None
            async with secrets_store.bound(state.admin_db, org_id):
                return resolve_secrets(settings.refresh_token, credential=True)

        def signed_in() -> str:
            # Called from the thread that reads Graph; the store is the loop's.
            return asyncio.run_coroutine_threadsafe(
                oauth_store.stored_access_token(
                    state.tenant_db,
                    platform_url,
                    org_id,
                    source_id=source.id,
                    account=account,
                    exchange=exchange,
                    replaces=True,  # Microsoft answers every refresh with a newer approval
                    earlier=earlier,
                ),
                loop,
            ).result()

        # REQ-1994: an approval that does not grant what the source now reads is refused here,
        # once, naming what to grant. Microsoft names a scope without Graph's address.
        await oauth_store.require_granted(
            state.tenant_db,
            source_id=source.id,
            account=account,
            needed=[m365.RESOURCE_SCOPES[r] for r in settings.resources],
            name=lambda scope: scope.removeprefix("https://graph.microsoft.com/"),
        )
        return Reading(
            (settings.account,),
            signed_in,
            keep=keep,
            kept=kept,
            settle=settle,
            wait_seconds=wait_seconds,
            throttled=throttled,
            since=settings.since,
            keep_part=keep_part,
            forget_mailbox=forget_mailbox,
        )

    return connect
