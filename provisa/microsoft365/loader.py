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
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from typing import Any

import httpx

from provisa.microsoft365 import mail
from provisa.microsoft365.graph import Graph, http_send

TABLES: tuple[str, ...] = mail.MAIL_TABLES
#: A mailbox has no schema of its own; its tables are listed under this one.
SCHEMA = "default"

#: What reading a source needs, made where the organisation's vault is bound: the mailbox's
#: address, and a call that gives the access token for each request (called off the event loop).
Connection = tuple[str, Callable[[], str]]
Connect = Callable[[Any], Awaitable[Connection]]

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


async def _stepped(make: Callable[[], Iterator[list[dict]]]) -> AsyncIterator[list[dict]]:
    """A blocking iterator's batches, each produced off the event loop."""
    batches = await asyncio.to_thread(make)
    while True:
        rows = await asyncio.to_thread(next, batches, _END)
        if rows is _END:
            return
        yield rows  # type: ignore[misc]  # not the end marker


def make_microsoft365_loader(connect: Connect) -> Any:
    """The row-fetch of a Microsoft 365 source: one of its tables, whole, read through Graph
    as ``connect`` signs the source in."""

    async def _batches(
        source: Any, table: Any, unreadable: list[str] | None = None
    ) -> AsyncIterator[list[dict]]:
        if table.table_name not in TABLES:
            raise UnknownMailTable(table.table_name)
        account, token = await connect(source)
        with httpx.Client() as client:
            graph = Graph(http_send(client), token)
            async for rows in _stepped(
                lambda: table_rows(graph, account, table.table_name, unreadable)
            ):
                if rows:
                    yield rows

    async def _load(source: Any, table: Any) -> list[dict]:
        return [row async for rows in _batches(source, table) for row in rows]

    def _replica_source(source: Any, table: Any, columns: list[tuple[str, str]]) -> Any:
        from provisa.federation.data_replicator import unreadable_messages_note
        from provisa.federation.replica_source import CursorSource

        unreadable: list[str] = []

        def read(_batch_rows: int) -> AsyncIterator[list[dict]]:
            unreadable.clear()  # a build reads once; a read begun again counts again
            return _batches(source, table, unreadable)

        # The note is the one both mail sources give (same code and particulars).
        return CursorSource(read, columns, note=lambda: unreadable_messages_note(unreadable))

    _load.replica_source = _replica_source  # type: ignore[attr-defined]
    return _load


def make_connect(state: Any) -> Connect:
    """How a source is signed in for a read: the organisation's Microsoft client
    (``core.mail_platforms``, for the organisation the build runs in) and the source's own
    refresh token, exchanged under the lock every refresh of the source takes
    (``api_source.oauth_store``) because Microsoft replaces a refresh token each time it is
    used."""
    from provisa.api_source import oauth_store
    from provisa.api_source.oauth_grants import exchange_refresh_token
    from provisa.core import mail_platforms
    from provisa.core.auth_models import ApiAuthOAuth2RefreshToken
    from provisa.core.config_loader import load_control_plane
    from provisa.core.config_location import config_path_str
    from provisa.core.request_context import require_current_org
    from provisa.core.secrets import resolve_secrets
    from provisa.microsoft365 import SOURCE_TYPE
    from provisa.microsoft365 import settings as m365

    async def connect(source: Any) -> Connection:
        settings = m365.parse(dict(getattr(source, "mapping", None) or {}))
        org_id = require_current_org()
        client = await mail_platforms.require(state.admin_db, org_id, SOURCE_TYPE)
        # Resolved here, where the organisation's vault is bound: the exchange runs in a
        # worker thread, which holds none.
        client_secret = resolve_secrets(client.client_secret)
        url = m365.token_url(client.settings.get("tenant"))
        platform_url = load_control_plane(config_path_str()).resolved_platform_url()
        loop = asyncio.get_running_loop()

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

        def token() -> str:
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

        return settings.account, token

    return connect
