# Copyright (c) 2026 Kenneth Stott
# Canary: 5d0e7c3a-2b94-4f61-a8d7-9c1f6e3b4a20
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An access token for a source whose owner approved it at a sign-in.

The approval is a refresh token the provider issued. It is sign-in state, kept in the
organisation's state store beside what the source's reads reached
(``federation.mail_sync_state``, kind ``grant``, one for each source and account): written
when the sign-in completes, read here, and overwritten here when the provider replaces it.
It is in no vault and in no field of the source.

Some issuers answer every refresh with a new refresh token (Microsoft does; the one sent keeps
working only until its own lifetime ends). The state store then has to hold the newest one,
whichever process last refreshed. So one refresh of a source runs at a time, across every
process and node, and each reads the token it sends from the store after it has the lock:

- the lock is a session advisory lock on the platform control plane -- the form a replica build's
  lock takes (``provisa.federation.replica_locks``), released by the holder's death with nothing
  to renew;
- a process keeps the access token it was given and its expiry, never a refresh token, so a
  refresh can only send what the store holds;
- the replacement is written to the store before the access token is held: a write that fails
  fails the read and leaves no token behind.

A sign-in that keeps a new approval (:func:`store_refresh_token`) takes the same lock.
"""

from __future__ import annotations

import asyncio
import threading
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from sqlalchemy import text

from provisa.core import request_deadline

if TYPE_CHECKING:
    from provisa.core.database import Database

_REFRESH_LOCK_CLASS = 0x50564F52  # "PVOR"

#: How long a refresh waits for another holder of the source's lock before the read fails.
LOCK_WAIT_SECONDS = 30.0
#: How long the issuer is given to answer one exchange.
EXCHANGE_SECONDS = 30.0
#: An access token this close to its expiry is not handed out.
_EXPIRY_MARGIN_SECONDS = 60.0
_LOCK_POLL_SECONDS = 0.1


@dataclass(frozen=True)
class Grant:
    """What an issuer answered one refresh with."""

    access_token: str
    expires_in: float | None  # seconds; None when the issuer states no lifetime
    # The refresh token to send next time, where the issuer gave one.
    refresh_token: str | None = None

    def __repr__(self) -> str:  # a grant is never printed with its tokens
        return f"Grant(expires_in={self.expires_in})"


class RefreshBusy(RuntimeError):
    """Another refresh of the source held its lock for longer than a refresh waits."""


class RefreshTimedOut(RuntimeError):
    """The issuer did not answer an exchange in the time it is given."""


class ReplacementMissing(RuntimeError):
    """An issuer that replaces its refresh tokens answered without one."""


@dataclass
class _Held:
    access_token: str
    expires_at: float


_held: dict[tuple[str, str, str], _Held] = {}
_held_guard = threading.Lock()


def _fresh(key: tuple[str, str, str]) -> str | None:
    with _held_guard:
        held = _held.get(key)
    if held is None or held.expires_at - _EXPIRY_MARGIN_SECONDS <= time.monotonic():
        return None
    return held.access_token


def forget(org_id: str, source_id: str, account: str) -> None:
    """Drop the access token this process holds for the source's account."""
    with _held_guard:
        _held.pop((org_id, source_id, account.lower()), None)


class _RefreshLock:
    """The lock of one source's refresh. Every attempt is a try; the waiting is the caller's."""

    def __init__(self, platform_url: str, name: str) -> None:

        self._url = platform_url
        self._name = name
        self._engine: Any = None
        self._conn: Any = None
        self._held = False

    def try_acquire(self) -> bool:
        if self._conn is None:
            from provisa.core.database import control_plane_lock_engine

            shield = request_deadline.shielded()
            with shield.lock:
                shield.settle()
                self._engine = control_plane_lock_engine(self._url)
                self._conn = self._engine.connect().execution_options(isolation_level="AUTOCOMMIT")
        taken = bool(
            self._conn.execute(
                text("SELECT pg_try_advisory_lock(:cls, hashtext(:name))"),
                {"cls": _REFRESH_LOCK_CLASS, "name": self._name},
            ).scalar()
        )
        self._held = self._held or taken
        return taken

    def acquire(self, wait_seconds: float) -> bool:
        """Take the lock, trying for at most ``wait_seconds``. Blocking: run off the event loop."""
        deadline = time.monotonic() + wait_seconds
        while True:
            if self.try_acquire():
                return True
            if time.monotonic() >= deadline:
                return False
            time.sleep(_LOCK_POLL_SECONDS)

    def close(self) -> None:
        """Release the lock and what it was held on."""
        if self._conn is not None:
            if self._held and not self._conn.closed:  # a session that has ended holds no lock
                # Released by name before the session ends: PostgreSQL frees a session's locks only
                # once its backend has exited, which is after close() returns, so the next taker
                # could be refused a lock nobody holds.
                self._conn.execute(
                    text("SELECT pg_advisory_unlock(:cls, hashtext(:name))"),
                    {"cls": _REFRESH_LOCK_CLASS, "name": self._name},
                )
                self._held = False
            self._conn.close()
            self._conn = None
        if self._engine is not None:
            self._engine.dispose()
            self._engine = None


class _Locked:
    """``async with``: the source's refresh lock, taken and released off the event loop."""

    def __init__(self, platform_url: str, org_id: str, source_id: str, wait: float) -> None:
        self._lock = _RefreshLock(platform_url, f"{org_id}|{source_id}")
        self._source_id = source_id
        self._wait = wait

    async def __aenter__(self) -> None:
        try:
            taken = await asyncio.to_thread(self._lock.acquire, self._wait)
        except BaseException:
            await asyncio.to_thread(self._lock.close)
            raise
        if not taken:
            await asyncio.to_thread(self._lock.close)
            raise RefreshBusy(
                f"source {self._source_id!r}: another refresh of its credential has held the "
                f"lock for more than {self._wait:g}s"
            )

    async def __aexit__(self, *exc: object) -> None:
        await asyncio.to_thread(self._lock.close)


class NotConnected(RuntimeError):
    """No approval is kept for the source's account: its owner has not signed in."""


async def kept_refresh_token(
    state_db: "Database",
    *,
    source_id: str,
    account: str,
    earlier: Callable[[], Awaitable[str | None]] | None = None,
) -> str:
    """The approval kept for ``account`` of ``source_id`` in the organisation's state store.
    Where none is kept and ``earlier`` answers one (a source connected before approvals were
    kept here), that one is kept and answered. Refused by name when there is none at all."""
    from provisa.federation import mail_sync_state

    async with state_db.acquire() as conn:
        stored = await mail_sync_state.grant(conn, source_id, account)
    if stored is None and earlier is not None:
        stored = await earlier()
        if stored is not None:
            async with state_db.acquire() as conn:
                await mail_sync_state.keep_grant(conn, source_id, account, stored)
    if stored is None:
        raise NotConnected(
            f"source {source_id!r}: {account} is not connected; its owner signs in again from "
            "the source's form"
        )
    return stored


async def stored_access_token(
    state_db: "Database",
    platform_url: str,
    org_id: str,
    *,
    source_id: str,
    account: str,
    exchange: Callable[[str], Grant],
    replaces: bool,
    earlier: Callable[[], Awaitable[str | None]] | None = None,
    lock_wait_seconds: float = LOCK_WAIT_SECONDS,
    exchange_seconds: float = EXCHANGE_SECONDS,
) -> str:
    """An access token for ``account`` of ``source_id``, from the approval kept for it in the
    organisation's state store (``state_db``). ``exchange`` sends a refresh token to the issuer
    and returns its answer; it blocks, and is run off the event loop. ``replaces`` says the
    issuer answers every refresh with a new refresh token, which is then kept before the access
    token is handed out.

    ``earlier`` answers the approval of a source connected before approvals were kept here (it
    was named by a reference in the source's own settings), or None when there is none. It is
    asked only when the store holds nothing for the account, and what it answers is kept, so
    it is asked once: nothing already connected stops, and nobody has to sign in again."""
    from provisa.federation import mail_sync_state

    key = (org_id, source_id, account.lower())
    token = _fresh(key)
    if token is not None:
        return token
    async with _Locked(platform_url, org_id, source_id, lock_wait_seconds):
        token = _fresh(key)  # another task of this process refreshed while this one waited
        if token is not None:
            return token
        # Read under the lock: what another process kept while this one waited is what is sent.
        stored = await kept_refresh_token(
            state_db, source_id=source_id, account=account, earlier=earlier
        )
        try:
            grant = await asyncio.wait_for(
                asyncio.to_thread(exchange, stored), timeout=exchange_seconds
            )
        except asyncio.TimeoutError:
            raise RefreshTimedOut(
                f"source {source_id!r}: the issuer did not answer a refresh of its credential "
                f"in {exchange_seconds:g}s"
            ) from None
        if replaces and not grant.refresh_token:
            raise ReplacementMissing(
                f"source {source_id!r}: the issuer answered a refresh without a new refresh "
                "token; the sign-in did not grant offline access"
            )
        if grant.refresh_token and grant.refresh_token != stored:
            async with state_db.acquire() as conn:
                await mail_sync_state.keep_grant(conn, source_id, account, grant.refresh_token)
        if grant.expires_in is not None:  # a token of unstated lifetime is used once, not held
            with _held_guard:
                _held[key] = _Held(grant.access_token, time.monotonic() + grant.expires_in)
        return grant.access_token


async def store_refresh_token(
    state_db: "Database",
    platform_url: str,
    org_id: str,
    *,
    source_id: str,
    account: str,
    refresh_token: str,
    scopes: list[str],
    lock_wait_seconds: float = LOCK_WAIT_SECONDS,
) -> None:
    """Keep the approval a sign-in produced and the scopes it grants (REQ-1994), under the lock
    a refresh takes, and drop the access token this process held under the one it replaces."""
    from provisa.federation import mail_sync_state

    async with _Locked(platform_url, org_id, source_id, lock_wait_seconds):
        async with state_db.acquire() as conn:
            await mail_sync_state.keep_grant(conn, source_id, account, refresh_token)
            await mail_sync_state.keep_granted(conn, source_id, account, scopes)
        forget(org_id, source_id, account)


class ScopesNotGranted(RuntimeError):
    """The owner's approval does not grant everything the source now reads (REQ-1994): the
    source asked for more after its owner signed in."""


async def require_granted(
    state_db: "Database",
    *,
    source_id: str,
    account: str,
    needed: list[str],
    name: Callable[[str], str] = lambda scope: scope,
) -> None:
    """Refuse by name, asking its owner to sign in again, a source whose kept approval does not
    grant every scope of ``needed``. ``name`` puts a scope as the issuer writes it in its
    answer (Microsoft answers ``Mail.Read`` for ``https://graph.microsoft.com/Mail.Read``).

    An approval given before what it grants was recorded has no record: nothing is said of it
    here, and the provider's own refusal of a call it does not cover names what to do (the
    Calendar and Tasks clients' ``ScopeNotGranted``). Such a source keeps reading what it was
    approved for -- the standing rule of approvals kept before (``kept_refresh_token``)."""
    from provisa.federation import mail_sync_state

    async with state_db.acquire() as conn:
        granted = await mail_sync_state.granted(conn, source_id, account)
    if granted is None:
        return
    have = {name(s).lower() for s in granted}
    missing = [s for s in needed if name(s).lower() not in have]
    if missing:
        raise ScopesNotGranted(
            f"source {source_id!r}: {account} signed in before the source asked for "
            f"{', '.join(name(s) for s in missing)}. Its owner signs in again from the "
            "source's form to grant it."
        )
