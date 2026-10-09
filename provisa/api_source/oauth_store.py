# Copyright (c) 2026 Kenneth Stott
# Canary: 5d0e7c3a-2b94-4f61-a8d7-9c1f6e3b4a20
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An access token for a source whose refresh token lives in the org's vault.

Some issuers answer every refresh with a new refresh token (Microsoft does; the one sent keeps
working only until its own lifetime ends). The vault then has to hold the newest one, whichever
process last refreshed. So one refresh of a source runs at a time, across every process and
node, and each reads the token it sends from the vault after it has the lock:

- the lock is a session advisory lock on the platform control plane, or a ``flock`` beside a
  control plane that is a file on one host -- the same two forms a replica build's lock takes
  (``provisa.federation.replica_locks``), released by the holder's death with nothing to renew;
- a process keeps the access token it was given and its expiry, never a refresh token, so a
  refresh can only send what the vault holds;
- the replacement is written to the vault before the access token is held: a write that fails
  fails the read and leaves no token behind.

A sign-in that stores a new refresh token (:func:`store_refresh_token`) takes the same lock.
"""

from __future__ import annotations

import asyncio
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from sqlalchemy import text

from provisa.core import request_deadline, secrets_store
from provisa.core.host_lock import FileLock, control_plane_lock_dir, lock_name

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


_held: dict[tuple[str, str], _Held] = {}
_held_guard = threading.Lock()


def _fresh(key: tuple[str, str]) -> str | None:
    with _held_guard:
        held = _held.get(key)
    if held is None or held.expires_at - _EXPIRY_MARGIN_SECONDS <= time.monotonic():
        return None
    return held.access_token


def forget(org_id: str, source_id: str) -> None:
    """Drop the access token this process holds for the source."""
    with _held_guard:
        _held.pop((org_id, source_id), None)


class _RefreshLock:
    """The lock of one source's refresh. Every attempt is a try; the waiting is the caller's."""

    def __init__(self, platform_url: str, name: str) -> None:
        from sqlalchemy import make_url

        from provisa.core.database import sync_store_url

        self._url = platform_url
        self._name = name
        self._engine: Any = None
        self._conn: Any = None
        self._file: FileLock | None = None
        self._postgres = make_url(sync_store_url(platform_url)).get_backend_name() == "postgresql"

    def try_acquire(self) -> bool:
        if not self._postgres:
            lock = FileLock(
                control_plane_lock_dir(self._url) / lock_name("oauth-refresh", self._name)
            )
            if not lock.try_acquire():
                return False
            self._file = lock
            return True
        if self._conn is None:
            from provisa.core.database import control_plane_lock_engine

            shield = request_deadline.shielded()
            with shield.lock:
                shield.settle()
                self._engine = control_plane_lock_engine(self._url)
                self._conn = self._engine.connect().execution_options(isolation_level="AUTOCOMMIT")
        return bool(
            self._conn.execute(
                text("SELECT pg_try_advisory_lock(:cls, hashtext(:name))"),
                {"cls": _REFRESH_LOCK_CLASS, "name": self._name},
            ).scalar()
        )

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
        if self._file is not None:
            self._file.release()
            self._file = None
        if self._conn is not None:
            self._conn.close()  # ends the session, and with it the lock
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


async def _replace(admin_db: "Database", org_id: str, name: str, value: str, *, actor: str) -> None:
    """Store ``value`` as the org's secret ``name``, keeping the description it has."""
    owner = secrets_store.ORG_OWNER
    known = await secrets_store.describe(admin_db, org_id, name, owner_id=owner)
    await secrets_store.put(
        admin_db,
        org_id,
        name,
        value,
        owner_id=owner,
        description=None if known is None else known.description,
        actor=actor,
    )


async def stored_access_token(
    admin_db: "Database",
    platform_url: str,
    org_id: str,
    *,
    source_id: str,
    secret_name: str,
    exchange: Callable[[str], Grant],
    replaces: bool,
    lock_wait_seconds: float = LOCK_WAIT_SECONDS,
    exchange_seconds: float = EXCHANGE_SECONDS,
) -> str:
    """An access token for ``source_id``, whose refresh token is the org's secret
    ``secret_name``. ``exchange`` sends a refresh token to the issuer and returns its answer; it
    blocks, and is run off the event loop. ``replaces`` says the issuer answers every refresh
    with a new refresh token, which is then stored before the access token is handed out."""
    key = (org_id, source_id)
    token = _fresh(key)
    if token is not None:
        return token
    async with _Locked(platform_url, org_id, source_id, lock_wait_seconds):
        token = _fresh(key)  # another task of this process refreshed while this one waited
        if token is not None:
            return token
        # Read under the lock: what another process stored while this one waited is what is sent.
        stored = await secrets_store.value_of(
            admin_db, org_id, secret_name, owner_id=secrets_store.ORG_OWNER
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
            await _replace(
                admin_db, org_id, secret_name, grant.refresh_token, actor=f"source:{source_id}"
            )
        if grant.expires_in is not None:  # a token of unstated lifetime is used once, not held
            with _held_guard:
                _held[key] = _Held(grant.access_token, time.monotonic() + grant.expires_in)
        return grant.access_token


async def store_refresh_token(
    admin_db: "Database",
    platform_url: str,
    org_id: str,
    *,
    source_id: str,
    secret_name: str,
    refresh_token: str,
    actor: str,
    lock_wait_seconds: float = LOCK_WAIT_SECONDS,
) -> None:
    """Store the refresh token a sign-in produced, under the lock a refresh takes, and drop the
    access token this process held under the one it replaces."""
    async with _Locked(platform_url, org_id, source_id, lock_wait_seconds):
        await _replace(admin_db, org_id, secret_name, refresh_token, actor=actor)
        forget(org_id, source_id)
