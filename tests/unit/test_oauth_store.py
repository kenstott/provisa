# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""The refresh of a credential whose issuer replaces the refresh token on every use: one refresh
of a source at a time, the token sent is the one the vault holds, and the replacement is stored
before the access token is handed out."""

from __future__ import annotations

import asyncio
import logging
import sys
import threading
import time

import pytest

from provisa.api_source import oauth_store
from provisa.api_source.oauth_store import (
    Grant,
    RefreshBusy,
    RefreshTimedOut,
    ReplacementMissing,
    stored_access_token,
    store_refresh_token,
)
from provisa.core import secrets_store
from provisa.core.secrets_store import ORG_OWNER
from provisa.encryption.runtime import reset_encryption

ORG = "acme"
SOURCE = "m365"
NAME = "M365_REFRESH"


@pytest.fixture(autouse=True)
def _isolated(tmp_path, monkeypatch):
    monkeypatch.setenv("PROVISA_DATA_DIR", str(tmp_path))
    monkeypatch.delenv("PROVISA_ENCRYPTION_KEY", raising=False)
    monkeypatch.setitem(sys.modules, "keyring", None)
    reset_encryption()
    oauth_store._held.clear()
    yield
    oauth_store._held.clear()
    reset_encryption()


@pytest.fixture
def plane(tmp_path):
    """A platform control plane (SQLite) holding the vault, and its URL (where its locks go)."""
    from provisa.core.database import Database, create_engine_from_url
    from provisa.core.schema_admin import deployment_encryption_key, metadata
    from provisa.core.schema_admin import secrets_store as table

    url = f"sqlite+pysqlite:///{tmp_path / 'platform.db'}"
    engine = create_engine_from_url(url)
    with engine.begin() as conn:
        metadata.create_all(conn, tables=[table, deployment_encryption_key])
    db = Database(engine, name="admin")
    yield db, url
    engine.dispose()


async def _seed(db, value: str = "rt-0") -> None:
    await secrets_store.put(db, ORG, NAME, value, owner_id=ORG_OWNER, description="sign-in")


async def _stored(db) -> str:
    return await secrets_store.value_of(db, ORG, NAME, owner_id=ORG_OWNER)


def _token(db, url, exchange, **kw):
    return stored_access_token(
        db, url, ORG, source_id=SOURCE, secret_name=NAME, exchange=exchange, replaces=True, **kw
    )


class Issuer:
    """Answers each refresh with the next refresh token and an access token naming the one it
    was sent. Counts the exchanges running at once."""

    def __init__(self, *, expires_in: float = 3600.0, delay: float = 0.0) -> None:
        self.sent: list[str] = []
        self.expires_in = expires_in
        self.delay = delay
        self.running = 0
        self.most_at_once = 0
        self._guard = threading.Lock()

    def __call__(self, refresh_token: str) -> Grant:
        with self._guard:
            self.running += 1
            self.most_at_once = max(self.most_at_once, self.running)
            self.sent.append(refresh_token)
            n = len(self.sent)
        time.sleep(self.delay)
        with self._guard:
            self.running -= 1
        return Grant(f"at-for-{refresh_token}", self.expires_in, f"rt-{n}")


def test_a_refresh_stores_the_replacement_and_keeps_the_description(plane):
    db, url = plane

    async def run():
        await _seed(db)
        issuer = Issuer()
        assert await _token(db, url, issuer) == "at-for-rt-0"
        assert await _stored(db) == "rt-1"
        found = await secrets_store.describe(db, ORG, NAME, owner_id=ORG_OWNER)
        assert found.description == "sign-in"
        assert found.updated_by == f"source:{SOURCE}"
        # A second read is served from what the process holds: the issuer is not asked again.
        assert await _token(db, url, issuer) == "at-for-rt-0"
        assert issuer.sent == ["rt-0"]

    asyncio.run(run())


def test_concurrent_reads_make_one_exchange(plane):
    db, url = plane

    async def run():
        await _seed(db)
        issuer = Issuer(delay=0.2)
        tokens = await asyncio.gather(*[_token(db, url, issuer) for _ in range(5)])
        assert set(tokens) == {"at-for-rt-0"}
        assert issuer.sent == ["rt-0"]
        assert await _stored(db) == "rt-1"

    asyncio.run(run())


def test_refreshes_never_overlap_and_each_sends_what_the_last_one_stored(plane):
    """Tokens that expire at once force every read to refresh. However they interleave, one
    exchange runs at a time, each sends the token the one before it stored, and the vault ends
    holding the last one issued: a newer token is never overwritten by an older."""
    db, url = plane

    async def run():
        await _seed(db)
        issuer = Issuer(expires_in=0.0, delay=0.05)
        await asyncio.gather(*[_token(db, url, issuer) for _ in range(4)])
        assert issuer.most_at_once == 1
        assert issuer.sent == ["rt-0", "rt-1", "rt-2", "rt-3"]
        assert await _stored(db) == "rt-4"

    asyncio.run(run())


def test_a_process_holding_no_access_token_sends_the_token_another_process_stored(plane):
    """Another process refreshed: the vault holds its replacement, and this process holds no
    access token of its own. It refreshes with what the vault holds now."""
    db, url = plane

    async def run():
        await _seed(db)
        issuer = Issuer()
        await _token(db, url, issuer)  # this process: rt-0 sent, rt-1 stored
        await secrets_store.put(db, ORG, NAME, "rt-from-elsewhere", owner_id=ORG_OWNER)
        oauth_store.forget(ORG, SOURCE)  # as a second process: nothing held in memory
        assert await _token(db, url, issuer) == "at-for-rt-from-elsewhere"
        assert issuer.sent == ["rt-0", "rt-from-elsewhere"]

    asyncio.run(run())


def test_a_failed_vault_write_fails_the_read_and_holds_no_access_token(plane, monkeypatch):
    db, url = plane

    async def run():
        await _seed(db)
        issuer = Issuer()

        async def refuse(*a, **kw):
            raise RuntimeError("the vault cannot be written")

        with monkeypatch.context() as patched:
            patched.setattr(secrets_store, "put", refuse)
            with pytest.raises(RuntimeError, match="cannot be written"):
                await _token(db, url, issuer)
        assert oauth_store._held == {}
        assert await _stored(db) == "rt-0"
        # The stored token still works: the next read refreshes with it.
        assert await _token(db, url, issuer) == "at-for-rt-0"
        assert issuer.sent == ["rt-0", "rt-0"]

    asyncio.run(run())


def test_an_answer_without_a_replacement_is_refused_by_name(plane):
    db, url = plane

    async def run():
        await _seed(db)
        with pytest.raises(ReplacementMissing, match="offline access"):
            await _token(db, url, lambda rt: Grant("at", 3600.0, None))
        assert oauth_store._held == {}
        assert await _stored(db) == "rt-0"

    asyncio.run(run())


def test_an_issuer_that_does_not_replace_writes_nothing(plane):
    db, url = plane

    async def run():
        await _seed(db)
        token = await stored_access_token(
            db,
            url,
            ORG,
            source_id=SOURCE,
            secret_name=NAME,
            exchange=lambda rt: Grant("at", 3600.0, None),
            replaces=False,
        )
        assert token == "at"
        found = await secrets_store.describe(db, ORG, NAME, owner_id=ORG_OWNER)
        assert found.updated_by is None

    asyncio.run(run())


def test_a_token_of_unstated_lifetime_is_not_held(plane):
    db, url = plane

    async def run():
        await _seed(db)
        asked: list[str] = []

        def exchange(refresh_token: str) -> Grant:
            asked.append(refresh_token)
            return Grant("at", None, None)

        for _ in range(2):
            assert (
                await stored_access_token(
                    db,
                    url,
                    ORG,
                    source_id=SOURCE,
                    secret_name=NAME,
                    exchange=exchange,
                    replaces=False,
                )
                == "at"
            )
        assert asked == ["rt-0", "rt-0"]
        assert oauth_store._held == {}

    asyncio.run(run())


def test_a_refresh_that_cannot_take_the_lock_in_time_fails_by_name(plane):
    db, url = plane

    async def run():
        await _seed(db)
        holder = oauth_store._RefreshLock(url, f"{ORG}|{SOURCE}")
        assert holder.try_acquire()
        issuer = Issuer()
        try:
            with pytest.raises(RefreshBusy, match="another refresh"):
                await _token(db, url, issuer, lock_wait_seconds=0.3)
        finally:
            holder.close()
        assert issuer.sent == []  # it did not go on without the lock
        assert await _token(db, url, issuer) == "at-for-rt-0"

    asyncio.run(run())


def test_an_issuer_that_does_not_answer_in_time_fails_by_name_and_frees_the_lock(plane):
    db, url = plane

    async def run():
        await _seed(db)
        with pytest.raises(RefreshTimedOut, match="did not answer"):
            await _token(db, url, Issuer(delay=0.5), exchange_seconds=0.1)
        assert oauth_store._held == {}
        await asyncio.sleep(0.6)  # the abandoned exchange ends
        assert await _token(db, url, Issuer()) == "at-for-rt-0"

    asyncio.run(run())


def test_a_sign_in_takes_the_refresh_lock_and_drops_the_held_access_token(plane):
    db, url = plane

    async def run():
        await _seed(db)
        issuer = Issuer()
        await _token(db, url, issuer)
        holder = oauth_store._RefreshLock(url, f"{ORG}|{SOURCE}")
        assert holder.try_acquire()
        try:
            with pytest.raises(RefreshBusy):
                await store_refresh_token(
                    db,
                    url,
                    ORG,
                    source_id=SOURCE,
                    secret_name=NAME,
                    refresh_token="rt-signed-in",
                    actor="steward",
                    lock_wait_seconds=0.2,
                )
        finally:
            holder.close()
        await store_refresh_token(
            db,
            url,
            ORG,
            source_id=SOURCE,
            secret_name=NAME,
            refresh_token="rt-signed-in",
            actor="steward",
        )
        assert await _stored(db) == "rt-signed-in"
        assert await _token(db, url, issuer) == "at-for-rt-signed-in"

    asyncio.run(run())


def test_no_token_reaches_a_log_record_or_an_error(plane, caplog):
    db, url = plane
    secrets = ("rt-0", "rt-1", "at-for-rt-0", "rt-signed-in")

    async def run() -> list[str]:
        await _seed(db)
        said: list[str] = [repr(Grant("at-for-rt-0", 1.0, "rt-1"))]
        await _token(db, url, Issuer())
        oauth_store.forget(ORG, SOURCE)
        for exchange, kw in (
            (lambda rt: Grant("at-for-rt-0", 3600.0, None), {}),
            (Issuer(delay=0.3), {"exchange_seconds": 0.05}),
        ):
            with pytest.raises(RuntimeError) as caught:
                await _token(db, url, exchange, **kw)
            said.append(str(caught.value))
        holder = oauth_store._RefreshLock(url, f"{ORG}|{SOURCE}")
        assert holder.try_acquire()
        try:
            with pytest.raises(RefreshBusy) as caught:
                await _token(db, url, Issuer(), lock_wait_seconds=0.1)
            said.append(str(caught.value))
        finally:
            holder.close()
        return said

    with caplog.at_level(logging.DEBUG):
        said = asyncio.run(run())
    said += [record.getMessage() for record in caplog.records]
    said += [str(record.args) for record in caplog.records]
    for text in said:
        for secret in secrets:
            assert secret not in text


def test_on_postgresql_every_lock_holds_a_session_of_its_own(monkeypatch):
    """A session advisory lock is re-entered by its own session, so two refreshes that shared a
    connection would not exclude each other. Each lock opens its own unpooled engine and
    connection (``control_plane_lock_engine``) and closes both when it is released."""
    from provisa.core import database

    opened: list = []

    class Conn:
        def __init__(self) -> None:
            self.closed = False

        def execution_options(self, **options):
            assert options == {"isolation_level": "AUTOCOMMIT"}
            return self

        def execute(self, statement, params):
            assert "pg_try_advisory_lock" in str(statement)
            assert params == {"cls": oauth_store._REFRESH_LOCK_CLASS, "name": "acme|m365"}

            class Answer:
                @staticmethod
                def scalar():
                    return True

            return Answer()

        def close(self) -> None:
            self.closed = True

    class Engine:
        def __init__(self) -> None:
            self.conn = Conn()
            self.disposed = False

        def connect(self):
            return self.conn

        def dispose(self) -> None:
            self.disposed = True

    def lock_engine(url):
        opened.append(Engine())
        return opened[-1]

    monkeypatch.setattr(database, "control_plane_lock_engine", lock_engine)
    url = "postgresql+psycopg://u:p@db:5432/platform"
    first = oauth_store._RefreshLock(url, "acme|m365")
    second = oauth_store._RefreshLock(url, "acme|m365")
    assert first.try_acquire() and second.try_acquire()
    assert len(opened) == 2 and opened[0].conn is not opened[1].conn
    first.close()
    assert opened[0].conn.closed and opened[0].disposed
    assert not opened[1].conn.closed
    second.close()
