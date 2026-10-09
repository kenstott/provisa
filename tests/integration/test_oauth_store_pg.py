# Copyright (c) 2026 Kenneth Stott
# Canary: c8e62f55-7154-4804-8c57-e2f09c81f045
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""The refresh of a replaced-on-use credential on a PostgreSQL control plane: the source's
refresh lock is a session advisory lock on a connection of the holder's own, so two refreshes
exclude each other within one process as well as between processes."""

from __future__ import annotations

import asyncio
import os
import sys
import threading
import time
import uuid

import pytest
import sqlalchemy as sa

from provisa.api_source import oauth_store
from provisa.api_source.oauth_store import Grant, RefreshBusy, stored_access_token
from provisa.core import secrets_store
from provisa.core.database import Database, create_engine_from_url
from provisa.core.schema_admin import REGISTRY_TABLES, metadata, orgs
from provisa.core.secrets_store import ORG_OWNER
from provisa.encryption.runtime import reset_encryption

pytestmark = [pytest.mark.integration]

_PG_HOST = os.environ.get("PG_HOST", "localhost")
_PG_PORT = int(os.environ.get("PG_PORT", "5432"))
_PG_USER = os.environ.get("PG_USER", "provisa")
_PG_PASSWORD = os.environ.get("PG_PASSWORD", "provisa")
_BASE = f"postgresql+psycopg://{_PG_USER}:{_PG_PASSWORD}@{_PG_HOST}:{_PG_PORT}"
_ADMIN_URL = f"{_BASE}/{os.environ.get('PG_DATABASE', 'provisa')}"
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
def plane():
    """A fresh control-plane database holding the vault, and its URL."""
    name = f"oauth_store_{uuid.uuid4().hex[:10]}"
    admin = sa.create_engine(_ADMIN_URL, isolation_level="AUTOCOMMIT")
    with admin.connect() as conn:
        conn.execute(sa.text(f'CREATE DATABASE "{name}"'))
    url = f"{_BASE}/{name}"
    engine = create_engine_from_url(url)
    with engine.begin() as conn:
        # The control plane's own tables, as the org tests build them: the vault refers to the
        # organisation that holds each secret.
        metadata.create_all(conn, tables=REGISTRY_TABLES)
        conn.execute(sa.insert(orgs).values(id=ORG, name=ORG))
    try:
        yield Database(engine, "test"), url
    finally:
        engine.dispose()
        with admin.connect() as conn:
            conn.execute(sa.text(f'DROP DATABASE "{name}" WITH (FORCE)'))
        admin.dispose()


class Issuer:
    def __init__(self, delay: float) -> None:
        self.sent: list[str] = []
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
        return Grant(f"at-for-{refresh_token}", 0.0, f"rt-{n}")  # lapsed at once: always refresh


def test_two_locks_of_one_process_exclude_each_other(plane):
    _db, url = plane
    first = oauth_store._RefreshLock(url, f"{ORG}|{SOURCE}")
    second = oauth_store._RefreshLock(url, f"{ORG}|{SOURCE}")
    other = oauth_store._RefreshLock(url, f"{ORG}|another")
    try:
        assert first.try_acquire()
        assert not second.try_acquire()  # a session of its own: the lock is not re-entered
        assert other.try_acquire()  # another source's refresh is not held up
        first.close()
        assert second.try_acquire()
    finally:
        for lock in (first, second, other):
            lock.close()


def test_a_holder_that_ends_frees_the_lock(plane):
    _db, url = plane
    holder = oauth_store._RefreshLock(url, f"{ORG}|{SOURCE}")
    assert holder.try_acquire()
    holder._conn.close()  # the holder's session ends without an unlock
    waiter = oauth_store._RefreshLock(url, f"{ORG}|{SOURCE}")
    try:
        assert waiter.acquire(5.0)
    finally:
        waiter.close()
        holder.close()


def test_concurrent_refreshes_in_one_process_run_one_at_a_time(plane):
    db, url = plane

    async def run():
        await secrets_store.put(db, ORG, NAME, "rt-0", owner_id=ORG_OWNER)
        issuer = Issuer(delay=0.1)
        await asyncio.gather(
            *[
                stored_access_token(
                    db,
                    url,
                    ORG,
                    source_id=SOURCE,
                    secret_name=NAME,
                    exchange=issuer,
                    replaces=True,
                )
                for _ in range(4)
            ]
        )
        assert issuer.most_at_once == 1
        assert issuer.sent == ["rt-0", "rt-1", "rt-2", "rt-3"]
        assert await secrets_store.value_of(db, ORG, NAME, owner_id=ORG_OWNER) == "rt-4"

    asyncio.run(run())


def test_a_refresh_that_cannot_take_the_lock_fails_by_name(plane):
    db, url = plane

    async def run():
        await secrets_store.put(db, ORG, NAME, "rt-0", owner_id=ORG_OWNER)
        holder = oauth_store._RefreshLock(url, f"{ORG}|{SOURCE}")
        assert holder.try_acquire()
        issuer = Issuer(delay=0.0)
        try:
            with pytest.raises(RefreshBusy):
                await stored_access_token(
                    db,
                    url,
                    ORG,
                    source_id=SOURCE,
                    secret_name=NAME,
                    exchange=issuer,
                    replaces=True,
                    lock_wait_seconds=0.5,
                )
        finally:
            holder.close()
        assert issuer.sent == []

    asyncio.run(run())
