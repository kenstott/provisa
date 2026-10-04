# Copyright (c) 2026 Kenneth Stott
# Canary: 98076df8-7be7-4af8-b0ac-8cd3ca1b52dc
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The catalog-registration lock holds through a transaction-pooling PgBouncer.

``one_registrar`` serializes the processes of one deployment around registering the shared
coordinator's system catalogs. It took a session advisory lock; through a transaction-pooling
PgBouncer (the Helm chart's default) a session belongs to no one client, so the lock was held by
whichever server connection took it and the unlock ran on whichever one came next. It now takes
a transaction-scoped lock in a transaction that spans the block. These tests reach the control
plane through the stack's PgBouncer (POOL_MODE transaction), as a pooled deployment does.
"""

# Requirements: REQ-1429

from __future__ import annotations

import os
import threading
import time

import psycopg2
import psycopg2.errors
import pytest
from sqlalchemy.engine import URL

from provisa.core.trino_system_catalogs import _CATALOG_LOCK_KEY, one_registrar

pytestmark = [pytest.mark.integration]


def _url(port_env: str) -> URL:
    return URL.create(
        "postgresql",
        username=os.environ.get("PG_USER", "provisa"),
        password=os.environ.get("PG_PASSWORD", "provisa"),
        host="localhost",
        port=int(os.environ[port_env]),
        database=os.environ.get("PG_DATABASE", "provisa"),
    )


def _held_on_the_server() -> int:
    """Advisory locks with the registration key that Postgres itself (not PgBouncer) holds."""
    url = _url("PG_PORT")
    with psycopg2.connect(
        host=url.host, port=url.port, dbname=url.database, user=url.username, password=url.password
    ) as pg:
        with pg.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND objid = %s",
                (_CATALOG_LOCK_KEY,),
            )
            row = cur.fetchone()
            assert row is not None
            return row[0]


def test_two_registrars_through_pgbouncer_take_turns():
    pooled = _url("PGBOUNCER_PORT")
    entered = threading.Event()
    release = threading.Event()

    def _first() -> None:
        with one_registrar(pooled, 30):
            entered.set()
            release.wait(30)

    first = threading.Thread(target=_first)
    first.start()
    try:
        assert entered.wait(30)
        assert _held_on_the_server() == 1
        # While the first holds it, the second cannot have it: it waits, then times out.
        began = time.monotonic()
        with pytest.raises(psycopg2.errors.LockNotAvailable):
            with one_registrar(pooled, 2):
                pass
        assert time.monotonic() - began >= 1.5
    finally:
        release.set()
        first.join(30)
    # The first's transaction ended, and the lock with it: the second gets it at once.
    began = time.monotonic()
    with one_registrar(pooled, 5):
        assert time.monotonic() - began < 2
    assert _held_on_the_server() == 0


def test_the_lock_is_released_when_the_block_fails():
    pooled = _url("PGBOUNCER_PORT")
    with pytest.raises(RuntimeError, match="registration failed"):
        with one_registrar(pooled, 5):
            raise RuntimeError("registration failed")
    assert _held_on_the_server() == 0
    with one_registrar(pooled, 2):
        pass
