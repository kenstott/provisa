# Copyright (c) 2026 Kenneth Stott
# Canary: 9d2e7b14-6a38-4f51-8c09-e1f4a6b3d725
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Two creators of the same store schema at once: the second finds the schema the first created.

``CREATE SCHEMA IF NOT EXISTS`` loses to a concurrent creator: in run 43359313187 the admin
refresh of a view landed its staged rows (``PgFederationRuntime.land_table``) while the boot's
event-loop wiring reconciled the same view's table (``store_writer.reconcile_table``), both
creating ``org_aggregate_mv_mv_cache``, and the refresh failed with ``duplicate key value violates
unique constraint "pg_namespace_nspname_index"``.

Each test holds one creator in the middle of its create (its transaction open, the schema not yet
committed), starts a real writer of the same schema beside it, waits until that writer is blocked
on the first, and lets the first commit. The writer must then find the schema and go on."""

from __future__ import annotations

import asyncio
import threading
import time
import uuid
from collections.abc import Callable

import psycopg2
import pytest
import sqlalchemy as sa

from provisa.core.schema_create import create_schema_sync
from provisa.federation import store_writer
from provisa.federation.pg_runtime import PgFederationRuntime

pytestmark = pytest.mark.unit

_COLUMNS = [("id", "integer"), ("amount", "double")]


def _schema() -> str:
    return f"org_t{uuid.uuid4().hex[:10]}_mv_cache"


def _blocked_on_a_lock(dsn: str, timeout: float = 15.0) -> None:
    """Wait until some session of this database is waiting on a lock: the writer has reached
    its create and stands behind the held creator."""
    con = psycopg2.connect(dsn)
    con.autocommit = True
    try:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            cur = con.cursor()
            cur.execute(
                "SELECT count(*) FROM pg_stat_activity "
                "WHERE wait_event_type = 'Lock' AND pid <> pg_backend_pid()"
            )
            row = cur.fetchone()
            assert row is not None
            if row[0] > 0:
                return
            time.sleep(0.05)
        raise AssertionError("the writer never reached the schema's create")
    finally:
        con.close()


def _beside_a_held_creator(pg, schema: str, write: Callable[[], object]) -> None:
    """Run ``write`` while another session is midway through creating ``schema`` through
    :func:`create_schema_sync`; raise whatever ``write`` raised."""
    engine = sa.create_engine(pg.url())
    errors: list[BaseException] = []

    def _run() -> None:
        try:
            write()
        except BaseException as exc:  # handed to the test thread, which raises it
            errors.append(exc)

    try:
        with engine.connect() as held:
            tx = held.begin()
            create_schema_sync(held, schema)
            writer = threading.Thread(target=_run)
            writer.start()
            _blocked_on_a_lock(pg.dsn())
            tx.commit()
        writer.join(timeout=30)
        assert not writer.is_alive(), "the writer is still blocked after the creator committed"
    finally:
        engine.dispose()
    if errors:
        raise errors[0]


def _tables(pg, schema: str) -> set[str]:
    con = psycopg2.connect(pg.dsn())
    try:
        cur = con.cursor()
        cur.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = %s", (schema,)
        )
        return {row[0] for row in cur.fetchall()}
    finally:
        con.close()


def test_the_engine_store_land_waits_for_a_concurrent_creator_of_its_schema(embedded_postgres):
    schema = _schema()
    runtime = PgFederationRuntime(engine_dsn=embedded_postgres.dsn())
    try:
        _beside_a_held_creator(
            embedded_postgres,
            schema,
            lambda: asyncio.run(
                runtime.land_table(
                    schema=schema,
                    table="mv_x__direct",
                    columns=_COLUMNS,
                    rows=[{"id": 1, "amount": 2.0}],
                )
            ),
        )
    finally:
        runtime.close()
    assert _tables(embedded_postgres, schema) == {"mv_x__direct"}


def test_the_store_writer_reconcile_waits_for_a_concurrent_creator_of_its_schema(
    embedded_postgres,
):
    schema = _schema()
    _beside_a_held_creator(
        embedded_postgres,
        schema,
        lambda: asyncio.run(
            store_writer.reconcile_table(
                embedded_postgres.url(), schema=schema, table="mv_x", columns=_COLUMNS
            )
        ),
    )
    assert _tables(embedded_postgres, schema) == {"mv_x"}


def test_the_engine_store_replica_reconcile_waits_for_a_concurrent_creator_of_its_schema(
    embedded_postgres,
):
    schema = f"org_t{uuid.uuid4().hex[:10]}_replicas"
    runtime = PgFederationRuntime(engine_dsn=embedded_postgres.dsn())
    try:
        _beside_a_held_creator(
            embedded_postgres,
            schema,
            lambda: asyncio.run(
                runtime.reconcile_replica(schema=schema, table="orders", columns=_COLUMNS)
            ),
        )
    finally:
        runtime.close()
    assert _tables(embedded_postgres, schema) == {"orders"}
