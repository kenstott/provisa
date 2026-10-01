# Copyright (c) 2026 Kenneth Stott
# Canary: 3d6a9e52-8f1c-4b07-a4e9-0c2b7d5f1a83
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration: the Postgres engine runtime lands on the thread that asked (REQ-1882).

A real ``PgFederationRuntime`` over the test stack's Postgres. A request lands a table the way
``ensure_resident`` does — ``land_table`` on the request thread's connection loop. The rows must
arrive, no work may be handed to a pool thread, and two requests landing different tables at once
must both succeed on the runtime's one connection.
"""

# Requirements: REQ-1882

from __future__ import annotations

import concurrent.futures
import os
import threading
import uuid

import pytest

from provisa.core.connection_loop import connection_loop

pytestmark = [pytest.mark.integration]

psycopg2 = pytest.importorskip("psycopg2")

_COLUMNS = [("id", "integer"), ("name", "varchar")]


def _dsn() -> str:
    return (
        f"postgresql://{os.environ['PG_USER']}:{os.environ['PG_PASSWORD']}"
        f"@{os.environ['PG_HOST']}:{os.environ['PG_PORT']}/{os.environ['PG_DATABASE']}"
    )


@pytest.fixture()
def runtime():
    from provisa.federation.pg_runtime import PgFederationRuntime

    rt = PgFederationRuntime(engine_dsn=_dsn())
    schema = f"land_thread_{uuid.uuid4().hex[:8]}"
    con = psycopg2.connect(_dsn())
    con.autocommit = True
    try:
        con.cursor().execute(f'CREATE SCHEMA "{schema}"')  # the store schema a land writes into
    finally:
        con.close()
    try:
        yield rt, schema
    finally:
        con = psycopg2.connect(_dsn())
        con.autocommit = True
        try:
            con.cursor().execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        finally:
            con.close()
        rt.close()


def _count(schema: str, table: str) -> int:
    con = psycopg2.connect(_dsn())
    try:
        cur = con.cursor()
        cur.execute(f'SELECT count(*) FROM "{schema}"."{table}"')
        return cur.fetchone()[0]
    finally:
        con.close()


class _PoolSubmissions:
    """Records every hand-off to a thread pool made while it is active."""

    def __init__(self, monkeypatch) -> None:
        self.from_threads: list[int] = []
        real = concurrent.futures.ThreadPoolExecutor.submit
        seen = self.from_threads

        def submit(pool, fn, /, *args, **kwargs):
            seen.append(threading.get_ident())
            return real(pool, fn, *args, **kwargs)

        monkeypatch.setattr(concurrent.futures.ThreadPoolExecutor, "submit", submit)


def test_a_land_runs_on_the_request_thread_and_the_rows_arrive(runtime, monkeypatch):
    rt, schema = runtime
    rows = [{"id": i, "name": f"n{i}"} for i in range(500)]
    submissions = _PoolSubmissions(monkeypatch)

    with connection_loop() as cl:
        landed = cl.run(rt.land_table(schema=schema, table="orders", columns=_COLUMNS, rows=rows))

    assert landed == f"{schema}.orders"
    assert _count(schema, "orders") == 500
    assert threading.get_ident() not in submissions.from_threads, (
        "the land was handed to a pool thread instead of running on the request's thread"
    )


def test_two_requests_landing_different_tables_both_land(runtime, monkeypatch):
    rt, schema = runtime
    submissions = _PoolSubmissions(monkeypatch)
    start = threading.Barrier(2)
    request_threads: list[int] = []
    errors: list[BaseException] = []

    def _request(table: str) -> None:
        try:
            request_threads.append(threading.get_ident())
            start.wait(timeout=10)
            rows = [{"id": i, "name": table} for i in range(2_000)]
            with connection_loop() as cl:
                cl.run(rt.land_table(schema=schema, table=table, columns=_COLUMNS, rows=rows))
        except BaseException as exc:  # reported by the assertion below with the real cause
            errors.append(exc)

    threads = [threading.Thread(target=_request, args=(t,)) for t in ("a", "b")]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=60)

    assert not errors, errors
    assert _count(schema, "a") == 2_000
    assert _count(schema, "b") == 2_000
    assert not set(request_threads) & set(submissions.from_threads), (
        "a land was handed to a pool thread instead of running on its request's thread"
    )
