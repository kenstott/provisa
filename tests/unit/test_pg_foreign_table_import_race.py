# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Two processes ensuring one foreign table converge on it (REQ-1915).

A table's foreign table is imported where a live attach or a replica build first needs it, by
whichever process gets there (``PgFederationRuntime._ensure_foreign_table``). Two servers sharing
an engine both found it absent and both imported it; the second failed ``type "orders" already
exists`` and its build waited out its retry. The check and the import are one step across
processes: the second waits for the first, then finds the table there."""

# Requirements: REQ-1915

from __future__ import annotations

import threading
import uuid

import psycopg2
import pytest

from provisa.federation.pg_runtime import PgFederationRuntime
from tests.embedded_pg import PASSWORD, ROLE

pytestmark = pytest.mark.unit


@pytest.fixture
def engine_and_source(embedded_postgres):
    suffix = uuid.uuid4().hex[:8]
    source_db, engine_db = f"src_{suffix}", f"eng_{suffix}"
    admin = psycopg2.connect(embedded_postgres.dsn())
    admin.autocommit = True
    with admin.cursor() as cur:
        cur.execute(f'CREATE DATABASE "{source_db}"')
        cur.execute(f'CREATE DATABASE "{engine_db}"')
    src = psycopg2.connect(embedded_postgres.dsn(source_db))
    src.autocommit = True
    with src.cursor() as cur:
        cur.execute("CREATE TABLE public.orders (id integer)")
    src.close()
    details = {
        "server_ddl_for_copy": [
            "CREATE EXTENSION IF NOT EXISTS postgres_fdw",
            f'CREATE SERVER IF NOT EXISTS "src" FOREIGN DATA WRAPPER postgres_fdw '
            f"OPTIONS (host '127.0.0.1', port '{embedded_postgres.port}', dbname '{source_db}')",
            f'CREATE USER MAPPING IF NOT EXISTS FOR CURRENT_USER SERVER "src" '
            f"OPTIONS (user '{ROLE}', password '{PASSWORD}')",
            'CREATE SCHEMA IF NOT EXISTS "fdw_src"',
        ],
        "local_schema": "fdw_src",
        "remote_schema": "public",
        "server": "src",
    }
    # The engine's server and schema exist before the race, as after a source is attached once.
    eng = psycopg2.connect(embedded_postgres.dsn(engine_db))
    eng.autocommit = True
    with eng.cursor() as cur:
        for ddl in details["server_ddl_for_copy"]:
            cur.execute(ddl)
    eng.close()
    yield embedded_postgres.dsn(engine_db), details
    with admin.cursor() as cur:
        cur.execute(f'DROP DATABASE "{engine_db}" WITH (FORCE)')
        cur.execute(f'DROP DATABASE "{source_db}" WITH (FORCE)')
    admin.close()


class _PausesAfterItsCheck:
    """A cursor that, once it has asked whether the foreign table exists, waits until the other
    process has asked too (or a moment has passed): both find it absent unless one waits for the
    other's import."""

    def __init__(self, cur, both_checked: threading.Barrier) -> None:
        self._cur = cur
        self._both_checked = both_checked

    def execute(self, sql, params=None):
        self._cur.execute(sql, params)
        if "to_regclass" in sql:
            try:
                self._both_checked.wait(timeout=2)
            except threading.BrokenBarrierError:
                pass  # the other is waiting for this one's import: go on alone

    def __getattr__(self, name):
        return getattr(self._cur, name)


def test_two_processes_ensuring_one_foreign_table_both_converge_on_it(engine_and_source):
    dsn, details = engine_and_source
    both_checked = threading.Barrier(2)
    failures: list[BaseException] = []

    def _ensure() -> None:
        con = psycopg2.connect(dsn)
        con.autocommit = True
        try:
            with con.cursor() as cur:
                PgFederationRuntime._ensure_foreign_table(
                    _PausesAfterItsCheck(cur, both_checked), details, "orders"
                )
        except BaseException as exc:  # noqa: BLE001 -- the outcome is what this test reports
            failures.append(exc)
        finally:
            con.close()

    threads = [threading.Thread(target=_ensure) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)
    assert failures == []
    con = psycopg2.connect(dsn)
    try:
        with con.cursor() as cur:
            cur.execute('SELECT to_regclass(\'"fdw_src"."orders"\') IS NOT NULL')
            assert cur.fetchone()[0]
    finally:
        con.close()
