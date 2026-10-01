# Copyright (c) 2026 Kenneth Stott
# Canary: 2e7c4b91-6a3f-4d58-9c0e-5b1d8f3a7e62
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""N worker processes boot against one fresh platform control plane at the same time (REQ-1900).

``uvicorn --workers N`` runs the whole startup sequence in every worker. On an empty PostgreSQL
control plane each worker's ``CREATE TABLE`` raced the others in the catalog and all but the first
died with ``duplicate key value violates unique constraint "pg_type_typname_nsp_index"``."""

# Requirements: REQ-1900

from __future__ import annotations

import asyncio
import os
import threading
import uuid

import pytest
import sqlalchemy as sa

from provisa.core.database import Database, create_engine_from_url
from provisa.core.schema_admin import init_registry_schema, orgs

pytestmark = [pytest.mark.integration]

_BASE = (
    f"postgresql+psycopg://{os.environ.get('PG_USER', 'provisa')}"
    f":{os.environ.get('PG_PASSWORD', 'provisa')}"
    f"@{os.environ.get('PG_HOST', 'localhost')}:{os.environ.get('PG_PORT', '5432')}"
)
_ADMIN_URL = f"{_BASE}/{os.environ.get('PG_DATABASE', 'provisa')}"
_WORKERS = 12


@pytest.fixture
def fresh_database():
    name = f"boot_race_{uuid.uuid4().hex[:10]}"
    admin = sa.create_engine(_ADMIN_URL, isolation_level="AUTOCOMMIT")
    with admin.connect() as conn:
        conn.execute(sa.text(f'CREATE DATABASE "{name}"'))
    try:
        yield f"{_BASE}/{name}"
    finally:
        with admin.connect() as conn:
            conn.execute(sa.text(f'DROP DATABASE "{name}" WITH (FORCE)'))
        admin.dispose()


def test_workers_booting_together_all_initialise_the_registry(fresh_database):
    start = threading.Barrier(_WORKERS)
    failures: list[BaseException] = []

    def _boot() -> None:
        db = Database(
            create_engine_from_url(fresh_database, pool_size=1, max_overflow=0), name="platform"
        )
        try:
            start.wait(timeout=30)
            asyncio.run(init_registry_schema(db, "default"))
        except BaseException as exc:  # collected and asserted on below
            failures.append(exc)
        finally:
            db.engine.dispose()

    threads = [threading.Thread(target=_boot) for _ in range(_WORKERS)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=120)

    assert not failures, f"{len(failures)} of {_WORKERS} boots failed: {failures[0]!r}"
    check = sa.create_engine(fresh_database)
    try:
        with check.connect() as conn:
            assert conn.execute(sa.select(sa.func.count()).select_from(orgs)).scalar() == 1
    finally:
        check.dispose()


def test_the_boot_lock_admits_one_process_at_a_time(fresh_database):
    """The whole boot sequence — DDL, seeds, config apply — runs under one lock on the platform
    control plane, so two workers never write it together."""
    import time

    from provisa.core.boot_lock import control_plane_boot_lock

    inside = 0
    peak = 0
    guard = threading.Lock()
    start = threading.Barrier(6)
    failures: list[BaseException] = []

    def _boot() -> None:
        nonlocal inside, peak
        try:
            start.wait(timeout=30)
            with control_plane_boot_lock(fresh_database):
                with guard:
                    inside += 1
                    peak = max(peak, inside)
                time.sleep(0.2)
                with guard:
                    inside -= 1
        except BaseException as exc:  # collected and asserted on below
            failures.append(exc)

    threads = [threading.Thread(target=_boot) for _ in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=60)
    assert not failures, failures
    assert peak == 1, f"{peak} boots held the lock together"


def test_a_file_control_plane_takes_no_lock(tmp_path):
    from provisa.core.boot_lock import control_plane_boot_lock

    with control_plane_boot_lock(f"sqlite+pysqlite:///{tmp_path / 'cp.db'}"):
        pass
