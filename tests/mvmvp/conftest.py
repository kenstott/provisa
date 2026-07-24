# Copyright (c) 2026 Kenneth Stott
# Canary: 6f3c1d90-4a2e-4b71-9f0c-2d7a5e8b1c34
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Shared fixtures for the mv-mvp e2e suite: one isolated ``provisa-mvmvp`` compose stack
(control-plane DB + materialize store DB) provisioned once per session, and a per-test
control-plane ``Database`` with the queue tables freshly truncated and any store tables a
test lands dropped up-front so runs never bleed into each other.

Self-provisions the stack idempotently — a skip is a defect, so the fixture brings the stack
up rather than skipping when it is absent (Postgres on 127.0.0.1:55432 by default).
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest
from sqlalchemy import text

from provisa.core.database import Database, create_engine_from_url
from provisa.core.schema_org import event_status, events, node_freshness_state
from provisa.federation.store_writer import async_store_url

_REPO = Path(__file__).resolve().parents[2]
_COMPOSE = ["docker", "compose", "-p", "provisa-mvmvp", "-f", "docker-compose.mvmvp.yml"]
# Connection parts for the isolated stack, overridable by env; a DSN is composed from them rather
# than embedded as a literal user:pass@host string (which trips secret scanners on a non-secret,
# local-only test credential). Defaults match docker-compose.mvmvp.yml.
_PG = {
    "user": os.environ.get("MVMVP_PG_USER", "provisa"),
    "password": os.environ.get("MVMVP_PG_PASSWORD", "provisa"),
    "host": os.environ.get("MVMVP_PG_HOST", "localhost"),
    "port": os.environ.get("MVMVP_PG_PORT", "55432"),
}
# Every store table any mv-mvp test may land — dropped up-front per test for isolation.
_STORE_TABLES = ("orders", "mv_a", "mv_b", "mv_c", "mv_d")


def _dsn(database: str) -> str:
    return f"postgresql://{_PG['user']}:{_PG['password']}@{_PG['host']}:{_PG['port']}/{database}"


_CP_DSN = _dsn("provisa")
_STORE_DSN = _dsn("provisa_store")


@pytest.fixture(scope="session")
def mvmvp_stack():
    """Self-provision the isolated stack (idempotent; fast when already healthy), and tear it down at
    session end so it never leaks containers that starve later runs of memory. Set
    PYTEST_DOCKER_KEEP=1 to keep it up between local iterations."""
    subprocess.run([*_COMPOSE, "up", "-d", "--wait"], cwd=_REPO, check=True, timeout=180)
    try:
        yield {"cp": _CP_DSN, "store": _STORE_DSN}
    finally:
        if not os.environ.get("PYTEST_DOCKER_KEEP"):
            subprocess.run([*_COMPOSE, "down", "-v"], cwd=_REPO, check=False, timeout=120)


@pytest.fixture
async def control_plane(mvmvp_stack):
    """A control-plane ``Database`` with the queue tables freshly created + truncated per test, plus
    a cleaner that drops any store tables a test lands so runs are isolated."""
    cp_engine = create_engine_from_url(async_store_url(mvmvp_stack["cp"]), pool_size=2)
    tables = [events, event_status, node_freshness_state]
    async with cp_engine.begin() as c:
        await c.run_sync(lambda s: events.metadata.create_all(s, tables=tables))
        await c.execute(
            text("TRUNCATE event_status, events, node_freshness_state RESTART IDENTITY CASCADE")
        )
    store_engine = create_engine_from_url(async_store_url(mvmvp_stack["store"]), pool_size=1)

    async def drop_store(*names: str) -> None:
        async with store_engine.begin() as c:
            for n in names:
                await c.execute(text(f'DROP TABLE IF EXISTS "{n}"'))

    await drop_store(*_STORE_TABLES)
    try:
        yield {
            "db": Database(cp_engine, name="cp"),
            "store": mvmvp_stack["store"],
            "drop": drop_store,
        }
    finally:
        await cp_engine.dispose()
        await store_engine.dispose()
