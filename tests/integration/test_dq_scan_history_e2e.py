# Copyright (c) 2026 Kenneth Stott
# Canary: 2d9f4a61-7c3e-4b85-a0d2-6e1b8c5f9a37
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E (REQ-1443, amended 2026-10-12): a checker table's scans accumulate, read in place.

A real server registers a Great Expectations checker table from its configuration, once on the DuckDB
engine (a control plane database of its own, read through its control-plane attach) and once on the test stack's
Trino (Postgres control plane, read through the provisa_admin catalog). The table's result relation
is created in the org's control plane at registration, under ``<name>_<source>_dq``, and the table
publishes the name it was given: SQL reads it as ``dqh.orders_quality``.

Two scans' rows are appended to the relation the way a scan appends them (``results_sa_table``, the
writer's own table), one after the other with a read between. A replica would be built once and
answer the first scan's rows only; a read in place answers both, the moment they are written.
"""

# Requirements: REQ-1443

from __future__ import annotations

import os
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import pytest
import pytest_asyncio
import sqlalchemy as sa

pytestmark = [
    pytest.mark.requires_trino,
    pytest.mark.integration,
    pytest.mark.asyncio(loop_scope="session"),
]

pytest.importorskip("duckdb")

_CONFIG = "tests/fixtures/dq_scan_history_config.yaml"
_ORG = "dq_scan_history_e2e"
_SCHEMA = "dq_hist_e2e"
_RELATION = "orders_quality_dq_hist_dq"


async def _pg():
    import asyncpg

    return await asyncpg.connect(
        host=os.environ.get("PG_HOST", "localhost"),
        port=int(os.environ.get("PG_PORT", "5432")),
        user=os.environ.get("PG_USER", "provisa"),
        password=os.environ.get("PG_PASSWORD", "provisa"),
        database=os.environ.get("PG_DATABASE", "provisa"),
        timeout=15,
    )


async def _seed() -> None:
    conn = await _pg()
    try:
        await conn.execute(f"DROP SCHEMA IF EXISTS {_SCHEMA} CASCADE")
        await conn.execute(f"CREATE SCHEMA {_SCHEMA}")
        await conn.execute(f"CREATE TABLE {_SCHEMA}.orders (id int PRIMARY KEY, customer text)")
        await conn.execute(f"INSERT INTO {_SCHEMA}.orders VALUES (1, 'ann'), (2, NULL)")
    finally:
        await conn.close()


async def _drop() -> None:
    conn = await _pg()
    try:
        await conn.execute(f"DROP SCHEMA IF EXISTS {_SCHEMA} CASCADE")
    finally:
        await conn.close()


def _relation_engine(server) -> tuple[sa.Engine, str | None]:
    """The control-plane store holding the relation, and its schema (the org's): the server's own
    database when it has one, else the session's PostgreSQL."""
    if server.plane is not None:
        return server.plane.engine(), f"org_{server.org_id}"
    user = os.environ.get("PG_USER", "provisa")
    password = os.environ.get("PG_PASSWORD", "provisa")
    host = os.environ.get("PG_HOST", "localhost")
    port = os.environ.get("PG_PORT", "5432")
    database = os.environ.get("PG_DATABASE", "provisa")
    engine = sa.create_engine(f"postgresql+psycopg://{user}:{password}@{host}:{port}/{database}")
    return engine, f"org_{server.org_id}"


def _append_scan(server, scan_id: str, scan_time: datetime) -> None:
    """One scan's rows, appended as a scan appends them."""
    from provisa.dq.results import RESULT_FIELDS, results_sa_table

    engine, schema = _relation_engine(server)
    try:
        relation = results_sa_table(_RELATION).to_metadata(sa.MetaData(), schema=schema)
        row: dict[str, Any] = {field: None for field in RESULT_FIELDS}
        row.update(
            scan_id=scan_id,
            scan_time=scan_time,
            checker="great_expectations",
            dataset="provisa/dqh/orders",
            target_table="dqh.orders",
            column_name="customer",
            check_name="expect_column_values_to_not_be_null",
            check_type="expect_column_values_to_not_be_null",
            outcome="fail",
            rows_tested=2,
            failed_rows=1,
        )
        with engine.begin() as conn:
            conn.execute(relation.insert(), [row])
    finally:
        engine.dispose()


@pytest_asyncio.fixture(scope="module", params=["duckdb", "trino"])
async def server(request):
    from tests.integration.isolated_server import IsolatedServer, drop_org_schema

    await _seed()
    org = f"{_ORG}_{request.param}"
    if request.param == "duckdb":
        srv = IsolatedServer(org, engine="duckdb", config=_CONFIG, control_plane="own")
    else:
        srv = IsolatedServer(org, engine="trino", config=_CONFIG)
    srv.start()
    try:
        yield srv
    finally:
        srv.stop_process()
        await _drop()
        if request.param == "trino":
            await drop_org_schema(org)


async def _scan_ids(server) -> list[str]:
    async with httpx.AsyncClient(base_url=server.base_url, timeout=120.0) as client:
        resp = await client.post(
            "/data/sql",
            json={
                "sql": "SELECT scan_id FROM dqh.orders_quality ORDER BY scan_time",
                "role": "org_admin",
            },
        )
    assert resp.status_code == 200, f"{resp.text}\n{server.dump_stderr_debug()[-6000:]}"
    body = resp.json()
    rows = body.get("data") or body.get("rows") or []
    if isinstance(rows, dict):
        rows = next(iter(rows.values()))
    return [r["scan_id"] for r in rows]


async def test_the_table_reads_as_an_empty_history_before_its_first_scan(server):
    assert await _scan_ids(server) == []


async def test_every_scan_is_read_in_place_beside_the_earlier_ones(server):
    first, second = f"scan-{uuid.uuid4().hex[:8]}", f"scan-{uuid.uuid4().hex[:8]}"
    now = datetime.now(UTC)

    _append_scan(server, first, now)
    assert await _scan_ids(server) == [first]

    _append_scan(server, second, now + timedelta(seconds=1))
    assert await _scan_ids(server) == [first, second]
