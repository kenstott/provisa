# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-941: concrete processor handles — source land + MV generate through the real write face."""

from __future__ import annotations

import pytest

from provisa.events.handlers import make_mv_generate, make_source_land
from provisa.federation import store_writer

_COLS = [("id", "bigint"), ("status", "text")]


def _dsn(tmp_path) -> str:
    return f"sqlite+aiosqlite:///{tmp_path / 'store.db'}"


@pytest.mark.asyncio
async def test_source_land_lands_rows_and_reports_shape(tmp_path):
    dsn = _dsn(tmp_path)

    async def fetch(pending):
        assert pending  # the claimed events drove the fetch
        return [{"id": 1, "status": "new"}, {"id": 2, "status": "sold"}]

    land = make_source_land(
        dsn,
        schema="",
        table="orders",
        columns=_COLS,
        change_signal="ttl_probe",
        watermark_column="updated_at",
        pk_columns=["id"],
        fetch=fetch,
    )
    result = await land([{"id": 99, "event_type": "append"}], prior_hash=None)
    # poll+watermark → append; append rows are new by definition → no content hash (None)
    assert result == ("append", {"rows": 2, "landed": "orders"}, None)
    async with store_writer.store_connection(dsn) as conn:
        rows = await conn.fetch("SELECT id, status FROM orders ORDER BY id")
    assert [(r[0], r[1]) for r in rows] == [(1, "new"), (2, "sold")]


@pytest.mark.asyncio
async def test_source_land_empty_fetch_is_noop(tmp_path):
    async def fetch(pending):
        return []

    land = make_source_land(
        _dsn(tmp_path),
        schema="",
        table="orders",
        columns=_COLS,
        change_signal="ttl",
        watermark_column=None,
        pk_columns=None,
        fetch=fetch,
    )
    assert await land([{"e": 1}], prior_hash=None) is None  # nothing landed → no re-post


@pytest.mark.asyncio
async def test_mv_generate_runs_query_lands_replace(tmp_path):
    dsn = _dsn(tmp_path)

    async def run_query():
        return [{"id": 7, "status": "agg"}]

    generate = make_mv_generate(
        dsn, schema="", table="mv_daily", columns=_COLS, run_query=run_query
    )
    event_type, payload, digest = await generate([{"e": 1}], prior_hash=None)
    assert (event_type, payload) == ("replace", {"rows": 1, "landed": "mv_daily"})
    assert isinstance(digest, str) and digest  # MV replace carries a content hash
    async with store_writer.store_connection(dsn) as conn:
        rows = await conn.fetch("SELECT id FROM mv_daily")
    assert [r[0] for r in rows] == [7]
    # REQ-981: re-generating the identical result matches prior_hash → gated (no re-post)
    assert await generate([{"e": 2}], prior_hash=digest) is None
