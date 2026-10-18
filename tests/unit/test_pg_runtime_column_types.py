# Copyright (c) 2026 Kenneth Stott
# Canary: 575b9aa7-3594-43ac-8f20-95bbb67dfffc
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The pg engine's answer carries its columns' types, on every read path (REQ-846).

The event loop learns a materialized view's output columns from a LIMIT 0 read of its build
statement (events/app_wiring.wire_event_loop): a view whose read reports no types is left off the
scheduler, so it is never built. The engine's description of a result names each column's type
whether or not a row comes back."""

# Requirements: REQ-846

from __future__ import annotations

import pytest

from provisa.federation.pg_runtime import PgFederationRuntime

pytestmark = pytest.mark.unit


@pytest.fixture
def runtime(embedded_postgres):
    rt = PgFederationRuntime(engine_dsn=embedded_postgres.dsn())
    yield rt
    rt.close()


_READ = "SELECT 'us'::text AS region, SUM(n) AS total, COUNT(*) AS n FROM (VALUES (1), (2)) AS t(n)"


async def test_a_read_reports_its_columns_types_with_no_row_returned(runtime):
    result = await runtime.run(f"SELECT * FROM ({_READ}) AS probe LIMIT 0")
    assert result.rows == []
    assert result.column_names == ["region", "total", "n"]
    assert result.column_types == ["text", "int8", "int8"]


async def test_a_read_reports_the_same_types_as_the_streamed_read(runtime):
    streamed = runtime.run_sync(_READ)
    try:
        expected = streamed.column_types
    finally:
        streamed.close()
    assert (await runtime.run(_READ)).column_types == expected
