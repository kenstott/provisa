# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""E2E: ClickHouseFederationRuntime federates a file source in place (REQ-909).

Drives the runtime object directly — the NativeEngineBackend execution protocol (attach_source,
run/run_sync) — against embedded chdb (in-process ClickHouse, no server). A green run proves the
ClickHouse engine's runtime: a CSV source is mounted via a File table engine, wrapped in a physical-
named view, and a federated query returns its rows.
"""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

chdb = pytest.importorskip("chdb")

from provisa.federation.clickhouse_runtime import ClickHouseFederationRuntime  # noqa: E402

_FILES = Path(__file__).parent.parent.parent / "demo" / "files"


async def test_clickhouse_runtime_federates_csv_source():
    rt = ClickHouseFederationRuntime.embedded()
    try:
        src = SimpleNamespace(
            id="cust",
            type=SimpleNamespace(value="csv"),
            path=str(_FILES / "customers.csv"),
            schema_name="sales",
            table_name="customers",
            federation_hints={},
        )
        rt.attach_source(src)

        res = rt.run_sync('SELECT count(*) AS n FROM "sales"."customers"')
        assert res.rows[0][0] > 0

        rows = rt.run_sync('SELECT "id" FROM "sales"."customers" ORDER BY "id" LIMIT 3')
        assert len(rows.rows) == 3
    finally:
        rt.close()
