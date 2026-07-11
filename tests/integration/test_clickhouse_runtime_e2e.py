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


async def test_clickhouse_runtime_arrow_transport():
    """REQ-986: the ENGINE ARROW terminal returns a native pyarrow Table, no row materialization."""
    import pyarrow as pa

    rt = ClickHouseFederationRuntime.embedded()
    try:
        table = rt.run_arrow("SELECT number AS n, toString(number) AS s FROM numbers(5) ORDER BY n")
        assert isinstance(table, pa.Table)
        assert table.num_rows == 5
        assert table.column_names == ["n", "s"]
        assert table.column("n").to_pylist() == [0, 1, 2, 3, 4]
    finally:
        rt.close()


async def test_clickhouse_runtime_arrow_stream_transport():
    """REQ-986: the ENGINE ARROW_STREAM terminal returns (schema, lazy RecordBatch iterator)."""
    import pyarrow as pa

    rt = ClickHouseFederationRuntime.embedded()
    try:
        schema, batches = rt.run_arrow_stream("SELECT number AS n FROM numbers(9) ORDER BY n")
        assert isinstance(schema, pa.Schema)
        assert schema.names == ["n"]
        collected = list(batches)
        assert all(isinstance(b, pa.RecordBatch) for b in collected)
        assert sum(b.num_rows for b in collected) == 9
    finally:
        rt.close()


async def test_clickhouse_backend_arrow_capabilities_wired():
    """REQ-986: the engine declares ARROW/ARROW_STREAM and the backend honors both."""
    import pyarrow as pa

    from provisa.federation.clickhouse_backend import ClickHouseBackend
    from provisa.federation.engine import build_clickhouse_engine
    from provisa.federation.runtime import EngineCapability

    engine = build_clickhouse_engine()
    assert EngineCapability.ARROW in engine.capabilities
    assert EngineCapability.ARROW_STREAM in engine.capabilities

    backend = ClickHouseBackend(engine)
    state = SimpleNamespace(config=None)
    table = backend.execute_arrow(state, "SELECT number AS n FROM numbers(3)")
    assert isinstance(table, pa.Table)
    assert table.num_rows == 3

    schema, batches = backend.execute_stream(state, "SELECT number AS n FROM numbers(4)")
    assert schema.names == ["n"]
    assert sum(b.num_rows for b in batches) == 4
