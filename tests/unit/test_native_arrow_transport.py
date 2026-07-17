# Copyright (c) 2026 Kenneth Stott
# Canary: 92445ad3-29c6-4106-9dc9-68588b428463
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-986: native federation engines surface Arrow through the runtime — ``run_arrow`` returns a
pyarrow.Table and ``run_arrow_stream`` returns ``(schema, record-batch generator)`` for the Flight
server's RecordBatchStream / GeneratorStream, with no Python row materialization. Covers DuckDB (the
zero-config default) and embedded ClickHouse (chdb); the remote ClickHouse backends and warehouse
engines share the same runtime contract, exercised live in integration."""

from __future__ import annotations

import pyarrow as pa
import pytest


def test_duckdb_run_arrow_returns_table():
    from provisa.federation.duckdb_runtime import DuckDBFederationRuntime

    rt = DuckDBFederationRuntime()
    try:
        table = rt.run_arrow("SELECT 1 AS id, 'a' AS s UNION ALL SELECT 2, 'b'")
        assert isinstance(table, pa.Table)
        assert table.num_rows == 2
        assert table.column_names == ["id", "s"]
    finally:
        rt.close()


def test_duckdb_run_arrow_stream_yields_batches():
    from provisa.federation.duckdb_runtime import DuckDBFederationRuntime

    rt = DuckDBFederationRuntime()
    try:
        schema, batches = rt.run_arrow_stream("SELECT n FROM range(5) t(n)")
        assert isinstance(schema, pa.Schema)
        assert schema.names == ["n"]
        rows = sum(b.num_rows for b in batches)
        assert rows == 5
    finally:
        rt.close()


def test_clickhouse_embedded_run_arrow_returns_table():
    pytest.importorskip("chdb")
    from provisa.federation.clickhouse_runtime import ClickHouseFederationRuntime

    rt = ClickHouseFederationRuntime.embedded()
    try:
        table = rt.run_arrow("SELECT number AS n FROM numbers(3)")
        assert isinstance(table, pa.Table)
        assert table.num_rows == 3
        assert table.column_names == ["n"]
    finally:
        rt.close()


def test_clickhouse_embedded_run_arrow_stream_yields_batches():
    pytest.importorskip("chdb")
    from provisa.federation.clickhouse_runtime import ClickHouseFederationRuntime

    rt = ClickHouseFederationRuntime.embedded()
    try:
        schema, batches = rt.run_arrow_stream("SELECT number AS n FROM numbers(5)")
        assert isinstance(schema, pa.Schema)
        assert sum(b.num_rows for b in batches) == 5
    finally:
        rt.close()


if __name__ == "__main__":
    pytest.main([__file__, "-q"])
