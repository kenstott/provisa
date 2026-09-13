# Copyright (c) 2026 Kenneth Stott
# Canary: de671e4c-269a-4ff5-af3f-7d860bddaef5
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""E2E: plain embedded DuckDB reads a real Delta Lake table IN PLACE (REQ-899).

No docker, no JVM, no cloud creds. Writes a small local Delta table with the ``deltalake``
package (delta-rs; pure Rust wheel), then drives the REAL DuckDBDeltaConnector's delta_scan
against a plain ``duckdb.connect()`` (not pg_duckdb — the plain DuckDB engine is the cheapest
proof of the delta_scan mechanism, since delta is a CORE extension needing no compiled-in build).

Skips unless the ``deltalake`` package is installed.
"""

from __future__ import annotations

import pytest

pytestmark = [pytest.mark.integration]

duckdb = pytest.importorskip("duckdb")
pytest.importorskip("pyarrow")
deltalake = pytest.importorskip("deltalake")

from provisa.core.models import Source, SourceType  # noqa: E402
from provisa.federation.connector_duckdb import DuckDBDeltaConnector  # noqa: E402


def _make_delta_table(root) -> str:
    """Write a small local Delta table with deltalake (delta-rs) — DuckDB's delta extension is
    read-only (delta_scan), so it cannot produce the fixture's _delta_log itself."""
    import pyarrow as pa

    path = str(root / "orders")
    data = pa.table(
        {
            "id": pa.array([1, 2, 3, 4], pa.int32()),
            "region": pa.array(["us-east", "us-west", "us-east", "apac"]),
            "amount": pa.array([10.5, 20.0, 5.25, 7.75], pa.float64()),
        }
    )
    deltalake.write_deltalake(path, data)
    return path


def test_duckdb_delta_connector_reads_in_place(tmp_path):
    """The REAL DuckDBDeltaConnector's delta_scan reads a Delta table in place via plain DuckDB."""
    path = _make_delta_table(tmp_path)
    source = Source(id="orders", type=SourceType.delta_lake, path=path)
    view_ddl = DuckDBDeltaConnector().details(source)["view_ddl"]
    assert "delta_scan(" in view_ddl

    con = duckdb.connect()
    try:
        con.execute("INSTALL delta")
        con.execute("LOAD delta")
        con.execute(view_ddl)
        rows = con.execute("SELECT id, region, amount FROM orders ORDER BY id").fetchall()
        assert rows == [
            (1, "us-east", 10.5),
            (2, "us-west", 20.0),
            (3, "us-east", 5.25),
            (4, "apac", 7.75),
        ]  # read from Delta Lake in place, zero copy

        # a filter against the delta_scan-backed view proves it behaves like an ordinary relation
        filtered = con.execute(
            "SELECT id FROM orders WHERE region = 'us-east' ORDER BY id"
        ).fetchall()
        assert filtered == [(1,), (3,)]
    finally:
        con.close()


if __name__ == "__main__":
    pytest.main([__file__, "-q"])
