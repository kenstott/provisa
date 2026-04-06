# Copyright (c) 2026 Kenneth Stott
# Canary: f1a2b3c4-d5e6-7890-abcd-ef1234567890
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration tests for output format serialization: NDJSON, tabular (CSV/Parquet), Arrow."""

from __future__ import annotations

import json
import os

import pytest
import pytest_asyncio

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

# ---------------------------------------------------------------------------
# Optional-dependency guards
# ---------------------------------------------------------------------------

try:
    import pyarrow as pa
    _HAVE_PYARROW = True
except ImportError:
    _HAVE_PYARROW = False

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

from provisa.compiler.sql_gen import ColumnRef
from provisa.executor.formats.ndjson import rows_to_ndjson
from provisa.executor.formats.tabular import rows_to_csv, rows_to_parquet
from provisa.executor.formats.arrow import rows_to_arrow_ipc, rows_to_arrow_table


def _make_cols(*names: str, nested_in: str | None = None) -> list[ColumnRef]:
    return [
        ColumnRef(alias=None, column=n, field_name=n, nested_in=nested_in)
        for n in names
    ]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest_asyncio.fixture(scope="session")
async def source_pool():
    """Real PG pool for execute_direct tests."""
    try:
        from provisa.executor.pool import SourcePool
        sp = SourcePool()
        await sp.add(
            "test-pg",
            source_type="postgresql",
            host=os.environ.get("PG_HOST", "localhost"),
            port=int(os.environ.get("PG_PORT", "5432")),
            database=os.environ.get("PG_DATABASE", "provisa"),
            user=os.environ.get("PG_USER", "provisa"),
            password=os.environ.get("PG_PASSWORD", "provisa"),
        )
        yield sp
        await sp.close_all()
    except Exception:
        yield None


async def _fetch_orders(source_pool):
    """Fetch a small result set from PG for format tests. Skip if unavailable."""
    if source_pool is None:
        pytest.skip("PostgreSQL unavailable")
    try:
        from provisa.executor.direct import execute_direct
        result = await execute_direct(
            source_pool, "test-pg",
            'SELECT "id", "amount" FROM "public"."orders" LIMIT 5',
        )
        return result
    except Exception as exc:
        pytest.skip(f"PostgreSQL unavailable: {exc}")


# ---------------------------------------------------------------------------
# NDJSON tests
# ---------------------------------------------------------------------------

class TestNdjsonFormat:
    def test_ndjson_format_produces_one_json_per_line(self):
        rows = [(1, 99.5), (2, 42.0)]
        cols = _make_cols("id", "amount")
        output = rows_to_ndjson(rows, cols)
        lines = [ln for ln in output.splitlines() if ln.strip()]
        assert len(lines) == 2
        for line in lines:
            obj = json.loads(line)
            assert isinstance(obj, dict)
            assert "id" in obj
            assert "amount" in obj

    def test_ndjson_format_count_matches_rows(self):
        rows = [(i, float(i * 10)) for i in range(7)]
        cols = _make_cols("id", "value")
        output = rows_to_ndjson(rows, cols)
        lines = [ln for ln in output.splitlines() if ln.strip()]
        assert len(lines) == len(rows)

    def test_ndjson_empty_rows_produces_empty_string(self):
        cols = _make_cols("id", "value")
        output = rows_to_ndjson([], cols)
        assert output == ""

    def test_ndjson_nested_col_uses_dotted_key(self):
        cols = [ColumnRef(alias=None, column="name", field_name="name", nested_in="customer")]
        rows = [("Alice",)]
        output = rows_to_ndjson(rows, cols)
        obj = json.loads(output.strip())
        assert "customer.name" in obj

    async def test_ndjson_with_real_pg_rows(self, source_pool):
        result = await _fetch_orders(source_pool)
        cols = [
            ColumnRef(alias=None, column=c, field_name=c, nested_in=None)
            for c in result.column_names
        ]
        output = rows_to_ndjson(result.rows, cols)
        lines = [ln for ln in output.splitlines() if ln.strip()]
        assert len(lines) == len(result.rows)
        for line in lines:
            obj = json.loads(line)
            assert set(obj.keys()) == set(result.column_names)


# ---------------------------------------------------------------------------
# Tabular (CSV) tests
# ---------------------------------------------------------------------------

class TestTabularFormat:
    def test_tabular_normalized_format(self):
        """rows_to_csv produces header row + data rows with matching columns."""
        rows = [(1, 100.5), (2, 200.0)]
        cols = _make_cols("id", "amount")
        csv_output = rows_to_csv(rows, cols)
        lines = csv_output.strip().splitlines()
        # First line is header
        header = lines[0].split(",")
        assert "id" in header
        assert "amount" in header
        # Data rows follow
        assert len(lines) == 3  # header + 2 data rows

    def test_tabular_denormalized_format(self):
        """rows_to_csv with nested column uses dotted name in header."""
        cols = [
            ColumnRef(alias=None, column="region", field_name="region", nested_in="orders"),
        ]
        rows = [("us-east",), ("eu-west",)]
        csv_output = rows_to_csv(rows, cols)
        first_line = csv_output.splitlines()[0]
        assert "orders.region" in first_line

    def test_tabular_csv_row_count(self):
        rows = [(i,) for i in range(10)]
        cols = _make_cols("id")
        csv_output = rows_to_csv(rows, cols)
        lines = [ln for ln in csv_output.strip().splitlines() if ln]
        # header + 10 data rows
        assert len(lines) == 11

    def test_tabular_empty_rows(self):
        cols = _make_cols("id")
        csv_output = rows_to_csv([], cols)
        lines = [ln for ln in csv_output.strip().splitlines() if ln]
        assert len(lines) == 1  # header only

    async def test_tabular_with_real_pg_rows(self, source_pool):
        result = await _fetch_orders(source_pool)
        cols = [
            ColumnRef(alias=None, column=c, field_name=c, nested_in=None)
            for c in result.column_names
        ]
        csv_output = rows_to_csv(result.rows, cols)
        lines = csv_output.strip().splitlines()
        assert len(lines) == len(result.rows) + 1  # header + data


# ---------------------------------------------------------------------------
# Arrow tests
# ---------------------------------------------------------------------------

@pytest.mark.skipif(not _HAVE_PYARROW, reason="pyarrow not installed")
class TestArrowFormat:
    def test_arrow_format_produces_record_batch(self):
        rows = [(1, 2.5), (2, 3.5)]
        cols = _make_cols("id", "score")
        ipc_bytes = rows_to_arrow_ipc(rows, cols)
        assert isinstance(ipc_bytes, bytes)
        assert len(ipc_bytes) > 0
        # Round-trip: read the IPC stream back
        import io
        reader = pa.ipc.open_stream(pa.BufferReader(ipc_bytes))
        table = reader.read_all()
        assert table.num_rows == 2

    def test_arrow_schema_matches_columns(self):
        rows = [(10, "hello")]
        cols = _make_cols("order_id", "region")
        table = rows_to_arrow_table(rows, cols)
        field_names = [f.name for f in table.schema]
        assert "order_id" in field_names
        assert "region" in field_names

    def test_arrow_types_preserved(self):
        """Integer data inferred as int64, float as float64, string as large_string."""
        rows = [(1, 3.14, "foo")]
        cols = _make_cols("int_col", "float_col", "str_col")
        table = rows_to_arrow_table(rows, cols)
        schema_map = {f.name: f.type for f in table.schema}
        assert pa.types.is_integer(schema_map["int_col"])
        assert pa.types.is_floating(schema_map["float_col"])
        assert pa.types.is_large_string(schema_map["str_col"]) or pa.types.is_string(schema_map["str_col"])

    def test_arrow_empty_rows(self):
        cols = _make_cols("id")
        table = rows_to_arrow_table([], cols)
        assert table.num_rows == 0
        assert "id" in [f.name for f in table.schema]

    def test_arrow_ipc_roundtrip(self):
        import io
        rows = [(i, float(i) * 1.1) for i in range(50)]
        cols = _make_cols("id", "val")
        ipc_bytes = rows_to_arrow_ipc(rows, cols)
        reader = pa.ipc.open_stream(pa.BufferReader(ipc_bytes))
        recovered = reader.read_all()
        assert recovered.num_rows == 50
        assert list(recovered.column("id").to_pylist()) == list(range(50))

    async def test_arrow_with_real_pg_rows(self, source_pool):
        result = await _fetch_orders(source_pool)
        cols = [
            ColumnRef(alias=None, column=c, field_name=c, nested_in=None)
            for c in result.column_names
        ]
        table = rows_to_arrow_table(result.rows, cols)
        assert table.num_rows == len(result.rows)
        field_names = [f.name for f in table.schema]
        for col_name in result.column_names:
            assert col_name in field_names
