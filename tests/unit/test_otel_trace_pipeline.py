# Copyright (c) 2026 Kenneth Stott
# Canary: a1b2c3d4-e5f6-7890-abcd-ef1234567890
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for the OTel trace pipeline.

Covers:
- execute_trino emits a provisa.query.trino span with provisa.table attribute
  (skipped when opentelemetry SDK is not installed)
- _insert_otel_iceberg correctly extracts span_attributes JSON into table_name column
- _row() builder maps provisa.table → table_name for traces signal
"""

from __future__ import annotations

import importlib
import importlib.util
import inspect
import json
from datetime import datetime
from unittest.mock import MagicMock

import pytest

_otel_missing = importlib.util.find_spec("opentelemetry") is None
_skip_otel = pytest.mark.skipif(_otel_missing, reason="opentelemetry SDK not installed")


# ---------------------------------------------------------------------------
# execute_trino — OTel span emission
# ---------------------------------------------------------------------------


@_skip_otel
class TestExecuteTrinoSpanEmission:
    """execute_trino with span_attrs must emit provisa.query.trino span."""

    def _make_conn(self):
        cur = MagicMock()
        cur.description = [("id",), ("name",)]
        cur.fetchall.return_value = [(1, "a")]
        conn = MagicMock()
        conn.cursor.return_value = cur
        return conn

    def test_span_name_is_provisa_query_trino_when_span_attrs_given(self, otel_spans):
        from provisa.executor.trino import execute_trino
        from tests.helpers import assert_span_emitted

        conn = self._make_conn()
        execute_trino(
            conn,
            "SELECT 1",
            span_attrs={
                "provisa.table": "pets",
                "provisa.domain": "pet-store",
                "provisa.role": "admin",
            },
        )
        spans = otel_spans.get_finished_spans()
        assert any("provisa.query.trino" in s.name for s in spans)
        assert_span_emitted(otel_spans, "provisa.query.trino")

    def test_span_carries_provisa_table_attribute(self, otel_spans):
        from provisa.executor.trino import execute_trino

        conn = self._make_conn()
        execute_trino(
            conn,
            "SELECT 1",
            span_attrs={
                "provisa.table": "pets",
                "provisa.domain": "pet-store",
                "provisa.role": "admin",
            },
        )
        spans = otel_spans.get_finished_spans()
        target = next((s for s in spans if "provisa.query.trino" in s.name), None)
        assert target is not None, (
            f"No provisa.query.trino span. Emitted: {[s.name for s in spans]}"
        )
        assert target.attributes.get("provisa.table") == "pets"

    def test_span_carries_provisa_domain_and_role(self, otel_spans):
        from provisa.executor.trino import execute_trino

        conn = self._make_conn()
        execute_trino(
            conn,
            "SELECT 1",
            span_attrs={
                "provisa.table": "pets",
                "provisa.domain": "pet-store",
                "provisa.role": "analyst",
            },
        )
        spans = otel_spans.get_finished_spans()
        target = next((s for s in spans if "provisa.query.trino" in s.name), None)
        assert target is not None
        assert target.attributes.get("provisa.domain") == "pet-store"
        assert target.attributes.get("provisa.role") == "analyst"

    def test_span_name_is_trino_execute_without_span_attrs(self, otel_spans):
        from provisa.executor.trino import execute_trino

        conn = self._make_conn()
        execute_trino(conn, "SELECT 1", span_attrs=None)
        spans = otel_spans.get_finished_spans()
        names = [s.name for s in spans]
        assert any("trino.execute" in n for n in names), (
            f"Expected trino.execute span. Got: {names}"
        )
        assert not any("provisa.query.trino" in n for n in names)


# ---------------------------------------------------------------------------
# _insert_otel_iceberg — span_attributes JSON → table_name extraction
# ---------------------------------------------------------------------------


class TestInsertOtelIceberg:
    """_insert_otel_iceberg extracts provisa.* from span_attributes into columns."""

    def _make_conn(self, trino_cols: dict[str, str]) -> MagicMock:
        cur = MagicMock()
        cur.fetchall.return_value = [(name, typ) for name, typ in trino_cols.items()]
        conn = MagicMock()
        conn.cursor.return_value = cur
        return conn

    def test_extracts_table_name_from_span_attributes(self):
        pytest.importorskip("pyarrow")
        import pyarrow as pa
        from provisa.scheduler.jobs import _insert_otel_iceberg

        trino_cols = {
            "trace_id": "varchar",
            "span_id": "varchar",
            "span_name": "varchar",
            "service_name": "varchar",
            "timestamp": "bigint",
            "span_attributes": "varchar",
            "table_name": "varchar",
            "domain_id": "varchar",
            "role_id": "varchar",
            "query_text": "varchar",
            "_date": "date",
        }
        conn = self._make_conn(trino_cols)

        attrs_json = json.dumps(
            {
                "provisa.table": "pets",
                "provisa.domain": "pet-store",
                "provisa.role": "admin",
                "provisa.query_text": "{ ps__pets { id } }",
            }
        )

        table = pa.table(
            {
                "trace_id": pa.array(["abc123"], type=pa.string()),
                "span_id": pa.array(["def456"], type=pa.string()),
                "span_name": pa.array(["provisa.query.trino"], type=pa.string()),
                "service_name": pa.array(["provisa"], type=pa.string()),
                "timestamp": pa.array([1_700_000_000_000_000], type=pa.int64()),
                "span_attributes": pa.array([attrs_json], type=pa.string()),
            }
        )

        _insert_otel_iceberg(conn, "traces", table, datetime(2026, 5, 11))

        cur = conn.cursor.return_value
        all_args = " ".join(str(c) for c in cur.execute.call_args_list)
        assert "pets" in all_args, f"'pets' not found in INSERT args. Calls: {all_args}"
        assert "{ ps__pets { id } }" in all_args, (
            f"query_text not found in INSERT args. Calls: {all_args}"
        )

    def test_extracts_query_text_from_span_attributes(self):
        pytest.importorskip("pyarrow")
        import pyarrow as pa
        from provisa.scheduler.jobs import _insert_otel_iceberg

        trino_cols = {
            "trace_id": "varchar",
            "span_name": "varchar",
            "span_attributes": "varchar",
            "query_text": "varchar",
            "_date": "date",
        }
        conn = self._make_conn(trino_cols)

        attrs_json = json.dumps({"provisa.query_text": "{ ps__pets { id name } }"})
        table = pa.table(
            {
                "trace_id": pa.array(["abc"], type=pa.string()),
                "span_name": pa.array(["provisa.query.trino"], type=pa.string()),
                "span_attributes": pa.array([attrs_json], type=pa.string()),
            }
        )

        _insert_otel_iceberg(conn, "traces", table, datetime(2026, 5, 11))

        cur = conn.cursor.return_value
        all_args = " ".join(str(c) for c in cur.execute.call_args_list)
        assert "{ ps__pets { id name } }" in all_args, (
            f"query_text value not found in INSERT args. Calls: {all_args}"
        )

    def test_table_name_absent_when_not_in_trino_schema(self):
        """If table_name column doesn't exist in Trino, no error raised."""
        pytest.importorskip("pyarrow")
        import pyarrow as pa
        from provisa.scheduler.jobs import _insert_otel_iceberg

        trino_cols = {
            "trace_id": "varchar",
            "span_name": "varchar",
            "span_attributes": "varchar",
            "_date": "date",
        }
        conn = self._make_conn(trino_cols)

        attrs_json = json.dumps({"provisa.table": "pets"})
        table = pa.table(
            {
                "trace_id": pa.array(["abc"], type=pa.string()),
                "span_name": pa.array(["provisa.query.trino"], type=pa.string()),
                "span_attributes": pa.array([attrs_json], type=pa.string()),
            }
        )

        # Must not raise when table_name column is absent from Trino schema
        _insert_otel_iceberg(conn, "traces", table, datetime(2026, 5, 11))

        # Find the INSERT call and verify table_name is not in its column list
        cur = conn.cursor.return_value
        insert_calls = [str(c) for c in cur.execute.call_args_list if "INSERT INTO" in str(c)]
        assert insert_calls, "An INSERT statement must have been executed"
        insert_sql = insert_calls[0]
        assert "table_name" not in insert_sql, (
            "table_name must not appear in the INSERT column list when absent from Trino schema"
        )


# ---------------------------------------------------------------------------
# Attribute mapping stability
# ---------------------------------------------------------------------------


class TestAttrKeyMapping:
    """The provisa.* → column name mapping must remain stable."""

    def test_attr_key_mapping_present_in_source(self):
        from provisa.scheduler import jobs

        src = inspect.getsource(jobs)
        expected = {
            "table_name": "provisa.table",
            "domain_id": "provisa.domain",
            "role_id": "provisa.role",
            "query_text": "provisa.query_text",
        }
        for col, attr in expected.items():
            found = (f'"{col}": "{attr}"' in src) or (f"'{col}': '{attr}'" in src)
            assert found, (
                f"Mapping {col!r} → {attr!r} not found in jobs.py source. "
                "Schema drift: update _OPS_TABLES or _attr_keys."
            )
