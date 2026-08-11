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
        cur.description = [
            ("id", "bigint", None, None, None, None, None),
            ("name", "varchar", None, None, None, None, None),
        ]
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

    def _make_engine(self, engine_cols: dict[str, str]):
        from provisa.executor.result import QueryResult

        class _FakeEngine:
            def __init__(self, cols):
                self._cols = cols
                self.calls = []  # (sql, params)

            def execute_engine_sync(self, sql, params=None):
                self.calls.append((sql, params))
                if "SHOW COLUMNS" in sql:
                    return QueryResult(
                        rows=[(n, t) for n, t in self._cols.items()], column_names=[]
                    )
                return QueryResult(rows=[], column_names=[])

        return _FakeEngine(engine_cols)

    def test_extracts_table_name_from_span_attributes(self):
        pytest.importorskip("pyarrow")
        import pyarrow as pa
        from provisa.scheduler.jobs import _insert_otel_iceberg

        engine_cols = {
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
        engine = self._make_engine(engine_cols)

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

        _insert_otel_iceberg(engine, "traces", table, datetime(2026, 5, 11))

        all_args = " ".join(str(c) for c in engine.calls)
        assert "pets" in all_args, f"'pets' not found in INSERT args. Calls: {all_args}"
        assert "{ ps__pets { id } }" in all_args, (
            f"query_text not found in INSERT args. Calls: {all_args}"
        )

    def test_extracts_query_text_from_span_attributes(self):
        pytest.importorskip("pyarrow")
        import pyarrow as pa
        from provisa.scheduler.jobs import _insert_otel_iceberg

        engine_cols = {
            "trace_id": "varchar",
            "span_name": "varchar",
            "span_attributes": "varchar",
            "query_text": "varchar",
            "_date": "date",
        }
        engine = self._make_engine(engine_cols)

        attrs_json = json.dumps({"provisa.query_text": "{ ps__pets { id name } }"})
        table = pa.table(
            {
                "trace_id": pa.array(["abc"], type=pa.string()),
                "span_name": pa.array(["provisa.query.trino"], type=pa.string()),
                "span_attributes": pa.array([attrs_json], type=pa.string()),
            }
        )

        _insert_otel_iceberg(engine, "traces", table, datetime(2026, 5, 11))

        all_args = " ".join(str(c) for c in engine.calls)
        assert "{ ps__pets { id name } }" in all_args, (
            f"query_text value not found in INSERT args. Calls: {all_args}"
        )

    def test_table_name_absent_when_not_in_trino_schema(self):
        """If table_name column doesn't exist in Trino, no error raised."""
        pytest.importorskip("pyarrow")
        import pyarrow as pa
        from provisa.scheduler.jobs import _insert_otel_iceberg

        engine_cols = {
            "trace_id": "varchar",
            "span_name": "varchar",
            "span_attributes": "varchar",
            "_date": "date",
        }
        engine = self._make_engine(engine_cols)

        attrs_json = json.dumps({"provisa.table": "pets"})
        table = pa.table(
            {
                "trace_id": pa.array(["abc"], type=pa.string()),
                "span_name": pa.array(["provisa.query.trino"], type=pa.string()),
                "span_attributes": pa.array([attrs_json], type=pa.string()),
            }
        )

        # Must not raise when table_name column is absent from Trino schema
        _insert_otel_iceberg(engine, "traces", table, datetime(2026, 5, 11))

        # Find the INSERT call and verify table_name is not in its column list
        insert_calls = [str(c) for c in engine.calls if "INSERT INTO" in str(c)]
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


# ---------------------------------------------------------------------------
# Object-store layout: where each signal's service partition actually sits
# ---------------------------------------------------------------------------


class _FakePaginator:
    def __init__(self, keys, recorder):
        self._keys = keys
        self._recorder = recorder

    def paginate(self, Bucket, Prefix):  # noqa: N803 — boto3's own parameter names
        self._recorder["prefix"] = Prefix
        return [{"Contents": [{"Key": k} for k in self._keys if k.startswith(Prefix)]}]


class _FakeS3:
    def __init__(self, keys, recorder):
        self._keys = keys
        self._recorder = recorder

    def get_paginator(self, _name):
        return _FakePaginator(self._keys, self._recorder)

    def get_object(self, Bucket, Key):  # noqa: N803
        self._recorder.setdefault("fetched", []).append(Key)
        raise RuntimeError("stop after selection")


class TestSignalPartitionLayout:
    """otlp2parquet nests metrics one level deeper than traces and logs.

    traces/logs land at {signal}/{service}/{date}; metrics land at
    {signal}/{instrument}/{service}/{date}, because a metric's parquet schema depends on its
    instrument type. A prefix pinned to {signal}/{service}/ matched no metric file ever written,
    so the metrics report read zero rows on a node whose writer produced a file every 15 seconds.
    """

    def _run(self, monkeypatch, signal, keys, max_files=50):
        import boto3

        from provisa.scheduler import jobs

        recorder: dict = {}
        monkeypatch.setenv("OTEL_SERVICE_NAME", "provisa")
        monkeypatch.setattr(boto3, "client", lambda *a, **kw: _FakeS3(keys, recorder))
        jobs._compact_signal(
            signal,
            datetime(2026, 8, 11),
            "http://minio:9000",
            "k",
            "s",
            "provisa-otel",
            50,
            max_files,
            None,
        )
        return recorder

    def test_metrics_are_found_under_the_instrument_segment(self, monkeypatch):
        key = "metrics/histogram/provisa/year=2026/month=08/day=11/hour=03/a.parquet"
        recorder = self._run(monkeypatch, "metrics", [key])
        assert recorder["fetched"] == [key]

    def test_traces_keep_the_service_first_layout(self, monkeypatch):
        key = "traces/provisa/year=2026/month=08/day=11/hour=03/a.parquet"
        recorder = self._run(monkeypatch, "traces", [key])
        assert recorder["fetched"] == [key]

    def test_every_reporting_services_partition_is_compacted(self, monkeypatch):
        """The federated engine exports its own spans under its own service name. Filtering keys
        on OTEL_SERVICE_NAME left those files unread and undeleted forever — 26521 of them on the
        SaaS node — while the traces report showed nothing of what the engine actually did."""
        key = "traces/trino/year=2026/month=08/day=11/hour=03/a.parquet"
        recorder = self._run(monkeypatch, "traces", [key])
        assert recorder["fetched"] == [key]

    def test_one_run_takes_no_more_than_the_per_signal_budget(self, monkeypatch):
        """The signals are compacted in a fixed order, so an unbounded backlog in an earlier one
        starves every later one: metrics held the whole run and traces — last in the list — never
        got a turn, which is why the traces report stayed empty while metrics filled."""
        pytest.importorskip("pyarrow")
        import io

        import boto3
        import pyarrow as pa
        import pyarrow.parquet as pq

        from provisa.scheduler import jobs

        buf = io.BytesIO()
        pq.write_table(pa.table({"trace_id": ["a"]}), buf)
        payload = buf.getvalue()

        keys = [f"traces/provisa/year=2026/month=08/day=11/hour=03/{i}.parquet" for i in range(10)]
        fetched: list[str] = []
        deleted: list[str] = []

        class _ReadableS3(_FakeS3):
            def get_object(self, Bucket, Key):  # noqa: N803
                fetched.append(Key)
                return {"Body": io.BytesIO(payload)}

            def delete_objects(self, Bucket, Delete):  # noqa: N803
                deleted.extend(o["Key"] for o in Delete["Objects"])
                return {}

        class _DescribingEngine(_FakeEngine):
            def execute_engine_sync(self, sql, params=None, **kw):
                super().execute_engine_sync(sql, params, **kw)
                from provisa.executor.result import QueryResult

                if "SHOW COLUMNS" in sql:
                    return QueryResult(rows=[("trace_id", "varchar")], column_names=[])
                return QueryResult(rows=[], column_names=[])

        monkeypatch.setattr(boto3, "client", lambda *a, **kw: _ReadableS3(keys, {}))
        jobs._compact_signal(
            "traces",
            datetime(2026, 8, 11),
            "http://minio:9000",
            "k",
            "s",
            "provisa-otel",
            50,
            3,
            _DescribingEngine(),
        )
        assert fetched == keys[:3]
        assert deleted == keys[:3]

    def test_another_days_partition_is_not_compacted(self, monkeypatch):
        key = "traces/provisa/year=2026/month=08/day=10/hour=03/a.parquet"
        recorder = self._run(monkeypatch, "traces", [key])
        assert "fetched" not in recorder


class _FakeEngine:
    """Records every statement the compactor sends, so commit count is observable."""

    def __init__(self):
        self.statements: list[str] = []

    def execute_engine_sync(self, sql, params=None, **kw):
        assert params is None, (
            "compaction inlines its literals; a parameterised statement is capped"
        )
        self.statements.append(sql)
        return None


class TestInsertCommitAmplification:
    """Each INSERT is one Iceberg commit, and each commit writes a metadata.json embedding the
    whole snapshot list. Five rows per statement made the SaaS node write 23857 metadata files —
    67 GiB — for 30 MiB of trace data, until the root filesystem filled and every commit failed
    with ICEBERG_COMMIT_ERROR."""

    def _cols(self):
        return ['"span_name"', '"_date"'], ["CAST(? AS VARCHAR)", "CAST(? AS DATE)"]

    def test_the_configured_batch_size_is_what_runs(self):
        from provisa.scheduler import jobs

        col_names, placeholders = self._cols()
        engine = _FakeEngine()
        rows = [(f"span-{i}", "2026-08-11") for i in range(1000)]
        jobs._execute_batch_inserts(engine, "traces", rows, col_names, placeholders, 500)
        assert len(engine.statements) == 2

    def test_no_signal_is_clamped_below_the_configured_size(self, monkeypatch):
        from provisa.api.app import state
        from provisa.scheduler import jobs

        monkeypatch.setattr(state, "otel_compact_batch_size", 1000, raising=False)
        assert jobs._resolve_batch_size("traces") == 1000
        assert jobs._resolve_batch_size("metrics") == 1000

    def test_a_quote_in_a_span_attribute_cannot_break_out_of_its_literal(self):
        from provisa.scheduler import jobs

        col_names, placeholders = self._cols()
        engine = _FakeEngine()
        jobs._execute_batch_inserts(
            engine,
            "traces",
            [("o'brien'); DROP TABLE x --", "2026-08-11")],
            col_names,
            placeholders,
            100,
        )
        assert "'o''brien''); DROP TABLE x --'" in engine.statements[0]
        assert engine.statements[0].count("INSERT") == 1

    def test_a_long_row_splits_the_statement_before_trinos_query_length_limit(self):
        from provisa.scheduler import jobs

        col_names, placeholders = self._cols()
        engine = _FakeEngine()
        rows = [("x" * 100_000, "2026-08-11") for _ in range(10)]
        jobs._execute_batch_inserts(engine, "traces", rows, col_names, placeholders, 1000)
        assert len(engine.statements) > 1
        assert max(len(s) for s in engine.statements) < 1_000_000
