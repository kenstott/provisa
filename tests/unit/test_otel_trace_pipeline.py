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

    def test_the_scheduled_run_drains_earlier_dates_oldest_first(self, monkeypatch):
        """The scheduled job pins no date. Listing only today's partition was an unbounded leak:
        once a file's date segment rolled over it was never listed again, so it was never inserted
        and never deleted — 70586 such files (15 GiB under traces/trino/) accumulated on the SaaS
        node and filled the coordinator's disk. Oldest first, so the backlog drains from the tail
        instead of being permanently outrun by the current partition."""
        import boto3

        from provisa.scheduler import jobs

        old = "traces/provisa/year=2026/month=08/day=09/hour=03/a.parquet"
        new = "traces/provisa/year=2026/month=08/day=11/hour=03/b.parquet"
        recorder: dict = {}
        monkeypatch.setattr(boto3, "client", lambda *a, **kw: _FakeS3([new, old], recorder))
        jobs._compact_signal(
            "traces", None, "http://minio:9000", "k", "s", "provisa-otel", 50, 50, None
        )
        assert recorder["fetched"] == [old]  # raised before reaching the newer date

    def test_each_batch_lands_in_its_own_keys_date_partition(self, monkeypatch):
        """Draining a backlog must not misfile an earlier day's spans under today — each batch
        carries its own key's date into the `_date` partition."""
        pytest.importorskip("pyarrow")
        import io

        import boto3
        import pyarrow as pa
        import pyarrow.parquet as pq

        from provisa.scheduler import jobs

        buf = io.BytesIO()
        pq.write_table(pa.table({"trace_id": ["a"]}), buf)
        payload = buf.getvalue()
        keys = [
            "traces/provisa/year=2026/month=08/day=09/hour=03/a.parquet",
            "traces/provisa/year=2026/month=08/day=11/hour=03/b.parquet",
        ]

        class _ReadableS3(_FakeS3):
            def get_object(self, Bucket, Key):  # noqa: N803
                return {"Body": io.BytesIO(payload)}

            def delete_objects(self, Bucket, Delete):  # noqa: N803
                return {}

        dates: list = []
        monkeypatch.setattr(boto3, "client", lambda *a, **kw: _ReadableS3(keys, {}))
        monkeypatch.setattr(
            jobs,
            "_insert_otel_iceberg",
            lambda engine, signal, table, date_val: dates.append(date_val),
        )
        jobs._compact_signal(
            "traces", None, "http://minio:9000", "k", "s", "provisa-otel", 50, 50, None
        )
        assert dates == [datetime(2026, 8, 9), datetime(2026, 8, 11)]


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


class TestPipelineSpanAttributes:
    """The ops `queries` report reads spans named provisa.query.* whose provisa.* attributes
    TRACE_ATTR_COLS lifts into the trace table. The ONE pipeline mints those attributes and the
    ENGINE terminal hands them to the engine — without that, every span is anonymous and the
    report is empty no matter how much traffic ran."""

    def test_attrs_name_the_root_table_domain_and_role(self):
        from provisa.observability.span_attrs import span_attrs_from_semantic_sql

        attrs = span_attrs_from_semantic_sql(
            "SELECT * FROM pet_store.pets WHERE status = 'available'", "analyst", "raw text"
        )
        assert attrs["provisa.table"] == "pet_store.pets"
        assert attrs["provisa.domain"] == "pet_store"
        assert attrs["provisa.role"] == "analyst"
        assert attrs["provisa.query_text"] == "raw text"

    def test_a_table_less_statement_is_labelled_by_its_surface(self):
        from provisa.observability.span_attrs import span_attrs_from_semantic_sql

        attrs = span_attrs_from_semantic_sql("SELECT 1", "analyst", no_table_label="sql")
        assert attrs["provisa.table"] == "sql"
        assert "provisa.query_text" not in attrs

    def test_no_attrs_are_minted_when_no_principal_is_acting(self):
        from provisa.pgwire._pipeline import _plan_span_attrs

        assert _plan_span_attrs("SELECT * FROM pet_store.pets", "analyst", "q", None) is None

    async def test_the_engine_terminal_forwards_the_plans_attrs(self):
        from provisa.executor.result import QueryResult
        from provisa.pgwire import _pipeline
        from provisa.pgwire._pipeline import _Plan, _mint_stamp
        from provisa.transpiler.router import Route

        seen: dict = {}

        class _Engine:
            async def execute_engine(self, sql, params=None, session_hints=None, span_attrs=None):
                seen["span_attrs"] = span_attrs
                return QueryResult(rows=[(1,)], column_names=["n"])

        class _State:
            federation_engine = _Engine()

        attrs = {"provisa.table": "pet_store.pets", "provisa.role": "analyst"}
        plan = _Plan(
            route=Route.ENGINE,
            sql="SELECT 1",
            source_id="pg",
            dialect="trino",
            physical_sql="SELECT 1",
            span_attrs=attrs,
            stamp=_mint_stamp(),
        )
        await _pipeline._execute_plan(plan, state=_State())
        assert seen["span_attrs"] == attrs


class TestOtelStorageReclamation:
    """Expiring snapshots alone does NOT free space — it only unlinks them; the files an expired
    snapshot referenced become unreferenced, and ONLY remove_orphan_files deletes those. Nothing
    ran it, so the SaaS node reached 57 GiB of metadata behind 93 MiB of data and filled the
    coordinator's disk. Trino floors both retentions at 7 days and REJECTS a shorter threshold
    outright, so the configured value only applies if the catalog session properties are set with
    the statement."""

    async def _run(self, monkeypatch, retention):
        from provisa.api import app as app_module
        from provisa.scheduler import jobs

        calls: list[tuple[str, dict]] = []

        class _Engine:
            def execute_engine_sync(self, sql, params=None, *, session_hints=None):
                calls.append((sql, session_hints))

        monkeypatch.setattr(app_module.state, "otel_snapshot_retention_hours", retention)
        monkeypatch.setattr(app_module.state, "federation_engine", _Engine())
        await jobs.reclaim_otel_storage()
        return calls

    async def test_orphan_files_are_removed_not_just_snapshots_expired(self, monkeypatch):
        calls = await self._run(monkeypatch, 1)
        for signal in ("logs", "metrics", "traces"):
            assert any(
                f"otel.signals.{signal} EXECUTE remove_orphan_files" in sql for sql, _ in calls
            )
            assert any(f"otel.signals.{signal} EXECUTE expire_snapshots" in sql for sql, _ in calls)

    async def test_the_configured_retention_overrides_trinos_seven_day_floor(self, monkeypatch):
        calls = await self._run(monkeypatch, 1)
        for sql, hints in calls:
            assert "retention_threshold => '1h'" in sql
            proc = "expire_snapshots" if "expire_snapshots" in sql else "remove_orphan_files"
            assert hints == {f"otel.{proc}_min_retention": "1h"}

    async def test_no_configured_retention_reclaims_nothing(self, monkeypatch):
        assert await self._run(monkeypatch, None) == []


# ---------------------------------------------------------------------------
# DIRECT + admin terminals — span coverage for the ops `queries` report
# ---------------------------------------------------------------------------


@_skip_otel
class TestNonEngineTerminalsAreReported:
    """The ops `queries` report selects spans named provisa.query.*. Only the ENGINE terminal
    emitted one, so a single-source (pushed-down) statement and every meta/ops statement executed
    without ever appearing in the report — the report read zero rows on a node that had served
    hundreds of audited queries."""

    class _Pool:
        def has(self, source_id):
            return True

        async def execute(self, source_id, sql, params):
            from provisa.executor.result import QueryResult

            return QueryResult(rows=[(1,)], column_names=["id"])

    _ATTRS = {"provisa.table": "pets", "provisa.domain": "pet-store", "provisa.role": "analyst"}

    async def test_direct_terminal_emits_an_attributed_query_span(self, otel_spans):
        from provisa.executor.direct import execute_direct

        await execute_direct(self._Pool(), "pg", "SELECT 1", None, self._ATTRS)
        target = next(
            (s for s in otel_spans.get_finished_spans() if s.name == "provisa.query.direct"), None
        )
        assert target is not None, [s.name for s in otel_spans.get_finished_spans()]
        assert target.attributes.get("provisa.table") == "pets"
        assert target.attributes.get("provisa.role") == "analyst"

    async def test_unattributed_direct_execution_keeps_the_plain_span_name(self, otel_spans):
        from provisa.executor.direct import execute_direct

        await execute_direct(self._Pool(), "pg", "SELECT 1")
        names = [s.name for s in otel_spans.get_finished_spans()]
        assert "direct.execute" in names
        assert "provisa.query.direct" not in names

    async def test_engine_runtime_forwards_span_attrs_to_the_native_terminal(self, otel_spans):
        from provisa.federation.runtime import EngineRuntime

        captured: dict = {}

        async def _fake(pool, source_id, sql, params=None, span_attrs=None):
            captured["span_attrs"] = span_attrs

        runtime = EngineRuntime.__new__(EngineRuntime)
        runtime.execute_native = _fake  # type: ignore[method-assign]
        from provisa.transpiler.router import Route

        class _Decision:
            route = Route.DIRECT
            source_id = "pg"

        await runtime.execute(
            _Decision(), "SELECT 1", None, source_pools=self._Pool(), span_attrs=self._ATTRS
        )
        assert captured["span_attrs"] == self._ATTRS

    def test_the_direct_plan_carries_the_span_attributes(self):
        """Both planners' DIRECT branches must set span_attrs; only their ENGINE branches did."""
        import inspect

        from provisa.pgwire import _pipeline

        for fn in (_pipeline._govern_and_route, _pipeline._govern_and_route_compiled):
            src = inspect.getsource(fn)
            assert src.count("span_attrs=_plan_span_attrs(") == 2, fn.__name__

    async def test_admin_terminal_emits_an_attributed_query_span(self, otel_spans):
        from contextlib import asynccontextmanager

        from provisa.pgwire._pipeline import _Plan, _run_plan_terminal
        from provisa.transpiler.router import Route

        class _Row(dict):
            def keys(self):
                return ["n"]

            def __iter__(self):
                return iter([1])

        class _Conn:
            async def fetch(self, sql):
                return [_Row()]

        class _TenantDB:
            @asynccontextmanager
            async def acquire(self):
                yield _Conn()

        class _Pools:
            def has(self, source_id):
                return False

        class _State:
            tenant_db = _TenantDB()
            source_pools = _Pools()
            source_types: dict = {}
            federation_engine = None

        plan = _Plan(
            route=Route.DIRECT,
            sql="SELECT 1 AS n",
            source_id="provisa-admin",
            dialect="postgres",
            span_attrs=self._ATTRS,
        )
        await _run_plan_terminal(plan, _State())
        target = next(
            (s for s in otel_spans.get_finished_spans() if s.name == "provisa.query.postgres"), None
        )
        assert target is not None, [s.name for s in otel_spans.get_finished_spans()]
        assert target.attributes.get("provisa.table") == "pets"


def test_the_shipped_compaction_batch_is_not_the_parameterised_era_value():
    """REQ-1428: a deployment that configures nothing must still drain faster than it ingests.

    Every batch is one Iceberg commit, and a commit costs about four seconds whatever it carries —
    so batch size, not row count, sets the job's throughput. OtelConfig still defaulted to
    ten rows, the ceiling the Trino client's parameterised-statement header once imposed, while
    AppState's own default said a thousand; app startup applied the config, so the node committed
    ten spans at a time, drained roughly a hundred rows a minute against an ingest rate many times
    that, and admin / reports / queries read an empty table while queries were executing.
    """
    from provisa.api.app import AppState
    from provisa.core.models import OtelConfig

    assert OtelConfig().compact_batch_size == AppState().otel_compact_batch_size
    assert OtelConfig().compact_batch_size >= 1000


def test_pre_filter_backlog_objects_do_not_carry_asyncpg_rows_into_iceberg():
    """REQ-1428: the collector filter protects new objects; the bucket still holds the old ones.

    When the asyncpg drop shipped, 21 000 trace objects were already in R2 in which control-plane
    statement spans were most of the rows. Compaction walks oldest-date-first, so every one of
    those rows would be committed to Iceberg — at seconds per commit — before the job reached the
    provisa.query spans the ops report reads, and the report stayed empty while queries ran. The
    same predicate therefore applies at compaction, on rows already read.
    """
    import pyarrow as pa

    from provisa.scheduler import jobs

    table = pa.table(
        {
            "scope_name": pa.array(
                [
                    "opentelemetry.instrumentation.asyncpg",
                    "provisa.executor.trino",
                    "opentelemetry.instrumentation.asyncpg",
                ]
            ),
            "span_name": pa.array(["BEGIN;", "provisa.query.trino", "COMMIT;"]),
        }
    )
    kept = jobs._drop_foreign_rows("traces", table)
    assert kept.column("span_name").to_pylist() == ["provisa.query.trino"]

    # Logs and metrics carry no span scope to judge; they pass through untouched.
    assert jobs._drop_foreign_rows("logs", table).num_rows == 3


def test_compaction_drops_spans_from_other_services():
    """REQ-1425: the Trino coordinator's own spans never enter the Iceberg lane.

    The javaagent emits a span per stage, task, split and internal HTTP call. On the cloud node
    those reached 3.57M of the 3.58M rows in otel.signals.traces against 59 provisa.query spans,
    so the queries report's 51-row page scanned all of them and timed out. The collector filters
    them by service name; objects written before that filter shipped are still in the bucket, so
    compaction applies the same predicate to rows it has already read.
    """
    import pyarrow as pa

    from provisa.scheduler import jobs

    table = pa.table(
        {
            "service_name": pa.array(["trino", "provisa", "trino", None]),
            "span_name": pa.array(["split", "provisa.query.trino", "process", "orphan"]),
        }
    )
    kept = jobs._drop_foreign_rows("traces", table)
    assert kept.column("span_name").to_pylist() == ["provisa.query.trino"]


def test_compaction_reinterprets_epoch_integer_columns_as_instants():
    """REQ-1435: otlp2parquet writes some instants as an integer epoch count.

    Left as integers, the Iceberg column came out BIGINT and every ops report rendered the instant
    as a long digit string — undatable by a reader, unsortable as time by the grid. The cast is
    done in two steps: reinterpret in the unit the writer used, then rescale to the microseconds
    the lane's stores keep.
    """
    import datetime as _dt

    import pyarrow as pa

    from provisa.scheduler import jobs

    table = pa.table(
        {
            # traces.timestamp already arrives as an arrow timestamp and is left alone
            "timestamp": pa.array([_dt.datetime(2024, 7, 3, 9, 46, 40)], type=pa.timestamp("us")),
            # traces.end_timestamp arrives as epoch MILLISECONDS
            "end_timestamp": pa.array([1_720_000_000_050], type=pa.int64()),
            "duration": pa.array([50], type=pa.int64()),
            "span_name": pa.array(["provisa.query.trino"]),
        }
    )
    out = jobs._instants_from_epoch_ints("traces", table)

    assert out.schema.field("timestamp").type == pa.timestamp("us")
    assert out.schema.field("end_timestamp").type == pa.timestamp("us")
    assert out.column("end_timestamp").to_pylist() == [_dt.datetime(2024, 7, 3, 9, 46, 40, 50_000)]
    # duration is an elapsed count, not an instant — it stays an integer
    assert out.schema.field("duration").type == pa.int64()
    assert out.schema.field("span_name").type == pa.string()


def test_compaction_reads_each_signals_integer_instant_in_its_own_unit():
    """REQ-1435: the writer does not use one epoch unit across the signals.

    logs.observed_timestamp is microseconds and metrics.start_timestamp is milliseconds; reading
    either in the other's unit lands the row decades away from the span it belongs to.
    """
    import datetime as _dt

    import pyarrow as pa

    from provisa.scheduler import jobs

    logs = pa.table({"observed_timestamp": pa.array([1_720_000_000_000_050], type=pa.int64())})
    assert jobs._instants_from_epoch_ints("logs", logs).column(
        "observed_timestamp"
    ).to_pylist() == [_dt.datetime(2024, 7, 3, 9, 46, 40, 50)]

    metrics = pa.table({"start_timestamp": pa.array([1_720_000_000_050], type=pa.int64())})
    assert jobs._instants_from_epoch_ints("metrics", metrics).column(
        "start_timestamp"
    ).to_pylist() == [_dt.datetime(2024, 7, 3, 9, 46, 40, 50_000)]


def test_compaction_refuses_an_unmapped_integer_instant_column():
    """An integer instant with no mapped unit is a writer schema change, not something to guess.

    Picking a unit by feel is how end_timestamp landed in 1970 for every span on the node.
    """
    import pyarrow as pa
    import pytest

    from provisa.scheduler import jobs

    table = pa.table({"start_timestamp": pa.array([1_720_000_000_050], type=pa.int64())})
    with pytest.raises(ValueError, match="no epoch unit mapped"):
        jobs._instants_from_epoch_ints("traces", table)


def test_compaction_refuses_to_narrow_an_instant_back_to_an_integer():
    """A table created before REQ-1435 still has BIGINT instant columns.

    Casting the arrow timestamp to int64 to fit it would write microseconds into a column whose
    rows are nanoseconds — silently wrong data. The table has to be recreated, so the compactor
    says so rather than committing the bad value.
    """
    import pyarrow as pa
    import pytest

    from provisa.scheduler import jobs

    table = pa.table({"timestamp": pa.array([0], type=pa.timestamp("us"))})
    with pytest.raises(ValueError, match="drop the table"):
        jobs._cast_table_to_physical_schema("traces", table, {"timestamp": "bigint"})
