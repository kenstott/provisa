# Copyright (c) 2026 Kenneth Stott
# Canary: 4c977ebd-647f-4304-a444-0e9dd8dc5873
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The ops signals (traces, metrics, logs) are one store every org's work lands in. Each span
records the org it was started for, the store keeps it as ``tenant_id``, and a read of the signals
answers only the reading org's rows: a span of no org is no org's to read."""

# Requirements: REQ-302, REQ-1349

from __future__ import annotations

import json
from contextlib import contextmanager
from datetime import datetime

import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from provisa.api.otel_setup import OrgSpanProcessor
from provisa.compiler.rls import ORG_SCOPED_SIGNALS_RULE, RLSContext, org_scoped_signals
from provisa.core.request_context import (
    ORG_VAR,
    current_org,
    reset_current_org,
    reset_session_vars,
    session_vars_for,
    set_current_org,
    set_session_vars,
)
from provisa.pgwire._pipeline import _resolve_session_settings
from provisa.scheduler.jobs import _TRACE_EXTRA_COLS, _build_row, _insert_otel_iceberg

pytestmark = pytest.mark.unit


@contextmanager
def _serving(org: str | None):
    """``org`` bound as the request's org, or none at all (a background path)."""
    token = current_org.set(org)
    try:
        yield
    finally:
        current_org.reset(token)


def _spans(org: str | None) -> tuple:
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(OrgSpanProcessor())
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    with _serving(org), provider.get_tracer("t").start_as_current_span("work"):
        pass
    return exporter.get_finished_spans()


def test_a_span_records_the_org_it_was_started_for():
    [span] = _spans("acme")
    assert span.attributes["provisa.tenant"] == "acme"


def test_a_span_started_for_no_org_records_none():
    [span] = _spans(None)
    assert "provisa.tenant" not in (span.attributes or {})


def test_the_store_keeps_a_spans_org_as_its_tenant():
    attrs = {"provisa.tenant": "acme", "provisa.table": "orders"}
    row = {"trace_id": "t1", "span_attributes": json.dumps(attrs)}
    engine_cols = {c: "varchar" for c in ("trace_id", *_TRACE_EXTRA_COLS)}
    built = _build_row(
        row, ["trace_id"], "2026-10-17", "span_attributes", _TRACE_EXTRA_COLS, engine_cols
    )
    by_col = dict(zip(["trace_id", "_date", *_TRACE_EXTRA_COLS], built))
    assert by_col["tenant_id"] == "acme"
    assert by_col["table_name"] == "orders"


def test_the_reading_org_is_a_session_value_no_caller_can_set():
    token = set_current_org("acme")
    request = set_session_vars({ORG_VAR: "other"})
    try:
        role = {"session_vars": {ORG_VAR: "another"}}
        assert session_vars_for(role)[ORG_VAR] == "acme"
    finally:
        reset_session_vars(request)
        reset_current_org(token)


def test_with_no_org_bound_there_is_no_reading_org():
    request = set_session_vars({ORG_VAR: "other"})
    try:
        with _serving(None):
            assert ORG_VAR not in session_vars_for({"session_vars": {ORG_VAR: "another"}})
    finally:
        reset_session_vars(request)


_TABLES = [
    {"id": 7, "source_id": "provisa-otel", "table_name": "traces"},
    {"id": 8, "source_id": "provisa-otel", "table_name": "metrics"},
    {"id": 9, "source_id": "provisa-otel", "table_name": "logs"},
    {"id": 1, "source_id": "provisa-admin", "table_name": "registered_tables"},
    {"id": 2, "source_id": "orders-db", "table_name": "orders"},
]


def test_every_signals_table_answers_only_the_reading_orgs_rows():
    scoped = org_scoped_signals(RLSContext.empty(), _TABLES)
    assert scoped.rules == {tid: ORG_SCOPED_SIGNALS_RULE for tid in (7, 8, 9)}


def test_a_rule_of_the_role_on_a_signals_table_still_holds_beside_the_orgs():
    rls = RLSContext(rules={7: "role_id = 'analyst'", 2: "region = 'EU'"})
    scoped = org_scoped_signals(rls, _TABLES)
    assert scoped.rules[7] == f"({ORG_SCOPED_SIGNALS_RULE}) AND (role_id = 'analyst')"
    assert scoped.rules[2] == "region = 'EU'"  # not a signals table: untouched
    assert rls.rules == {7: "role_id = 'analyst'", 2: "region = 'EU'"}  # input not mutated


def test_the_rule_reads_the_bound_org_and_no_org_reads_nothing():
    with _serving("acme"):
        bound = _resolve_session_settings(ORG_SCOPED_SIGNALS_RULE, session_vars_for(None), "trino")
    assert bound == "tenant_id = 'acme'"
    # Unbound (a background path): NULL, which no row's tenant equals -- not even a NULL one.
    with _serving(None):
        unbound = _resolve_session_settings(
            ORG_SCOPED_SIGNALS_RULE, session_vars_for(None), "trino"
        )
    assert unbound == "tenant_id = NULL"


def _finished_logs(org: str | None) -> list:
    """The records a log line written for ``org`` is exported as."""
    import logging

    from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
    from opentelemetry.sdk._logs.export import InMemoryLogRecordExporter, SimpleLogRecordProcessor

    from provisa.api.otel_setup import OrgLogFilter

    exporter = InMemoryLogRecordExporter()
    provider = LoggerProvider()
    provider.add_log_record_processor(SimpleLogRecordProcessor(exporter))
    handler = LoggingHandler(level=logging.WARNING, logger_provider=provider)
    handler.addFilter(OrgLogFilter())
    logger = logging.getLogger("provisa.test.org_logs")
    logger.addHandler(handler)
    try:
        with _serving(org):
            logger.warning("refresh failed")
    finally:
        logger.removeHandler(handler)
    return list(exporter.get_finished_logs())


def _log_records(org: str | None) -> list:
    return [finished.log_record for finished in _finished_logs(org)]


def test_a_log_record_records_the_org_it_was_written_for():
    [record] = _log_records("acme")
    assert record.attributes["provisa.tenant"] == "acme"


def test_a_log_record_written_for_no_org_records_none():
    [record] = _log_records(None)
    assert "provisa.tenant" not in (record.attributes or {})


def test_a_query_metric_records_the_org_it_ran_for(monkeypatch):
    from provisa import otel_compat

    recorded: list[dict] = []

    class _Instrument:
        def add(self, _value, attrs):
            recorded.append(attrs)

        def record(self, _value, attrs):
            recorded.append(attrs)

    monkeypatch.setattr(otel_compat, "_query_counter", _Instrument())
    monkeypatch.setattr(otel_compat, "_query_duration", _Instrument())
    with _serving("acme"):
        otel_compat.record_query(
            transport="http", route="direct", engine="trino", status_code=200, duration_ms=3.0
        )
    with _serving(None):
        otel_compat.record_query(
            transport="http", route="direct", engine="trino", status_code=200, duration_ms=3.0
        )
    assert [a.get("provisa.tenant") for a in recorded] == ["acme", "acme", None, None]


def test_otlp2sql_keeps_a_metric_points_and_a_log_records_org():
    from opentelemetry.exporter.otlp.proto.common._log_encoder import encode_logs
    from opentelemetry.exporter.otlp.proto.common.metrics_encoder import encode_metrics
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.metrics.export import InMemoryMetricReader

    from provisa.observability import otlp2sql

    reader = InMemoryMetricReader()
    meter = MeterProvider(metric_readers=[reader]).get_meter("provisa")
    meter.create_counter("provisa.query.executed").add(1, {"provisa.tenant": "acme"})
    [metric_row] = otlp2sql._metric_rows(encode_metrics(reader.get_metrics_data()))
    assert metric_row["tenant_id"] == "acme"

    [log_row] = otlp2sql._log_rows(encode_logs(_finished_logs("acme")))
    assert log_row["tenant_id"] == "acme"


class _CompactingEngine:
    """The Trino side of compaction: the signal table's columns, and every statement run."""

    dialect = "trino"

    def __init__(self, cols: dict[str, str]):
        self._cols = cols
        self.sql: list[str] = []

    def execute_engine_sync(self, sql, params=None, authorization=None):
        from provisa.executor.result import QueryResult

        self.sql.append(sql)
        rows = list(self._cols.items()) if "SHOW COLUMNS" in sql else []
        return QueryResult(rows=rows, column_names=[])


@pytest.mark.parametrize(
    ("signal", "attr_col"), [("metrics", "metric_attributes"), ("logs", "log_attributes")]
)
def test_the_store_keeps_a_metric_points_and_a_log_records_org_as_its_tenant(signal, attr_col):
    import pyarrow as pa

    from provisa.scheduler.jobs import _build_iceberg_col_defs

    table = pa.table(
        {
            "metric_name" if signal == "metrics" else "body": ["provisa.query.executed"],
            attr_col: [json.dumps({"provisa.tenant": "acme"})],
        }
    )
    assert '"tenant_id" VARCHAR' in _build_iceberg_col_defs(signal, table)
    engine = _CompactingEngine({n: "varchar" for n in (*table.schema.names, "tenant_id")})
    engine._cols["_date"] = "date"
    _insert_otel_iceberg(engine, signal, table, datetime(2026, 10, 18))
    [insert] = [s for s in engine.sql if s.startswith("INSERT")]
    assert '"tenant_id"' in insert
    assert "'acme'" in insert
