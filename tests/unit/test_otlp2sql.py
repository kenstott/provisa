# Copyright (c) 2026 Kenneth Stott
# Canary: f2b2057c-c6c9-4217-a9e2-04bbde8534aa
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""otlp2sql: OTLP/HTTP spans land as ops-schema rows with inline attribute
extraction (no compaction), against any SQLAlchemy URL."""

import sqlalchemy as sa
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest
from opentelemetry.proto.common.v1.common_pb2 import AnyValue, KeyValue
from opentelemetry.proto.trace.v1.trace_pb2 import ResourceSpans, ScopeSpans, Span, Status
from starlette.testclient import TestClient

from provisa.observability import otlp2sql
from provisa.observability.ops_schema import OPS_TABLES, ops_db_url


def _kv(k, v):
    return KeyValue(key=k, value=AnyValue(string_value=v))


def _trace_request() -> ExportTraceServiceRequest:
    span = Span(
        trace_id=bytes.fromhex("0af7651916cd43dd8448eb211c80319c"),
        span_id=bytes.fromhex("b7ad6b7169203331"),
        name="query.execute",
        kind=Span.SPAN_KIND_SERVER,
        start_time_unix_nano=1_720_000_000_000_000_000,
        end_time_unix_nano=1_720_000_000_050_000_000,
        status=Status(code=Status.STATUS_CODE_OK),
        attributes=[
            _kv("provisa.table", "customers"),
            _kv("provisa.domain", "sales"),
            _kv("provisa.role", "analyst"),
            _kv("provisa.query_text", "SELECT 1"),
        ],
    )
    return ExportTraceServiceRequest(
        resource_spans=[ResourceSpans(scope_spans=[ScopeSpans(spans=[span])])]
    )


def test_otlp_trace_lands_as_ops_row(tmp_path):
    url = f"sqlite:///{tmp_path / 'ops.sqlite'}"
    app = otlp2sql.build_app(url)
    with TestClient(app) as client:
        resp = client.post(
            "/v1/traces",
            content=_trace_request().SerializeToString(),
            headers={"content-type": "application/x-protobuf"},
        )
    assert resp.status_code == 200

    eng = sa.create_engine(url)
    with eng.connect() as cx:
        row = (
            cx.execute(
                sa.text(
                    "SELECT trace_id, span_name, table_name, domain_id, role_id, "
                    "query_text, timestamp, duration, _date FROM traces"
                )
            )
            .mappings()
            .one()
        )

    # inline extraction == what the old Trino compaction produced
    assert row["trace_id"] == "0af7651916cd43dd8448eb211c80319c"
    assert row["span_name"] == "query.execute"
    assert row["table_name"] == "customers"
    assert row["domain_id"] == "sales"
    assert row["role_id"] == "analyst"
    assert row["query_text"] == "SELECT 1"
    assert row["timestamp"] == 1_720_000_000_000
    assert row["duration"] == 50
    assert str(row["_date"]) == "2024-07-03"


def test_ops_db_url_is_dedicated_telemetry_store(monkeypatch, tmp_path):
    # Telemetry gets its OWN store — a dedicated DuckDB, never the control plane.
    monkeypatch.delenv("PROVISA_OPS_DB_URL", raising=False)
    monkeypatch.setenv("PROVISA_TELEMETRY_DIR", str(tmp_path))
    url = ops_db_url()
    assert url.startswith("duckdb:///")
    assert url.endswith("telemetry.duckdb")
    # explicit override (e.g. a warehouse) wins
    monkeypatch.setenv("PROVISA_OPS_DB_URL", "sqlite:///x.db")
    assert ops_db_url() == "sqlite:///x.db"


def test_schema_shared_with_app():
    # app.py imports this same object — one source of truth, no drift.
    assert set(OPS_TABLES) == {"traces", "metrics", "logs"}


def test_batching_buffers_then_flushes_on_size(monkeypatch, tmp_path):
    # Buffer until the size trigger; don't wait on the interval.
    monkeypatch.setenv("OTLP2SQL_BATCH_MAX_ROWS", "2")
    monkeypatch.setenv("OTLP2SQL_BATCH_MAX_SECS", "60")
    url = f"sqlite:///{tmp_path / 'batch.sqlite'}"
    app = otlp2sql.build_app(url)
    eng = sa.create_engine(url)

    def _post(client):
        client.post(
            "/v1/traces",
            content=_trace_request().SerializeToString(),
            headers={"content-type": "application/x-protobuf"},
        )

    def _count():
        with eng.connect() as cx:
            return cx.execute(sa.text("SELECT count(*) FROM traces")).scalar()

    with TestClient(app) as client:
        _post(client)
        assert _count() == 0  # buffered, interval not reached
        _post(client)
        assert _count() == 2  # size trigger flushed the batch
