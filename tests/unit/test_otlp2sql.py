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
    # REQ-1435: the instant is stored as an instant, not as its integer nanosecond encoding —
    # ops reports rendered the encoding as an undatable 19-digit number.
    assert str(row["timestamp"]) == "2024-07-03 09:46:40.000000"
    assert row["duration"] == 50_000_000  # elapsed nanoseconds, per the catalog description
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
    assert set(OPS_TABLES) == {"traces", "trace_details", "metrics", "logs"}


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


# -- REQ-1910: one traces row per request; every other span is a trace_details row ----------------

_TRACE = bytes.fromhex("0af7651916cd43dd8448eb211c80319c")


def _span(span_id: str, name: str, *, parent: str = "", attrs: dict | None = None, trace=_TRACE):
    return Span(
        trace_id=trace,
        span_id=bytes.fromhex(span_id),
        parent_span_id=bytes.fromhex(parent) if parent else b"",
        name=name,
        kind=Span.SPAN_KIND_INTERNAL,
        start_time_unix_nano=1_720_000_000_000_000_000,
        end_time_unix_nano=1_720_000_000_050_000_000,
        status=Status(code=Status.STATUS_CODE_OK),
        attributes=[_kv(k, v) for k, v in (attrs or {}).items()],
    )


def _ingest(tmp_path, spans) -> tuple[list[str], list[str]]:
    """Post ``spans`` in one export; the span names that landed in (traces, trace_details)."""
    url = f"sqlite:///{tmp_path / 'ops.sqlite'}"
    req = ExportTraceServiceRequest(
        resource_spans=[ResourceSpans(scope_spans=[ScopeSpans(spans=spans)])]
    )
    with TestClient(otlp2sql.build_app(url)) as client:
        resp = client.post(
            "/v1/traces",
            content=req.SerializeToString(),
            headers={"content-type": "application/x-protobuf"},
        )
        assert resp.status_code == 200
    with sa.create_engine(url).connect() as cx:
        requests = cx.execute(sa.text("SELECT span_name FROM traces ORDER BY span_name"))
        details = cx.execute(sa.text("SELECT span_name FROM trace_details ORDER BY span_name"))
        return [r[0] for r in requests], [r[0] for r in details]


_REQUEST = "aaaaaaaaaaaaaaa1"


def test_a_normal_request_is_one_traces_row_and_no_detail_rows(tmp_path):
    spans = [_span(_REQUEST, "POST /data/graphql", attrs={"provisa.transport": "graphql"})]
    assert _ingest(tmp_path, spans) == (["POST /data/graphql"], [])


def test_a_debug_request_of_n_spans_is_one_traces_row_and_n_minus_one_detail_rows(tmp_path):
    children = ["cache.get", "GET GET", "http receive", "http send", "compiler.compile_query"]
    spans = [_span(_REQUEST, "POST /data/graphql", attrs={"provisa.transport": "graphql"})] + [
        _span(f"bbbbbbbbbbbbbbb{i}", name, parent=_REQUEST) for i, name in enumerate(children)
    ]
    requests, details = _ingest(tmp_path, spans)
    assert requests == ["POST /data/graphql"]
    assert details == sorted(children)


def test_a_request_span_with_a_client_traceparent_is_still_the_request_record(tmp_path):
    """The caller sent a traceparent, so the request span has a parent — in another process."""
    spans = [
        _span(
            _REQUEST,
            "pgwire.query",
            parent="cccccccccccccccc",
            attrs={"provisa.transport": "pgwire"},
        ),
        _span("bbbbbbbbbbbbbbb1", "provisa.query.direct", parent=_REQUEST),
    ]
    assert _ingest(tmp_path, spans) == (["pgwire.query"], ["provisa.query.direct"])


def test_background_work_keeps_its_own_record(tmp_path):
    """A refresh or scheduler job is not a request: its root span is its record, and the spans
    under it are detail."""
    other = bytes.fromhex("1af7651916cd43dd8448eb211c80319c")
    spans = [
        _span("ddddddddddddddd1", "mv.refresh.column_lineage", trace=other),
        _span("ddddddddddddddd2", "duckdb.execute", parent="ddddddddddddddd1", trace=other),
    ]
    assert _ingest(tmp_path, spans) == (["mv.refresh.column_lineage"], ["duckdb.execute"])


def test_expired_detail_rows_are_pruned_and_request_rows_are_kept(tmp_path, monkeypatch):
    monkeypatch.setenv("OTLP2SQL_DETAIL_RETENTION_HOURS", "1")
    url = f"sqlite:///{tmp_path / 'ops.sqlite'}"
    old = [  # 2024-07-03: far past any retention
        _span(_REQUEST, "pgwire.query", attrs={"provisa.transport": "pgwire"}),
        _span("bbbbbbbbbbbbbbb1", "provisa.query.direct", parent=_REQUEST),
    ]
    req = ExportTraceServiceRequest(
        resource_spans=[ResourceSpans(scope_spans=[ScopeSpans(spans=old)])]
    )
    with TestClient(otlp2sql.build_app(url)) as client:
        client.post(
            "/v1/traces",
            content=req.SerializeToString(),
            headers={"content-type": "application/x-protobuf"},
        )
    # A second start prunes what the first one left behind.
    with TestClient(otlp2sql.build_app(url)):
        pass
    with sa.create_engine(url).connect() as cx:
        assert cx.execute(sa.text("SELECT count(*) FROM traces")).scalar() == 1
        assert cx.execute(sa.text("SELECT count(*) FROM trace_details")).scalar() == 0


# -- REQ-1910: the ops `queries` report reads the same statements off the request record ----------

# A fixed request mix: (request span name, transport, table, domain, role, engine span name).
_MIX = [
    ("POST /data/graphql", "graphql", "sales.orders", "sales", "analyst", "provisa.query.duckdb"),
    ("pgwire.query", "pgwire", "sales.customers", "sales", "org_admin", "provisa.query.direct"),
    ("grpc.rpc", "grpc", "hr.people", "hr", "analyst", "provisa.query.trino"),
    ("POST /data/sql", "sql", "sales.orders", "sales", "org_admin", "provisa.query.postgres"),
]
_REPORT_COLS = "table_name, domain_id, role_id, status_code"


def _mix_spans(*, all_spans: bool) -> list:
    """The mix as spans. ``all_spans``: the pre-REQ-1910 shape — a request span, a child span per
    Redis command and a child ``provisa.query.*`` span carrying the statement's attributes.
    Otherwise the normal-detail shape — the request span alone, carrying them itself."""
    spans = []
    for i, (name, transport, table, domain, role, engine_span) in enumerate(_MIX):
        trace = bytes.fromhex(f"{i:032x}".replace("0", "a", 1))
        request = f"{i + 1:016x}"
        statement = {"provisa.table": table, "provisa.domain": domain, "provisa.role": role}
        if all_spans:
            spans += [
                _span(request, name, trace=trace),
                _span(f"{i + 101:016x}", "GET GET", parent=request, trace=trace),
                _span(f"{i + 201:016x}", engine_span, parent=request, attrs=statement, trace=trace),
            ]
        else:
            spans.append(
                _span(
                    request, name, attrs={"provisa.transport": transport, **statement}, trace=trace
                )
            )
    # A request that read no registered table (a health check) is in neither report.
    spans.append(_span("00000000000000ff", "GET /health", trace=bytes.fromhex("f" * 32)))
    return spans


def _queries_view_select() -> str:
    """The SELECT of the seeded ``queries`` view, against this store's unqualified table."""
    from provisa.api.startup_seed import _OPS_VIEWS

    ddl = next(ddl for name, _cols, ddl in _OPS_VIEWS if name == "queries")
    return ddl.split(" AS\n", 1)[1].replace("otel.signals.traces", "traces")


def test_the_queries_report_is_the_same_from_request_records_as_from_all_spans(tmp_path):
    # Before: every span was a traces row and the view selected the provisa.query.* spans.
    before_url = f"sqlite:///{tmp_path / 'before.sqlite'}"
    eng = sa.create_engine(before_url)
    from provisa.observability.ops_schema import ensure_tables

    traces = ensure_tables(eng)["traces"]
    req = ExportTraceServiceRequest(
        resource_spans=[ResourceSpans(scope_spans=[ScopeSpans(spans=_mix_spans(all_spans=True))])]
    )
    with eng.begin() as cx:
        cx.execute(sa.insert(traces), otlp2sql._trace_rows(req))
        before = sorted(
            tuple(r)
            for r in cx.execute(
                sa.text(f"SELECT {_REPORT_COLS} FROM traces WHERE span_name LIKE 'provisa.query%'")
            )
        )

    # After: one request record per request, ingested by the collector, read by the seeded view.
    after_url = f"sqlite:///{tmp_path / 'after.sqlite'}"
    req = ExportTraceServiceRequest(
        resource_spans=[ResourceSpans(scope_spans=[ScopeSpans(spans=_mix_spans(all_spans=False))])]
    )
    with TestClient(otlp2sql.build_app(after_url)) as client:
        client.post(
            "/v1/traces",
            content=req.SerializeToString(),
            headers={"content-type": "application/x-protobuf"},
        )
    with sa.create_engine(after_url).connect() as cx:
        after = sorted(
            tuple(r)
            for r in cx.execute(
                sa.text(f"SELECT {_REPORT_COLS} FROM ({_queries_view_select()}) AS queries")
            )
        )
        rows_in_traces = cx.execute(sa.text("SELECT count(*) FROM traces")).scalar()

    assert len(before) == len(_MIX)
    assert after == before
    assert rows_in_traces == len(_MIX) + 1  # one per request, the health check included
