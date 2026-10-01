# Copyright (c) 2026 Kenneth Stott
# Canary: 6b0e2f4a-91c3-4d57-8a1e-3f7c5d9b2e64
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An OTLP/HTTP receiver that runs inside the test process.

A server under test is pointed at it with ``OTEL_EXPORTER_OTLP_ENDPOINT`` and
``OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf``; every span and metric data point the server
exports is decoded and kept, so a test can count exactly what one request put on the wire.
"""

# Requirements: REQ-1910

from __future__ import annotations

import gzip
import threading
import time
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from opentelemetry.proto.collector.metrics.v1.metrics_service_pb2 import (
    ExportMetricsServiceRequest,
)
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest


def _any_value(v: Any) -> Any:
    kind = v.WhichOneof("value")
    return getattr(v, kind) if kind else None


@dataclass
class ReceivedSpan:
    name: str
    trace_id: str
    span_id: str
    parent_span_id: str
    scope: str
    start_ns: int
    end_ns: int
    attrs: dict[str, Any]


@dataclass
class ReceivedPoint:
    """One exported data point of one metric."""

    metric: str
    attrs: dict[str, Any]
    count: int  # histogram count, or the sum's value for a counter


@dataclass
class OtlpReceiver:
    port: int
    spans: list[ReceivedSpan] = field(default_factory=list)
    points: list[ReceivedPoint] = field(default_factory=list)
    _lock: threading.Lock = field(default_factory=threading.Lock)
    _httpd: ThreadingHTTPServer | None = None

    @property
    def endpoint(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def start(self) -> None:
        receiver = self

        class _Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:  # noqa: N802 (http.server's name)
                body = self.rfile.read(int(self.headers["Content-Length"]))
                if self.headers.get("Content-Encoding") == "gzip":
                    body = gzip.decompress(body)
                if self.path == "/v1/traces":
                    receiver._take_traces(body)
                elif self.path == "/v1/metrics":
                    receiver._take_metrics(body)
                self.send_response(200)
                self.send_header("Content-Type", "application/x-protobuf")
                self.send_header("Content-Length", "0")
                self.end_headers()

            def log_message(self, format: str, *args: Any) -> None:  # noqa: A002, ARG002
                pass

        self._httpd = ThreadingHTTPServer(("127.0.0.1", self.port), _Handler)
        threading.Thread(target=self._httpd.serve_forever, daemon=True).start()

    def stop(self) -> None:
        if self._httpd is not None:
            self._httpd.shutdown()
            self._httpd.server_close()

    def _take_traces(self, body: bytes) -> None:
        req = ExportTraceServiceRequest()
        req.ParseFromString(body)
        got = [
            ReceivedSpan(
                name=sp.name,
                trace_id=sp.trace_id.hex(),
                span_id=sp.span_id.hex(),
                parent_span_id=sp.parent_span_id.hex(),
                scope=ss.scope.name,
                start_ns=sp.start_time_unix_nano,
                end_ns=sp.end_time_unix_nano,
                attrs={a.key: _any_value(a.value) for a in sp.attributes},
            )
            for rs in req.resource_spans
            for ss in rs.scope_spans
            for sp in ss.spans
        ]
        with self._lock:
            self.spans.extend(got)

    def _take_metrics(self, body: bytes) -> None:
        req = ExportMetricsServiceRequest()
        req.ParseFromString(body)
        got: list[ReceivedPoint] = []
        for rm in req.resource_metrics:
            for sm in rm.scope_metrics:
                for m in sm.metrics:
                    kind = m.WhichOneof("data")
                    assert kind is not None, f"metric {m.name} carries no data"
                    for dp in getattr(m, kind).data_points:
                        attrs = {a.key: _any_value(a.value) for a in dp.attributes}
                        if kind == "histogram":
                            count = dp.count
                        else:
                            count = int(getattr(dp, dp.WhichOneof("value")))
                        got.append(ReceivedPoint(metric=m.name, attrs=attrs, count=count))
        with self._lock:
            self.points.extend(got)

    def snapshot(self) -> list[ReceivedSpan]:
        with self._lock:
            return list(self.spans)

    def metric_points(self) -> list[ReceivedPoint]:
        with self._lock:
            return list(self.points)

    def settle(self, quiet: float = 1.5, timeout: float = 30.0) -> list[ReceivedSpan]:
        """Every span received once none has arrived for ``quiet`` seconds."""
        deadline = time.monotonic() + timeout
        seen = len(self.snapshot())
        last_change = time.monotonic()
        while time.monotonic() < deadline:
            time.sleep(0.1)
            now = len(self.snapshot())
            if now != seen:
                seen, last_change = now, time.monotonic()
            elif time.monotonic() - last_change >= quiet:
                return self.snapshot()
        latest = [f"{s.name} [{s.scope}]" for s in self.snapshot()[-8:]]
        raise TimeoutError(f"spans were still arriving after {timeout}s; the latest were {latest}")
