#!/usr/bin/env python3
# Copyright (c) 2026 Kenneth Stott
# Canary: 92017e19-db9e-4733-86d8-f7150001f5d1
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""A no-op OTLP/HTTP receiver for benchmark runs.

In production the trace collector and the trace database run on other machines; on the single
benchmark VM they would share the 16 vCPUs with the server under test. This stands in for the
collector: it accepts OTLP traces, metrics and logs over HTTP (the protocol Provisa exports with,
``OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf``), reads and discards each body, and answers success.
Provisa's exporters still serialize and send every batch, so the server pays what it would pay in
production; nothing downstream of the wire runs here.

Usage: noop_otlp_receiver.py [port] [--tally]   (default 4319, the port --demo points Provisa at)

``--tally`` (REQ-2092, the local pass): the receiver also decodes each trace batch and adds up, per
transport, the request spans it has seen and their per-stage figures (``stage.<name>.ms``,
``.cpu_ms``, ``.count``). ``GET /stages`` answers the totals so far; the benchmark reads them at
both ends of a step and divides by the request spans between. Decoding costs the receiver CPU, so
a run that measures the machine as a whole leaves it off.
"""

from __future__ import annotations

import argparse
import gzip
import json
import threading
from collections.abc import Iterable, Mapping
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

_PATHS = ("/v1/traces", "/v1/metrics", "/v1/logs")
_STAGE_FIGURES = ("ms", "cpu_ms", "count")


class StageTally:
    """Request spans and their stage figures, per transport, added up across batches."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._requests: dict[str, int] = {}
        self._stages: dict[str, dict[str, dict[str, float]]] = {}

    def add_span(self, attrs: Mapping[str, Any]) -> None:
        transport = attrs.get("provisa.transport")
        if transport is None:
            return  # not a request span
        with self._lock:
            self._requests[transport] = self._requests.get(transport, 0) + 1
            by_stage = self._stages.setdefault(transport, {})
            for key, value in attrs.items():
                parts = key.split(".")
                if len(parts) != 3 or parts[0] != "stage" or parts[2] != "ms":
                    continue
                name = parts[1]
                figures = by_stage.setdefault(name, dict.fromkeys(_STAGE_FIGURES, 0.0))
                figures["ms"] += value
                figures["cpu_ms"] += attrs[f"stage.{name}.cpu_ms"]
                # a stage entered once has no count attribute (otel_compat._add_stage_time)
                figures["count"] += attrs.get(f"stage.{name}.count", 1)

    def add_batch(self, body: bytes) -> None:
        from opentelemetry.proto.collector.trace.v1 import trace_service_pb2

        request = trace_service_pb2.ExportTraceServiceRequest()
        request.ParseFromString(body)
        for resource_spans in request.resource_spans:
            for scope_spans in resource_spans.scope_spans:
                for span in scope_spans.spans:
                    self.add_span(_attributes(span.attributes))

    def totals(self) -> dict[str, Any]:
        with self._lock:
            return {
                "requests": dict(self._requests),
                "stages": {t: {k: dict(v) for k, v in s.items()} for t, s in self._stages.items()},
            }


def _attributes(kvs: Iterable[Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for kv in kvs:
        kind = kv.value.WhichOneof("value")
        if kind in ("string_value", "int_value", "double_value", "bool_value"):
            out[kv.key] = getattr(kv.value, kind)
    return out


def handler(tally: StageTally | None) -> type[BaseHTTPRequestHandler]:
    class _Discard(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"  # keep-alive: exporters reuse one connection

        def do_GET(self) -> None:  # noqa: N802 - http.server's handler naming
            if tally is None or self.path != "/stages":
                self.send_response(404)
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            body = json.dumps(tally.totals()).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_POST(self) -> None:  # noqa: N802 - http.server's handler naming
            _reply(self, tally)

        def log_message(self, format: str, *args: object) -> None:  # noqa: A002 - base signature
            """Silent: a per-request log line would be the receiver's main cost."""

    return _Discard


def _reply(req: BaseHTTPRequestHandler, tally: StageTally | None) -> None:
    length = int(req.headers.get("Content-Length") or 0)
    body = req.rfile.read(length) if length else b""
    if tally is not None and req.path == "/v1/traces" and body:
        if req.headers.get("Content-Encoding") == "gzip":
            body = gzip.decompress(body)
        tally.add_batch(body)
    if req.path not in _PATHS:
        req.send_response(404)
        req.send_header("Content-Length", "0")
        req.end_headers()
        return
    # An empty Export*ServiceResponse: zero bytes of protobuf means full success.
    req.send_response(200)
    req.send_header("Content-Type", "application/x-protobuf")
    req.send_header("Content-Length", "0")
    req.end_headers()


def main() -> None:
    parser = argparse.ArgumentParser(description="A no-op OTLP/HTTP receiver")
    parser.add_argument("port", nargs="?", type=int, default=4319)
    parser.add_argument("--tally", action="store_true", help="add up request spans' stages")
    args = parser.parse_args()
    tally = StageTally() if args.tally else None
    server = ThreadingHTTPServer(("127.0.0.1", args.port), handler(tally))
    mode = " (tallying stages: GET /stages)" if tally else ""
    print(f"noop OTLP receiver listening on 127.0.0.1:{args.port}{mode}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
