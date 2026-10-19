# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""REQ-2092: the local pass's per-stage figures. The trace receiver adds up the request spans'
stage time, thread CPU and entries per transport; a step reports them per request span."""

# Requirements: REQ-2092

from __future__ import annotations

import gzip
import sys
import threading
from collections.abc import Iterator
from http.server import ThreadingHTTPServer
from pathlib import Path

import httpx
import pytest
from opentelemetry.exporter.otlp.proto.common.trace_encoder import encode_spans
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

BENCH = Path(__file__).resolve().parents[3] / "demo" / "named" / "perf" / "bench"
sys.path.insert(0, str(BENCH))

import noop_otlp_receiver as receiver  # noqa: E402
import optimistic_load as ol  # noqa: E402

from provisa.otel_compat import request_span, stage  # noqa: E402


def _batch(n: int, transport: str) -> bytes:
    """``n`` requests' spans as Provisa exports them: a request span with its stages, and a
    background span that is no request."""
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    tracer = provider.get_tracer("t")
    for _ in range(n):
        with request_span(tracer, "req", transport=transport):
            for _ in range(2):
                with stage(tracer, "rls.inject", name="governance"):
                    pass
            with stage(tracer, "direct", name="execute"):
                pass
    with tracer.start_as_current_span("scheduler.tick"):
        pass
    return encode_spans(exporter.get_finished_spans()).SerializeToString()


@pytest.fixture
def tally_url() -> Iterator[str]:
    server = ThreadingHTTPServer(("127.0.0.1", 0), receiver.handler(receiver.StageTally()))
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}"
    finally:
        server.shutdown()
        server.server_close()


def test_the_receiver_adds_up_request_spans_and_their_stages(tally_url: str) -> None:
    assert httpx.post(f"{tally_url}/v1/traces", content=_batch(3, "graphql")).status_code == 200
    gz = gzip.compress(_batch(2, "graphql"))
    resp = httpx.post(f"{tally_url}/v1/traces", content=gz, headers={"Content-Encoding": "gzip"})
    assert resp.status_code == 200
    httpx.post(f"{tally_url}/v1/traces", content=_batch(1, "pgwire"))
    totals = httpx.get(f"{tally_url}/stages").json()
    assert totals["requests"] == {"graphql": 5, "pgwire": 1}  # the background span is no request
    governance = totals["stages"]["graphql"]["governance"]
    assert governance["count"] == 10  # entered twice in each of 5 requests
    # summed, not compared: at microseconds each is rounded on its own (test_trace_detail compares)
    assert governance["ms"] >= 0 and governance["cpu_ms"] >= 0
    assert totals["stages"]["graphql"]["execute"]["count"] == 5  # once each: no count attribute


def test_a_receiver_without_tally_answers_no_stages() -> None:
    server = ThreadingHTTPServer(("127.0.0.1", 0), receiver.handler(None))
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    try:
        url = f"http://127.0.0.1:{server.server_address[1]}"
        assert httpx.post(f"{url}/v1/traces", content=_batch(1, "graphql")).status_code == 200
        assert httpx.get(f"{url}/stages").status_code == 404
    finally:
        server.shutdown()
        server.server_close()


def test_a_step_reports_each_stage_per_request_span() -> None:
    before = {
        "requests": {"graphql": 10, "pgwire": 4},
        "stages": {
            "graphql": {"governance": {"ms": 20.0, "cpu_ms": 10.0, "count": 10}},
            "pgwire": {"execute": {"ms": 8.0, "cpu_ms": 2.0, "count": 4}},
        },
    }
    after = {
        "requests": {"graphql": 110, "pgwire": 4, "mcp": 2},
        "stages": {
            "graphql": {
                "governance": {"ms": 220.0, "cpu_ms": 160.0, "count": 210},
                "encode": {"ms": 50.0, "cpu_ms": 40.0, "count": 100},
            },
            "pgwire": {"execute": {"ms": 8.0, "cpu_ms": 2.0, "count": 4}},
            "mcp": {"execute": {"ms": 3.0, "cpu_ms": 1.0, "count": 2}},
        },
    }
    assert ol.stage_delta(before, after) == {
        "graphql": {
            "request_spans": 100,
            "stages": {
                "governance": {
                    "ms_per_request": 2.0,
                    "cpu_ms_per_request": 1.5,
                    "count_per_request": 2.0,
                },
                # first entered in this step
                "encode": {
                    "ms_per_request": 0.5,
                    "cpu_ms_per_request": 0.4,
                    "count_per_request": 1.0,
                },
            },
        },
        "mcp": {
            "request_spans": 2,
            "stages": {
                "execute": {
                    "ms_per_request": 1.5,
                    "cpu_ms_per_request": 0.5,
                    "count_per_request": 1.0,
                }
            },
        },
    }
