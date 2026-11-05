# Copyright (c) 2026 Kenneth Stott
# Canary: 196ae8b1-7e8c-4502-ae03-ba1eb5896bda
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A collector that cannot be reached is said once, in one line, and once more when it is back.

The metric export thread logged a traceback on every tick it could not export. One of those
was the exporter library's own: after a failed attempt it waits out its backoff and sends
again with whatever time is left; a late wake-up leaves none, and the HTTP client refuses a
timeout of zero or less with a ValueError ("Attempted to set connect timeout to
-0.5677816867828369", a maintainer's server log). Since opentelemetry 1.45 the library sends
that attempt with no time rather than less than none and counts the refusal as a failed
export, so it comes back as the exporter's failure result."""

from __future__ import annotations

import logging
from types import SimpleNamespace

import pytest
import requests
from opentelemetry.exporter.otlp.common import http as lib
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.metrics import MeterProvider

from provisa.api.otel_setup import (
    _make_log_exporter,
    _make_span_exporter,
    _MetricExports,
    _Reported,
)

ENDPOINT = "http://collector.invalid:4318"
_OURS = "provisa.api.otel_setup"


class _Reply:
    ok = True
    status_code = 200
    reason = "OK"


class _Collector:
    """A stand-in for the exporter's HTTP session: refuses the connection until ``up``, and
    refuses a timeout of zero or less exactly as urllib3 does."""

    def __init__(self) -> None:
        self.up = False
        self.timeouts: list[float] = []

    def request(self, timeout=None, **_request):
        self.timeouts.append(timeout)
        if timeout <= 0:
            raise ValueError(
                f"Attempted to set connect timeout to {timeout}, but the timeout cannot be set "
                "to a value less than or equal to 0."
            )
        if not self.up:
            raise requests.exceptions.ConnectionError("connection refused")
        return _Reply()


@pytest.fixture
def export(monkeypatch):
    """The process's metric export, its collector, and a clock the backoff wait moves."""
    exporter = OTLPMetricExporter(endpoint=ENDPOINT + "/v1/metrics", timeout=1)
    collector = _Collector()
    client = exporter._client
    monkeypatch.setattr(client._transport._session, "request", collector.request)
    clock = [1000.0]
    monkeypatch.setattr(lib, "time", SimpleNamespace(time=lambda: clock[0]))
    monkeypatch.setattr(lib.random, "uniform", lambda _low, _high: 1.0)  # a 1 s first backoff

    def _late_wake(seconds: float) -> bool:
        # The wait returns half a second late: the export's one-second deadline has passed.
        clock[0] += seconds + 0.5677816867828369
        return False

    monkeypatch.setattr(client._shutdown_event, "wait", _late_wake)
    exports = _MetricExports(exporter, interval_millis=3_600_000)  # the thread never ticks here
    provider = MeterProvider(metric_readers=[exports.reader])
    counter = provider.get_meter("test").create_counter("requests")
    exports.start(exporter, ENDPOINT)
    try:
        yield exports, collector, counter
    finally:
        exports.stop()


def _ours(caplog) -> list[logging.LogRecord]:
    return [r for r in caplog.records if r.name == _OURS and r.levelno >= logging.WARNING]


def test_the_library_sends_again_with_no_time_left(export):
    """The condition itself, shown on the installed library: the attempt after the backoff is
    made with no time left, and its refusal comes back as a failed export rather than raising."""
    exports, collector, counter = export
    counter.add(1)
    exports.reader.collect()
    # The first attempt has the whole second (the library sends it twice at once on a refused
    # connection); the attempt after the backoff is sent with none.
    assert collector.timeouts[0] == pytest.approx(1.0)
    assert collector.timeouts[-1] == 0.0


def test_an_export_that_cannot_be_sent_is_one_line_naming_the_endpoint(export, caplog):
    exports, _collector, counter = export
    caplog.set_level(logging.DEBUG)
    counter.add(1)
    exports.collect_once()
    (line,) = _ours(caplog)
    assert line.getMessage() == (
        f"OTel metrics are not reaching {ENDPOINT}: the exporter reported a failed export"
    )
    assert line.exc_info is None, "a traceback every tick is what this replaces"


def test_it_is_said_once_until_the_collector_is_back_and_then_once_more(export, caplog):
    exports, collector, counter = export
    caplog.set_level(logging.WARNING)
    for _tick in range(3):
        counter.add(1)
        exports.collect_once()
    assert len(_ours(caplog)) == 1, [r.getMessage() for r in _ours(caplog)]

    collector.up = True
    counter.add(1)
    exports.collect_once()
    lines = [r.getMessage() for r in _ours(caplog)]
    assert lines[1] == f"OTel metrics are reaching {ENDPOINT} again (3 export(s) did not)"

    counter.add(1)
    exports.collect_once()
    assert len(_ours(caplog)) == 2, "an export that succeeds says nothing"


def test_an_export_the_exporter_reports_failed_is_the_same_condition(export, caplog, monkeypatch):
    """Not every failure raises: a refused connection with no time left to retry comes back as
    the exporter's failure result."""
    exports, collector, counter = export
    caplog.set_level(logging.WARNING)
    # No late wake-up here: the library sees its backoff does not fit and gives up by itself.
    monkeypatch.setattr(lib.random, "uniform", lambda _low, _high: 1.2)
    counter.add(1)
    exports.collect_once()
    (line,) = _ours(caplog)
    assert line.getMessage() == (
        f"OTel metrics are not reaching {ENDPOINT}: the exporter reported a failed export"
    )
    assert all(t > 0 for t in collector.timeouts)


# -- traces and logs: the same report, around the exporter the SDK's batch processor calls --------


class _Exporter:
    """A stand-in span or log exporter: answers what it is told to, or raises it."""

    def __init__(self, success, failure) -> None:
        self.success, self.failure = success, failure
        self.answers: list = []
        self.batches: list = []

    def export(self, batch):
        self.batches.append(batch)
        answer = self.answers.pop(0)
        if isinstance(answer, BaseException):
            raise answer
        return answer

    def shutdown(self) -> str:
        return "shut down"


def _signal(name: str):
    if name == "traces":
        from opentelemetry.sdk.trace.export import SpanExportResult as Result
    else:
        from opentelemetry.sdk._logs.export import LogRecordExportResult as Result
    inner = _Exporter(Result.SUCCESS, Result.FAILURE)
    return inner, _Reported(inner, name, ENDPOINT, Result.FAILURE)


@pytest.mark.parametrize("name", ["traces", "logs"])
def test_spans_and_logs_that_cannot_be_sent_are_reported_the_same_way(name, caplog):
    inner, reported = _signal(name)
    caplog.set_level(logging.DEBUG)
    late = ValueError(
        "Attempted to set connect timeout to -0.5, but the timeout cannot be set to a value less than or equal to 0."
    )
    inner.answers = [late, inner.failure, inner.failure, inner.success, inner.success]

    # An export that raises is answered to the batch processor as a failed export, not raised
    # into it (it would log the traceback for every batch).
    assert reported.export(["a"]) is inner.failure
    (line,) = _ours(caplog)
    assert line.getMessage().startswith(f"OTel {name} are not reaching {ENDPOINT}: ValueError: ")
    assert line.exc_info is None
    # ... and its traceback is still to be had, at debug level.
    assert any(
        r.name == _OURS and r.levelno == logging.DEBUG and r.exc_info for r in caplog.records
    )

    assert reported.export(["b"]) is inner.failure
    assert reported.export(["c"]) is inner.failure
    assert len(_ours(caplog)) == 1, "said once while it lasts"

    assert reported.export(["d"]) is inner.success
    assert _ours(caplog)[1].getMessage() == (
        f"OTel {name} are reaching {ENDPOINT} again (3 export(s) did not)"
    )
    assert reported.export(["e"]) is inner.success
    assert len(_ours(caplog)) == 2
    assert inner.batches == [["a"], ["b"], ["c"], ["d"], ["e"]], "every batch reached the exporter"


@pytest.mark.parametrize("name", ["traces", "logs"])
def test_a_failed_export_the_exporter_reports_is_the_same_condition(name, caplog):
    inner, reported = _signal(name)
    caplog.set_level(logging.WARNING)
    inner.answers = [inner.failure]
    assert reported.export(["a"]) is inner.failure
    (line,) = _ours(caplog)
    assert line.getMessage() == (
        f"OTel {name} are not reaching {ENDPOINT}: the exporter reported a failed export"
    )


def test_everything_else_is_the_exporters_own():
    inner, reported = _signal("traces")
    assert reported.shutdown() == "shut down"
    assert reported.batches is inner.batches


@pytest.mark.parametrize(
    ("make", "name", "path"),
    [(_make_span_exporter, "traces", "/v1/traces"), (_make_log_exporter, "logs", "/v1/logs")],
)
def test_the_exporters_the_process_makes_are_reported_ones(make, name, path):
    exporter = make(ENDPOINT, "http/protobuf")
    assert isinstance(exporter, _Reported)
    assert exporter._report._signal == name and exporter._report._endpoint == ENDPOINT
    assert exporter._endpoint == ENDPOINT + path  # the wrapped exporter's own attribute
    exporter.shutdown()
