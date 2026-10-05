# Copyright (c) 2026 Kenneth Stott
# Canary: 1b9b7b5b-f785-4724-80db-1afcac35c3cb
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A process holds one OTLP trace, metric and log pipeline (REQ-302, REQ-545, REQ-549).

Setting up again -- an app built twice in one process, or the exporter endpoint changed at
runtime -- replaces what exports: the replaced exporter is shut down, its thread and HTTP client
with it, never left exporting beside the new one. shutdown_otel stops every export thread.
"""

# Requirements: REQ-545, REQ-549

from __future__ import annotations

import logging
import os
import threading

from fastapi import FastAPI

from provisa.api import otel_setup


def _otel_handlers() -> list:
    from opentelemetry.sdk._logs import LoggingHandler

    return [h for h in logging.getLogger().handlers if isinstance(h, LoggingHandler)]


def _spy_shutdown(provider, shut: list) -> None:
    real = provider.shutdown

    def _shutdown():
        shut.append(provider)
        real()

    provider.shutdown = _shutdown


def test_setting_up_again_replaces_the_log_pipeline():
    otel_setup.setup_otel(FastAPI())  # the unit suite's endpoint is set (tests/conftest.py)
    first = otel_setup._log_provider
    assert first is not None
    shut: list = []
    _spy_shutdown(first, shut)

    otel_setup.setup_otel(FastAPI())
    assert otel_setup._log_provider is not first
    assert shut == [first]
    assert len(_otel_handlers()) == 1

    otel_setup.shutdown_otel()
    assert _otel_handlers() == [] and otel_setup._log_provider is None


def test_an_endpoint_change_replaces_the_log_pipeline():
    otel_setup.setup_otel(FastAPI())
    first = otel_setup._log_provider
    assert first is not None
    shut: list = []
    _spy_shutdown(first, shut)

    # The session's own receiver (tests/conftest.py): a changed endpoint that accepts exports.
    otel_setup.attach_otlp_exporters(
        os.environ["OTEL_EXPORTER_OTLP_ENDPOINT"], "provisa", "http/protobuf"
    )
    assert otel_setup._log_provider is not first and otel_setup._log_provider is not None
    assert shut == [first]
    assert len(_otel_handlers()) == 1

    otel_setup.shutdown_otel()
    assert _otel_handlers() == []


def _export_threads() -> dict[str, int]:
    names = [t.name for t in threading.enumerate() if t.is_alive()]
    return {
        kind: sum(1 for n in names if n == name)
        for kind, name in (
            ("span", "OtelBatchSpanRecordProcessor"),
            ("log", "OtelBatchLogRecordProcessor"),
            ("metric", "provisa-otel-metrics"),
        )
    }


def test_two_apps_in_one_process_leave_one_export_thread_of_each_kind():
    """The leak: every app built in a process started its own trace and metric export threads,
    which the process never installed and never stopped -- the metric one POSTing to the
    collector on every interval for the rest of the process."""
    otel_setup.shutdown_otel()
    assert _export_threads() == {"span": 0, "log": 0, "metric": 0}
    otel_setup.setup_otel(FastAPI())
    otel_setup.setup_otel(FastAPI())
    otel_setup.attach_otlp_exporters(
        os.environ["OTEL_EXPORTER_OTLP_ENDPOINT"], "provisa", "http/protobuf"
    )
    assert _export_threads() == {"span": 1, "log": 1, "metric": 1}

    otel_setup.shutdown_otel()
    assert _export_threads() == {"span": 0, "log": 0, "metric": 0}
