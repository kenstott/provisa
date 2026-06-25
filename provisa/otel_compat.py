# Copyright (c) 2026 Kenneth Stott
# Canary: 25e523a2-8c94-41af-b521-1f8176333c1a
# Canary: PLACEHOLDER
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holders.

"""No-op OpenTelemetry shim.

Provides ``get_tracer(name)`` that returns the real OTel tracer when the
``opentelemetry`` package is installed, or a no-op tracer otherwise.
Unit tests run without opentelemetry installed; production uses the real SDK.
"""

# Requirements: REQ-302, REQ-303

from __future__ import annotations

from typing import Protocol, cast


class _NoopSpan:
    def __enter__(self):
        return self

    def __exit__(self, *_):
        pass

    def set_attribute(self, *_):
        pass

    def record_exception(self, *_):
        pass

    def set_status(self, *_):
        pass


class TracerProtocol(Protocol):  # REQ-545
    def start_as_current_span(self, name: str, **kwargs) -> _NoopSpan: ...
    def start_span(self, name: str, **kwargs) -> _NoopSpan: ...


class _NoopTracer:
    def start_as_current_span(self, name: str, **kwargs) -> _NoopSpan:  # noqa: ARG002
        return _NoopSpan()

    def start_span(self, name: str, **kwargs) -> _NoopSpan:  # noqa: ARG002
        return _NoopSpan()


def get_tracer(name: str) -> TracerProtocol:  # REQ-302, REQ-303
    """Return the OTel tracer for *name*, or a no-op tracer if OTel is absent."""
    try:
        from opentelemetry import trace as _trace

        return cast(TracerProtocol, _trace.get_tracer(name))
    except ImportError:
        return _NoopTracer()
