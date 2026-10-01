# Copyright (c) 2026 Kenneth Stott
# Canary: 8d1e6b3f-2a9c-4f7d-b5e8-3c6a9d0f1e42
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Resolved server limits, published by the API layer at config load (REQ-1678).

``provisa.compiler.sql_gen`` reads the default row cap from here rather than from ``api.app``
state, which is what keeps the compiler off the API import path. The API layer is the only writer.
"""

# Requirements: REQ-005, REQ-1678

from __future__ import annotations

import contextlib
import contextvars
from collections.abc import Generator

_server_limits: dict = {}

# REQ-1905: the transports a request arrives on, each with a request timeout of its own.
REQUEST_TRANSPORTS = (
    "graphql",
    "rest",
    "jsonapi",
    "sql_http",
    "cypher_http",
    "pgwire",
    "flight",
    "bolt",
    "grpc",
    "mcp",
)


def _check_transport(transport: str) -> None:
    if transport not in REQUEST_TRANSPORTS:
        raise ValueError(
            f"unknown transport {transport!r}: a request timeout exists for {REQUEST_TRANSPORTS}"
        )


def request_timeout_for(transport: str) -> float:  # REQ-1905
    """Seconds a request arriving on ``transport`` may take: THE resolver every place that binds
    a request deadline asks.

    The transport's own value (``limits.request_timeouts.<transport>``) when it has one, else
    the default every transport shares (``limits.request_timeout``). Both are operator settings
    resolved by the settings registry — stored value, environment, config file, shipped default
    — so a change made through the admin API is in force in every worker."""
    _check_transport(transport)
    from provisa.core import settings_registry

    own = settings_registry.value("limits.request_timeouts")[transport]
    if own is None:
        return float(settings_registry.value("limits.request_timeout"))
    return float(own)


# REQ-1905: the transport of the HTTP request being served, bound from its route by the API's
# transport middleware. The other transports are named by the surface their statements carry.
_request_transport: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "provisa_request_transport", default=None
)

# The HTTP data routes that are transports of their own. Any other HTTP route (the admin API, a
# discovery or schema route) has no value of its own and uses the default.
_HTTP_ROUTE_TRANSPORT = (
    ("/data/graphql", "graphql"),
    ("/data/sql", "sql_http"),
    ("/data/cypher", "cypher_http"),
    ("/data/rest", "rest"),
    ("/data/jsonapi", "jsonapi"),
)

# The audit surface a statement arrives under -> its transport. "airport" is the DuckDB airport
# service, which speaks Arrow Flight.
_SURFACE_TRANSPORT = {
    "pgwire": "pgwire",
    "flight": "flight",
    "airport": "flight",
    "bolt": "bolt",
    "grpc": "grpc",
    "mcp": "mcp",
}


def http_transport_for_path(path: str) -> str | None:
    """The transport an HTTP request to ``path`` is on, or ``None`` for a route that is not one."""
    for prefix, transport in _HTTP_ROUTE_TRANSPORT:
        if path == prefix or path.startswith(prefix + "/"):
            return transport
    return None


@contextlib.contextmanager
def bound_request_transport(transport: str | None) -> Generator[None]:
    """Name the transport of the request being served, for the enclosed work."""
    token = _request_transport.set(transport)
    try:
        yield
    finally:
        _request_transport.reset(token)


def statement_timeout(surface: str) -> tuple[float, str, str]:  # REQ-1905
    """For a statement arriving under ``surface``: (seconds it may take, the transport it is on,
    the setting that value comes from). An HTTP request is on the transport its route bound; a
    statement on another protocol is on that protocol's. An HTTP route that is not a transport
    of its own is timed by the default."""
    transport = _request_transport.get() or _SURFACE_TRANSPORT.get(surface)
    if transport is None:
        from provisa.core import settings_registry

        return (
            float(settings_registry.value("limits.request_timeout")),
            surface,
            ("limits.request_timeout"),
        )
    return request_timeout_for(transport), transport, request_timeout_setting(transport)


def request_timeout_setting(transport: str) -> str:  # REQ-1905
    """The name of the setting ``request_timeout_for(transport)`` answers from — what a
    timed-out request's error names, so the operator knows which value to change."""
    _check_transport(transport)
    from provisa.core import settings_registry

    if settings_registry.value("limits.request_timeouts")[transport] is None:
        return "limits.request_timeout"
    return f"limits.request_timeouts.{transport}"


def set_server_limits(limits: dict) -> None:
    """Publish the resolved limits (app_loaders does this once per config load)."""
    global _server_limits
    _server_limits = dict(limits)


def server_limits() -> dict:
    return _server_limits


def default_row_limit() -> int:
    """The hard cap on rows returned when the caller supplies no explicit LIMIT.

    ``PROVISA_DEFAULT_ROW_LIMIT`` overrides the configured value; the configured value is
    ``server.limits.default_row_limit`` (100 when the config sets none — the config schema's own
    default, mirrored from app_loaders).
    """
    # REQ-1900: a limit changed through the admin API is a row in the control plane, read by
    # every worker — not an environment write in the one worker that served the change.
    # REQ-1913: resolved through the settings registry (stored, then env, then config, then the
    # declared default), which is where its environment variable and default are stated.
    from provisa.core import settings_registry

    return settings_registry.value("limits.default_row_limit")


def flight_stream_default_limit() -> int:  # REQ-1905
    """The per-worker default for the Flight stream limit: the host's Flight budget — a third of
    ``min(32, cpus + 4)``, the figure the limit has always defaulted to for one process — divided
    among the launch's worker processes, and never below 2 per worker. Here, in core, because the
    settings catalog states it as the setting's default and core does not import the API."""
    import os

    from provisa.core.boot_lock import expected_workers

    host_budget = min(32, (os.cpu_count() or 1) + 4) // 3
    return max(2, host_budget // expected_workers())
