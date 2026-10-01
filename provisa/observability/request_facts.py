# Copyright (c) 2026 Kenneth Stott
# Canary: 0a7d3c58-e1b4-4f92-a6c3-5b8e2d1f9047
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What a request span and the request metrics say about a statement (REQ-1910).

A statement's outcome is observed where every transport already converges — the audit seam
(``finalize_audit``) and the GraphQL response-cache hit — not from spans. That is what lets normal
trace detail drop every child span without losing the route, the engine, the sources, the role and
the status from the request span, and what keeps the request counter and the latency histogram
identical in either detail.
"""

# Requirements: REQ-1910

from __future__ import annotations

import time
from typing import Any

from fastapi.responses import JSONResponse

from provisa.core.request_context import current_org
from provisa.otel_compat import (
    annotate_request,
    record_query,
    record_stage,
    request_fact,
    request_transport,
)


# Status codes from here up mark the request record as an error.
_FIRST_ERROR_STATUS = 400

# The engine label of a request that reached no engine and no source: a GraphQL cache hit.
_NO_ENGINE = "none"


def _transport(plan_audit: Any) -> str | None:
    """The transport the statement arrived on: the request span's, else the audit surface. None
    when neither exists — a statement with no acting principal and no request (seeding, a
    rebuild, a refresh), which is not a request and is not counted as one."""
    transport = request_transport()
    if transport is not None:
        return transport
    return plan_audit.surface if plan_audit is not None else None


def observe_plan(plan: Any, status_code: int, *, cache_hit: bool = False) -> None:
    """Record a governed plan's outcome: request facts on the request span, and the metrics.

    ``cache_hit``: the statement was served from the response cache. It is reported under the
    cache route with no engine — the plan's own route is the one it would have run on."""
    transport = _transport(plan.audit)
    if transport is None:
        return
    # The plan's dialect is what the statement runs on: the federation engine's for the ENGINE
    # route, the source's own for DIRECT.
    route = "cache" if cache_hit else plan.route.name.lower()
    engine = _NO_ENGINE if cache_hit else plan.dialect
    if plan.span_attrs:
        # The ops `queries` report reads provisa.table/domain/role off the request record. A
        # request of several statements keeps its FIRST statement's in those columns, lists every
        # statement's table in provisa.tables and counts the statements.
        statements = (request_fact("provisa.statements") or 0) + 1
        if statements == 1:
            annotate_request(**{k.replace(".", "__"): v for k, v in plan.span_attrs.items()})
        annotate_request(
            provisa__statements=statements,
            provisa__tables=[
                *(request_fact("provisa.tables") or ()),
                plan.span_attrs["provisa.table"],
            ],
        )
    annotate_request(
        provisa__route=route,
        provisa__engine=engine,
        provisa__role=plan.role_id,
        provisa__sources=sorted(plan.sources) if plan.sources else None,
        db__source_id=plan.source_id,
        provisa__status=status_code,
        org_id=current_org.get(),
        cache__hit=True if cache_hit else None,
        error=True if status_code >= _FIRST_ERROR_STATUS else None,
    )
    if plan.audit is not None:
        record_query(
            transport=transport,
            route=route,
            engine=engine,
            status_code=status_code,
            duration_ms=(time.monotonic() - plan.audit.started) * 1000,
        )


def observe_cache_hit(*, role_id: str, sources: Any, rows: int, started: float) -> None:
    """Record a GraphQL response-cache hit — a request served without reaching the audit seam.

    ``started`` is the field's ``time.perf_counter()`` start."""
    annotate_request(
        provisa__route="cache",
        provisa__role=role_id,
        provisa__sources=sorted(sources) if sources else None,
        db__row_count=rows,
        cache__hit=True,
        provisa__status=200,
        org_id=current_org.get(),
    )
    transport = request_transport()
    if transport is not None:
        record_query(
            transport=transport,
            route="cache",
            engine=_NO_ENGINE,
            status_code=200,
            duration_ms=(time.perf_counter() - started) * 1000,
        )


class TimedJSONResponse(JSONResponse):
    """A JSONResponse whose body encoding is reported as the request's ``encode`` stage."""

    def render(self, content: Any) -> bytes:
        started = time.perf_counter()
        body = super().render(content)
        record_stage("encode", started)
        return body
