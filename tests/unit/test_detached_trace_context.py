# Copyright (c) 2026 Kenneth Stott
# Canary: 4c1d7e2a-3b9f-4a68-8e5d-2f7c0b91a6e3
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Background work started from inside a request must not be parented under the request's span.

One ``POST /auth/redeem-invite`` on cloud carried 1338 spans over 47 minutes: the engine prewarm
task and the APScheduler wakeup chain (re-rooted by an ``add_job`` from the runtime build) both
inherited the request's contextvars, span included, and every poll they ever made was attributed
to that one request.
"""

import asyncio

import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider

from provisa.otel_compat import detached_trace_context

pytestmark = pytest.mark.unit


@pytest.fixture(scope="module")
def tracer():
    provider = TracerProvider()
    return provider.get_tracer("test")


def test_body_runs_with_no_active_span_and_the_caller_keeps_its_own(tracer):
    with tracer.start_as_current_span("request") as request_span:
        with detached_trace_context():
            assert not trace.get_current_span().get_span_context().is_valid
        assert trace.get_current_span() is request_span


async def test_a_task_spawned_inside_the_body_roots_its_own_trace(tracer):
    seen: list[bool] = []

    async def background() -> None:
        seen.append(trace.get_current_span().get_span_context().is_valid)

    with tracer.start_as_current_span("request"):
        with detached_trace_context():
            task = asyncio.create_task(background())
        inherited = asyncio.create_task(background())
    await asyncio.gather(task, inherited)
    assert seen == [False, True], "detached task had no span; an undetached one inherits it"


async def test_scheduler_wakeup_runs_detached(tracer):
    """APScheduler's ``wakeup`` hands ``_process_jobs`` to the loop and re-arms its timer, and both
    hand-offs copy the CALLER's context. ``add_job`` from inside a request calls ``wakeup`` -- so
    the whole chain, and every job it runs from then on, inherited that request's span."""
    from provisa.scheduler.jobs import new_scheduler

    scheduler = new_scheduler()
    scheduler._eventloop = asyncio.get_running_loop()
    inside: list[bool] = []
    scheduler._process_jobs = lambda: inside.append(  # type: ignore[method-assign]
        trace.get_current_span().get_span_context().is_valid
    )
    scheduler._start_timer = lambda _w: None  # type: ignore[method-assign]
    scheduler._stop_timer = lambda: None  # type: ignore[method-assign]
    with tracer.start_as_current_span("request"):
        scheduler.wakeup()
        await asyncio.sleep(0)
    assert inside == [False]
