# Copyright (c) 2026 Kenneth Stott
# Canary: 5e8b2c71-0d4a-4f39-9a62-c3b7e1f08d54
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A scheduled job is not the request that last registered a job (REQ-074, REQ-1386).

``add_job`` on a running scheduler calls ``wakeup`` synchronously, and APScheduler hands
``_process_jobs`` to the loop and re-arms its timer with the CALLER's contextvars. A job registered
from inside an HTTP request (a runtime build wiring its event loop, an admin change re-wiring a
view) therefore re-rooted the whole chain in that request's context, the audit identity among
them: every job the scheduler ran from then on -- a view's build, governed through the one pipeline
as the region's administrator -- wrote its ``query_audit_log`` row as that request's user, on the
``http`` surface. The bench's features test found one such row among the rows of its /data/sql
requests: no masks, no row filters, no row cap -- the administrator's build, not the request.
"""

import asyncio
import threading

import pytest

from provisa.audit.context import audit_identity_scope, current_audit_identity

pytestmark = pytest.mark.unit


async def test_the_wakeup_chain_holds_no_audit_identity_of_the_request_that_armed_it():
    from provisa.scheduler.jobs import new_scheduler

    scheduler = new_scheduler()
    scheduler._eventloop = asyncio.get_running_loop()
    seen: list[object] = []
    scheduler._process_jobs = lambda: seen.append(current_audit_identity())  # type: ignore[method-assign]
    scheduler._start_timer = lambda _w: None  # type: ignore[method-assign]
    scheduler._stop_timer = lambda: None  # type: ignore[method-assign]
    with audit_identity_scope("a-request-user", "http"):
        scheduler.wakeup()
        await asyncio.sleep(0)
        assert current_audit_identity() is not None, "the request keeps its own identity"
    assert seen == [None]


async def test_a_job_registered_inside_a_request_runs_with_no_audit_identity():
    """End to end: the server's scheduler, a job added from inside a request, the job's own run
    on a background worker -- it records nothing as that request's user."""
    from provisa.scheduler.jobs import new_scheduler

    scheduler = new_scheduler()
    scheduler.start()
    ran = threading.Event()
    seen: list[object] = []

    async def job() -> None:
        seen.append(current_audit_identity())
        ran.set()

    try:
        # the scheduler's own start-up wakeup runs first, so the job is found by the wakeup the
        # request's add_job arms
        await asyncio.sleep(0.2)
        with audit_identity_scope("a-request-user", "http"):
            scheduler.add_job(job, id="audit-identity-probe")
        assert await asyncio.to_thread(ran.wait, 10), "the job did not run"
    finally:
        scheduler.shutdown(wait=False)
    assert seen == [None]
