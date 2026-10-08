# Copyright (c) 2026 Kenneth Stott
# Canary: 5a54c9a2-de12-4051-9554-3c14c0b37811
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A background pass whose runtime is gone leaves its work to the runtime that replaces it
(REQ-1266, REQ-1529).

Wiring a runtime's jobs and converging its replicas are detached from the build they follow. A
change of an environment's data drops that runtime and builds another, and the detached pass,
still bound to the environment, then found "no runtime built" and logged it three times as an
error with a traceback (poll-job wiring, replica convergence, the lifecycle task). It is not an
error: the next runtime's build does the work."""

from __future__ import annotations

import logging
from types import SimpleNamespace

import pytest

from provisa.core.request_context import (
    reset_current_env,
    reset_current_org,
    set_current_env,
    set_current_org,
)
from provisa.core.runtime_gone import RuntimeNotBuilt, left_to_the_next_runtime


@pytest.fixture
def bound_to_a_dropped_environment():
    org, env = set_current_org("acme"), set_current_env("recopied")
    yield
    reset_current_env(env)
    reset_current_org(org)


def test_work_bound_to_an_environment_with_no_runtime_is_refused_by_name(
    bound_to_a_dropped_environment,
):
    from provisa.api.app import state

    with pytest.raises(RuntimeNotBuilt) as raised:
        state._active_runtime()
    assert (raised.value.org_id, raised.value.env) == ("acme", "recopied")
    assert "no runtime built for environment 'recopied' of org 'acme'" in str(raised.value)
    assert isinstance(raised.value, RuntimeError)  # what a request's refusal has always been


def test_the_skip_is_stated_once_at_debug_without_a_traceback(caplog):
    gone = RuntimeNotBuilt("acme", "recopied", "no runtime built for environment 'recopied'")
    with caplog.at_level(logging.DEBUG, logger="provisa.core.runtime_gone"):
        left_to_the_next_runtime(gone, "replica convergence")
    (record,) = caplog.records
    assert record.levelno == logging.DEBUG and record.exc_info is None
    assert "replica convergence left to the next runtime of org acme environment recopied" in (
        record.getMessage()
    )


async def test_replica_convergence_for_a_dropped_runtime_reports_no_error(
    bound_to_a_dropped_environment, caplog, monkeypatch
):
    from provisa.federation import replica_converge

    async def _converge(_state):
        raise RuntimeNotBuilt("acme", "recopied", "no runtime built for environment 'recopied'")

    monkeypatch.setattr(replica_converge, "converge_replicas", _converge)
    state = SimpleNamespace(federation_engine=object(), tenant_db=object())
    with caplog.at_level(logging.DEBUG):
        await replica_converge.converge_logged(state)
    assert not [r for r in caplog.records if r.levelno >= logging.WARNING]
    assert "acme" not in replica_converge.last_error  # nothing kept as a convergence failure


async def test_poll_job_wiring_for_a_dropped_runtime_hands_the_decision_to_its_caller(
    bound_to_a_dropped_environment, caplog
):
    """It used to log "poll-job wiring failed" and return 0; the caller now decides, in the one
    place (left_to_the_next_runtime)."""
    from provisa.api.app import state
    from provisa.events.app_wiring import wire_new_poll_jobs

    with caplog.at_level(logging.DEBUG), pytest.raises(RuntimeNotBuilt):
        await wire_new_poll_jobs(state=state, log=logging.getLogger("test"))
    assert not [r for r in caplog.records if r.levelno >= logging.ERROR]
