# Copyright (c) 2026 Kenneth Stott
# Canary: 16f41389-3802-4e4a-b799-e020f99b5279
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1194: a request that asks for its result in the results store (``X-Provisa-Redirect``)
and whose engine-side write fails is refused by name.

It used to log the failure and answer with the rows inline: the caller received the result it
had asked not to receive, with nothing to say the redirect had failed."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from provisa.api.errors import ApiError
from provisa.compiler.directives import NO_CACHE_HINT
from provisa.executor.redirect import DeliveryFailed
from provisa.transpiler.router import Route


def _field_through(monkeypatch, *, materialize, failure):
    """``_execute_one_field`` over a stand-in pipeline whose plan's terminal raises ``failure``."""
    from provisa.api.data import endpoint
    from provisa.pgwire import _pipeline

    plan = SimpleNamespace(route=Route.ENGINE, cache_hit=None, materialize=materialize)
    monkeypatch.setattr(_pipeline, "_govern_and_route_compiled", AsyncMock(return_value=plan))
    monkeypatch.setattr(_pipeline, "_execute_plan", AsyncMock(side_effect=failure))
    monkeypatch.setattr(endpoint, "note_request_route", lambda route: None)
    compiled = SimpleNamespace(root_field="orders", sql="SELECT 1", params=[], nodes_sql=None)
    return endpoint._execute_one_field(
        compiled,
        SimpleNamespace(),
        SimpleNamespace(),
        "analyst",
        "json",
        delivery=materialize,
        as_of=None,
        steward_hint=None,
        query_session_props=None,
        cache_hint=NO_CACHE_HINT,
    )


async def test_a_forced_redirect_that_fails_is_refused_and_never_answered_inline(monkeypatch):
    delivery = SimpleNamespace(output_format="parquet")
    failed = DeliveryFailed(RuntimeError("bucket unreachable"), forced=True)
    with pytest.raises(ApiError) as refused:
        await _field_through(monkeypatch, materialize=delivery, failure=failed)
    assert (refused.value.status_code, refused.value.code) == (502, "data.redirect_failed")
    assert refused.value.params == {"error": "bucket unreachable"}


async def test_a_threshold_redirect_that_fails_is_refused_by_its_own_name(monkeypatch):
    """REQ-171: the delivery the threshold chose fails the request too, never inline."""
    failed = DeliveryFailed(RuntimeError("bucket unreachable"), forced=False)
    with pytest.raises(ApiError) as refused:
        await _field_through(monkeypatch, materialize=None, failure=failed)
    assert (refused.value.status_code, refused.value.code) == (502, "data.redirect_upload_failed")


async def test_the_pipeline_names_a_failed_delivery(monkeypatch):
    from provisa.executor import redirect

    monkeypatch.setattr(
        redirect, "run_materialize", AsyncMock(side_effect=OSError("bucket unreachable"))
    )
    with pytest.raises(DeliveryFailed) as named:
        await redirect.deliver(SimpleNamespace(), "SELECT 1", SimpleNamespace(), None, forced=True)
    assert named.value.forced is True and isinstance(named.value.cause, OSError)


async def test_a_deadline_during_a_delivery_is_a_timeout_not_a_delivery_failure(monkeypatch):
    from provisa.executor import redirect

    monkeypatch.setattr(redirect, "run_materialize", AsyncMock(side_effect=TimeoutError("late")))
    with pytest.raises(TimeoutError):
        await redirect.deliver(SimpleNamespace(), "SELECT 1", SimpleNamespace(), None, forced=False)


async def test_a_failed_read_with_no_delivery_is_not_named_a_redirect_failure(monkeypatch):
    """Any other failure is a 500 naming its cause; a resource error is a 503."""
    from fastapi import HTTPException

    with pytest.raises(HTTPException) as failed:
        await _field_through(monkeypatch, materialize=None, failure=RuntimeError("engine down"))
    assert not isinstance(failed.value, ApiError)
    assert (failed.value.status_code, failed.value.detail) == (500, "engine down")
    with pytest.raises(HTTPException) as unavailable:
        await _field_through(monkeypatch, materialize=None, failure=ConnectionError("refused"))
    assert (unavailable.value.status_code, unavailable.value.detail) == (503, "refused")
