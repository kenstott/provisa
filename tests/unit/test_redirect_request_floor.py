# Copyright (c) 2026 Kenneth Stott
# Canary: 8d2f6a41-0b7c-4e39-a5d1-3c9e7f2b6a84
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A request's redirect threshold stays at or under the operator's (REQ-029, amended 2026-09-30).

The operator's large-result threshold protects the platform: past it a result leaves the response
body for the object store. A request may lower it (redirect sooner), never raise it — a higher
request threshold would put a larger inline body through the platform than the operator allows,
so it is rejected with an error naming the setting, on every transport that takes one.
"""

# Requirements: REQ-029, REQ-1194

from __future__ import annotations

import pytest

from provisa.executor import redirect
from provisa.executor.redirect import (
    RedirectFloorViolation,
    delivery_from_request,
    request_redirect_config,
)


@pytest.fixture
def operator_threshold(monkeypatch):
    monkeypatch.setenv("PROVISA_REDIRECT_ENABLED", "true")
    monkeypatch.setenv("PROVISA_REDIRECT_THRESHOLD", "1000")
    monkeypatch.setattr(redirect, "_org_overrides_resolver", None)
    return 1000


def test_a_request_may_lower_the_threshold(operator_threshold):
    assert request_redirect_config(10).threshold == 10


def test_a_request_may_not_raise_the_threshold_above_the_operators(operator_threshold):
    with pytest.raises(RedirectFloorViolation) as exc:
        request_redirect_config(1_000_000)
    assert "1000" in str(exc.value) and "threshold" in str(exc.value)


def test_the_violation_is_a_permission_error_every_transport_maps():
    assert issubclass(RedirectFloorViolation, PermissionError)


def test_bolt_and_jsonapi_requests_are_bounded_the_same_way(operator_threshold):
    with pytest.raises(RedirectFloorViolation):
        delivery_from_request(force_redirect=True, redirect_format=None, threshold=5000, role="r")
    ok = delivery_from_request(force_redirect=True, redirect_format=None, threshold=5, role="r")
    assert ok is not None and ok.config.threshold == 5


def test_with_redirect_disabled_a_request_threshold_only_redirects_sooner(monkeypatch):
    monkeypatch.setenv("PROVISA_REDIRECT_ENABLED", "false")
    monkeypatch.setattr(redirect, "_org_overrides_resolver", None)
    cfg = request_redirect_config(50)
    assert cfg.enabled and cfg.threshold == 50
