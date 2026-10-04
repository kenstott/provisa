# Copyright (c) 2026 Kenneth Stott
# Canary: edfe84a5-dc0e-4a70-afa0-a027ca7c3cba
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A webhook that requires approval is called only once the approval hook approves the call.

REQ-209: a webhook configured with ``governance: requires_approval`` is put to the deployment's
approval hook (REQ-203) before its endpoint is called — the same approval a command declares
(REQ-1924). Unapproved, nothing reaches the endpoint; with no hook configured the call is
refused; approved, the endpoint is called with the call's arguments.
"""

# Requirements: REQ-209, REQ-1924

from __future__ import annotations

import pytest

from provisa.api.errors import ApiError
from provisa.core.models import FunctionArgument, Webhook
from tests.webhook_call import REPLY, ROLE, Endpoint, RecordingHook, call_webhook

_ARGS = {"order_id": 42, "reason": "manual-retry"}


def _webhook(**extra) -> Webhook:
    return Webhook(
        name="trigger_external_service",
        url="https://api.example.com/trigger",
        method="POST",
        arguments=[
            FunctionArgument(name="order_id", type="Int"),
            FunctionArgument(name="reason", type="String"),
        ],
        visible_to=[ROLE],
        **extra,
    )


def test_the_requirement_spelling_sets_approval_and_any_other_governance_is_refused():
    assert _webhook(governance="requires_approval").requires_approval is True
    assert _webhook(requires_approval=True).requires_approval is True
    assert _webhook().requires_approval is False
    with pytest.raises(ValueError, match="governance 'registry-required' is not a webhook setting"):
        _webhook(governance="registry-required")


def test_with_no_hook_configured_the_call_is_refused_and_nothing_is_sent(monkeypatch):
    endpoint = Endpoint()
    with pytest.raises(ApiError) as err:
        call_webhook(
            _webhook(governance="requires_approval"),
            _ARGS,
            hook=None,
            endpoint=endpoint,
            monkeypatch=monkeypatch,
        )
    assert (err.value.status_code, err.value.code) == (403, "functions.approval_unavailable")
    assert endpoint.requests == []


def test_a_call_the_hook_denies_is_refused_and_nothing_is_sent(monkeypatch):
    hook = RecordingHook(approved=False, reason="outside the change window")
    endpoint = Endpoint()
    with pytest.raises(ApiError) as err:
        call_webhook(
            _webhook(governance="requires_approval"),
            _ARGS,
            hook=hook,
            endpoint=endpoint,
            monkeypatch=monkeypatch,
        )
    assert (err.value.status_code, err.value.code) == (403, "functions.approval_denied")
    assert "outside the change window" in err.value.detail
    assert endpoint.requests == []
    # The hook was asked about this call: the command, the role and the arguments.
    (asked,) = hook.asked
    assert (asked.operation, asked.command, asked.roles, asked.arguments) == (
        "command",
        "trigger_external_service",
        [ROLE],
        _ARGS,
    )


def test_an_approved_call_reaches_the_endpoint_with_its_arguments(monkeypatch):
    hook = RecordingHook(approved=True)
    endpoint = Endpoint()
    rows = call_webhook(
        _webhook(governance="requires_approval"),
        _ARGS,
        hook=hook,
        endpoint=endpoint,
        monkeypatch=monkeypatch,
    )
    assert len(hook.asked) == 1
    assert endpoint.requests == [("POST", "https://api.example.com/trigger", _ARGS)]
    assert rows == [REPLY]


def test_a_webhook_that_requires_no_approval_is_not_put_to_the_hook(monkeypatch):
    hook = RecordingHook(approved=False)
    endpoint = Endpoint()
    rows = call_webhook(_webhook(), _ARGS, hook=hook, endpoint=endpoint, monkeypatch=monkeypatch)
    assert hook.asked == []
    assert endpoint.requests == [("POST", "https://api.example.com/trigger", _ARGS)]
    assert rows == [REPLY]
