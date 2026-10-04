# Copyright (c) 2026 Kenneth Stott
# Canary: 9d59d63c-9c94-4c76-8cf2-120a2a9bf7c5
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""One call of a registered webhook through the real invocation path, for tests.

``call_webhook`` runs ``action_exec.invoke_tracked_webhook`` — the path every surface takes:
the command admission, the approval where the webhook declares it, the record, the HTTP call
and the governed rows. Only the edges are stood in for: the deployment's approval hook (which
answers as told and records what it was asked) and the remote endpoint (an httpx transport that
records each request and answers one row).
"""

# Requirements: REQ-209, REQ-1924

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import Any

import httpx

from provisa.auth.approval_hook import ApprovalRequest, ApprovalResponse
from provisa.core.models import Webhook

ROLE = "admin"
REPLY = {"id": 42, "region": "us-east"}
# The client the path opens, taken before any call replaces it, so calls can follow one another.
_REAL_CLIENT = httpx.AsyncClient


class RecordingHook:
    """An approval hook that answers ``approved`` and keeps every request it was asked."""

    def __init__(self, approved: bool, reason: str = "") -> None:
        self.approved = approved
        self.reason = reason
        self.asked: list[ApprovalRequest] = []

    async def evaluate(self, request: ApprovalRequest) -> ApprovalResponse:
        self.asked.append(request)
        return ApprovalResponse(approved=self.approved, reason=self.reason)


@dataclass
class Endpoint:
    """The webhook's remote endpoint: every request made to it, as (method, url, json body)."""

    requests: list[tuple[str, str, Any]] = field(default_factory=list)

    def transport(self) -> httpx.MockTransport:
        def _answer(request: httpx.Request) -> httpx.Response:
            self.requests.append((request.method, str(request.url), json.loads(request.content)))
            return httpx.Response(200, json=REPLY)

        return httpx.MockTransport(_answer)


def call_webhook(
    webhook: Webhook,
    args: dict,
    *,
    hook: RecordingHook | None,
    endpoint: Endpoint,
    monkeypatch: Any,
) -> list[dict]:
    """Call ``webhook`` with ``args`` as the role ``ROLE``, which it is assigned to, on a deployment
    whose approval hook is ``hook`` (None: none configured), its endpoint recording into
    ``endpoint``. Returns the rows answered; an ApiError from the path propagates."""
    from provisa.api.data import action_exec

    def _client(*a: Any, **k: Any) -> httpx.AsyncClient:
        return _REAL_CLIENT(*a, transport=endpoint.transport(), **k)

    monkeypatch.setattr(action_exec.httpx, "AsyncClient", _client)
    state = SimpleNamespace(
        tracked_functions={},
        # As the loader registers it: the stored row, a plain dict.
        tracked_webhooks={webhook.name: webhook.model_dump()},
        roles={
            ROLE: {"id": ROLE, "capabilities": ["write"], "domain_access": ["*"]},
        },
        approval_hook=hook,
        model_stamp=1,
    )
    return asyncio.run(action_exec.invoke_tracked_webhook(webhook.name, args, state, ROLE))
