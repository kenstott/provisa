# Copyright (c) 2026 Kenneth Stott
# Canary: d27d97fa-9bb1-4b64-b047-bd8257b6fc4f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""One HTTP request is one model change (REQ-1524).

The request runs inside a change scope (:mod:`provisa.core.model_change`). What it writes to an
environment's model is committed once, when its response is about to start — so a caller that
reads the environment's history after the answer finds the commit there — and again at its end
for anything a streamed response wrote after it started.
"""

# Requirements: REQ-1524

from __future__ import annotations

from typing import Any

from provisa.core import model_change


def _actor(scope: dict) -> str | None:
    """The member the request was made by, or None for an act no member signed (the system
    author stands in, :data:`provisa.core.env_repo.SYSTEM_AUTHOR`)."""
    identity = (scope.get("state") or {}).get("identity")
    user_id = getattr(identity, "user_id", None)
    return None if user_id in (None, "anonymous") else user_id


class ModelChangeMiddleware:
    def __init__(self, inner: Any) -> None:
        self._inner = inner

    async def __call__(self, scope: Any, receive: Any, send: Any) -> None:
        if scope["type"] != "http":
            await self._inner(scope, receive, send)
            return
        label = f"{scope.get('method', '')} {scope.get('path', '')}"
        async with model_change.scope(label, actor=lambda: _actor(scope)) as change:

            async def _send(message: dict) -> None:
                if message["type"] == "http.response.start":
                    await model_change.flush(change)
                await send(message)

            await self._inner(scope, receive, _send)
