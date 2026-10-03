# Copyright (c) 2026 Kenneth Stott
# Canary: 89e6ce75-307f-4fe0-bd2f-e460ae556829
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The HTTP face of "a coordinator serves no data requests" (REQ-1916).

Every request under ``/data`` is a data request. On a coordinator it is answered 503 with the
coded refusal before it reaches a route; the admin, health and every other route are served. The
other transports refuse at their own request boundary (``process_mode.refuse_data_request``)."""

# Requirements: REQ-1916

from __future__ import annotations

import json
from typing import Any

from provisa.core import process_mode

_DATA_PREFIX = "/data"


class CoordinatorDataGate:
    """ASGI middleware refusing ``/data`` requests on a coordinator, by name."""

    def __init__(self, inner: Any) -> None:
        self._inner = inner

    async def __call__(self, scope: Any, receive: Any, send: Any) -> None:
        path = scope.get("path", "")
        if scope["type"] == "http" and (
            path == _DATA_PREFIX or path.startswith(_DATA_PREFIX + "/")
        ):
            try:
                process_mode.refuse_data_request("http")
            except process_mode.CoordinatorServesNoData as refused:
                # The shape every ApiError is answered with (app._api_error_handler).
                body = json.dumps(
                    {
                        "detail": str(refused),
                        "code": refused.code,
                        "params": {"transport": refused.transport},
                    }
                ).encode()
                await send(
                    {
                        "type": "http.response.start",
                        "status": 503,
                        "headers": [(b"content-type", b"application/json")],
                    }
                )
                await send({"type": "http.response.body", "body": body})
                return
        await self._inner(scope, receive, send)
