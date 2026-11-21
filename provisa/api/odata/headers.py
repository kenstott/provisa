# Copyright (c) 2026 Kenneth Stott
# Canary: d0108088-632f-4034-8eaa-d93b1e40b7d0
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What every response of the OData service carries (REQ-2193): ``OData-Version``, and on a 401
the challenge a client answers by asking for credentials.

A middleware, because a request with no credential never reaches the router: the auth middleware
answers it. So this wraps that one, and it is registered after it.
"""

# Requirements: REQ-2193

from __future__ import annotations

from typing import Any

from starlette.datastructures import MutableHeaders

SERVICE_ROOT = "/data/odata"
_CHALLENGES = ('Basic realm="Provisa"', 'Bearer realm="Provisa"')


class ODataResponseHeaders:
    def __init__(self, app: Any) -> None:  # noqa: ANN401 — the ASGI app it wraps
        self._app = app

    async def __call__(self, scope: Any, receive: Any, send: Any) -> None:  # noqa: ANN401
        path = scope.get("path", "")
        if scope["type"] != "http" or not (
            path == SERVICE_ROOT or path.startswith(f"{SERVICE_ROOT}/")
        ):
            await self._app(scope, receive, send)
            return

        async def _send(message: Any) -> None:  # noqa: ANN401 — an ASGI message
            if message["type"] == "http.response.start":
                headers = MutableHeaders(scope=message)
                headers["OData-Version"] = "4.0"
                if message["status"] == 401:
                    for challenge in _CHALLENGES:
                        headers.append("WWW-Authenticate", challenge)
            await send(message)

        await self._app(scope, receive, _send)
