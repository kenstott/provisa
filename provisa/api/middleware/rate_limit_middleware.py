# Copyright (c) 2026 Kenneth Stott
# Canary: b392126e-e8cf-47b5-8890-05c6b1570987
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Per-role request rate limiting (REQ-369).

Enforced at the API layer, before route handlers compile or execute anything.
Requests over the role's ``requests_per_second`` get HTTP 429 + ``Retry-After``.
Must run AFTER the auth middleware (which sets ``request.state.role``); in Starlette
that means it is added BEFORE ``wire_auth`` so auth ends up the outer layer.
"""

# Requirements: REQ-369, REQ-371

from __future__ import annotations

from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import JSONResponse, Response


class RateLimitMiddleware(BaseHTTPMiddleware):  # REQ-369, REQ-371
    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        from provisa.api.app import state
        from starlette.requests import ClientDisconnect

        limiter = getattr(state, "rate_limiter", None)
        role_id = getattr(request.state, "role", None)
        if limiter is None or not role_id:
            try:
                return await call_next(request)
            except ClientDisconnect:
                return Response(status_code=499)

        # An empty roles registry means no config was loaded (unsecured / not-yet-set-up native
        # server): there is no role model to validate against and no per-role limits to apply, so
        # pass through. A POPULATED registry still denies an unknown role — it must not be silently
        # treated as unlimited. Without this, the unsecured default role ('admin') is "unknown" and
        # every request 403s, walling off a configless server (and its setup flow) entirely.
        if state.roles:
            if role_id not in state.roles:
                return JSONResponse(
                    status_code=403,
                    content={"error": "forbidden", "detail": f"unknown role {role_id!r}"},
                )
            role = state.roles[role_id] or {}
            rate_limit = role.get("rate_limit") or {}
            rps = rate_limit.get("requests_per_second")
            if rps:
                allowed, retry_after = await limiter.allow(f"rl:req:{role_id}", rps, 1.0)
                if not allowed:
                    return JSONResponse(
                        status_code=429,
                        content={"error": "rate_limited", "detail": "request rate limit exceeded"},
                        headers={"Retry-After": str(max(1, int(retry_after + 0.999)))},
                    )
        try:
            return await call_next(request)
        except ClientDisconnect:
            return Response(status_code=499)
