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

from starlette.responses import JSONResponse


# Plain ASGI middleware, not starlette.middleware.base.BaseHTTPMiddleware: that class relays the
# inner app's response body through a background task + anyio memory stream, which fails to signal
# completion to the client for unbounded StreamingResponse bodies (SSE subscriptions, REQ-219) even
# after the inner generator has fully finished — the connection hangs open. A pure ASGI middleware
# calls the inner app's `send` directly, so no such relay exists.
class RateLimitMiddleware:  # REQ-369, REQ-371
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        response = await self._process(scope)
        if response is not None:
            await response(scope, receive, send)
            return
        await self.app(scope, receive, send)

    async def _process(self, scope):
        from provisa.api.app import state

        limiter = getattr(state, "rate_limiter", None)
        role_id = scope.setdefault("state", {}).get("role")
        if limiter is None or not role_id:
            return None

        # An empty roles registry means no config was loaded (unsecured / not-yet-set-up native
        # server): there is no role model to validate against and no per-role limits to apply, so
        # pass through. A POPULATED registry still denies an unknown role — it must not be silently
        # treated as unlimited. Without this, the unsecured default role ('org_admin') is "unknown"
        # and every request 403s, walling off a configless server (and its setup flow) entirely.
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
        return None
