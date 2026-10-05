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

# Requirements: REQ-369, REQ-371, REQ-1266, REQ-1327

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
        request_state = scope.setdefault("state", {})
        role_id = request_state.get("role")
        if limiter is None or not role_id:
            return None
        # REQ-1266/REQ-1327: a request acting in an org is bound to it and its role is that org's.
        # A request acting in no org (the platform plane: a signed-in user with no org named or no
        # membership yet) is bound to nothing, and its role is a platform role, read as such.
        # The bucket is the org's role, never a role id shared across orgs: one org's traffic
        # must not spend another's limit. The platform plane's roles have a bucket of their own.
        active_org_id = request_state.get("active_org_id")
        if active_org_id is None:
            roles = state.platform_roles
            bucket = f"rl:platform:req:{role_id}"
        else:
            roles = state.roles
            bucket = f"rl:req:{active_org_id}:{role_id}"

        # An empty roles registry means no config was loaded (unsecured / not-yet-set-up native
        # server): there is no role model to validate against and no per-role limits to apply, so
        # pass through. A POPULATED registry still denies an unknown role — it must not be silently
        # treated as unlimited. Without this, the unsecured default role ('org_admin') is "unknown"
        # and every request 403s, walling off a configless server (and its setup flow) entirely.
        if roles:
            if role_id not in roles:
                return JSONResponse(
                    status_code=403,
                    content={"error": "forbidden", "detail": f"unknown role {role_id!r}"},
                )
            role = roles[role_id] or {}
            rate_limit = role.get("rate_limit") or {}
            rps = rate_limit.get("requests_per_second")
            if rps:
                allowed, retry_after = await limiter.allow(bucket, rps, 1.0)
                if not allowed:
                    return JSONResponse(
                        status_code=429,
                        content={"error": "rate_limited", "detail": "request rate limit exceeded"},
                        headers={"Retry-After": str(max(1, int(retry_after + 0.999)))},
                    )
        return None
