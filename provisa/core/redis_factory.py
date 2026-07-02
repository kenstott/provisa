# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Single Redis client factory with an embedded fakeredis fallback (REQ-829).

Every Redis connection in Provisa routes through :func:`make_redis`. When a URL
is configured it returns a real ``redis.asyncio`` client; otherwise it returns
an embedded ``fakeredis.FakeAsyncRedis`` backed by one process-wide
``fakeredis.FakeServer``. All fake clients — which differ only in
``decode_responses`` — share that single server, so they see the same in-memory
store. This lets a developer run the full backend (result cache, APQ cache, hot
tables, rate limiter, invalidation index) with zero Redis and zero Docker while
exercising the identical code paths production runs against a real Redis, rather
than the previous silent no-op fallbacks.

Tenant isolation for the embedded medium is enforced in the app layer via the
existing key namespacing (``provisa:cache:<tenant_id>:...`` etc.), not by any
store-native RLS; the single shared FakeServer is acceptable because desktop is
single-tenant.
"""

from __future__ import annotations

import threading
from typing import Any

_fake_server: Any = None
_fake_lock = threading.Lock()


def _get_fake_server() -> Any:
    """Return the process-wide FakeServer, creating it on first use."""
    global _fake_server
    if _fake_server is None:
        with _fake_lock:
            if _fake_server is None:
                import fakeredis

                _fake_server = fakeredis.FakeServer()
    return _fake_server


def make_redis(url: str | None, *, decode_responses: bool) -> Any:
    """Return an async Redis client.

    Args:
        url: Redis connection URL. When falsy, an embedded fakeredis client
            backed by the shared process-wide FakeServer is returned.
        decode_responses: Whether string responses are decoded to ``str``.
    """
    if url:
        import redis.asyncio as aioredis

        return aioredis.from_url(url, decode_responses=decode_responses)

    import fakeredis

    return fakeredis.FakeAsyncRedis(server=_get_fake_server(), decode_responses=decode_responses)
