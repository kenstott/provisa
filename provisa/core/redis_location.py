# Copyright (c) 2026 Kenneth Stott
# Canary: b6d2f409-17ac-4e83-9d5b-c0f3a8e1247d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Which Redis this deployment uses (REQ-829, REQ-1913).

One answer for every caller — the response cache, the rate limiter, the NL job store, the per-org
Redis ACLs. They used to read ``REDIS_URL`` each for itself, some honouring the desktop launch's
embedded Redis and the config file and some not.
"""

# Requirements: REQ-829, REQ-1913

from __future__ import annotations

from provisa.core import settings_registry


def redis_url() -> str | None:
    """The Redis server's URL, or ``None`` for the embedded in-process Redis.

    A desktop launch (``cache.redis_embedded``, set by the launcher) uses the embedded one whatever
    URL is configured, so no Redis server is dialed where none runs (REQ-829).
    """
    if settings_registry.value("cache.redis_embedded"):
        return None
    return settings_registry.value("cache.redis_url")


def embedded() -> bool:
    """Whether this is a desktop launch running on the embedded Redis."""
    return settings_registry.value("cache.redis_embedded")
